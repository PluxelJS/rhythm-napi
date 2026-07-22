# 具体实现

本文解释设计如何映射到代码，以及修改底层实现时必须保持的约束。所有权和运行时序分别以
[architecture.md](architecture.md) 与 [runtime.md](runtime.md) 为准。

## 模块职责

| 模块 | 职责 | 不应承担的职责 |
| --- | --- | --- |
| `session` | 纯 actor 状态机、generation 过滤、action/event 规划 | task、socket、codec、playlist |
| `runtime` | action 编排、producer/sender 生命周期、共享资源 | provider 策略、协议字节细节 |
| `runtime/producer` | source 到 Opus queue 的 generation worker | RTP 时钟、playlist |
| `runtime/sender` | prebuffer、pacing、RTP/RTCP、进度 | source、decode、DSP、encode |
| `runtime/opus_queue` | 唯一的 duration-bounded encoded bridge | 第二级缓存或业务状态 |
| `source` | file/URL/live 获取、cache、共享 flight、配额 | CPU 调度策略、playlist retry |
| `audio` | decode、normalize、resample、DSP、Opus | async I/O、RTP session |
| `transport` | RTP/RTCP 配置、marshal/parse | pacing、媒体生产、平台协商 |
| `music_stream_napi` | Promise、类型转换、事件桥接、runtime registry | 媒体状态旁路或阻塞等待 |

## 控制原语

### PauseGate

同一个 gate 同时服务 async source task 和 blocking codec worker。状态使用原子值读取，async waiter
通过 watch 唤醒，blocking waiter 通过 condvar 唤醒；cancel 也必须让两类 waiter 在有限时间内退出。
pause 不能用 cancellation 代替，因为 cancellation 表示 generation 生命周期终止，而 pause 要保留
当前 HTTP response、decoder 状态和 queue。

### Generation

generation 单调递增并随每次 current/next 替换进入 producer、queue frame 和 worker event。runtime
操作 handle 前校验 generation，actor 接收事件时再次校验。这个双边校验防止旧 task 的晚到结果
污染新媒体。

### Staged commit

actor command 在副本上执行，runtime action 完成后才覆盖正式 actor。事件也在提交后发布。新增
控制操作时必须先定义：哪些步骤可回滚、失败后保留旧 generation 还是关闭整个 runtime，以及 API
返回时哪些效果已经得到 acknowledgement。

## Source 实现

### 本地文件

Tokio 只执行 metadata 校验。文件打开、probe、demux 和 decode 在 blocking worker 中完成。
非空 regular file 才能成为 artifact；seek 使用新 decoder，不在旧 decoder 上并发改变游标。

### Bounded URL 与 growing spool

下载前按 `maxBytes` 申请最坏值 tempfile quota，再申请 HTTP slot。tempfile 创建通过 blocking task，
body 写入使用 Tokio file。每次成功写入后，spool 更新 `available_bytes` 并唤醒所有 reader。

每个 `GrowingSpoolReader` 拥有独立文件描述符和 position：

- position 小于 `available_bytes` 时正常读取；
- 追上写边界时通过 condvar 等待并让 wait observer 释放 CPU lease；
- complete 后在文件末尾返回 EOF；
- failure/cancel 转为明确 read error并唤醒等待者。

writer 未正常 finish 就 drop 会把 spool 标记为失败。partial spool 由 cleanup owner 删除，永远不会
进入 artifact cache。

### Shared URL flight

`TrackSource.id` 是内容身份，临时签名 URL 不是 key。single-flight key 由内容 ID、URL/header 的
非可逆 fingerprint 与完整 HTTP 传输策略组成；临时凭据不会出现在诊断 key，且 URL、header、
`maxBytes`、timeout、retry 或 cache 设置不同的请求不共享 flight。
artifact cache 虽按内容 ID 索引，每次命中仍重新检查当前调用者的 `maxBytes`，不能用历史
大上限绕过新请求的小上限。registry 只保存 weak flight，flight 自己拥有
reader/terminal watch、transfer pause gate、cancellation 和双层 task supervision。

registry每64次查询机会性移除 dead weak key，因此顺序播放大量唯一 ID不会让 key表永久增长。

subscriber 保存独立的 paused/current 状态。聚合状态满足：任一 active 则 transfer 继续；任一
current 使传输永久提升为 current admission；最后一个 subscriber 离开则 cancel。sticky promotion
避免已经产生 partial body 后重新申请 preload quota。

artifact resolver 和 progressive reader 都订阅同一 flight。完成后先形成只读 artifact，再尝试
放入 LRU；cache 拒绝并不改变当前 subscriber 使用完整 artifact 的能力。

### HTTP retry

有界 URL 只对连接/timeout、408、429 和 5xx 使用有界退避重试。每次 attempt 使用全新 tempfile；
任何正文已交付给 progressive reader 后都禁止重连，因为不能证明新响应与旧容器字节连续。
401/403 直接映射为 `SOURCE_AUTH_EXPIRED`，其他 terminal 4xx、字节上限和本地文件错误立即失败。

active I/O timeout 按未暂停的网络等待计算。pause 保持同一 future pinned；恢复时重置这段 active
deadline，但不重建 request。

### Artifact cache 与清理

cache 只保存完整 artifact，按稳定内容 ID 做 LRU，最多使用 `Streamer` tempfile 预算的一半。
调用方必须让 ID 包含内容版本语义；runtime 不根据 URL 或 provider metadata 猜 TTL。

artifact 最后一个引用释放时只向专用 cleanup thread 投递删除命令。quota permit 跟随 cleanup owner，
在文件真实删除后才释放。shutdown 使用 barrier 等待此前的删除命令完成，避免“API 已返回但磁盘
和配额尚未收敛”。

### Live HTTP

live open timeout 只覆盖建连与响应头，idle timeout 覆盖相邻 body chunk 之间的无数据时间，两者
都不是总播放时长。body chunk 按实际字节同时申请 per-stream 和 `Streamer` permit；大于 bridge
容量的 chunk 以 `Bytes::slice` 分段，不复制整个 body。

误标为 bounded URL 的响应若在正文交付前出现 `icy-*`，或同时出现 Icecast server 与音频 MIME，
source transfer 返回内部路由信号，current producer 释放 tempfile/HTTP admission 后改走既有 live
管线，并通知 actor 修正 source 能力。检测只使用强响应头证据，不用 MIME、未知长度或 cache header
猜测。显式传入 `live` 不经过这次兼容回退。

live 只允许在首个媒体字节交付前重试。partial body 失败必须终止 decoder，禁止把新响应拼接到
旧容器。reader 只有在实际 `blocking_recv` 前调用 wait observer，拿到字节后重新申请 CPU lease。

### HLS

`.m3u8` path 或显式 `formatHint: m3u8` 在进入 actor 前规范化为 live/current-only 语义。HLS source
使用共享 HTTP client 解析 master/media playlist、相对 URL，并按可支持 codec、默认/autoselect、声道数
和带宽选择音频 rendition；没有独立音频 rendition 时优先 audio-only 和低带宽候选；
VOD 遇到 `ENDLIST` 正常结束，live 从靠近末尾的三个 segment 开始并按 target duration 重载。
opaque bounded URL 若在正文前返回标准 playlist Content-Type，或最终 redirect URL 以 `.m3u8`
结尾，也会释放 artifact admission 并重路由到 HLS。

playlist 限 1 MiB、单 segment 限 16 MiB，open/idle timeout、取消、受限 header 和重试沿用 live
配置。响应在读取 body 前一次性申请共享 live-byte permit；已知长度按 `Content-Length`，未知长度按
受限上限预留，避免并发下载各占部分 permit 后互相等待。MPEG-TS 在原 allocation 内移除 TS/PES
header，再把 permit 连同 ADTS AAC 或 MPEG audio 字节移交给 live bridge，不复制整段媒体。

fMP4/CMAF 先获取一次有界 `EXT-X-MAP`，校验 `ftyp`/`moov` 后与 `moof`/`mdat` fragment 原样串入
Symphonia 的 non-seekable fragmented-MP4 demux。playlist 解析后线性继承 MAP/KEY 状态并补全隐式
byte-range offset；range 请求必须返回匹配的 206、`Content-Range` 和精确长度，服务端忽略或错返
range 时 fail closed。init map 改变仍要求新的 media generation。

`EXT-X-GAP` 只推进 media sequence，不请求声明缺失的 segment。`EXT-X-DISCONTINUITY` 在初始化 map
和实际音频容器/codec均未改变时作为连续解码边界接受；map引入、移除或变更以及 codec切换仍会
fail closed，等待显式 media-generation 重建能力。

标准 `METHOD=AES-128` 使用 identity key format：16-byte key 经同一有界 HTTP 路径获取并按 key URL
缓存，显式 IV 最多 128 bit且左侧补零，缺省 segment IV 由 media sequence生成；加密 init map按规范
必须给显式 IV。CBC/PKCS#7 在原 segment allocation 内解密，padding、block alignment、key长度或
key format不符都 fail closed。当前不支持 SAMPLE-AES/DRM、跨 discontinuity 的初始化段/codec切换和
LL-HLS partial segment。

## 音频管线

### Decode

`SymphoniaFileDecoder` 用于 seekable artifact，`SymphoniaStreamDecoder` 用于 growing spool 和 live
reader。decoder 返回 backend 自然 chunk，不强制上游切成固定 PCM 包。Symphonia 继续负责 Ogg
demux；Ogg Opus 的 mono/stereo packet 交给已有 libopus 解码，并遵守容器给出的 start/end trim。
其他已注册 codec 仍使用 Symphonia decoder。

连续损坏 packet 有显式预算；成功 frame 会重置计数。超过预算或整个 source 没有产生可解码 PCM
时返回 `DECODE_ERROR`，不能把损坏输入伪装成正常 track end。

### Normalize 与 resample

DSP 前统一为 stereo、48 kHz。mono 扩展到双声道，多声道按确定规则 downmix；Rubato 只在 sample
rate 不匹配时工作，并复用其输出缓冲。等待 source 数据不属于 resample 工作，不能持有 CPU lease。

### Frame assembly 与 DSP

FrameAssembler 从 decoder chunk 组装 960 samples/channel 的 20 ms frame。完整 frame 直接借用输入
slice，只保存跨 chunk 的短尾部；source end 的最后一个 partial frame 补零后编码，防止尾音或极短
音频被静默丢弃。

volume 使用感知曲线，gain 使用显式 dB；正增益经过 soft limiter。ReplayGain 只根据宿主提供的
metadata 计算建议值，不自动扫描音频，也不隐式改变当前 gain。

### Opus

libopus 接收固定 48 kHz stereo float frame。payload 上限来自 `MTU - 12-byte RTP header`，编码结果
直接进入 immutable `Bytes` frame。不要为“异步化”把编码搬到 Tokio worker，也不要在 producer 与
sender 之间增加另一个 channel。

## Opus queue

queue capacity 和水位都以媒体毫秒表示。blocking sender 满载时通过 condvar 等待，async receiver
通过 watch 观察 buffered duration 和 sender lifetime。最后一个 producer sender guard drop 后才
标记 source closed，确保 failure event 有机会先到 actor，而不是被 sender 误判为正常 drained。

迟滞恢复只在至少还有一个后继 frame 时丢弃最旧 frame；唯一可播 frame 保留。receiver drop 会清空
queue、关闭写端并唤醒所有 producer。

next producer 在 prime 水位发布 ready 后等待 promotion，而不是继续填满更大的 encoded
capacity。promotion 是身份转换：释放 blocking preload permit，提升共享 URL subscriber，
使后续 CPU lease 按 current 调度，并打开 promotion gate。supervisor 在上报失败时读取当前
身份，不使用 spawn 时的过期 role。

## RTP/RTCP

`rtp`、`rtcp` 和 `webrtc-util` 负责协议 marshal/parse。packetizer 将固定 RTP header 和 Opus payload
写入 sender 复用的 `BytesMut` scratch，UDP 直接发送该 buffer。

sender 在成功完整发送 datagram 后才更新 packet、byte、octet、sequence 和 timestamp 统计。UDP
不存在有意义的 partial datagram；若返回长度不一致即视为发送错误。RTP send 有 deadline，失败
终止当前 generation但不在 sender 内重试或 burst 补发。

RTCP SR 默认每五秒发送。mux 模式复用 RTP socket，非 mux 模式使用独立 socket。RR 解析形成 loss、
jitter 和 RTT snapshot，再进入 rolling quality window；质量事件只是宿主策略输入。

配置非 plaintext protection 但没有平台 protector 时 fail closed。当前实现绝不能静默降级明文，
也不能声称已经提供具体平台加密。

## 错误优先级

- source terminal error 优先于同时间发生的 decoder EOF/容器错误；
- generation cancellation 不是用户可见媒体错误；
- stale generation error 直接忽略；
- current error 触发 promotion/idle，next error 不影响 current；
- actor action 无法安全提交时关闭整个 runtime；
- callback failure与媒体动作隔离；
- shutdown 聚合有限数量的详细错误，同时继续清理其他 stream。

producer和sender都由外层 supervisor拥有；worker panic必须先变成 generation failure event。handle
drop/timeout同时中止worker和supervisor，不能留下 detached async task。

错误码必须稳定。新增错误场景优先复用语义准确的 code；只有宿主确实需要不同恢复策略时才增加
新 code。

## 观测

library 只调用 `metrics` 和 `tracing` facade，不安装全局 recorder/subscriber。宿主决定采集后端。
关键指标覆盖 source/cache、admission wait、activation-to-prebuffer、CPU turn、queue duration、
underrun、RTP lateness/drop、RTCP 和 stop/shutdown 收敛。

优化结论必须同时看首包、实时 deadline、资源等待和 event-loop delay，不能只看离线吞吐。

## 禁止模式

- Node 方法中 `block_on` 或同步等待 runtime。
- async task 中执行 blocking HTTP、codec 或同步文件清理。
- sender 中 decode、DSP、encode 或 playlist 决策。
- per-track sender、SSRC、sequence 或 timestamp。
- unbounded channel、buffer、tempfile、事件队列或 retry。
- 两层 Opus queue、跨 producer 共享可变 PCM 或 decoder。
- 用 cancel 模拟 pause，或把 partial response 与新 response 拼接。
- 以兼容为由保留第二套状态机、旧 runtime 或空 feature。

## 扩展准入

底层扩展必须回答：谁拥有状态、占用哪一级预算、等待时释放什么、如何取消、错误交给谁、是否
改变 RTP 时钟，以及如何验证。无法回答这些问题的优化不应进入媒体热路径。
