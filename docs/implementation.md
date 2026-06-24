# 实现契约

这份文档把 architecture/runtime/testing 中的关键约束收敛成实现时必须遵守的契约。实现者可以按这里写代码，遇到冲突时以本页的不变量优先，再回到对应专题文档补细节。

## 不变量

这些规则一旦破坏，通常会导致切歌漏旧帧、实时抖动、内存膨胀或未来大改：

- Rust 只拥有 `current_slot` 和 `next_slot`，不拥有 playlist。
- actor 是 stream 状态的唯一写入者；worker 只能发 `WorkerEvent`。
- seek、switchTrack、track switch 必须生成新的 generation。
- 所有进入队列的 frame 都带 generation。
- sender 只发送 active generation。
- RTP sender 不执行 source read、decode、normalize、resample、Opus encode。
- 所有音频数据队列 bounded，容量用毫秒表达。
- decode/encode 必须 ahead-of-time 维持水位，不等 sender tick 才工作。
- PCM scratch 只属于一个 active slot；active current/next 不共享 mutable buffer。
- Opus payload 入队后 immutable。
- source artifact 生命周期独立于 TrackSlot。
- Node/TS 解析业务 URL、鉴权、playlist 和播放模式；Rust 不猜业务含义。
- Node/TS 完成 voice gateway 鉴权和 endpoint/key 获取；Rust 只接收可校验的 RTP transport config。

## 模块边界

推荐 crate 内部边界：

```text
engine/       Engine registry, StreamHandle, public command routing
session/      StreamActor, state machine, playout slot orchestration
source/       File/HTTP/tempfile/live source and artifact cache
audio/decode/ DecoderBackend trait and backends
audio/dsp/    normalize, downmix, volume, limiter
audio/resample/
audio/opus/
transport/    RTP/RTCP session, sender, sinks
event/        public event and snapshot mapping
model/        shared command/status/error types
```

`music_stream_napi` 只做 Node ABI、类型映射、runtime lifecycle 和 ThreadsafeFunction。不要把播放状态机、source 策略或音频热路径放进 napi crate。

## 命令与事件

actor 串行处理 public command：

```text
public command -> StreamActor -> TaskAction
worker result  -> WorkerEvent  -> StreamActor -> TaskAction/PublicEvent/SnapshotPatch
```

命令必须是幂等或返回明确错误：

- `stop` 可重复调用，第一次进入 stopping，之后返回当前终态或 in-progress 状态。
- `pause` 在 paused 状态下不重复重建任务。
- `play` 在 playing 状态下只返回 snapshot。
- `setNext(null)` 只取消 next slot，不影响 current。
- `switchTrack` 由 TS 指定目标，Rust 不用 playlist 推导目标。

worker 不允许直接改 snapshot，不允许直接 publish public event。这样能把 race 收敛到 actor mailbox 和 generation 过滤。

## Worker loop 模式

current decode/encode worker 的目标是维持水位：

```text
loop:
  if cancellation requested: exit
  if encoded_queue_ms >= encoded_high_water: wait for drain or cancel
  if decoded_queue_ms >= decoded_high_water: wait for drain or cancel
  read/decode backend packets until one of:
    decoded_queue_ms reaches target
    worker turn reaches decode_batch_ms
    source needs more data
    cancellation requested
  normalize/resample/assemble/encode frames
  send immutable frames to encoded queue
  report progress through WorkerEvent
```

推荐水位：

```text
decode_batch_ms = 40..120
decoded_low_water_ms = 80..120
decoded_high_water_ms = 200..300
encoded_low_water_ms = 60..120
encoded_high_water_ms = 300..500
next_prime_ms = 100..300
pause_encoded_limit_ms = configurable, e.g. <= 2000
```

使用 low/high watermarks，避免 queue 在边界附近频繁唤醒。具体值必须经过 benchmark 和真实接收端调参，但实现接口一开始就按毫秒建模。

next priming 复用同一套 worker loop，但约束更强：

- 低优先级 permit。
- 到达 `next_prime_ms` 立即停止。
- current 水位低时让出 CPU。
- `setNext` 替换时可快速取消。

## RTP sender loop

sender 是实时节拍器：

```text
wait until active generation has prebuffer frames
base = monotonic clock now
media_index = 0
loop:
  deadline = base + media_index * frame_duration
  sleep_until(deadline)
  frame = pop active generation frame
  if no frame: enter underrun and rebase after next frame
  if stale generation: drop
  if frame too late: drop according to late policy and rebase if needed
  packetize with RTP session
  send
  media_index += 1
```

sender 禁止：

- 为了补空洞同步 decode。
- burst 发送 backlog 追进度。
- 重复上一帧伪造连续性。
- 在热循环写 info log。
- 持有跨 `await` 的 mutable packet scratch borrow。

## RTP transport config

连接参数必须集中在一个配置对象内，字段语义对齐旧 C++ `StreamInfo` 但命名更明确：

```text
remote_ip
remote_rtp_port
remote_rtcp_port optional
local_ip / local_rtp_port optional
payload_type
ssrc
mtu
rtcp_mux
opus_bitrate_bps optional
encryption config
```

配置层只做传输语义，不做鉴权。Node 可以传入网关返回的 port、SSRC、payload type、bitrate、rtcp mux 和 encryption mode/key。Rust 负责：

- 校验端口、payload type、MTU 和 bitrate 范围。
- 从配置派生 `RtpPacketizerConfig`。
- 从配置建立 UDP sink。
- 通过 `RtpPacketProtector` 在 RTP marshal 后、UDP send 前执行平台加密/保护。
- 非 `none` 加密模式在没有 packet protector 时返回明确错误，禁止假装加密或静默降级明文。

UDP sink 只发送已经 packetized 的 RTP bytes。加密如果启用，位置必须在 RTP marshal 之后、UDP send 之前，避免把加密细节泄漏到 decoder、encoder、slot 或 actor。

## RTCP feedback

RTCP wire format 必须继续由 `rtcp` crate 编解码。Rust runtime 只维护会话级反馈快照：

- Sender Report 按 interval 在 RTP session active 后发送，统计 packet count、octet count 和最后 RTP timestamp。
- Receiver Report 非阻塞读取，按本地 SSRC 过滤 report block。
- status 中保留 RR 原始字段：fraction lost、total lost、extended highest sequence、jitter、LSR、DLSR。
- status 同时给出当前 RR 的派生指标：`fractionLostRatio`、`fractionLostPercent`、`jitterMs`、可计算时的 `roundTripTimeMs`。

RTT 只在 RR 携带有效 LSR 时计算，公式遵循 RTCP compact NTP：`arrival - lsr - dlsr`。jitter 对 Opus RTP 固定按 48 kHz clock 转换为时间。metrics/runtime 层可以消费这些快照做窗口聚合，actor 可以基于当前 generation 发布质量等级变化事件；N-API `networkQualityChanged` 会携带 rolling window 的 loss/jitter/RTT 摘要，供 TS 策略层消费。transport parser 不能变成策略引擎。自动降码率或业务策略动作属于后续策略层。

## Clock 与 timestamp

内部必须区分三种时间：

```text
wall clock        Instant，用于调度 sleep/deadline
media clock       RTP timestamp/sample count，用于接收端 playout
track position    当前 track 内样本位置，用于 progress/seek
```

规则：

- pause 不推进 media clock。
- resume 重建 wall-clock base，但 RTP sequence/timestamp 继续单调。
- seek 改变 track position，不让 RTP timestamp 回退。
- switchTrack 首包 marker set，RTP timestamp 继续单调。
- underrun 后重设 wall-clock base，不 burst 历史帧。

## Source 与 artifact

Source 选择必须先判断 seekability 和容器需求：

```text
local file -> FileSource
bounded HTTP currently implemented -> TempFileSource
bounded small HTTP later may become MemorySource when it proves useful
seekable HTTP/container -> TempFileSource
live/non-seekable -> StreamingSource
container requires seek but source cannot seek -> spool to tempfile or reject
```

`TrackKind::Url` 在当前实现中表示有界 HTTP/HTTPS media，默认可 seek，因为 resolver 会先完成 tempfile artifact；真正无限流或不可 seek 输入必须使用 `TrackKind::Live` 或显式 `seekable=false`。

N-API 的 `startStream.source.http` 可以配置有界 HTTP source policy：

- `timeoutMs` 控制单次 HTTP resolve 超时，必须大于 0。
- `maxBytes` 控制下载上限，必须大于 0；Content-Length 或实际下载超过上限都会返回 `InvalidSource`。
- `cacheTempFiles` 控制是否启用进程内 artifact LRU cache；默认保持启用以复用预热/重复播放的完整 seekable tempfile，显式 `false` 时同一 track id 不会在 HTTP server 关闭后复用旧 artifact。
- 同一 stream 后续 `setNext`、`switchTrack`、`seek` 和 preload promotion 沿用 `startStream` 时的 source policy。

N-API 的 `startStream.source.liveHttp` 单独配置 live HTTP source policy，不影响有界 URL artifact：

- `timeoutMs` 控制 live HTTP 请求/读超时，必须大于 0。
- `maxBufferedBytes` 控制 live byte pipe 最大缓冲，必须大于 0。
- `readChunkBytes` 控制每次 HTTP body read 的 chunk 大小，必须大于 0 且不能超过 `maxBufferedBytes`。
- `maxRetries` 控制 408/429/5xx/timeout/连接类错误的基础 retry 次数，必须能放入 u8。
- `retryBackoffMs` 控制 retry backoff；当 `maxRetries > 0` 时必须大于 0。`401/403` 不重试，会映射为 `SOURCE_AUTH_EXPIRED` 并触发 `sourceRefreshNeeded`。

artifact cache 只缓存 source artifact，不缓存 TrackSlot：

- complete + seekable + within budget 才可进入 LRU。
- 当前实现已经有进程内 bounded LRU，用 `TrackSource::stable_key()` 命中，命中后复用同一个临时文件 artifact。
- 当前 HTTP tempfile 下载支持同一次 resolve 内的断线 Range resume：中断后按已落盘字节发送 `Range: bytes=N-`，只有服务端返回 `206 Partial Content` 才继续追加；如果服务端忽略 Range 或重试仍失败，则整个 partial artifact 删除并返回 `InvalidSource`。
- partial artifact 默认删除；当前不把半成品写入 artifact cache，也不保留持久 resume record。
- live stream 永不进入 artifact cache。
- cache write 是低优先级 IO，超过预算直接丢弃。

## 错误模型

错误必须有稳定 code，不能只把字符串传到 Node：

```text
INVALID_SOURCE
SOURCE_TIMEOUT
SOURCE_AUTH_EXPIRED
NOT_SEEKABLE
UNSUPPORTED_FORMAT
DECODE_ERROR
RESAMPLE_ERROR
ENCODE_ERROR
RTP_SEND_ERROR
STREAM_CLOSED
BUSY
INTERNAL
```

错误分层：

- source error：可 retry、触发 `sourceRefreshNeeded` 后由 TS 刷新 URL 并调用 `refreshCurrentSource`，或让 TS 切歌。
- track error：当前 track 失败，但 stream 不一定失败；next ready 时可 promote。
- stream error：连续 track error 超阈值、runtime 关闭、不可恢复内部错误。
- command error：当前状态下命令不合法或资源忙。

## 观测指标

必须从第一版实现就保留 metrics hook，否则后续性能调优会返工：

```text
decoded_queue_ms
encoded_queue_ms
decode_batch_ms_actual
decode_realtime_factor
resample_realtime_factor
opus_encode_p95_us
sender_drift_p95_us
underrun_count
late_drop_count
stale_generation_drop_count
next_prime_ms_ready
next_cpu_wait_ms
hot_path_allocations_per_frame in debug/bench
source_bytes_read
artifact_cache_bytes
```

高频路径只更新 counter/histogram，不直接发大量 public event。

## 音量、增益与响度

运行时音量控制分两层：

- `volume` 是用户音量，范围 0.0..1.0，使用感知曲线映射到 dB/linear gain。
- `gainDb` 是额外校准增益，范围 -60..+12 dB，适合 Node 或业务层传入 ReplayGain、用户预放大或频道修正结果。

PCM 进入 Opus 编码前统一应用最终增益；正增益路径带 soft limiter，避免简单 hard clip。DSP 层提供 `PcmLevelMeter` 用于 peak/RMS dBFS 和保守 headroom gain 计算。ReplayGain 当前采用显式推荐模型：Node/TS 提供 track/album gain、peak、preamp、防削波和 fallback policy，Rust `recommend_replay_gain` / N-API `recommendReplayGain` 返回推荐 `gainDb`、requested gain 以及 clipping/range 限制标记；调用方必须显式把推荐值传给 `startStream` 或 `setGain`。runtime 不会在没有命令的情况下自行改变播放增益。真正 EBU R128 自动分析、preload/offline scan 和运行时渐变仍属于后续策略层。

当前实现直接使用 `metrics` crate 作为 metrics facade，使用 `tracing` 在 source resolve 和 runtime worker/preload 边界发 span/event；library 默认不安装全局 recorder/subscriber，宿主负责接入 Prometheus/OpenTelemetry/StatsD/log exporter。runtime worker 已上报 current/preload worker turn 耗时、decoded/encoded frame counters、decoded/encoded queue ms gauge、RTP packet/byte/media counters、prebuffer/underrun/pacing waits、每 tick 最大 RTP pacing late ms、RTCP SR/RR counters，以及 RTCP quality window 的 loss/jitter/RTT 聚合 gauge；quality level 变化会通过 actor/N-API 发布带 snapshot 的 `networkQualityChanged`。source resolver 已上报 HTTP bytes、resolve timing、cache hit/miss/insert 和 resolve error counters。benchmark 统一走 Criterion，当前 `criterion_pipeline` 覆盖 fake pipeline worker + sender drain 热路径；后续需要 profiling 时再补具体 Criterion benchmark。

## 实现阶段与当前状态

推荐按风险排序实现：

1. 已完成：Model、error code、actor skeleton、fake event sink。
2. 已完成：Fake decoder/encoder + deterministic sender，证明 generation、seek、switch、prebuffer 和 next promotion。
3. 部分完成：RTP packetize + memory/UDP sink + RTCP Sender/Receiver Report，用 crate unmarshal 回读字段，并暴露最新 RR 的 loss/jitter/RTT 派生指标；metrics 层窗口聚合和带 snapshot 的网络质量等级变化事件已实现，自动降码率或策略动作尚未实现。
4. 已完成：Audio pipeline scalar path：decode trait、normalize、frame assembler、Opus wrapper。
5. 已完成：Rubato resample，并保持 decoder/resampler/pipeline 分层。
6. 部分完成：local file source、bounded HTTP tempfile source、同次 resolve 内 HTTP Range resume、基础 artifact LRU cache、live current source/runtime、live HTTP 基础 retry、断流事件语义和同 stream current 恢复入口已实现；`startStream` 与运行中 `switchTrack` 都可进入 live current；`refreshCurrentSource` 可在 TS 刷新 URL 后重启 current；更高阶 live 自动刷新/reconnect 策略、跨进程/跨 resolve 持久化 cache metadata 尚未实现。
7. 已完成当前阶段：napi binding 已接入本地文件、有界 HTTP URL 和 live current 的 start/pause/resume/seek/stop/status/volume/gain/setNext/switchTrack/refreshCurrentSource、ReplayGain 推荐、transport config 校验、`setEventCallback` 事件通知和 `drainEvents` 轮询补偿出口；live current 固定 non-seekable，live next/preload 显式 unsupported，`switchTrack` 可从 seekable current 切到 live current；N-API runtime orchestration 已通过 per-stream `StreamActorMailbox`、generation-scoped playback/preload/promotion registry 和 deterministic stop/shutdown 接入 Tokio actor/task lifecycle。后续 async UDP/timer/source IO 替换必须保持 actor/action/task-tree 边界。
8. 部分完成：source/runtime 已迁到 `metrics` facade，基础 source/cache/queue/RTP/RTCP counters 和 RTCP quality window metrics 已实现并有 `metrics::Recorder` 测试；真实 localhost RTP 短 integration 已覆盖 packet count、序列号和 timestamp 单调；Criterion benchmark 已覆盖 fake pipeline worker + sender drain 热路径。更多 codec corpus 和真实源规模调参数据等到有 profiling 需求后再补，不在项目内维护自定义 long-soak 观测基线。

每阶段都应保持可运行测试，不把 napi 或真实网络作为核心状态机的前置条件。

## 反模式

这些做法会显著增加返工风险：

- sender 缺帧时直接调用 decoder。
- 用 unbounded channel 临时解决卡顿。
- 把下载 buffer 直接挂在播放 task 生命周期上。
- 用一个全局 decoder/encoder/scratch 给多个 stream 或 slot 复用。
- seek 时在同一个 decoder 上共享 mutable seek 状态，并让旧 frame 继续流动。
- 用 playlist index 判断 next 是否同一首。
- 在 Rust 里解析业务缓存 URL、cookie 刷新或播放模式。
- 用日志替代 metrics。
- 先接 napi 再补核心测试。
- 为了兼容某个格式让 FFmpeg backend 接管状态机。
