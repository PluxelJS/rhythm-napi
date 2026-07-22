# 首包延迟模型

首包延迟是从 current generation 被激活，到第一个 RTP packet 成功交给 UDP socket 的时间。它不应
等于整首歌曲下载时间。对可渐进解析的零起点 URL，HTTP 下载、decode/encode 和 RTP playout 是重叠
执行的流水线。

## 延迟由什么组成

```text
T_first_rtp
  = T_resource_admission
  + T_http_open
  + T_container_probe
  + T_decode_and_encode_prebuffer
  + T_sender_schedule_and_udp
```

- `T_resource_admission`：等待 tempfile、HTTP、blocking producer 和 CPU 额度；
- `T_http_open`：连接池命中，或 DNS/TCP/TLS/响应头；
- `T_container_probe`：获得足够字节让 Symphonia 识别容器、codec 和首个 packet；
- `T_decode_and_encode_prebuffer`：产生默认 100 ms Opus 媒体；
- `T_sender_schedule_and_udp`：sender 获得 prebuffer、设置首个 deadline并发送 marker packet。

总文件大小通常只影响后台下载完成和 cache promotion，不应出现在零起点首包关键路径中。

## 没下完为什么能播放

cache miss 的渐进 URL 使用一个同时可写、可读的 growing tempfile：

```text
reserve worst-case tempfile quota
  → acquire current-priority HTTP slot
  → open response and validate headers/status
  → create tempfile and publish GrowingSpool
       ├── HTTP task: body chunks → append to file → advance available_bytes
       └── codec task: independent fd from offset 0
             → probe as soon as enough bytes exist
             → decode → resample → DSP → OpusQueue
                   → 100 ms prebuffer → first RTP packet

HTTP task continues → response complete → artifact cache promotion
```

reader 追上 `available_bytes` 时在 condvar 等待。等待前释放 CPU lease，HTTP 写入新范围后再唤醒并
重新参与 CPU 调度。因此慢下载不占 codec 计算额度，codec 也不需要把 URL body复制到另一份长期
内存 bridge。

HTTP task 与 decoder 使用同一个 tempfile但不同文件描述符。每个 shared-flight subscriber 都从
offset 0 获得独立 reader；它们共享网络正文，不共享 decoder 或读游标。

## 当前何时启用渐进路径

当前实现只有同时满足以下条件才走 growing spool：

- source kind 是 `url`；
- generation 从 0 ms 开始；
- cache 中没有完整 artifact；
- 显式 `formatHint`或 URL path扩展名是 `aac`、`flac`、`mp3`、`oga`、`ogg`、`opus`、`wav`、
  `wave`、`m4a` 或 `mp4`。

扩展名判断会忽略 query和fragment；没有扩展名的签名URL可由宿主提供规范化 `formatHint`。
前八种格式直接发布 growing spool；M4A/MP4先用 O(1) 内存扫描顶层 BMFF box，只有在前 4 MiB内
确认完整 `ftyp`、完整 `moov` 且 `moov` 位于首个 `mdat` 前时才发布 reader。尾部 `moov`、损坏、
超限或未知布局继续下载完整 artifact，再使用 seekable file decoder。

非零 seek同样等待完整 artifact。任意 HTTP byte offset 不能直接代表音频时间，在没有容器索引和
Range response身份验证前，错误的“渐进 seek”比延迟更危险。

## 为什么不直接使用内存流

普通歌曲最终需要完整 artifact 来支持 seek 和 cache。如果 HTTP body先进入大内存 pipe，再复制到
tempfile，会同时保留两份数据路径，增加内存、复制和一致性状态。growing tempfile让渐进 decoder
和最终 artifact共享同一份字节，同时由磁盘 quota约束总量。

live 不形成完整 artifact，因此使用 bounded memory byte bridge；bounded URL 与 live 的存储模型
不能混为一套。

## Pause、共享下载与首包

单 subscriber pause会立即冻结自己的 decoder/encoder/sender。若还有其他 subscriber消费相同内容，
shared HTTP transfer继续；全部 subscriber暂停时才冻结网络 future。resume继续同一 response，暂停
时间不计入 active I/O timeout。

若 current加入一个正在等待资源的 preload flight，flight永久提升为 current priority。这样可以复用
已经选择的 response和partial spool，又不会让 current继续受 preload子额度阻塞。

## Cache 与 next 如何降低延迟

- cache hit跳过 HTTP、tempfile写入和完整性等待，直接打开 seekable decoder；
- shared-flight follower跳过重复 DNS/TLS/body传输，但仍拥有独立 decode/prebuffer成本；
- ready next已经完成 source、probe、decode 和 prime，promotion只把 queue receiver交给 sender，
  因而切歌关键路径不再包含下载和 codec冷启动；
- 进程级 reqwest client复用 DNS/TLS/HTTP连接，避免每个 producer建立新连接池。

低首包不仅依赖 current快，也依赖宿主提前提供准确 next。

## 调优边界

`prebufferMs` 是最直接的首包/抗抖动权衡。默认 100 ms意味着至少编码五个 20 ms frame；降低它会
更早开始发送，但增大 source或CPU短抖动造成 underrun的概率。`decodeBatchMs`影响 producer一次 CPU
turn做多少工作；过大可能增加其他 current等待，过小会增加调度开销。

扩大 `encodedCapacityMs`不会自动降低首包，反而允许积累更多媒体。提高 HTTP并发、CPU worker或
blocking producer只在对应 admission确实饱和时有效；盲目增大总量可能放大磁盘、内存和调度竞争。

`source.http.maxBytes`按最坏情况在开连接前预留tempfile quota，避免多个partial文件共同填满磁盘
后死锁。响应若提供可信范围内的 `Content-Length`，reservation会在响应头后立即缩减；未知长度仍
保留最坏值。因此过大的per-source上限仍会减少未知长度URL的并发启动，应按真实曲库设置。

## 当前仍值得优化的问题

### 1. 尾部 metadata 的 M4A/MP4

faststart M4A/MP4 已可安全渐进；`moov` 在尾部仍等待完整下载。只有实现经过容器索引验证、严格校验
响应身份的 Range reader 后才应优化该路径，不能把媒体时间直接换算成任意 byte offset。

### 2. 用阶段指标建立生产基线

现已记录HTTP open/first body byte、tempfile create、spool ready、source ready、codec start、decoder
open/probe、first Opus、prebuffer和first RTP packet。下一步不是继续加时间点，而是在真实provider
按cache miss/hit/shared follower/ready next统计p50/p95/p99，确认主导阶段。

### 3. 未知长度的最坏值磁盘预留

安全的全额预留牺牲了一部分高并发启动能力。若真实数据证明它是瓶颈，可以研究“保证最小可播放
窗口 + 可证明不会死锁的增量扩容”，或利用可信 Content-Length缩减 reservation；任何方案都必须
保持磁盘上限和 current优先，不能回到边写边无限增长。

### 4. 默认 prebuffer需要真实网络校准

100 ms是保守低延迟默认，不是所有部署的最优值。需要按 provider RTT、codec、CPU并发和接收端
jitter buffer测量 40/60/80/100 ms档位的首包与 underrun，再决定是否改变默认值或按 source类型配置。

### 5. 冷启动和 corpus仍需验证

首次 DNS/TLS、不同 codec probe字节量、超大 metadata和真实 VBR都会影响首包。短 WAV/MP3 localhost
测试证明流水线并行，不代表生产 provider的尾延迟。容量结论必须来自真实 corpus和并发分位数。

## 验收首延迟优化

任何改动都应报告至少：

- cache miss/cache hit/shared follower/ready next各自的首包 p50/p95/p99；
- admission、HTTP open/first byte、probe、first PCM、prebuffer和first UDP各阶段耗时；
- 同时发生的 underrun、sender lateness和drop；
- CPU、blocking、HTTP、tempfile等待；
- 峰值 tempfile/live memory和Node event-loop delay。

只有首包下降且没有破坏有界资源、错误完整性、pause/cancel或实时 pacing，才算有效优化。
