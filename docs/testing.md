# 可测试性设计

本页是目标测试矩阵和扩展计划，不表示所有条目都已经实现。当前已落地的测试范围见 [status.md](status.md) 和 `cargo test --workspace` 输出；新增能力必须同步补进这里或状态页。

## 原则

核心逻辑必须能脱离 Node 和真实网络测试。工程拆成：

```text
music_stream       播放器、音频管线、RTP packetize/transport、任务模型、测试
music_stream_napi  napi 类型映射、runtime 持有、ThreadsafeFunction
```

## 关键测试对象

### TS/Rust playout boundary

纯同步测试：

- Rust 不维护 playlist/order/play mode。
- `setNext` 只影响 next slot，不影响 current。
- `switchTrack` 按 TS 指定目标原子切换 current。
- current 结束且 next ready 时自动 promote。
- current 结束且 next 缺失时发出 `nextNeeded` 并等待 TS。

### TrackSlot state machine

输入：

```text
Command
WorkerEvent
NextSlotEvent
```

输出：

```text
StateTransition
TaskAction
PublicEvent
SnapshotPatch
```

seek、switchTrack、stop、next promotion 都应不依赖真实 decoder 测试。

### FrameAssembler

property test：

```text
任意 PCM chunk 分割
frame_size = 960 or 1920 samples/channel
outputs = floor(total / frame_size)
remaining = total % frame_size
concat(outputs + remaining) == original
```

### Normalizer/Volume

覆盖：

- mono -> stereo。
- multi-channel downmix。
- int/f32 sample conversion。
- NaN/inf clamp。
- user volume 0、0.5、1 映射到预期 dB/gain。
- expert gain dB 正确转换为线性 gain。
- mute 快路径。
- limiter 不溢出。

### Resampler

覆盖：

- 48k stereo bypass 不创建额外输出。
- 44.1k -> 48k 输出帧数在容差内。
- chunk 边界变化时连续性正确。
- `process_into_buffer` 使用预分配 buffer，无热路径分配。
- source sample rate 变化时重建 resampler。

### Buffer reuse

覆盖：

- Opus encode 循环不为每帧分配 PCM buffer。
- RTP sender 复用 packet scratch，进入队列/发送的 payload 不被后续写覆盖。
- next slot 和 current slot 没有共享 mutable scratch。
- switchTrack 后旧 generation buffer 不再被 sender 使用。

### Ahead-of-time pipeline

使用 fake decoder/encoder 和可控 queue：

- sender 启动前必须等待 prebuffer 达标。
- decoder worker 在 sender 消费前填充到目标水位。
- encoded queue 高水位后 encoder 停止生产。
- encoded queue 回到低水位后 encoder 恢复生产。
- `decode_batch_ms` 限制每次 worker turn 的最大工作量。
- source 暂时缺数据时 worker 不 busy loop。
- bounded streaming fake decoder 满载时 producer 得到 `BUSY`，空但未结束时 pipeline 得到 `NeedMore`，finish 后 drain 到 `End`。
- next priming 达到 `next_prime_ms` 后停止。
- current 水位低时 next priming 让出 CPU permit。

### Streaming source primitives

Source 层已有 bounded byte pipe 测试：

- `try_push` 在 byte budget 满时返回 `BUSY`。
- `push_blocking` 会等待读端消费并释放容量。
- reader 在无数据且未 finish 时阻塞等待 writer。
- writer `finish` 映射 reader EOF。
- writer `fail` 映射 reader error。
- close 会唤醒读写两端，避免 live source shutdown 卡住。
- `SymphoniaStreamDecoder` 可从 `StreamingByteReader` 这种 non-seekable reader 解码，测试覆盖 byte pipe -> Symphonia `ReadOnlySource`/`MediaSourceStream` -> decoded chunk。
- `HttpLiveStream` 可把真实 localhost HTTP body 写入 bounded byte pipe，测试覆盖 chunk 读取、pipe 满时 stop 解除背压阻塞、HTTP 403 映射 `SOURCE_AUTH_EXPIRED`、HTTP 503 retry 后继续喂给同一个 reader。
- `LiveStreamSlot` 可把 finite live HTTP WAV body 接入现有 slot/pipeline，测试覆盖 `CurrentPrebufferReady`、sender drain 和 `CurrentEnded`。

### RTP/RTCP session

不测试 crate 的 wire format 本身，测试项目给 crate 的字段和 session 生命周期：

- payload type。
- marker。
- sequence wrapping。
- timestamp step。
- SSRC stability。
- RTCP SR packet count/octet count。
- generation stale frame drop。

用 crate 的 unmarshal 回读已生成 packet，确保字段一致。

### RtpSender

使用虚拟时间和 memory socket：

- prebuffer 未满不发。
- prebuffer 满后按 deadline 发。
- pause 后不发且 timestamp 不推进。
- resume 后继续 sequence/timestamp。
- underrun 后重设 base。
- late frame drop。
- switch generation 后首包 marker。
- sender 缺帧时不调用 decoder/encoder。
- backlog 大量积压时不 burst 追发历史帧。
- stale generation frame 即使排在队首也会丢弃。

### Local-file runtime worker

已落地的真实 IO 测试：

- 本地 MP3 fixture 经 Symphonia 解码、Rubato 重采样、libopus 编码、RTP packetize 后发送到 localhost UDP socket。
- 本地 WAV 短 integration test 会运行真实 paced worker，接收实际 RTP datagram，并验证 packet count、sequence number 和 RTP timestamp 按 Opus 20ms frame 单调推进。
- 有界 HTTP URL 会先下载到受生命周期保护的临时文件，再复用同一套 Symphonia/Rubato/libopus/RTP slot runner；测试覆盖外层 artifact drop 后 driver 仍持有临时文件，runner drop 后临时文件被删除。
- Source 层覆盖 HTTP artifact cache 命中、LRU 超预算驱逐和临时文件随 cache 生命周期清理；N-API smoke 会在 HTTP server 关闭后用同一 track id 再次推流，验证实际 runtime 路径命中 cache。
- runtime worker 使用 `RtpPacer` 和 `drain_due_packets`，不会把 encoded backlog 一次性 burst 出去。
- `pause` 后已发送媒体时长不增长，`resume` 后按 pacing 继续推进。
- `seek` 可取消旧 current generation，用 Symphonia 从目标时间附近重新打开本地文件 decoder，并让状态进度从 seek 目标继续累加。
- live current runtime 可从 localhost HTTP body 通过 bounded byte pipe、non-seekable Symphonia stream decoder、Rubato、libopus 和 RTP pacer 发送到 localhost UDP；测试覆盖 `CurrentPrebufferReady`、`CurrentEnded`、SSRC/payload type/timestamp、stop 唤醒 live source，以及 HTTP 503 retry 后仍能进入 RTP playback。
- runtime 可周期性发送 RTCP Sender Report；mux 模式走 RTP socket，SR packet count/octet count 使用已发送 RTP 统计。
- runtime 可接收并解析 RTCP Receiver Report，按本地 SSRC 过滤 report block，并通过 status 暴露最新反馈和当前 RR 的 jitter/RTT 等派生指标；RTCP quality window 有独立单元测试，runtime 收到 RR 后会把窗口内 loss/jitter/RTT 聚合 gauge 写入 `metrics` recorder，并在 quality level 变化时经 actor/N-API 产出带 snapshot 的 `networkQualityChanged`。
- start-time next preload 可提前解码到 `NextReady`，promotion 后复用已预热 driver 直接 RTP/UDP 推流。
- 运行中 `setNext` 可取消旧 preload、启动新 preload，并在 current 结束后按 actor action promotion。
- 运行中 `switchTrack` 可取消旧 current/next，按 TS 指定的新 current/next 重建 runtime，并隔离旧 generation。
- worker event 回到 actor 后更新状态，完成时报告 `CurrentEnded`。
- `setVolume` 可下发到正在运行的 playback handle，对后续 PCM frame 生效。

### N-API smoke

`crates/music_stream_napi` 的 `npm test` 会先 build native addon，再执行 `tsc --noEmit` 和 Vitest。N-API smoke 使用 TypeScript 测试文件，直接消费 addon 生成的 `index.d.ts`，测试文件按边界分组：

- `rtp-feedback.test.ts`：本地文件 RTP、RTCP Receiver Report、quality event 和 callback/drainEvents。
- `source-policy.test.ts`：有界 HTTP URL、artifact cache、source policy、URL 鉴权过期和 next preload 鉴权失败。
- `live-stream.test.ts`：live current、non-seekable、live next/preload unsupported、live startup retry/auth failure。
- `playback-controls.test.ts`：next promotion、volume/gain、pause/resume/seek、file/live `switchTrack`、stop/reuse 和 shutdown。
- `loudness.test.ts`：显式 ReplayGain 推荐、track/album fallback、防削波限制和缺失 metadata 错误。

这些 smoke 动态生成短 WAV 文件，避免依赖外部 fixture：

- 绑定 localhost UDP socket，调用 `Streamer.startStream` 启动本地文件或有界 HTTP URL RTP 推流。
- 使用 worker thread 中的本地 HTTP server 验证 `kind: "url"` 的有界 HTTP source 能通过同步 N-API 调用完成下载并推 RTP，且同一 track id 可在 server 关闭后从 artifact cache 再次推流。
- 使用 N-API smoke 验证 `source.http.timeoutMs/maxBytes/cacheTempFiles` 和 `source.liveHttp.timeoutMs/maxBufferedBytes/readChunkBytes/maxRetries/retryBackoffMs` 配置校验，覆盖 maxBytes 拒绝、关闭 cache 后不从旧 artifact 重启。
- 使用 N-API smoke 验证 HTTP 403 会映射为 `SOURCE_AUTH_EXPIRED` 并通过 `drainEvents` 暴露 `sourceRefreshNeeded`。
- 使用 N-API smoke 验证 next 预载同步鉴权失败会回灌 actor worker event，暴露 `sourceRefreshNeeded`，但不会中断已经成功启动的 current playback。
- 使用 N-API smoke 验证 `startStream.next` 成功预载后会在 current 结束时 promotion，并且 promotion 后继续发送 RTP。
- 使用核心 crate HTTP source 单元测试覆盖中断后 `Range: bytes=N-` 续传成功，以及服务端忽略 Range 时拒绝 partial artifact。
- 使用 N-API smoke 验证 `kind: "live"` current 可从本地 HTTP server 推 RTP，输入即使传 `seekable=true` 也会归一化为 non-seekable，`seekStream` 返回稳定 not seekable 错误。
- 使用 N-API smoke 验证运行中 `switchTrack` 可从 seekable file current 切到 live current，继续通过同一 RTP transport 发送，并保持 live non-seekable。
- 使用 N-API smoke 验证 live next/preload 显式返回 unsupported error，不进入 artifact cache 或 next promotion。
- 使用 N-API smoke 验证 live startup 503 在基础 retry 后同步拒绝 `startStream` 且不泄漏 stream；live startup 403 同步拒绝、排队 `sourceRefreshNeeded`，并且同样不泄漏 stream。
- 使用 N-API smoke 验证 finite live current 结束后，`refreshCurrentSource` 可在同一 stream 上用新 live URL 重启 current，并继续通过原 RTP transport 发包。
- 接收实际 RTP datagram，并用发送端地址回送 RTCP Receiver Report。
- 轮询 `getStatus`，确认 `receiverReport` 暴露原始 RR 字段和 `jitterMs` 等派生指标。
- 注册 `setEventCallback`，确认 RR 驱动的 `networkQualityChanged` 事件可到达 JS，且携带 `good`、`degraded` 或 `poor` 等级，以及 quality window 的样本数、loss、jitter 和 RTT 摘要。
- 注册 `setEventCallback`，确认 actor event 会低延迟回调到 JS，同时 `drainEvents` 仍保留可靠补偿队列语义。
- 使用 N-API smoke 验证 `recommendReplayGain` 只返回推荐 `gainDb` 和限制标记，不隐式修改 stream 状态；调用方仍需显式传给 `startStream` 或 `setGain`。
- 通过 N-API 调用 `setVolume`、`setGain`、`pauseStream`、`resumeStream`、`seekStream` 和 `switchTrack`，确认状态、runtime 进度、file current 切换和 live current 切换后的 RTP 发送符合预期。
- 使用 N-API smoke 验证 `stopStream` 会关闭 current/preload/promotion/actor lifecycle，仍保留 stopped status 供幂等 stop/status 查询，并允许同一 `streamId` 重新 start。
- 调用 `Streamer.shutdown()`，确认 active runtime 被停止、stream registry 被清空，并且同一 `streamId` 可以重新启动。

这些测试只覆盖 Node ABI、类型映射、native lifecycle 和真实 UDP wiring；状态机、pacing、音频热路径和错误分支仍以 Rust 单元/集成测试为准。避免为了覆盖数扩张 N-API smoke；能在 Rust fake/内存层稳定验证的行为不要重复搬到 JS。

### StreamActor

使用 fake decoder、fake encoder、fake source、memory RTP sink：

- start -> buffering -> playing。
- pause/play。
- switchTrack cancels old generation。
- seek creates new generation。
- setNext cancels/replaces next slot only。
- stop cancels current and next slot。
- track error promotes ready next slot or emits `nextNeeded`；current source failure without next emits `error`, clears current, enters `idle`, then emits `nextNeeded`。
- consecutive error threshold stops stream。

### StreamActorMailbox / runtime lifecycle

已落地的 Tokio runtime lifecycle 测试：

- `RuntimeTaskGroup` cooperative shutdown 会 cancel 并 join task。
- task failure 会在 shutdown report 中保留稳定 error code，且不吞掉已成功 task。
- uncooperative task 超时后会被 abort，并标记 `timed_out`。
- `GenerationTaskSlot` 只会按匹配 generation 取出 active task，替换时返回被替换旧 task，避免 stale action 误操作新 handle。
- `StreamActorMailbox` 使用 bounded command/event queue；队列满时 public command 立即返回 `BUSY`。
- public command、status query 和 worker event 在同一个 actor task 内串行处理。
- worker event 仍走 `StreamActor` 既有 generation 过滤，mailbox 不复制状态策略。
- mailbox shutdown 使用同一个 lifecycle 原语 deterministic cancel/join。
- N-API runtime controller 使用 per-stream mailbox registry，不再直接写 `Engine`；worker callback 只发送 `WorkerEvent` 到 mailbox，actor 输出 `TaskAction` 后再执行 runtime action。
- playback、preload 和 preload promotion 都按 generation-scoped registry 管理；stop/switch/seek 只能取消匹配 generation 的 handle。
- stop/shutdown 会 stop/join playback/preload、abort tracked promotion task、shutdown actor mailbox；stop 后只保留 inactive status。

## Fake 组件

```rust
struct FakeDecoder {
    chunks: Vec<AudioChunk>,
    seekable: bool,
    fail_at: Option<usize>,
}

struct FakeEncoder {
    frame_size: usize,
}

struct MemoryRtpSink {
    packets: Arc<Mutex<Vec<Bytes>>>,
}
```

Fake payload 中写入 generation、sequence、first_sample，方便断言 seek/switchTrack 后旧帧不再发送。

## 必须覆盖的行为测试

### 防旧帧

```text
start track A
send 5 frames
switchTrack to track B
advance virtual time
assert no RTP payload from generation A after switchTrack resolved
```

### 暂停

```text
pause
advance virtual time 10s
assert sent packet count unchanged
resume
advance one frame duration
assert packet count increments by one
```

### 预载隔离

```text
setNext B while A playing
assert B never sends before actor promotes B
```

### next 替换与 artifact

```text
setNext B
B downloads to complete tempfile
setNext C
assert B slot canceled
assert B artifact retained only if cacheable and within budget
assert C slot starts
```

```text
setNext live stream B
setNext C
assert B network reader canceled
assert no live artifact retained
```

### 无限 MP3/live stream

```text
start live MP3 stream
assert timeTotalMs is null
seek returns NOT_SEEKABLE
pause stops RTP send and source obeys backpressure
retryable HTTP/source interruption retries before stable error/nextNeeded events
refreshCurrentSource restarts current after TS refreshes live URL
```

### 停止

```text
stop stream
assert source/decoder/encoder/sender tasks joined
assert UDP sink closed
assert no callback after final streamStopped
```

## Fixture corpus

使用小体积、可再生成音频：

```text
tone-1s.mp3
tone-1s.flac
tone-1s.m4a
tone-1s.wav
corrupt.mp3
```

单元测试不依赖真实音频；集成测试使用 fixture；兼容性测试用更大 corpus 手动或 nightly 跑。

## Benchmark

当前保留一个 Criterion benchmark 入口：

```sh
cargo bench -p music_stream --bench criterion_pipeline
```

`criterion_pipeline` 使用 Criterion 跑主流统计型 microbenchmark，目前覆盖 fake decoder/encoder 的 pipeline worker + sender drain 热路径。Criterion 负责采样、统计、基线比较和本地报告。

后续可继续补更多 Criterion benchmark：

- decode additional common formats。
- resample parameter sweep。
- standalone Opus encode 20ms。
- RTP packet session build。
- sender memory sink 60s。
- next replacement cache cleanup and larger production-sized cache eviction sweeps。
- live source backpressure.
- hot path allocations per Opus frame。
- gain/downmix/interleave scalar vs optional SIMD。
- rubato `process_into_buffer` chunk size sensitivity。

指标：

- realtime factor。
- allocations per frame in real codec/source paths。
- p95/p99 encode time。
- sender scheduling drift。
- next slot CPU wait time。
- resampler realtime factor by input rate.
- high/low watermark wakeup frequency。
- cancellation latency for decode worker turn。
- source backpressure memory ceiling。

## Contract 测试矩阵

每个核心不变量都要能落到测试：

```text
actor single writer        -> worker cannot mutate snapshot/state directly
generation isolation       -> stale frame never sent after seek/switch
bounded queues             -> producer blocks or yields at high water
ahead-of-time ready frames  -> sender waits for prebuffer and never decodes
slot isolation             -> current/next scratch pointers never alias
artifact separation        -> canceled next slot can retain source artifact only by policy
clock separation           -> pause freezes media clock, resume rebuilds wall-clock base
stable error codes         -> each failure path maps to public error code
metrics presence           -> queue/RTP/RTCP counters observable; performance work uses Criterion or external profiling
```

## Race 测试

重点 race：

- stop 与 seek 同时发生。
- switchTrack 与 decoder EOF 同时发生。
- setNext 与 nextReady 同时发生。
- pause 与 underrun 同时发生。
- Node drop 与 event callback 同时发生。

设计上通过 actor 串行命令和 generation 过滤降低 race 面，而不是靠多把锁协作。
