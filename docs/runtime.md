# Runtime 与任务模型

本页描述目标 runtime 模型。当前代码已经实现确定性的 pipeline、slot runner、RTP packetize、RTCP Sender/Receiver Report、RTCP quality window metrics 和带 snapshot 的质量等级变化事件、UDP sink、纯 `RtpPacer`，以及可由 N-API `startStream` 启动的本地文件/有界 HTTP URL RTP worker。该 worker 已能解码本地 MP3/WAV 或先下载成临时文件的 HTTP URL 并按 pacing 推到 UDP，且支持 pause/resume/seek 不 burst 发送 backlog、next runtime 预载/promotion、运行中 `setNext`、运行中 `switchTrack` 和 N-API ThreadsafeFunction 事件回调。N-API runtime orchestration 已接入 per-stream Tokio actor/task lifecycle：public command、status query 和 worker event 都通过 bounded `StreamActorMailbox` 串行进入 actor，actor 输出 generation-scoped `TaskAction`，runtime controller 再执行 current/preload/promotion task action。当前 RTP media worker 仍保留为受控 blocking worker handle；后续替换为 async UDP/timer/source task 时必须保持这个 actor/action/task-tree 边界。当前完成度见 [status.md](status.md)。

## 分层

推流包含三类工作：

```text
IO-bound       HTTP/file/source load, UDP send, event dispatch
CPU-bound      decode, normalize, resample, Opus encode
Realtime-bound RTP pacing and RTCP schedule
```

Tokio 负责 actor、async IO、UDP、timer 和取消。CPU 工作运行在独立 blocking lane，并受 semaphore 控制。RTP sender 只运行 timer + UDP send，不执行解码/编码。

## Actor 模型

每个 stream 一个 `StreamActor`：

```rust
enum StreamCommand {
    Play,
    Pause,
    Stop,
    Seek { seconds: f64 },
    SetNext(Option<TrackSource>),
    SwitchTrack { current: TrackSource, next: Option<TrackSource> },
    SetVolume(f32),
    QueryStatus(oneshot::Sender<StreamStatus>),
}
```

命令串行处理。worker 通过 internal event 回到 actor，由 actor 统一更新 snapshot 和决定 task action。decoder、encoder、sender 不直接修改同一个状态对象。

`StreamActorMailbox` 用单个 bounded Tokio channel 承载 public command、status query 和 worker event，进入 `StreamActor` 前不分叉；channel 满时直接返回 `BUSY`，channel 关闭时返回 `STREAM_CLOSED`。N-API 已使用它作为每个 stream 的状态唯一入口，worker callback 也只回灌 `WorkerEvent` 到 mailbox，再由 actor 产出 `TaskAction`。runtime controller 执行 action 时只操作匹配 generation 的 playback、preload 或 promotion handle；preload promotion task 等待 core preload completion 通知，醒来后再校验 generation，避免 polling 或 stale promotion。

实现规则：

- public API handler 只负责校验参数、发送 command、等待 snapshot/result。
- actor 是 `StreamState`、slot 引用、generation、snapshot 的唯一写入者。
- worker 只持有完成工作所需的 token、queue handle、source/decoder/encoder owned state。
- worker 不能直接 emit public event；只能发 `WorkerEvent` 让 actor 决定。
- actor 内部不能执行 CPU-bound 工作；只做状态转换和 task orchestration。

## Channel 与背压

所有音频数据队列必须 bounded：

```text
DecodedPcmFrame channel: small bounded queue
EncodedOpusFrame channel: bounded by target latency
Command channel: bounded; full returns BUSY
Event channel: bounded; metrics 可丢，error/state 不丢
```

背压规则：

- encoded queue 满：encoder 阻塞。
- decoded queue 满：decoder 阻塞。
- pause：sender 不消费，上游自然阻塞到队列上限。
- next/preload：受更低 CPU permit 限制，不能影响 current playback。

队列应使用 high/low watermarks，而不是只用单个容量上限。worker 在高水位停止生产，在低水位或 drain notification 后恢复，减少频繁唤醒和小批量抖动。

## Chunk 与 frame 尺寸

RTP/Opus frame 是最终节拍，其他 chunk 都应围绕它服务：

```text
opus frame default: 20ms = 960 samples/channel @ 48k
optional frame: 40ms = 1920 samples/channel @ 48k
decoder turn: backend natural packets batched up to a small time budget
decoder chunk: backend natural size, immediately normalized
resampler chunk: sized by rubato preferred input/output frames
sender tick: exactly one Opus frame duration
```

不要把 decoder chunk 直接等同于发送 frame。decoder 输出大小由 codec 决定，FrameAssembler 负责聚合/切分成 Opus frame。

解码/编码是 ahead-of-time 工作：worker 应在 sender 需要之前准备好一段未来音频。每次 worker turn 可以连续读取/解码若干 backend packet，直到达到 `decode_batch_ms`、队列水位、取消点或 source 暂时缺数据。这样能减少 per-frame 调用开销、改善热 buffer 的 CPU cache 局部性，并避免 RTP sender 线程承担突发 CPU 工作。

队列容量按毫秒计算：

```text
decode_batch_ms = 40..120
decoded_low_water_ms = 80..120
decoded_queue_ms = 100..300
encoded_low_water_ms = 60..120
encoded_queue_ms = 80..500
pause_encoded_limit_ms = configurable
next_prime_ms = 100..300
```

实现中可以换算成 frame 数，但配置和指标用时间表达，更符合播放体验。

## Buffer 生命周期

每个 current/next slot 拥有自己的热路径 scratch。跨 slot 复用只能发生在 slot 结束后通过 pool 归还，不能在 active slot 间共享同一个 mutable buffer。

```text
TrackSlotBuffers
  decode_scratch
  stereo_planar
  resample_in/out
  frame_assembler_tail
  opus_input_frame
  opus_output
  rtp_packet_scratch
```

所有进入 channel 的数据必须是独占或不可变：

- PCM frame 进入 decoded queue 后不再修改。
- Opus payload 进入 encoded queue 后是 immutable `Bytes`。
- RTP packet scratch 只在 sender 内复用，不跨 await 保留 mutable borrow。

这个约束比复杂 buffer pool 更重要。先保证所有权清晰，再用 profiling 决定是否池化。

## CPU 调度

CPU work lane 使用 semaphore 表达优先级：

```text
P0 stop/cancel
P1 RTP timer and UDP send
P2 current decode/encode/resample
P3 current source load
P4 next slot resolve/load/probe
P5 next slot priming
P6 metrics
```

Rust/Tokio 没有内置任务优先级，因此通过独立 semaphore、bounded queue 和 actor 调度表达。实现可以使用 `spawn_blocking` 或 dedicated CPU pool，但接口必须保持可替换。

CPU-bound worker 一次处理的工作单元不应太大：

- decoder 每次处理一个 packet 或小批 packet，并受 `decode_batch_ms` 和队列水位限制，然后检查取消。
- resampler 每个 chunk 后检查取消。
- Opus encoder 每个 frame 后检查取消。
- next slot priming 到达 `next_prime_ms` 后停止，不继续抢 CPU。

这样 seek/switchTrack/stop 能快速生效。

current playback 的 CPU worker 目标是维持 `decoded_queue_ms` / `encoded_queue_ms` 水位，而不是等 sender 请求才工作。next slot priming 也使用同一套批处理逻辑，但必须低优先级、短窗口、可随时取消。当前实现用轻量进程内 CPU permit lane 限制 worker turn 并发：current 按可用并行度限流，preload 保留更保守的 permit。CPU permit 获取是非阻塞的；拿不到 permit 时记录 busy 并让出本轮 tick，不阻塞 RTP/RTCP 实时 IO。

不要用 Tokio task priority 的假设表达实时优先级。真实优先级来自：

- bounded queue 形成背压。
- current worker 和 next worker 使用不同 permit。
- sender 不进入 CPU-bound lane。
- actor 可以取消低优先级 next priming。
- worker turn 有 `decode_batch_ms` 上限。
- playback ready 后 runtime tick 先执行 RTP/RTCP IO，再 opportunistic 做 CPU worker turn；prebuffer 前保持 worker-first 以维持 ready 事件语义。

当前受控 blocking RTP worker 的 idle、paused、pacer wait 和 preload idle 等待会被 stop、pause/resume、volume/gain 控制唤醒，减少固定 sleep 轮询造成的控制延迟。worker 内部已经拆成控制同步、RTP/RTCP IO、CPU worker turn 和 wait decision。完整 async UDP/timer/source IO 只有在 profiling 证明必要时才应作为局部内部替换来做，而不是改变 actor/action/generation 边界。

## 取消模型

使用 `CancellationToken` 树：

```text
stream_token
  -> current_track_token
      -> decoder_token
      -> encoder_token
      -> sender_token
  -> next_track_token
```

命令语义：

- stop：取消 stream token。
- switchTrack：取消 current track token，递增 generation，使用 TS 给定的目标曲目。
- seek：取消 current decoder/encoder，保留 track identity，递增 generation。
- setNext：取消/重建 next track token，不影响 current generation。

worker 必须在 source chunk、decode packet、resample chunk、encode frame、sender sleep 前后检查取消。

## next slot 资源策略

next slot 替换时要区分“播放实例”和“下载 artifact”：

```text
TrackSlot cancellation:
  cancel decode/encode/prime task
  close frame queues
  invalidate generation

SourceArtifact handling:
  complete + seekable + cacheable -> optional bounded LRU cache
  partial + resumable during same resolve -> range retry, then complete or delete
  partial or live -> delete/drop
```

artifact cache 必须受全局限制：

- max total bytes。
- max item bytes。
- TTL。
- stream stop cleanup。
- explicit cache key from TS。

预载不能因为缓存策略而影响 current playback。cache 写入是低优先级 IO，超过预算直接丢弃。

## 无限流任务

Live/无限 stream 的运行模型不同于普通 track：

```text
duration unknown
EOF means stream ended, not normal track boundary
seek unsupported
preload next is TS-driven, not remaining-time-driven
source reader obeys backpressure
retry policy is source-level, not playlist-level
```

Live source 断流处理：

1. 短暂网络错误：按 source policy retry。
2. 鉴权失败：发送稳定 `error` 和 `sourceRefreshNeeded`，由 TS 刷新 URL。
3. 运行中 current 失败且没有 next：发送 `error`，stream 进入 `idle` 并发 `nextNeeded`。
4. TS 拿到新 live endpoint 后可调用 `refreshCurrentSource`，在同一 stream/transport/source policy 下递增 generation 并重启 current。
5. 如果 next slot ready，actor 可 promote；Rust 不自行按 playlist 跳。

## RTP sender pacing

RTP sender 是实时节拍器。

```text
wait prebuffer frames for active generation
base = Instant::now()
for each frame:
  deadline = base + media_frame_index * frame_duration
  sleep_until(deadline)
  send packet through RTP library and UDP socket
```

underrun：

- deadline 到但没有 frame，进入 underrun。
- 等待新 frame 后重设 base。
- 不重复上一帧，不伪造静音，除非接收平台明确要求。

late frame：

- 小幅迟到立即发送。
- 大幅迟到丢弃过期 frame，重设 base，记录指标。
- 不 burst 发送历史 backlog。

generation：

- 旧 generation frame 一律丢弃。
- 新 generation 只能由 actor command 激活。

## Snapshot 与事件

Snapshot 由 actor 统一生成：

```text
stream state
current track id
generation
next slot state
played ms
total ms
source kind
seekable
cache artifact state
queue lengths
last error
metrics counters
scratch reuse stats in debug/bench builds
allocation counters in debug/bench builds
```

事件通过 ThreadsafeFunction 回到 Node。事件是外部观察，不参与 Rust 内部控制。

推荐事件：

```text
streamStarted
streamStopped
stateChanged
trackPreparing
trackReady
trackStarted
trackFinished
trackSwitched
nextNeeded
sourceRefreshNeeded
nextPreparing
nextReady
error
metrics
```

## Runtime shutdown

当前可复用的 shutdown 原语是 `RuntimeTaskGroup`：调用 shutdown 时先 cancel token，等待 cooperative task 结束；超过 timeout 后 abort 剩余 task，并汇总 completed、failed、panicked、aborted 和 timed_out。`GenerationTaskSlot` 负责保存带 generation 的 active handle，避免 seek/switch 后 stale cancel、pause/resume、set volume 或 set gain 操作到新任务。`StreamActorMailbox` 使用同一 lifecycle 原语管理 actor task；N-API runtime controller 在 stop/shutdown 时会 stop/join current playback 和 preload，abort tracked promotion task，然后 deterministic shutdown actor mailbox。stop 后只保留 inactive status，不保留 actor task。

`Streamer` drop：

1. 标记 engine closing。
2. 拒绝新 stream。
3. 给所有 stream 发送 stop。
4. 等待 stream task 结束。
5. 超时后 abort。
6. 释放 ThreadsafeFunction。
7. shutdown runtime。

这个顺序必须有测试覆盖，避免 Node 进程退出时 Rust 后台任务悬挂。
