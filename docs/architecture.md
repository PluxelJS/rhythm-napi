# 架构

## 系统边界

TypeScript 负责 playlist、播放模式、业务 URL 解析、鉴权刷新、网关协商和产品策略。Rust 只接收确定的 current/next source 和 RTP transport 配置。

```text
TypeScript
  └── Streamer Promise API
        └── StreamRuntime
              ├── StreamActor
              ├── current Opus producer
              ├── next Opus producer
              ├── source artifact cache
              └── persistent RTP/RTCP sender
```

## StreamRuntime

每个 stream 一个 `StreamRuntime`。它串行执行 command 和 worker event，`StreamActor` 是播放状态、current/next 和 generation 的唯一写入者。

同一个 Streamer 的 runtime共享 `RuntimeResources`：CPU并行额度、blocking producer总量、
preload子额度、current优先的bounded HTTP下载、live连接并发、live bridge总字节和 artifact cache都由这里统一拥有。
URL spool与 cache还共享进程级 tempfile字节预算；cache和 active attempt各有明确容量余量。
每流上限不能替代进程级上限。

runtime先在 actor副本上生成 `TaskAction`，action全部生效后才提交actor并发布事件。替换操作先
建立新 producer/receiver再覆盖旧槽；无法回滚的 action失败统一停止整个runtime。它不会为
每首歌重建 RTP sender。

公开播放状态只有 `Idle / Buffering / Playing / Paused / Stopped`；不暴露实现从未进入的过渡态。事件 callback 在 action 生效后调用，不能改变 actor 或 sender 的执行顺序。

## Producer

producer 是一次 generation-scoped 播放实例：

```text
TrackSource
  → async source resolve/live read
  → Symphonia decode
  → channel normalize/downmix
  → Rubato 48 kHz resample
  → volume/gain/limiter
  → libopus 20 ms frame
  → duration-bounded OpusQueue
```

本地文件直接形成 seekable artifact。零起点有界 URL写入可多读growing tempfile，current/next
无需等待完整下载即可解码；同内容并发miss订阅registry-owned flight并从offset 0各自读取，
响应完整结束后才把 tempfile提升为seekable cache artifact。cache hit和非零seek仍使用文件
decoder。live source使用独立的全局连接与streaming byte预算，但不写tempfile。

live reader 暴露精确的 blocking-wait 边界：等待网络字节时释放 CPU lease，返回 decoder 计算时重新获取。source 层不知道调度策略，只报告 wait 前后时机。

producer 和 sender 共享唯一的 `OpusQueue`：blocking producer 在媒体时长达到 `encoded_capacity_ms` 时等待，async sender 通过 watch 通知被唤醒。sender 不再把 frame 搬入第二个内部队列，因此暂停、慢网络和预载都不能解除背压或无限提前编码。

next producer 在 queue 达到 `next_prime_ms` 后报告 ready。promotion 只把 receiver 转交给 sender，已经编码的 Opus frame 不丢失，也不重新解码。

## RTP session

每个 stream 的 sender 从创建到 stop 始终存活并独占：

- UDP socket；
- SSRC、payload type 和 MTU；
- RTP sequence 和 timestamp；
- pacing deadline；
- RTCP SR/RR 状态和质量窗口；
- active generation 的 Opus receiver。

seek、switch 和 promotion 只替换 active receiver。首包设置 marker，sequence/timestamp 继续单调递增。
sender迟滞超过配置上限时丢弃已经过时且存在后继的 Opus frame；timestamp跳过相应媒体采样，
sequence仍只统计真正发送的 packet，因此接收端时钟与丢包语义均保持正确。

## N-API

所有可能等待 actor、socket 或清理任务的方法都返回 Promise：

```ts
startStream(...): Promise<StreamStatus>
getStatus(...): Promise<StreamStatus>
setNext(...): Promise<StreamStatus>
switchTrack(...): Promise<StreamStatus>
seekStream(...): Promise<StreamStatus>
pauseStream(...): Promise<StreamStatus>
resumeStream(...): Promise<StreamStatus>
stopStream(...): Promise<StreamStatus>
shutdown(): Promise<void>
```

同步方法仅限纯配置校验、ReplayGain 计算、事件 callback 注册和补偿队列 drain。

## 不变量

- Rust 只拥有 current 和 next，不拥有 playlist。
- seek/switch/promotion/refresh 必须校验 generation。
- Opus frame 入 queue 后 immutable。
- sender 不调用 source、decoder、resampler、DSP 或 encoder。
- RTP session clock 不属于 track pipeline。
- live source 不进入 artifact cache。
- HTTP、UDP、timer 和 Node 等待不得阻塞 JS event loop。
- CPU codec 工作不得运行在 Tokio async worker 上。
