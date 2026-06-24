# 播放模型精简版

这份文档是 `architecture.md`、`runtime.md` 和 `implementation.md` 的精炼入口，用来快速理解当前播放模型。更完整的目标约束、实现细节和测试矩阵仍以对应专题文档为准。

## 一句话模型

Rust 只管理一个 stream 的实时播放内核：`current` 正在播，`next` 可预载，所有切换靠 `generation` 隔离旧任务和旧帧。Node/TypeScript 管 playlist、播放模式、业务鉴权和下一首决策；Rust 管 source、解码、重采样、Opus、RTP/RTCP、状态机和任务生命周期。

```text
TypeScript playlist/policy
  -> N-API Streamer
  -> per-stream actor mailbox
  -> StreamActor: current + next + generation + status
  -> TaskAction
  -> runtime controller
  -> source -> decode -> normalize/gain -> resample -> Opus -> RTP/RTCP
  -> WorkerEvent -> actor
  -> StreamEvent/status -> N-API callback + drainEvents
```

## 责任边界

Node/TypeScript 负责业务含义：

- playlist、随机/循环/推荐、跳转到哪一首。
- 平台 URL 解析、鉴权刷新、voice gateway 协商。
- live 断流后的自动重连/放弃策略。
- 是否应用 ReplayGain 推荐值、是否根据网络质量降码率。

Rust 负责实时媒体内核：

- 只维护 `current` 和 `next`，不维护完整 playlist。
- 校验并消费 `TrackSource`、source policy 和 RTP transport config。
- 解码、DSP、重采样、Opus 编码、RTP pacing、RTCP feedback。
- actor 状态机、generation 过滤、任务启动/取消/回收。
- 稳定错误 code、状态快照和事件队列。

## StreamActor 是状态唯一写入者

每个 stream 有一个 bounded actor mailbox。public command、status query 和 worker event 都串行进入 `StreamActor`。

```text
public command -> StreamActor -> ActorOutput { actions, events, status }
worker result  -> WorkerEvent  -> StreamActor -> ActorOutput { actions, events, status }
```

actor 只做状态转换，不做 CPU/IO 重活。它输出 `TaskAction`，由 N-API/runtime controller 执行实际 worker 操作。worker 不能直接改状态，也不能直接决定 public event；worker 只能回报 `WorkerEvent`，再由 actor 判断是否属于当前 generation、是否该 promote next、是否该发 error/nextNeeded/sourceRefreshNeeded。

这个边界解决的是 race：所有状态变化只有一个入口，runtime task 只是 actor action 的执行结果。

## Current、Next 与 Generation

一个 stream 同时最多有两个轨道槽：

- `current`：正在播放、缓冲或等待恢复的当前曲目。
- `next`：TS 指定的下一首，可做有界预载，但不能直接发送 RTP。

每次 `seek`、`switchTrack`、current 结束后 promote next、刷新 current source，都会进入新的 generation。playback/preload/promotion handle 都按 generation 存取；旧 worker event、旧 frame、旧 preload completion 到达时会被过滤或只能操作匹配 generation 的任务。

```text
generation 是旧数据防线：
  old frame 不发送
  old worker event 不改状态
  old task handle 不取消新任务
  old preload completion 不 promote 新 current
```

## 播放状态与命令

核心状态可以理解为：

```text
idle -> buffering -> playing
playing <-> paused
playing/buffering -> stopped
failure -> idle/error
```

重要命令语义：

- `startStream`：创建 actor，应用启动 volume/gain，执行 play，启动 current worker，并为 next 安排 preload。
- `pauseStream`：冻结 playout，不销毁整条 pipeline；上游队列靠 bounded backpressure 停住。
- `resumeStream`：恢复 pacing，RTP sequence/timestamp 保持单调。
- `seekStream`：只对 seekable current 生效；递增 generation，取消旧 current，按目标位置重建播放 worker。
- `setNext`：只替换或清空 next，不影响 current；live next 明确 unsupported，因为无限流不能预载为 next。
- `switchTrack`：由 TS 指定新的 current/next；Rust 不从 playlist 推导目标。
- `refreshCurrentSource`：用于同一 current 身份的 URL/live endpoint 刷新，递增 generation 并重启 current。
- `stopStream`：停止并 join current/preload，abort promotion，关闭 actor，只保留轻量 inactive status 供幂等 stop/status 和 streamId 复用。

## 媒体管线

RTP sender 不解码、不重采样、不编码。发送前，worker ahead-of-time 准备未来一小段音频：

```text
source
  -> decoder
  -> channel normalize + volume/gain + limiter
  -> resampler
  -> frame assembler
  -> Opus encoder
  -> bounded encoded queue
  -> RTP sender pacing
  -> UDP
```

队列按毫秒水位控制，不使用无界音频队列。encoded queue 满时 encoder 停，decoded queue 满时 decoder 停，pause 时 sender 不消费，背压自然向上游传播。

RTP sender 是实时节拍器：

```text
等待 active generation 预缓冲
按 Opus frame duration 计算 deadline
到点只取已编码好的 active frame
无 frame 则 underrun/rebase
不 burst 发送历史 backlog
不在 sender tick 临时 decode/encode
```

## Source 模型

source artifact 和播放 slot 是不同生命周期。

- `file`：本地文件，直接走 seekable 文件路径。
- `url`：有界 HTTP/HTTPS media，先解析/下载成 tempfile artifact，默认 seekable，可进入 bounded LRU cache。
- `live`：无限或 non-seekable HTTP stream，走 bounded byte pipe，不进入 tempfile artifact cache，不支持 seek，不支持 next preload。

HTTP 401/403 会映射成稳定 `SOURCE_AUTH_EXPIRED`，并通过 `sourceRefreshNeeded` 事件交给 TS 刷新 URL。Rust 不复制业务鉴权逻辑。

## 预载与 Promotion

next preload 的目标是减少曲间空白，不是无限提前解码。

```text
TS 提供 next
actor 生成 PrepareNext action
runtime 启动 preload worker
preload 到 ready 后发 NextReady
current 结束时 actor 才能 promote ready next
promotion 后 next generation 成为 current generation
```

如果 current 结束但没有 ready next：

- 有 next 但未 ready：进入 buffering，等待 preload completion。
- 没有 next：进入 idle，发 `nextNeeded`，等待 TS 决策。

如果 next preload 失败：

- 鉴权失败额外发 `sourceRefreshNeeded`。
- next 被清空并发 error。
- current 不被中断。

## 事件与状态

`ActorOutput.status` 是强制存在的当前 actor 快照。N-API command 执行 runtime actions 后，会重新读取 actor 当前状态返回，避免 runtime 同步失败回灌 actor 后返回旧快照。

事件有两个出口：

- `setEventCallback`：低延迟通知。
- `drainEvents`：可靠补偿队列。

callback 不能替代 drain。调用方如果需要不丢事件，应定期 drain。启动阶段的 volume/gain/play 状态变化、worker failure、nextNeeded、sourceRefreshNeeded、networkQualityChanged 都应进入事件队列。

## 继续精进的重点

播放模型本身已经收敛，后续精进应围绕可观测性、策略输入和生命周期一致性，不应改变 Rust 只管理 `current + next + generation` 的边界。

- 策略闭环放在 TS：live recovery、网络质量动作、playlist fallback 和 ReplayGain 应由宿主消费事件后显式调用 Rust command。
- 观测要覆盖失败入口：source resolve、live retry、RTP/RTCP、CPU permit、stop/join latency 和 mailbox busy 都应有稳定指标；失败路径也要记录耗时和错误计数。
- 生命周期路径保持单一入口：stop、shutdown、switch、seek、promotion 只能通过 generation-scoped handle cleanup，避免旧任务误取消新任务。
- 真实兼容性靠 corpus 验证：格式、HTTP Range、live 断流、慢响应和损坏样本应形成手动或 nightly 矩阵，不把这些业务样本塞进 actor 单元测试。
- async 化只做内部替换：只有 profiling 证明 UDP/timer/source IO 是瓶颈时才替换 worker 内部实现，actor/action/generation/task-tree 不变。

## 错误处理原则

- 错误必须有稳定 code，Node 不解析字符串判断类型。
- worker failure 先回到 actor，再由 actor 决定状态和事件。
- current 失败且无 next：进入 idle，发 error 和 nextNeeded。
- 连续 track 错误超过阈值才升级为 stream error。
- startup current 失败会清理 stream；鉴权失败可额外排队 sourceRefreshNeeded。

## 必须保持的不变量

- Rust 不维护 playlist。
- actor 是 stream 状态唯一写入者。
- worker 只能回报 WorkerEvent。
- seek/switch/promotion/refresh 必须用 generation 隔离旧任务。
- sender 只发送 active generation。
- sender 不做 source read、decode、resample、encode。
- 所有音频队列 bounded，并用毫秒水位表达容量。
- live 不进入 tempfile artifact cache。
- source artifact 生命周期独立于 TrackSlot。
- N-API 只做 ABI、类型映射、runtime orchestration 和生命周期收口。
