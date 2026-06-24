# Codex 接手说明

这份文档用于新开的 Codex session 快速接手当前代码库。它已经按现有代码、测试和状态文档重写；如果这里和代码冲突，以代码和测试为准，其次看 [status.md](status.md)、[testing.md](testing.md)、[architecture.md](architecture.md)、[implementation.md](implementation.md)、[runtime.md](runtime.md)。

## 先读顺序

1. `docs/handoff.md`：当前事实和下一步建议。
2. `docs/status.md`：已落地能力和仍未完成的目标设计。
3. `docs/design-review.md`：下一阶段策略、观测、兼容性、宿主集成和代码库优化顺序。
4. `docs/testing.md`：测试矩阵、Criterion bench 入口和新增能力应补的测试层次。
5. `docs/implementation.md`：实现不变量、反模式、source/metrics/音量约束。
6. `docs/runtime.md`：runtime、背压、任务分层和后续 async 演进边界。

核心代码入口：

- `crates/music_stream/src/session/mod.rs`：纯 actor 状态机，current/next slot、generation、worker event、TaskAction。
- `crates/music_stream/src/session/mailbox.rs`：per-stream bounded Tokio actor mailbox。
- `crates/music_stream/src/lifecycle.rs`：`RuntimeTaskGroup`、`GenerationTaskSlot` 等 lifecycle 原语。
- `crates/music_stream/src/runtime.rs`：当前本地文件/有界 HTTP URL RTP playback/preload worker。
- `crates/music_stream/src/slot.rs`：slot driver、preload/current、RTP drain。
- `crates/music_stream/src/audio/pipeline.rs`：ahead-of-time decode/encode queue。
- `crates/music_stream/src/source/mod.rs`：file/HTTP tempfile source、Range resume、artifact cache。
- `crates/music_stream/src/transport/mod.rs`：RTP/RTCP config、packetize、UDP sink、pacer、RR 解析。
- `crates/music_stream/src/quality.rs`：RTCP quality rolling window 和等级判定。
- `crates/music_stream_napi/src/lib.rs`：N-API 类型映射、runtime orchestration、event callback/drain。
- `crates/music_stream/benches/criterion_pipeline.rs`：Criterion 统计型 pipeline microbenchmark。
- `crates/music_stream/tests/playout_flow.rs`：核心集成流、RTP runtime 和 HTTP source。
- `crates/music_stream_napi/test/*.test.ts`：TypeScript Vitest N-API smoke，按 RTP/RTCP、source policy、live stream、playback controls 分组。

## 当前实际状态

当前代码库已经超过 MVP。不要把以下能力当成待办：

- 本地 MP3/WAV 和有界 HTTP/HTTPS URL 可以解码、重采样、Opus 编码，并按 RTP pacing 推到 UDP。
- sender 不做 source read、decode、resample、encode，只从 encoded queue 取 active generation frame。
- decode/encode 是 ahead-of-time，使用毫秒水位和 bounded queue。
- `StreamingPcmDecoder`/`StreamingPcmWriter` 已提供 live/streaming 的内部 fake/backpressure 模型：producer handle 可在 decoder 交给 pipeline 后继续 bounded push，满载 `BUSY`，空流 `NeedMore`，finish 后 `End`；pipeline 测试已覆盖它和现有水位机制的配合。
- `StreamingByteReader`/`StreamingByteWriter` 已提供 live source reader 和 non-seekable decoder 输入之间的 bounded byte pipe：`try_push` 满载返回 `BUSY`，`push_blocking` 等待读端释放容量，reader 实现 `std::io::Read`，finish/fail/close 会稳定唤醒读写两端。
- `SymphoniaStreamDecoder` 已能消费 non-seekable reader；测试覆盖 `StreamingByteReader` -> Symphonia `ReadOnlySource`/`MediaSourceStream` -> decoded chunk。
- `HttpLiveStream` 已能读取真实 HTTP body 到 bounded byte pipe；pipe 满会阻塞形成背压，stop/close 会唤醒阻塞写，EOF/401/403 有稳定语义；408/429/5xx/timeout/连接类错误已有保守 retry/backoff，耗尽后 fail reader 并走 current failure。
- `LiveStreamSlot` 已能把 `HttpLiveStream -> SymphoniaStreamDecoder -> Rubato -> Opus -> SlotDriver` 串起来，并复用现有 prebuffer、bounded queue、sender drain 和 worker event 语义。
- N-API source policy 已分成 `source.http` 和 `source.liveHttp`：前者只控制有界 HTTP tempfile artifact，后者控制 live HTTP timeout、byte pipe buffer、read chunk、retry 次数和 retry backoff。
- current/next slot 分离，next 可以预载到 `NextReady`；current 结束后 promotion 复用已预热 driver。
- pause/resume/seek/switchTrack/setNext/stop/shutdown 已覆盖本地文件和有界 HTTP URL。
- seek/switchTrack 会递增 generation；旧 worker event 和旧 frame 不应影响新 current。
- 用户 `volume` 和额外 `gainDb` 已实现，正增益路径有 soft limiter。
- DSP 层已有 `PcmLevelMeter`，可算 peak/RMS dBFS 和保守 headroom gain；另有显式 ReplayGain 推荐器，N-API `recommendReplayGain` 可根据 Node 传入的 track/album gain、peak、preamp、防削波策略返回推荐 `gainDb`，但不会自动改播放增益。
- RTP packetize 和 RTCP SR/RR 使用 crates，不手写 wire format。
- RTCP Sender Report、Receiver Report 解析、latest RR 派生 loss/jitter/RTT 已实现。
- RTCP quality window 已在 metrics/runtime 层实现，并通过 actor/N-API 产出 `networkQualityChanged` 等级变化事件；事件携带 rolling window 的样本数、loss/jitter/RTT 摘要，供 TS 策略层消费。
- `RtpPacketProtector` 扩展点已存在；未安装 protector 时非 `none` 加密会明确失败。
- HTTP source 支持 timeout/max bytes/cache temp files policy，N-API 可配置。
- HTTP resolver 复用 per-resolver blocking client，避免每次 resolve 重建 client/连接池。
- HTTP 下载中断支持同次 resolve 内 `Range: bytes=N-` resume；只接受正确 `206 Partial Content` 和匹配的 `Content-Range`。
- artifact cache 是进程内 bounded LRU，只缓存完整、seekable、cacheable tempfile artifact，不缓存 slot/pipeline。
- HTTP 401/403 映射为 `SOURCE_AUTH_EXPIRED`，并通过 `sourceRefreshNeeded` 事件交给 Node 重新解析 URL。
- N-API 已有 `refreshCurrentSource(streamId, current)`：TS 在刷新 live/URL 鉴权或等到新 live endpoint 后，可在同一个 stream/transport/source policy 下重启 current；该命令递增 generation、保留 next slot、不维护 playlist。
- next 预载鉴权失败不会中断已启动的 current playback。
- N-API 有 `setEventCallback` 低延迟事件和 `drainEvents` 可靠补偿队列。
- `Streamer.shutdown()` 会 stop/join active playback/preload，清空 transport/events/source cache/engine。
- `stopStream` 会 stop/join current playback/preload、abort promotion、shutdown actor mailbox，只保留轻量 inactive status，支持幂等 stop/status 和 streamId 复用。

## Runtime/lifecycle 边界

Tokio actor/task lifecycle 已经接入 N-API runtime orchestration，不再是未完成的大项：

- `RuntimeTaskGroup` 固定 cancel、join、超时 abort、panic/failure 汇总语义。
- `StreamActorMailbox` 使用 bounded Tokio channel；队列满返回 `BUSY`。
- public command、status query 和 worker event 在 per-stream actor task 中串行处理。
- N-API 不再直接写 `Engine`，而是持有 mailbox registry；同步 ABI 内部 `block_on` mailbox command/status。
- worker callback 回灌 mailbox，actor 输出 `TaskAction` 后由 runtime controller 统一执行。
- `GenerationTaskSlot` 管理 playback/preload/promotion，所有 handle 操作都按 generation 校验。
- preload promotion waiter 等待 `LocalFilePreloadCompletion` 通知，不用 sleep 轮询；stop/switch/cancel 会 abort 对应 promotion 并停止同 generation preload。
- 当前 RTP media worker 仍是受控 blocking worker handle，但已经由 actor action 和 generation-scoped registry 管理。
- worker idle/pacer/preload 等待会被 stop、pause/resume、volume/gain 唤醒；current/preload 有轻量 CPU permit lane。

如果后续继续 async 化 worker 内部，要保持这些边界不退化：actor/action/generation/task-tree、bounded queue、deterministic stop/join、sender 只做 timer + UDP send。

## Benchmark 现状

bench 保持基础版本，不要把产品化观测或自定义趋势基线当成当前主线。

可运行入口：

```sh
cargo bench -p music_stream --bench criterion_pipeline
```

`criterion_pipeline` 已接入 Criterion，用社区主流统计型 bench 覆盖 fake pipeline worker + sender drain 热路径。

短 RTP integration test 继续覆盖真实 localhost UDP、packet count、sequence number 和 RTP timestamp 单调。后续有明确 profiling 需求时，再按 Criterion 的方式补 codec、resample、Opus encode、source/cache 等具体 benchmark；不要恢复项目内自定义 long-soak 统计框架。

## 当前测试覆盖

常规验证命令：

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p music_stream --no-default-features
cargo clippy -p music_stream --no-default-features --all-targets -- -D warnings

cd crates/music_stream_napi
npm test
```

N-API `npm test` 会先执行 `napi build --platform`，再跑 `tsc --noEmit` 和 `vitest run`。当前 smoke 拆成 `rtp-feedback.test.ts`、`source-policy.test.ts`、`live-stream.test.ts` 和 `playback-controls.test.ts`，覆盖本地 WAV、HTTP URL、source policy、cache 行为、RTCP RR/status、`networkQualityChanged`、callback/drainEvents、volume/gain、pause/resume/seek/switch/stop/shutdown 等边界。

Rust 侧已覆盖：

- fake decoder/encoder + memory sink 的确定性 actor/pipeline/slot 测试。
- Symphonia/Rubato/libopus/RTP 的真实 core integration。
- localhost UDP 的真实 RTP worker。
- 本地 MP3 fixture 到 RTP。
- 有界 HTTP URL 下载到 tempfile 后走同一解码链路。
- HTTP artifact cache 复用、LRU 驱逐、tempfile 生命周期清理。
- HTTP Range resume 成功、服务端忽略 Range、错误 Content-Range。
- HTTP 403 -> `SOURCE_AUTH_EXPIRED` -> `sourceRefreshNeeded`。
- RTCP SR/RR 基础链路、latest RR status、RTCP quality window metrics 和带 snapshot 的网络质量等级变化事件。
- 短 RTP integration、pipeline Criterion bench 和 source/cache 行为测试。

## 最终设计模型与剩余实现

这一节把还没有完全产品化的方向收敛成最终模型。原则是：Rust core 只做确定的实时内核、协议解析、source 原语和稳定事件；TS/Node 负责业务策略、playlist、鉴权刷新、自动动作和持久化元数据。除非下面明确写成 Rust 责任，不要把策略继续下沉到 runtime。

### 1. Live/non-seekable streaming

`TrackKind::Live` 当前已对外启用为 live current playback：N-API `startStream` 和运行中 `switchTrack` 都可接收 live HTTP URL，runtime 走 `HttpLiveStream -> StreamingByteReader -> SymphoniaStreamDecoder -> Rubato -> Opus -> RTP`，不经过 artifact resolver/cache。live 输入会归一化为 non-seekable；`seekStream` 对 live 返回 not seekable。live next/preload 显式 unsupported，不进入 tempfile/cache/promotion。

最终模型已经固定：

- Rust 只保证“一个 live current”的确定性运行：bounded byte pipe、下游 backpressure、source-level finite retry、稳定错误码、generation-scoped restart。
- `source.http` 只控制有界 URL tempfile artifact；`source.liveHttp` 只控制 live HTTP timeout、buffer、read chunk、retry 和 backoff。
- 401/403 不在 Rust 内刷新鉴权，只映射 `SOURCE_AUTH_EXPIRED` 并发送 `sourceRefreshNeeded`。
- 运行中 current source failure 固定为 `error`；无 next 时进入 `idle` 并发 `nextNeeded`；TS 可以在同一 stream 上调用 `refreshCurrentSource` 递增 generation 并恢复 current。
- live 不进入 artifact cache，不复用无限增长 tempfile，不参与 next preload/promotion。

TS 侧如果要做自动恢复，应按这个确定状态机实现，而不是改变 Rust runtime：

```text
PlayingLive
  -> SourceRefreshNeeded(auth/source error)
  -> ResolveFreshEndpoint(TS business resolver)
  -> RefreshCurrentSource(streamId, current)
  -> PlayingLive on success
  -> GiveUpAndAskPlaylist after explicit retry budget
```

推荐默认策略是显式关闭自动刷新；宿主启用时必须配置总尝试次数、退避、总耗时上限和失败后的 playlist 动作。失败后的唯一收敛动作是 `switchTrack`、`setNext`、`stopStream` 或保持 idle 等待；Rust 不自动猜下一首。

### 2. RTCP quality strategy feedback

当前已有 RR 解析、latest snapshot、rolling quality window、metrics gauge 和 `networkQualityChanged` 事件；N-API 事件已带 quality snapshot，可作为策略输入。最终模型是“反馈输入已完成，自动动作属于策略层”：

- transport parser 只解析 RTCP、维护原始字段和单位换算。
- runtime/metrics 只做 rolling window、质量等级和事件。
- N-API 只把 snapshot 稳定映射给 TS。
- TS 根据 `good/degraded/poor`、样本数、loss/jitter/RTT、冷却时间和业务目标决定动作。

自动网络策略如果要做，放在 TS orchestrator，并且默认禁用。一个确定的策略形态如下：

```text
networkQualityChanged
  -> require minSamples and cooldown
  -> degraded: lower Opus bitrate hint for next restart or next track
  -> poor: refresh/switch transport only if host owns that capability
  -> good for N consecutive windows: allow one upward step
```

Rust 不应实现“自动降码率”“自动换节点”“自动重连语音网关”。这些动作都需要业务上下文，必须由 TS 下发明确命令或重新 start/switch。

### 3. ReplayGain / EBU R128 loudness

已有手动 `gainDb`、soft limiter、PCM peak/RMS meter，以及显式 ReplayGain 推荐 API：

- metadata 当前由 Node/TS 传入，不由 Rust runtime 自动分析。
- `recommendReplayGain` 支持 track/album mode、fallback、preamp、peak 防削波和 `GainLevel` 范围限制。
- 返回值只是推荐 `gainDb` 和限制原因；调用方必须显式把它传给 `startStream` 或 `setGain`，runtime 不会自行改变响度。

最终响度模型分三层，顺序固定：

```text
user volume 0.0..1.0
  + explicit gainDb from TS policy
  + soft limiter/headroom protection
  -> Opus encode
```

EBU R128 如果要做，应该作为离线或 preload 前的 metadata scan：TS 或独立 scanner 生成 loudness metadata，缓存到业务库或 track metadata，再调用现有 `recommendReplayGain`/`setGain`。Rust runtime 不在播放中偷偷扫描整首歌、不自动改 gain、不把多来源响度状态藏进 slot。运行中渐变可以作为显式 `setGain` 命令的平滑增强，但必须仍由调用方触发。

### 4. Source artifact 与持久缓存

当前进程内 bounded LRU artifact cache 已经是 Rust core 的最终默认能力：只缓存完整、seekable、cacheable tempfile artifact，不缓存 slot/pipeline，不缓存 live，不缓存 partial artifact。同次 resolve 内的 Range resume 已实现；跨 resolve、跨进程、跨启动的持久缓存 metadata 不属于当前 Rust core。

如果未来需要持久缓存，最终模型应放在 TS/业务缓存层：

- TS 用业务稳定 key 管理持久文件、TTL、配额和授权有效期。
- TS 把命中的持久文件作为 local file track 传给 Rust。
- Rust 仍只看到 local file 或有界 URL，不知道业务 cache index。
- Rust artifact cache 继续作为短生命周期优化，不承担产品级离线缓存。

不要把半下载 artifact 放入 Rust LRU，也不要让 Rust 保存跨进程 resume record；这会把业务权限、过期和磁盘策略耦合进实时内核。

### 5. RTP packet protection

`RtpPacketProtector` 扩展点已存在；未安装 protector 时非 `none` 加密会明确失败。最终模型是“Rust 定义插入点和失败语义，平台算法由宿主注入”：

- 加密/保护位置固定在 RTP marshal 后、UDP send 前。
- transport config 只携带 mode/key material 等已解析参数，不做 voice gateway 鉴权。
- 未安装匹配 protector 时必须 fail closed，不能静默明文发送。
- 具体算法、密钥轮换和平台协商属于 TS/宿主集成，不进入 decoder、slot、actor 或 source。

### 6. Worker 内部最终形态

控制面 lifecycle 已经完成；RTP worker 内部也已收敛到当前最终形态。这里不再把“换成 Tokio”当成目标本身；目标是实时 RTP/RTCP tick 不被 source/decode/encode 阻塞，同时保持 stop/join 的确定性。

当前模型：

- actor/action/generation/task-tree 不变。
- public command、status query 和 worker event 继续通过 per-stream bounded mailbox 串行化。
- current playback tick 已拆成控制同步、RTP/RTCP IO、CPU worker turn 和 wait decision。
- prebuffer 前保持 worker-first，确保 `CurrentPrebufferReady` 事件语义；ready 后改为 IO-first，先 drain RTP、处理 RTCP SR/RR，再 opportunistic 做 decode/encode worker turn。
- CPU permit lane 是非阻塞尝试获取；拿不到 permit 只记录 busy 并让出本轮，不阻塞 RTP/RTCP tick。
- source IO 可以继续使用当前受控 blocking reader；如果未来替换成 async reader，也必须只替换 source/IO 内部，不改变 actor 和 slot 边界。
- decode/resample/encode 仍走 CPU lane 或 dedicated CPU pool，不进入 sender 实时路径。
- 所有队列继续 bounded，并用毫秒水位表达背压。
- sender 仍只做 timer + UDP send/recv RTCP，不做 decode/encode/source read。
- stop/switch/seek/shutdown 必须保持 deterministic cancel/join/abort 语义。

后续只有 profiling 明确显示 UDP/timer 或 source IO 是瓶颈时，才做局部 async 替换。验收标准不是“用了 Tokio”，而是测试继续覆盖 pause/resume/seek/switch/stop/shutdown、generation 过滤、RTP timestamp 单调、无 burst backlog、RTCP SR/RR、preload promotion 和 live writer stop 唤醒。

## 必须保持的不变量

- Rust 不维护 playlist。
- sender 不解码、不编码、不读 source。
- 所有音频队列 bounded，用毫秒水位表达。
- seek/switchTrack 必须递增 generation。
- worker event、playback/preload/promotion handle 必须 generation-scoped。
- source artifact 与 `TrackSlot` 生命周期分离。
- cache 只保存完整 artifact，不保存运行中的 pipeline/slot。
- live stream 不进入 tempfile artifact cache。
- N-API 只做 ABI、类型映射、runtime orchestration。
- 错误必须有稳定 code，Node 不应只能解析字符串。
- callback event 只是低延迟通知，`drainEvents` 才是可靠补偿队列。
- library 不安装全局 metrics recorder 或 tracing subscriber。

## 避免的反模式

- 用 unbounded channel 解决卡顿。
- sender tick 到来时临时 decode/encode。
- 把 live stream 塞进无限 tempfile。
- 在 Rust 里复制业务鉴权刷新。
- 在 transport parser 里塞网络质量策略。
- 为了“Tokio 化”破坏当前 shutdown/join 的确定性。
- 为了自动响度在 runtime 中偷偷改 gain。
- 把 bench 已有能力重复当成主线任务，除非目标是趋势化或扩展 corpus。

## 当前验证状态

最近文档记录的完整验证通过：

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p music_stream --no-default-features
cargo clippy -p music_stream --no-default-features --all-targets -- -D warnings
npm test
```

`npm test` 需要在 `crates/music_stream_napi` 目录运行。
