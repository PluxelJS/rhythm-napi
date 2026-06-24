# Music Streamer 设计文档索引

## 按目的阅读

新 session 接手：

1. [handoff.md](handoff.md)：当前事实、代码入口、验证命令、不要重复做的事项。
2. [async-lifecycle-handoff.md](async-lifecycle-handoff.md)：专项优化 lock、join、promotion cancellation 和异步 lifecycle 时先读。
3. [status.md](status.md)：已落地能力、设计预留但未产品化的部分、当前一致性结论。
4. [design-review.md](design-review.md)：下一阶段真正值得设计和优化的方向。

理解播放模型：

1. [playback-model.md](playback-model.md)：精简入口，先理解 current/next、actor、generation、runtime 和事件边界。
2. [architecture.md](architecture.md)：目标架构、播放模型、音频管线、RTP/RTCP 会话。
3. [runtime.md](runtime.md)：任务模型、CPU/IO/实时分层、背压、取消和预载调度。

准备实现改动：

1. [implementation.md](implementation.md)：实现契约、不变量、worker loop、水位、错误、metrics 和反模式。
2. [testing.md](testing.md)：可测试性边界、fake 组件、虚拟时间、协议和压力测试。
3. [dependencies.md](dependencies.md)：必要依赖、选型理由、禁用方向和替换条件。

`architecture.md`、`implementation.md`、`runtime.md` 描述的是目标契约和演进方向，不表示所有条目都已经实现。当前完成度以 [status.md](status.md) 为准。

## 核心设计原则

- Node/TypeScript 负责业务编排、playlist/order/play mode、平台 URL 解析、权限和产品语义。
- Rust 负责媒体实时链路：source、decode、normalize、resample、Opus、RTP/RTCP、状态机。
- 协议层不手写 RTP/RTCP wire format，使用经过测试的 `rtp`/`rtcp` crate。
- Rust 播放逻辑只围绕 `current slot + next/preload slot + generation` 建模，确保切歌/seek 后旧帧不会泄漏。
- CPU-bound、IO-bound、realtime-bound 任务分层调度，RTP sender 永远不被解码/编码阻塞。
- 音频热路径放在核心 crate `music_stream` 的顶层 `audio/` 模块；stream/session/source/transport 编排也在 `music_stream`，napi crate `music_stream_napi` 只做 Node ABI 和类型映射。
- 旧 C++ 中有价值的体验特性要保留并重构：预载下一首、暂停恢复、seek、音量控制、流式音源；实现方式按 Rust 的状态机、背压和 source 策略重新设计。
- 具体实现先过 [implementation.md](implementation.md) 的不变量和反模式检查，再写代码。

## 实现硬约束

- Rust 不维护完整 playlist。
- 热路径不做无界队列、不做每帧临时分配、不跨 active slot 共享 mutable buffer。
- seek/switchTrack 必须递增 generation。
- decode/encode ahead-of-time 维持水位，sender tick 到来时不能临时解码。
- resample 使用 rubato 预分配 buffer 路径。
- SIMD 只在 profiling 证明必要后局部引入，scalar 路径始终是正确基线。

## 实现前检查

- 是否能用 fake source/decoder/encoder 和 virtual time 测试？
- 是否所有队列容量都用时间水位表达？
- 是否 public command 只进入 actor，worker 只返回 event？
- 是否错误有稳定 code，metrics 能观测队列水位、drift、underrun 和 allocation？
- 是否没有把 napi、真实网络或真实 codec 作为核心状态机测试前置条件？
- 当前代码与目标契约是否仍能在 [status.md](status.md) 中逐项对上？
- 这项改动是否属于 Rust 实时内核？如果需要 playlist、鉴权、voice gateway、自动恢复或持久缓存上下文，先看 [design-review.md](design-review.md)。
