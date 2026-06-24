# Async Lifecycle 专项交接

这份文档只交接第 3 点：N-API/runtime orchestration 里的 lock、join、blocking work、promotion cancellation 和异步模型统一。它不重新解释播放模型；模型边界仍以 `docs/playback-model.md` 为准。

## 目标

专项优化目标不是“全部 Tokio 化”，而是让生命周期路径满足这几条：

- registry lock 只做查找、take、insert、remove，不跨越 stop/join、source/decode/encode、spawn playback 等慢操作。
- worker callback、actor mailbox、Tokio async worker 不直接执行 blocking join。
- stop、seek、switch、promotion、shutdown 都保持 generation-scoped cleanup。
- promotion 被 abort 后，即使 blocking pool 中的闭包已经开始，也不能再 promote stale preload。
- N-API 同步 ABI 可以继续 `block_on` mailbox command/status，但不要把 blocking media cleanup 混进 actor 状态路径。

## 当前已完成

已落地的相关改动：

- `crates/music_stream_napi/src/lib.rs`
  - `PlaybackRegistry`、`PreloadRegistry`、`PromotionRegistry` 继续用 `GenerationTaskSlot`。
  - current/preload cleanup 基本都是先从 registry lock 中取出 handle，再在锁外 stop/join。
  - playback/preload stop-join 已记录 histogram 和 error counter。
  - 替换 current 时的旧 playback 不再只 `stop()` 丢弃 handle，而是 `stop()` 后交给 `tokio.spawn_blocking` best-effort join。
  - preload promotion waiter 在 async task 中等待 completion；completion 后的 `join_for_promotion + start_promoted_playback` 已移到 `spawn_blocking`。
  - `PromotionRuntime { token, task }` 包住 promotion waiter；abort 时同时 cancel token 和 abort outer task。
  - blocking promotion 在进入、take preload 后、join preload 后、start promoted playback 前检查 cancellation token。
- `crates/music_stream_napi/Cargo.toml`
  - N-API crate 显式依赖 `metrics` 和 `tokio-util`。
- `crates/music_stream/src/session/mailbox.rs`
  - mailbox 已有 accepted/busy/closed/queue_depth metrics，便于观察 actor 压力。

已跑通过：

```sh
cargo fmt --all -- --check
cargo test --workspace --all-targets --all-features
cd crates/music_stream_napi && npm test
```

## 关键代码入口

先读这些位置：

- `crates/music_stream_napi/src/lib.rs`
  - `PreloadRuntime`
  - `PromotionRuntime`
  - `RuntimeHandles`
  - `RuntimeCallbackContext::handle_worker_event`
  - `Streamer::stop_stream`
  - `Streamer::shutdown_inner`
  - `cleanup_runtime_handles_for_stream`
  - `stop_join_playback`
  - `stop_replaced_playback`
  - `stop_join_playback_best_effort`
  - `take_stream_task`
  - `take_generation_task`
  - `cancel_current_playback`
  - `cancel_next_preload`
  - `abort_promotion`
  - `start_preload_promotion_waiter`
  - `start_promoted_playback`
- `crates/music_stream/src/lifecycle.rs`
  - `GenerationTaskSlot`
  - `RuntimeTaskGroup`
- `crates/music_stream/src/runtime.rs`
  - `LocalFileRtpPlayback::join`
  - `LocalFilePreload::join`
  - current/preload worker thread lifecycle
- `crates/music_stream/src/session/mailbox.rs`
  - bounded mailbox、shutdown、metrics。

## 需要继续深挖的问题

### 1. Promotion blocking cancellation 是否足够严格

当前用 `CancellationToken` 补了 abort 后仍进入 blocking pool 的情况，但仍要专项 review：

- token check 是否覆盖所有会产生 side effect 的点。
- `take_preload` 之后如果 token 被 cancel，当前 preload handle 会被消费并 drop/stop；这是否是期望语义。
- `start_promoted_playback` 内部 spawn playback 后，如果 insert current 前后发生 cancel，是否需要更细的 cleanup。
- outer promotion task 被 abort 时，blocking task 仍可能继续到 token check；是否还需要独立记录 orphan blocking task 指标。

验收思路：写一个可控 fake/preload promotion 测试，模拟 completion 后立即 cancel，确认不会 promote stale next，也不会留下 registry handle。

### 2. `spawn_blocking` 使用是否需要统一 helper

目前有两类 blocking cleanup：

- replaced playback best-effort join。
- preload promotion join + start promoted playback。

可以考虑增加很小的 helper，但不要抽象过度。只有在它能统一记录 error、latency、cancellation 和事件投递时才值得做。候选形态：

```text
spawn_blocking_lifecycle(name, token?, work)
```

不建议引入新的 task framework 或复杂 trait；N-API orchestration 当前还在单文件中，过早拆模块会增加阅读成本。

### 3. Stop/shutdown 是否应该等待 async cleanup

当前 replaced playback 的 cleanup 是 best-effort blocking task，不在 shutdown report 中等待。需要明确是否接受：

- 好处：不会在 callback/promotion 路径阻塞。
- 风险：shutdown 可能返回时还有 best-effort join task 在 blocking pool 中收尾。

如果要更严格，可以给 replaced cleanup 也进 registry 或一个 dedicated cleanup registry。但这会增加状态面，必须证明收益高于复杂度。

推荐下一步先加 metrics/测试确认 replaced cleanup 不会长期悬挂，再决定是否引入 cleanup registry。

### 4. `reap_finished_playbacks` 语义

`reap_finished_playbacks` 当前会取出 finished playback 后同步 `join()`。它在 `startStream`、`switchTrack`、`refreshCurrentSource`、`seekStream` 前调用。因为只 join 已 finished task，通常不会阻塞，但仍值得验证：

- `is_finished()` 和 `join()` 之间是否有竞态导致短暂阻塞。
- 是否应该统一记录 join latency/error。
- 是否应该复用 `stop_join` 指标或单独 `reap_join_us`。

不要直接把 reap 改成 fire-and-forget；finished report 可能携带错误，丢掉会降低诊断能力。

### 5. N-API `block_on` 边界

同步 N-API 方法目前内部 `block_on` actor mailbox command/status。这个设计可以保留，但要避免：

- 在 actor task 中反向调用会阻塞自己的 N-API 方法。
- 在 worker callback 中形成长链 blocking orchestration。
- 在持有 registry lock 时进入 `block_on`。

专项 review 时用 `rg "block_on"` 和 `rg ".write\\(|.read\\("` 逐个看上下文。

## 下一步推荐顺序

1. 为 promotion cancellation 写 focused Rust/N-API 侧测试，优先覆盖“completion 后 cancel 不 promote stale next”。
2. 为 replaced playback cleanup 添加更可观测的指标或测试，确认 `spawn_blocking` 路径不会 self-join、不会漏 join。
3. 给 `reap_finished_playbacks` 补 join latency/error 记录，评估是否需要迁移到 blocking pool。
4. 再决定是否引入极小 lifecycle helper；没有重复到 3 处以上不要抽象。
5. 最后才考虑拆 `crates/music_stream_napi/src/lib.rs` 的 runtime orchestration；拆分必须以“更好测试 lifecycle”为目标，不以文件长度为目标。

## 不要做

- 不要把 playlist、live recovery、network quality policy 下沉到 Rust。
- 不要把 actor mailbox 换成 unbounded channel。
- 不要让 sender tick 做 source read、decode、resample、encode。
- 不要用裸 `std::thread::spawn` 做每次替换 cleanup。
- 不要在 registry lock 内 stop/join。
- 不要认为 `JoinHandle::abort()` 能取消已经开始的 `spawn_blocking` 闭包。
- 不要为了统一 async 模型破坏 deterministic stop/join/shutdown。

## 新 session 建议 prompt

```text
请专项优化 async lifecycle / lock / join 路径。先读 docs/async-lifecycle-handoff.md、docs/playback-model.md，以及 crates/music_stream_napi/src/lib.rs 里的 PromotionRuntime、start_preload_promotion_waiter、cleanup_runtime_handles_for_stream、stop_join_playback。目标是保持 current+next+generation 模型不变，继续收敛 blocking join、promotion cancellation 和 registry lock 边界。不要做业务策略、playlist、live recovery 或大规模拆文件。完成后跑 cargo fmt --all -- --check、cargo test --workspace --all-targets --all-features、cd crates/music_stream_napi && npm test。
```
