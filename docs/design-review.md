# 设计审视与优化方向

这份文档把当前还值得继续设计、验证和取舍的问题集中在一起。它不重复 `status.md` 的完成度清单，也不替代 `architecture.md`、`implementation.md`、`runtime.md` 的契约；它回答的是下一阶段应该把注意力放在哪里，以及哪些方向不应继续下沉到 Rust core。

## 当前判断

代码已经越过“播放器内核能否成立”的阶段。actor/generation、bounded queue、ahead-of-time pipeline、本地文件、有界 HTTP、live current、RTCP RR/quality event、next preload/promotion、N-API lifecycle 和显式 shutdown 都已落地。接下来最值得优化的不是继续扩 Rust runtime 的业务能力，而是把策略边界、可观测数据、真实音源兼容性和宿主集成面设计清楚。

核心方向：

- Rust core 继续保持确定性实时内核：source 原语、decode/resample/encode/RTP/RTCP、状态机、稳定事件和指标。
- TS/Node 负责业务策略：playlist、鉴权刷新、live 自动恢复、网络质量动作、持久缓存、voice gateway 和平台加密算法。
- 新能力先问“是否需要业务上下文”。需要业务上下文的，默认不进入 Rust core。

## 优先级

### P0：保持边界不退化

这些不是新功能，但每次改动都应主动检查：

- public command、worker event、status query 必须继续通过 per-stream actor mailbox 串行进入 actor。
- seek/switch/promotion/refresh 必须递增 generation 或校验 generation。
- sender 仍只做 pacing、RTP/RTCP IO 和 active generation frame drain，不进入 source/decode/encode。
- live 不进入 tempfile artifact cache，不参与 next preload/promotion。
- callback 只是低延迟通知，`drainEvents` 才是可靠补偿队列。
- library 不安装全局 metrics recorder 或 tracing subscriber。

如果后续做 async UDP/timer/source IO，验收标准不是“用了 Tokio”，而是这些边界保持不变，同时 stop/switch/seek/shutdown 仍能 deterministic cancel/join。

### P1：TS 策略层设计

Rust 已提供足够的策略输入，但自动动作还没有产品语义。建议先在 TS 层设计三个小 orchestrator，而不是扩大 Rust runtime：

1. Live source recovery
   - 输入：`sourceRefreshNeeded`、`error`、`nextNeeded`、当前 retry budget、业务 resolver。
   - 动作：重新解析 endpoint 后调用 `refreshCurrentSource`；超过预算后 `switchTrack`、`setNext`、`stopStream` 或保持 idle。
   - 默认关闭自动恢复，宿主显式配置最大次数、退避、总耗时上限和失败后的 playlist 动作。

2. Network quality policy
   - 输入：`networkQualityChanged` snapshot、min samples、cooldown、业务端点能力。
   - 动作：记录质量、必要时重启 transport、调整下一次启动的 bitrate hint，或通知宿主。
   - Rust 不做自动降码率、换节点、重连 voice gateway。

3. Loudness policy
   - 输入：业务库中的 ReplayGain/EBU R128 metadata、用户音量、preamp、防削波配置。
   - 动作：调用 `recommendReplayGain`，再显式传给 `startStream` 或 `setGain`。
   - Rust runtime 不在播放中隐式扫描整首歌，也不自动改变 gain。

### P1：真实音源与格式兼容性

现有测试偏短 WAV/MP3 和 finite live WAV，足以验证内核，但不足以证明真实曲库兼容性。建议补一个外部或可选的 corpus 验证流程：

- 常见容器：MP3、AAC/M4A、FLAC、OGG/Vorbis、WAV。
- 异常样本：损坏头、超大 metadata、VBR 时长不准、短音频、非 48 kHz、多声道。
- HTTP 行为：无 `Content-Length`、Range 不支持、Range 错误、慢速响应、中途断流、错误 MIME。
- live 行为：长时间无数据、短暂 5xx、鉴权过期、server close、chunk 很小或很大。

这类验证不一定都进常规 CI；可以先做手动/夜间脚本，产出兼容性矩阵和明确失败 code。

### P1：观测与调参基线

已有 metrics hook 和 Criterion bench，但还缺“调参时看什么”的固定视图。建议把以下指标定义成默认 dashboard 或测试记录模板：

- current/preload queue ms：decoded、encoded、prebuffer wait。
- RTP：sent packets/bytes、underrun、late drop、max pacing late、timestamp monotonic。
- RTCP：RR samples、loss/jitter/RTT avg/max、quality level transition。
- Source：resolve duration、bytes read、cache hit/miss/insert、retry count、auth expired。
- CPU：worker turn duration、decode/resample/encode realtime factor、CPU permit busy count。
- Lifecycle：stop/join latency、abort count、mailbox BUSY count。

调参优先看真实接收端和 localhost RTP integration，再扩 Criterion microbench。不要恢复项目内自定义 long-soak 统计框架。

### P2：持久缓存归属

Rust 进程内 artifact LRU 已足够作为短生命周期优化。持久缓存需要处理配额、授权、TTL、业务 key 和跨进程元数据，建议归 TS/业务层：

- TS 命中持久缓存后，把文件作为 `kind: "file"` 传给 Rust。
- Rust 继续只处理 local file、有界 URL 或 live URL。
- Rust 不保存跨进程 resume record，不把 partial artifact 放进 LRU。

如果未来确实需要 Rust 参与持久缓存，先写独立 ADR，说明权限、过期、磁盘配额和清理策略。

### P2：平台 RTP protection

当前 `RtpPacketProtector` 插入点已经固定。下一步设计点是宿主如何注入平台算法：

- 加密位置保持在 RTP marshal 后、UDP send 前。
- 非 `none` 配置且没有匹配 protector 时继续 fail closed。
- key rotation、gateway 协商、session token 不进入 decoder、slot、actor 或 source。
- 如果算法需要 nonce/sequence 派生，接口只暴露最小 packet context。

## 需要明确的决策

| 问题 | 推荐方向 | 原因 |
| --- | --- | --- |
| live 自动恢复放哪里 | TS orchestrator | 需要业务 resolver、预算和失败后的 playlist 决策 |
| 网络质量自动动作放哪里 | TS orchestrator，默认禁用 | 降码率、换节点、重连都依赖宿主能力 |
| EBU R128 是否进 runtime | 不进播放热路径 | 应离线或 preload 前生成 metadata，再显式 setGain |
| 持久缓存放哪里 | TS/业务层 | 权限、TTL、配额和跨进程生命周期是产品问题 |
| async 化 worker 是否是目标 | 不是目标本身 | 只有 profiling 证明 UDP/timer/source IO 成为瓶颈时才替换内部实现 |
| FFmpeg backend 何时引入 | 真实 corpus 证明 Symphonia 不足时 | 避免过早引入部署和安全复杂度 |
| `@types/node` major | 已跟运行时 Node 24 major 对齐 | 当前 package 运行时锁 Node 24，dev dependency 使用 `@types/node@24.x` |

## 策略执行规格

Live recovery 默认关闭。启用时 TS/宿主必须配置 `maxAttempts`、退避、总耗时上限和 terminal action。只消费 `sourceRefreshNeeded`、`error`、`nextNeeded`，只调用 `refreshCurrentSource`、`switchTrack`、`setNext` 或 `stopStream`。预算耗尽后必须收敛，不能无限重试。

Network quality policy 默认关闭。启用时必须配置 `minSamples`、`cooldownMs` 和 bitrate ladder。`networkQualityChanged` 只能驱动 TS/宿主动作，例如下一次启动的 bitrate hint 或宿主拥有的 transport restart；Rust runtime 不自动降码率、不自动换节点。

Observability/corpus 保持轻量入口。常规 CI 继续覆盖 Rust unit/integration 和 N-API smoke；真实音源 corpus 放在手动或 nightly。每次性能优化必须说明目标指标：queue ms、RTP underrun/late、RR loss/jitter/RTT、source retry/cache、CPU worker turn 或 stop/join latency。

## 代码库优化顺序

1. 依赖和文档一致性：Node 运行线、类型包、README/status/handoff 不漂移。
2. N-API 边界拆分：先拆 DTO/types 和 conversion，保持生成的 `index.d.ts` 不发生非预期变化。
3. Source 模块拆分：优先拆 `StreamingByteReader/Writer`、`HttpLiveStream`、bounded HTTP artifact/cache，避免同时改 retry/cache 语义。
4. Metrics 名称集中化：只做常量或小模块，不引入自定义 sink trait，不替代 `metrics` facade。
5. Runtime 内部拆分：等 profiling/corpus 证明需要后再做，不为“async 化”改变 actor/action/generation/task-tree。

当前已经完成 Node 24 types 对齐，把 N-API DTO、conversion/config parsing、event mapping/dispatch 从 `lib.rs` 拆出，把 source 的 live HTTP/streaming byte pipe 拆成单独 `source/live.rs`，收拢 source/runtime 指标名常量，并收敛 N-API runtime cleanup/shutdown 路径。后续不建议继续按文件大小机械拆分；只有当某块职责能独立测试、独立解释且不改变边界时再拆。若继续收缩 N-API，候选是 runtime action orchestration。

## 下一步建议

1. 为 TS live recovery 写最小 policy 单元测试，不新增 Rust playlist 语义。
2. 建一个小 corpus checklist，先记录失败样本和稳定 error code。
3. 把宿主 dashboard 字段固定下来，避免调参只看日志。
4. 等真实数据证明需要后，再评估 FFmpeg backend、async source IO 或 SIMD。

## 明确不做

- 不把 playlist、play mode、随机/循环、业务推荐下沉到 Rust。
- 不在 Rust 内做平台 URL 鉴权刷新或 voice gateway 协商。
- 不让 live stream 写入无限 tempfile。
- 不用 unbounded channel 解决卡顿。
- 不让 sender tick 临时 decode/encode。
- 不在 transport parser 内实现网络质量策略。
- 不在播放 runtime 内隐式改响度。
- 不为“架构完整”提前引入 FFmpeg、完整 WebRTC stack 或默认 SIMD。
