# 底层重构路线

本文记录底层重构的完成状态和仍需外部产品输入的生产扩展。当前架构契约仍以
`architecture.md`、`runtime.md` 和 `implementation.md` 为准。

## 最终目标

构建一个长期运行、低首帧延迟、全局资源有界的音乐媒体节点：异步获取 source，
在受控 CPU 执行器中完成解码、声道归一、重采样、DSP 和 Opus 编码，再由每个 stream
唯一的长期 sender 按实时 deadline 发送 RTP/RTCP。

必须持续满足以下不变量：

- HTTP、UDP、timer 和 Node 等待不得阻塞 Tokio worker或 JavaScript event loop。
- codec、DSP 和 resample 不在 async worker 上执行，也不伪装成 async 计算。
- 每个 stream 只有一个 RTP session owner；track generation 不拥有 RTP clock。
- 所有 queue、下载、临时文件、live bridge、CPU worker和事件都必须有局部及全局上限。
- current 优先于 preload，但 preload 不得永久饥饿。
- pause、cancel、seek、switch、failure 和 shutdown 都必须有确定的资源收敛语义。
- 不为兼容旧接口保留重复状态、空实现或双轨路径。

## 阶段一：全局资源治理与任务监督

当前进度：已完成。共享 `RuntimeResources` 已纳入 CPU worker、blocking producer、preload、
bounded HTTP 下载、live连接/总缓冲和 artifact cache；sender/producer task result、停止超时、
sender command deadline及 UDP send deadline已经进入监督路径。in-flight tempfile/cache预算、
actor staged commit、原子替换及失败后的 terminal收敛也已完成。更大规模故障注入属于生产验证，
不再是当前架构重构的缺口。
URL资源 admission已按 tempfile、HTTP、growing spool、blocking worker的顺序收敛，避免上游等待占住
稀缺下游资源。

### 资源治理

引入进程级 `RuntimeResources`，由所有 `StreamRuntime` 共享，统一管理：

- active current CPU job；
- active preload CPU job；
- parked live decoder；
- bounded HTTP 下载并发；
- live bridge 总字节预算；
- in-flight tempfile和 cache 磁盘预算。

实现同时限制 active CPU job、blocking producer和 parked live decoder；等待 HTTP、tempfile、
live admission或 CPU turn 不占用更下游的稀缺资源。admission wait、CPU wait、降级和取消延迟
进入运行指标。

### 结构化任务监督

- sender、current producer和 next producer都必须保留 task handle及 terminal result。
- sender command、RTP send、RTCP send和 stop acknowledgement必须有明确 deadline。
- producer stop必须区分正常取消、退出超时和 task panic，禁止静默吞掉异常。
- shutdown必须尝试收敛全部任务、清理 cache，再聚合返回错误。
- runtime action失败后，actor状态不得继续伪装成动作已完成。

验收条件：

- 50 路并发不会产生与 producer数量线性增长的无界 blocking threads。
- current 在 preload饱和时仍能在限定时间内获得 CPU。
- 注入 sender/producer panic、send stall和 stop timeout后，API 返回稳定错误且没有遗留任务。

## 阶段二：实时延迟治理

当前进度：默认 100 ms最大迟滞、过时帧丢弃、RTP timestamp/sequence语义和恢复指标均已
实现，并有真实 UDP调度停顿测试。后续仅在产品确实需要完整音频模式时再增加 policy枚举。

当前 sender禁止 burst，但严重迟到后会从 `now` 重新按 20 ms pacing，已经积累的延迟不会
自动消失。增加显式 playout latency policy：

- 小幅迟到继续发送；
- 超过 `maxPlayoutLatenessMs` 时丢弃最旧 Opus frame；
- RTP timestamp跳过被丢弃的媒体时间，sequence只对实际发送 packet递增；
- 可选择低延迟丢帧模式或完整音频但允许延迟模式；
- 记录最大 lateness、丢帧数、恢复次数和恢复后的 queue depth。

验收条件：一次 200 ms sender stall后，低延迟模式能在配置窗口内恢复目标延迟，且不会
burst、倒退 timestamp或重置 RTP session。

## 阶段三：渐进式 URL spool

当前进度：已完成。零起点 URL 使用可多读的 growing tempfile渐进解码，不再复制URL内存bridge；
完整响应后才提升为 cache artifact。cache hit和非零 seek使用完整的 seekable file artifact；HTTP已交付
任何正文后的失败禁止重连。Range seek可在有真实首延迟数据和容器级校验后作为独立优化加入，
不是当前正确性或架构收口的前置条件。

已完成的 growing spool 数据流：

```text
HTTP response
  -> bounded growing spool
       -> current decoder reader
       -> completion/cache promotion
```

- current在容器 probe和 prebuffer所需字节到达后立即解码。
- 下载完成后将 spool原子提升为完整 cache artifact。
- decoder读取尚未到达的区间时等待 source progress，不占 CPU permit。
- pause冻结 HTTP、spool reader、decoder和 sender；cancel唤醒所有等待者。
- 零起点播放使用 growing reader；非零 seek在完整 artifact上执行，绝不把未完成spool伪装成
  任意可寻址文件。
- partial artifact和不同 HTTP response绝不拼接成一个伪完整文件。

验收条件：大文件的首个 RTP packet不再等待完整下载；完整下载后的 seek和 cache hit仍保持
当前语义。

## 阶段四：共享下载与 cache身份

当前进度：已完成。registry-owned single-flight、多reader、subscriber pause/cancel聚合、current优先级
提升、transfer panic监督和cache promotion均已实现。共享key使用业务稳定track id，不使用临时
签名URL。

解决相同内容并发 miss造成的重复下载：

- single-flight以内容身份而不是临时签名 URL为 key；
- follower订阅同一 download progress或完整 artifact；
- 单个 follower pause不能阻塞其他 active subscriber；
- 所有 subscriber暂停时才冻结 transfer，全部取消时终止 transfer；
- cache key由调用方提供的稳定内容身份承担版本区分；cache受内存条目、磁盘字节预算和异步cleanup
  barrier约束，不在 runtime内猜测 provider TTL。

禁止用简单 keyed mutex实现，因为 paused leader会把 active follower一起阻塞。

## 阶段五：基于 profile 的热路径优化

只有 allocation/CPU profile证明收益后才实施：

- recyclable Opus payload slab，消除每 20 ms `Bytes::copy_from_slice`；
- decoder PCM chunk和 Rubato output buffer复用；
- RTCP receive buffer避免小额复制；
- 根据并发负载配置 Opus complexity、FEC、DTX和 packet-loss percentage。

现有 RTP scratch、PCM frame借用和单一 Opus queue必须保留，不能重新引入第二层 encoded
queue或跨 sender payload复制。

## 阶段六：网络保护与生产验证

- 安装实际平台 RTP packet protector；非 plaintext继续 fail closed。
- 明确 RTCP feedback到 bitrate/FEC策略的边界，playlist和节点切换仍留在 TypeScript。
- 建立 AAC/M4A、ALAC、FLAC、OGG/Vorbis、VBR、损坏文件和超大 metadata corpus。
- 运行 10/50 路、多小时 soak及 DNS/TLS stall、磁盘满、UDP backpressure、任务 panic等
  chaos场景。

## 每阶段通用验证

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --all-targets`
- `cargo doc --workspace --no-deps`
- `npm test`
- `git diff --check`
- 记录首帧时间、CPU realtime factor、blocking thread数量、queue媒体时长、sender lateness、
  stop收敛时间和 Node event-loop delay。
