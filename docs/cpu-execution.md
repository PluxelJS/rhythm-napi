# CPU 执行与调度设计

本文定义实时 codec 工作如何使用 Tokio、为什么当前不引入 Rayon，以及什么证据足以重新评估该
选择。它描述的是当前设计理由和变更准入条件，不把某个线程池实现当成不可替换的产品能力。

## 结论

当前最符合播放需求的结构是：

```text
napi-rs Tokio runtime
├── async worker pool
│   └── command / actor / HTTP / HLS / timer / UDP / supervisor
│
└── Tokio blocking pool                         只负责承载可阻塞的 OS thread
    └── blocking producer admission             限制长期 codec producer 数量
        └── Rhythm CpuScheduler / CpuLease       限制真实 CPU 并发并实现 current 优先
            └── decode → resample → DSP → Opus
```

线程由 Tokio 创建和回收；`CpuScheduler` 不是线程池，它只决定 blocking worker 何时可以做计算。
实时 producer 当前不使用 Rayon，也不应在 `spawn_blocking` 内再嵌套 Rayon。Tokio async scheduler
的 work stealing 对大量短小 I/O future 有益，但不是 codec 公平性或优先级的来源。

只有出现独立、有限、纯计算的离线批任务，或者 codec 已经重构为绝不等待 I/O/queue 的短 turn，
Rayon 才重新成为候选。即使届时采用，也必须使用显式容量的专用 pool，并与实时播放共享同一份
CPU 总预算；不能直接使用 Rayon global pool。

## 必须满足的需求

执行器选择服务于以下约束，不能只比较吞吐量：

1. RTP/RTCP sender、Tokio timer 和 Node event loop 不能执行或等待 codec 同步计算。
2. 一个 generation 的 decoder、resampler、DSP 和 encoder 是有状态且严格有序的，只能由一个
   执行流依次推进；不同 generation 可以并发。
3. current 的首包、补充 queue 和恢复播放优先于 speculative next；next 不能耗尽全部 CPU、
   blocking worker 或 source 资源。
4. 等 source 字节、满 Opus queue、pause、promotion 或 event capacity 不属于 CPU 工作，等待时
   必须归还 CPU 名额。
5. stream、长期 blocking producer、真实 CPU 并发、preload、内存、磁盘和连接都必须有界。
6. cancellation、generation 替换和 shutdown 必须唤醒 async 与 blocking waiter，并在有限时间内
   收敛；不能依赖强杀正在运行的同步闭包。
7. 调度必须可观测，且优化应改善首包、underrun、lateness 或资源上界，而不只是提高离线吞吐。

以下不是当前目标：并行处理同一条连续音频的任意 chunk、追求离线转码最大吞吐，或提供硬实时
线程调度保证。Rubato 和 codec 的滤波器/容器状态跨 chunk 延续；任意分块并行会改变结果或要求
额外的边界算法。

## 当前运行方式

### 两类 Tokio worker

napi-rs 的 `tokio_rt` feature 为 Node Promise 创建一个 multi-thread Tokio runtime。项目目前没有
创建自定义 runtime，因此 async worker 数量、blocking pool 上限和线程回收策略使用所锁定 Tokio/
napi-rs 版本的默认值。这些上游默认值不是 Rhythm 的稳定合同；若未来自定义 runtime，必须重新
核算本节所有额度，尤其不能让 blocking pool 容量小于可能同时存活的 blocking producer和必要的
文件/清理任务。

当前 lockfile 中 Tokio 1.52.3 的默认 multi-thread runtime 使用每个可用 CPU 一个 async worker，
并允许最多 512 个额外 blocking thread；版本升级时这个数字必须重新核对。Rhythm 自己更小的
blocking producer/CPU额度才是媒体容量合同，不能依赖 512 作为可用业务并发。

`tokio::spawn` 承载：

- stream worker-event loop 与 producer/sender supervisor；
- reqwest 下载、shared flight、live/HLS 获取；
- channel、watch、semaphore、timeout 和 cancellation 等控制等待；
- RTP/RTCP sender 的 deadline、UDP I/O 和反馈接收。

这些 future 应短暂 poll 后返回 `Pending`，可以由 Tokio 多线程 scheduler 在 worker 间迁移和窃取。
任何同步 codec 或无界文件操作进入这里都会让同一 worker 上的 timer、UDP 和其他 stream 失去调度
机会。

`tokio::task::spawn_blocking` 承载一个 generation 的同步 codec 生命周期：Symphonia 打开/probe/
decode、Rubato resample、音量与 limiter、libopus encode，以及同步 reader/queue 桥接。它使用 Tokio
专用 blocking pool，不运行在 async worker 上。

### 三个不同的容量概念

不能把 blocking thread、blocking producer 和 CPU worker 当成同一个数量：

| 名称 | 提供者 | 表达的资源 | 默认策略 |
| --- | --- | --- | --- |
| Tokio blocking thread | Tokio runtime | 可执行同步闭包的 OS thread | 上游 runtime 默认，Rhythm 当前不配置 |
| `maxBlockingProducers` | Rhythm semaphore | 可长期存活的 codec producer | `clamp(maxCpuWorkers * 4, 64, 256)` |
| `maxCpuWorkers` | Rhythm `CpuScheduler` | 同时真正进行 codec 计算的 turn | `available_parallelism - 1`，至少1、最多256 |

next 还受 `maxBlockingPreloads = maxBlockingProducers / 4` 限制。多核时 next 重新获取 CPU lease
必须满足没有 current waiter，并至少给 current 留一个 CPU 名额。promotion 会让同一个 producer
以后按 current 身份竞争，不重建 decoder 或 encoder。

current 与 next 使用分离的 waiter 队列，每个 producer 拥有一个可复用的 condition variable，
permit 释放时只精确唤醒一个符合角色与预留规则的 waiter；若仍有空位，获得 permit 的 waiter 会继续
唤醒下一个。无竞争获取不进入队列，也不产生逐 turn 分配或引用计数修改。不能在每个 Opus frame
边界广播唤醒全部 producer，否则高并发下会把公平性开销变成 mutex 惊群。

current waiter 按其 Opus queue 的实时 `bufferedMs` 从低到高选择；相同深度保持 FIFO。等待 CPU 时
producer 不能补充 queue，而 sender 仍会继续消费，因此健康 current 会自然向低水位老化，不需要
额外 timer 或人为 priority boost。next 仍严格 FIFO，且只有没有 current waiter并满足 `N-1` 预留时
才能取得 permit。队列深度通过无锁只读投影提供，scheduler 不获取 Opus queue mutex。诊断仍同时
报告 current/next waiter。

因此允许存在：

```text
存活的 blocking producer > 持有 CPU lease 的 producer
```

这是有意设计。同步 decoder 可能正在等 growing spool/live reader，producer 也可能等满 queue 或
promotion；用可扩展 blocking thread 承载这些等待，再用更小的 CPU lease 控制真实计算，可以避免
慢 source 占走计算配额。代价是等待仍占用 OS thread，所以 blocking producer 另有硬上限。

### CPU turn 和主动让出

producer 获得 CPU lease 后推进有界的媒体 turn，当前默认 `decodeBatchMs` 为 80 ms。这个值是一次
turn 最多处理的媒体时长，不是 80 ms 墙钟时间，也不是不可抢占的 CPU 时间。

blocking closure 开始后必须先取得 CPU lease，再执行 Symphonia open/probe、seek、Rubato pipeline
准备和 libopus encoder 初始化。这样并发冷启动同样服从 CPU 总预算；progressive/live reader 在 probe
期间等待新字节时仍通过 observer 归还 lease。

每个 Opus frame 进入 duration-bounded queue 前，producer 先归还 lease，再执行可能阻塞的 send，
send 返回后重新参与调度。reader 在真正等待新 source 字节前也通过 wait observer 归还 lease。
pause、next prime、event backpressure 和 turn 结束同样不持有 lease。这样 current waiter 通常可以在
frame/turn 边界获得机会，而不是等待另一首歌完成。

`spawn_blocking` 已经开始运行后，Tokio 的 `abort` 不能强行终止同步闭包。正确停止路径始终是
`CancellationToken`、关闭 queue/source、唤醒 condvar 和在循环边界检查 cancellation；abort 只可
作为尚未开始任务的辅助行为，不能成为资源释放证明。

## 为什么当前不使用 Rayon

“Rubato、Symphonia 或 libopus 能否在 Rayon thread 上运行”不是主要问题。只要被移动的状态满足
`Send` 且库没有线程亲和性，技术上可以运行。真正不匹配的是任务生命周期和调度语义。

### 不并行同一 generation

同一 generation 的 demux/decoder、resampler滤波器、frame assembler和 encoder 都包含跨 frame
状态。输出还必须保持 generation、媒体位置和 Opus frame 顺序。把连续音频切成多个 `par_iter`
元素不能保持这些合同。即使某个阶段内部可并行，也需要证明额外复制、排序、边界状态和延迟对实时
播放有净收益。

### 多 generation 已经并行

不同 stream/current/next 本来就是独立 blocking producer，操作系统可把它们调度到不同核心；
`CpuScheduler` 已把真实并发限制在 `maxCpuWorkers`。Rayon 的 work stealing不会凭空增加 CPU，
也不会自动改善这种数量少、生命周期长、每个任务工作量相近的并行。

### 整个 producer 不是 Rayon job

Rayon 适合有限的纯计算任务。当前 producer 会同步等待 source、queue、pause 和 promotion。若把整个
producer 提交给固定大小 Rayon pool，几个慢 source 或满 queue 就能占住全部 Rayon worker；即使它们
逻辑上归还了 Rhythm CPU lease，Rayon 也没有空闲 thread 去运行新的 current。

### Rayon 不提供播放优先级

Rayon global/custom pool 默认不知道 current、next、promotion、preload 子额度或 `N-1` 预留。
另建 current/next 两个 pool 会把一个 CPU 总预算拆成互相竞争的上限，仍可能过度订阅；把优先级放回
外层则重新实现了当前 `CpuScheduler`，work stealing本身没有替代它。

### 不能嵌套两套 CPU 池

在 `spawn_blocking` 中调用 Rayon，或者让实时 codec 同时使用 Tokio blocking pool 与 Rayon global
pool，会让两套执行器都按整机 CPU 数扩张。结果可能是更多上下文切换、cache 抖动和 sender 迟滞，
而不是更高实时吞吐。实时链路必须只有一个 CPU admission authority。

## 候选设计比较

| 方案 | 结论 | 主要理由 |
| --- | --- | --- |
| codec 直接 `tokio::spawn` | 拒绝 | 同步 CPU/阻塞 read 会饿死 async timer、UDP 和控制任务 |
| 当前 `spawn_blocking` + `CpuScheduler` | 采用 | 容忍同步等待，同时提供独立 CPU 上限、current 优先和可控演进成本 |
| 整个 producer 提交 Rayon | 拒绝 | 长期等待占满固定 worker；没有播放优先级和 async 生命周期桥接 |
| `spawn_blocking` 内嵌 Rayon | 拒绝 | 双池过度订阅；同一 generation 又缺乏安全、有效的并行边界 |
| 多文件/clip 离线任务使用专用 Rayon pool | 条件采用 | 任务独立、有限、纯计算时 work stealing能平衡不规则工作量 |
| 短 codec turn 的专用优先级 executor | 条件评估 | 可严格绑定 thread 数，但前提是 turn 内绝不等待 source/queue |

## 当前设计的已知代价

当前选择不是“无需再测”的终点：

- `maxBlockingProducers` 下的 producer 可以各自占驻 blocking thread，即使同时计算的只有
  `maxCpuWorkers` 个。小 CPU、大并发或许会出现较多 parked thread、虚拟栈空间和调度开销。
- 默认 `maxCpuWorkers` 在可见并行度大于1时保留一个logical CPU给Tokio、Node和系统；这只是最小
  headroom。多个current饱和时仍会与Tokio worker、Node、内核和TLS/文件系统竞争物理CPU。
- Tokio blocking pool 还承载少量 tempfile、cleanup 等辅助操作。若长期 producer 把上游 pool
  容量占满，这些操作也会排队；Rhythm 自己的 semaphore 不能观察上游 pool queue。
- `music_stream.runtime.worker_turn_us` 记录整个 turn 的墙钟时间，其中可能包含 source 或 queue
  等待；应结合 `cpuCurrentHold`/`cpuNextHold`、`sourceWait` 和 `outputWait` 区分阶段，不能单独把它
  当作纯 CPU service time或证明 Rayon 会更快。
- `available_parallelism - 1` 是安全起点，不代表所有容器 CPU quota、SMT、共享宿主和延迟目标下的
  最佳配置。显式`maxCpuWorkers`可以覆盖默认，诊断会报告parallelism、maximum和实际headroom。

因此生产调优可以降低 `maxCpuWorkers` 给 async/Node/系统留下余量，也可以收紧 blocking producer，
但必须用相同流量和 source 组合比较首包、underrun、lateness 与 admission wait，不能仅看平均 CPU。

## 重新评估所需证据

满足以下任一现象时，可以重新比较执行器；单纯“Rayon 有 work stealing”不足以启动重构：

1. `cpuActive` 接近上限且存在 current waiter，但主机 CPU 明显未充分利用；能够证明 lease holder
   没有在做 source/queue/pause等待，仍无法解释这段空转。
2. blocking admission 已取得后，closure 实际开始显著排队，且与 Tokio blocking pool 饱和相关。
3. OS thread 数、栈虚拟内存或上下文切换随 blocking producer 增长，成为明确资源瓶颈。
4. `cpuCurrentWaiters`、首包 p95/p99、sender underrun/lateness 在 CPU 饱和时稳定相关，调整
   `decodeBatchMs` 或 `maxCpuWorkers` 无法解决。
5. 新增了大量独立、有限、纯计算的离线任务，现有 realtime admission 无法表达其低优先级预算。

`getResourceDiagnostics()` 已内置current/next blocking admission、从提交 `spawn_blocking` 到闭包开始、
current/next CPU lease wait/hold、source wait和output handoff的累计样本、总时间、最大值及log2近似
p50/p95/p99，不依赖宿主安装Rust metrics recorder。`metrics` histogram仍保留role/source标签，供已
安装recorder的宿主做更细分聚合。

评估执行器前仍需由进程/操作系统补齐 thread数、上下文切换、CPU time、迁核、NUMA remote access和
内存，并把这些数据与sender deadline lateness、underrun、drop关联。`cpu lease hold`是受准入保护的
墙钟时间；本地文件page fault等同步等待仍可能包含其中，不能冒充线程CPU time。

基准至少覆盖 1/2/4/8/16 核或等价 CPU quota，以及 current-only、current+ready next、慢渐进
URL、live/HLS、pause/promotion、cache hit/miss 和并发 shutdown。比较结果必须包含 p50/p95/p99
首包与 sender 指标，不能只报告每秒编码帧数。

## 条件演进方向：纯 CPU 短 turn executor

如果 parked blocking thread 已被证明是瓶颈，理想目标不是简单把长期 producer 从 Tokio 移到
Rayon，而是先改变 CPU 工作单元：

```text
async producer coordinator
  ├── await source readiness / queue capacity / pause / promotion
  ├── submit { CodecState, bounded input } with Current | Next priority
  ▼
bounded CPU executor
  └── decode/resample/DSP/encode one non-blocking turn
  ▼
TurnOutcome { CodecState, frames, need_input/end/error }
```

只有满足下面条件，这种 executor 才比当前设计更强：

- `CodecState` 可以安全在线程间移动并保持单 owner；
- turn 内所有 source read 都已证明不会阻塞，满 output也不会在 worker 中等待；
- encoded frame 或未完成 assembler状态有明确、有限的 owner；
- executor 原生支持 current 优先、next 的 `N-1` 上限、取消和有界 queue；
- panic、任务丢失和 coordinator取消都能归还 `CodecState` 或确定性终止 generation；
- 文件 codec 的同步 read/probe 不会把“纯 CPU executor”重新变成隐式 blocking pool。

当前 Symphonia reader和阻塞式 queue桥接尚不满足这些前提。为了换线程池而改写 decoder I/O边界，
风险高于预期收益；应先由上述观测证明 parked thread确实是主瓶颈。

达到前提后，Rayon custom pool可以作为底层候选，但不是默认答案。带有明确 current/next优先级、
有界提交队列和运行时诊断的专用 executor可能更贴合需求。无论底层选择什么，`RuntimeResources`
仍必须是进程内实时媒体 CPU预算的唯一 authority。

## 变更复核清单

修改 codec 执行方式、Tokio runtime、blocking/CPU额度或引入 Rayon前，必须逐项回答：

- 工作单元是否可能等待网络、磁盘、queue、pause、promotion 或 event capacity？
- 同一 generation 的可变 codec状态由谁唯一拥有，panic/cancel时如何回收？
- current 如何越过已饱和的 next，promotion如何原地改变优先级？
- 实际 OS thread、活跃 CPU turn、排队 job 和 preload各自的硬上限是什么？
- 是否存在第二个不受 `RuntimeResources` 控制的 global CPU pool？
- sender deadline和 Tokio async worker如何避免与 CPU饱和互相拖累？
- 哪些指标能区分 CPU service、executor queue、source wait和output backpressure？
- 哪组基准证明新设计改善用户可见延迟/可靠性，而不是只改善离线吞吐？
- cancellation和 shutdown是否在最慢 source、满 queue和所有 worker忙时仍能有限收敛？

任何一项没有明确 owner、上限、唤醒路径或验证方法，都不应进入实时播放实现。
