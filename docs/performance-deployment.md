# CPU、NUMA 与 I/O 部署调优

本页定义生产部署如何给 Rhythm 划分硬件，以及何时才值得改变执行器或 I/O backend。容量目标是降低
首包、sender lateness 和 underrun 的尾延迟，不是让进程长期显示 100% CPU。

## CPU 预算

`maxCpuWorkers` 的默认值来自进程可见 `available_parallelism`，它通常包含 SMT logical CPU，也不会自动
为 Node event loop、Tokio async workers、TLS、磁盘、内核和 IRQ 保留余量。生产必须以实际 cpuset 为
边界测试，而不是按宿主机总核数填写。

建议从以下矩阵起步：

- 只创建一个长生命周期 `Streamer`；每个实例拥有独立的整套 CPU/resource预算；
- 在进程可见 CPU 中为非codec工作保留至少一个logical CPU，大于8个logical CPU时同时比较保留1/2个；
- 对 SMT 主机同时测试物理核数、物理核数加部分 sibling和全部logical CPU，不能假设超线程线性扩展；
- `maxBlockingProducers`按允许同时存活的current、next和慢source设置，不因CPU worker较少而盲目保留
  64/256个parked thread；`maxBlockingPreloads`只覆盖确实需要的预载；
- 用相同流量比较`blockingCurrentAdmissionWait`/`blockingNextAdmissionWait`、`blockingStartWait`、
  `cpuCurrentWait.p95Us/p99Us`、`cpuCurrentHold`、首包、sender `maxLatenessMs`、underrun、Node
  event-loop delay、thread数和context switch。

若 `cpuActive`长期等于上限且current waiter增长，同时整机CPU仍明显空闲，先确认lease holder是否在本地
文件page fault等未拆分同步等待中，再考虑改变executor。若CPU已饱和且sender/event loop迟滞，应该降低
`maxCpuWorkers`，而不是继续增加blocking thread。

## NUMA

Tokio blocking pool与async pool都由napi-rs runtime拥有，Rhythm当前不在closure中调用
`sched_setaffinity`。这是刻意约束：Tokio会复用blocking thread，closure内设置的affinity会泄漏给后续
tempfile、清理或其他crate提交的工作；只绑定CPU不绑定memory还可能增加remote access。

多socket/多NUMA节点优先采用进程级分片：

1. 每个NUMA node启动一个Node进程和一个`Streamer`；
2. 同时约束CPU cpuset与memory node；
3. 把流在宿主层稳定分配到对应进程，避免同一stream跨node迁移；
4. `maxCpuWorkers`只按该进程cpuset核算；
5. 用`numastat -p`、`perf stat`和`/proc/<pid>/status`验证remote memory、迁核和allowed list。

裸机可在经过验证的Linux服务脚本中使用等价于下面的启动策略：

```sh
numactl --cpunodebind=0 --membind=0 node server.js
```

容器应使用编排器的CPU Manager/Topology Manager或明确cpuset与memory policy；不要只设置CPU quota后
假设获得了NUMA locality。单NUMA机器不应增加应用级亲和代码。

只有进程级分片仍被remote access、迁核或共享LLC证明限制时，才进入“Rhythm拥有专用codec thread”的
设计。那时thread创建、pin、memory first-touch、current/next优先级和shutdown必须由同一个executor负责。

## 发布指令集

正式npm包必须保持目标平台基线兼容，不能全局使用`target-cpu=native`。当前release已经启用单codegen
unit与thin LTO；libopus可使用自己的runtime dispatch，但通用Rust代码仍受target基线约束。

固定硬件的私有部署可以另建native二进制并与正式通用包做同语料对比：

```sh
RUSTFLAGS='-C target-cpu=native' npm run build
```

公开发布若需要更高指令集，应增加显式的x86-64-v3等分档包或对已profile出的纯Rust热点使用runtime
multiversion；不能静默替换baseline。验收必须同时报告CPU、首包和sender尾延迟，并验证相同音频输出合同。

## io_uring

当前Linux网络走Tokio/Mio，`tokio::fs`使用blocking工作，Symphonia使用同步`Read`。这不等于遗漏了一个
免费加速开关：每路RTP通常只有50 packet/s，pacing又禁止burst；codec/resample/Opus往往比socket syscall
更早成为瓶颈。容器安全策略和旧内核也可能禁用或限制io_uring。

只有满足下面证据时才增加Linux可选io_uring backend：

- `iostat`/eBPF/perf显示growing spool写入或大量独立文件I/O占主导，并与首包/underrun相关；
- `blockingStartWait`与Tokio文件任务排队相关，而不是codec producer或CPU lease饱和；
- 同一workload下io_uring降低p95/p99且没有扩大内存、取消时间或文件完整性风险；
- fallback继续覆盖不支持io_uring的Linux、Windows和macOS。

首个候选应是有界spool writer的批量写/flush，而不是把同步Symphonia reader套进async/sync桥。若传输
syscall真的成为瓶颈，再分别比较io_uring、datagram batching和socket布局；不能以“使用了io_uring”本身
作为优化验收。

## 最小生产验收

每次改变CPU额度、NUMA布局、编译指令集或I/O backend，至少覆盖1/2/4/8/16 CPU quota、10/50路、
current-only、current+next、cache hit/miss、慢渐进URL、live/HLS、pause/promotion和并发shutdown。记录：

- activation到source/probe/first Opus/prebuffer/first RTP的p50/p95/p99；
- resource diagnostics的blocking/CPU/source/output分阶段摘要；
- sender lateness、underrun、drop、packet coverage与Node event-loop delay；
- process CPU time、RSS、thread数、voluntary/involuntary context switch、page fault和NUMA remote access。

只有用户可见尾延迟或资源上界改善、且取消与实时语义不退化，优化才可以成为默认值。
