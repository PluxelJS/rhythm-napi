# 当前状态

## 已实现

- 本地文件、渐进式有界 HTTP spool/cache、live HTTP 三种 source。
- Symphonia decode、Rubato resample、DSP、libopus。
- async HTTP/UDP/timer 和真正的 N-API Promise 边界。
- 每 stream 一个长期 RTP/RTCP sender。
- seek、switch、promotion 保持 sequence/timestamp连续。
- bounded live byte bridge、唯一的 duration-bounded Opus queue和跨 generation/跨曲空窗的暂停背压。
- current 优先的统一 CPU turn scheduler。
- Streamer级 CPU、blocking producer/preload、current优先HTTP下载、live连接、live streaming byte及 tempfile/cache磁盘总预算。
- current/next、预载、promotion、volume/gain、ReplayGain。
- RTP/RTCP mux和非 mux，SR/RR、RTT/jitter/loss snapshot和质量事件。
- generation 过滤、显式 stop/shutdown和事件补偿队列。
- sender/producer结构化任务监督、producer panic即时事件化、command/send/stop deadline和异常收敛。
- sender最大实时迟滞、时间轴正确的过时帧丢弃和恢复指标。
- actor staged commit、原子producer替换及 action失败后的确定性 terminal收敛。
- HTTP byte limit、retry/backoff、完整 tempfile LRU、异步运行时外的 tempfile 创建/清理和鉴权错误映射。
- registry-owned共享URL flight、可多读growing spool及subscriber级pause/cancel聚合。
- shutdown tempfile cleanup barrier，返回时磁盘文件与quota已经收敛。

## 明确不属于 Rust runtime

- playlist、随机/循环、推荐和跳转策略。
- provider URL 刷新与鉴权业务流程。
- voice gateway 协商、节点切换和自动降码率策略。
- 跨进程持久缓存配额与 TTL。

## 尚未完成

- 具体平台 RTP packet protection 算法；当前非 plaintext 配置 fail closed。
- 全格式真实曲库 corpus 和高并发 soak 数据。
- 基于 profiling 的 Opus payload pool或 SIMD；PCM frame 已直接借用 decoder chunk，Rubato 与 RTP scratch 已复用。
- EBU R128 离线分析；运行时只提供显式 ReplayGain建议。
