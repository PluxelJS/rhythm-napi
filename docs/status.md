# 能力边界

## 当前系统保证

- 本地文件、有界 HTTP 文件和 live HTTP 三种 source 模型。
- 有界 URL 的渐进式 growing spool、内容+传输策略 single-flight、服从当前字节上限的
  完整 artifact LRU 和磁盘配额。
- Symphonia decode、stereo normalize、Rubato 48 kHz resample、volume/gain/limiter 和 libopus。
- 每 stream 一个长期 RTP/RTCP sender，seek、switch 和 promotion 保持 session clock 连续。
- current/next、预载、显式暂停、source refresh、ReplayGain 建议和 RTCP quality snapshot。
- HTTP/UDP/timer/Node async 边界与受控 blocking codec worker。
- CPU、blocking producer、HTTP/live、live bytes、Opus queue、tempfile/cache 和事件容量治理。
- `maxStreams`、bounded stopped-status LRU和dead-flight key清理。
- generation 过滤、staged commit、task supervision、deadline 和确定性 shutdown。
- 迟滞上限、过期媒体丢弃、无 burst pacing 和可观测的首包/queue/RTCP 指标。

这些是当前设计契约，不是可选目标。

## 明确属于宿主

- playlist、循环/随机、推荐、跳过和连续失败策略；
- provider URL 解析、签名、鉴权刷新和凭据生命周期；
- voice gateway 协商、节点选择、迁移和自动降码率；
- 跨进程 cache、持久 metadata、TTL 和内容版本规则；
- 如何响应 `nextNeeded`、`sourceRefreshNeeded` 与网络质量事件。

把这些能力加入 Rust 会混淆媒体执行与产品策略，除非系统边界被明确重新定义。

## 尚需外部输入的生产扩展

### RTP protection

当前非 plaintext 配置 fail closed，但没有具体平台 packet protector。实现需要明确目标协议、密钥
生命周期、nonce/rollover、RTCP 保护和网关兼容性，不能在缺少平台契约时猜测。

### 真实媒体与规模验证

仍需用生产 corpus 和环境验证 AAC/M4A、ALAC、FLAC、OGG/Vorbis、VBR、超大 metadata、损坏文件、
慢 DNS/TLS、磁盘满、UDP stall 以及 10/50 路多小时运行。现有测试证明语义，不替代容量规划。

### 媒体性能待办

后续优化按生产 corpus 和分阶段 profile 排序，而不是默认从手写 SIMD 开始：

- 为 MP3、AAC、FLAC、M4A/MP4 分别建立 decode、resample、DSP、Opus 和 allocation 基线；
- 验证 fast-start M4A/MP4 的安全渐进 probe，并在容器索引、Range 语义和响应身份可证明时再扩展；
- 比较 Opus complexity 10/8/6/5 的 CPU、音质、sender lateness 和并发尾延迟，再决定是否暴露配置；
- 对真实 44.1 kHz 曲库比较 Rubato sinc 参数的 CPU/音质，不以合成吞吐单独改变默认质量；
- profile 证明必要后再增加 Opus payload pool、更多 PCM buffer 复用和 gain/downmix/limiter SIMD；
- 用 10/50 路长时间运行验证 allocator 压力、RSS、underrun、取消时间和 Node event-loop delay。

这些优化不得重新引入第二层 queue、跨 sender payload 复制或更差的取消语义。

### Loudness 分析

当前支持宿主提供 ReplayGain metadata 后计算安全建议，不进行 EBU R128 离线扫描。是否加入扫描
取决于 metadata 来源、预载成本和产品一致性要求。

### 可选 Range seek

非零 URL seek 当前等待完整 artifact，语义正确但可能增加未缓存 seek 延迟。只有在按容器验证
索引/offset、服务器 Range 语义和响应身份后，才能增加局部 Range 优化；不能把任意 byte offset
当作音频时间。

## 演进判断标准

未来改动应优先改善可测量问题，而不是增加抽象层。值得进入底层的改动至少满足一项：

- 降低 activation-to-prebuffer 或 sender lateness；
- 降低明确 profile 中的 CPU/allocation；
- 收紧资源上限或取消/失败收敛；
- 扩展经过真实 corpus 验证的格式或协议能力；
- 让宿主获得此前无法表达的必要策略输入。

同时必须保持 architecture 中的所有权与 RTP 时钟不变量。
