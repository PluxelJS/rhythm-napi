# 依赖边界

依赖按系统边界选择，而不是按便利性堆叠。

| 依赖 | 采用理由 | 使用边界 |
| --- | --- | --- |
| `tokio`, `tokio-util` | async task、UDP、timer、channel、cancellation | 不运行 codec/DSP，不用 runtime worker 承担同步清理 |
| `reqwest` | async HTTP、连接池、TLS | 禁止 blocking client；业务鉴权和 URL 解析留给宿主 |
| `symphonia` | 容器 probe、demux、decode、seek | 只在受控 blocking worker 中调用 |
| `rubato` | 高质量 sample-rate conversion | 只处理 PCM，不负责调度或 buffering |
| `opus` | libopus 安全封装 | 固定媒体格式，编码不进入 sender task |
| `rtp`, `rtcp`, `webrtc-util` | 标准协议对象与 marshal/parse | 不承载 pacing、网关协商或 protection 策略 |
| `bytes` | immutable payload 和低成本 slice | 不作为无界缓存容器 |
| `tempfile`, `lru` | 完整 HTTP artifact 生命周期与内存索引 | 磁盘容量由 runtime semaphore 约束 |
| `napi`, `napi-derive` | Node Promise 和 bounded ThreadsafeFunction | 不旁路 actor，不阻塞 JavaScript event loop |
| `metrics`, `tracing` | 宿主可选观测 facade | library 不安装全局 recorder/subscriber |
| `rand` | RTP sequence/timestamp 随机初值 | 不用于内容身份或安全密钥生成 |

## 准入原则

新增依赖必须解决明确的模块边界，许可证和维护状态可接受，并能在测试中验证其失败语义。依赖
不能把 playlist/provider 策略带入 Rust，不能引入隐藏线程池或无界缓存，也不能迫使实时 sender
执行阻塞工作。

若标准库或现有依赖已经能清晰表达所有权和取消语义，不为少量语法便利增加新依赖。若替换 codec、
HTTP 或协议库，必须先证明它能保持当前 source、blocking-worker 和 RTP-session 边界。
