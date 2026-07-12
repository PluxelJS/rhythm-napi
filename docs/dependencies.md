# 依赖

- `tokio` / `tokio-util`：runtime、UDP、timer、channel、取消。
- `reqwest`：纯 async HTTP；禁止启用或使用 blocking client。
- `symphonia`：demux/decode backend。
- `rubato`：48 kHz sample-rate conversion。
- `opus`：libopus 安全封装。
- `rtp` / `rtcp` / `webrtc-util`：协议对象和 marshal。
- `bytes`：immutable Opus/RTP payload。
- `tempfile` / `lru`：seekable HTTP artifact和进程内完整文件 cache。
- `napi` / `napi-derive`：Node Promise API和 ThreadsafeFunction。
- `metrics` / `tracing`：宿主可选观测；library不安装全局 recorder或 subscriber。
- `rand`：RTP sequence/timestamp随机初值。

依赖准入要求：解决明确边界、许可证清晰、能被测试覆盖，并且不能把业务策略或阻塞 I/O 带入实时 sender。
