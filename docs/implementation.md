# 实现契约

## Source

- `file`：async metadata 校验，CPU worker 打开 decoder。
- `url`：async reqwest body写入growing tempfile，stream decoder从独立blocking reader渐进读取；完整响应后才进入artifact LRU，非零seek保持完整文件路径。
- 同一内容身份的URL miss进入共享flight；每个subscriber拥有独立reader和pause/cancel状态，
  transfer由registry监督，panic、HTTP错误和完成状态统一广播。
- `live`：async reqwest body进入 bounded byte bridge，Symphonia 从 blocking `Read` 消费；`openTimeoutMs` 只限制建连与响应头，`idleTimeoutMs` 限制相邻 body chunk 的无数据时间，两者都不限制直播总时长。
- live 只允许在尚未交付媒体字节时重试；partial body 断开必须失败，禁止把新响应拼接进旧容器流。
- reqwest chunk 大于 byte bridge 总预算时先切分，再按实际字节申请 permit，不会等待不可能满足的 semaphore 配额。
- 有界 URL 只重试连接/超时、408、429 和 5xx；4xx、字节上限及本地 tempfile 错误立即失败。
- bounded URL 与 live 共用进程级 reqwest Client/TLS 连接池；每个 producer 不再重复创建连接池。
- bounded URL 下载必须先获得 Streamer级 async admission；排队完成后再次检查 cache，等待期间的 pause/cancel不占下载槽。
- tempfile 创建在 blocking worker；artifact 最后一个引用释放时只投递到专用清理线程，cache hit/eviction/shutdown 不在 Tokio worker 上执行 metadata 或 unlink；shutdown通过barrier等待清理完成。
- 401/403 统一映射为 `SOURCE_AUTH_EXPIRED`。
- partial artifact 不进入 cache。
- 渐进 URL 一旦向decoder交付正文，后续失败不得重连拼接；失败必须显式通知byte reader，不能伪装成正常EOF。

## Audio

- decoder 输出 backend 自然 chunk。
- channel 统一为 stereo，sample rate统一为 48 kHz。
- FrameAssembler 直接借用 decoder chunk 生成 20 ms PCM frame，只复制不足一帧的尾部。
- volume 与 gain 在 Opus 前应用，正增益启用 soft limiter。
- Opus payload 最大值必须小于 RTP MTU 减固定头。
- source end 时不足 20 ms 的最后 PCM frame 补零后编码，避免静默丢弃尾音或让极短音频零输出。

## Error policy

- 有界 HTTP 仅对连接/超时、408、429 和 5xx 重试，每次尝试使用全新 partial artifact。
- 401/403 不自动重试，映射为 `SOURCE_AUTH_EXPIRED`，由 actor 产生 `sourceRefreshNeeded`。
- live 只在首个媒体字节前重试；partial body 失败直接终止，禁止拼接响应。
- live body 超过 `idleTimeoutMs` 无新字节时报告 `SOURCE_TIMEOUT`；仅首字节前可按 retry budget 重试。
- Symphonia 允许最多 32 个连续损坏 packet；成功 frame 会重置预算，超限报告 `DECODE_ERROR`。
- 完全没有可解码 PCM 的 source 报告 `DECODE_ERROR`，不会伪装成正常 track end。
- RTP marshal/send 失败是当前 generation 的终止错误；不会在 sender 内重试或 burst 补发。
- current producer 或 sender 失败时先 deactivate 该 generation 并丢弃 backlog，再 promotion 或回到 Idle。
- producer外层保留queue sender lifetime guard；失败事件先进入worker channel，随后才允许queue变为drained，禁止 `CurrentEnded` 抢先覆盖真实错误。
- source和decoder近同时失败时优先保留非取消型source terminal code，鉴权/timeout不能降级成容器EOF或DecodeError。
- switch/seek/stop 的 cancellation 会穿透 pause gate、HTTP transfer、CPU scheduler 和 Opus queue；stale generation error 由 actor 丢弃。
- actor 不做“连续三次失败”之类 playlist 策略；每次失败都确定地 promotion 或回到 Idle，后续选择由 TypeScript 决定。
- runtime action 先执行再投递事件；callback panic 被隔离并计数，不能跳过 pause/switch/stop 等媒体动作。

## Session

- public command 和 worker event 必须经同一个 orchestration mutex 串行处理。
- actor 只输出 action/event/status，不持有 socket、decoder或 task handle。
- stale generation event 必须无副作用。
- next 未 ready 不能 promotion。
- Paused 是跨 seek/switch/refresh 和 current/next 空窗持续存在的播放意图；只有显式 Play 才恢复 source、CPU 和 RTP。
- CPU permit、blocking producer、preload、HTTP download和 URL/live streaming bytes同时受共享资源预算约束；preload额度必须小于 blocking producer总额度。

## Transport

- sequence 和 timestamp随机初始化并由长期 sender 独占。
- track 切换不重置 RTP clock。
- RTP/RTCP marshal 使用 `rtp`、`rtcp` 和 `webrtc-util`。
- 配置了 protection 但没有 protector 时 fail closed，禁止静默明文。
- packet send 错误不得触发同步 decode 或 backlog burst。

## 禁止模式

- Node 方法中 `block_on`。
- async task中调用 blocking reqwest、文件读写或 codec。
- sender 与 decoder 在同一个循环或线程。
- per-track sender、sequence 或 timestamp。
- unbounded channel、无限 tempfile、无限事件队列。
- 在 Rust 内推导 playlist 下一首或自动业务重试动作。
