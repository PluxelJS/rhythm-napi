# 验证策略

测试的目的不是覆盖函数数量，而是保护跨任务、跨 generation 和跨协议的设计不变量。单元测试负责
纯状态与原语，集成测试负责真实 task/HTTP/UDP 时序，Node 测试负责 Promise、类型和宿主边界。

## 必须通过的验证

```sh
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps

cd crates/music_stream_napi
npm test
```

`--all-targets` 会运行 Criterion debug harness，确保 fake encoder 和 libopus 两条 pipeline 至少能够
完整构建与执行。性能采样应另外使用 release benchmark。

## 不变量矩阵

| 设计风险 | 必须验证的行为 |
| --- | --- |
| actor 与异步结果竞争 | generation 过滤、staged commit、callback failure 不跳过 action |
| next 抢占 current | CPU、blocking、HTTP 和 tempfile 都保留 current 能力；live 不允许作为 next |
| stream/status/registry增长 | `maxStreams`硬 admission、stopped LRU和dead weak key周期清理 |
| pause 被实现成 cancel | URL 保持同一 response；超时不计算暂停时间；resume 不重置 RTP clock |
| shared download 相互阻塞 | 单 subscriber pause/cancel 不影响 active follower；全部 pause 才冻结 |
| partial response 被伪装完整 | 正文交付后不重试拼接；partial 不进入 cache |
| codec 阻塞 realtime | source wait 和 queue wait 释放 CPU lease；sender 独立按 deadline 发送 |
| queue 无界或重复 | 唯一 Opus queue 按媒体时长限流；next prime 后停止生产 |
| RTP session 被 track 重置 | switch、seek、promotion 后 sequence/timestamp 连续 |
| sender stall 形成 burst | 超限丢旧媒体；timestamp 跳过、sequence 只统计实际包 |
| 错误被 EOF 覆盖 | source/auth/timeout terminal 优先；failure event 先于 drained |
| 任务 panic 或关闭泄漏 | producer/shared-flight panic 事件化；stop/shutdown 有 deadline 与 barrier |
| sender panic 后假 Playing | supervisor立即发布active generation failure并收敛runtime |
| Node event loop 被阻塞 | URL 启动期间 heartbeat 继续；所有等待型方法返回 Promise |

## 测试层次

### 纯单元测试

- actor 的状态、generation、promotion、pause 意图和错误策略；
- PCM frame assembly、downmix、gain/limiter、ReplayGain；
- decoder 错误预算、resample 和真实 libopus frame；
- Opus queue 的容量、关闭、drain 和 stale drop；
- RTP packetize、RTCP parse 和 quality window；
- PauseGate、CPU scheduler、resource validation。

这些测试不依赖 wall-clock 网络，应尽量保持确定性。

### 并发与 source 测试

- growing spool 多 reader 都从 offset 0 获得相同字节；
- artifact/progressive resolver 共用一次 HTTP flight；
- preload 饱和时 current 仍获得 admission；
- paused transfer 不建新连接，resume 后继续；
- HTTP retry 使用全新 artifact，partial body 不拼接；
- live open/idle timeout、chunk split 和全局 byte budget；
- tempfile 删除完成后才释放 quota，shutdown barrier 等待真实清理。

涉及 race 的短测试应在本地或 CI 中重复运行，尤其是 source terminal、pause/cancel 和 task panic
路径。重复通过不能证明没有 race，但能暴露依赖不安全轮询或错误事件顺序的实现。

### RTP/HTTP 集成测试

localhost UDP 测试必须观察真实 packet，而不是只检查内部计数：

- 首包 marker、SSRC、sequence 和 timestamp；
- pacing 不 burst；
- 文件、渐进 URL 和 finite live 能完成 decode → Opus → RTP；
- next promotion、switch 和 seek 不重建 session；
- pause 期间停止 playout，resume 从同一 session 继续；
- RTCP mux/non-mux、SR 和 RR status；
- 慢 source 不占用 sender deadline。

### N-API 测试

Node 层验证生成的 `index.d.ts` 能被 TypeScript 消费，并覆盖 lifecycle Promise、配置校验、事件
callback/补偿队列、鉴权刷新信号、批量 status、ReplayGain 和 shutdown。Rust 单元测试不能替代
这些 ABI 与 event-loop 契约。

## 性能判断

离线 pipeline 吞吐只是一个下限。当前开发机的 release Criterion 参考值（5 秒 48 kHz stereo
PCM）为：fake encoder 约 176 µs，libopus pipeline 约 39.1 ms，约 128 倍实时速度。该数字说明当前
样本中主要 CPU 成本位于 libopus，但不能直接推导生产并发容量。

任何性能改动至少同时记录：

- activation-to-prebuffer；
- decode/resample/encode realtime factor；
- CPU 和 admission wait；
- Opus queue duration 与 underrun；
- sender lateness、drop 和恢复次数；
- pause/seek/switch/stop 收敛时间；
- tempfile/live byte 峰值；
- Node event-loop delay。

只提升离线吞吐但增加首包、尾延迟、内存或取消时间的改动不算优化。

## 生产验证

代码库内测试保护语义，部署前还需要真实 corpus、规模和故障注入：多格式/VBR/损坏媒体、慢
DNS/TLS、无 Content-Length、磁盘满、UDP stall、多路 current/preload 竞争和多小时 soak。结果应
用于设置资源默认值，而不是在单元测试中硬编码某台机器的容量结论。
