# 测试

## 常规验证

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
cd crates/music_stream_napi && npm test
```

## 当前覆盖

- actor generation、seek、switch、next promotion、跨曲空窗 pause/resume和错误事件。
- WAV/MP3 decode、44.1 kHz resample、真实 libopus encode。
- shared growing URL spool、bounded live byte bridge和 async HTTP source。
- localhost UDP 实际 RTP pacing。
- switch 和 preload promotion 后 sequence/timestamp连续。
- pause 停止 playout，resume继续同一 RTP session。
- 多路 current 与 next preload 并发时所有 RTP clock 前进，current 不被 preload 饿死。
- 短于 prebuffer 的 source 仍完整发送。
- live body 可超过 open timeout 持续读取，partial body 失败不进行字节流拼接重连。
- live body 的 chunk idle timeout 产生 `SOURCE_TIMEOUT`，但不会把直播总时长误当作超时。
- HTTP chunk 大于 live byte budget 时保持有界并完整切分。
- 有界 HTTP 对 404 不重试，对 500 使用全新 artifact 重试。
- live byte reader 只在 channel receive 边界释放/恢复 CPU lease。
- CPU scheduler 饱和时，取消中的 waiter 能在有限时间内退出。
- partial URL body 暂停超过 `ioTimeoutMs` 后仍从同一 pinned open/body future 恢复并发送 RTP；该场景重复运行验证 cancellation safety。
- paused 状态新增 next URL 不建连，resume 后才启动 preload。
- paused 状态 switch 到 URL 保持 Paused 且不建连，显式 resume 后才下载。
- 慢速有界 URL在服务器释放剩余HTTP body前已经产生首个RTP packet。
- 渐进 URL完整下载后提升为cache artifact；交付部分正文后断流不重试或拼接。
- 同内容并发URL subscriber只建立一个HTTP transfer；单个pause/cancel不阻塞其他subscriber，全部pause才冻结。
- preload饱和时current保留HTTP/tempfile admission；排队flight可被current提升优先级。
- shared transfer panic稳定广播，shutdown cleanup barrier等待tempfile删除和quota释放。
- source失败事件必须先于queue drained；N-API鉴权错误竞态重复运行验证稳定的 `SOURCE_AUTH_EXPIRED`。
- file current 暂停时取消 live next 连接，resume 后建立全新 live preload。
- final partial PCM frame 补零发送，连续损坏 packet 有显式错误预算。
- 注入 event callback panic 时媒体 action 仍然执行并产生 RTP。
- RTCP RR 解析和 N-API status 回写。
- N-API Promise 类型、URL 启动不阻塞 JS heartbeat、live HTTP。
- ReplayGain 和 source/transport 配置校验。

## 性能验收

2026-07-11 release Criterion 基线（当前开发机，5 秒 48 kHz stereo PCM）：

- pipeline + fake encoder：约 176 µs；
- pipeline + libopus：约 39.1 ms，即约 128 倍实时速度。

该结果表明当前 CPU 主成本明确集中在 libopus；在没有 allocation profile 证明收益前，不引入复杂的跨 sender payload pool。

新增优化不能只报告吞吐，至少同时观察：

- decode/resample/encode realtime factor；
- encoded queue 媒体时长；
- prebuffer 时间和 underrun；
- RTP deadline lateness；
- current 与 preload CPU 竞争；
- stop/switch/seek 收敛时间；
- Node event-loop delay。

## 仍需扩充的 corpus

- AAC/M4A、ALAC、FLAC、OGG/Vorbis。
- VBR、超大 metadata、损坏 packet和极短文件。
- mono、multichannel、44.1/48/96 kHz。
- 慢 HTTP、无 Content-Length、中途断流和长时间 live stall。
- 10/50 路并发和持续数小时 soak。
