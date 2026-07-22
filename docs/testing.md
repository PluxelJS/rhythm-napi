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

`criterion_media`把确定性真实fixture拆成decode和完整pipeline，并独立测量44.1→48 kHz Rubato与
非静音Opus。2026-07-22同一开发机的初始中位数为：

| 阶段/fixture | 媒体时长 | 运行时间 | 约实时倍数 |
| --- | ---: | ---: | ---: |
| Rubato mono 44.1→stereo 48 kHz | 5 s | 22.2 ms | 225x |
| Opus stereo 48 kHz complexity 10 | 5 s | 35.1 ms | 142x |
| MP3完整pipeline | 0.25 s | 5.07 ms | 49x |
| FLAC完整pipeline | 0.20 s | 3.98 ms | 50x |
| Vorbis完整pipeline | 0.25 s | 5.58 ms | 45x |
| ALAC/M4A完整pipeline | 0.20 s | 4.09 ms | 49x |
| AAC/M4A完整pipeline | 0.50 s | 8.20 ms | 61x |

fixture很短且命中OS page cache，只适合检测代码级回归。保存优化前基线并让Criterion统计比较：

```sh
cargo bench --bench criterion_media -- --save-baseline before
# 修改后
cargo bench --bench criterion_media -- --baseline before
```

结果显示生产常见44.1 kHz输入不能只看Opus：Rubato也是主要CPU阶段；下一步优化应按真实曲库的
格式占比和并发尾延迟排序。

音乐播放采用质量优先基线：Opus complexity固定为10，Rubato默认质量参数不因吞吐测试下调。
benchmark用于寻找等质量实现中的CPU、复制和allocation浪费，不用于论证降低编码或重采样质量。

`allocation_profile`用release构建直接统计每次workload的allocation次数和申请字节，不测耗时：

```sh
cargo bench --bench allocation_profile
```

2026-07-22的5秒基线中，Rubato是527次/574462 bytes，Opus是250次/80570 bytes；Opus恰好每个
20 ms frame产生一次payload allocation，但总量只有约16 KiB/s。短fixture的完整pipeline为
568至2280次allocation，其中Vorbis的高值主要来自open/init（open并drain为1737次，预先open后的
steady drain只有4次），不能把每首歌冷启动成本误判成持续decode热点。基于这组证据，暂不增加
Opus payload pool；先用长时间多路运行确认allocator或RSS确实构成压力。若要定位Rubato内部
callsite，应使用保留debug symbol的profiling构建，strip后的Massif `UnknownFn`不能支持代码改动。

### 多路质量 soak

N-API runner固定使用Opus complexity 10、128 kbps和现有高质量Rubato配置，循环播放确定性的
44.1 kHz stereo非静音fixture，并持续补充next来覆盖promotion：

```sh
cd crates/music_stream_napi
npm run soak -- --streams 10 --duration-seconds 1800 --sample-seconds 2 --fixture-seconds 5
npm run soak -- --streams 50 --duration-seconds 7200 --sample-seconds 2 --fixture-seconds 5
```

runner输出JSONL：

- `sample`中的packet/byte、underrun、drop和recovery是相邻diagnostics样本的差值；首个样本没有
  previous，因而这些差值为0；
- `bufferedMs`给出所有当前可观测stream的min/p50/p95/max；promotion或sender尚未发布首份progress
  时，个别stream可短暂计入`missingDiagnostics`，持续缺失才表示异常；
- `playStates`、`currentTracks`和`nextTracks`揭示actor层跨曲空档；`rtpPacketCoverageRatio`把实收RTP
  与每路每秒50个20 ms packet的基线比较，用来发现underrun计数覆盖不到的无current/next时段；
- `sinkRtp*`只统计音频RTP，`sinkRtcp*`统计mux到同一socket的RTCP，前者可与sender的
  `packetsSent`交叉检查；
- `summary`给出sender生命周期累计计数、RSS/heap基线与峰值、全程最大event-loop delay及
  diagnostics缺失样本数，适合直接比较不同commit；
- runtime error会使进程非零退出；underrun、drop或RSS增长不会被runner用任意阈值自动隐藏或判定，
  应结合运行时长、机器和基线判断。

runner在每次sample时补充next，因此`sample-seconds`不得大于`fixture-seconds`的一半，否则测试工具
自身会制造跨曲空档；无效组合会直接拒绝。覆盖率仍可能包含低频RTP keepalive，应结合play state和
current/next计数判断，不能把它单独当作音质分数。

短基线（同一开发机release构建）中，10路/15秒为0 underrun、0 drop、0 recovery，最大lateness
4 ms。修正runner对无效采样周期的接受后，50路/60秒有效基线的RTP packet覆盖率为1.000，持续
50路playing，0 underrun/drop/recovery/error，最大lateness 8 ms；RSS从启动前62.5 MiB、首样本
103.5 MiB增长到峰值137.6 MiB，增长速度已放缓但一分钟不足以证明平台期。5秒fixture刻意放大
decoder冷启动和promotion频率；allocator稳定性至少需要30至120分钟运行，格式/容器质量与长曲
切换仍必须使用生产corpus验证，不能由循环WAV fixture替代。

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
