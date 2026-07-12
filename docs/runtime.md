# Runtime

## 任务树

```text
StreamRuntime
  ├── worker-event loop
  ├── persistent RTP/RTCP sender task
  ├── current producer task
  │     └── blocking CPU decode task
  └── optional next producer task
        └── blocking CPU decode task
```

producer task 负责 async source 准备，并通过 `spawn_blocking` 运行 codec。sender task 只做 queue receive、prebuffer、`sleep_until`、packetize 和 socket I/O。supervisor、producer、Opus queue 与 sender 分属独立模块。

producer只有在 async admission获得 blocking producer额度后才创建 `spawn_blocking` task；next还必须先获得更小的 preload额度，因此预载不能占满所有 blocking worker容量。CPU scheduler独立限制真正执行 codec的并行 turn并保持 current优先。

URL flight的HTTP和tempfile admission也区分current/preload：preload只能使用子额度，至少为
current保留一个HTTP槽和四分之一tempfile预算。若current加入一个仍在排队的preload flight，
该有限传输永久提升为current优先级，避免在partial response中途重新申请资源。
live HTTP连接由 `maxConcurrentLiveStreams` 独立限制并同样为current保留一个位置；所有live
byte bridge共享 `maxLiveBufferedBytes`，不再使用会混淆URL spool的泛化buffer名称。

零起点 URL的HTTP task与stream decoder并行运行：writer按响应顺序写入growing tempfile，
decoder达到prebuffer后即可驱动sender；HTTP完成只决定cache promotion，不再决定首包时间。
seek起点非零时仍等待完整artifact并使用Symphonia准确seek。

decoder拥有独立文件reader；读取位置追上已写长度时在condvar等待并释放CPU lease，新的磁盘
区间可见后再恢复调度。URL不再复制一份长期内存byte bridge，同一spool可扩展为多个从offset 0
开始的reader，同时仍保持严格顺序和完整cache。sender记录
`activation_to_prebuffer_us`作为端到端首缓冲延迟。

## 背压

- live byte bridge 使用 byte-counting semaphore，容量是实际字节而不是 chunk 数量；过大的 HTTP chunk 会零拷贝切片后逐段进入 bridge。
- Opus bridge 是唯一的 duration-bounded queue，容量直接使用 `encoded_capacity_ms`，不按对象数近似。
- sender pause 后停止消费，producer 最终阻塞在同一个 queue。
- next producer 填满预载窗口后阻塞，不会无限提前编码。
- receiver 被切换或取消时，producer 的 blocking send 被唤醒并退出。

## CPU 调度

文件、tempfile 和 live producer 的 codec 阶段都通过统一 CPU scheduler。current 有优先权；preload 最多使用 `CPU-1` 的预算，至少为 current 保留一个执行位置。worker 在等待满 Opus queue 或 worker event queue 前释放 CPU lease，背压不会伪装成 CPU 占用。

live 的 `StreamingByteReader` 只在真正进入 `blocking_recv` 前释放 lease，拿到网络字节后再参与调度；因此 Symphonia/Rubato/DSP/Opus 受统一预算约束，而网络 stall 不占 CPU 位置。等待 scheduler 的任务定期检查 cancellation，已取消 generation 不会残留在 current-priority waiter 中。

## Pacing

默认 Opus frame 是 20 ms、960 samples/channel、48 kHz。sender 每次 deadline 只发送一帧：
Node调用方可通过 `startStream.buffer` 调整 `prebufferMs`、`encodedCapacityMs`、
`nextPrimeMs`、`decodeBatchMs`和 `maxPlayoutLatenessMs`；所有媒体时长关系在启动前统一校验。

```text
wait prebuffer
deadline = now
loop:
  sleep_until(deadline)
  if lateness > max_playout_lateness:
    drop stale frames while a newer frame is available
    advance RTP timestamp and media position, not sequence
  pop one frame
  packetize with persistent sequence/timestamp
  async UDP send
  deadline += frame duration
  if late: rebase to now + frame duration
```

不会 burst 发送 backlog。默认最多保留 100 ms wall-clock迟滞；恢复时只丢弃已有后续帧的
最旧 Opus frame，绝不丢掉暂时 starve 时唯一可播的帧。被丢媒体推进 timestamp和播放位置，
但不消耗 sequence、packet/octet计数；首个实际 packet仍带 marker。underrun 后重新满足
prebuffer，再从新的 wall-clock base恢复；RTP media clock不回退。progress和 metrics记录
dropped frames/media、恢复次数及观测到的最大 lateness。

packetizer 直接 marshal 到 sender 复用的 `BytesMut` 并原地发送，不再为每个 RTP packet 复制第二份 buffer。

## Pause 与 resume

- file 和有界 URL 的 pause 是 stream 级冻结：current、next preload、source transfer、decode、encode 和 RTP 同时停止。
- 暂停状态下新增或替换的 producer 从创建起保持 paused，不会偷偷建连或预解码。
- current 已结束而 next 尚未 ready 的空窗也受同一 pause gate 控制；next promotion 不会清除 Paused，seek/switch/refresh 也不会隐式恢复。
- 有界 HTTP 的 `ioTimeoutMs` 只计算单次活跃网络 I/O；open/body-read future 在 pause 期间保持 pinned，只冻结并重置 deadline，resume 后继续同一 attempt/response。丢弃 reqwest future 会关闭已发送请求，因此禁止用 cancellation 模拟 pause。
- 同内容URL共享一个registry-owned transfer。任一subscriber活跃时继续下载，全部paused才冻结；
  单个subscriber取消只关闭自己的reader，最后一个subscriber退出才取消HTTP与删除partial spool。
- 恢复顺序是先打开 producer gate，再等待 sender resume acknowledgement；已有 Opus queue 从原位置继续，RTP timestamp 不跨暂停推进。
- live source 没有持久 timeshift store，无法可靠恢复暂停时刻，因此 pause 明确返回 `UNSUPPORTED`。
- live 作为 next 预载时若 stream 暂停，会取消该 HTTP generation；resume 后从新响应重新预载，不在内存中伪造 timeshift。
- 暂停期间的 seek/switch/refresh/setNext 只更新 slot，不创建 producer；显式 resume 才分配 source、CPU 和 queue 资源。

## RTCP

- SR 默认每 5 秒发送。
- mux 模式复用 RTP socket；非 mux 使用独立 RTCP socket和 remote RTCP port。
- RR 解析结果进入 status snapshot。
- rolling quality window 在等级变化时产生 `networkQualityChanged`。

## 取消与 shutdown

- activate、pause、resume、deactivate 和 shutdown 都有 sender acknowledgement；API 返回时控制动作已经生效。
- command和 worker event先在 actor副本上规划，runtime action成功后才提交状态并发布事件。
- generation替换由新 producer/receiver原子覆盖旧槽；创建或 activate失败时旧状态不会被提前取消。
- 不可回滚的 runtime action失败会收敛 current/next/sender，并以原始错误进入 `Stopped`，禁止留下无任务的假 `Buffering`。
- live cancellation 同时停止 HTTP task并关闭 byte bridge。
- producer 正常等待退出，超时后才 abort 外层 task。
- sender 的拥塞事件使用固定语义槽位：prebuffer、terminal 和 latest quality；关键状态不丢，quality 自动合并。
- sender task由 handle监督；command、RTP/RTCP datagram和 shutdown都有 deadline。producer stop会报告 panic或退出超时，不再静默 abort。
- `Streamer.shutdown()` 遍历所有 stream，停止 runtime并清空 cache/event registry。

## 临时文件磁盘治理

同一 `Streamer` 的 URL spool和完整 artifact cache共享 `maxTempfileBytes`。cache最多占预算一半；
每个 HTTP attempt在开连接前按 `source.http.maxBytes` 做 async最坏值 admission，完成后缩减为
实际文件大小。预留整个 attempt而不是边写边申请，可避免多个半成品互相占满剩余配额后
全部等待的死锁。admission顺序固定为 tempfile quota、HTTP并发槽、growing spool、
blocking decoder worker；磁盘压力或慢网络不会占用尚未创建的下游worker，磁盘等待也不会
占住网络槽。quota以 1 MiB为粒度，并且只在后台实际删除 tempfile之后释放。
`Streamer.shutdown()`在drop cache和所有flight后向cleanup worker发送barrier，确认文件删除和
quota释放完成后才返回。
