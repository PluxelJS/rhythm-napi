# 运行模型

## 任务结构

```text
Streamer
├── shared RuntimeResources
├── shared URL flight supervisors
└── StreamRuntime
    ├── worker-event loop
    ├── persistent RTP/RTCP sender task
    ├── current producer supervisor
    │   └── producer worker
    │       └── optional blocking codec task
    └── optional next producer supervisor
        └── producer worker
            └── optional blocking codec task
```

producer worker 先异步等待 source 和资源 admission，只有真正要进入 codec 阶段时才占用 blocking
producer 额度并创建 `spawn_blocking`。blocking task 内部以短 turn 获取 CPU lease；在等待 source
字节、满 Opus queue 或 worker event capacity 时释放 lease。

sender task 独立执行 queue receive、prebuffer、`sleep_until`、packetize、UDP 和 RTCP。producer
退出、panic 或失败由 supervisor 转换成 generation-scoped worker event；已经事件化的媒体失败在
stop 时不会重复上报。supervisor 自身失败和退出超时才升级为 runtime failure。

## 状态模型

公开状态只有五种：

| 状态 | 含义 |
| --- | --- |
| `idle` | runtime 存在，但当前没有可播放 generation；可等待宿主提供 next/current |
| `buffering` | current 已启动但尚未达到 prebuffer，或 promotion 前仍在等待 ready |
| `playing` | current 已 prebuffer 并由 sender 按实时节拍发送 |
| `paused` | 宿主明确冻结了可暂停媒体；该意图跨 current/next 空窗和 generation 替换保留 |
| `stopped` | runtime 已终止，sender 与 producer 已收敛，不再接受播放控制 |

`Buffering` 不承诺正在占用 CPU，它也可能在等待 HTTP、tempfile、blocking admission 或足够的
媒体字节。`Paused` 是用户意图，不是某个 task 的瞬时状态；seek、switch、refresh 和 promotion
都不能隐式恢复播放。

## 启动与首包

启动按以下顺序发生：

```text
validate all configuration
  → reserve stream identity
  → bind persistent RTP/RTCP sender
  → create actor and current producer
  → attach current queue receiver to sender
  → producer obtains source and builds codec pipeline
  → queue reaches prebuffer threshold
  → actor enters Playing
  → sender emits first RTP packet
```

先绑定 sender 能让启动期 transport 错误同步失败，而不是在后台留下假 stream。URL 的 HTTP task
与 stream decoder 并行；首包只依赖 probe、decode 和 prebuffer 所需字节，完整下载只决定 cache
promotion。`activation_to_prebuffer_us` 记录从 generation 激活到 prebuffer ready 的时间。

完整首包分解、渐进格式边界和优化判断见 [latency.md](latency.md)。

默认媒体参数是 20 ms Opus frame、100 ms prebuffer、400 ms encoded capacity、200 ms next prime、
80 ms decode batch 和 100 ms最大 playout lateness。这些值表达低延迟与抗抖动之间的默认折中，
不是吞吐越大越好的缓存目标。

## Current、next 与 promotion

current 是 sender 当前消费的 generation。next 是独立 producer，它可以提前完成 source、decode、
resample 和 encode，直到 queue 达到 `nextPrimeMs` 后报告 ready，然后在专用 promotion gate 上停止
继续解码。提升时不重建 producer：同一 decoder/encoder 切换为 current CPU 优先级，释放
blocking preload 子额并继续向已交给 sender 的 queue 生产。提升后的失败按 current failure
上报，不能因 producer 的初始身份而错分为 next failure。

current 正常结束时：

- ready next 的 receiver 直接转交给 sender；
- next 尚未 ready 时保留等待关系，不把未准备好的媒体强行激活；
- 没有 next 时进入 idle 并产生 `nextNeeded`；
- promotion 保持 RTP session、音量、增益和显式 pause 意图。

next 失败只移除 next 并报告错误，不中断仍在播放的 current。current 失败则丢弃它的 backlog，
按同样规则尝试 promotion；Rust 不决定失败后应该重播、跳过还是换节点。

## Seek、switch 与 source refresh

三者都会创建或替换 generation，但用途不同：

- seek 保持 current 内容身份，取消旧 producer，并从目标时间创建新 generation；
- switch 接受新的 current/next，是宿主主动改变播放内容；
- source refresh 只允许相同稳定内容身份，用于 provider URL 过期后替换临时 URL。

文件和有界 URL 默认 seekable。文件直接 seek；有界 URL 非零 seek 等待完整 artifact 后使用
Symphonia accurate seek。live 永远 non-seekable。所有旧 generation 的异步结果都由 actor 丢弃。

替换遵循“先建新、后覆盖旧”的可原子部分；若 sender activate 等不可回滚动作失败，runtime
统一进入 stopped，而不是提交一半状态。

## Pause 与恢复

### File 和 bounded URL

pause 冻结 sender 消费、current producer 和 next producer。已有 Opus queue 保持原位，RTP media
timestamp 不因暂停的 wall-clock 时间推进。resume 先打开 producer gate，再等待 sender resume
acknowledgement，从原媒体位置继续。

有界 HTTP 的 active I/O timeout 只计算活跃读取时间。pause 保留同一个 pinned request/body future，
恢复后继续当前 response；不能通过丢弃 reqwest future 模拟暂停，因为那会关闭请求并破坏容器流。

shared URL flight 按 subscriber 聚合暂停：一个 stream 暂停会停止自己的 reader 和 producer；若仍
有其他 active subscriber，物理 HTTP transfer 继续。只有所有 subscriber 都暂停时 transfer 才冻结，
最后一个 subscriber 离开才取消传输并清理 partial spool。

暂停期间设置 next、switch、seek 或 refresh 只改变 slot 和播放意图，不建立新连接或启动 codec；
显式 resume 才分配资源。

### Live

live 没有持久 timeshift store，pause 后无法回到准确媒体位置，因此 pause 返回 `UNSUPPORTED`。
系统不把“停止消费但继续下载并丢弃”伪装成 pause。live 同样不能 seek或作为 next preload；宿主
需要切入 live时应把它作为新的 current，此时才建立接近 live edge的连接。

## Admission 与资源优先级

等待上游资源时不能先占住下游稀缺资源。bounded URL 的顺序固定为：

```text
tempfile worst-case quota
  → HTTP connection slot
  → growing spool publication
  → blocking producer slot
  → CPU turn
```

因此磁盘压力不占 HTTP 槽，慢网络不占 blocking worker，decoder 等新字节不占 CPU lease。

`RuntimeResources` 为 current 保留资源：

| 资源 | 总限制 | preload 约束 |
| --- | --- | --- |
| stream | `maxStreams` | 硬限制 active/starting runtime、sender task和UDP socket |
| CPU turn | `maxCpuWorkers` | current 等待时 next 不重新获取；多核时至少留一个位置 |
| blocking producer | `maxBlockingProducers` | next 受更小的 `maxBlockingPreloads` 限制 |
| bounded HTTP | `maxConcurrentHttpDownloads` | 至少留一个 current 槽 |
| live connection | `maxConcurrentLiveStreams` | 至少留一个 current 槽 |
| tempfile | `maxTempfileBytes` | preload 最多使用四分之一预算 |
| live bytes | `maxLiveBufferedBytes` | 所有 live bridge 与 HLS playlist/segment allocation 共享 |

每个 source 的 `maxBytes` 不得超过 `Streamer` tempfile 预算的四分之一，单个 live bridge 也必须
小于共享 live byte 预算。这些关系在启动前校验，避免运行中等待永远无法满足的 semaphore。

## 背压

背压沿着真实数据路径传播：

```text
UDP pacing
  ← OpusQueue capacity
  ← encoder / DSP / resampler / decoder
  ← growing spool or live byte bridge
  ← HTTP body
```

- Opus queue 按媒体毫秒计量，不按 frame 个数近似。
- live bridge 按实际字节申请全局和每流 permit；超大 HTTP chunk 被零拷贝切片后分段进入。
- next 达到 prime 窗口后停止生产，不无限提前解码整首歌。
- receiver 被替换时关闭 queue 并唤醒 blocking producer。
- wait observer 只在真正阻塞读取前释放 CPU lease，返回计算后重新参与调度。

## 实时 pacing 与迟滞恢复

sender 每个 deadline 最多发送一帧，绝不通过 burst 清空 backlog：

```text
wait for prebuffer
deadline = now
loop:
  sleep_until(deadline)
  while lateness exceeds limit and a newer frame exists:
    drop oldest stale frame
    advance RTP timestamp and media position
  send one frame
  increment sequence only after successful UDP send
  schedule next frame deadline
```

丢弃的媒体推进 timestamp，因为接收端媒体时钟必须跳过那段时间；sequence 不推进，因为没有 RTP
packet。唯一可播放帧不会因暂时 starve 被丢掉。underrun 后 sender 重新等待 prebuffer，并从新的
wall-clock base 恢复，不倒退 RTP clock。

## 错误与监督

错误分为三层：

- source/codec/发送错误：关联当前 generation，进入 actor 的正常失败与 promotion 逻辑；
- runtime action 错误：状态无法安全提交，停止 current、next 和 sender 后进入 stopped；
- supervisor/stop/shutdown 错误：表示任务没有按约定收敛，返回稳定 `INTERNAL` 或原始错误。

producer和sender panic都由独立 supervisor转换为 active generation的 `INTERNAL` failure event。
source failure 与 decoder EOF
近同时发生时，优先保留非取消型 source terminal code，避免鉴权或 timeout 被降级成 decode error。
sender command、RTP/RTCP datagram和 stop 都有 deadline。

## 事件交付

媒体 action 成功后才发布事件。每个事件分配 `Streamer` 内单调 sequence；callback与补偿队列中的
同一事件使用同一 sequence，可可靠去重。callback 是低延迟通知，使用有界 non-blocking bridge；
补偿队列按 stream合并旧 `stateChanged`/quality快照并保留关键事件供宿主 drain。宿主不能假设
callback永不丢失，callback panic/异常也与媒体动作隔离。

事件语义属于事实通知：`nextNeeded` 请求宿主提供策略结果，`sourceRefreshNeeded` 通过
`sourceRole=current|next` 请求对应 slot 的同内容新 source，`networkQualityChanged` 提供策略输入，
`error` 报告本次 generation 失败。Rust 不在事件
处理过程中执行 playlist 决策。

Node 可见 status/event 中的 source 只包含 ID、kind、format hint 和 seekable 属性，不回显 URL
或本地 path。HTTP 错误同样只输出稳定分类与 status code，不包含可能携带签名 query 的
reqwest 原始文本。

## Stop 与 shutdown

单流 stop取消 current/next，等待 producer收敛，关闭 sender，并在容量为 `maxStreams` 的 LRU中
保留最近 stopped status供幂等查询。取消会穿透 pause gate、HTTP、CPU scheduler和Opus queue。

`Streamer.shutdown()`进入不可逆但幂等的 closed状态，阻止新的媒体操作，并发停止所有active runtime，清空inactive status、
事件和 source cache。tempfile 最后一个引用释放后由专用清理线程删除；shutdown 最后等待 cleanup
barrier，只有文件真实删除且 quota 释放后才返回。宿主必须显式等待 shutdown，不能依赖进程退出或
对象析构完成异步清理。
