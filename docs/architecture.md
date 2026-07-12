# 设计总览

## 要解决的问题

系统需要长期承载多路音乐流：异步获得音源，尽早开始解码，稳定完成 DSP 和 Opus 编码，
再以真实时间节拍持续发送 RTP/RTCP。它同时追求四件事：低首包延迟、实时 sender 不被 CPU
工作干扰、资源总量有界，以及控制操作发生后状态与资源一致。

这些目标不能靠“所有代码都 async”实现。网络和等待适合 async，codec 和 DSP 是阻塞型 CPU
工作；把后者放进 async worker 只会让 timer 和 UDP 调度失去确定性。因此系统明确分离控制面、
媒体生产面和实时发送面。

## 系统边界

```text
TypeScript host
  owns: playlist, repeat/shuffle, recommendation, provider auth,
        source URL refresh, gateway negotiation, node selection
                │
                ▼
Streamer Promise boundary
  owns: active stream registry, shared resource budgets, event delivery
                │
                ▼
StreamRuntime
  owns: one actor, current/next producers, one persistent RTP/RTCP sender
```

Rust 不猜下一首，不保存 provider 凭据，不根据网络质量自行换节点，也不把有限重试扩展成业务
重试循环。宿主提供确定的 current/next source；Rust 对这次媒体执行负责。

## 所有权层次

理解所有权比理解模块名称更重要：

| 作用域 | 唯一所有者 | 拥有的状态 |
| --- | --- | --- |
| 进程 | 静态 HTTP client | DNS/TLS/HTTP 连接池 |
| `Streamer` | `RuntimeResources` | stream总量、CPU、blocking producer、HTTP/live、live bytes、tempfile/cache 配额和共享 URL flight registry |
| stream | `StreamRuntime` | actor、current/next handle、事件入口和 persistent sender |
| RTP session | sender | socket、SSRC、sequence、timestamp、pacing、RTCP 和 active receiver |
| generation | producer | source reader、decoder、resampler、DSP、encoder 和 Opus queue sender |
| artifact | cache/使用者引用 | 完整文件及其延迟清理凭据 |

`Streamer` 级预算不是进程级预算。创建多个 `Streamer` 会得到多套媒体资源额度；只有 HTTP
client 的连接池在进程内共享。生产宿主通常应该创建一个长生命周期 `Streamer`。

## 控制面：actor 与事务式 action

`StreamActor` 是 play state、current、next 和 generation 的唯一写入者。它只处理纯状态转换，
输出 `TaskAction`、事件和新状态，不持有 socket、decoder 或 task handle。

```text
command / worker event
        │
        ▼
clone current actor state
        │
        ▼
plan state + actions
        │
        ▼
execute runtime actions
   ├── success → commit actor → publish events
   └── failure → converge runtime → terminal state
```

这种 staged commit 避免 API 已返回成功但 producer 或 sender 实际未切换。可原子替换的操作先
创建新 producer/receiver，再覆盖旧槽；不可回滚的 action 失败则停止整个 runtime，不能留下
没有任务支撑的假 `Buffering`。

所有 command 与 worker event 经过同一个 orchestration mutex 串行化。generation 是异步结果的
版本号：seek、switch、refresh 或 promotion 后，旧 generation 的 ready、ended、error 和 quality
事件都没有副作用。

## 数据面：producer 与 sender 分离

每个 generation 拥有一个 producer：

```text
TrackSource
  → source preparation
  → Symphonia decode
  → stereo normalize/downmix
  → Rubato 48 kHz resample
  → volume + gain + limiter
  → libopus 20 ms frame
  → duration-bounded OpusQueue
```

producer 只生产 immutable Opus frame。current 与 next 各自拥有独立 decoder、PCM scratch、encoder
和 queue；它们不共享 frame。next promotion 是把已经预热的 queue receiver 所有权交给 sender，
不是复制 frame，也不是重新解码。

sender 从 stream 创建到 stop 始终存活，只做 prebuffer、pacing、RTP packetize、UDP I/O、RTCP
和进度统计。sender 永远不调用 source、decoder、resampler、DSP 或 encoder，因此慢解码和网络
下载不能直接阻塞 RTP deadline。

## 为什么 RTP session 属于 stream

曲目是可替换媒体，RTP session 是接收端看到的连续传输。若每首歌重建 sender，切歌会重置
SSRC、sequence、timestamp、RTCP 统计和 socket，接收端会把无缝切换误判为新会话。

因此 seek、switch 和 promotion 只替换 active generation 的 queue receiver：

- SSRC 保持不变；
- sequence 只对实际发送的 RTP packet 递增；
- timestamp 按媒体采样推进，包括为追赶实时进度而丢弃的媒体；
- 新 generation 的首个实际包设置 marker；
- RTCP SR/RR 和质量窗口持续存在。

## 三种 source 模型

### File

本地文件经 async metadata 校验后，由 blocking worker 打开 seekable decoder。seek 直接创建新
generation 并从目标位置打开文件。

### Bounded URL

有界 URL 表示最终会完成、可形成完整 artifact 的远程文件。零起点播放时，HTTP 顺序写入
growing tempfile，decoder 使用独立文件描述符从 offset 0 渐进读取。达到容器 probe 和 prebuffer
所需字节即可开始发送，不等待完整下载。

相同稳定内容 ID 且 HTTP 字节上限、超时、重试与 cache 策略一致的并发 miss 订阅同一个
shared flight。传输策略不同时必须隔离，避免一个 stream 绕过自己的硬上限。显式
`formatHint`描述真实媒体字节，使
没有扩展名的签名 URL也能选择正确渐进 decoder；每个 subscriber 有独立 reader 和
pause/cancel 状态；完整响应才提升为 cache artifact，partial 文件永远不进入 cache。非零 seek
在完整 artifact 上执行，避免把顺序 spool 伪装成任意可寻址文件。

渐进路径的格式门槛、首包关键路径和退化条件见 [latency.md](latency.md)。

### Live

live HTTP 没有完整 artifact，body 进入按字节计量的 bounded bridge，decoder 从 non-seekable
reader 消费。它不进入 tempfile/cache，也没有 timeshift，因此 pause、seek 和 next preload明确
不支持；切换到 live应在成为 current时建立连接。

## 背压与资源策略

系统使用背压而不是无限缓存：

- live bridge 按实际字节限流；
- growing URL 按最坏文件大小预留 tempfile quota；
- producer 和 sender 之间只有一个按媒体毫秒计量的 Opus queue；
- next 达到 prime 窗口后停止提前编码；promotion 会把同一 producer 一次性提升为
  current，释放 preload worker 准入并切换 CPU/source 优先级；
- sender pause 后停止消费，压力逐级传播回 producer 和 source。

资源优先级表达的是播放价值：current 必须能从饱和的 preload 中获得 CPU、blocking worker、
HTTP 连接和 tempfile 空间。live 只有 current 总额度，不存在 preload 子额。其他 preload 只能占子额度；
current 加入已有 shared flight 时可把这次
有限传输永久提升为 current，避免 partial response 中途重新申请配额。

## 设计不变量

- 一个 stream 只有一个 actor 和一个 RTP session owner。
- generation 不拥有 RTP clock。
- current/next 不共享 decoder、PCM scratch 或可变 frame。
- sender 与 codec 不在同一个执行循环。
- 网络、timer、Node 和清理等待不阻塞 Tokio worker 或 JavaScript event loop。
- codec/DSP 不运行在 Tokio async worker。
- stream、停止状态、flight key、queue、事件、连接、worker、内存和磁盘都有局部或 `Streamer`
  级上限。
- partial HTTP response 不与新响应拼接，也不进入 cache。
- stale generation 结果没有副作用。
- playlist 和 provider 策略不进入 Rust runtime。

后续重构只有在保持这些不变量、提供失败语义并补充对应测试时才可接受。
