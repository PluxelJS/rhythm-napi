# 使用指南

本文面向负责 playlist、provider 和 voice gateway 的 Node.js 宿主。它不逐项复制 TypeScript API，
而是解释如何按 runtime 的设计正确组织调用。具体参数形状以构建生成的 `index.d.ts` 为准。

## 先建立正确心智模型

一个 `Streamer` 是一个进程内媒体资源域；一个 `streamId` 是一条长期 RTP session；一首 current
或 next 是这条 session 上可替换的媒体 generation。

```text
one process
  └── preferably one long-lived Streamer
        ├── stream A → one RTP session → changing track generations
        ├── stream B → one RTP session → changing track generations
        └── shared CPU / HTTP / live / tempfile budgets
```

不要为每首歌创建 `Streamer`，也不要通过 stop + start 模拟普通切歌。普通 seek、switch 和 next
promotion 应保留同一个 stream，使接收端继续看到相同 SSRC 和单调 RTP 时钟。

## 宿主负责什么

宿主应该拥有：

- playlist、循环/随机、推荐、跳过与重试策略；
- 把 provider track 解析成 file/URL/live source；
- 稳定内容 ID 和版本规则；
- URL 过期后的重新签名；
- RTP 目标、SSRC、payload type 和 gateway 生命周期；
- 响应 next、refresh、quality 和 error 事件；
- 进程结束前显式 shutdown。

runtime 负责一次确定媒体执行：下载、解码、预载、Opus、RTP/RTCP、暂停语义、资源上限和任务
收敛。不要让两边同时维护同一份 current/next 状态；以命令返回的 status 和事件作为 Rust 状态事实。

## 创建资源域

通常在媒体进程启动时创建一个 `Streamer`，在进程退出时复用它完成所有 stream。默认资源适合作为
起点，只有实际 profile 和并发目标证明需要时才调整。

```ts
import { Streamer } from '@rhythm-app/streamer'

const streamer = new Streamer({
  maxStreams: 1_024,
  maxCpuWorkers: 8,
  maxBlockingProducers: 64,
  maxBlockingPreloads: 16,
  maxConcurrentHttpDownloads: 8,
  maxConcurrentLiveStreams: 64,
  maxLiveBufferedBytes: 64 * 1024 * 1024,
  maxTempfileBytes: 1024 * 1024 * 1024,
})
```

这里的限制属于这个 `Streamer`，不是单个 stream。`maxStreams`硬限制sender/socket/task总量，
最近停止状态也只保留同等容量。关键关系是：CPU worker 不得超过 blocking
producer；preload 必须严格小于 blocking 总量；HTTP 至少有两个槽以便从 preload 中保留
current；live 只有 current，因此最小为一；单个
URL `maxBytes` 不得超过 tempfile 总预算四分之一。

## 正确描述 source

### 稳定内容 ID

`TrackSource.id` 同时用于 source refresh身份校验、URL single-flight 和 artifact cache。它必须表示
内容版本，而不是临时签名 URL。

```ts
const source = {
  id: `${provider}:${trackId}:${contentVersion}`,
  kind: 'url',
  url: signedUrl,
  formatHint: 'mp3',
}
```

相同字节内容使用相同 ID，新的转码或版本必须使用新 ID。若不同内容错误复用 ID，runtime 可能
合理地复用旧 flight/cache；若相同内容每次生成随机 ID，则失去共享下载与 cache。
single-flight 只在 HTTP 字节上限、超时、重试和 cache 策略也相同时发生；不同安全策略不会
相互继承。临时 URL 或鉴权 header 不属于内容身份，但会进入 flight 的非可逆 fingerprint，因此同一
内容同时使用不同签名/凭据时不会错误共用在途请求。已缓存 artifact 也必须符合当前 `maxBytes` 才会命中。

### 选择 source 类型

| 类型 | 适用场景 | 行为 |
| --- | --- | --- |
| `file` | 本机完整文件 | seekable，无下载和 tempfile 配额 |
| `url` | 有大小上限、最终会完成的远程音频 | 零起点渐进播放，完成后可 cache/seek |
| `live` | 不确定总长度的直播 body | bounded byte bridge，不 cache，作为 current 时不 pause/seek |
| HLS（`.m3u8` 或 hint） | 音频 VOD/live playlist | 解析 playlist 后分类；VOD 为 `url`，直播为 `live`，都不可 seek/cache |

不要把直播伪装成 bounded URL，否则 `maxBytes` 和完整 artifact 语义不成立；也不要把普通歌曲标成
live，否则会失去 pause、seek、完整 artifact 和 cache语义。

若宿主把 Icecast/ICY 直播误标为 `url`，runtime 会在读取正文前根据最终 HTTP 响应中的 `icy-*`
或 Icecast server 加音频 MIME 强信号自动切换到 live 管线，并把状态修正为 `kind: 'live'`、
`seekable: false`。单独的 `audio/mpeg`、缺少 `Content-Length` 或 `no-cache` 都不足以判定直播，
因为普通有限文件也可能具有这些特征。自动纠错是兼容措施；已知是电台的来源仍应直接传 `live`，
避免错误请求产生一次额外重连。

HLS 不依赖调用方准确传入 kind：URL path 以 `.m3u8` 结尾或显式传入 `formatHint: 'm3u8'` 时进入
HLS 管线；遗漏 hint 时，标准 HLS Content-Type 或跳转后的 `.m3u8` URL 也会在读取正文前触发一次
安全重路由。runtime 读取最终 media playlist 后，用 `EXT-X-ENDLIST` 区分有限 VOD 与直播。当前常用
路径支持 master/media playlist、相对 URL、
live reload、packed ADTS AAC/MP3、MPEG-TS 内的 ADTS AAC/MP3，以及带 `EXT-X-MAP` 的
fMP4/CMAF AAC；独立文件和共享文件 byte range 两种 CMAF 布局都支持。标准 whole-segment
AES-128 支持显式或 media-sequence IV，并复用 source header获取受保护的 key。master playlist会避开
已知不支持的音频 codec并优先可播放的 audio rendition；`EXT-X-GAP` 会安全跳过，初始化段和 codec
不变的 `EXT-X-DISCONTINUITY` 可连续播放。SAMPLE-AES/DRM、跨 discontinuity 的初始化段/codec切换和
LL-HLS partial segment 尚未支持，会返回明确错误。

零起点 URL并非对所有容器都能渐进解码。M4A/MP4 会先做有界 faststart 检测：若完整 `moov` 位于
首个 `mdat` 前且落在前 4 MiB内，可边下载边播放；尾部 `moov`、超限或不明确布局仍等待完整
artifact。无扩展名签名 URL应提供显式 `formatHint`。详细关键路径和格式边界见
[latency.md](latency.md)。hint描述媒体字节而不是 MIME type，例如使用 `mp3`，不能传 `audio/mpeg`。

需要 Referer、User-Agent、Cookie 或 Authorization 的来源可在 `headers` 中提供每源 HTTP header。
header 只用于服务端请求，不会出现在 status/event；最多 16 项、总计 16 KiB，并拒绝 Host、Range、
Content-Length、Connection 等 transport framing 字段。不要把 header 或签名 URL 写进 `id`。
这些 header 属于受信 provider 能力，HLS 获取会把它们用于 variant、segment、init map 和 key请求；
provider 必须只返回自己信任的媒体图。`networkPolicy: 'public-only'` 面向用户直链，禁止自定义 header，
且入口及所有 HLS 派生 URL 都必须是默认端口 HTTPS 并解析到公网地址。

## 建立一条 stream

启动时一次提供 current、可选 next、RTP transport 和媒体策略。下面示例展示组合关系，而不是
完整字段参考：

```ts
const status = await streamer.startStream({
  streamId: voiceConnectionId,
  current: {
    id: 'provider:track-a:v3',
    kind: 'url',
    url: signedCurrentUrl,
    formatHint: 'mp3',
    headers: { referer: 'https://provider.example/' },
  },
  next: {
    id: 'provider:track-b:v1',
    kind: 'url',
    url: signedNextUrl,
    formatHint: 'mp3',
  },
  transport: {
    ip: gatewayIp,
    port: gatewayRtpPort,
    rtcpPort: gatewayRtcpPort,
    rtcpMux: false,
    audioSsrc: negotiatedSsrc,
    audioPt: 96,
    bitrate: 128_000,
    // Optional sparse Opus silence RTP during idle/buffering/paused gaps.
    // This preserves NAT/media-server state without advancing track progress.
    rtpKeepaliveIntervalMs: 5_000,
    mtu: 1_200,
  },
  source: {
    http: {
      maxBytes: 256 * 1024 * 1024,
      ioTimeoutMs: 30_000,
      maxRetries: 2,
      retryBackoffMs: 250,
      cacheTempFiles: true,
    },
    liveHttp: {
      openTimeoutMs: 30_000,
      idleTimeoutMs: 30_000,
      maxBufferedBytes: 512 * 1024,
      maxRetries: 2,
      retryBackoffMs: 250,
    },
  },
  buffer: {
    decodeBatchMs: 80,
    encodedCapacityMs: 400,
    prebufferMs: 100,
    nextPrimeMs: 200,
    maxPlayoutLatenessMs: 100,
  },
  volume: 1,
  gainDb: 0,
})
```

启动 Promise 返回表示 runtime、sender 和 current generation 已建立，不等于第一包已经发送。
状态通常先是 `buffering`，prebuffer ready 后通过状态事件进入 `playing`。

RTP transport 必须来自实际 gateway 协商。`rtcpMux=false` 时必须提供 RTCP port。当前仅支持
plaintext；配置其他 protection mode 会 fail closed，不能依赖静默降级。

## 用事件驱动 playlist

callback 是低延迟唤醒，补偿队列才适合统一串行处理。推荐 callback 只触发 drain，不同时处理
callback payload 和 drain 结果，以免同一事件处理两次；同时保留低频定时 drain，以覆盖 callback
队列拥塞或宿主调度延迟。

```ts
let drainScheduled = false
let eventChain = Promise.resolve()

function scheduleDrain() {
  if (drainScheduled) return
  drainScheduled = true
  queueMicrotask(() => {
    drainScheduled = false
    const events = streamer.drainEvents()
    eventChain = eventChain
      .then(async () => {
        for (const event of events) await handleMediaEvent(event)
      })
      .catch(reportMediaPolicyError)
  })
}

streamer.setEventCallback(scheduleDrain)
const fallback = setInterval(scheduleDrain, 1_000)
```

callback与drain中的同一事件拥有相同 `sequence`。事件处理器应以sequence去重，并按事实通知做
幂等策略；若按stream过滤drain，sequence间隔也可能来自其他stream或快照合并。native 会先发布对应的
`stateChanged`，再发布需要宿主采取动作的 `nextNeeded` / `sourceRefreshNeeded`，避免宿主的新命令被旧状态覆盖：

- `nextNeeded`：native 已经没有 current，也没有可自动晋级的 next；从 playlist 重新选择并设置或切换
  current。健康的预加载晋级只通过 `stateChanged` 报告，不会发送 `nextNeeded`。
- `sourceRefreshNeeded`：根据 `sourceRole` 为 `current` 或 `next` 重新向 provider 获取同一内容 ID 的
  URL，再 refresh current 或重新设置 next；不能只凭 track ID 判断，因为循环预载可与 current 同 ID。
- `networkQualityChanged`：作为换节点、码率或 FEC 等宿主策略输入，不要期待 Rust 自动动作。
- `error`：根据稳定 code 决定刷新、跳过、重试或关闭；随后查询 status确认 current/next 状态。
- `stateChanged`：更新外部可见状态，不反向推导新的媒体 action。

事件队列有界，宿主应持续 drain。不要把 callback 当成可靠消息总线；若需要跨进程可靠性，应把
drained 事件写入自己的队列或状态存储。

## 维持 next 预载

低切歌延迟依赖 next 提前准备。推荐宿主始终让 runtime 知道至多一个 next：

1. 启动时若 playlist 已知下一首，一并提供。
2. 收到 `nextNeeded` 后解析下一首 source并设置 next。
3. playlist 改变时用新的 next 替换旧 next，或显式清空。
4. current 结束后让 runtime promotion，不要 stop + start。

next使用独立producer但受preload子额度限制。它只预编码到prime窗口，不会把整首歌放进内存。
live不能作为next；需要切入直播时使用switch把它设为current，连接才从切换时刻建立。

preload 子额度属于 `next` 角色，不属于 transfer、artifact 或 producer 的完整生命周期。next promotion 为
current 时，已经取得的 HTTP、tempfile 和 blocking preload-only permit 必须立即释放；全局额度仍保留到真实
资源销毁。只让尚未取得 permit 的 waiter 观察 promotion 不够，否则 promoted current 会继续占住下一首需要的
preload 额度，形成 `current 等 next ready、next 等 current artifact 释放` 的循环。任何资源策略变更都必须用
至少三个连续 bounded URL source 验证，而不能用 local file 代替第二次预载。

## Pause、seek 与切换

### Pause

file 和 bounded URL 可以暂停。Promise 返回后 sender 已确认暂停，producer/source 会沿背压冻结。
shared URL 若仍被另一 stream 使用，物理下载可能继续，但本 stream 的媒体位置不会前进。

live pause 返回 `UNSUPPORTED`。产品若需要 live timeshift，应先设计持久 ring store，而不是循环调用
stop/start 或在宿主丢弃字节。

### Seek

seek 创建新 generation，RTP session 不变。bounded URL 在完整 artifact 前执行非零 seek可能需要
等待下载完成。UI 应把 Promise 当作异步媒体重建，不应假设立即完成。live 不可 seek。

### Switch

用户主动跳歌时使用 switch 并同时给出新的可选 next。它替换 generation但保留 RTP session。
不要先 stop 旧 stream再 start 同一 `streamId`，除非 gateway session 本身确实结束。

### URL refresh

refresh 只用于相同内容身份的新定位信息。传入的 current 必须保持同一稳定 ID；更换内容应使用
switch。401/403 不在 Rust 内无限重试，宿主收到 refresh 请求后决定重新签名、跳过或终止。

## 音量、gain 与 ReplayGain

volume 是 0..1 的用户控制并使用感知曲线；gain 是 -60..+12 dB 的媒体校正。二者都在 Opus 前
生效且不重启 generation。正 gain 有 limiter，但 limiter 不是修复错误 loudness metadata 的理由。

ReplayGain helper 只根据宿主传入的 track/album gain 与 peak 计算建议。宿主应在开始播放前得到
metadata并显式应用建议；当前 runtime 不扫描 EBU R128，也不会自动改变 gain。

## 错误恢复

Promise rejection 的 message 以稳定 code 和冒号开头，异步媒体错误事件也带同一套结构化
code。这是 napi-rs Tokio Promise 边界的明确契约：宿主只取第一个冒号前的 code 做恢复分类，
其余 message 只用于诊断：

| Code | 宿主建议 |
| --- | --- |
| `SOURCE_AUTH_EXPIRED` | 获取同 ID 新 URL并 refresh；失败则按 playlist 策略跳过 |
| `SOURCE_TIMEOUT` | 根据 provider/节点策略有限重试或切换，不在紧循环重启 |
| `INVALID_SOURCE`, `DECODE_ERROR` | 标记本次 source不可用，选择下一首或上报内容问题 |
| `NOT_SEEKABLE`, `UNSUPPORTED` | 修正产品操作，不把它当网络重试 |
| `OUTPUT_ERROR` | 检查 gateway/session；通常需要重建外部连接，而非重试同一 packet |
| `BUSY` | 等待正在进行的生命周期操作完成，避免并发重复命令 |
| `INTERNAL`, `STREAM_CLOSED` | 查询状态并收敛该 stream；持续出现时记录诊断并重建 runtime |

current failure可能已经 promotion 到 next，不能看到 `error` 就无条件 start 新 stream。先消费后续
状态事件或查询 status，再执行宿主策略。

## 调优原则

默认值优先保证低延迟和有界资源：

| 参数组 | 默认思路 | 调大后的主要代价 |
| --- | --- | --- |
| prebuffer 100 ms | 小范围网络/CPU 抖动 | 首包更慢 |
| encoded capacity 400 ms | 有界 producer/sender 解耦 | 每流内存和可积累延迟增加 |
| next prime 200 ms | 低 promotion 延迟 | preload CPU/内存增加 |
| max lateness 100 ms | 实时优先，严重 stall 后追赶 | 更大值保留更多延迟 |
| URL max 256 MiB | 有界歌曲 artifact | 需要更大 tempfile worst-case reservation |
| live bridge 512 KiB/流 | 解耦 HTTP chunk 与 decoder | live 内存增加 |

不要只因机器内存充足就扩大 queue。先观察 activation-to-prebuffer、underrun、CPU/admission wait、
sender lateness 和 Node event-loop delay，再调整最接近瓶颈的一层。

## 状态查询与并发控制

命令按 stream 串行化，但宿主仍应避免对同一 stream 同时发出相互矛盾的 pause/resume/switch/stop。
以每个 Promise 的返回 status作为该操作提交后的快照。批量 status 适合监控，不应作为高频播放时钟；
`timePlayedMs` 表示已发送或为实时追赶而跳过的媒体位置，不是 wall-clock 计时器。
单次批量查询最多接受 4096 个 ID，所有 stream ID 都必须是 1..512 bytes 的非空字符串。

主动查询的status还可能包含`playoutDiagnostics`：

- `bufferedMs`：最新encoded queue深度；持续贴近0且`underruns`增长表示producer/source供给不足；
- `droppedFrames`/`droppedMediaMs`与`latencyRecoveries`：sender为恢复实时性丢弃的旧媒体；
- `maxLatenessMs`：该sender生命周期观察到的最大deadline迟滞；
- `packetsSent`/`bytesSent`、`sequence`/`rtpTimestamp`：实际成功交给UDP的累计进度和RTP clock。

计数跨switch、seek和promotion累计，因为sender不会随track重建。10/50路soak应每1至5秒用
`getStatuses`批量采样并计算相邻差值，不要把status轮询当播放时钟。actor产生的`stateChanged`事件
可能不含sender快照；需要诊断时主动查询。

stop 后保留最近的轻量 stopped status，因此重复 stop/status 可以用于收敛。该历史是容量等于
`maxStreams` 的 LRU，旧 ID 会被淘汰，不是持久存储。新的 start 可以复用已停止的 `streamId`；
active 或 starting 的同名 stream会拒绝重复 start。

## 正确关闭

进程退出、热重载或 worker 回收时：

```ts
clearInterval(fallback)
streamer.setEventCallback(null)
const finalEvents = streamer.drainEvents()
eventChain = eventChain.then(async () => {
  for (const event of finalEvents) await handleMediaEvent(event)
})
await eventChain
await streamer.shutdown()
```

shutdown会把 `Streamer`永久置为closed，重复调用幂等成功，其他媒体操作返回`STREAM_CLOSED`；它
停止全部stream、清空事件/cache并等待tempfile真实删除与quota释放。必须await；
不要依赖垃圾回收或 Node 进程退出完成异步 socket/task/磁盘清理。

## 接入检查表

- 全进程复用一个长生命周期 `Streamer`。
- `streamId` 对应外部 RTP session，track ID 对应稳定内容版本。
- file/URL/live 分类与真实 source 生命周期一致。
- 正常切歌使用 next promotion或 switch，不重建 RTP session。
- promotion 会释放所有已取得的 preload-only permit；用连续三个 URL source 验证没有角色资源死锁。
- callback 作为 drain 唤醒，并有低频补偿 drain。
- 401/403 refresh 保持同一 track ID。
- live 不调用 pause/seek，也不作为 next；切入 live时直接成为 current。
- 所有 Promise 都 await，并为 rejection 执行确定策略。
- 根据实测指标调资源，不建立无界业务重试。
- 进程结束前 await `shutdown()`。
