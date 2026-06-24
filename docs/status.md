# 当前实现状态

本页记录当前代码已经落地的能力，以及文档中仍属于目标设计的部分。实现细节以代码和测试为准；目标契约以 `architecture.md`、`implementation.md`、`runtime.md` 为准。

下一阶段的策略边界、观测基线、真实音源兼容性和宿主集成问题集中记录在 [design-review.md](design-review.md)。本页只回答“现在实现到哪里了”。

## 已落地

- `music_stream` 核心 crate 已有 actor/session 状态机，Rust 只管理 current slot 和 next slot，不维护 playlist；pause/resume、volume/gain、cancel/start/prepare 都通过 generation-scoped `TaskAction` 表达，N-API 只执行 actor action，不再旁路修改 session 状态。
- `StreamActor` 通过 generation 隔离 seek、switchTrack、track switch 后的旧 worker event。
- `PlayoutPipeline` 已实现 ahead-of-time decode/encode，使用毫秒水位、bounded queue、FrameAssembler 和 SenderCore。
- `StreamingPcmDecoder`/`StreamingPcmWriter` 已作为 live/streaming source 的内部前置模型落地：producer handle 可在 decoder 交给 pipeline 后继续按毫秒容量 bounded push，满载返回 `BUSY`，空但未结束时向 pipeline 返回 `NeedMore`，finish 后 drain 到 `End`；pipeline 单元测试已覆盖该模型和现有水位/backpressure 的配合。
- `StreamingByteReader`/`StreamingByteWriter` 已作为 live source reader 与非 seekable decoder 输入之间的 bounded byte pipe 落地：producer 侧支持 `try_push` 返回 `BUSY` 和 `push_blocking` 等待读端释放容量，reader 实现 `std::io::Read`，finish 映射 EOF，fail 映射读错误，close 会唤醒读写两端；该原语已被 live HTTP reader 和 Symphonia stream decoder 复用。
- `SymphoniaStreamDecoder` 已接入 non-seekable reader path：可从 `StreamingByteReader` 通过 Symphonia `ReadOnlySource`/`MediaSourceStream` 解码 WAV 等流式输入；测试覆盖 bounded byte pipe -> non-seekable Symphonia stream decoder -> decoded chunk。
- `HttpLiveStream` 已接入真实 HTTP body reader：按 `HttpLiveStreamConfig` 下载 live URL body 到 `StreamingByteWriter`，bounded pipe 满时阻塞形成背压，stop/close 会唤醒阻塞写，EOF 映射 finish，HTTP 401/403 映射 `SOURCE_AUTH_EXPIRED`；408/429/5xx/timeout/连接类错误会按保守默认 retry/backoff 重试，耗尽后 fail reader 并走现有 current failure 事件路径。测试覆盖 chunk 读取、背压 stop、鉴权过期和 503 后重试成功。
- `LiveStreamSlot` 已接入 source/decoder/pipeline 层：`HttpLiveStream -> StreamingByteReader -> SymphoniaStreamDecoder -> RubatoResamplingDecoder -> LibOpusEncoder -> SlotDriver` 可复用现有 bounded queue、prebuffer、sender drain 和 worker event 语义；测试覆盖 finite live HTTP WAV body 进入 live slot 后产出 `CurrentPrebufferReady`、sender drain 和 `CurrentEnded`。
- sender 不执行 decode、resample 或 Opus encode，只从 encoded queue 取 active generation frame。
- `RtpPacer` 已实现纯 pacing 状态机，slot runner 可用 `drain_due_packets(now_ms, max_packets)` 防止 encoded backlog 被 burst 发送。
- 本地文件 source 已实现；`TrackKind::Url` 表示有界 HTTP/HTTPS media，默认 seekable，并支持下载到临时文件后复用本地文件解码链路；HTTP source policy 支持 timeout/max bytes/cache temp files 配置，N-API 可通过 `source.http.timeoutMs`、`source.http.maxBytes`、`source.http.cacheTempFiles` 覆盖默认值；resolver 会复用 per-resolver blocking HTTP client，避免每次 URL resolve 重建 client/连接池；下载中断时可在同一次 resolve 内用 `Range: bytes=N-` 续传，且只有服务端返回 `206 Partial Content` 才追加；401/403 会映射为 `SOURCE_AUTH_EXPIRED` 并产出 `sourceRefreshNeeded` 事件供 Node 重新解析 URL；基础 artifact LRU cache 可复用完整、seekable、cacheable 的临时文件；`TrackKind::Live` 不走 artifact resolver/cache，而是由 live runtime 直接消费 HTTP body。
- Symphonia 文件解码、Rubato 重采样、libopus 编码已接入默认 feature。
- RTP packetize 使用 `rtp` crate 和 `webrtc-util` marshal，不手写 RTP wire format。
- UDP RTP sink 已实现，支持通过 `RtpTransportConfig` 配置 remote/local 地址、port、payload type、SSRC、MTU、RTCP mux、bitrate hint 和 encryption config。
- RTCP Sender Report 已实现：使用 `rtcp` crate marshal，按 runtime interval 周期发送 SR；`rtcp_mux=true` 时走 RTP socket，非 mux 时走 `remoteRtcpPort`。Receiver Report 已能非阻塞读取、解析并记录最新 report block，同时派生当前 RR 的 loss ratio/percent、jitter ms 和可计算时的 RTT ms；runtime/metrics 层已有 rolling RTCP quality window，按窗口上报 loss/jitter/RTT 聚合 gauge，并在 quality level 变化时通过 actor/N-API 产出 `networkQualityChanged` 事件。该事件携带 quality snapshot，包括样本数、latest/average/max loss、average/max jitter 和 average/max RTT。
- `RtpPacketProtector` 已提供 RTP marshal 后、UDP send 前的加密/保护插入点。未安装 protector 时，非 `none` 加密配置会返回明确错误，不会静默明文发送。
- `LocalFileRtpPlayback` 已实现本地文件和有界 HTTP URL 的 RTP runtime worker：构建已解析为文件 artifact 的 slot、连接 UDP sink、复用 `RtpPacer` 按 Opus frame duration 节奏发送，并支持 pause/resume、seek、stop/join、运行中音量更新和低成本进度快照。worker idle/pacer/preload 等待会被 stop、pause/resume、volume/gain 控制唤醒，避免只靠固定 sleep 轮询；runtime tick 已拆成控制同步、RTP/RTCP IO、CPU worker turn 和 wait decision。prebuffer 前保持 worker-first，ready 后先执行 RTP/RTCP IO，再 opportunistic 做 decode/resample/encode worker turn；CPU permit lane 非阻塞，拿不到 permit 只记录 busy 并让出本轮，不阻塞实时 IO。preload 继续使用更保守的 permit lane。
- live RTP runtime 已接入 current playback：`spawn_live_stream_rtp_playback` 复用同一 RTP pacing/RTCP/progress/control handle，构建 `LiveStreamSlot` 后从 bounded HTTP body pipe 解码、重采样、Opus 编码并发送到 UDP；stop/drop 会同时唤醒 live HTTP writer 和 playback control，避免阻塞在 source read/write。N-API `source.liveHttp` 可配置 live HTTP timeout、byte pipe buffer、read chunk、retry 次数和 backoff；`source.http` 继续只控制有界 URL artifact。live current 可通过 `startStream` 或运行中 `switchTrack` 启动，不进入 next preload/promotion，不进入 artifact cache，且在 N-API 输入归一化为 non-seekable。
- 本地文件和有界 HTTP URL next preload 已接入 runtime 和 N-API orchestration：next slot 可 ahead-of-time 解码/重采样/编码到 `NextReady`，current 结束后由 actor action 触发 promotion，保留已预热 queue 并提升为 current RTP playback，不需要重新冷启动解码。
- N-API `Streamer.shutdown()` 已实现显式 lifecycle 收口：停止并 join active playback/preload，清空 transport、event queue、source cache 和 engine registry；`Drop` 复用同一套 best-effort 兜底。
- Tokio actor/task lifecycle 已接入 N-API runtime orchestration：`RuntimeTaskGroup` 固定 cancel、join、timeout abort、panic/failure 汇总语义；`StreamActorMailbox` 用 bounded Tokio channel 包住纯 `StreamActor`，队列满返回 `BUSY`，public command、status query 和 worker event 都在 per-stream actor task 内串行处理，并继续复用现有 generation 过滤；N-API 不再直接写 `Engine`，而是通过 actor mailbox 获取 `TaskAction` 后执行 runtime action。`GenerationTaskSlot` 固定“只操作匹配 generation 的 active handle”规则，playback/preload/promotion registry 已用它收口 stale cancel/progress/control 边界。preload promotion waiter 使用 `LocalFilePreloadCompletion` 通知等待，不再用 sleep 轮询，并在 promotion 前重新校验 registry generation。stop/shutdown 会 stop/join current playback、preload，abort tracked promotion task，并 deterministic shutdown actor mailbox；stop 后仅保留轻量 inactive status 以支持幂等 stop/status 和 streamId 复用。
- 基础音量/增益已实现：用户音量 0.0..1.0 使用感知曲线映射到 dB/linear gain；额外 `gainDb` 使用 -60..+12 dB 强类型范围控制，可在 start 时设置并可运行中 `setGain` 更新；最终增益在 PCM 进入 Opus 编码前生效，正增益路径带 soft limiter，mute 保持 frame shape。DSP 层已有轻量 `PcmLevelMeter`，可统计 peak/RMS dBFS 并计算保守 headroom gain；显式 ReplayGain 推荐器和 N-API `recommendReplayGain` 已实现，可根据 Node 提供的 track/album gain、peak、preamp、防削波策略返回推荐 `gainDb`，但不会自动应用。
- source/runtime 已直接使用 `metrics` crate facade，并用 `tracing` 覆盖 source resolve 与 runtime worker/preload 边界：library 不安装全局 recorder/subscriber，默认无 recorder 时 no-op；测试或宿主可通过 `metrics::Recorder`/local recorder 捕获指标。source resolver 上报 HTTP bytes、resolve timing、cache hit/miss/insert 和 resolve errors，runtime 上报 current/preload worker turn timing、decode/encode counters、queue ms gauge、RTP packet/byte/media counters、prebuffer/underrun/pacing waits、每 tick 最大 RTP pacing late ms 和 RTCP SR/RR counters，并由 recording recorder 测试覆盖。
- MP3/WAV 本地文件和有界 HTTP URL 到 RTP/UDP localhost 的端到端测试已覆盖，包括确定性 slot runner、真实 paced playback worker、短 RTP integration、source cache 复用和 N-API smoke 路径。
- `music_stream_napi` 当前暴露 `startStream`、`pauseStream`、`resumeStream`、`seekStream`、`stopStream`、`getStatus`、`setVolume`、`setNext`、`switchTrack`、`refreshCurrentSource`、`drainEvents`、`setEventCallback`、占位状态接口、`validateRtpTransportConfig`、`validateSourceResolverConfig` 和 `recommendReplayGain`。`startStream` 已可接收 Node 提供的本地文件、有界 HTTP/HTTPS URL 或 live HTTP track、RTP transport config 和可选 source policy，启动 MP3/WAV/finite live WAV 等解码推流；`source.http` 配置 bounded URL artifact policy，`source.liveHttp` 配置 live HTTP policy；同一 stream 的 `setNext`、`switchTrack`、`seek` 和自动 promotion 沿用 start 时的 source policy，其中 live current 固定 non-seekable，live next/preload 显式 unsupported；`getStatus().timePlayedMs` 会叠加 seek 起点和 runtime 已发送媒体时长，`receiverReport` 会携带最新 RTCP RR 原始反馈和派生质量指标，inactive `streamId` 可在下一次 `startStream` 时复用。
- N-API 已有 TypeScript Vitest smoke test：动态生成本地 WAV，通过 `startStream` 推本地文件、有界 HTTP URL 和 live HTTP RTP 到 localhost UDP，回送 RTCP RR，并确认 `getStatus().receiverReport` 暴露原始字段和派生质量指标；`npm test` 会在 Vitest 前执行 `tsc --noEmit`，让测试消费 addon 生成的 `index.d.ts`；测试按 RTP/RTCP、source policy、live stream、playback controls 和 loudness 拆分，同时覆盖 source policy 校验、maxBytes 拒绝、关闭 artifact cache 后不复用、HTTP 403 触发 `sourceRefreshNeeded`、next 预载鉴权失败不影响 current 播放、live current 可推 RTP、`switchTrack` 可切到 live current、`refreshCurrentSource` 可在同一 stream 上重启 live current、live seek 拒绝、live next/preload 显式 unsupported、non-seekable seek 拒绝、`setEventCallback` 事件通知、`drainEvents` 可靠补偿队列、ReplayGain 推荐、防削波限制、`setVolume`、`pauseStream`、`resumeStream` 和 `seekStream` 的 addon 边界。

## 设计已预留但未完成

- RTCP Receiver Report 的基础解析、status 暴露、单次最新 RR 派生质量指标、metrics 层窗口聚合和带 snapshot 的质量等级变化事件已实现；自动降码率或自动策略动作尚未实现。
- Tokio actor/task lifecycle 已成为 N-API runtime 控制面；当前 RTP media worker 仍是受控 blocking worker handle，复用纯 `RtpPacer` 和 paced drain，由 actor action 以 generation-scoped handle 启动、暂停、恢复、取消、stop/join。worker 内部已经减少固定 sleep 空转并加上 CPU permit lane；后续如果要把 UDP/timer/source IO 改为 async task，应在保持这个 actor/action/task-tree 边界不退化的前提下替换 worker 内部实现。
- bounded HTTP tempfile source、同次 resolve 内 Range resume、N-API source policy 配置和基础 artifact LRU cache 已实现并测试；live streaming 已有 bounded byte pipe、真实 HTTP body reader、non-seekable Symphonia stream decoder wiring、live slot pipeline、live RTP runtime、基础 HTTP retry 和 N-API current playback，且 `startStream` 与运行中 `switchTrack` 都可进入 live current；live startup source failure 会同步拒绝并清理 stream，401/403 startup 会额外排队 `sourceRefreshNeeded`；运行中 current source failure 会发 `error`，没有 next 时进入 `idle` 并发 `nextNeeded`；TS 可用 `refreshCurrentSource` 在同一 stream/transport/source policy 下重启 current。更高阶的自动刷新/reconnect 策略尚未实现，跨进程/跨 resolve 持久化 cache metadata 尚未实现。
- ReplayGain 显式推荐 API 已实现，采用 Node/TS 传入 metadata、Rust 计算推荐 `gainDb`、调用方显式应用的边界；EBU R128 loudness metadata 自动分析、preload/offline scan 和运行中渐变尚未实现。
- pacing late 已有单 tick 指标，真实 localhost RTP 短 integration 已覆盖 packet count、序列号和 timestamp 单调；`criterion_pipeline` 已接入 Criterion，覆盖 fake pipeline worker + sender drain 统计型 microbenchmark。更多 codec corpus 和真实源规模调参数据等到有 profiling 需求后再补，不在当前代码库里维护自定义 long-soak 观测基线。
- seek 已覆盖文件 artifact runtime：actor 生成新 generation，runtime 取消旧 current，并用 Symphonia `SeekMode::Accurate` 从目标秒数附近重新打开 decoder；当前对外 API 仍是秒级 seek。
- pause/resume、seek、运行中 `setNext` 和运行中 `switchTrack` 已覆盖本地文件和有界 HTTP URL runtime；`switchTrack` 也已覆盖切到 live current，并保持 live non-seekable。
- 当前 N-API 已覆盖本地文件、有界 HTTP URL 和 live current 的 `startStream`、`pauseStream`、`resumeStream`、`seekStream`、`stopStream`、`setNext`、`switchTrack`、状态查询、事件 callback/轮询、音量、transport config 校验和 source resolver config 校验。
- `startStream.next` 与运行中 `setNext` 已支持本地文件和有界 HTTP URL next preload/promotion；live next/preload 显式 unsupported，不会进入 artifact cache 或无限 tempfile；`switchTrack` 已可按 actor action 取消旧 current/next 并启动 Node 指定的新 current/next。
- 具体平台加密算法尚未实现；目前只有 packet protector 扩展点和配置校验。

## 当前一致性结论

当前实践和核心设计方向一致：

- 没有把 playlist、鉴权、voice gateway 协商放进 Rust core。
- 没有让 sender 临时解码或编码。
- 没有使用 unbounded 音频队列。
- 没有手写 RTP packet wire format。
- source artifact 与 TrackSlot 生命周期已经分离，基础 LRU cache 只保存完整 artifact，不保存 slot/pipeline。
- transport config 已集中建模，不再把 port、SSRC、payload type 等参数散落在构造函数里。
- runtime worker 回来的 actor events 已有 N-API `setEventCallback` 和 `drainEvents` 出口。callback 是低延迟通知，`drainEvents` 是可靠补偿队列；Node 可接收 `stateChanged`、`nextNeeded`、`error` 等事件。
- N-API 已有显式 shutdown 和 Drop 兜底，不依赖调用方逐个 stop 才能释放 playback/preload/source cache。

需要继续推进的关键缺口不再是 actor/task lifecycle、live current playback、live `switchTrack`、基础断流事件语义、底层 current 恢复入口或 RTCP quality 策略输入，而是 TS 层 live 自动刷新/reconnect 策略、更多 codec/source/cache 规模调参数据，以及后续策略层的自动网络反馈动作。当前代码已经能正常解码本地 MP3/WAV、有界 HTTP URL 和 finite live HTTP WAV 并按 RTP pacing 推到 UDP，且支持 HTTP 下载中断后的同次 resolve Range resume、live HTTP 503 后基础 retry、live startup 失败清理与 source refresh 事件、运行中 source failure 的 `error`/`nextNeeded` 语义、`refreshCurrentSource` 同 stream current 恢复、RTCP Sender/Receiver Report 基础链路、最新 RR 派生质量指标、RTCP quality window metrics 和带 snapshot 的 `networkQualityChanged` 事件、pause/resume/seek 不 burst 发送 backlog、start-time next preload/promotion、运行中 `setNext`、运行中 `switchTrack`、N-API 事件 callback，以及 per-stream Tokio bounded actor mailbox 驱动的 runtime task lifecycle。
