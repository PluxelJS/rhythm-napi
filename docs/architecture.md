# 目标架构

## 目标

用 Rust + napi-rs 实现 `@music/streamer`，形成一个 Node.js 原生 addon。Rust 模块负责音乐解码、Opus 编码和 RTP/RTCP 推流；Node 层负责业务编排、playlist/order/play mode 和源地址解析。旧 C++ 的 protobuf/ZMQ、全局单例、下载/解码/推流事件拼装不作为继承目标。

本页描述目标架构和必须保留的设计方向。当前已经实现的范围、尚未实现的模块和验证状态见 [status.md](status.md)。如果本页与当前代码完成度看起来冲突，以 `status.md` 的完成度说明为准，以本页的不变量作为后续实现约束。

## 顶层边界

```text
TypeScript
  -> Streamer napi class
  -> music_stream::Engine
      -> StreamSession
          -> PlayoutSlots
          -> Source
          -> audio
              -> Decode
              -> DSP
              -> Resample
              -> Opus
          -> RtpSession
          -> EventSink
```

对外 API 应以对象和方法表达，不再暴露 protobuf：

```ts
class Streamer {
  startStream(options: StartStreamOptions): Promise<StreamStatus>;
  stopStream(streamId: string): Promise<void>;
  getStatus(streamId: string): StreamStatus;
  play(streamId: string): StreamStatus;
  pause(streamId: string): StreamStatus;
  seek(streamId: string, seconds: number): Promise<StreamStatus>;
  setNext(streamId: string, next: TrackSource | null): Promise<StreamStatus>;
  switchTrack(streamId: string, current: TrackSource, next?: TrackSource): Promise<StreamStatus>;
  setVolume(streamId: string, volume: number): StreamStatus;
  onEvent(callback: (event: StreamEvent) => void): void;
}
```

Rust API 不提供 `updatePlaylist`、`getPlaylist`、`setPlayMode`、`skipByOffset` 这类列表语义。TS 根据自己的 playlist、随机/循环/推荐策略决定当前曲目和下一曲目，再把“当前/下一首”交给 Rust。Rust 只保证这些曲目的实时准备、切换和推流正确。

## 设计硬约束

这些约束比具体实现技巧更重要：

- Rust 内核只处理 `current_slot` 和 `next_slot`，不维护完整 playlist。
- 所有 seek/switchTrack/track switch 都递增 generation。
- 任何进入 encoded queue 的 Opus payload 都是 immutable bytes。
- RTP/RTCP wire format 交给 `rtp`/`rtcp` crate。
- RTP sender 不执行 decode、resample、Opus encode。
- Source artifact 和 TrackSlot 是不同生命周期，不把下载 buffer 绑死到播放实例。
- 热路径先保证所有权清晰和可测，再基于 profiling 做 buffer pool 或 SIMD。

## 旧 C++ feature 的取舍

旧 C++ 版本里有一些方向是正确的，但实现过于拼装。Rust 版本应保留目标，不保留耦合方式：

| 旧 feature          | 是否保留           | Rust 目标设计                                                                                   |
| ------------------- | ------------------ | ----------------------------------------------------------------------------------------------- |
| 预载下一首          | 保留并强化         | TS 提供 next，Rust 建立 `next_slot`，受限 resolve/load/probe/prime，promote 时原子切 generation |
| 暂停/继续           | 保留               | pause 只冻结 RTP playout，上游靠 bounded queue 背压，不取消整条管线                             |
| seek                | 保留               | seekable source 重建 decoder、递增 generation、清旧队列，RTP timestamp 单调                     |
| 音量                | 保留并改进         | API 使用感知响度曲线，内部 f32 增益 + limiter，不做粗糙 int16 线性乘法                          |
| 边下边播            | 保留为 Source 能力 | 明确区分 seekable file、tempfile、memory、live streaming，不用一个自增长 buffer 处理所有输入    |
| 播放列表模式        | 上移 TS            | Rust 不维护列表顺序、随机、循环，避免业务逻辑和实时内核耦合                                     |
| protobuf/ZMQ 控制面 | 删除               | napi-rs typed API + event callback                                                              |

旧 C++ 还暴露出几个反面经验：下载完成、解码完成、播放完成不能混为一个事件；FFmpeg/mpg123 返回码不能跨 backend 混用；RTP 发送节奏不能被编码队列 backlog 牵着走；seek/skip 后清队列必须有 generation 防线。Rust 设计用明确状态机、trait backend、bounded queue 和 generation 处理这些问题。

旧项目值得保留但要重构表达的正向设计：

- producer/consumer/sender 分离：source/decoder/encoder 与 RTP sender 不在同一实时循环里。
- ahead-of-time decode/encode：发送前准备未来一小段 ready frame，降低曲间空白和 sender 抖动。
- ring buffer 思路：用 bounded queue 表达背压，但容量按播放毫秒和水位配置，不写死任意帧数。
- 预分配热路径 buffer：decode、转换、resample、Opus 输出和 RTP packet scratch 尽量复用。
- 对齐和 SIMD 方向：保留“热循环可优化”的意识，但 Rust 版先用清晰 scalar 基线和 profiling 决定是否加 target-specific SIMD。
- 动态 pacing 意识：sender 以 monotonic clock 和 RTP timestamp 建模 drift/late/underrun，不被上游 backlog 拉着 burst。
- 格式探测和 backend fallback：保留 trait backend 能力，但不让具体 decoder 返回码或状态泄漏到播放状态机。

## 播放模型

### TrackSource、TrackPlan、TrackSlot

```text
TrackSource  静态输入：id、url/path、headers、seekable hint
TrackPlan    解析结果：最终 source 类型、headers、seekability、长度策略
TrackSlot    一次播放运行：generation、source、decoder、queues、progress
```

同一个 TS playlist 条目在单曲循环或重新播放时会创建新的 `TrackSlot`。Rust 不需要知道它在 playlist 中的 index 或 play mode。

### Current slot 与 preload slot

每个 stream 同时最多拥有：

- `current_slot`：正在播放或即将播放的 track。
- `next_slot`：TS 指定的下一首预载对象。

`next_slot` 只能做 resolve、load、probe 和受限 priming，不能直接向 RTP sender 写帧。slot promotion 只能由 `StreamActor` 完成。TS 可以随时用 `setNext` 替换或清空 next slot。

### Generation

每次 seek、switchTrack、track switch 都递增 generation。所有 PCM/Opus frame 都携带 generation。

```rust
struct OpusFrame {
    generation: u64,
    payload: Bytes,
    samples_per_channel: u32,
    marker: bool,
    track_position_samples: u64,
}
```

RTP sender 只发送 active generation。这个规则是防止旧 frame 泄漏的最后防线。

## 播放状态机

```text
Idle
  -> Starting
  -> Buffering
  -> Playing
  -> Paused
  -> Switching
  -> Stopping
  -> Stopped
  -> Error
```

语义：

- `Paused` 冻结 playout，不推进 RTP media clock；上游通过 bounded queue 自然背压。
- `Switching` 原子切换 generation，清空旧队列，必要时 promote next slot。
- `Stopped` 是终态，所有 task、socket、event handle 都已释放。
- track 级错误不会直接进入 stream `Error`。如果 next slot 已 ready，Rust 可以 promote next slot；如果没有 next，Rust 发出 `nextNeeded/error` 事件等待 TS 决策。连续错误超过阈值才升级为 stream `Error`。

## 播放命令语义

### pause/play

pause 不取消整条管线，只冻结 RTP sender。decode/encode 可以继续到小型队列上限后自然阻塞。resume 重新建立 sender 的 pacing base，RTP sequence/timestamp 继续单调递增。

### switchTrack

TS 决定跳到哪一首，Rust 执行一次原子切换：

```text
mark current generation stale
cancel current decode/encode
clear decoded/encoded queues
use TS-provided target track
promote next slot if target matches
otherwise create new TrackSlot
prebuffer target generation
send marker on first packet
```

### seek

seek 对 seekable source 生效。实现策略是重建 decoder，而不是在活跃 decoder 中共享 mutable seek 状态：

```text
increase generation
cancel decoder/encoder for current slot
clear queues
open decoder at target timestamp
prime frames
resume pacing with monotonic RTP timestamp
```

## 预载策略

预载目标是减少曲间空白，不是无限提前解码。Rust 不推导 playlist 下一首，只准备 TS 提供的 next track。

```text
lookahead = exactly the TS-provided next track
next slot can resolve/load/probe
next slot priming is bounded and lower priority than current playback
next slot never sends before promotion
```

next slot 的来源：

- `startStream` 可携带 `current` 和 `next`。
- `setNext` 可在任意时间更新下一首。
- current 接近结束但没有 next 时，Rust 发出 `nextNeeded` 事件，TS 应尽快调用 `setNext`。
- current 结束且 next 已 ready 时，Rust 原子 promote。
- current 结束但 next 缺失时，Rust 进入 `Idle/WaitingForNext`，等待 TS 提供下一首。

TS playlist 替换不会直接进入 Rust。TS 只需要根据新列表调用 `setNext` 或 `switchTrack`。这让列表业务和实时播放边界分离。

### 无缝切歌目标

目标不是简单“当前曲 EOF 后再打开下一首”，而是让下一首在切换前进入可播放状态：

```text
next_slot target state before promotion:
  source ready
  decoder probed
  normalized format known
  first Opus frames primed within a bounded queue when source/CPU budget allows
```

切换时：

```text
current generation marked stale
sender drains or drops according to switch policy
next generation activated
prebuffer satisfied
first packet marker set
RTP timestamp remains monotonic
```

默认目标是“无明显曲间空白”。严格 gapless 需要处理 encoder delay、container gapless metadata、末尾 padding 和跨曲时钟对齐，作为同一模型下的增强能力，而不是改变状态机。

这里继承旧 `voice_sender` 的正确方向：发送前就应该有一段未来音频已经解码、处理并编码好，RTP sender 只按时钟取 ready frame。Rust 版把这个策略显式建模为 `current_slot` 的 prebuffer 和 `next_slot` 的 bounded priming，而不是在 sender tick 到来时临时解码。

### next 突然改变

TS 可以随时调用 `setNext(newNext)`。Rust 处理规则：

```text
if old next is absent:
  create next_slot(newNext)
if old next exists and same stable track key:
  keep existing next_slot, update metadata only if safe
if old next exists and different:
  cancel old next_slot CPU work
  decide whether retained artifact can enter source cache
  create next_slot(newNext)
if newNext is null:
  cancel next_slot; current continues
```

`stable track key` 不应只用展示名，应由 TS 传入稳定 `id` 或 `{provider, media_id, url_signature}`。Rust 不判断业务等价性。

### 已下载/半下载 next 如何处理

next 被替换时，已经下载的数据不能简单粗暴都丢，也不能无界缓存。设计为 artifact cache：

```text
SourceArtifact:
  key
  kind = memory | temp_file | streaming_not_cacheable
  size
  complete
  seekable
  expires_at
```

处理策略：

- 完整且 seekable 的临时文件可以进入小型 LRU cache，供 TS 之后再次设为 next/current 时复用。
- 半下载文件默认取消并删除；当前只支持同一次 resolve 内的临时 Range resume，不把半下载文件放入 LRU，也不保留跨 resolve 的 resume metadata。
- live/无限流不进入 artifact cache。
- cache 有总大小、单文件大小、TTL、stream 归属和显式 cleanup，不能变成隐藏下载器。

这比旧 C++ 把下载 buffer 和播放 task 绑在一起更清晰：下载结果是 source artifact，播放实例是 track slot，两者生命周期不同。

### nextNeeded 事件

Rust 不知道 TS playlist，但它知道何时需要 next：

```text
current remaining time <= prepare_threshold and next_slot absent
current source is live and TS policy wants continuation
current track failed and next_slot absent
current track EOF and next_slot absent
```

Rust 发送 `nextNeeded`，TS 决定是否 `setNext`、`switchTrack`、停止或保持等待。

## 音频管线

内部统一管线：

```text
Source
  -> DecoderBackend
  -> Normalizer
  -> Resampler
  -> Volume/Limiter
  -> FrameAssembler
  -> OpusEncoder
  -> RtpSession
```

推荐内部 PCM 格式：

```text
sample_rate = 48000 after resample
channels = stereo
dsp_format = f32 planar
opus_input = encoder wrapper decides f32 or i16
frame_duration = 20ms default, 40ms configurable
```

decoder backend 只负责输出原始 PCM chunk。sample format、channel layout、resample、volume 和 limiter 集中在 pipeline 层处理，不散落到播放逻辑。

### 特化后的 PCM 数据路径

不要为了“通用音频框架”抽象过度。当前任务固定输出 Opus/RTP，因此内部只保留一种主路径：

```text
decoder native output
  -> copy/convert into reusable f32 planar scratch
  -> optional downmix to stereo
  -> optional resample to 48 kHz
  -> gain/limiter
  -> frame assembler creates 20ms/40ms f32 interleaved Opus input
  -> opus encode_float into reusable u8 output buffer
```

设计约束：

- decode/normalize/resample/encode lane 必须领先 RTP sender 一个受限时间窗。sender 缺帧时只能进入 underrun/rebase，不能在 sender 热路径同步解码。
- decoder 每次工作按“可取消的小批量时间窗”推进，而不是只解出一个待发送 frame。批量窗口应足够摊薄 decoder/backend 调用成本并提升缓存局部性，但不得大到让 seek/switch/stop 延迟明显增加。
- 主 DSP 格式是 f32 planar，避免多套 sample format 在管线里传播。
- Opus 输入优先用 f32 interleaved，使用 encoder 的 `encode_float` 能力，避免最后一步强制转 i16。
- 只有 RTP payload 使用 `Bytes`；PCM 用复用的 `Vec<f32>` / scratch buffer，不用 `Bytes`。
- 每个 `TrackSlot` 持有自己的 decoder/resampler/assembler scratch，避免跨 track 共享导致锁和生命周期复杂化。
- 每个 stream 可以有一个小型 buffer pool 供 current/next slot 复用，但不要做全局复杂 allocator。

### Buffer 复用

热路径不应每帧分配。每个 active slot 预分配：

```text
decode_scratch_per_channel: Vec<f32>
resample_in: Vec<Vec<f32>> or channel-major adapter
resample_out: Vec<Vec<f32>>
stereo_planar: [Vec<f32>; 2]
opus_interleaved_frame: Vec<f32> sized to frame_samples * 2
opus_output: Vec<u8> sized to max opus packet
rtp_packet_buffer: BytesMut sized to mtu
```

复用规则：

- decoder chunk 边界可以变化，但写入 scratch 后立即归一化到内部格式。
- frame assembler 只保存不足一个 Opus frame 的尾部样本。
- encoded `Bytes` 进入队列后不可再修改；下一个 packet 复用新的 `BytesMut` 或从 small pool 取 buffer。
- next slot priming 的 buffer 独立于 current，避免 promote 时发生所有权搬移和锁等待。

队列容量应按时间而不是任意帧数描述：

```text
decode batch target: 40-120ms PCM per worker turn
decoded queue target: 100-300ms PCM
encoded queue target: 80-500ms Opus
pause upper bound: configurable, e.g. encoded <= 2s
next priming: small, e.g. 100-300ms encoded
```

这些值是策略参数，必须能通过 benchmark 和真实接收端调优。

### Resample 方案

默认使用 rubato 做 resample。rubato 文档建议实时场景使用 `process_into_buffer`，将输出写入预分配 buffer，避免 `process` 每次分配。

策略：

```text
if input sample_rate == 48000 and channel layout is stereo:
  bypass resampler
else:
  convert to f32 planar
  downmix to stereo if needed
  call rubato process_into_buffer with preallocated buffers
```

resampler 类型选择：

- 固定比例常见路径，如 44.1k -> 48k，使用固定比例 sinc resampler。
- live stream 或源采样率可能变化时，重建 resampler，而不是在一个实例里处理不兼容格式。
- 不在 RTP sender 线程执行 resample；resample 属于 CPU-bound lane。

质量策略：

- 默认选择音乐质量优先但 CPU 可控的 sinc 配置。
- 提供 `quality = balanced | high | lowLatency` 配置，映射到 rubato 参数。
- next slot priming 使用与 current 相同质量，避免切换瞬间音色差异。

### SIMD 策略

SIMD 只用于明确的热循环：

- gain。
- clamp/soft clip。
- planar/interleaved 转换。
- simple downmix。

不为了 SIMD 引入复杂抽象。`std::simd` 仍是 nightly experimental API，不能作为默认稳定实现。默认先写清晰的 scalar 版本，让 LLVM auto-vectorize；profiling 证明瓶颈后再用小模块封装 SIMD：

```text
dsp/scalar.rs
dsp/x86_avx2.rs behind target_feature
dsp/aarch64_neon.rs behind target_feature
```

SIMD 模块必须满足：

- 与 scalar 输出误差有测试约束。
- 运行时 feature detection。
- 没有 SIMD 支持时自动回退 scalar。
- 不跨越管线边界泄漏 SIMD 类型。

优先不要引入 nightly portable SIMD 或大体量 DSP 框架。对本任务，resample 和 Opus 已由专门库处理，项目自写 SIMD 只应覆盖简单样本级操作。

## 音量与响度

对用户暴露的音量不应直接等于 PCM 线性倍数。人耳对响度接近对数感知，UI 音量更适合映射为 dB gain。

推荐 API：

```ts
setVolume(streamId, volume: number) // 0.0..1.0 user volume
setGainDb(streamId, gainDb: number) // optional expert API, e.g. -60..+6 dB
recommendReplayGain(input)          // explicit helper; returns gainDb recommendation
```

映射策略：

```text
volume = 0      -> mute
volume = 1      -> 0 dB
0 < volume < 1  -> min_db + (0 - min_db) * curve(volume)
default min_db  -> -60 dB
curve           -> perceptual curve, e.g. volume^2 or equal-power style mapping
```

内部处理：

```text
decoder output
  -> normalize to f32
  -> gainDb explicitly chosen by TS, possibly from recommendReplayGain
  -> limiter/soft clip
  -> Opus frame assembly
```

要求：

- 音量变更对后续 PCM frame 生效。
- 不重编码已进入 encoded queue 的旧帧，除非 TS 要求立即生效并接受短暂清队列。
- 支持 mute 快路径：RTP 可继续发送 Opus silence 或暂停音频发送取决于接收端要求；默认不要破坏 RTP session。
- 防止 >0 dB 增益削波，使用 limiter 或 soft clip。
- ReplayGain 当前只做显式 recommendation，不自动分析、不自动应用；TS 必须把推荐值传给 `startStream` 或 `setGain`。

后续可加入 EBU R128 loudness analysis、预载/离线 scan 和运行中渐变，但它们属于增强，不应阻塞基础推流。

## Decoder backend

默认 backend 是 Symphonia。FFmpeg 是可选兼容 backend，受 feature gate 控制。播放逻辑依赖 `DecoderBackend` trait，不依赖具体库。

```rust
trait DecoderBackend: Send {
    fn info(&self) -> AudioInfo;
    fn seek(&mut self, target: Duration) -> Result<()>;
    fn next_chunk(&mut self) -> Result<Option<AudioChunk>>;
}
```

Symphonia 不支持或真实曲库兼容性不足时再启用 FFmpeg 兼容 backend。兼容 backend 必须仍输出统一 `AudioChunk`，不能接管播放状态机。

## RTP/RTCP 会话

项目不手写 RTP/RTCP wire format。协议层使用 `rtp` 和 `rtcp` crate。项目代码只维护会话语义：

- remote IP / RTP port / optional RTCP port。
- payload type。
- SSRC。
- MTU。
- RTCP mux。
- optional Opus bitrate hint。
- encryption mode and key material passed through from Node, without doing auth or gateway negotiation in Rust。
- sequence number lifecycle。
- RTP timestamp lifecycle。
- Opus frame 到 RTP packet 的一包一帧映射。
- RTCP Sender Report 周期和统计。
- prebuffer、underrun、late frame drop。
- generation 过滤。

RTP 规则：

```text
payload = one Opus packet
timestamp step = samples_per_channel
sequence = wrapping increment per packet
timestamp never goes backwards across seek/switchTrack
marker = stream start / track switch / seek first packet
```

RTCP 规则：

```text
send Sender Report periodically when RTP session is active
rtcp_mux sends to RTP socket
non-mux sends to rtcpPort
maintain packet count and octet count
parse Receiver Report non-blockingly
filter Receiver Report blocks by local SSRC
derive latest loss ratio, jitter time and RTT when LSR/DLSR are valid
```

RTCP feedback 的底层 parser 只产出最新快照和确定性单位换算，不做窗口聚合、趋势告警或自适应策略。后续如果要做网络质量事件，应该在 metrics/session 层消费这些快照，保持 transport 层只负责协议语义。

不引入完整 WebRTC stack，除非未来需要 ICE/DTLS/SRTP/SDP 或完整 WebRTC 协商。

### 连接配置边界

Node/TS 负责 voice gateway、鉴权、endpoint 选择和密钥获取。Rust 只接收已经解析好的 transport 参数：

```text
remote_ip
remote_rtp_port
remote_rtcp_port optional
local_ip/local_rtp_port optional
audio_ssrc
audio_payload_type
rtcp_mux
mtu
opus_bitrate_bps optional
encryption mode + key material optional
```

这些字段必须收敛为一个 `RtpTransportConfig`，而不是散落在 startStream 的多个参数里。`RtpTransportConfig` 负责范围校验和默认值归一化，`RtpPacketizerConfig` 只表达 RTP header 需要的 payload type、SSRC 和 MTU。

加密实现必须在 packet bytes 已由 `rtp` crate marshal 后、UDP send 前完成。当前代码提供 `RtpPacketProtector` 插入点；具体平台算法由后续实现或上层构造 protector。未安装 protector 时，配置了非 `none` 加密模式必须返回明确错误，不能静默明文发送。鉴权 token、session id、gateway websocket 状态不进入 Rust core。

## Source 策略

Source 层按 seekability 和容器能力选择实现：

```text
local file -> FileSource
small bounded HTTP -> MemorySource
seekable HTTP/container -> TempFileSource
live/non-seekable -> StreamingSource
container requires seek -> reject streaming or spool to seekable source
```

Rust 不解析业务缓存 URL。Node 层应把平台/插件/cached id 解析为最终 URL、headers、cookie、referer 后传入 Rust。

### 有限文件与无限流

Source 必须显式区分 bounded media 和 live/unknown-length stream。

```text
BoundedFileSource:
  known or discoverable duration
  seekable
  supports preload and artifact cache

ProgressiveHttpSource:
  content-length may be known
  can spool to temp file
  may become seekable after enough data or full download

LiveStreamSource:
  unknown duration
  non-seekable
  no automatic next promotion by EOF unless stream ends
  no artifact cache
```

博客/播客里的“无限 MP3 流”通常应视为一个 `LiveStreamSource` 任务，而不是 playlist 里无限展开的很多 track。它的行为：

- `seek` 返回 `NOT_SEEKABLE`，除非 TS 提供 DVR/window 能力。
- `timeTotalMs = null`。
- 预载 next 可存在，但不会按 remaining time 自动触发；TS 可按业务策略手动切换。
- 断流后按 source retry policy 重连，超过阈值后发 error/nextNeeded。
- backpressure 必须从 encoded queue 传回 source reader，不能无限读网络流。

### Source retry 与重定向

HTTP source 策略：

- TS 提供 headers/cookie/referer/proxy 语义，Rust 只使用。
- Rust 可以处理标准 redirect、timeout、range resume，但不理解业务鉴权刷新。
- 401/403 或业务签名过期时，Rust 映射为 `SOURCE_AUTH_EXPIRED` 并发 `sourceRefreshNeeded`；TS 重新解析 URL 后，对同一 current 调用 `refreshCurrentSource`，或按 playlist 策略调用 `switchTrack`。

这避免 Rust 内部复制业务平台逻辑。
