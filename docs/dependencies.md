# 依赖策略

## 原则

只引入对边界有明确价值、维护活跃、测试充分、能降低实现风险的库。协议、编解码、重采样等领域逻辑优先使用成熟库；业务状态机、背压和播放策略由项目实现并测试。

依赖准入标准：

- 必须解决明确边界问题，不能只为少写少量代码引入。
- 必须有公开文档、许可证清晰、版本发布正常。
- 协议和媒体领域优先选已有测试覆盖的库，项目不重复实现 wire format 或 codec 细节。
- 必须能被 trait 隔离，避免核心播放模型绑定到某个 backend。
- 必须能在 CI 中被单元测试、集成测试或 fixture 测试覆盖。
- 非热路径依赖从简，profiling 证明需要后再引入并发/性能专用结构。

## 目标依赖

### Node 边界

- `napi`
- `napi-derive`
- `napi-build`

用途：暴露 Node addon、生成类型、处理 ThreadsafeFunction。napi 层只做类型映射和 runtime 持有，不包含播放器核心逻辑。

### Runtime

- `tokio`
- `tokio-util`
- `futures`

用途：actor、mpsc/oneshot、UDP、timer、HTTP IO、取消。`tokio-util::sync::CancellationToken` 用于结构化取消。

### Source

- `reqwest`
- `tempfile`
- `lru`

用途：HTTP 下载、seekable 临时文件源和 bounded artifact LRU cache。Source 层根据 seekability 选择 File/TempFile/Streaming 策略，缓存淘汰使用社区 LRU 实现，不在项目里维护自定义 LRU 队列。

### Audio decode

- `symphonia`

用途：默认音乐 demux/decode backend。Symphonia 是 pure Rust audio decoding and multimedia demuxing framework，并通过 feature flags 启用格式和 codec 支持。

可选：

- `ffmpeg-next` 或 `ffmpeg-sys-next`

用途：兼容 backend。只有真实曲库或目标平台要求证明 Symphonia 覆盖不足时启用。FFmpeg backend 必须被封装在 `DecoderBackend` trait 后面。

### DSP/resample

- `rubato`

用途：sample rate conversion。Rubato 提供 chunk-based resampling，适合 f32 planar 内部格式和预分配 buffer。

使用要求：

- 实时路径使用 `process_into_buffer`，不使用会为每次调用分配输出的 convenience API。
- resampler 和 input/output buffer 在进入热路径前创建。
- 不启用会影响实时路径的日志特性。

### Opus

优先 spike：

- `opus`

如果高层 crate 缺少必要 CTL：

- `opus-sys` + 项目内最小安全 wrapper

wrapper 必须隐藏 unsafe，只暴露：

```rust
struct OpusEncoder { ... }
impl OpusEncoder {
    fn encode(&mut self, frame: AudioFrame) -> Result<Bytes>;
}
```

优先使用 float encode 路径。这样内部 f32 DSP 可以直接进入 Opus，减少 i16 转换和削波风险。

### RTP/RTCP

- `rtp`
- `rtcp`

用途：协议包结构、marshal/unmarshal、RTCP Sender Report/Receiver Report。项目不手写 RTP/RTCP wire format。

`rtp` crate 暴露 header、packet、packetizer、sequence 等模块；`rtcp` crate 明确实现 RFC 3550/5506 的 RTCP packet 编码/解码。

### Buffer/state/error/log

- `bytes`
- `thiserror`
- `tracing`
- `tracing-subscriber` only for examples/tests/binaries, not library global init

策略：

- Opus/RTP payload 用 `Bytes`/`BytesMut`。
- PCM 用 `Vec<f32>`/复用 buffer，不用 `Bytes`。
- 错误用稳定 code，不到处返回字符串。
- 高频 sender 不写 info log，指标走 counters/snapshot。
- library 不初始化全局 logger/tracing subscriber，由宿主或测试设置。

### Metrics

使用 `metrics` crate 作为唯一 metrics facade。core 直接用 `metrics::counter!`、`metrics::gauge!` 和 `metrics::histogram!` 发指标，不维护项目自定义 metrics sink trait。默认没有 recorder 时由 `metrics` facade 走 no-op；集成测试通过 dev-only `metrics-util` debugging recorder 和 local recorder 捕获指标；宿主或最终 binary 负责安装 Prometheus/OpenTelemetry/StatsD 等 exporter。

library 不初始化全局 recorder。需要隔离测试或 worker thread 局部采集时，可在 runtime config 里传入 `metrics::Recorder`，由 runtime 在 source resolve 和 worker loop 边界设置 local recorder。不要在音频热路径引入 exporter 依赖或阻塞 IO。

不默认引入 SIMD crate。`std::simd` 仍是 nightly experimental；稳定实现先依赖 scalar + LLVM auto-vectorization。只有 profiling 证明 gain/downmix/interleave 是瓶颈时，才添加小范围 target-specific SIMD 模块。

Registry 使用 `RwLock<HashMap<StreamId, StreamHandle>>` 或等价简单结构。DashMap 只有 profiling 证明 registry 锁竞争后再引入。

错误类型使用项目内 enum + `thiserror`：

```rust
enum ErrorCode {
    InvalidSource,
    SourceTimeout,
    SourceAuthExpired,
    NotSeekable,
    UnsupportedFormat,
    DecodeError,
    ResampleError,
    EncodeError,
    RtpSendError,
    StreamClosed,
    Busy,
    Internal,
}
```

napi 层只把 `ErrorCode` 映射到 JS error/status，不把 Rust 内部错误字符串当作稳定 API。

## 不作为核心依赖

### rodio/kira

适合本地播放或游戏音频，不适合控制 Opus frame、RTP timestamp 和网络 playout。

### GStreamer

媒体 graph 强，但部署重。当前目标不需要完整 graph。

### full WebRTC stack

当前目标是向已知 UDP 端点发送 Opus RTP，不需要 ICE/DTLS/SRTP/SDP/transceiver。未来需要完整 WebRTC 协商时再评估。

### mpg123 快路径

旧 C++ 使用 mpg123 不代表 Rust 版需要保留。只有基准证明 MP3 解码是瓶颈且 Symphonia/FFmpeg 无法满足时再讨论。

### DashMap 默认化

registry 不是热路径。简单结构更易测、更易推理。

## Cargo feature 建议

```toml
[features]
default = ["decoder-symphonia", "resampler-rubato", "transport-rtp"]
decoder-symphonia = ["dep:symphonia"]
decoder-ffmpeg = ["dep:ffmpeg-next"]
resampler-rubato = ["dep:rubato"]
transport-rtp = ["dep:rtp", "dep:rtcp"]
bench = []
test-fixtures = []
```

## 参考资料

截至 2026-06-24 核对。Node.js 运行线选择 Node 24 LTS，`package.json` 当前约束为 `>=24.17.0 <26`，不以 Node 26 Current 作为运行目标。TypeScript 类型包跟随 Node 24 major；当前使用 `@types/node` 24.x。

- [rtp docs.rs](https://docs.rs/rtp/latest/rtp/)：`rtp` 0.17.1，MIT/Apache-2.0，包含 `header`、`packet`、`packetizer`、`sequence` 等模块。
- [rtcp docs.rs](https://docs.rs/rtcp/latest/rtcp/)：`rtcp` 0.17.1，说明其按 RFC 3550 和 RFC 5506 实现 RTCP packet 编码/解码。
- [Symphonia docs.rs](https://docs.rs/symphonia/latest/symphonia/)：说明其为 100% pure Rust audio decoding and multimedia demuxing framework，格式和 codec 通过 feature flags 启用。
- [rubato docs.rs](https://docs.rs/rubato/latest/rubato/)：Rust audio sample rate conversion library，支持 chunk-based resampling。
- [CancellationToken docs.rs](https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html)：用于结构化取消。
