# Rhythm NAPI

Rhythm NAPI 是面向 Node.js 宿主的 Rust 音频推流内核。它接收已经确定的音源和 RTP
传输参数，把本地文件、有界 HTTP 音频或 live HTTP 音频统一解码为 48 kHz stereo PCM，
编码为 20 ms Opus frame，并按实时节拍发送 RTP/RTCP。

它解决的是媒体节点问题，不是播放器产品策略问题。playlist、循环/随机、推荐、业务 URL
解析、鉴权刷新、网关协商和跨节点调度由 TypeScript 宿主负责。

## 设计核心

```text
TypeScript policy
       │ commands / source refresh / next selection
       ▼
StreamActor ── transactional actions ── StreamRuntime
                                          ├── current producer
                                          ├── next producer
                                          └── persistent RTP/RTCP sender

async source I/O              bounded blocking CPU work           async realtime I/O
file / URL / live ──────────> decode → resample → DSP → Opus ───> OpusQueue ───> RTP/RTCP
```

- 每个 stream 只有一个长期 sender。切歌、seek 和 next promotion 只替换媒体 generation，
  不重置 SSRC、RTP sequence 或 timestamp。
- HTTP、UDP、timer、清理等待和 Node Promise 边界使用 async；Symphonia、Rubato、DSP 和
  libopus 只在受控 blocking worker 中执行。
- producer 与 sender 之间只有一个按媒体时长计量的有界 Opus queue，不存在第二层 encoded
  backlog。
- 零起点有界 URL 使用可多读 growing tempfile 渐进解码；完整响应才成为 seekable cache
  artifact。无扩展名签名 URL可通过显式 `formatHint`进入渐进路径，相同内容身份的并发请求共享
  一次传输。
- current 在 CPU、blocking worker、HTTP 和 tempfile admission 上优先于 next；live 只能
  作为 current，所有连接与媒体资源都有明确上限。
- pause、cancel、错误、generation 替换和 shutdown 都必须唤醒等待者并确定性收敛资源。
- active stream、停止状态、flight registry、事件、连接、worker、内存和磁盘都有明确上限。

## 文档

- [设计总览](docs/architecture.md)：问题边界、所有权、核心决策与理由。
- [运行模型](docs/runtime.md)：状态、任务、背压、暂停、失败和关闭语义。
- [首包延迟](docs/latency.md)：没下完即播放的流水线、退化条件和后续优化重点。
- [具体实现](docs/implementation.md)：source、音频管线、传输和实现约束。
- [使用指南](docs/user-doc.md)：宿主如何正确驱动播放器并处理事件与错误。
- [文档索引](docs/index.md)：其余验证、依赖和能力边界文档。

## Workspace

```text
crates/music_stream/       Rust 状态机、source、音频管线和 RTP/RTCP runtime
crates/music_stream_napi/  Node 类型转换、Promise 边界和事件桥接
docs/                      当前设计与使用契约
```

## N-API 包

```sh
cd crates/music_stream_napi
npm ci
npm run build
npm run create:npm
```

`build` 始终生成经过 LTO 和 strip 的 release 原生库；`npm test` 使用单独的 debug 构建。
正式发布由 GitHub Actions 分 target 构建并通过 `napi artifacts`/`napi pre-publish` 组装主包和
平台子包，详见 [发布契约](docs/releasing.md)。

## 验证

```sh
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps

cd crates/music_stream_napi
npm test
```
