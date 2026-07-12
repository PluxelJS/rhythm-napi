# Music Streamer

一个面向 Node.js 的 Rust 原生音频推流内核：读取本地文件、有界 HTTP 音频或 live HTTP 音频，解码并统一为 48 kHz PCM，编码为 Opus，然后按实时节拍发送 RTP/RTCP。

## 核心设计

```text
Node Promise API
      │
      ▼
StreamRuntime / StreamActor
      ├── current producer
      ├── next preload producer
      └── persistent RTP session

async source I/O                 blocking CPU worker
HTTP/file/live ────────────────> decode → resample → DSP → Opus
                                                   │
                                    duration-bounded Opus queue
                                                   │
                                                   ▼
                                      async RTP/RTCP sender
```

- 每个 stream 只有一个长期存活的 RTP session；切歌和 seek 不重置 sequence、timestamp 或 SSRC。
- source、timer、UDP 和 Node 边界使用 async。
- Symphonia、Rubato、DSP 和 libopus 运行在 blocking CPU worker，绝不占用 RTP sender 调度。
- current 与 next 只共享 immutable Opus frame，不共享 decoder 或 PCM scratch。
- URL使用进程级磁盘预算下的共享growing spool，live与Opus桥接均有明确容量；暂停和预载通过聚合控制与背压停止生产。
- current在CPU、blocking worker、HTTP、live连接和tempfile admission中均优先于next preload。
- Rust 不维护 playlist、随机/循环模式、鉴权刷新、voice gateway 或业务重试策略。

## Workspace

```text
crates/music_stream/       source、音频管线、状态机、RTP/RTCP runtime
crates/music_stream_napi/  Node 类型映射、Promise API、事件 callback
docs/                      当前实现契约
```

## 验证

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets

cd crates/music_stream_napi
npm test
```

设计入口见 [docs/index.md](docs/index.md)。
