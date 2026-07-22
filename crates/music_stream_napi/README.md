# @rhythm-app/streamer

```sh
npm install @rhythm-app/streamer
```

Rust N-API media runtime for decoding file, bounded HTTP, live HTTP, and audio HLS, encoding fixed
48 kHz stereo Opus frames, and sending paced RTP/RTCP.

Create one long-lived `Streamer` per media process. A `streamId` owns one persistent RTP session;
seek, switch, and next promotion replace media generations without resetting SSRC, RTP sequence, or
timestamp. Playlist policy, provider authentication, URL refresh, and gateway negotiation remain in
the Node host.

Bounded HTTP sources can start playback before the response completes. Give signed URLs without an
extension an accurate `formatHint`. Per-source `headers` support authenticated HTTP media without
projecting credentials in status or events. Live sources are current-only and do not support pause,
seek, or next preload because the runtime deliberately has no implicit timeshift store.
`.m3u8` URLs, or opaque playlist URLs with `formatHint: 'm3u8'`, automatically use the bounded HLS
path and the same current-only live semantics.

All lifecycle methods return Promises. Await `shutdown()` before process exit; shutdown is idempotent
and permanently closes the instance. Promise errors begin with a stable media error code followed by
a colon. Media failure events expose the same code in their `code` field.

The generated `index.d.ts` is the API reference. The repository documentation explains the ownership,
latency, pause, retry, event, and resource-limit contracts in depth.

Published releases use napi-rs platform packages for Linux x64 GNU, Windows x64, macOS x64, and
macOS arm64. The main package contains no platform-specific binary.
