# Music Streamer

Rust + napi-rs music decoding and RTP streaming addon.

This workspace is intentionally small:

```text
crates/music_stream/       Rust core: source, audio, session, transport
crates/music_stream_napi/  Node ABI boundary and npm package root
docs/                      Design notes and implementation constraints
voice_sender/              Old C++ reference project
```

Core boundaries:

- TypeScript owns playlist, play mode, provider URL resolution, auth, and product policy.
- `music_stream` owns realtime media work: source access, decode, DSP, resample, Opus, RTP/RTCP, state machine.
- `music_stream_napi` maps napi types and native lifecycle. Its package scripts build the native addon and run Node smoke tests for the local-file, bounded HTTP URL, and live HTTP RTP paths.
- RTP/RTCP wire format must use tested crates, not handwritten packet serialization.
- Audio is a top-level module inside `music_stream`, not a separate crate until it has a real independent boundary.

Current implementation status is tracked in `docs/status.md`; new Codex sessions should start with `docs/handoff.md`, then use `docs/design-review.md` for the next design questions. The local-file, bounded HTTP URL, and live HTTP current paths are covered by tests, including real paced RTP playback, configurable source policies, bounded HTTP range resume, source artifact cache reuse, runtime metrics hooks, manual gain/gainDb control, ReplayGain recommendation, RTCP SR/RR feedback with quality-window events, N-API event callbacks, explicit shutdown, and the pause/seek/switch/next lifecycle. Remaining product-level work is mostly in TS/host policy: live auto recovery, network-quality actions, persistent cache ownership, broader codec/source corpus validation, and platform-specific RTP protection algorithms.

Checks:

```sh
cargo update
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features

cd crates/music_stream_napi
npm run build
npm test
```
