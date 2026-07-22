# Source TODO

本文只记录尚未实现的 source 与格式工作，不代表当前能力。实现时继续复用现有 producer、
Opus queue、sender 和资源预算，不为每种协议复制播放状态机。

## P0：能力准确性

- [ ] 若真实语料需要，扩展 Ogg Opus multistream/channel mapping；当前输入支持常见 mono/stereo。
- [ ] 继续扩大小型格式语料测试；当前已有 ADTS AAC、Ogg Opus、WAV，仍需覆盖 MP3、FLAC、
  Ogg Vorbis、M4A/MP4 及损坏输入。
- [ ] 评估仍返回非标准 `ICY 200 OK` 状态行的老式 Shoutcast 服务；当前标准 HTTP Icecast/ICY
  响应已支持。

## P1：HLS 后续

基础音频 HLS 已实现 master/media playlist、相对 URL、VOD/live reload、有界获取，以及 packed 或
MPEG-TS 内的 ADTS AAC/MP3；以下能力继续按真实 provider 需求增加：

- [ ] fMP4/CMAF 与 `EXT-X-MAP`；不能把 fragment 直接拼接成普通文件。
- [ ] AES-128 等加密、byte range、midstream discontinuity 和 codec generation 切换。
- [ ] LL-HLS partial segment、blocking reload 和 preload hint。
- [ ] 若语料需要，基于 PAT/PMT 扩展多 program/多 audio PID 选择，并评估 LATM AAC、AC-3/E-AC-3。

## P2：MP4/M4A 渐进

- [ ] `moov` 在前的 faststart 文件允许顺序渐进。
- [ ] `moov` 在尾部且服务端支持 Range 时，研究有界 range reader；不支持 Range 则保持完整下载退化。
- [ ] seek 只能基于已验证的容器索引，不能把音频时间直接换算成 HTTP byte offset。

## P3：可选体验能力

- [ ] 若产品确实需要直播 pause/seek，增加显式启用、有磁盘和时长上限的 timeshift ring；默认 live
  仍不保留历史。
- [ ] 评估向状态暴露 `bufferedMs`、source protocol/container/codec 和 rebuffer 计数；只有宿主会据此
  改善 UI 或策略时才增加 N-API 字段。
