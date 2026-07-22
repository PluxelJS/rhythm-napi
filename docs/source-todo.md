# Source TODO

本文只记录尚未实现的 source 与格式工作，不代表当前能力。实现时继续复用现有 producer、
Opus queue、sender 和资源预算，不为每种协议复制播放状态机。

## P0：能力准确性

- [ ] 若真实语料需要，扩展 Ogg Opus multistream/channel mapping；当前输入支持常见 mono/stereo。
- [ ] 继续扩充生产语料；小型确定性测试现已覆盖 ADTS AAC、MP3、FLAC、Ogg Opus/Vorbis、WAV、
  faststart M4A AAC/ALAC和损坏 FLAC header，仍需真实 VBR、多声道、超大 metadata及更多损坏样本。
- [ ] 评估仍返回非标准 `ICY 200 OK` 状态行的老式 Shoutcast 服务；当前标准 HTTP Icecast/ICY
  响应已支持。

## P1：HLS 后续

基础音频 HLS 已实现 master/media playlist、相对 URL、VOD/live reload、有界获取，以及 packed 或
MPEG-TS 内的 ADTS AAC/MP3、`EXT-X-MAP`/fMP4/CMAF 和 byte range；以下能力继续按真实 provider
需求增加：

- [ ] SAMPLE-AES/DRM，以及跨 discontinuity 的初始化段/codec generation 切换；标准 AES-128、
  `EXT-X-GAP` 和同初始化段/codec discontinuity 已支持。
- [ ] LL-HLS partial segment、blocking reload 和 preload hint。
- [ ] 若语料需要，基于 PAT/PMT 扩展多 program/多 audio PID 选择，并评估 LATM AAC、AC-3/E-AC-3。

## P2：MP4/M4A 后续

`moov` 在前且 metadata位于前 4 MiB内的 faststart文件已允许顺序渐进；以下随机访问能力仍未实现：

- [ ] `moov` 在尾部且服务端支持 Range 时，研究有界 range reader；不支持 Range 则保持完整下载退化。
- [ ] seek 只能基于已验证的容器索引，不能把音频时间直接换算成 HTTP byte offset。

## P3：可选体验能力

- [ ] 若产品确实需要直播 pause/seek，增加显式启用、有磁盘和时长上限的 timeshift ring；默认 live
  仍不保留历史。
- [ ] 若宿主会据此改善UI或provider策略，再向状态暴露source protocol/container/codec。sender的
  `bufferedMs`、underrun、lateness和drop诊断已可通过N-API status批量查询。
