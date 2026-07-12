use crate::audio::decode::{DecodePoll, DecoderBackend};
use crate::audio::dsp::VolumeConfig;
use crate::audio::frame::{FrameAssembler, OpusFrame};
use crate::audio::opus::OpusEncoderBackend;
use crate::error::{MusicStreamError, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipelineConfig {
    pub generation: u64,
    pub sample_rate: u32,
    pub channels: u16,
    pub frame_samples_per_channel: u32,
    pub decode_batch_ms: u64,
}

impl PipelineConfig {
    pub fn validate(&self) -> Result<()> {
        if self.generation == 0
            || self.sample_rate == 0
            || self.channels == 0
            || self.frame_samples_per_channel == 0
            || self.decode_batch_ms == 0
        {
            return Err(MusicStreamError::InvalidConfig(
                "pipeline generation, format, frame size and batch must be non-zero".to_owned(),
            ));
        }
        let frame_ms =
            u64::from(self.frame_samples_per_channel) * 1_000 / u64::from(self.sample_rate);
        if frame_ms == 0 || frame_ms > self.decode_batch_ms {
            return Err(MusicStreamError::InvalidConfig(
                "decode batch must fit at least one output frame".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerTurnReport {
    pub decoded_chunks: usize,
    pub pcm_frames: usize,
    pub opus_frames: usize,
    pub source_need_more: bool,
    pub source_ended: bool,
}

impl WorkerTurnReport {
    #[must_use]
    pub fn made_progress(&self) -> bool {
        self.decoded_chunks > 0 || self.pcm_frames > 0 || self.opus_frames > 0 || self.source_ended
    }
}

/// A synchronous CPU pipeline with no playout queue or RTP state.
///
/// The caller supplies the only output sink. In production this is a bounded Tokio channel;
/// blocking on that sink is the backpressure mechanism, so a second encoded queue here would
/// only duplicate latency and memory.
#[derive(Debug)]
pub struct PlayoutPipeline<D, E> {
    decoder: D,
    encoder: E,
    config: PipelineConfig,
    assembler: FrameAssembler,
    volume: VolumeConfig,
    source_ended: bool,
}

impl<D, E> PlayoutPipeline<D, E>
where
    D: DecoderBackend,
    E: OpusEncoderBackend,
{
    pub fn new(decoder: D, encoder: E, config: PipelineConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            decoder,
            encoder,
            assembler: FrameAssembler::new(config.channels, config.frame_samples_per_channel)?,
            config,
            volume: VolumeConfig::default(),
            source_ended: false,
        })
    }

    pub fn set_volume(&mut self, volume: VolumeConfig) -> Result<()> {
        volume.validate()?;
        self.volume = volume;
        Ok(())
    }

    #[must_use]
    pub fn volume(&self) -> VolumeConfig {
        self.volume
    }

    #[must_use]
    pub fn source_ended(&self) -> bool {
        self.source_ended
    }

    pub fn process_turn(
        &mut self,
        mut emit: impl FnMut(OpusFrame) -> Result<()>,
    ) -> Result<WorkerTurnReport> {
        let mut report = WorkerTurnReport::default();
        if self.source_ended {
            report.source_ended = true;
            return Ok(report);
        }

        let mut decoded_ms = 0_u64;
        while decoded_ms < self.config.decode_batch_ms {
            match self.decoder.poll_decode()? {
                DecodePoll::Chunk(mut chunk) => {
                    self.validate_chunk(&chunk)?;
                    decoded_ms = decoded_ms.saturating_add(chunk.duration_ms());
                    report.decoded_chunks += 1;
                    let volume = self.volume;
                    let encoder = &mut self.encoder;
                    self.assembler.process_interleaved(
                        self.config.generation,
                        chunk.sample_rate,
                        &mut chunk.samples_interleaved,
                        |frame| {
                            volume.apply_in_place(frame.samples);
                            let encoded = encoder.encode(&frame)?;
                            report.pcm_frames += 1;
                            emit(encoded)?;
                            report.opus_frames += 1;
                            Ok(())
                        },
                    )?;
                }
                DecodePoll::NeedMore => {
                    report.source_need_more = true;
                    break;
                }
                DecodePoll::End => {
                    let volume = self.volume;
                    let encoder = &mut self.encoder;
                    if self.assembler.flush_padded(
                        self.config.generation,
                        self.config.sample_rate,
                        |frame| {
                            volume.apply_in_place(frame.samples);
                            let encoded = encoder.encode(&frame)?;
                            report.pcm_frames += 1;
                            emit(encoded)?;
                            report.opus_frames += 1;
                            Ok(())
                        },
                    )? {
                        metrics::counter!("music_stream.audio.padded_final_frames").increment(1);
                    }
                    self.source_ended = true;
                    report.source_ended = true;
                    break;
                }
            }
        }
        Ok(report)
    }

    fn validate_chunk(&self, chunk: &crate::audio::decode::DecodedChunk) -> Result<()> {
        if chunk.sample_rate != self.config.sample_rate || chunk.channels != self.config.channels {
            return Err(MusicStreamError::DecodeError(
                "decoded chunk must be normalized before entering the encoder pipeline".to_owned(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use bytes::Bytes;

    use super::*;
    use crate::audio::decode::{DecodedChunk, MemoryDecoder};
    use crate::audio::frame::PcmFrame;

    #[derive(Debug)]
    struct FakeDecoder {
        polls: VecDeque<DecodePoll>,
    }

    impl DecoderBackend for FakeDecoder {
        fn poll_decode(&mut self) -> Result<DecodePoll> {
            Ok(self.polls.pop_front().unwrap_or(DecodePoll::End))
        }
    }

    #[derive(Debug)]
    struct FakeEncoder;

    impl OpusEncoderBackend for FakeEncoder {
        fn encode(&mut self, frame: &PcmFrame<'_>) -> Result<OpusFrame> {
            Ok(OpusFrame {
                generation: frame.generation,
                payload: Bytes::from_static(b"opus"),
                samples_per_channel: frame.samples_per_channel,
                duration_ms: frame.duration_ms(),
                marker: frame.track_position_samples == 0,
                track_position_samples: frame.track_position_samples,
            })
        }
    }

    fn config() -> PipelineConfig {
        PipelineConfig {
            generation: 1,
            sample_rate: 48_000,
            channels: 2,
            frame_samples_per_channel: 960,
            decode_batch_ms: 80,
        }
    }

    fn chunk(frames: usize) -> DecodedChunk {
        DecodedChunk {
            sample_rate: 48_000,
            channels: 2,
            samples_interleaved: vec![0.25; frames * 960 * 2],
        }
    }

    #[test]
    fn emits_directly_without_an_internal_encoded_queue() {
        let decoder = MemoryDecoder::new([chunk(3)]);
        let mut pipeline = PlayoutPipeline::new(decoder, FakeEncoder, config()).expect("pipeline");
        let mut output = Vec::new();
        let report = pipeline
            .process_turn(|frame| {
                output.push(frame);
                Ok(())
            })
            .expect("turn");
        assert_eq!(report.opus_frames, 3);
        assert_eq!(output.len(), 3);
    }

    #[test]
    fn need_more_returns_without_spinning() {
        let decoder = FakeDecoder {
            polls: VecDeque::from([DecodePoll::NeedMore]),
        };
        let mut pipeline = PlayoutPipeline::new(decoder, FakeEncoder, config()).expect("pipeline");
        let report = pipeline.process_turn(|_| Ok(())).expect("turn");
        assert!(report.source_need_more);
        assert!(!report.made_progress());
    }

    #[test]
    fn rejects_non_normalized_input() {
        let decoder = MemoryDecoder::new([DecodedChunk {
            sample_rate: 44_100,
            channels: 2,
            samples_interleaved: vec![0.0; 1_920],
        }]);
        let mut pipeline = PlayoutPipeline::new(decoder, FakeEncoder, config()).expect("pipeline");
        assert!(pipeline.process_turn(|_| Ok(())).is_err());
    }

    #[test]
    fn pads_and_emits_a_final_partial_frame() {
        let decoder = MemoryDecoder::new([DecodedChunk {
            sample_rate: 48_000,
            channels: 2,
            samples_interleaved: vec![0.25; 480 * 2],
        }]);
        let mut pipeline = PlayoutPipeline::new(decoder, FakeEncoder, config()).expect("pipeline");
        let mut output = Vec::new();
        let report = pipeline
            .process_turn(|frame| {
                output.push(frame);
                Ok(())
            })
            .expect("turn");
        assert!(report.source_ended);
        assert_eq!(report.opus_frames, 1);
        assert_eq!(output.len(), 1);
    }
}
