use std::collections::VecDeque;

use crate::audio::decode::{DecodePoll, DecoderBackend};
use crate::audio::dsp::VolumeConfig;
use crate::audio::frame::{
    FrameAssembler, FrameQueue, OpusFrame, PcmFrame, QueueWatermarks, TimedFrame,
};
use crate::audio::opus::OpusEncoderBackend;
use crate::error::{MusicStreamError, Result};
use crate::model::WatermarkConfig;
#[cfg(feature = "transport-rtp")]
use crate::transport::{RtpPacketizer, RtpSenderStep};
use crate::transport::{SenderCore, SenderStep};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipelineConfig {
    pub generation: u64,
    pub sample_rate: u32,
    pub channels: u16,
    pub frame_samples_per_channel: u32,
    pub watermarks: WatermarkConfig,
    pub prebuffer_ms: u64,
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use bytes::Bytes;

    use super::*;
    use crate::audio::decode::{DecodedChunk, StreamingPcmDecoder};
    use crate::model::VolumeLevel;
    use crate::transport::SenderStep;
    #[cfg(feature = "transport-rtp")]
    use crate::transport::{RtpPacketizer, RtpPacketizerConfig, RtpSenderStep};

    #[derive(Debug)]
    struct FakeDecoder {
        polls: VecDeque<DecodePoll>,
    }

    impl FakeDecoder {
        fn new(polls: impl IntoIterator<Item = DecodePoll>) -> Self {
            Self {
                polls: polls.into_iter().collect(),
            }
        }
    }

    impl DecoderBackend for FakeDecoder {
        fn poll_decode(&mut self) -> Result<DecodePoll> {
            Ok(self.polls.pop_front().unwrap_or(DecodePoll::End))
        }
    }

    #[derive(Debug, Default)]
    struct FakeEncoder;

    impl OpusEncoderBackend for FakeEncoder {
        fn encode(&mut self, frame: &PcmFrame) -> Result<OpusFrame> {
            Ok(OpusFrame {
                generation: frame.generation,
                payload: Bytes::copy_from_slice(&frame.track_position_samples.to_le_bytes()),
                samples_per_channel: frame.samples_per_channel,
                duration_ms: frame.duration_ms(),
                marker: frame.track_position_samples == 0,
                track_position_samples: frame.track_position_samples,
            })
        }
    }

    #[derive(Debug, Default)]
    struct CapturingEncoder {
        captured: Vec<Vec<f32>>,
    }

    impl OpusEncoderBackend for CapturingEncoder {
        fn encode(&mut self, frame: &PcmFrame) -> Result<OpusFrame> {
            self.captured.push(frame.samples.clone());
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
            watermarks: WatermarkConfig {
                decode_batch_ms: 100,
                decoded_low_water_ms: 20,
                decoded_high_water_ms: 60,
                encoded_low_water_ms: 20,
                encoded_high_water_ms: 100,
                next_prime_ms: 60,
                pause_encoded_limit_ms: 2_000,
            },
            prebuffer_ms: 40,
        }
    }

    fn chunk(frames: usize) -> DecodePoll {
        DecodePoll::Chunk(decoded_chunk(frames))
    }

    fn decoded_chunk(frames: usize) -> DecodedChunk {
        let samples_per_frame = 960 * 2;
        let sample_count = frames * samples_per_frame;
        let samples = (0..sample_count).map(|sample| sample as f32).collect();
        DecodedChunk {
            sample_rate: 48_000,
            channels: 2,
            samples_interleaved: samples,
        }
    }

    #[test]
    fn worker_turn_primes_encoded_queue_ahead_of_sender() {
        let decoder = FakeDecoder::new([chunk(3), DecodePoll::End]);
        let mut pipeline = PlayoutPipeline::new(decoder, FakeEncoder, config()).expect("pipeline");

        let report = pipeline.worker_turn().expect("worker turn");
        assert_eq!(report.decoded_chunks, 1);
        assert_eq!(report.encoded_frames, 3);
        assert_eq!(pipeline.encoded_queue_ms(), 60);
        assert!(report.prebuffer_ready);
        assert!(report.next_prime_ready);
        assert!(!report.playout_drained);

        assert!(matches!(
            pipeline.sender_step(),
            SenderStep::Send {
                rtp_timestamp: 0,
                sequence: 0,
                ..
            }
        ));
    }

    #[test]
    fn large_decoder_chunk_crosses_high_water_without_losing_frames() {
        let decoder = FakeDecoder::new([chunk(5), DecodePoll::End]);
        let mut pipeline = PlayoutPipeline::new(decoder, FakeEncoder, config()).expect("pipeline");

        let first = pipeline.worker_turn().expect("first worker turn");
        assert_eq!(first.decoded_frames, 3);
        assert_eq!(first.encoded_frames, 3);

        let sent_first = drain_sender(&mut pipeline);
        assert_eq!(sent_first, 3);

        let second = pipeline.worker_turn().expect("second worker turn");
        assert_eq!(second.decoded_frames, 2);
        assert_eq!(second.encoded_frames, 2);

        let sent_second = drain_sender(&mut pipeline);
        assert_eq!(sent_second, 2);
    }

    #[test]
    fn need_more_stops_turn_without_busy_looping() {
        let decoder = FakeDecoder::new([DecodePoll::NeedMore]);
        let mut pipeline = PlayoutPipeline::new(decoder, FakeEncoder, config()).expect("pipeline");

        let report = pipeline.worker_turn().expect("worker turn");
        assert!(report.source_need_more);
        assert_eq!(report.decoded_chunks, 0);
        assert_eq!(report.encoded_frames, 0);
    }

    #[test]
    fn streaming_decoder_respects_pipeline_backpressure() {
        let decoder = StreamingPcmDecoder::new(60).expect("streaming decoder");
        let writer = decoder.writer();
        writer.try_push(decoded_chunk(1)).expect("push 20ms");
        writer.try_push(decoded_chunk(1)).expect("push 40ms");
        writer.try_push(decoded_chunk(1)).expect("push 60ms");
        let error = writer.try_push(decoded_chunk(1)).expect_err("source full");
        assert_eq!(error.code(), crate::error::ErrorCode::Busy);

        let mut pipeline = PlayoutPipeline::new(decoder, FakeEncoder, config()).expect("pipeline");
        let first = pipeline.worker_turn().expect("first worker turn");
        assert_eq!(first.decoded_chunks, 3);
        assert_eq!(first.encoded_frames, 3);
        assert!(first.hit_decoded_high_water);
        assert!(!first.source_need_more);
        assert_eq!(pipeline.decoder.buffered_ms(), 0);
        assert_eq!(pipeline.encoded_queue_ms(), 60);

        let second = pipeline.worker_turn().expect("empty streaming turn");
        assert!(second.source_need_more);
        assert_eq!(second.decoded_chunks, 0);
        assert_eq!(second.encoded_frames, 0);

        writer
            .try_push(decoded_chunk(1))
            .expect("push resumed 20ms");
        writer
            .try_push(decoded_chunk(1))
            .expect("push resumed 40ms");
        let third = pipeline.worker_turn().expect("resumed worker turn");
        assert_eq!(third.decoded_chunks, 2);
        assert_eq!(third.encoded_frames, 2);
        assert!(third.hit_encoded_high_water);
        assert_eq!(pipeline.encoded_queue_ms(), 100);
    }

    #[test]
    fn short_finished_source_can_start_below_prebuffer_threshold() {
        let decoder = FakeDecoder::new([chunk(1), DecodePoll::End]);
        let mut pipeline = PlayoutPipeline::new(decoder, FakeEncoder, config()).expect("pipeline");

        let report = pipeline.worker_turn().expect("worker turn");
        assert_eq!(report.encoded_queue_ms, 20);
        assert!(report.source_ended);
        assert!(report.prebuffer_ready);
        assert!(report.next_prime_ready);
        assert!(!report.playout_drained);
    }

    #[test]
    fn incompatible_decoder_format_is_rejected_before_queueing() {
        let bad_chunk = DecodePoll::Chunk(DecodedChunk {
            sample_rate: 44_100,
            channels: 2,
            samples_interleaved: vec![0.0; 960 * 2],
        });
        let decoder = FakeDecoder::new([bad_chunk]);
        let mut pipeline = PlayoutPipeline::new(decoder, FakeEncoder, config()).expect("pipeline");

        let error = pipeline.worker_turn().expect_err("format mismatch");
        assert_eq!(error.code(), crate::error::ErrorCode::DecodeError);
    }

    #[test]
    fn pipeline_applies_volume_to_pcm_before_encoding() {
        let decoder = FakeDecoder::new([DecodePoll::Chunk(DecodedChunk {
            sample_rate: 48_000,
            channels: 2,
            samples_interleaved: vec![0.5; 960 * 2],
        })]);
        let mut pipeline =
            PlayoutPipeline::new(decoder, CapturingEncoder::default(), config()).expect("pipeline");
        pipeline
            .set_volume(VolumeConfig::from_level(
                VolumeLevel::from_unit(0.5).expect("volume"),
            ))
            .expect("set volume");

        pipeline.worker_turn().expect("worker turn");

        let encoder = &pipeline.encoder;
        assert_eq!(encoder.captured.len(), 1);
        let first_sample = encoder.captured[0][0];
        assert!(first_sample > 0.0);
        assert!(first_sample < 0.5);
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn pipeline_can_packetize_sender_step_as_rtp_without_owning_transport() {
        let decoder = FakeDecoder::new([chunk(2), DecodePoll::End]);
        let mut pipeline = PlayoutPipeline::new(decoder, FakeEncoder, config()).expect("pipeline");
        let packetizer = RtpPacketizer::new(RtpPacketizerConfig {
            payload_type: 111,
            ssrc: 99,
            mtu: 1_200,
        })
        .expect("packetizer");
        let mut scratch = bytes::BytesMut::new();

        pipeline.worker_turn().expect("worker turn");

        let first = match pipeline
            .rtp_sender_step(&packetizer, &mut scratch)
            .expect("rtp step")
        {
            RtpSenderStep::Packet(packetized) => packetized,
            other => panic!("unexpected sender step: {other:?}"),
        };
        let second = match pipeline
            .rtp_sender_step(&packetizer, &mut scratch)
            .expect("rtp step")
        {
            RtpSenderStep::Packet(packetized) => packetized,
            other => panic!("unexpected sender step: {other:?}"),
        };

        assert_eq!(first.sequence, 0);
        assert_eq!(first.rtp_timestamp, 0);
        assert_eq!(second.sequence, 1);
        assert_eq!(second.rtp_timestamp, 960);
        assert_eq!(second.ssrc, 99);
    }

    fn drain_sender<D, E>(pipeline: &mut PlayoutPipeline<D, E>) -> usize
    where
        D: DecoderBackend,
        E: OpusEncoderBackend,
    {
        let mut sent = 0;
        while let SenderStep::Send { .. } = pipeline.sender_step() {
            sent += 1;
        }
        sent
    }
}

impl PipelineConfig {
    pub fn validate(&self) -> Result<()> {
        self.watermarks.validate()?;
        if self.generation == 0 {
            return Err(MusicStreamError::InvalidConfig(
                "pipeline generation must be greater than zero".to_owned(),
            ));
        }
        if self.sample_rate == 0 {
            return Err(MusicStreamError::InvalidConfig(
                "pipeline sample_rate must be greater than zero".to_owned(),
            ));
        }
        if self.channels == 0 {
            return Err(MusicStreamError::InvalidConfig(
                "pipeline channels must be greater than zero".to_owned(),
            ));
        }
        if self.frame_samples_per_channel == 0 {
            return Err(MusicStreamError::InvalidConfig(
                "pipeline frame_samples_per_channel must be greater than zero".to_owned(),
            ));
        }

        let frame_duration_ms =
            (u64::from(self.frame_samples_per_channel) * 1_000) / u64::from(self.sample_rate);
        if frame_duration_ms == 0 {
            return Err(MusicStreamError::InvalidConfig(
                "pipeline frame duration must be at least 1ms".to_owned(),
            ));
        }
        if frame_duration_ms > self.watermarks.decoded_high_water_ms
            || frame_duration_ms > self.watermarks.encoded_high_water_ms
        {
            return Err(MusicStreamError::InvalidConfig(
                "queue high watermarks must fit at least one frame".to_owned(),
            ));
        }
        if self.prebuffer_ms > self.watermarks.encoded_high_water_ms {
            return Err(MusicStreamError::InvalidConfig(
                "prebuffer_ms must not exceed encoded high watermark".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerTurnReport {
    pub decoded_chunks: usize,
    pub decoded_frames: usize,
    pub encoded_frames: usize,
    pub decoded_queue_ms: u64,
    pub encoded_queue_ms: u64,
    pub prebuffer_ready: bool,
    pub next_prime_ready: bool,
    pub playout_drained: bool,
    pub source_need_more: bool,
    pub source_ended: bool,
    pub hit_decoded_high_water: bool,
    pub hit_encoded_high_water: bool,
}

#[derive(Debug)]
pub struct PlayoutPipeline<D, E> {
    decoder: D,
    encoder: E,
    config: PipelineConfig,
    assembler: FrameAssembler,
    decoded_queue: FrameQueue<PcmFrame>,
    encoded_queue: FrameQueue<OpusFrame>,
    pending_decoded: VecDeque<PcmFrame>,
    pending_encoded: VecDeque<OpusFrame>,
    sender: SenderCore,
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
        let assembler = FrameAssembler::new(config.channels, config.frame_samples_per_channel)?;
        let decoded_queue = FrameQueue::new(QueueWatermarks::new(
            config.watermarks.decoded_low_water_ms,
            config.watermarks.decoded_high_water_ms,
        )?);
        let encoded_queue = FrameQueue::new(QueueWatermarks::new(
            config.watermarks.encoded_low_water_ms,
            config.watermarks.encoded_high_water_ms,
        )?);
        let sender = SenderCore::new(config.generation, config.prebuffer_ms);

        Ok(Self {
            decoder,
            encoder,
            config,
            assembler,
            decoded_queue,
            encoded_queue,
            pending_decoded: VecDeque::new(),
            pending_encoded: VecDeque::new(),
            sender,
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

    pub fn worker_turn(&mut self) -> Result<WorkerTurnReport> {
        let mut report = WorkerTurnReport::default();
        self.encode_turn(&mut report)?;
        self.decode_turn(&mut report)?;
        self.encode_turn(&mut report)?;
        report.decoded_queue_ms = self.decoded_queue.duration_ms();
        report.encoded_queue_ms = self.encoded_queue.duration_ms();
        report.prebuffer_ready = self.is_prebuffer_ready();
        report.next_prime_ready = self.is_next_prime_ready();
        report.playout_drained = self.is_playout_drained();
        report.source_ended = self.source_ended;
        Ok(report)
    }

    pub fn sender_step(&mut self) -> SenderStep {
        let prebuffer_ready = self.is_prebuffer_ready();
        self.sender
            .next_step_with_prebuffer_ready(&mut self.encoded_queue, prebuffer_ready)
    }

    #[cfg(feature = "transport-rtp")]
    pub fn rtp_sender_step(
        &mut self,
        packetizer: &RtpPacketizer,
        scratch: &mut bytes::BytesMut,
    ) -> Result<RtpSenderStep> {
        match self.sender_step() {
            SenderStep::WaitPrebuffer {
                queued_ms,
                needed_ms,
            } => Ok(RtpSenderStep::WaitPrebuffer {
                queued_ms,
                needed_ms,
            }),
            SenderStep::Send {
                frame,
                rtp_timestamp,
                sequence,
            } => packetizer
                .packetize_send(frame, rtp_timestamp, sequence, scratch)
                .map(RtpSenderStep::Packet),
            SenderStep::Underrun { generation } => Ok(RtpSenderStep::Underrun { generation }),
        }
    }

    #[must_use]
    pub fn encoded_queue_ms(&self) -> u64 {
        self.encoded_queue.duration_ms()
    }

    #[must_use]
    pub fn decoded_queue_ms(&self) -> u64 {
        self.decoded_queue.duration_ms()
    }

    #[must_use]
    pub fn is_prebuffer_ready(&self) -> bool {
        self.encoded_queue.duration_ms() >= self.config.prebuffer_ms
            || (self.source_ended && !self.encoded_queue.is_empty())
    }

    #[must_use]
    pub fn is_next_prime_ready(&self) -> bool {
        self.encoded_queue.duration_ms() >= self.config.watermarks.next_prime_ms
            || (self.source_ended && !self.encoded_queue.is_empty())
    }

    #[must_use]
    pub fn source_ended(&self) -> bool {
        self.source_ended
    }

    #[must_use]
    pub fn is_playout_drained(&self) -> bool {
        self.source_ended
            && self.pending_decoded.is_empty()
            && self.pending_encoded.is_empty()
            && self.decoded_queue.is_empty()
            && self.encoded_queue.is_empty()
    }

    fn decode_turn(&mut self, report: &mut WorkerTurnReport) -> Result<()> {
        self.flush_pending_decoded(report);
        if self.source_ended || self.decoded_queue.is_full() {
            report.hit_decoded_high_water = self.decoded_queue.is_full();
            return Ok(());
        }

        let mut decoded_ms = 0;
        while decoded_ms < self.config.watermarks.decode_batch_ms && !self.decoded_queue.is_full() {
            match self.decoder.poll_decode()? {
                DecodePoll::Chunk(chunk) => {
                    self.validate_chunk_format(&chunk)?;
                    decoded_ms = decoded_ms.saturating_add(chunk.duration_ms());
                    report.decoded_chunks += 1;
                    let frames = self.assembler.push_interleaved(
                        self.config.generation,
                        chunk.sample_rate,
                        &chunk.samples_interleaved,
                    )?;
                    self.pending_decoded.extend(frames);
                    self.flush_pending_decoded(report);
                    if !self.pending_decoded.is_empty() {
                        report.hit_decoded_high_water = true;
                        break;
                    }
                }
                DecodePoll::NeedMore => {
                    report.source_need_more = true;
                    break;
                }
                DecodePoll::End => {
                    self.source_ended = true;
                    report.source_ended = true;
                    break;
                }
            }
        }

        report.hit_decoded_high_water |= self.decoded_queue.is_full();
        Ok(())
    }

    fn encode_turn(&mut self, report: &mut WorkerTurnReport) -> Result<()> {
        self.flush_pending_encoded(report);
        if !self.pending_encoded.is_empty() || self.encoded_queue.is_full() {
            report.hit_encoded_high_water = self.encoded_queue.is_full();
            return Ok(());
        }

        while !self.encoded_queue.is_full() {
            let Some(mut frame) = self.decoded_queue.pop_active(self.config.generation) else {
                break;
            };
            self.volume.apply_in_place(&mut frame.samples);
            let encoded = self.encoder.encode(&frame)?;
            self.pending_encoded.push_back(encoded);
            self.flush_pending_encoded(report);
            if !self.pending_encoded.is_empty() {
                report.hit_encoded_high_water = true;
                break;
            }
        }

        report.hit_encoded_high_water |= self.encoded_queue.is_full();
        Ok(())
    }

    fn flush_pending_decoded(&mut self, report: &mut WorkerTurnReport) {
        while let Some(frame) = self.pending_decoded.front() {
            if !self.decoded_queue.can_accept_duration(frame.duration_ms()) {
                break;
            }

            let frame = self.pending_decoded.pop_front().expect("front checked");
            self.decoded_queue
                .push(frame)
                .expect("duration was checked before push");
            report.decoded_frames += 1;
        }
    }

    fn flush_pending_encoded(&mut self, report: &mut WorkerTurnReport) {
        while let Some(frame) = self.pending_encoded.front() {
            if !self.encoded_queue.can_accept_duration(frame.duration_ms()) {
                break;
            }

            let frame = self.pending_encoded.pop_front().expect("front checked");
            self.encoded_queue
                .push(frame)
                .expect("duration was checked before push");
            report.encoded_frames += 1;
        }
    }

    fn validate_chunk_format(&self, chunk: &crate::audio::decode::DecodedChunk) -> Result<()> {
        if chunk.sample_rate != self.config.sample_rate || chunk.channels != self.config.channels {
            return Err(MusicStreamError::DecodeError(
                "decoded chunk must be normalized before entering playout pipeline".to_owned(),
            ));
        }

        Ok(())
    }
}
