//! Thin playout-slot driver that bridges pipeline state to actor worker events.

use crate::audio::decode::DecoderBackend;
use crate::audio::opus::OpusEncoderBackend;
use crate::audio::pipeline::{PlayoutPipeline, WorkerTurnReport};
#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
use crate::error::MusicStreamError;
use crate::error::Result;
use crate::model::{GainLevel, VolumeLevel};
use crate::session::WorkerEvent;
use crate::source::SourceArtifact;
#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
use crate::source::{
    FileSourceResolver, HttpLiveStream, HttpLiveStreamConfig, SourceResolver,
    spawn_http_live_stream,
};
use crate::transport::SenderStep;
#[cfg(feature = "transport-rtp")]
use crate::transport::{RtpPaceDecision, RtpPacer, RtpPacketSink, RtpPacketizer, RtpSenderStep};

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
use crate::audio::AudioFormat;
#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
use crate::audio::decode::{SymphoniaFileDecoder, SymphoniaStreamDecoder};
#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
use crate::audio::opus::{LibOpusEncoder, LibOpusEncoderConfig};
#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
use crate::audio::pipeline::PipelineConfig;
#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
use crate::audio::resample::{RubatoResamplerConfig, RubatoResamplingDecoder};
#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
use crate::model::TrackSource;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotRole {
    Current,
    Next,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SlotTurnReport {
    pub worker: WorkerTurnReport,
    pub event: Option<WorkerEvent>,
}

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
pub type LocalFileSlotDriver =
    SlotDriver<RubatoResamplingDecoder<SymphoniaFileDecoder>, LibOpusEncoder>;

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
pub type LiveStreamSlotDriver =
    SlotDriver<RubatoResamplingDecoder<SymphoniaStreamDecoder>, LibOpusEncoder>;

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
#[derive(Debug)]
pub struct LocalFileSlot {
    pub artifact: SourceArtifact,
    pub driver: LocalFileSlotDriver,
}

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
#[derive(Debug)]
pub struct LiveStreamSlot {
    pub stream: HttpLiveStream,
    pub driver: LiveStreamSlotDriver,
}

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
#[derive(Clone, Debug)]
pub struct LocalFileSlotConfig {
    pub pipeline: PipelineConfig,
    pub opus: LibOpusEncoderConfig,
    pub resampler: RubatoResamplerConfig,
    pub start_position_ms: u64,
}

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
#[derive(Clone, Debug)]
pub struct LiveStreamSlotConfig {
    pub pipeline: PipelineConfig,
    pub opus: LibOpusEncoderConfig,
    pub resampler: RubatoResamplerConfig,
    pub http: HttpLiveStreamConfig,
}

#[cfg(feature = "transport-rtp")]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RtpSlotDrainReport {
    pub packets_sent: usize,
    pub bytes_sent: usize,
    pub payload_bytes_sent: usize,
    pub media_sent_ms: u64,
    pub max_pacing_late_ms: u64,
    pub last_rtp_timestamp: Option<u32>,
    pub stopped_on_prebuffer: bool,
    pub stopped_on_underrun: bool,
    pub stopped_on_pacing: bool,
}

#[cfg(feature = "transport-rtp")]
impl RtpSlotDrainReport {
    pub fn merge(&mut self, other: &Self) {
        self.packets_sent += other.packets_sent;
        self.bytes_sent += other.bytes_sent;
        self.payload_bytes_sent += other.payload_bytes_sent;
        self.media_sent_ms = self.media_sent_ms.saturating_add(other.media_sent_ms);
        self.max_pacing_late_ms = self.max_pacing_late_ms.max(other.max_pacing_late_ms);
        self.last_rtp_timestamp = other.last_rtp_timestamp.or(self.last_rtp_timestamp);
        self.stopped_on_prebuffer |= other.stopped_on_prebuffer;
        self.stopped_on_underrun |= other.stopped_on_underrun;
        self.stopped_on_pacing |= other.stopped_on_pacing;
    }
}

#[cfg(feature = "transport-rtp")]
#[derive(Clone, Debug, PartialEq)]
pub struct RtpSlotTickReport {
    pub worker: WorkerTurnReport,
    pub events: Vec<WorkerEvent>,
    pub drain: RtpSlotDrainReport,
}

#[cfg(feature = "transport-rtp")]
impl RtpSlotTickReport {
    #[must_use]
    pub fn made_progress(&self) -> bool {
        self.worker.decoded_chunks > 0
            || self.worker.decoded_frames > 0
            || self.worker.encoded_frames > 0
            || !self.events.is_empty()
            || self.drain.packets_sent > 0
    }
}

#[cfg(feature = "transport-rtp")]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RtpSlotRunReport {
    pub ticks: usize,
    pub events: Vec<WorkerEvent>,
    pub drain: RtpSlotDrainReport,
    pub completed: bool,
    pub stopped_on_limit: bool,
}

#[cfg(feature = "transport-rtp")]
#[derive(Debug)]
pub struct RtpSlotRunner<D, E, S> {
    driver: SlotDriver<D, E>,
    packetizer: RtpPacketizer,
    sink: S,
    scratch: bytes::BytesMut,
    pacer: RtpPacer,
}

#[cfg(feature = "transport-rtp")]
impl<D, E, S> RtpSlotRunner<D, E, S>
where
    D: DecoderBackend,
    E: OpusEncoderBackend,
    S: RtpPacketSink,
{
    #[must_use]
    pub fn new(driver: SlotDriver<D, E>, packetizer: RtpPacketizer, sink: S) -> Self {
        Self {
            driver,
            packetizer,
            sink,
            scratch: bytes::BytesMut::new(),
            pacer: RtpPacer::new(),
        }
    }

    #[must_use]
    pub fn driver(&self) -> &SlotDriver<D, E> {
        &self.driver
    }

    #[must_use]
    pub fn sink(&self) -> &S {
        &self.sink
    }

    pub fn sink_mut(&mut self) -> &mut S {
        &mut self.sink
    }

    #[must_use]
    pub fn pacer(&self) -> &RtpPacer {
        &self.pacer
    }

    pub fn pacer_mut(&mut self) -> &mut RtpPacer {
        &mut self.pacer
    }

    pub fn into_parts(self) -> (SlotDriver<D, E>, RtpPacketizer, S) {
        (self.driver, self.packetizer, self.sink)
    }

    pub fn worker_turn(&mut self) -> Result<SlotTurnReport> {
        self.driver.worker_turn()
    }

    pub fn set_volume(&mut self, volume: VolumeLevel) -> Result<()> {
        self.driver.set_volume(volume)
    }

    pub fn set_gain(&mut self, gain: GainLevel) -> Result<()> {
        self.driver.set_gain(gain)
    }

    pub fn tick(&mut self, max_packets: usize) -> Result<RtpSlotTickReport> {
        let turn = self.worker_turn()?;
        let mut events = Vec::new();
        if let Some(event) = turn.event {
            events.push(event);
        }
        let drain = self.drain_ready_packets(max_packets)?;

        Ok(RtpSlotTickReport {
            worker: turn.worker,
            events,
            drain,
        })
    }

    pub fn run_until_idle(
        &mut self,
        max_ticks: usize,
        max_packets_per_tick: usize,
    ) -> Result<RtpSlotRunReport> {
        let mut report = RtpSlotRunReport::default();
        for _ in 0..max_ticks {
            let tick = self.tick(max_packets_per_tick)?;
            report.ticks += 1;
            report.events.extend(tick.events.iter().cloned());
            report.drain.merge(&tick.drain);

            if self.driver.ended_reported() {
                report.completed = true;
                return Ok(report);
            }
            if !tick.made_progress() {
                return Ok(report);
            }
        }

        report.stopped_on_limit = !self.driver.ended_reported();
        report.completed = self.driver.ended_reported();
        Ok(report)
    }

    pub fn drain_ready_packets(&mut self, max_packets: usize) -> Result<RtpSlotDrainReport> {
        self.drain_packets(max_packets, None)
    }

    pub fn drain_due_packets(
        &mut self,
        now_ms: u64,
        max_packets: usize,
    ) -> Result<RtpSlotDrainReport> {
        self.drain_packets(max_packets, Some(now_ms))
    }

    fn drain_packets(
        &mut self,
        max_packets: usize,
        pacing_now_ms: Option<u64>,
    ) -> Result<RtpSlotDrainReport> {
        let mut report = RtpSlotDrainReport::default();
        for _ in 0..max_packets {
            if let Some(now_ms) = pacing_now_ms
                && matches!(self.pacer.poll(now_ms), RtpPaceDecision::Wait { .. })
            {
                report.stopped_on_pacing = true;
                break;
            }

            match self
                .driver
                .rtp_sender_step(&self.packetizer, &mut self.scratch)?
            {
                RtpSenderStep::Packet(packet) => {
                    let packet_len = packet.bytes.len();
                    let payload_len = packet.payload_len;
                    let packet_duration_ms = packet.duration_ms;
                    let rtp_timestamp = packet.rtp_timestamp;
                    self.sink.send(packet)?;
                    report.packets_sent += 1;
                    report.bytes_sent += packet_len;
                    report.payload_bytes_sent += payload_len;
                    report.media_sent_ms = report.media_sent_ms.saturating_add(packet_duration_ms);
                    if let Some(now_ms) = pacing_now_ms {
                        report.max_pacing_late_ms = report
                            .max_pacing_late_ms
                            .max(self.pacer.lateness_ms(now_ms));
                    }
                    report.last_rtp_timestamp = Some(rtp_timestamp);
                    if let Some(now_ms) = pacing_now_ms {
                        self.pacer.on_packet_sent(now_ms, packet_duration_ms);
                    }
                }
                RtpSenderStep::WaitPrebuffer { .. } => {
                    report.stopped_on_prebuffer = true;
                    break;
                }
                RtpSenderStep::Underrun { .. } => {
                    if pacing_now_ms.is_some() {
                        self.pacer.on_underrun();
                    }
                    report.stopped_on_underrun = true;
                    break;
                }
            }
        }

        Ok(report)
    }
}

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
impl LocalFileSlotConfig {
    #[must_use]
    pub fn new(pipeline: PipelineConfig) -> Self {
        let target = AudioFormat {
            sample_rate: pipeline.sample_rate,
            channels: pipeline.channels,
        };
        let opus = LibOpusEncoderConfig {
            sample_rate: pipeline.sample_rate,
            channels: pipeline.channels,
            ..LibOpusEncoderConfig::default()
        };

        Self {
            pipeline,
            opus,
            resampler: RubatoResamplerConfig::new(target),
            start_position_ms: 0,
        }
    }
}

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
impl LiveStreamSlotConfig {
    #[must_use]
    pub fn new(pipeline: PipelineConfig) -> Self {
        let target = AudioFormat {
            sample_rate: pipeline.sample_rate,
            channels: pipeline.channels,
        };
        let opus = LibOpusEncoderConfig {
            sample_rate: pipeline.sample_rate,
            channels: pipeline.channels,
            ..LibOpusEncoderConfig::default()
        };

        Self {
            pipeline,
            opus,
            resampler: RubatoResamplerConfig::new(target),
            http: HttpLiveStreamConfig::default(),
        }
    }
}

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
pub fn build_local_file_slot(
    role: SlotRole,
    source: &TrackSource,
    config: LocalFileSlotConfig,
) -> Result<LocalFileSlot> {
    build_local_file_slot_with_resolver(role, source, config, &FileSourceResolver::default())
}

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
pub fn build_local_file_slot_with_resolver<R>(
    role: SlotRole,
    source: &TrackSource,
    config: LocalFileSlotConfig,
    resolver: &R,
) -> Result<LocalFileSlot>
where
    R: SourceResolver,
{
    let artifact = resolver.resolve(source)?;
    let decoder = SymphoniaFileDecoder::open_at(artifact.path(), config.start_position_ms)?;
    let decoder = RubatoResamplingDecoder::new(decoder, config.resampler)?;
    let encoder = LibOpusEncoder::new(config.opus)?;
    let generation = config.pipeline.generation;
    let pipeline = PlayoutPipeline::new(decoder, encoder, config.pipeline)?;
    let driver = SlotDriver::new(role, generation, pipeline).with_artifact(artifact.clone());

    Ok(LocalFileSlot { artifact, driver })
}

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
pub fn build_live_stream_slot(
    role: SlotRole,
    source: &TrackSource,
    config: LiveStreamSlotConfig,
) -> Result<LiveStreamSlot> {
    if !source.is_live() {
        return Err(MusicStreamError::InvalidSource(
            "live stream slot requires a live track source".to_owned(),
        ));
    }

    let mut stream = spawn_http_live_stream(source, config.http)?;
    let reader = stream.take_reader().ok_or_else(|| {
        MusicStreamError::Internal("live HTTP stream reader was already taken".to_owned())
    })?;
    let decoder = match SymphoniaStreamDecoder::open(reader, live_hint_extension(source)) {
        Ok(decoder) => decoder,
        Err(decode_error) => {
            stream.stop();
            return match stream.join() {
                Err(source_error) => Err(source_error),
                Ok(_) => Err(decode_error),
            };
        }
    };
    let decoder = RubatoResamplingDecoder::new(decoder, config.resampler)?;
    let encoder = LibOpusEncoder::new(config.opus)?;
    let generation = config.pipeline.generation;
    let pipeline = PlayoutPipeline::new(decoder, encoder, config.pipeline)?;
    let driver = SlotDriver::new(role, generation, pipeline);

    Ok(LiveStreamSlot { stream, driver })
}

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
fn live_hint_extension(source: &TrackSource) -> Option<&str> {
    let url = source.url.as_deref()?;
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let extension = path.rsplit('/').next()?.rsplit_once('.')?.1;
    if extension.is_empty()
        || extension.len() > 8
        || !extension.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return None;
    }
    Some(extension)
}

#[derive(Debug)]
pub struct SlotDriver<D, E> {
    role: SlotRole,
    generation: u64,
    pipeline: PlayoutPipeline<D, E>,
    ready_reported: bool,
    ended_reported: bool,
    artifact: Option<SourceArtifact>,
}

impl<D, E> SlotDriver<D, E>
where
    D: DecoderBackend,
    E: OpusEncoderBackend,
{
    #[must_use]
    pub fn new(role: SlotRole, generation: u64, pipeline: PlayoutPipeline<D, E>) -> Self {
        Self {
            role,
            generation,
            pipeline,
            ready_reported: false,
            ended_reported: false,
            artifact: None,
        }
    }

    #[must_use]
    pub fn with_artifact(mut self, artifact: SourceArtifact) -> Self {
        self.artifact = Some(artifact);
        self
    }

    #[must_use]
    pub fn artifact(&self) -> Option<&SourceArtifact> {
        self.artifact.as_ref()
    }

    #[must_use]
    pub fn role(&self) -> SlotRole {
        self.role
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn into_role(mut self, role: SlotRole) -> Self {
        self.role = role;
        self.ready_reported = false;
        self.ended_reported = false;
        self
    }

    #[must_use]
    pub fn ready_reported(&self) -> bool {
        self.ready_reported
    }

    #[must_use]
    pub fn ended_reported(&self) -> bool {
        self.ended_reported
    }

    #[must_use]
    pub fn pipeline(&self) -> &PlayoutPipeline<D, E> {
        &self.pipeline
    }

    pub fn into_pipeline(self) -> PlayoutPipeline<D, E> {
        self.pipeline
    }

    pub fn worker_turn(&mut self) -> Result<SlotTurnReport> {
        let worker = self.pipeline.worker_turn()?;
        let event = self.event_for_report(&worker);
        Ok(SlotTurnReport { worker, event })
    }

    pub fn sender_step(&mut self) -> SenderStep {
        self.pipeline.sender_step()
    }

    pub fn set_volume(&mut self, volume: VolumeLevel) -> Result<()> {
        let mut config = self.pipeline.volume();
        config.level = volume;
        self.pipeline.set_volume(config)
    }

    pub fn set_gain(&mut self, gain: GainLevel) -> Result<()> {
        let mut config = self.pipeline.volume();
        config.extra_gain_db = gain.as_db();
        self.pipeline.set_volume(config)
    }

    #[cfg(feature = "transport-rtp")]
    pub fn rtp_sender_step(
        &mut self,
        packetizer: &RtpPacketizer,
        scratch: &mut bytes::BytesMut,
    ) -> Result<RtpSenderStep> {
        self.pipeline.rtp_sender_step(packetizer, scratch)
    }

    fn event_for_report(&mut self, report: &WorkerTurnReport) -> Option<WorkerEvent> {
        if !self.ready_reported {
            let ready = match self.role {
                SlotRole::Current => report.prebuffer_ready,
                SlotRole::Next => report.next_prime_ready,
            };
            if ready {
                self.ready_reported = true;
                return Some(match self.role {
                    SlotRole::Current => WorkerEvent::CurrentPrebufferReady {
                        generation: self.generation,
                    },
                    SlotRole::Next => WorkerEvent::NextReady {
                        generation: self.generation,
                    },
                });
            }
        }

        if self.role == SlotRole::Current && !self.ended_reported && report.playout_drained {
            self.ended_reported = true;
            return Some(WorkerEvent::CurrentEnded {
                generation: self.generation,
            });
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    #[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
    use std::io::{Read, Write};
    #[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
    use std::net::TcpListener;
    #[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
    use std::thread;
    #[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;
    use crate::audio::decode::{DecodePoll, DecodedChunk};
    use crate::audio::frame::{OpusFrame, PcmFrame};
    use crate::audio::pipeline::PipelineConfig;
    use crate::model::WatermarkConfig;
    #[cfg(feature = "transport-rtp")]
    use crate::transport::{MemoryRtpPacketSink, RtpPacketizerConfig};

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

    fn config(generation: u64) -> PipelineConfig {
        PipelineConfig {
            generation,
            sample_rate: 48_000,
            channels: 2,
            frame_samples_per_channel: 960,
            watermarks: WatermarkConfig {
                decode_batch_ms: 100,
                decoded_low_water_ms: 20,
                decoded_high_water_ms: 80,
                encoded_low_water_ms: 20,
                encoded_high_water_ms: 100,
                next_prime_ms: 40,
                pause_encoded_limit_ms: 2_000,
            },
            prebuffer_ms: 40,
        }
    }

    fn chunk(frames: usize) -> DecodePoll {
        DecodePoll::Chunk(DecodedChunk {
            sample_rate: 48_000,
            channels: 2,
            samples_interleaved: vec![0.0; frames * 960 * 2],
        })
    }

    fn slot(
        role: SlotRole,
        generation: u64,
        frames: usize,
    ) -> SlotDriver<FakeDecoder, FakeEncoder> {
        let pipeline = PlayoutPipeline::new(
            FakeDecoder::new([chunk(frames), DecodePoll::End]),
            FakeEncoder,
            config(generation),
        )
        .expect("pipeline");
        SlotDriver::new(role, generation, pipeline)
    }

    #[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
    fn serve_live_bytes(path: &'static str, body: Vec<u8>) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind live slot HTTP server");
        let addr = listener.local_addr().expect("live slot HTTP addr");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept live slot request");
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request);
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(headers.as_bytes())
                .expect("write live slot headers");
            stream.write_all(&body).expect("write live slot body");
            stream.flush().expect("flush live slot body");
        });
        (format!("http://{addr}{path}"), handle)
    }

    #[test]
    fn current_slot_reports_prebuffer_ready_once() {
        let mut slot = slot(SlotRole::Current, 7, 2);

        let first = slot.worker_turn().expect("first turn");
        assert_eq!(
            first.event,
            Some(WorkerEvent::CurrentPrebufferReady { generation: 7 })
        );
        assert!(slot.ready_reported());

        let second = slot.worker_turn().expect("second turn");
        assert_eq!(second.event, None);
    }

    #[test]
    fn next_slot_reports_next_ready_from_prime_watermark() {
        let mut slot = slot(SlotRole::Next, 9, 2);

        let report = slot.worker_turn().expect("turn");
        assert_eq!(report.event, Some(WorkerEvent::NextReady { generation: 9 }));
        assert!(slot.ready_reported());
    }

    #[test]
    fn current_slot_reports_ended_after_playout_drains() {
        let mut slot = slot(SlotRole::Current, 11, 1);
        assert!(matches!(
            slot.worker_turn().expect("ready").event,
            Some(WorkerEvent::CurrentPrebufferReady { generation: 11 })
        ));

        assert!(matches!(slot.sender_step(), SenderStep::Send { .. }));
        let ended = slot.worker_turn().expect("ended");

        assert_eq!(
            ended.event,
            Some(WorkerEvent::CurrentEnded { generation: 11 })
        );
        assert!(slot.ended_reported());
        assert_eq!(slot.worker_turn().expect("dedup ended").event, None);
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtp_slot_runner_drains_ready_packets_into_sink() {
        let mut runner = RtpSlotRunner::new(
            slot(SlotRole::Current, 13, 3),
            RtpPacketizer::new(RtpPacketizerConfig {
                payload_type: 111,
                ssrc: 42,
                mtu: 1_200,
            })
            .expect("packetizer"),
            MemoryRtpPacketSink::default(),
        );

        let turn = runner.worker_turn().expect("worker turn");
        assert_eq!(
            turn.event,
            Some(WorkerEvent::CurrentPrebufferReady { generation: 13 })
        );

        let drain = runner.drain_ready_packets(8).expect("drain packets");
        assert_eq!(drain.packets_sent, 3);
        assert!(drain.bytes_sent > 0);
        assert!(drain.stopped_on_underrun);
        assert!(!drain.stopped_on_prebuffer);

        let sink = runner.sink();
        assert_eq!(sink.len(), 3);
        assert_eq!(sink.packets()[0].sequence, 0);
        assert_eq!(sink.packets()[1].rtp_timestamp, 960);
        assert_eq!(sink.packets()[2].ssrc, 42);
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtp_slot_runner_respects_packet_drain_limit() {
        let mut runner = RtpSlotRunner::new(
            slot(SlotRole::Current, 14, 3),
            RtpPacketizer::new(RtpPacketizerConfig::default()).expect("packetizer"),
            MemoryRtpPacketSink::default(),
        );

        runner.worker_turn().expect("worker turn");

        let first = runner.drain_ready_packets(1).expect("first drain");
        assert_eq!(first.packets_sent, 1);
        assert!(!first.stopped_on_underrun);
        assert_eq!(runner.sink().len(), 1);

        let second = runner.drain_ready_packets(8).expect("second drain");
        assert_eq!(second.packets_sent, 2);
        assert!(second.stopped_on_underrun);
        assert_eq!(runner.sink().len(), 3);
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtp_slot_runner_paced_drain_does_not_burst_encoded_backlog() {
        let mut runner = RtpSlotRunner::new(
            slot(SlotRole::Current, 19, 3),
            RtpPacketizer::new(RtpPacketizerConfig::default()).expect("packetizer"),
            MemoryRtpPacketSink::default(),
        );

        runner.worker_turn().expect("worker turn");

        let first = runner.drain_due_packets(1_000, 8).expect("first drain");
        assert_eq!(first.packets_sent, 1);
        assert_eq!(first.max_pacing_late_ms, 0);
        assert!(first.stopped_on_pacing);
        assert_eq!(runner.pacer().next_deadline_ms(), Some(1_020));
        assert_eq!(runner.sink().len(), 1);

        let too_early = runner.drain_due_packets(1_019, 8).expect("too early drain");
        assert_eq!(too_early.packets_sent, 0);
        assert!(too_early.stopped_on_pacing);
        assert_eq!(runner.sink().len(), 1);

        let second = runner.drain_due_packets(1_020, 8).expect("second drain");
        assert_eq!(second.packets_sent, 1);
        assert_eq!(second.max_pacing_late_ms, 0);
        assert!(second.stopped_on_pacing);
        assert_eq!(runner.sink().len(), 2);
        assert_eq!(runner.sink().packets()[1].sequence, 1);
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtp_slot_runner_reports_pacing_lateness_without_bursting() {
        let mut runner = RtpSlotRunner::new(
            slot(SlotRole::Current, 20, 3),
            RtpPacketizer::new(RtpPacketizerConfig::default()).expect("packetizer"),
            MemoryRtpPacketSink::default(),
        );

        runner.worker_turn().expect("worker turn");
        let first = runner.drain_due_packets(1_000, 8).expect("first drain");
        assert_eq!(first.packets_sent, 1);

        let late = runner.drain_due_packets(1_035, 8).expect("late drain");
        assert_eq!(late.packets_sent, 1);
        assert_eq!(late.max_pacing_late_ms, 15);
        assert!(late.stopped_on_pacing);
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtp_slot_runner_tick_reports_worker_events_and_drain_together() {
        let mut runner = RtpSlotRunner::new(
            slot(SlotRole::Current, 15, 2),
            RtpPacketizer::new(RtpPacketizerConfig::default()).expect("packetizer"),
            MemoryRtpPacketSink::default(),
        );

        let tick = runner.tick(1).expect("tick");

        assert_eq!(
            tick.events,
            vec![WorkerEvent::CurrentPrebufferReady { generation: 15 }]
        );
        assert_eq!(tick.drain.packets_sent, 1);
        assert!(tick.made_progress());
        assert_eq!(runner.sink().len(), 1);
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtp_slot_runner_can_run_current_slot_until_ended_event() {
        let mut runner = RtpSlotRunner::new(
            slot(SlotRole::Current, 16, 3),
            RtpPacketizer::new(RtpPacketizerConfig::default()).expect("packetizer"),
            MemoryRtpPacketSink::default(),
        );

        let report = runner.run_until_idle(8, 2).expect("run until idle");

        assert!(report.completed);
        assert!(!report.stopped_on_limit);
        assert_eq!(report.drain.packets_sent, 3);
        assert_eq!(runner.sink().len(), 3);
        assert_eq!(
            report.events,
            vec![
                WorkerEvent::CurrentPrebufferReady { generation: 16 },
                WorkerEvent::CurrentEnded { generation: 16 },
            ]
        );
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtp_slot_runner_stops_on_tick_limit_without_claiming_completion() {
        let mut runner = RtpSlotRunner::new(
            slot(SlotRole::Current, 18, 3),
            RtpPacketizer::new(RtpPacketizerConfig::default()).expect("packetizer"),
            MemoryRtpPacketSink::default(),
        );

        let report = runner.run_until_idle(1, 1).expect("run until limit");

        assert!(!report.completed);
        assert!(report.stopped_on_limit);
        assert_eq!(report.drain.packets_sent, 1);
        assert_eq!(
            report.events,
            vec![WorkerEvent::CurrentPrebufferReady { generation: 18 }]
        );
    }

    #[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
    #[test]
    fn local_file_slot_factory_builds_real_decode_pipeline() {
        let temp = tempfile::Builder::new()
            .suffix(".wav")
            .tempfile()
            .expect("temp wav");
        write_test_wav(temp.path(), 4_410, 44_100, 1).expect("write wav");

        let source = crate::model::TrackSource {
            id: "file-a".to_owned(),
            kind: crate::model::TrackKind::File,
            url: None,
            path: Some(temp.path().to_string_lossy().into_owned()),
            seekable: Some(true),
        };
        let built = build_local_file_slot(
            SlotRole::Current,
            &source,
            LocalFileSlotConfig::new(config(17)),
        )
        .expect("local file slot");

        assert_eq!(built.artifact.track_id, "file-a");
        assert_eq!(built.driver.generation(), 17);

        let mut driver = built.driver;
        let turn = driver.worker_turn().expect("worker turn");
        assert!(turn.worker.encoded_frames > 0);
        assert_eq!(
            turn.event,
            Some(WorkerEvent::CurrentPrebufferReady { generation: 17 })
        );
    }

    #[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
    #[test]
    fn live_stream_slot_factory_builds_non_seekable_decode_pipeline() {
        let temp = tempfile::Builder::new()
            .suffix(".wav")
            .tempfile()
            .expect("temp wav");
        write_test_wav(temp.path(), 4_410, 44_100, 1).expect("write wav");
        let body = std::fs::read(temp.path()).expect("read wav");
        let (url, server) = serve_live_bytes("/live.wav", body.clone());
        let source = crate::model::TrackSource {
            id: "live-a".to_owned(),
            kind: crate::model::TrackKind::Live,
            url: Some(url),
            path: None,
            seekable: Some(false),
        };
        let mut config = LiveStreamSlotConfig::new(config(21));
        config.http.max_buffered_bytes = body.len() + 1024;
        config.http.read_chunk_bytes = 1024;

        let built =
            build_live_stream_slot(SlotRole::Current, &source, config).expect("live stream slot");
        let LiveStreamSlot { stream, mut driver } = built;
        let turn = driver.worker_turn().expect("worker turn");
        assert!(turn.worker.encoded_frames > 0);
        assert_eq!(
            turn.event,
            Some(WorkerEvent::CurrentPrebufferReady { generation: 21 })
        );

        let mut sent = 0;
        while matches!(driver.sender_step(), SenderStep::Send { .. }) {
            sent += 1;
        }
        assert!(sent > 0);
        let mut ended = None;
        for _ in 0..16 {
            let turn = driver.worker_turn().expect("ended turn");
            if turn.event.is_some() {
                ended = turn.event;
                break;
            }
            while matches!(driver.sender_step(), SenderStep::Send { .. }) {}
        }
        assert_eq!(ended, Some(WorkerEvent::CurrentEnded { generation: 21 }));
        let report = stream.join().expect("join live stream");
        server.join().expect("live slot server");
        assert!(report.completed);
    }

    #[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
    fn write_test_wav(
        path: &std::path::Path,
        samples_per_channel: usize,
        sample_rate: u32,
        channels: u16,
    ) -> std::io::Result<()> {
        use std::io::Write;

        let bits_per_sample = 16_u16;
        let bytes_per_sample = bits_per_sample / 8;
        let data_bytes =
            samples_per_channel as u32 * u32::from(channels) * u32::from(bytes_per_sample);
        let byte_rate = sample_rate * u32::from(channels) * u32::from(bytes_per_sample);
        let block_align = channels * bytes_per_sample;

        let mut file = std::fs::File::create(path)?;
        file.write_all(b"RIFF")?;
        file.write_all(&(36 + data_bytes).to_le_bytes())?;
        file.write_all(b"WAVE")?;
        file.write_all(b"fmt ")?;
        file.write_all(&16_u32.to_le_bytes())?;
        file.write_all(&1_u16.to_le_bytes())?;
        file.write_all(&channels.to_le_bytes())?;
        file.write_all(&sample_rate.to_le_bytes())?;
        file.write_all(&byte_rate.to_le_bytes())?;
        file.write_all(&block_align.to_le_bytes())?;
        file.write_all(&bits_per_sample.to_le_bytes())?;
        file.write_all(b"data")?;
        file.write_all(&data_bytes.to_le_bytes())?;
        for index in 0..samples_per_channel {
            let left = ((index as i16) % 1024).to_le_bytes();
            let right = (-(index as i16) % 1024).to_le_bytes();
            file.write_all(&left)?;
            if channels > 1 {
                file.write_all(&right)?;
            }
        }

        Ok(())
    }
}
