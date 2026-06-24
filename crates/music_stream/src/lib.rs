pub mod audio;
pub mod engine;
pub mod error;
pub mod event;
pub mod lifecycle;
pub mod model;
pub mod quality;
pub mod runtime;
pub mod session;
pub mod slot;
pub mod source;
pub mod transport;

pub use audio::AudioFormat;
pub use audio::decode::{
    DecodePoll, DecodedChunk, DecoderBackend, MemoryDecoder, NormalizingDecoder,
    StreamingPcmDecoder, StreamingPcmWriter,
};
#[cfg(feature = "decoder-symphonia")]
pub use audio::decode::{SymphoniaFileDecoder, SymphoniaStreamDecoder};
pub use audio::dsp::{
    ReplayGainConfig, ReplayGainMetadata, ReplayGainMode, ReplayGainRecommendation,
    ReplayGainSource, VolumeConfig, apply_gain_in_place, apply_gain_with_soft_limit_in_place,
    copy_f32_interleaved, db_to_linear, i16_to_f32_interleaved, recommend_replay_gain,
    soft_limit_sample, to_stereo_interleaved,
};
pub use audio::frame::{
    FrameAssembler, FrameQueue, OpusFrame, PcmFrame, QueueSnapshot, QueueWatermarks,
};
pub use audio::opus::{LibOpusEncoder, LibOpusEncoderConfig, OpusEncoderBackend};
pub use audio::pipeline::{PipelineConfig, PlayoutPipeline, WorkerTurnReport};
#[cfg(feature = "resampler-rubato")]
pub use audio::resample::{RubatoResamplerConfig, RubatoResamplingDecoder};
pub use engine::Engine;
pub use error::{ErrorCode, MusicStreamError, Result};
pub use event::StreamEvent;
pub use lifecycle::{
    GenerationScopedTask, GenerationTaskSlot, RuntimeTaskFailure, RuntimeTaskGroup,
    RuntimeTaskShutdownReport,
};
pub use model::{
    GainLevel, PlayState, StreamStatus, TrackKind, TrackSource, VolumeLevel, WatermarkConfig,
};
pub use quality::RtcpNetworkQualityLevel;
#[cfg(feature = "transport-rtp")]
pub use quality::{
    RtcpQualitySample, RtcpQualityWindow, RtcpQualityWindowConfig, RtcpQualityWindowSnapshot,
};
#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
pub use runtime::{
    LocalFilePreload, LocalFilePreloadCompletion, LocalFilePreloadReport, LocalFileRtpPlayback,
    LocalFileRtpPlaybackConfig, LocalFileRtpPlaybackProgress, LocalFileRtpPlaybackReport,
    default_pipeline_config, spawn_live_stream_rtp_playback, spawn_local_file_preload,
    spawn_local_file_rtp_playback, spawn_local_file_rtp_playback_from_driver,
};
pub use session::{
    ActorOutput, DEFAULT_STREAM_ACTOR_MAILBOX_CAPACITY, StreamActor, StreamActorMailbox,
    StreamActorMailboxHandle, StreamActorMailboxReply, StreamCommand, TaskAction, WorkerEvent,
};
#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
pub use slot::{
    LiveStreamSlot, LiveStreamSlotConfig, LiveStreamSlotDriver, LocalFileSlot, LocalFileSlotConfig,
    LocalFileSlotDriver, build_live_stream_slot, build_local_file_slot,
    build_local_file_slot_with_resolver,
};
#[cfg(feature = "transport-rtp")]
pub use slot::{RtpSlotDrainReport, RtpSlotRunReport, RtpSlotRunner, RtpSlotTickReport};
pub use slot::{SlotDriver, SlotRole, SlotTurnReport};
pub use source::{
    FileSourceResolver, HttpLiveStream, HttpLiveStreamConfig, HttpLiveStreamReport,
    HttpLiveStreamStopHandle, HttpSourceConfig, SharedSourceArtifactCache, SourceArtifact,
    SourceArtifactCache, SourceArtifactKind, SourceResolver, SourceResolverConfig,
    StreamingByteReader, StreamingByteSnapshot, StreamingByteWriter, resolve_http_temp_file,
    resolve_http_temp_file_with_config, resolve_local_file, spawn_http_live_stream,
};
#[cfg(feature = "transport-rtp")]
pub use transport::{
    MemoryRtpPacketSink, PlaintextRtpPacketProtector, ProtectedRtpPacketSink,
    RTP_OPUS_CLOCK_RATE_HZ, RtcpReceiverReportSnapshot, RtcpSenderReportPacket,
    RtpEncryptionConfig, RtpPaceDecision, RtpPacer, RtpPacketProtector, RtpPacketSink,
    RtpPacketized, RtpPacketizer, RtpPacketizerConfig, RtpSender, RtpSenderStep,
    RtpTransportConfig, UdpRtcpPacketSink, UdpRtpPacketSink, build_rtcp_sender_report,
    compact_ntp_timestamp, ntp_timestamp, parse_rtcp_receiver_reports,
    parse_rtcp_receiver_reports_at, rtcp_compact_duration_micros, rtcp_round_trip_time_micros,
    rtp_jitter_micros,
};
pub use transport::{SenderCore, SenderStep};
