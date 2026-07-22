//! Deterministic media core for source resolution, audio processing, and RTP playout.
//!
//! Consumers use the crate-root exports below. Internal module layout is intentionally
//! private so implementation refactors do not become downstream API changes.

mod audio;
mod control;
mod error;
mod event;
mod model;
mod quality;
mod runtime;
mod session;
mod source;
mod transport;

pub use audio::decode::{DecodePoll, DecodedChunk, DecoderBackend};
pub use audio::dsp::{
    ReplayGainConfig, ReplayGainMetadata, ReplayGainMode, ReplayGainRecommendation,
    ReplayGainSource, recommend_replay_gain,
};
pub use audio::frame::{OpusFrame, PcmFrame};
pub use audio::opus::{LibOpusEncoder, LibOpusEncoderConfig, OpusEncoderBackend};
pub use audio::pipeline::{PipelineConfig, PlayoutPipeline, WorkerTurnReport};
pub use error::{ErrorCode, MusicStreamError, Result};
pub use event::{SourceRole, StreamEvent};
pub use model::{
    GainLevel, MediaBufferConfig, NetworkPolicy, PlayState, StreamStatus, TrackKind, TrackSource,
    VolumeLevel,
};
pub use quality::{RtcpNetworkQualityLevel, RtcpQualityWindowSnapshot};
pub use runtime::{
    RuntimeResourceLimits, RuntimeResources, StreamRuntime, StreamRuntimeConfig,
    StreamRuntimeProgress, StreamRuntimeSnapshot,
};
pub use session::StreamCommand;
pub use source::{
    HttpLiveStreamConfig, HttpSourceConfig, SharedSourceArtifactCache, SourceArtifactCache,
    SourceResolverConfig,
};
pub use transport::{RtcpReceiverReportSnapshot, RtpEncryptionConfig, RtpTransportConfig};
