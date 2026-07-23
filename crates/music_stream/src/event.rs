use crate::error::ErrorCode;
use crate::model::StreamStatus;
use crate::quality::{RtcpNetworkQualityLevel, RtcpQualityWindowSnapshot};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SourceRole {
    Current,
    Next,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StreamEvent {
    StreamStarted {
        stream_id: String,
    },
    StreamStopped {
        stream_id: String,
    },
    StateChanged {
        status: Box<StreamStatus>,
    },
    NextNeeded {
        stream_id: String,
    },
    SourceRefreshNeeded {
        stream_id: String,
        attempt_id: String,
        track_id: String,
        source_role: SourceRole,
        generation: u64,
    },
    NetworkQualityChanged {
        stream_id: String,
        quality: RtcpNetworkQualityLevel,
        snapshot: RtcpQualityWindowSnapshot,
    },
    /// A single occurrence failed. Unlike `Error`, this is a recoverable media fact and carries
    /// enough identity for a policy layer to reject exactly that attempt without snapshot
    /// inference.
    AttemptFailed {
        stream_id: String,
        attempt_id: String,
        track_id: String,
        source_role: SourceRole,
        generation: u64,
        code: ErrorCode,
        message: String,
    },
    /// Runtime/output failure whose scope is the whole stream rather than one occurrence.
    Error {
        stream_id: String,
        code: ErrorCode,
        message: String,
    },
}
