use crate::error::ErrorCode;
use crate::model::StreamStatus;
use crate::quality::{RtcpNetworkQualityLevel, RtcpQualityWindowSnapshot};

#[derive(Clone, Debug, PartialEq)]
pub enum StreamEvent {
    StreamStarted {
        stream_id: String,
    },
    StreamStopped {
        stream_id: String,
    },
    StateChanged {
        status: StreamStatus,
    },
    NextNeeded {
        stream_id: String,
    },
    SourceRefreshNeeded {
        stream_id: String,
        track_id: String,
    },
    NetworkQualityChanged {
        stream_id: String,
        quality: RtcpNetworkQualityLevel,
        snapshot: RtcpQualityWindowSnapshot,
    },
    Error {
        stream_id: String,
        code: ErrorCode,
        message: String,
    },
}
