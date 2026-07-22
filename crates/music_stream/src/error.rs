use thiserror::Error;

pub type Result<T> = std::result::Result<T, MusicStreamError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    InvalidSource,
    SourceTimeout,
    SourceAuthExpired,
    NotSeekable,
    DecodeError,
    ResampleError,
    EncodeError,
    RtpSendError,
    StreamClosed,
    Busy,
    Internal,
    StreamNotFound,
    StreamAlreadyExists,
    InvalidConfig,
    Unsupported,
}

impl ErrorCode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidSource => "INVALID_SOURCE",
            Self::SourceTimeout => "SOURCE_TIMEOUT",
            Self::SourceAuthExpired => "SOURCE_AUTH_EXPIRED",
            Self::NotSeekable => "NOT_SEEKABLE",
            Self::DecodeError => "DECODE_ERROR",
            Self::ResampleError => "RESAMPLE_ERROR",
            Self::EncodeError => "ENCODE_ERROR",
            Self::RtpSendError => "RTP_SEND_ERROR",
            Self::StreamClosed => "STREAM_CLOSED",
            Self::Busy => "BUSY",
            Self::Internal => "INTERNAL",
            Self::StreamNotFound => "STREAM_NOT_FOUND",
            Self::StreamAlreadyExists => "STREAM_ALREADY_EXISTS",
            Self::InvalidConfig => "INVALID_CONFIG",
            Self::Unsupported => "UNSUPPORTED",
        }
    }
}

#[derive(Debug, Error, Clone)]
pub enum MusicStreamError {
    #[error("stream not found: {0}")]
    StreamNotFound(String),

    #[error("stream already exists: {0}")]
    StreamAlreadyExists(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("invalid source: {0}")]
    InvalidSource(String),

    /// Internal routing signal emitted before any response body is consumed.
    /// Producers must convert it into the live HTTP path rather than expose it.
    #[doc(hidden)]
    #[error("detected live HTTP source: {0}")]
    DetectedLiveSource(String),

    #[error("source timed out: {0}")]
    SourceTimeout(String),

    #[error("source auth expired: {0}")]
    SourceAuthExpired(String),

    #[error("operation is not supported: {0}")]
    Unsupported(String),

    #[error("operation is busy: {0}")]
    Busy(String),

    #[error("stream is closed: {0}")]
    StreamClosed(String),

    #[error("track is not seekable: {0}")]
    NotSeekable(String),

    #[error("decode error: {0}")]
    DecodeError(String),

    #[error("resample error: {0}")]
    ResampleError(String),

    #[error("encode error: {0}")]
    EncodeError(String),

    #[error("rtp send error: {0}")]
    RtpSendError(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl MusicStreamError {
    #[must_use]
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::StreamNotFound(_) => ErrorCode::StreamNotFound,
            Self::StreamAlreadyExists(_) => ErrorCode::StreamAlreadyExists,
            Self::InvalidConfig(_) => ErrorCode::InvalidConfig,
            Self::InvalidSource(_) => ErrorCode::InvalidSource,
            Self::DetectedLiveSource(_) => ErrorCode::Unsupported,
            Self::SourceTimeout(_) => ErrorCode::SourceTimeout,
            Self::SourceAuthExpired(_) => ErrorCode::SourceAuthExpired,
            Self::Unsupported(_) => ErrorCode::Unsupported,
            Self::Busy(_) => ErrorCode::Busy,
            Self::StreamClosed(_) => ErrorCode::StreamClosed,
            Self::NotSeekable(_) => ErrorCode::NotSeekable,
            Self::DecodeError(_) => ErrorCode::DecodeError,
            Self::ResampleError(_) => ErrorCode::ResampleError,
            Self::EncodeError(_) => ErrorCode::EncodeError,
            Self::RtpSendError(_) => ErrorCode::RtpSendError,
            Self::Internal(_) => ErrorCode::Internal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_strings_are_stable_for_bindings() {
        assert_eq!(ErrorCode::InvalidSource.as_str(), "INVALID_SOURCE");
        assert_eq!(ErrorCode::SourceAuthExpired.as_str(), "SOURCE_AUTH_EXPIRED");
        assert_eq!(ErrorCode::StreamNotFound.as_str(), "STREAM_NOT_FOUND");
        assert_eq!(ErrorCode::Unsupported.as_str(), "UNSUPPORTED");
    }
}
