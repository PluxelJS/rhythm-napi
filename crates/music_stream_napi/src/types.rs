use napi::bindgen_prelude::{BigInt, Buffer};
use napi_derive::napi;
use std::collections::HashMap;

#[derive(Debug)]
#[napi(object)]
pub struct RuntimeResourceLimitsInput {
    pub max_streams: Option<u32>,
    pub max_cpu_workers: Option<u32>,
    pub max_blocking_producers: Option<u32>,
    pub max_blocking_preloads: Option<u32>,
    pub max_concurrent_http_downloads: Option<u32>,
    pub max_concurrent_live_streams: Option<u32>,
    pub max_live_buffered_bytes: Option<i64>,
    pub max_tempfile_bytes: Option<i64>,
}

#[derive(Debug)]
#[napi(object)]
pub struct TrackSourceInput {
    pub attempt_id: Option<String>,
    pub id: String,
    #[napi(ts_type = "'file' | 'url' | 'live'")]
    pub kind: String,
    pub url: Option<String>,
    pub path: Option<String>,
    pub format_hint: Option<String>,
    pub seekable: Option<bool>,
    pub headers: Option<HashMap<String, String>>,
    #[napi(ts_type = "'public-only'")]
    pub network_policy: Option<String>,
}

#[derive(Debug)]
#[napi(object)]
pub struct DesiredPlaybackPlanInput {
    pub version: i64,
    pub current: Option<TrackSourceInput>,
    pub next: Option<TrackSourceInput>,
}

#[derive(Clone, Debug)]
#[napi(object)]
pub struct StreamStatusOutput {
    pub stream_id: String,
    pub current: Option<TrackSourceOutput>,
    pub next: Option<TrackSourceOutput>,
    #[napi(ts_type = "'idle' | 'buffering' | 'playing' | 'paused' | 'stopped'")]
    pub play_state: String,
    pub time_played_ms: i64,
    pub generation: i64,
    pub plan_version: i64,
    pub volume: f64,
    pub gain_db: f64,
    pub playout_diagnostics: Option<PlayoutDiagnosticsOutput>,
    pub receiver_report: Option<RtcpReceiverReportOutput>,
}

/// Bounded sender diagnostics. Counters are cumulative for the stream's persistent RTP sender,
/// including track switches; `bufferedMs` is the latest encoded queue depth.
#[derive(Clone, Debug)]
#[napi(object)]
pub struct PlayoutDiagnosticsOutput {
    pub buffered_ms: i64,
    pub packets_sent: i64,
    pub bytes_sent: i64,
    pub dropped_frames: i64,
    pub dropped_media_ms: i64,
    pub latency_recoveries: i64,
    pub underruns: i64,
    pub max_lateness_ms: i64,
    pub sequence: u32,
    pub rtp_timestamp: u32,
}

#[derive(Debug)]
#[napi(object)]
pub struct StreamStatusBatchItemOutput {
    pub stream_id: String,
    pub ok: bool,
    pub status: Option<StreamStatusOutput>,
    #[napi(
        ts_type = "'INVALID_SOURCE' | 'SOURCE_TIMEOUT' | 'SOURCE_AUTH_EXPIRED' | 'NOT_SEEKABLE' | 'DECODE_ERROR' | 'RESAMPLE_ERROR' | 'ENCODE_ERROR' | 'RTP_SEND_ERROR' | 'STREAM_CLOSED' | 'BUSY' | 'INTERNAL' | 'STREAM_NOT_FOUND' | 'STREAM_ALREADY_EXISTS' | 'INVALID_CONFIG' | 'UNSUPPORTED'"
    )]
    pub code: Option<String>,
    pub message: Option<String>,
}

#[derive(Clone, Debug)]
#[napi(object)]
pub struct TrackSourceOutput {
    pub attempt_id: Option<String>,
    pub id: String,
    #[napi(ts_type = "'file' | 'url' | 'live'")]
    pub kind: String,
    pub format_hint: Option<String>,
    pub seekable: Option<bool>,
}

#[derive(Debug)]
#[napi(object)]
pub struct StreamEventOutput {
    pub sequence: i64,
    #[napi(
        ts_type = "'streamStarted' | 'streamStopped' | 'stateChanged' | 'nextNeeded' | 'sourceRefreshNeeded' | 'networkQualityChanged' | 'attemptFailed' | 'error'"
    )]
    pub r#type: String,
    pub stream_id: Option<String>,
    pub track_id: Option<String>,
    pub attempt_id: Option<String>,
    pub generation: Option<i64>,
    #[napi(ts_type = "'current' | 'next'")]
    pub source_role: Option<String>,
    #[napi(ts_type = "'good' | 'degraded' | 'poor'")]
    pub quality: Option<String>,
    pub quality_samples: Option<u32>,
    pub latest_loss_percent: Option<f64>,
    pub average_loss_percent: Option<f64>,
    pub max_loss_percent: Option<f64>,
    pub average_jitter_ms: Option<f64>,
    pub max_jitter_ms: Option<f64>,
    pub average_round_trip_time_ms: Option<f64>,
    pub max_round_trip_time_ms: Option<f64>,
    #[napi(
        ts_type = "'INVALID_SOURCE' | 'SOURCE_TIMEOUT' | 'SOURCE_AUTH_EXPIRED' | 'NOT_SEEKABLE' | 'DECODE_ERROR' | 'RESAMPLE_ERROR' | 'ENCODE_ERROR' | 'RTP_SEND_ERROR' | 'STREAM_CLOSED' | 'BUSY' | 'INTERNAL' | 'STREAM_NOT_FOUND' | 'STREAM_ALREADY_EXISTS' | 'INVALID_CONFIG' | 'UNSUPPORTED'"
    )]
    pub code: Option<String>,
    pub message: Option<String>,
    pub status: Option<StreamStatusOutput>,
}

#[derive(Clone, Debug)]
#[napi(object)]
pub struct RtcpReceiverReportOutput {
    pub reports_received: u32,
    pub sender_ssrc: u32,
    pub source_ssrc: u32,
    pub fraction_lost: u32,
    pub fraction_lost_ratio: f64,
    pub fraction_lost_percent: f64,
    pub total_lost: u32,
    pub last_sequence_number: u32,
    pub jitter: u32,
    pub jitter_micros: i64,
    pub jitter_ms: f64,
    pub last_sender_report: u32,
    pub delay: u32,
    pub round_trip_time_micros: Option<i64>,
    pub round_trip_time_ms: Option<f64>,
}

#[derive(Debug)]
#[napi(object)]
pub struct ReplayGainInput {
    #[napi(ts_type = "'track' | 'album'")]
    pub mode: Option<String>,
    pub track_gain_db: Option<f64>,
    pub album_gain_db: Option<f64>,
    pub track_peak: Option<f64>,
    pub album_peak: Option<f64>,
    pub preamp_db: Option<f64>,
    pub prevent_clipping: Option<bool>,
    pub target_peak_dbfs: Option<f64>,
    pub fallback_to_other: Option<bool>,
}

#[derive(Debug)]
#[napi(object)]
pub struct ReplayGainRecommendationOutput {
    #[napi(ts_type = "'track' | 'album'")]
    pub source: String,
    pub gain_db: f64,
    pub requested_gain_db: f64,
    pub clipping_limited: bool,
    pub range_limited: bool,
}

#[napi(object)]
pub struct RtpEncryptionConfigInput {
    pub mode: String,
    pub secret_key: Option<Buffer>,
}

#[napi(object)]
pub struct RtpTransportConfigInput {
    pub ip: String,
    pub port: u32,
    pub rtcp_port: Option<u32>,
    pub audio_ssrc: u32,
    pub audio_pt: Option<u32>,
    pub bitrate: Option<u32>,
    pub rtcp_mux: Option<bool>,
    pub rtp_keepalive_interval_ms: Option<u32>,
    pub mtu: Option<u32>,
    pub local_ip: Option<String>,
    pub local_port: Option<u32>,
    pub encryption: Option<RtpEncryptionConfigInput>,
}

impl std::fmt::Debug for RtpEncryptionConfigInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RtpEncryptionConfigInput")
            .field("mode", &self.mode)
            .field(
                "secret_key_len",
                &self.secret_key.as_ref().map(|key| key.len()),
            )
            .finish()
    }
}

impl std::fmt::Debug for RtpTransportConfigInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RtpTransportConfigInput")
            .field("ip", &self.ip)
            .field("port", &self.port)
            .field("rtcp_port", &self.rtcp_port)
            .field("audio_ssrc", &self.audio_ssrc)
            .field("audio_pt", &self.audio_pt)
            .field("bitrate", &self.bitrate)
            .field("rtcp_mux", &self.rtcp_mux)
            .field("rtp_keepalive_interval_ms", &self.rtp_keepalive_interval_ms)
            .field("mtu", &self.mtu)
            .field("local_ip", &self.local_ip)
            .field("local_port", &self.local_port)
            .field("encryption", &self.encryption)
            .finish()
    }
}

#[derive(Debug)]
#[napi(object)]
pub struct HttpSourceConfigInput {
    pub io_timeout_ms: Option<i64>,
    pub max_bytes: Option<i64>,
    pub cache_temp_files: Option<bool>,
    pub max_retries: Option<u32>,
    pub retry_backoff_ms: Option<i64>,
}

#[derive(Debug)]
#[napi(object)]
pub struct HttpLiveSourceConfigInput {
    pub open_timeout_ms: Option<i64>,
    pub idle_timeout_ms: Option<i64>,
    pub max_buffered_bytes: Option<i64>,
    pub max_retries: Option<u32>,
    pub retry_backoff_ms: Option<i64>,
}

#[derive(Debug)]
#[napi(object)]
pub struct SourceResolverConfigInput {
    pub http: Option<HttpSourceConfigInput>,
    pub live_http: Option<HttpLiveSourceConfigInput>,
}

#[derive(Debug)]
#[napi(object)]
pub struct HttpSourceConfigOutput {
    pub io_timeout_ms: i64,
    pub max_bytes: i64,
    pub cache_temp_files: bool,
    pub max_retries: u32,
    pub retry_backoff_ms: i64,
}

#[derive(Debug)]
#[napi(object)]
pub struct HttpLiveSourceConfigOutput {
    pub open_timeout_ms: i64,
    pub idle_timeout_ms: i64,
    pub max_buffered_bytes: i64,
    pub max_retries: u32,
    pub retry_backoff_ms: i64,
}

#[derive(Debug)]
#[napi(object)]
pub struct SourceResolverConfigOutput {
    pub http: HttpSourceConfigOutput,
    pub live_http: HttpLiveSourceConfigOutput,
}

#[derive(Debug)]
#[napi(object)]
pub struct MediaBufferConfigInput {
    pub decode_batch_ms: Option<i64>,
    pub encoded_capacity_ms: Option<i64>,
    pub prebuffer_ms: Option<i64>,
    pub next_prime_ms: Option<i64>,
    pub max_playout_lateness_ms: Option<i64>,
}

#[derive(Debug)]
#[napi(object)]
pub struct ExternalPullConfigInput {
    pub bitrate: Option<i64>,
}

#[derive(Debug)]
#[napi(object)]
pub struct StartStreamInput {
    pub stream_id: String,
    pub current: TrackSourceInput,
    pub next: Option<TrackSourceInput>,
    pub transport: RtpTransportConfigInput,
    pub source: Option<SourceResolverConfigInput>,
    pub buffer: Option<MediaBufferConfigInput>,
    pub volume: Option<f64>,
    pub gain_db: Option<f64>,
    pub attempt_start_timeout_ms: Option<i64>,
}

#[derive(Debug)]
#[napi(object)]
pub struct StartExternalStreamInput {
    pub stream_id: String,
    pub current: TrackSourceInput,
    pub next: Option<TrackSourceInput>,
    pub output: Option<ExternalPullConfigInput>,
    pub source: Option<SourceResolverConfigInput>,
    pub buffer: Option<MediaBufferConfigInput>,
    pub volume: Option<f64>,
    pub gain_db: Option<f64>,
    pub attempt_start_timeout_ms: Option<i64>,
}

#[derive(Debug)]
#[napi(object)]
pub struct ExternalOpusFrameAckInput {
    pub lease_id: u32,
    pub generation: i64,
    #[napi(ts_type = "'sent' | 'late' | 'cancelled' | 'outputUnavailable'")]
    pub outcome: String,
}

#[napi(object)]
pub struct ExternalOpusFrameOutput {
    pub lease_id: u32,
    pub generation: i64,
    pub payload: Buffer,
    pub samples_per_channel: u32,
    pub media_position_ms: i64,
    pub deadline_monotonic_ns: BigInt,
}

impl std::fmt::Debug for ExternalOpusFrameOutput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExternalOpusFrameOutput")
            .field("lease_id", &self.lease_id)
            .field("generation", &self.generation)
            .field("payload_len", &self.payload.len())
            .field("samples_per_channel", &self.samples_per_channel)
            .field("media_position_ms", &self.media_position_ms)
            .field("deadline_monotonic_ns", &self.deadline_monotonic_ns)
            .finish()
    }
}

#[derive(Debug)]
#[napi(object)]
pub struct RtpTransportConfigOutput {
    pub remote_ip: String,
    pub remote_rtp_port: u32,
    pub remote_rtcp_port: Option<u32>,
    pub local_ip: String,
    pub local_rtp_port: u32,
    pub payload_type: u32,
    pub ssrc: u32,
    pub mtu: u32,
    pub rtcp_mux: bool,
    pub opus_bitrate_bps: Option<u32>,
    pub rtp_keepalive_interval_ms: Option<u32>,
    pub encryption_mode: String,
}
