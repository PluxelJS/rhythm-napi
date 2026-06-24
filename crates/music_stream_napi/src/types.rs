use napi::bindgen_prelude::Buffer;
use napi_derive::napi;

#[derive(Debug)]
#[napi(object)]
pub struct TrackSourceInput {
    pub id: String,
    pub kind: String,
    pub url: Option<String>,
    pub path: Option<String>,
    pub seekable: Option<bool>,
}

#[derive(Debug)]
#[napi(object)]
pub struct StreamStatusOutput {
    pub stream_id: String,
    pub current: Option<TrackSourceOutput>,
    pub next: Option<TrackSourceOutput>,
    pub play_state: String,
    pub time_played_ms: i64,
    pub time_total_ms: Option<i64>,
    pub generation: i64,
    pub volume: f64,
    pub gain_db: f64,
    pub receiver_report: Option<RtcpReceiverReportOutput>,
}

#[derive(Debug)]
#[napi(object)]
pub struct StreamStatusBatchItemOutput {
    pub stream_id: String,
    pub ok: bool,
    pub status: Option<StreamStatusOutput>,
    pub code: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug)]
#[napi(object)]
pub struct TrackSourceOutput {
    pub id: String,
    pub kind: String,
    pub url: Option<String>,
    pub path: Option<String>,
    pub seekable: Option<bool>,
}

#[derive(Debug)]
#[napi(object)]
pub struct StreamEventOutput {
    pub r#type: String,
    pub stream_id: Option<String>,
    pub track_id: Option<String>,
    pub quality: Option<String>,
    pub quality_samples: Option<u32>,
    pub latest_loss_percent: Option<f64>,
    pub average_loss_percent: Option<f64>,
    pub max_loss_percent: Option<f64>,
    pub average_jitter_ms: Option<f64>,
    pub max_jitter_ms: Option<f64>,
    pub average_round_trip_time_ms: Option<f64>,
    pub max_round_trip_time_ms: Option<f64>,
    pub code: Option<String>,
    pub message: Option<String>,
    pub status: Option<StreamStatusOutput>,
}

#[derive(Debug)]
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
    pub timeout_ms: Option<i64>,
    pub max_bytes: Option<i64>,
    pub cache_temp_files: Option<bool>,
}

#[derive(Debug)]
#[napi(object)]
pub struct HttpLiveSourceConfigInput {
    pub timeout_ms: Option<i64>,
    pub max_buffered_bytes: Option<i64>,
    pub read_chunk_bytes: Option<i64>,
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
    pub timeout_ms: i64,
    pub max_bytes: i64,
    pub cache_temp_files: bool,
}

#[derive(Debug)]
#[napi(object)]
pub struct HttpLiveSourceConfigOutput {
    pub timeout_ms: i64,
    pub max_buffered_bytes: i64,
    pub read_chunk_bytes: i64,
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
pub struct StartStreamInput {
    pub stream_id: String,
    pub current: TrackSourceInput,
    pub next: Option<TrackSourceInput>,
    pub transport: RtpTransportConfigInput,
    pub source: Option<SourceResolverConfigInput>,
    pub volume: Option<f64>,
    pub gain_db: Option<f64>,
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
    pub encryption_mode: String,
}
