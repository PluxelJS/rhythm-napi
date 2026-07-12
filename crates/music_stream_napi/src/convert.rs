use std::time::Duration;

use music_stream::{
    GainLevel, HttpLiveStreamConfig, HttpSourceConfig, MediaBufferConfig, MusicStreamError,
    ReplayGainConfig, ReplayGainMetadata, ReplayGainMode, ReplayGainRecommendation,
    ReplayGainSource, RtcpReceiverReportSnapshot, RtpEncryptionConfig, RtpTransportConfig,
    RuntimeResourceLimits, SourceResolverConfig, StreamRuntimeProgress, TrackSource,
};

use crate::types::*;

const DEFAULT_MUSIC_OPUS_BITRATE_BPS: u32 = 128_000;

pub(crate) fn media_buffer_config_from_input(
    input: Option<MediaBufferConfigInput>,
) -> std::result::Result<MediaBufferConfig, MusicStreamError> {
    let mut config = MediaBufferConfig::default();
    let Some(input) = input else {
        return Ok(config);
    };
    if let Some(value) = input.decode_batch_ms {
        config.decode_batch_ms = positive_millis(value, "buffer.decodeBatchMs")?;
    }
    if let Some(value) = input.encoded_capacity_ms {
        config.encoded_capacity_ms = positive_millis(value, "buffer.encodedCapacityMs")?;
    }
    if let Some(value) = input.prebuffer_ms {
        config.prebuffer_ms = positive_millis(value, "buffer.prebufferMs")?;
    }
    if let Some(value) = input.next_prime_ms {
        config.next_prime_ms = non_negative_millis(value, "buffer.nextPrimeMs")?;
    }
    if let Some(value) = input.max_playout_lateness_ms {
        config.max_playout_lateness_ms = non_negative_millis(value, "buffer.maxPlayoutLatenessMs")?;
    }
    config.validate()?;
    Ok(config)
}

fn positive_millis(value: i64, name: &str) -> std::result::Result<u64, MusicStreamError> {
    if value <= 0 {
        return Err(MusicStreamError::InvalidConfig(format!(
            "{name} must be greater than zero"
        )));
    }
    Ok(value as u64)
}

fn non_negative_millis(value: i64, name: &str) -> std::result::Result<u64, MusicStreamError> {
    u64::try_from(value)
        .map_err(|_| MusicStreamError::InvalidConfig(format!("{name} must be non-negative")))
}

pub(crate) fn runtime_resource_limits_from_input(
    input: Option<RuntimeResourceLimitsInput>,
) -> std::result::Result<RuntimeResourceLimits, MusicStreamError> {
    let mut limits = RuntimeResourceLimits::default();
    let Some(input) = input else {
        return Ok(limits);
    };
    if let Some(value) = input.max_streams {
        limits.max_streams = value as usize;
    }
    if let Some(value) = input.max_cpu_workers {
        limits.max_cpu_workers = value as usize;
    }
    if let Some(value) = input.max_blocking_producers {
        limits.max_blocking_producers = value as usize;
    }
    if let Some(value) = input.max_blocking_preloads {
        limits.max_blocking_preloads = value as usize;
    }
    if let Some(value) = input.max_concurrent_http_downloads {
        limits.max_concurrent_http_downloads = value as usize;
    }
    if let Some(value) = input.max_concurrent_live_streams {
        limits.max_concurrent_live_streams = value as usize;
    }
    if let Some(value) = input.max_live_buffered_bytes {
        limits.max_live_buffered_bytes = usize::try_from(value).map_err(|_| {
            MusicStreamError::InvalidConfig(
                "maxLiveBufferedBytes must fit in a positive usize".to_owned(),
            )
        })?;
    }
    if let Some(value) = input.max_tempfile_bytes {
        limits.max_tempfile_bytes = u64::try_from(value).map_err(|_| {
            MusicStreamError::InvalidConfig(
                "maxTempfileBytes must fit in a positive u64".to_owned(),
            )
        })?;
    }
    Ok(limits)
}

impl StreamStatusOutput {
    pub(crate) fn apply_progress(&mut self, progress: StreamRuntimeProgress) {
        self.time_played_ms = i64::try_from(progress.stream_position_ms()).unwrap_or(i64::MAX);
        self.receiver_report = progress.latest_receiver_report.map(Into::into);
    }
}

pub(crate) fn default_napi_source_config() -> SourceResolverConfig {
    let mut config = SourceResolverConfig::default();
    config.http.cache_temp_files = true;
    config
}

pub(crate) fn source_config_from_input(
    input: Option<SourceResolverConfigInput>,
) -> std::result::Result<SourceResolverConfig, MusicStreamError> {
    let mut config = default_napi_source_config();
    let Some(input) = input else {
        config.validate()?;
        return Ok(config);
    };
    if let Some(http) = input.http {
        if let Some(io_timeout_ms) = http.io_timeout_ms {
            if io_timeout_ms <= 0 {
                return Err(MusicStreamError::InvalidConfig(
                    "source.http.ioTimeoutMs must be greater than zero".to_owned(),
                ));
            }
            config.http.io_timeout = Duration::from_millis(io_timeout_ms as u64);
        }
        if let Some(max_bytes) = http.max_bytes {
            if max_bytes <= 0 {
                return Err(MusicStreamError::InvalidConfig(
                    "source.http.maxBytes must be greater than zero".to_owned(),
                ));
            }
            config.http.max_bytes = max_bytes as u64;
        }
        if let Some(cache_temp_files) = http.cache_temp_files {
            config.http.cache_temp_files = cache_temp_files;
        }
        if let Some(max_retries) = http.max_retries {
            config.http.max_retries = u8::try_from(max_retries).map_err(|_| {
                MusicStreamError::InvalidConfig("source.http.maxRetries must fit in u8".to_owned())
            })?;
        }
        if let Some(retry_backoff_ms) = http.retry_backoff_ms {
            if retry_backoff_ms < 0 {
                return Err(MusicStreamError::InvalidConfig(
                    "source.http.retryBackoffMs must be non-negative".to_owned(),
                ));
            }
            config.http.retry_backoff = Duration::from_millis(retry_backoff_ms as u64);
        }
    }
    if let Some(live_http) = input.live_http {
        if let Some(open_timeout_ms) = live_http.open_timeout_ms {
            if open_timeout_ms <= 0 {
                return Err(MusicStreamError::InvalidConfig(
                    "source.liveHttp.openTimeoutMs must be greater than zero".to_owned(),
                ));
            }
            config.live_http.open_timeout = Duration::from_millis(open_timeout_ms as u64);
        }
        if let Some(idle_timeout_ms) = live_http.idle_timeout_ms {
            if idle_timeout_ms <= 0 {
                return Err(MusicStreamError::InvalidConfig(
                    "source.liveHttp.idleTimeoutMs must be greater than zero".to_owned(),
                ));
            }
            config.live_http.idle_timeout = Duration::from_millis(idle_timeout_ms as u64);
        }
        if let Some(max_buffered_bytes) = live_http.max_buffered_bytes {
            if max_buffered_bytes <= 0 {
                return Err(MusicStreamError::InvalidConfig(
                    "source.liveHttp.maxBufferedBytes must be greater than zero".to_owned(),
                ));
            }
            config.live_http.max_buffered_bytes =
                usize::try_from(max_buffered_bytes).map_err(|_| {
                    MusicStreamError::InvalidConfig(
                        "source.liveHttp.maxBufferedBytes must fit in usize".to_owned(),
                    )
                })?;
        }
        if let Some(max_retries) = live_http.max_retries {
            if max_retries > u32::from(u8::MAX) {
                return Err(MusicStreamError::InvalidConfig(
                    "source.liveHttp.maxRetries must fit in u8".to_owned(),
                ));
            }
            config.live_http.max_retries = max_retries as u8;
        }
        if let Some(retry_backoff_ms) = live_http.retry_backoff_ms {
            if retry_backoff_ms < 0 {
                return Err(MusicStreamError::InvalidConfig(
                    "source.liveHttp.retryBackoffMs must be non-negative".to_owned(),
                ));
            }
            config.live_http.retry_backoff = Duration::from_millis(retry_backoff_ms as u64);
        }
    }
    config.validate()?;
    Ok(config)
}

pub(crate) fn replay_gain_from_input(
    input: ReplayGainInput,
) -> std::result::Result<(ReplayGainMetadata, ReplayGainConfig), MusicStreamError> {
    let mode = match input.mode.as_deref().unwrap_or("track") {
        "track" => ReplayGainMode::Track,
        "album" => ReplayGainMode::Album,
        mode => {
            return Err(MusicStreamError::InvalidConfig(format!(
                "unsupported ReplayGain mode: {mode}"
            )));
        }
    };

    let metadata = ReplayGainMetadata {
        track_gain: input
            .track_gain_db
            .map(|value| GainLevel::from_db(value as f32))
            .transpose()?,
        album_gain: input
            .album_gain_db
            .map(|value| GainLevel::from_db(value as f32))
            .transpose()?,
        track_peak: input.track_peak.map(checked_f32("trackPeak")).transpose()?,
        album_peak: input.album_peak.map(checked_f32("albumPeak")).transpose()?,
    };
    let config = ReplayGainConfig {
        mode,
        preamp: GainLevel::from_db(input.preamp_db.unwrap_or(0.0) as f32)?,
        prevent_clipping: input.prevent_clipping.unwrap_or(true),
        target_peak_dbfs: input.target_peak_dbfs.unwrap_or(-1.0) as f32,
        fallback_to_other: input.fallback_to_other.unwrap_or(true),
    };
    metadata.validate()?;
    config.validate()?;
    Ok((metadata, config))
}

impl TryFrom<TrackSourceInput> for TrackSource {
    type Error = music_stream::MusicStreamError;

    fn try_from(value: TrackSourceInput) -> std::result::Result<Self, Self::Error> {
        let kind = match value.kind.as_str() {
            "file" => music_stream::TrackKind::File,
            "url" => music_stream::TrackKind::Url,
            "live" => music_stream::TrackKind::Live,
            _ => {
                return Err(music_stream::MusicStreamError::InvalidSource(format!(
                    "unsupported track kind: {}",
                    value.kind
                )));
            }
        };

        let seekable = match kind {
            music_stream::TrackKind::Live => Some(false),
            music_stream::TrackKind::File | music_stream::TrackKind::Url => value.seekable,
        };
        let format_hint = value
            .format_hint
            .map(|hint| normalize_format_hint(&hint))
            .transpose()?;

        Ok(Self {
            id: value.id,
            kind,
            url: value.url,
            path: value.path,
            format_hint,
            seekable,
        })
    }
}

fn normalize_format_hint(hint: &str) -> std::result::Result<String, MusicStreamError> {
    let hint = hint.trim();
    if hint.is_empty() || hint.len() > 16 || !hint.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(MusicStreamError::InvalidSource(
            "formatHint must contain 1 to 16 ASCII letters or digits".to_owned(),
        ));
    }
    Ok(hint.to_ascii_lowercase())
}

impl From<music_stream::StreamStatus> for StreamStatusOutput {
    fn from(value: music_stream::StreamStatus) -> Self {
        Self {
            stream_id: value.stream_id,
            current: value.current.map(Into::into),
            next: value.next.map(Into::into),
            play_state: format!("{:?}", value.play_state).to_ascii_lowercase(),
            time_played_ms: value.time_played_ms as i64,
            generation: value.generation as i64,
            volume: f64::from(value.volume.as_unit()),
            gain_db: f64::from(value.gain.as_db()),
            receiver_report: None,
        }
    }
}

impl From<RtcpReceiverReportSnapshot> for RtcpReceiverReportOutput {
    fn from(value: RtcpReceiverReportSnapshot) -> Self {
        Self {
            reports_received: value.reports_received.try_into().unwrap_or(u32::MAX),
            sender_ssrc: value.sender_ssrc,
            source_ssrc: value.source_ssrc,
            fraction_lost: u32::from(value.fraction_lost),
            fraction_lost_ratio: f64::from(value.fraction_lost) / 256.0,
            fraction_lost_percent: (f64::from(value.fraction_lost) * 100.0) / 256.0,
            total_lost: value.total_lost,
            last_sequence_number: value.last_sequence_number,
            jitter: value.jitter,
            jitter_micros: value.jitter_micros.try_into().unwrap_or(i64::MAX),
            jitter_ms: value.jitter_micros as f64 / 1_000.0,
            last_sender_report: value.last_sender_report,
            delay: value.delay,
            round_trip_time_micros: value
                .round_trip_time_micros
                .map(|value| value.try_into().unwrap_or(i64::MAX)),
            round_trip_time_ms: value
                .round_trip_time_micros
                .map(|value| value as f64 / 1_000.0),
        }
    }
}

impl From<ReplayGainRecommendation> for ReplayGainRecommendationOutput {
    fn from(value: ReplayGainRecommendation) -> Self {
        let source = match value.source {
            ReplayGainSource::Track => "track",
            ReplayGainSource::Album => "album",
        };

        Self {
            source: source.to_owned(),
            gain_db: f64::from(value.gain.as_db()),
            requested_gain_db: f64::from(value.requested_gain_db),
            clipping_limited: value.clipping_limited,
            range_limited: value.range_limited,
        }
    }
}

impl From<TrackSource> for TrackSourceOutput {
    fn from(value: TrackSource) -> Self {
        let kind = match value.kind {
            music_stream::TrackKind::File => "file",
            music_stream::TrackKind::Url => "url",
            music_stream::TrackKind::Live => "live",
        };

        Self {
            id: value.id,
            kind: kind.to_owned(),
            format_hint: value.format_hint,
            seekable: value.seekable,
        }
    }
}

impl TryFrom<RtpTransportConfigInput> for RtpTransportConfig {
    type Error = music_stream::MusicStreamError;

    fn try_from(value: RtpTransportConfigInput) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            remote_ip: value.ip,
            remote_rtp_port: checked_port(value.port, "port", false)?,
            remote_rtcp_port: value
                .rtcp_port
                .map(|port| checked_port(port, "rtcpPort", false))
                .transpose()?,
            local_ip: value.local_ip.unwrap_or_else(|| "0.0.0.0".to_owned()),
            local_rtp_port: value
                .local_port
                .map(|port| checked_port(port, "localPort", true))
                .transpose()?
                .unwrap_or(0),
            payload_type: value
                .audio_pt
                .map(|payload_type| checked_u8(payload_type, "audioPt"))
                .transpose()?
                .unwrap_or(96),
            ssrc: value.audio_ssrc,
            mtu: value.mtu.unwrap_or(1_200) as usize,
            rtcp_mux: value.rtcp_mux.unwrap_or(true),
            opus_bitrate_bps: Some(value.bitrate.unwrap_or(DEFAULT_MUSIC_OPUS_BITRATE_BPS)),
            encryption: value
                .encryption
                .map(Into::into)
                .unwrap_or(RtpEncryptionConfig::None),
        })
    }
}

impl From<RtpEncryptionConfigInput> for RtpEncryptionConfig {
    fn from(value: RtpEncryptionConfigInput) -> Self {
        if value.mode == "none" {
            return Self::None;
        }

        Self::External {
            mode: value.mode,
            secret_key: value.secret_key.map(|buffer| buffer.to_vec()),
        }
    }
}

impl From<RtpTransportConfig> for RtpTransportConfigOutput {
    fn from(value: RtpTransportConfig) -> Self {
        let encryption_mode = match value.encryption {
            RtpEncryptionConfig::None => "none".to_owned(),
            RtpEncryptionConfig::External { mode, .. } => mode,
        };

        Self {
            remote_ip: value.remote_ip,
            remote_rtp_port: u32::from(value.remote_rtp_port),
            remote_rtcp_port: value.remote_rtcp_port.map(u32::from),
            local_ip: value.local_ip,
            local_rtp_port: u32::from(value.local_rtp_port),
            payload_type: u32::from(value.payload_type),
            ssrc: value.ssrc,
            mtu: value.mtu as u32,
            rtcp_mux: value.rtcp_mux,
            opus_bitrate_bps: value.opus_bitrate_bps,
            encryption_mode,
        }
    }
}

impl From<HttpSourceConfig> for HttpSourceConfigOutput {
    fn from(value: HttpSourceConfig) -> Self {
        Self {
            io_timeout_ms: value.io_timeout.as_millis().try_into().unwrap_or(i64::MAX),
            max_bytes: value.max_bytes.try_into().unwrap_or(i64::MAX),
            cache_temp_files: value.cache_temp_files,
            max_retries: u32::from(value.max_retries),
            retry_backoff_ms: value
                .retry_backoff
                .as_millis()
                .try_into()
                .unwrap_or(i64::MAX),
        }
    }
}

impl From<HttpLiveStreamConfig> for HttpLiveSourceConfigOutput {
    fn from(value: HttpLiveStreamConfig) -> Self {
        Self {
            open_timeout_ms: value
                .open_timeout
                .as_millis()
                .try_into()
                .unwrap_or(i64::MAX),
            idle_timeout_ms: value
                .idle_timeout
                .as_millis()
                .try_into()
                .unwrap_or(i64::MAX),
            max_buffered_bytes: value.max_buffered_bytes.try_into().unwrap_or(i64::MAX),
            max_retries: u32::from(value.max_retries),
            retry_backoff_ms: value
                .retry_backoff
                .as_millis()
                .try_into()
                .unwrap_or(i64::MAX),
        }
    }
}

impl From<SourceResolverConfig> for SourceResolverConfigOutput {
    fn from(value: SourceResolverConfig) -> Self {
        Self {
            http: value.http.into(),
            live_http: value.live_http.into(),
        }
    }
}

fn checked_port(
    value: u32,
    field: &'static str,
    allow_zero: bool,
) -> std::result::Result<u16, music_stream::MusicStreamError> {
    if value == 0 && allow_zero {
        return Ok(0);
    }
    if !(1..=u32::from(u16::MAX)).contains(&value) {
        return Err(music_stream::MusicStreamError::InvalidConfig(format!(
            "{field} must be between 1 and 65535"
        )));
    }
    Ok(value as u16)
}

fn checked_u8(
    value: u32,
    field: &'static str,
) -> std::result::Result<u8, music_stream::MusicStreamError> {
    if value > u32::from(u8::MAX) {
        return Err(music_stream::MusicStreamError::InvalidConfig(format!(
            "{field} must fit in u8"
        )));
    }
    Ok(value as u8)
}

fn checked_f32(
    field: &'static str,
) -> impl FnOnce(f64) -> std::result::Result<f32, music_stream::MusicStreamError> {
    move |value| {
        if !value.is_finite() || value < f64::from(f32::MIN) || value > f64::from(f32::MAX) {
            return Err(music_stream::MusicStreamError::InvalidConfig(format!(
                "{field} must be a finite f32"
            )));
        }
        Ok(value as f32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_format_hint_is_normalized_and_validated() {
        let track = TrackSource::try_from(TrackSourceInput {
            id: "opaque".to_owned(),
            kind: "url".to_owned(),
            url: Some("https://cdn.test/signed".to_owned()),
            path: None,
            format_hint: Some(" MP3 ".to_owned()),
            seekable: None,
        })
        .expect("format hint");
        assert_eq!(track.format_hint.as_deref(), Some("mp3"));

        let error = TrackSource::try_from(TrackSourceInput {
            id: "invalid".to_owned(),
            kind: "url".to_owned(),
            url: Some("https://cdn.test/signed".to_owned()),
            path: None,
            format_hint: Some("audio/mpeg".to_owned()),
            seekable: None,
        })
        .expect_err("MIME types are not format hints");
        assert_eq!(error.code(), music_stream::ErrorCode::InvalidSource);
    }
}
