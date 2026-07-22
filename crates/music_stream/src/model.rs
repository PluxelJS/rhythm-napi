use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrackKind {
    File,
    Url,
    Live,
}

const MAX_TRACK_ID_BYTES: usize = 512;
const MAX_SOURCE_LOCATION_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrackSource {
    pub id: String,
    pub kind: TrackKind,
    pub url: Option<String>,
    pub path: Option<String>,
    /// Optional media format hint such as `mp3`, `flac`, `ogg`, or `wav`.
    /// It describes the bytes, not the spelling of a temporary URL.
    pub format_hint: Option<String>,
    pub seekable: Option<bool>,
    /// Per-source HTTP request headers. They stay server-side and are never projected in status.
    pub headers: BTreeMap<String, String>,
}

impl TrackSource {
    pub fn validate(&self) -> crate::Result<()> {
        if self.id.trim().is_empty() || self.id.len() > MAX_TRACK_ID_BYTES {
            return Err(crate::MusicStreamError::InvalidSource(
                "track id must contain 1 to 512 bytes".to_owned(),
            ));
        }
        match self.kind {
            TrackKind::File
                if self.path.as_deref().is_none_or(|path| {
                    path.is_empty() || path.len() > MAX_SOURCE_LOCATION_BYTES
                }) =>
            {
                return Err(crate::MusicStreamError::InvalidSource(
                    "file source requires a path no longer than 16 KiB".to_owned(),
                ));
            }
            TrackKind::Url | TrackKind::Live => {
                let Some(url) = self.url.as_deref() else {
                    return Err(crate::MusicStreamError::InvalidSource(
                        "URL and live sources require url".to_owned(),
                    ));
                };
                if url.len() > MAX_SOURCE_LOCATION_BYTES
                    || !(url.starts_with("http://") || url.starts_with("https://"))
                {
                    return Err(crate::MusicStreamError::InvalidSource(
                        "URL and live sources require an HTTP(S) URL no longer than 16 KiB"
                            .to_owned(),
                    ));
                }
            }
            TrackKind::File => {}
        }
        if let Some(hint) = self.format_hint.as_deref()
            && (hint.is_empty()
                || hint.len() > 16
                || !hint.bytes().all(|byte| byte.is_ascii_alphanumeric()))
        {
            return Err(crate::MusicStreamError::InvalidSource(
                "format hint must contain 1 to 16 ASCII letters or digits".to_owned(),
            ));
        }
        validate_source_headers(self)?;
        Ok(())
    }

    #[must_use]
    pub fn stable_key(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub fn same_identity_as(&self, other: &Self) -> bool {
        self.stable_key() == other.stable_key()
    }

    #[must_use]
    pub fn is_live(&self) -> bool {
        self.kind == TrackKind::Live
    }

    #[must_use]
    pub fn is_hls(&self) -> bool {
        if self.kind == TrackKind::File {
            return false;
        }
        if self
            .format_hint
            .as_deref()
            .is_some_and(|hint| hint.eq_ignore_ascii_case("m3u8"))
        {
            return true;
        }
        let Some(url) = self.url.as_deref() else {
            return false;
        };
        let path = url.split(['?', '#']).next().unwrap_or(url);
        path.rsplit('/').next().is_some_and(|name| {
            name.rsplit_once('.')
                .is_some_and(|(_, extension)| extension.eq_ignore_ascii_case("m3u8"))
        })
    }

    #[must_use]
    pub fn with_detected_kind(mut self) -> Self {
        if self.is_hls() {
            self.kind = TrackKind::Live;
            self.seekable = Some(false);
        }
        self
    }

    #[must_use]
    pub fn is_seekable(&self) -> bool {
        match self.kind {
            TrackKind::Live => false,
            TrackKind::File | TrackKind::Url => self.seekable.unwrap_or(true),
        }
    }
}

fn validate_source_headers(source: &TrackSource) -> crate::Result<()> {
    const MAX_HEADERS: usize = 16;
    const MAX_HEADER_BYTES: usize = 16 * 1024;
    if source.kind == TrackKind::File && !source.headers.is_empty() {
        return Err(crate::MusicStreamError::InvalidSource(
            "file sources cannot contain HTTP headers".to_owned(),
        ));
    }
    if source.headers.len() > MAX_HEADERS
        || source
            .headers
            .iter()
            .map(|(name, value)| name.len().saturating_add(value.len()))
            .sum::<usize>()
            > MAX_HEADER_BYTES
    {
        return Err(crate::MusicStreamError::InvalidSource(
            "source headers exceed configured limits".to_owned(),
        ));
    }
    for (name, value) in &source.headers {
        let normalized = name.to_ascii_lowercase();
        if is_forbidden_source_header(&normalized)
            || reqwest::header::HeaderName::from_bytes(name.as_bytes()).is_err()
            || reqwest::header::HeaderValue::from_str(value).is_err()
        {
            return Err(crate::MusicStreamError::InvalidSource(format!(
                "unsupported source header: {name}"
            )));
        }
    }
    Ok(())
}

fn is_forbidden_source_header(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "content-length"
            | "host"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "range"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlayState {
    Idle,
    Buffering,
    Playing,
    Paused,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VolumeLevel {
    units: u16,
}

impl Default for VolumeLevel {
    fn default() -> Self {
        Self::MAX
    }
}

impl VolumeLevel {
    pub const SCALE: u16 = 10_000;
    pub const MIN: Self = Self { units: 0 };
    pub const MAX: Self = Self { units: Self::SCALE };

    pub fn from_unit(value: f32) -> crate::Result<Self> {
        if !value.is_finite() {
            return Err(crate::MusicStreamError::InvalidConfig(
                "volume must be finite".to_owned(),
            ));
        }
        if !(0.0..=1.0).contains(&value) {
            return Err(crate::MusicStreamError::InvalidConfig(
                "volume must be between 0.0 and 1.0".to_owned(),
            ));
        }

        Ok(Self {
            units: (value * f32::from(Self::SCALE)).round() as u16,
        })
    }

    pub fn from_units(units: u16) -> crate::Result<Self> {
        if units > Self::SCALE {
            return Err(crate::MusicStreamError::InvalidConfig(
                "volume units must be between 0 and 10000".to_owned(),
            ));
        }

        Ok(Self { units })
    }

    #[must_use]
    pub fn units(self) -> u16 {
        self.units
    }

    #[must_use]
    pub fn as_unit(self) -> f32 {
        f32::from(self.units) / f32::from(Self::SCALE)
    }

    #[must_use]
    pub fn is_muted(self) -> bool {
        self.units == 0
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GainLevel {
    centibels: i16,
}

impl GainLevel {
    pub const MIN_CENTIBELS: i16 = -6_000;
    pub const MAX_CENTIBELS: i16 = 1_200;

    pub fn from_db(value: f32) -> crate::Result<Self> {
        if !value.is_finite() {
            return Err(crate::MusicStreamError::InvalidConfig(
                "gainDb must be finite".to_owned(),
            ));
        }
        if !(-60.0..=12.0).contains(&value) {
            return Err(crate::MusicStreamError::InvalidConfig(
                "gainDb must be between -60 dB and +12 dB".to_owned(),
            ));
        }

        Ok(Self {
            centibels: (value * 100.0).round() as i16,
        })
    }

    pub fn from_centibels(centibels: i16) -> crate::Result<Self> {
        if !(Self::MIN_CENTIBELS..=Self::MAX_CENTIBELS).contains(&centibels) {
            return Err(crate::MusicStreamError::InvalidConfig(
                "gain centibels must be between -6000 and 1200".to_owned(),
            ));
        }
        Ok(Self { centibels })
    }

    #[must_use]
    pub fn centibels(self) -> i16 {
        self.centibels
    }

    #[must_use]
    pub fn as_db(self) -> f32 {
        f32::from(self.centibels) / 100.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamStatus {
    pub stream_id: String,
    pub current: Option<TrackSource>,
    pub next: Option<TrackSource>,
    pub play_state: PlayState,
    pub time_played_ms: u64,
    pub generation: u64,
    pub volume: VolumeLevel,
    pub gain: GainLevel,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MediaBufferConfig {
    pub decode_batch_ms: u64,
    pub encoded_capacity_ms: u64,
    pub prebuffer_ms: u64,
    pub next_prime_ms: u64,
    /// Maximum wall-clock delay retained by the real-time sender before stale
    /// encoded frames are discarded to catch up with the RTP timeline.
    pub max_playout_lateness_ms: u64,
}

const MAX_DECODE_BATCH_MS: u64 = 1_000;
const MAX_ENCODED_CAPACITY_MS: u64 = 10_000;

impl Default for MediaBufferConfig {
    fn default() -> Self {
        Self {
            decode_batch_ms: 80,
            encoded_capacity_ms: 400,
            prebuffer_ms: 100,
            next_prime_ms: 200,
            max_playout_lateness_ms: 100,
        }
    }
}

impl MediaBufferConfig {
    pub fn validate(&self) -> crate::Result<()> {
        if self.decode_batch_ms == 0 || self.encoded_capacity_ms == 0 || self.prebuffer_ms == 0 {
            return Err(crate::MusicStreamError::InvalidConfig(
                "media buffer durations must be greater than zero".to_owned(),
            ));
        }
        if self.decode_batch_ms > MAX_DECODE_BATCH_MS
            || self.encoded_capacity_ms > MAX_ENCODED_CAPACITY_MS
        {
            return Err(crate::MusicStreamError::InvalidConfig(
                "decode batch must not exceed 1 second and encoded capacity must not exceed 10 seconds"
                    .to_owned(),
            ));
        }
        if self.prebuffer_ms > self.encoded_capacity_ms
            || self.next_prime_ms > self.encoded_capacity_ms
            || self.max_playout_lateness_ms > self.encoded_capacity_ms
        {
            return Err(crate::MusicStreamError::InvalidConfig(
                "prebuffer, next prime, and maximum playout lateness must fit encoded capacity"
                    .to_owned(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(kind: TrackKind, seekable: Option<bool>) -> TrackSource {
        TrackSource {
            id: "track-a".to_owned(),
            kind,
            url: Some("https://example.test/audio".to_owned()),
            path: Some("/tmp/audio.wav".to_owned()),
            format_hint: None,
            seekable,
            headers: Default::default(),
        }
    }

    #[test]
    fn live_tracks_are_never_seekable_even_if_input_says_otherwise() {
        assert!(!source(TrackKind::Live, None).is_seekable());
        assert!(!source(TrackKind::Live, Some(false)).is_seekable());
        assert!(!source(TrackKind::Live, Some(true)).is_seekable());
    }

    #[test]
    fn bounded_tracks_default_to_seekable() {
        let file = source(TrackKind::File, None);
        let url = source(TrackKind::Url, None);
        let non_seekable_url = source(TrackKind::Url, Some(false));
        assert!(file.is_seekable());
        assert!(url.is_seekable());
        assert!(!non_seekable_url.is_seekable());
    }

    #[test]
    fn source_headers_are_bounded_and_cannot_override_transport_framing() {
        let mut source = source(TrackKind::Url, None);
        source
            .headers
            .insert("referer".to_owned(), "https://example.test/".to_owned());
        source.validate().expect("referer");
        source
            .headers
            .insert("host".to_owned(), "attacker.test".to_owned());
        assert!(source.validate().is_err());
    }

    #[test]
    fn source_validation_rejects_invalid_format_hint() {
        let mut url = source(TrackKind::Url, None);
        url.format_hint = Some("audio/mpeg".to_owned());
        assert_eq!(
            url.validate().expect_err("invalid hint").code(),
            crate::ErrorCode::InvalidSource
        );
    }

    #[test]
    fn media_buffer_validation_preserves_a_hard_memory_bound() {
        let error = MediaBufferConfig {
            encoded_capacity_ms: MAX_ENCODED_CAPACITY_MS + 1,
            ..MediaBufferConfig::default()
        }
        .validate()
        .expect_err("oversized encoded window");
        assert_eq!(error.code(), crate::ErrorCode::InvalidConfig);
    }

    #[test]
    fn hls_detection_normalizes_url_sources_to_live_semantics() {
        let mut by_extension = source(TrackKind::Url, Some(true));
        by_extension.url = Some("https://media.test/audio/index.m3u8?token=1".to_owned());
        assert!(by_extension.is_hls());
        let normalized = by_extension.with_detected_kind();
        assert_eq!(normalized.kind, TrackKind::Live);
        assert_eq!(normalized.seekable, Some(false));

        let mut by_hint = source(TrackKind::Url, None);
        by_hint.url = Some("https://media.test/opaque".to_owned());
        by_hint.format_hint = Some("M3U8".to_owned());
        assert!(by_hint.is_hls());
    }
}
