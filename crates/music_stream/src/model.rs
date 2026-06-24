#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrackKind {
    File,
    Url,
    Live,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrackSource {
    pub id: String,
    pub kind: TrackKind,
    pub url: Option<String>,
    pub path: Option<String>,
    pub seekable: Option<bool>,
}

impl TrackSource {
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
    pub fn is_artifact_backed(&self) -> bool {
        matches!(self.kind, TrackKind::File | TrackKind::Url)
    }

    #[must_use]
    pub fn can_preload_as_next(&self) -> bool {
        self.is_artifact_backed()
    }

    #[must_use]
    pub fn is_seekable(&self) -> bool {
        match self.kind {
            TrackKind::Live => false,
            TrackKind::File | TrackKind::Url => self.seekable.unwrap_or(true),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlayState {
    Idle,
    Starting,
    Buffering,
    Playing,
    Paused,
    Switching,
    Stopping,
    Stopped,
    Error,
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
    pub time_total_ms: Option<u64>,
    pub generation: u64,
    pub volume: VolumeLevel,
    pub gain: GainLevel,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatermarkConfig {
    pub decode_batch_ms: u64,
    pub decoded_low_water_ms: u64,
    pub decoded_high_water_ms: u64,
    pub encoded_low_water_ms: u64,
    pub encoded_high_water_ms: u64,
    pub next_prime_ms: u64,
    pub pause_encoded_limit_ms: u64,
}

impl Default for WatermarkConfig {
    fn default() -> Self {
        Self {
            decode_batch_ms: 80,
            decoded_low_water_ms: 100,
            decoded_high_water_ms: 250,
            encoded_low_water_ms: 100,
            encoded_high_water_ms: 400,
            next_prime_ms: 200,
            pause_encoded_limit_ms: 2_000,
        }
    }
}

impl WatermarkConfig {
    pub fn validate(&self) -> crate::Result<()> {
        if self.decode_batch_ms == 0 {
            return Err(crate::MusicStreamError::InvalidConfig(
                "decode_batch_ms must be greater than zero".to_owned(),
            ));
        }

        if self.decoded_low_water_ms >= self.decoded_high_water_ms {
            return Err(crate::MusicStreamError::InvalidConfig(
                "decoded low water must be lower than decoded high water".to_owned(),
            ));
        }

        if self.encoded_low_water_ms >= self.encoded_high_water_ms {
            return Err(crate::MusicStreamError::InvalidConfig(
                "encoded low water must be lower than encoded high water".to_owned(),
            ));
        }

        if self.next_prime_ms > self.encoded_high_water_ms {
            return Err(crate::MusicStreamError::InvalidConfig(
                "next_prime_ms must not exceed encoded_high_water_ms".to_owned(),
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
            seekable,
        }
    }

    #[test]
    fn live_tracks_are_never_seekable_even_if_input_says_otherwise() {
        assert!(!source(TrackKind::Live, None).is_seekable());
        assert!(!source(TrackKind::Live, Some(false)).is_seekable());
        assert!(!source(TrackKind::Live, Some(true)).is_seekable());
    }

    #[test]
    fn artifact_backed_tracks_default_to_seekable_and_can_preload() {
        let file = source(TrackKind::File, None);
        let url = source(TrackKind::Url, None);
        let non_seekable_url = source(TrackKind::Url, Some(false));
        let live = source(TrackKind::Live, None);

        assert!(file.is_seekable());
        assert!(url.is_seekable());
        assert!(!non_seekable_url.is_seekable());
        assert!(file.can_preload_as_next());
        assert!(url.can_preload_as_next());
        assert!(!live.can_preload_as_next());
    }
}
