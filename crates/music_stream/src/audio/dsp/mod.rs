//! Gain, loudness mapping, downmix, limiter, and frame layout conversion.

use crate::error::{MusicStreamError, Result};
use crate::model::{GainLevel, VolumeLevel};

const DB_FLOOR: f32 = -120.0;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReplayGainMode {
    #[default]
    Track,
    Album,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayGainSource {
    Track,
    Album,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ReplayGainMetadata {
    pub track_gain: Option<GainLevel>,
    pub album_gain: Option<GainLevel>,
    pub track_peak: Option<f32>,
    pub album_peak: Option<f32>,
}

impl ReplayGainMetadata {
    pub fn validate(&self) -> Result<()> {
        validate_replay_gain_peak(self.track_peak, "track peak")?;
        validate_replay_gain_peak(self.album_peak, "album peak")?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReplayGainConfig {
    pub mode: ReplayGainMode,
    pub preamp: GainLevel,
    pub prevent_clipping: bool,
    pub target_peak_dbfs: f32,
    pub fallback_to_other: bool,
}

impl Default for ReplayGainConfig {
    fn default() -> Self {
        Self {
            mode: ReplayGainMode::Track,
            preamp: GainLevel::default(),
            prevent_clipping: true,
            target_peak_dbfs: -1.0,
            fallback_to_other: true,
        }
    }
}

impl ReplayGainConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.target_peak_dbfs.is_finite() || self.target_peak_dbfs > 0.0 {
            return Err(MusicStreamError::InvalidConfig(
                "target peak dBFS must be finite and less than or equal to 0".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReplayGainRecommendation {
    pub source: ReplayGainSource,
    pub gain: GainLevel,
    pub requested_gain_db: f32,
    pub clipping_limited: bool,
    pub range_limited: bool,
}

pub fn recommend_replay_gain(
    metadata: ReplayGainMetadata,
    config: ReplayGainConfig,
) -> Result<ReplayGainRecommendation> {
    metadata.validate()?;
    config.validate()?;

    let (source, metadata_gain, peak) = select_replay_gain(metadata, config)?;
    let mut requested_gain_db = metadata_gain.as_db() + config.preamp.as_db();
    let unclipped_gain_db = requested_gain_db;
    if config.prevent_clipping
        && let Some(peak) = peak
    {
        let peak_limited_gain_db = config.target_peak_dbfs - amplitude_to_dbfs(peak);
        requested_gain_db = requested_gain_db.min(peak_limited_gain_db);
    }

    let clamped_gain_db = requested_gain_db.clamp(
        f32::from(GainLevel::MIN_CENTIBELS) / 100.0,
        f32::from(GainLevel::MAX_CENTIBELS) / 100.0,
    );
    let gain = GainLevel::from_db(clamped_gain_db)?;

    Ok(ReplayGainRecommendation {
        source,
        gain,
        requested_gain_db,
        clipping_limited: requested_gain_db < unclipped_gain_db,
        range_limited: (clamped_gain_db - requested_gain_db).abs() > 0.000_1,
    })
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VolumeConfig {
    pub level: VolumeLevel,
    pub min_db: f32,
    pub curve_power: f32,
    pub extra_gain_db: f32,
    pub soft_limit: bool,
}

impl Default for VolumeConfig {
    fn default() -> Self {
        Self {
            level: VolumeLevel::MAX,
            min_db: -60.0,
            curve_power: 0.5,
            extra_gain_db: 0.0,
            soft_limit: true,
        }
    }
}

impl VolumeConfig {
    pub fn from_level(level: VolumeLevel) -> Self {
        Self {
            level,
            ..Self::default()
        }
    }

    pub fn validate(&self) -> Result<()> {
        if !self.min_db.is_finite() || self.min_db >= 0.0 {
            return Err(MusicStreamError::InvalidConfig(
                "volume min_db must be finite and lower than 0".to_owned(),
            ));
        }
        if !self.curve_power.is_finite() || self.curve_power <= 0.0 {
            return Err(MusicStreamError::InvalidConfig(
                "volume curve_power must be finite and greater than 0".to_owned(),
            ));
        }
        if !self.extra_gain_db.is_finite() || !(-60.0..=12.0).contains(&self.extra_gain_db) {
            return Err(MusicStreamError::InvalidConfig(
                "volume extra_gain_db must be between -60 dB and +12 dB".to_owned(),
            ));
        }

        Ok(())
    }

    #[must_use]
    pub fn gain_db(&self) -> f32 {
        if self.level.is_muted() {
            return f32::NEG_INFINITY;
        }

        let shaped = self.level.as_unit().powf(self.curve_power);
        self.min_db + (0.0 - self.min_db) * shaped + self.extra_gain_db
    }

    #[must_use]
    pub fn linear_gain(&self) -> f32 {
        if self.level.is_muted() {
            return 0.0;
        }

        db_to_linear(self.gain_db())
    }

    pub fn apply_in_place(&self, samples: &mut [f32]) {
        let gain = self.linear_gain();
        if gain == 0.0 {
            samples.fill(0.0);
            return;
        }

        if self.soft_limit && gain > 1.0 {
            apply_gain_with_soft_limit_in_place(samples, gain);
        } else {
            apply_gain_in_place(samples, gain);
        }
    }
}

pub fn to_stereo_interleaved(input: &[f32], channels: u16, output: &mut Vec<f32>) -> Result<()> {
    if channels == 0 {
        return Err(MusicStreamError::InvalidConfig(
            "channel count must be greater than zero".to_owned(),
        ));
    }

    let channels = usize::from(channels);
    if !input.len().is_multiple_of(channels) {
        return Err(MusicStreamError::InvalidConfig(
            "interleaved sample count must be divisible by channel count".to_owned(),
        ));
    }

    output.clear();
    match channels {
        1 => {
            output.reserve(input.len() * 2);
            for mono in input {
                output.push(*mono);
                output.push(*mono);
            }
        }
        2 => {
            output.extend_from_slice(input);
        }
        _ => {
            let frames = input.len() / channels;
            output.reserve(frames * 2);
            for frame in input.chunks_exact(channels) {
                let left = frame[0];
                let right = frame[1];
                let surround_sum: f32 = frame[2..].iter().copied().sum();
                let surround_gain = 0.5 / (channels.saturating_sub(2) as f32);
                let surround = surround_sum * surround_gain;
                output.push((left + surround).clamp(-1.0, 1.0));
                output.push((right + surround).clamp(-1.0, 1.0));
            }
        }
    }

    Ok(())
}

pub fn apply_gain_in_place(samples: &mut [f32], gain: f32) {
    for sample in samples {
        *sample = (*sample * gain).clamp(-1.0, 1.0);
    }
}

#[must_use]
pub fn db_to_linear(db: f32) -> f32 {
    10.0_f32.powf(db / 20.0)
}

#[must_use]
pub fn amplitude_to_dbfs(amplitude: f32) -> f32 {
    if !amplitude.is_finite() || amplitude <= 0.0 {
        return DB_FLOOR;
    }
    (20.0 * amplitude.abs().log10()).max(DB_FLOOR)
}

pub fn apply_gain_with_soft_limit_in_place(samples: &mut [f32], gain: f32) {
    for sample in samples {
        *sample = soft_limit_sample(*sample * gain);
    }
}

fn validate_replay_gain_peak(peak: Option<f32>, label: &str) -> Result<()> {
    let Some(peak) = peak else {
        return Ok(());
    };
    if !peak.is_finite() || peak <= 0.0 {
        return Err(MusicStreamError::InvalidConfig(format!(
            "ReplayGain {label} must be finite and greater than zero"
        )));
    }
    Ok(())
}

fn select_replay_gain(
    metadata: ReplayGainMetadata,
    config: ReplayGainConfig,
) -> Result<(ReplayGainSource, GainLevel, Option<f32>)> {
    let primary = match config.mode {
        ReplayGainMode::Track => (
            ReplayGainSource::Track,
            metadata.track_gain,
            metadata.track_peak,
        ),
        ReplayGainMode::Album => (
            ReplayGainSource::Album,
            metadata.album_gain,
            metadata.album_peak,
        ),
    };
    if let Some(gain) = primary.1 {
        return Ok((primary.0, gain, primary.2));
    }

    if config.fallback_to_other {
        let fallback = match config.mode {
            ReplayGainMode::Track => (
                ReplayGainSource::Album,
                metadata.album_gain,
                metadata.album_peak,
            ),
            ReplayGainMode::Album => (
                ReplayGainSource::Track,
                metadata.track_gain,
                metadata.track_peak,
            ),
        };
        if let Some(gain) = fallback.1 {
            return Ok((fallback.0, gain, fallback.2));
        }
    }

    Err(MusicStreamError::InvalidConfig(
        "ReplayGain metadata does not contain a gain for the requested mode".to_owned(),
    ))
}

#[must_use]
pub fn soft_limit_sample(sample: f32) -> f32 {
    const KNEE_START: f32 = 0.95;
    const KNEE_WIDTH: f32 = 1.0 - KNEE_START;

    let abs = sample.abs();
    if abs <= KNEE_START {
        return sample;
    }

    let compressed = KNEE_START + KNEE_WIDTH * (1.0 - (-(abs - KNEE_START) / KNEE_WIDTH).exp());
    sample.signum() * compressed.min(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_mono_to_stereo() {
        let mut output = Vec::new();
        to_stereo_interleaved(&[0.25, -0.5], 1, &mut output).expect("mono");
        assert_eq!(output, vec![0.25, 0.25, -0.5, -0.5]);
    }

    #[test]
    fn keeps_stereo_interleaved() {
        let mut output = Vec::new();
        to_stereo_interleaved(&[0.1, 0.2, 0.3, 0.4], 2, &mut output).expect("stereo");
        assert_eq!(output, vec![0.1, 0.2, 0.3, 0.4]);
    }

    #[test]
    fn downmixes_multichannel_without_changing_frame_count() {
        let mut output = Vec::new();
        to_stereo_interleaved(&[0.2, 0.4, 0.6, 0.8], 4, &mut output).expect("downmix");
        assert_eq!(output.len(), 2);
        assert!(output[0] > 0.5);
        assert!(output[1] > 0.7);
    }

    #[test]
    fn applies_gain_with_clamp() {
        let mut samples = vec![0.5, -0.75];
        apply_gain_in_place(&mut samples, 2.0);
        assert_eq!(samples, vec![1.0, -1.0]);
    }

    #[test]
    fn volume_level_maps_user_volume_to_perceptual_gain() {
        let muted = VolumeConfig::from_level(VolumeLevel::MIN);
        assert_eq!(muted.linear_gain(), 0.0);

        let full = VolumeConfig::from_level(VolumeLevel::MAX);
        assert!((full.gain_db() - 0.0).abs() < f32::EPSILON);
        assert!((full.linear_gain() - 1.0).abs() < 0.000_001);

        let half = VolumeConfig::from_level(VolumeLevel::from_unit(0.5).expect("volume"));
        assert!(half.gain_db() > -30.0);
        assert!(half.gain_db() < -10.0);
    }

    #[test]
    fn volume_config_mutes_without_breaking_frame_shape() {
        let mut samples = vec![0.25, -0.25, 0.5, -0.5];
        VolumeConfig::from_level(VolumeLevel::MIN).apply_in_place(&mut samples);
        assert_eq!(samples, vec![0.0; 4]);
    }

    #[test]
    fn soft_limiter_keeps_boosted_samples_bounded_without_hard_clipping_everything() {
        let mut samples = vec![0.25, 0.8, -0.8];
        let config = VolumeConfig {
            extra_gain_db: 6.0,
            ..VolumeConfig::default()
        };
        config.apply_in_place(&mut samples);

        assert!(samples[0] > 0.45);
        assert!(samples[1] <= 1.0);
        assert!(samples[1] > 0.95);
        assert!(samples[2] >= -1.0);
        assert!(samples[2] < -0.95);
    }

    #[test]
    fn replay_gain_recommends_explicit_track_gain_with_preamp() {
        let recommendation = recommend_replay_gain(
            ReplayGainMetadata {
                track_gain: Some(GainLevel::from_db(-7.5).expect("track gain")),
                album_gain: Some(GainLevel::from_db(-5.0).expect("album gain")),
                track_peak: Some(0.5),
                album_peak: Some(0.75),
            },
            ReplayGainConfig {
                preamp: GainLevel::from_db(2.0).expect("preamp"),
                ..ReplayGainConfig::default()
            },
        )
        .expect("recommend ReplayGain");

        assert_eq!(recommendation.source, ReplayGainSource::Track);
        assert_eq!(recommendation.gain, GainLevel::from_db(-5.5).expect("gain"));
        assert!(!recommendation.clipping_limited);
        assert!(!recommendation.range_limited);
    }

    #[test]
    fn replay_gain_can_fall_back_to_album_gain() {
        let recommendation = recommend_replay_gain(
            ReplayGainMetadata {
                album_gain: Some(GainLevel::from_db(-4.0).expect("album gain")),
                album_peak: Some(0.8),
                ..ReplayGainMetadata::default()
            },
            ReplayGainConfig::default(),
        )
        .expect("fallback recommendation");

        assert_eq!(recommendation.source, ReplayGainSource::Album);
        assert_eq!(recommendation.gain, GainLevel::from_db(-4.0).expect("gain"));
    }

    #[test]
    fn replay_gain_prevent_clipping_caps_positive_gain_against_peak() {
        let recommendation = recommend_replay_gain(
            ReplayGainMetadata {
                track_gain: Some(GainLevel::from_db(6.0).expect("track gain")),
                track_peak: Some(0.9),
                ..ReplayGainMetadata::default()
            },
            ReplayGainConfig::default(),
        )
        .expect("clipping-limited recommendation");

        assert_eq!(recommendation.source, ReplayGainSource::Track);
        assert!(recommendation.clipping_limited);
        assert!((recommendation.requested_gain_db - (-0.084_85)).abs() < 0.001);
        assert_eq!(
            recommendation.gain,
            GainLevel::from_db(-0.08).expect("gain")
        );
    }

    #[test]
    fn replay_gain_rejects_missing_requested_metadata_without_fallback() {
        let error = recommend_replay_gain(
            ReplayGainMetadata {
                album_gain: Some(GainLevel::from_db(-4.0).expect("album gain")),
                ..ReplayGainMetadata::default()
            },
            ReplayGainConfig {
                fallback_to_other: false,
                ..ReplayGainConfig::default()
            },
        )
        .expect_err("missing track gain");

        assert_eq!(error.code(), crate::error::ErrorCode::InvalidConfig);
    }
}
