//! Opus encoder wrapper and packet payload preparation.

use bytes::{Bytes, BytesMut};

use crate::Result;
use crate::audio::frame::{OpusFrame, PcmFrame};
use crate::error::MusicStreamError;

pub trait OpusEncoderBackend {
    fn encode(&mut self, frame: &PcmFrame<'_>) -> Result<OpusFrame>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LibOpusEncoderConfig {
    pub sample_rate: u32,
    pub channels: u16,
    pub max_packet_bytes: usize,
    pub bitrate_bps: Option<i32>,
    pub complexity: i32,
    pub vbr: bool,
    pub constrained_vbr: bool,
}

impl Default for LibOpusEncoderConfig {
    fn default() -> Self {
        Self {
            sample_rate: 48_000,
            channels: 2,
            max_packet_bytes: 1_500,
            bitrate_bps: Some(128_000),
            complexity: 10,
            vbr: true,
            constrained_vbr: true,
        }
    }
}

#[derive(Debug)]
pub struct LibOpusEncoder {
    inner: opus::Encoder,
    config: LibOpusEncoderConfig,
    output: BytesMut,
}

impl LibOpusEncoder {
    pub fn new(config: LibOpusEncoderConfig) -> Result<Self> {
        if config.sample_rate != 48_000 {
            return Err(MusicStreamError::InvalidConfig(
                "Opus RTP output requires 48kHz input".to_owned(),
            ));
        }

        let channels = match config.channels {
            1 => opus::Channels::Mono,
            2 => opus::Channels::Stereo,
            _ => {
                return Err(MusicStreamError::InvalidConfig(
                    "Opus encoder supports mono or stereo input".to_owned(),
                ));
            }
        };

        if config.max_packet_bytes == 0 {
            return Err(MusicStreamError::InvalidConfig(
                "max_packet_bytes must be greater than zero".to_owned(),
            ));
        }
        if !(0..=10).contains(&config.complexity) {
            return Err(MusicStreamError::InvalidConfig(
                "Opus complexity must be between 0 and 10".to_owned(),
            ));
        }
        if config
            .bitrate_bps
            .is_some_and(|bitrate| !(500..=512_000).contains(&bitrate))
        {
            return Err(MusicStreamError::InvalidConfig(
                "Opus bitrate must be between 500 and 512000 bps".to_owned(),
            ));
        }

        let mut inner = opus::Encoder::new(config.sample_rate, channels, opus::Application::Audio)
            .map_err(|error| MusicStreamError::EncodeError(error.to_string()))?;
        inner
            .set_complexity(config.complexity)
            .map_err(|error| MusicStreamError::EncodeError(error.to_string()))?;
        inner
            .set_vbr(config.vbr)
            .map_err(|error| MusicStreamError::EncodeError(error.to_string()))?;
        inner
            .set_vbr_constraint(config.constrained_vbr)
            .map_err(|error| MusicStreamError::EncodeError(error.to_string()))?;
        if let Some(bitrate_bps) = config.bitrate_bps {
            inner
                .set_bitrate(opus::Bitrate::Bits(bitrate_bps))
                .map_err(|error| MusicStreamError::EncodeError(error.to_string()))?;
        }

        let output = BytesMut::zeroed(config.max_packet_bytes);
        Ok(Self {
            inner,
            config,
            output,
        })
    }
}

impl OpusEncoderBackend for LibOpusEncoder {
    fn encode(&mut self, frame: &PcmFrame<'_>) -> Result<OpusFrame> {
        if frame.sample_rate != self.config.sample_rate || frame.channels != self.config.channels {
            return Err(MusicStreamError::EncodeError(
                "PCM frame format does not match Opus encoder config".to_owned(),
            ));
        }

        let expected_samples =
            frame.samples_per_channel as usize * usize::from(self.config.channels);
        if frame.samples.len() != expected_samples {
            return Err(MusicStreamError::EncodeError(
                "PCM frame sample count does not match frame metadata".to_owned(),
            ));
        }

        let encoded_len = self
            .inner
            .encode_float(frame.samples, &mut self.output)
            .map_err(|error| MusicStreamError::EncodeError(error.to_string()))?;

        let payload = Bytes::copy_from_slice(&self.output[..encoded_len]);
        Ok(OpusFrame {
            generation: frame.generation,
            payload,
            samples_per_channel: frame.samples_per_channel,
            duration_ms: frame.duration_ms(),
            marker: frame.track_position_samples == 0,
            track_position_samples: frame.track_position_samples,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pcm(samples: &mut [f32], samples_per_channel: u32) -> PcmFrame<'_> {
        PcmFrame {
            generation: 1,
            samples_per_channel,
            sample_rate: 48_000,
            channels: 2,
            track_position_samples: 0,
            samples,
        }
    }

    #[test]
    fn libopus_encoder_encodes_float_stereo_frame() {
        let mut encoder = LibOpusEncoder::new(LibOpusEncoderConfig::default()).expect("encoder");
        let mut samples = vec![0.0; 960 * 2];
        let encoded = encoder.encode(&pcm(&mut samples, 960)).expect("encode");

        assert_eq!(encoded.generation, 1);
        assert_eq!(encoded.samples_per_channel, 960);
        assert_eq!(encoded.duration_ms, 20);
        assert!(!encoded.payload.is_empty());
        assert!(encoded.payload.len() <= 1_500);
        assert!(encoded.marker);
    }

    #[test]
    fn libopus_encoder_rejects_format_mismatch() {
        let mut encoder = LibOpusEncoder::new(LibOpusEncoderConfig::default()).expect("encoder");
        let mut samples = vec![0.0; 960 * 2];
        let mut frame = pcm(&mut samples, 960);
        frame.sample_rate = 44_100;

        let error = encoder.encode(&frame).expect_err("format mismatch");
        assert_eq!(error.code(), crate::error::ErrorCode::EncodeError);
    }
}
