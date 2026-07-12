use bytes::Bytes;

use crate::error::{MusicStreamError, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpusFrame {
    pub generation: u64,
    pub payload: Bytes,
    pub samples_per_channel: u32,
    pub duration_ms: u64,
    pub marker: bool,
    pub track_position_samples: u64,
}

#[derive(Debug)]
pub struct PcmFrame<'a> {
    pub generation: u64,
    pub samples_per_channel: u32,
    pub sample_rate: u32,
    pub channels: u16,
    pub track_position_samples: u64,
    pub samples: &'a mut [f32],
}

impl PcmFrame<'_> {
    #[must_use]
    pub fn duration_ms(&self) -> u64 {
        if self.sample_rate == 0 {
            0
        } else {
            u64::from(self.samples_per_channel) * 1_000 / u64::from(self.sample_rate)
        }
    }
}

/// Converts arbitrary interleaved decoder chunks into fixed-size PCM frame views.
///
/// The backing allocation is retained by the assembler. Callers process each frame inside the
/// callback, so no per-frame PCM `Vec` allocation is needed on the hot path.
#[derive(Debug)]
pub struct FrameAssembler {
    channels: u16,
    frame_samples_per_channel: u32,
    next_position_samples: u64,
    tail: Vec<f32>,
}

impl FrameAssembler {
    pub fn new(channels: u16, frame_samples_per_channel: u32) -> Result<Self> {
        Self::new_at(channels, frame_samples_per_channel, 0)
    }

    pub fn new_at(
        channels: u16,
        frame_samples_per_channel: u32,
        start_position_samples: u64,
    ) -> Result<Self> {
        if channels == 0 || frame_samples_per_channel == 0 {
            return Err(MusicStreamError::InvalidConfig(
                "frame channels and samples must be non-zero".to_owned(),
            ));
        }
        let frame_len = frame_samples_per_channel as usize * usize::from(channels);
        Ok(Self {
            channels,
            frame_samples_per_channel,
            next_position_samples: start_position_samples,
            tail: Vec::with_capacity(frame_len * 2),
        })
    }

    pub fn process_interleaved(
        &mut self,
        generation: u64,
        sample_rate: u32,
        samples: &mut [f32],
        mut process: impl FnMut(PcmFrame<'_>) -> Result<()>,
    ) -> Result<usize> {
        let channels = usize::from(self.channels);
        if !samples.len().is_multiple_of(channels) {
            return Err(MusicStreamError::InvalidConfig(
                "interleaved sample count must be divisible by channel count".to_owned(),
            ));
        }
        let frame_len = self.frame_len();
        let mut offset = 0;
        let mut frame_count = 0;

        if !self.tail.is_empty() {
            let copied = (frame_len - self.tail.len()).min(samples.len());
            self.tail.extend_from_slice(&samples[..copied]);
            offset = copied;
            if self.tail.len() == frame_len {
                process(PcmFrame {
                    generation,
                    samples_per_channel: self.frame_samples_per_channel,
                    sample_rate,
                    channels: self.channels,
                    track_position_samples: self.next_position_samples,
                    samples: &mut self.tail,
                })?;
                self.advance_position();
                self.tail.clear();
                frame_count += 1;
            }
        }

        let direct_samples = samples.len().saturating_sub(offset);
        let direct_len = direct_samples / frame_len * frame_len;
        for frame in samples[offset..offset + direct_len].chunks_exact_mut(frame_len) {
            process(PcmFrame {
                generation,
                samples_per_channel: self.frame_samples_per_channel,
                sample_rate,
                channels: self.channels,
                track_position_samples: self.next_position_samples,
                samples: frame,
            })?;
            self.advance_position();
            frame_count += 1;
        }

        self.tail.extend_from_slice(&samples[offset + direct_len..]);
        Ok(frame_count)
    }

    pub fn flush_padded(
        &mut self,
        generation: u64,
        sample_rate: u32,
        mut process: impl FnMut(PcmFrame<'_>) -> Result<()>,
    ) -> Result<bool> {
        if self.tail.is_empty() {
            return Ok(false);
        }
        self.tail.resize(self.frame_len(), 0.0);
        process(PcmFrame {
            generation,
            samples_per_channel: self.frame_samples_per_channel,
            sample_rate,
            channels: self.channels,
            track_position_samples: self.next_position_samples,
            samples: &mut self.tail,
        })?;
        self.advance_position();
        self.tail.clear();
        Ok(true)
    }

    fn frame_len(&self) -> usize {
        self.frame_samples_per_channel as usize * usize::from(self.channels)
    }

    fn advance_position(&mut self) {
        self.next_position_samples = self
            .next_position_samples
            .saturating_add(u64::from(self.frame_samples_per_channel));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assembler_reuses_storage_and_retains_only_the_tail() {
        let mut assembler = FrameAssembler::new(2, 4).expect("assembler");
        let mut frames = Vec::new();
        let mut first = [0.0, 0.1, 1.0, 1.1, 2.0, 2.1];
        assembler
            .process_interleaved(7, 48_000, &mut first, |_| Ok(()))
            .expect("first chunk");
        assert_eq!(assembler.tail.len() / usize::from(assembler.channels), 3);
        let mut second = [3.0, 3.1, 4.0, 4.1, 5.0, 5.1];
        assembler
            .process_interleaved(7, 48_000, &mut second, |frame| {
                frames.push((frame.track_position_samples, frame.samples.to_vec()));
                Ok(())
            })
            .expect("second chunk");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].0, 0);
        assert_eq!(assembler.tail.len() / usize::from(assembler.channels), 2);
        assert_eq!(assembler.next_position_samples, 4);
    }

    #[test]
    fn assembler_rejects_misaligned_interleaved_input() {
        let mut assembler = FrameAssembler::new(2, 4).expect("assembler");
        let mut samples = [0.0, 1.0, 2.0];
        assert!(
            assembler
                .process_interleaved(1, 48_000, &mut samples, |_| Ok(()))
                .is_err()
        );
    }

    #[test]
    fn assembler_zero_pads_the_final_partial_frame() {
        let mut assembler = FrameAssembler::new(2, 4).expect("assembler");
        let mut samples = [0.5, -0.5, 0.25, -0.25];
        assembler
            .process_interleaved(1, 48_000, &mut samples, |_| Ok(()))
            .expect("partial input");
        let mut output = Vec::new();
        assert!(
            assembler
                .flush_padded(1, 48_000, |frame| {
                    output.extend_from_slice(frame.samples);
                    Ok(())
                })
                .expect("flush")
        );
        assert_eq!(output, vec![0.5, -0.5, 0.25, -0.25, 0.0, 0.0, 0.0, 0.0]);
    }
}
