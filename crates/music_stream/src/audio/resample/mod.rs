//! Sample-rate conversion and reusable resampler buffers.

#[cfg(feature = "resampler-rubato")]
mod rubato_backend {
    use std::collections::VecDeque;

    use rubato::audioadapter_buffers::direct::InterleavedSlice;
    use rubato::{
        Async, FixedAsync, Indexing, Resampler, SincInterpolationParameters, SincInterpolationType,
        WindowFunction, calculate_cutoff,
    };

    use crate::audio::AudioFormat;
    use crate::audio::decode::{DecodePoll, DecodedChunk, DecoderBackend};
    use crate::audio::dsp::to_stereo_interleaved;
    use crate::error::{MusicStreamError, Result};

    const DEFAULT_CHUNK_FRAMES: usize = 1024;

    #[derive(Clone, Debug)]
    pub struct RubatoResamplerConfig {
        pub target: AudioFormat,
        pub chunk_frames: usize,
        pub max_resample_ratio_relative: f64,
        pub sinc_len: usize,
        pub oversampling_factor: usize,
        pub interpolation: SincInterpolationType,
        pub window: WindowFunction,
    }

    impl RubatoResamplerConfig {
        #[must_use]
        pub fn new(target: AudioFormat) -> Self {
            Self {
                target,
                chunk_frames: DEFAULT_CHUNK_FRAMES,
                max_resample_ratio_relative: 1.1,
                sinc_len: 128,
                oversampling_factor: 256,
                interpolation: SincInterpolationType::Quadratic,
                window: WindowFunction::Blackman2,
            }
        }

        pub fn validate(&self) -> Result<()> {
            if self.target.sample_rate == 0 {
                return Err(MusicStreamError::InvalidConfig(
                    "target sample_rate must be greater than zero".to_owned(),
                ));
            }
            if self.target.channels == 0 {
                return Err(MusicStreamError::InvalidConfig(
                    "target channels must be greater than zero".to_owned(),
                ));
            }
            if self.chunk_frames == 0 {
                return Err(MusicStreamError::InvalidConfig(
                    "rubato chunk_frames must be greater than zero".to_owned(),
                ));
            }
            if self.max_resample_ratio_relative <= 1.0 {
                return Err(MusicStreamError::InvalidConfig(
                    "max_resample_ratio_relative must be greater than 1.0".to_owned(),
                ));
            }
            if self.sinc_len == 0 {
                return Err(MusicStreamError::InvalidConfig(
                    "sinc_len must be greater than zero".to_owned(),
                ));
            }
            if self.oversampling_factor == 0 {
                return Err(MusicStreamError::InvalidConfig(
                    "oversampling_factor must be greater than zero".to_owned(),
                ));
            }

            Ok(())
        }
    }

    pub struct RubatoResamplingDecoder<D> {
        inner: D,
        config: RubatoResamplerConfig,
        resampler: Option<Box<dyn Resampler<f32>>>,
        input_sample_rate: Option<u32>,
        pending_input: Vec<f32>,
        pending_output: VecDeque<DecodedChunk>,
        output_scratch: Vec<f32>,
        trim_output_frames_remaining: usize,
        input_frames_seen: usize,
        output_frames_emitted: usize,
        source_ended: bool,
    }

    impl<D> std::fmt::Debug for RubatoResamplingDecoder<D>
    where
        D: std::fmt::Debug,
    {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("RubatoResamplingDecoder")
                .field("inner", &self.inner)
                .field("config", &self.config)
                .field("input_sample_rate", &self.input_sample_rate)
                .field("pending_input_samples", &self.pending_input.len())
                .field("pending_output_chunks", &self.pending_output.len())
                .field(
                    "trim_output_frames_remaining",
                    &self.trim_output_frames_remaining,
                )
                .field("input_frames_seen", &self.input_frames_seen)
                .field("output_frames_emitted", &self.output_frames_emitted)
                .field("source_ended", &self.source_ended)
                .finish_non_exhaustive()
        }
    }

    impl<D> RubatoResamplingDecoder<D> {
        pub fn new(inner: D, config: RubatoResamplerConfig) -> Result<Self> {
            config.validate()?;
            Ok(Self {
                inner,
                config,
                resampler: None,
                input_sample_rate: None,
                pending_input: Vec::new(),
                pending_output: VecDeque::new(),
                output_scratch: Vec::new(),
                trim_output_frames_remaining: 0,
                input_frames_seen: 0,
                output_frames_emitted: 0,
                source_ended: false,
            })
        }

        #[must_use]
        pub fn target(&self) -> &AudioFormat {
            &self.config.target
        }

        #[must_use]
        pub fn inner(&self) -> &D {
            &self.inner
        }

        pub fn into_inner(self) -> D {
            self.inner
        }
    }

    impl<D> DecoderBackend for RubatoResamplingDecoder<D>
    where
        D: DecoderBackend,
    {
        fn poll_decode(&mut self) -> Result<DecodePoll> {
            if let Some(chunk) = self.pending_output.pop_front() {
                return Ok(DecodePoll::Chunk(chunk));
            }
            if self.source_ended {
                return Ok(DecodePoll::End);
            }

            loop {
                match self.inner.poll_decode()? {
                    DecodePoll::Chunk(chunk) => {
                        self.accept_chunk(chunk)?;
                        if let Some(chunk) = self.pending_output.pop_front() {
                            return Ok(DecodePoll::Chunk(chunk));
                        }
                    }
                    DecodePoll::NeedMore => return Ok(DecodePoll::NeedMore),
                    DecodePoll::End => {
                        self.source_ended = true;
                        self.finish()?;
                        return Ok(self
                            .pending_output
                            .pop_front()
                            .map_or(DecodePoll::End, DecodePoll::Chunk));
                    }
                }
            }
        }
    }

    impl<D> RubatoResamplingDecoder<D> {
        fn accept_chunk(&mut self, chunk: DecodedChunk) -> Result<()> {
            let input_sample_rate = chunk.sample_rate;
            let mut normalized = self.normalize_channels(chunk)?;
            let frames = normalized.len() / usize::from(self.config.target.channels);
            if input_sample_rate == self.config.target.sample_rate {
                self.pending_output.push_back(DecodedChunk {
                    sample_rate: self.config.target.sample_rate,
                    channels: self.config.target.channels,
                    samples_interleaved: normalized,
                });
                return Ok(());
            }

            self.ensure_resampler(input_sample_rate)?;
            self.input_frames_seen = self.input_frames_seen.saturating_add(frames);
            self.pending_input.append(&mut normalized);
            self.process_full_chunks()
        }

        fn normalize_channels(&mut self, chunk: DecodedChunk) -> Result<Vec<f32>> {
            if chunk.channels == self.config.target.channels {
                return Ok(chunk.samples_interleaved);
            }

            if self.config.target.channels != 2 {
                return Err(MusicStreamError::Unsupported(format!(
                    "channel normalization to {} channels is not supported",
                    self.config.target.channels
                )));
            }

            let mut normalized = Vec::new();
            to_stereo_interleaved(&chunk.samples_interleaved, chunk.channels, &mut normalized)?;
            Ok(normalized)
        }

        fn ensure_resampler(&mut self, input_sample_rate: u32) -> Result<()> {
            if let Some(existing) = self.input_sample_rate {
                if existing != input_sample_rate {
                    return Err(MusicStreamError::ResampleError(
                        "decoder changed sample rate mid-stream".to_owned(),
                    ));
                }
                return Ok(());
            }

            let ratio = f64::from(self.config.target.sample_rate) / f64::from(input_sample_rate);
            let params = SincInterpolationParameters {
                sinc_len: self.config.sinc_len,
                f_cutoff: calculate_cutoff(self.config.sinc_len, self.config.window),
                interpolation: self.config.interpolation,
                oversampling_factor: self.config.oversampling_factor,
                window: self.config.window,
            };
            let resampler = Async::<f32>::new_sinc(
                ratio,
                self.config.max_resample_ratio_relative,
                &params,
                self.config.chunk_frames,
                usize::from(self.config.target.channels),
                FixedAsync::Input,
            )
            .map_err(map_rubato_construction_error)?;

            self.trim_output_frames_remaining = resampler.output_delay();
            self.input_sample_rate = Some(input_sample_rate);
            self.resampler = Some(Box::new(resampler));
            Ok(())
        }

        fn process_full_chunks(&mut self) -> Result<()> {
            loop {
                let input_frames_next = self
                    .resampler
                    .as_ref()
                    .ok_or_else(|| {
                        MusicStreamError::Internal(
                            "rubato resampler was not initialized".to_owned(),
                        )
                    })?
                    .input_frames_next();
                if self.pending_input_frames() < input_frames_next {
                    return Ok(());
                }

                self.process_one(None)?;
            }
        }

        fn finish(&mut self) -> Result<()> {
            if self.resampler.is_none() {
                return Ok(());
            }

            let pending_frames = self.pending_input_frames();
            if pending_frames > 0 {
                self.process_one(Some(pending_frames))?;
            }

            let expected_output_frames = self.expected_output_frames()?;
            while self.output_frames_emitted < expected_output_frames {
                self.process_one(Some(0))?;
            }

            Ok(())
        }

        fn process_one(&mut self, partial_len: Option<usize>) -> Result<()> {
            let channels = usize::from(self.config.target.channels);
            let input_frames = self.pending_input_frames();
            let resampler = self.resampler.as_mut().ok_or_else(|| {
                MusicStreamError::Internal("rubato resampler was not initialized".to_owned())
            })?;
            let output_frames_next = resampler.output_frames_next();
            self.output_scratch.clear();
            self.output_scratch
                .resize(output_frames_next * channels, 0.0);

            let input_adapter = InterleavedSlice::new(&self.pending_input, channels, input_frames)
                .map_err(map_size_error)?;
            let mut output_adapter =
                InterleavedSlice::new_mut(&mut self.output_scratch, channels, output_frames_next)
                    .map_err(map_size_error)?;
            let indexing = partial_len.map(|partial_len| Indexing {
                input_offset: 0,
                output_offset: 0,
                partial_len: Some(partial_len),
                active_channels_mask: None,
            });
            let (input_used, output_written) = resampler
                .process_into_buffer(&input_adapter, &mut output_adapter, indexing.as_ref())
                .map_err(map_rubato_error)?;

            let actual_input_used = partial_len.map_or(input_used, |len| len.min(input_frames));
            let consumed = actual_input_used * channels;
            if consumed > 0 {
                self.pending_input.drain(0..consumed);
            }

            let valid_samples = output_written * channels;
            let output = self.output_scratch[..valid_samples].to_vec();
            self.push_output(output, output_written)
        }

        fn push_output(&mut self, samples: Vec<f32>, frames: usize) -> Result<()> {
            if frames == 0 {
                return Ok(());
            }

            let channels = usize::from(self.config.target.channels);
            if self.trim_output_frames_remaining >= frames {
                self.trim_output_frames_remaining -= frames;
                return Ok(());
            }

            let skip_frames = self.trim_output_frames_remaining;
            self.trim_output_frames_remaining = 0;
            let mut emit_frames = frames - skip_frames;

            if self.source_ended {
                let remaining = self
                    .expected_output_frames()?
                    .saturating_sub(self.output_frames_emitted);
                emit_frames = emit_frames.min(remaining);
            }

            if emit_frames == 0 {
                return Ok(());
            }

            let start = skip_frames * channels;
            let end = start + emit_frames * channels;
            self.pending_output.push_back(DecodedChunk {
                sample_rate: self.config.target.sample_rate,
                channels: self.config.target.channels,
                samples_interleaved: samples[start..end].to_vec(),
            });
            self.output_frames_emitted = self.output_frames_emitted.saturating_add(emit_frames);
            Ok(())
        }

        fn pending_input_frames(&self) -> usize {
            self.pending_input.len() / usize::from(self.config.target.channels)
        }

        fn expected_output_frames(&self) -> Result<usize> {
            let input_rate = self.input_sample_rate.ok_or_else(|| {
                MusicStreamError::Internal("input sample rate was not initialized".to_owned())
            })?;
            Ok(
                (self.input_frames_seen as f64 * f64::from(self.config.target.sample_rate)
                    / f64::from(input_rate))
                .ceil() as usize,
            )
        }
    }

    fn map_rubato_error(error: rubato::ResampleError) -> MusicStreamError {
        MusicStreamError::ResampleError(error.to_string())
    }

    fn map_rubato_construction_error(
        error: rubato::ResamplerConstructionError,
    ) -> MusicStreamError {
        MusicStreamError::ResampleError(error.to_string())
    }

    fn map_size_error(error: rubato::audioadapter_buffers::SizeError) -> MusicStreamError {
        MusicStreamError::ResampleError(error.to_string())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::audio::decode::MemoryDecoder;

        #[test]
        fn resampling_decoder_converts_44100_mono_to_48000_stereo() {
            let frames = 4_410;
            let source = DecodedChunk {
                sample_rate: 44_100,
                channels: 1,
                samples_interleaved: (0..frames)
                    .map(|index| ((index as f32) / 32.0).sin() * 0.5)
                    .collect(),
            };
            let mut decoder = RubatoResamplingDecoder::new(
                MemoryDecoder::new([source]),
                RubatoResamplerConfig::new(AudioFormat {
                    sample_rate: 48_000,
                    channels: 2,
                }),
            )
            .expect("resampling decoder");

            let mut output_frames = 0;
            loop {
                match decoder.poll_decode().expect("decode") {
                    DecodePoll::Chunk(chunk) => {
                        assert_eq!(chunk.sample_rate, 48_000);
                        assert_eq!(chunk.channels, 2);
                        assert_eq!(chunk.samples_interleaved.len() % 2, 0);
                        output_frames += chunk.samples_per_channel();
                    }
                    DecodePoll::NeedMore => panic!("memory decoder should not need more data"),
                    DecodePoll::End => break,
                }
            }

            assert_eq!(output_frames, 4_800);
        }

        #[test]
        fn resampling_decoder_passes_through_matching_sample_rate() {
            let source = DecodedChunk {
                sample_rate: 48_000,
                channels: 1,
                samples_interleaved: vec![0.25, -0.25],
            };
            let mut decoder = RubatoResamplingDecoder::new(
                MemoryDecoder::new([source]),
                RubatoResamplerConfig::new(AudioFormat {
                    sample_rate: 48_000,
                    channels: 2,
                }),
            )
            .expect("resampling decoder");

            let chunk = match decoder.poll_decode().expect("decode") {
                DecodePoll::Chunk(chunk) => chunk,
                other => panic!("unexpected poll: {other:?}"),
            };

            assert_eq!(chunk.samples_interleaved, vec![0.25, 0.25, -0.25, -0.25]);
            assert_eq!(decoder.poll_decode().expect("end"), DecodePoll::End);
        }
    }
}

#[cfg(feature = "resampler-rubato")]
pub use rubato_backend::{RubatoResamplerConfig, RubatoResamplingDecoder};
