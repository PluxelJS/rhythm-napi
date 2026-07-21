//! Audio demuxing and decoding backends.

#[cfg(test)]
use std::collections::VecDeque;
use std::io::Read;
use std::path::Path;

use crate::Result;
use crate::error::MusicStreamError;

const MAX_CONSECUTIVE_DECODE_ERRORS: usize = 32;
const MAX_RECYCLED_PCM_SAMPLES: usize = 48_000 * 2;

#[derive(Clone, Debug, PartialEq)]
pub struct DecodedChunk {
    pub sample_rate: u32,
    pub channels: u16,
    pub samples_interleaved: Vec<f32>,
}

impl DecodedChunk {
    #[must_use]
    pub fn samples_per_channel(&self) -> usize {
        if self.channels == 0 {
            return 0;
        }

        self.samples_interleaved.len() / usize::from(self.channels)
    }

    #[must_use]
    pub fn duration_ms(&self) -> u64 {
        if self.sample_rate == 0 {
            return 0;
        }

        (self.samples_per_channel() as u64 * 1_000) / u64::from(self.sample_rate)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DecodePoll {
    Chunk(DecodedChunk),
    NeedMore,
    End,
}

pub trait DecoderBackend {
    fn poll_decode(&mut self) -> Result<DecodePoll>;

    /// Returns an owned PCM chunk after the caller has finished processing it.
    ///
    /// Stateful decoders may retain the allocation for the next decoded packet. Backends that do
    /// not own reusable storage can keep the default drop behavior.
    fn recycle(&mut self, _chunk: DecodedChunk) {}
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct MemoryDecoder {
    chunks: VecDeque<DecodedChunk>,
}

#[cfg(test)]
impl MemoryDecoder {
    #[must_use]
    pub fn new(chunks: impl IntoIterator<Item = DecodedChunk>) -> Self {
        Self {
            chunks: chunks.into_iter().collect(),
        }
    }
}

#[cfg(test)]
impl DecoderBackend for MemoryDecoder {
    fn poll_decode(&mut self) -> Result<DecodePoll> {
        if let Some(chunk) = self.chunks.pop_front() {
            return Ok(DecodePoll::Chunk(chunk));
        }

        Ok(DecodePoll::End)
    }
}
pub struct SymphoniaFileDecoder {
    format: SymphoniaFormatReader,
    decoder: SymphoniaAudioDecoder,
    track_id: u32,
    decode_errors: DecodeErrorBudget,
    recycled_samples: Vec<f32>,
}
impl std::fmt::Debug for SymphoniaFileDecoder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SymphoniaFileDecoder")
            .field("track_id", &self.track_id)
            .finish_non_exhaustive()
    }
}
pub struct SymphoniaStreamDecoder {
    format: SymphoniaFormatReader,
    decoder: SymphoniaAudioDecoder,
    track_id: u32,
    decode_errors: DecodeErrorBudget,
    recycled_samples: Vec<f32>,
}
impl std::fmt::Debug for SymphoniaStreamDecoder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SymphoniaStreamDecoder")
            .field("track_id", &self.track_id)
            .finish_non_exhaustive()
    }
}
impl SymphoniaFileDecoder {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        use std::fs::File;

        use symphonia::core::formats::probe::Hint;
        use symphonia::core::io::MediaSourceStream;

        let path = path.as_ref();
        let file =
            File::open(path).map_err(|error| MusicStreamError::InvalidSource(error.to_string()))?;
        let mss = MediaSourceStream::new(Box::new(file), Default::default());

        let mut hint = Hint::new();
        if let Some(extension) = path.extension().and_then(|value| value.to_str()) {
            hint.with_extension(extension);
        }

        let (format, decoder, track_id) = open_symphonia_decoder(mss, &hint)?;
        Ok(Self {
            format,
            decoder,
            track_id,
            decode_errors: DecodeErrorBudget::default(),
            recycled_samples: Vec::new(),
        })
    }

    pub fn open_at(path: impl AsRef<Path>, start_position_ms: u64) -> Result<Self> {
        let mut decoder = Self::open(path)?;
        decoder.seek_to_ms(start_position_ms)?;
        Ok(decoder)
    }

    fn seek_to_ms(&mut self, start_position_ms: u64) -> Result<()> {
        if start_position_ms == 0 {
            return Ok(());
        }

        use symphonia::core::formats::{SeekMode, SeekTo};
        use symphonia::core::units::Time;

        let time = Time::from_nanos_u64(start_position_ms.saturating_mul(1_000_000));
        self.format
            .seek(
                SeekMode::Accurate,
                SeekTo::Time {
                    time,
                    track_id: Some(self.track_id),
                },
            )
            .map_err(map_symphonia_error)?;
        self.decoder.reset();
        Ok(())
    }
}
impl DecoderBackend for SymphoniaFileDecoder {
    fn poll_decode(&mut self) -> Result<DecodePoll> {
        poll_symphonia_decode(
            &mut self.format,
            &mut self.decoder,
            self.track_id,
            &mut self.decode_errors,
            &mut self.recycled_samples,
        )
    }

    fn recycle(&mut self, chunk: DecodedChunk) {
        recycle_symphonia_chunk(&mut self.recycled_samples, chunk);
    }
}
impl SymphoniaStreamDecoder {
    pub fn open(
        reader: impl Read + Send + Sync + 'static,
        hint_extension: Option<&str>,
    ) -> Result<Self> {
        use symphonia::core::formats::probe::Hint;
        use symphonia::core::io::{MediaSourceStream, ReadOnlySource};

        let mut hint = Hint::new();
        if let Some(extension) = hint_extension {
            hint.with_extension(extension);
        }
        let mss = MediaSourceStream::new(Box::new(ReadOnlySource::new(reader)), Default::default());
        let (format, decoder, track_id) = open_symphonia_decoder(mss, &hint)?;
        Ok(Self {
            format,
            decoder,
            track_id,
            decode_errors: DecodeErrorBudget::default(),
            recycled_samples: Vec::new(),
        })
    }
}
impl DecoderBackend for SymphoniaStreamDecoder {
    fn poll_decode(&mut self) -> Result<DecodePoll> {
        poll_symphonia_decode(
            &mut self.format,
            &mut self.decoder,
            self.track_id,
            &mut self.decode_errors,
            &mut self.recycled_samples,
        )
    }

    fn recycle(&mut self, chunk: DecodedChunk) {
        recycle_symphonia_chunk(&mut self.recycled_samples, chunk);
    }
}
fn open_symphonia_decoder(
    mss: symphonia::core::io::MediaSourceStream<'static>,
    hint: &symphonia::core::formats::probe::Hint,
) -> Result<SymphoniaDecoderParts> {
    use symphonia::core::codecs::audio::AudioDecoderOptions;
    use symphonia::core::formats::{FormatOptions, TrackType};
    use symphonia::core::meta::MetadataOptions;

    let format = symphonia::default::get_probe()
        .probe(
            hint,
            mss,
            FormatOptions::default(),
            MetadataOptions::default(),
        )
        .map_err(map_symphonia_error)?;

    let track = format
        .default_track(TrackType::Audio)
        .ok_or_else(|| MusicStreamError::Unsupported("source has no audio track".to_owned()))?;
    let track_id = track.id;
    let codec_params = track
        .codec_params
        .as_ref()
        .and_then(|params| params.audio())
        .ok_or_else(|| {
            MusicStreamError::Unsupported("audio track has no codec parameters".to_owned())
        })?
        .clone();

    let decoder = symphonia::default::get_codecs()
        .make_audio_decoder(&codec_params, &AudioDecoderOptions::default())
        .map_err(map_symphonia_error)?;

    Ok((format, decoder, track_id))
}
fn poll_symphonia_decode(
    format: &mut SymphoniaFormatReader,
    decoder: &mut SymphoniaAudioDecoder,
    track_id: u32,
    decode_errors: &mut DecodeErrorBudget,
    recycled_samples: &mut Vec<f32>,
) -> Result<DecodePoll> {
    use symphonia::core::errors::Error as SymphoniaError;

    loop {
        let packet = match format.next_packet() {
            Ok(Some(packet)) => packet,
            Ok(None) => return Ok(DecodePoll::End),
            Err(SymphoniaError::ResetRequired) => {
                decoder.reset();
                continue;
            }
            Err(error) => return Err(map_symphonia_error(error)),
        };

        while !format.metadata().is_latest() {
            format.metadata().pop();
        }

        if packet.track_id != track_id {
            continue;
        }

        match decoder.decode(&packet) {
            Ok(audio_buf) => {
                decode_errors.reset();
                recycled_samples.resize(audio_buf.samples_interleaved(), 0.0);
                audio_buf.copy_to_slice_interleaved(&mut *recycled_samples);
                let spec = audio_buf.spec();
                return Ok(DecodePoll::Chunk(DecodedChunk {
                    sample_rate: spec.rate(),
                    channels: spec.channels().count() as u16,
                    samples_interleaved: std::mem::take(recycled_samples),
                }));
            }
            Err(error)
                if matches!(
                    error,
                    SymphoniaError::IoError(_) | SymphoniaError::DecodeError(_)
                ) =>
            {
                decode_errors.record(&error)?;
                continue;
            }
            Err(SymphoniaError::ResetRequired) => {
                decoder.reset();
                continue;
            }
            Err(error) => return Err(map_symphonia_error(error)),
        }
    }
}

fn recycle_symphonia_chunk(recycled_samples: &mut Vec<f32>, mut chunk: DecodedChunk) {
    chunk.samples_interleaved.clear();
    if chunk.samples_interleaved.capacity() <= MAX_RECYCLED_PCM_SAMPLES
        && chunk.samples_interleaved.capacity() > recycled_samples.capacity()
    {
        *recycled_samples = chunk.samples_interleaved;
    }
}

#[derive(Debug, Default)]
struct DecodeErrorBudget {
    consecutive: usize,
}

impl DecodeErrorBudget {
    fn record(&mut self, error: &symphonia::core::errors::Error) -> Result<()> {
        self.consecutive = self.consecutive.saturating_add(1);
        if self.consecutive > MAX_CONSECUTIVE_DECODE_ERRORS {
            return Err(MusicStreamError::DecodeError(format!(
                "decoder exceeded {MAX_CONSECUTIVE_DECODE_ERRORS} consecutive corrupt packets: {error}"
            )));
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.consecutive = 0;
    }
}
type SymphoniaFormatReader = Box<dyn symphonia::core::formats::FormatReader>;
type SymphoniaAudioDecoder = Box<dyn symphonia::core::codecs::audio::AudioDecoder>;
type SymphoniaDecoderParts = (SymphoniaFormatReader, SymphoniaAudioDecoder, u32);
fn map_symphonia_error(error: symphonia::core::errors::Error) -> MusicStreamError {
    use symphonia::core::errors::Error as SymphoniaError;

    match error {
        SymphoniaError::IoError(error) => MusicStreamError::DecodeError(error.to_string()),
        SymphoniaError::DecodeError(message)
        | SymphoniaError::Unsupported(message)
        | SymphoniaError::LimitError(message) => MusicStreamError::DecodeError(message.to_owned()),
        SymphoniaError::SeekError(error) => MusicStreamError::NotSeekable(format!("{error:?}")),
        SymphoniaError::ResetRequired => {
            MusicStreamError::DecodeError("decoder reset required".to_owned())
        }
        _ => MusicStreamError::DecodeError("unknown symphonia decode error".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consecutive_decode_error_budget_is_bounded() {
        let mut budget = DecodeErrorBudget::default();
        let error = symphonia::core::errors::Error::DecodeError("corrupt");
        for _ in 0..MAX_CONSECUTIVE_DECODE_ERRORS {
            budget.record(&error).expect("within budget");
        }
        assert_eq!(
            budget.record(&error).expect_err("budget exceeded").code(),
            crate::error::ErrorCode::DecodeError
        );
        budget.reset();
        budget.record(&error).expect("reset budget");
    }
    #[test]
    fn symphonia_file_decoder_reads_generated_wav() {
        let temp = tempfile::Builder::new()
            .suffix(".wav")
            .tempfile()
            .expect("temp wav");
        write_test_wav(temp.path(), 960).expect("write wav");

        let mut decoder = SymphoniaFileDecoder::open(temp.path()).expect("decoder");
        let chunk = match decoder.poll_decode().expect("decode") {
            DecodePoll::Chunk(chunk) => chunk,
            other => panic!("expected chunk, got {other:?}"),
        };

        assert_eq!(chunk.sample_rate, 48_000);
        assert_eq!(chunk.channels, 2);
        assert_eq!(chunk.samples_per_channel(), 960);
        assert_eq!(decoder.poll_decode().expect("end"), DecodePoll::End);
    }

    #[test]
    fn symphonia_file_decoder_reuses_recycled_pcm_allocation() {
        let temp = tempfile::Builder::new()
            .suffix(".wav")
            .tempfile()
            .expect("temp wav");
        write_test_wav(temp.path(), 9_600).expect("write wav");

        let mut decoder = SymphoniaFileDecoder::open(temp.path()).expect("decoder");
        let first = match decoder.poll_decode().expect("first decode") {
            DecodePoll::Chunk(chunk) => chunk,
            other => panic!("expected first chunk, got {other:?}"),
        };
        let first_allocation = first.samples_interleaved.as_ptr();
        decoder.recycle(first);

        let second = match decoder.poll_decode().expect("second decode") {
            DecodePoll::Chunk(chunk) => chunk,
            other => panic!("expected second chunk, got {other:?}"),
        };
        assert_eq!(second.samples_interleaved.as_ptr(), first_allocation);
    }
    #[test]
    fn symphonia_file_decoder_can_open_at_time_offset() {
        let temp = tempfile::Builder::new()
            .suffix(".wav")
            .tempfile()
            .expect("temp wav");
        write_test_wav(temp.path(), 9_600).expect("write wav");

        let mut decoder = SymphoniaFileDecoder::open_at(temp.path(), 100).expect("decoder");
        let chunk = match decoder.poll_decode().expect("decode") {
            DecodePoll::Chunk(chunk) => chunk,
            other => panic!("expected chunk, got {other:?}"),
        };

        assert_eq!(chunk.sample_rate, 48_000);
        assert_eq!(chunk.channels, 2);
        assert!(!chunk.samples_interleaved.is_empty());
        assert_ne!(chunk.samples_interleaved[0], 0.0);
    }
    #[tokio::test]
    async fn symphonia_stream_decoder_reads_non_seekable_streaming_bytes() {
        let temp = tempfile::Builder::new()
            .suffix(".wav")
            .tempfile()
            .expect("temp wav");
        write_test_wav(temp.path(), 960).expect("write wav");
        let bytes = std::fs::read(temp.path()).expect("read wav bytes");
        let (writer, reader) =
            crate::source::StreamingByteReader::new(bytes.len() + 1024).expect("byte pipe");
        writer
            .push(bytes::Bytes::from(bytes))
            .await
            .expect("push wav bytes");
        drop(writer);

        tokio::task::spawn_blocking(move || {
            let mut decoder = SymphoniaStreamDecoder::open(reader, Some("wav")).expect("decoder");
            let chunk = match decoder.poll_decode().expect("decode") {
                DecodePoll::Chunk(chunk) => chunk,
                other => panic!("expected chunk, got {other:?}"),
            };

            assert_eq!(chunk.sample_rate, 48_000);
            assert_eq!(chunk.channels, 2);
            assert_eq!(chunk.samples_per_channel(), 960);
            assert_eq!(decoder.poll_decode().expect("end"), DecodePoll::End);
        })
        .await
        .expect("decode task");
    }
    fn write_test_wav(path: &Path, samples_per_channel: usize) -> std::io::Result<()> {
        use std::io::Write;

        let channels = 2_u16;
        let sample_rate = 48_000_u32;
        let bits_per_sample = 16_u16;
        let bytes_per_sample = bits_per_sample / 8;
        let data_bytes =
            samples_per_channel as u32 * u32::from(channels) * u32::from(bytes_per_sample);
        let byte_rate = sample_rate * u32::from(channels) * u32::from(bytes_per_sample);
        let block_align = channels * bytes_per_sample;

        let mut file = std::fs::File::create(path)?;
        file.write_all(b"RIFF")?;
        file.write_all(&(36 + data_bytes).to_le_bytes())?;
        file.write_all(b"WAVE")?;
        file.write_all(b"fmt ")?;
        file.write_all(&16_u32.to_le_bytes())?;
        file.write_all(&1_u16.to_le_bytes())?;
        file.write_all(&channels.to_le_bytes())?;
        file.write_all(&sample_rate.to_le_bytes())?;
        file.write_all(&byte_rate.to_le_bytes())?;
        file.write_all(&block_align.to_le_bytes())?;
        file.write_all(&bits_per_sample.to_le_bytes())?;
        file.write_all(b"data")?;
        file.write_all(&data_bytes.to_le_bytes())?;
        for index in 0..samples_per_channel {
            let left = ((index as i16) % 1024).to_le_bytes();
            let right = (-(index as i16) % 1024).to_le_bytes();
            file.write_all(&left)?;
            file.write_all(&right)?;
        }

        Ok(())
    }
}
