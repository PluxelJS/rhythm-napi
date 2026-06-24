//! Audio demuxing and decoding backends.

use std::collections::VecDeque;
#[cfg(feature = "decoder-symphonia")]
use std::io::Read;
#[cfg(feature = "decoder-symphonia")]
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::Result;
use crate::audio::AudioFormat;
use crate::audio::dsp::to_stereo_interleaved;
#[cfg(feature = "decoder-symphonia")]
use crate::error::MusicStreamError;
use crate::error::MusicStreamError as Error;

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
}

#[derive(Debug)]
pub struct NormalizingDecoder<D> {
    inner: D,
    target: AudioFormat,
}

impl<D> NormalizingDecoder<D> {
    pub fn new(inner: D, target: AudioFormat) -> Result<Self> {
        if target.sample_rate == 0 {
            return Err(Error::InvalidConfig(
                "target sample_rate must be greater than zero".to_owned(),
            ));
        }
        if target.channels == 0 {
            return Err(Error::InvalidConfig(
                "target channels must be greater than zero".to_owned(),
            ));
        }

        Ok(Self { inner, target })
    }

    #[must_use]
    pub fn target(&self) -> &AudioFormat {
        &self.target
    }

    #[must_use]
    pub fn inner(&self) -> &D {
        &self.inner
    }

    pub fn into_inner(self) -> D {
        self.inner
    }
}

impl<D> DecoderBackend for NormalizingDecoder<D>
where
    D: DecoderBackend,
{
    fn poll_decode(&mut self) -> Result<DecodePoll> {
        match self.inner.poll_decode()? {
            DecodePoll::Chunk(chunk) => self.normalize_chunk(chunk).map(DecodePoll::Chunk),
            DecodePoll::NeedMore => Ok(DecodePoll::NeedMore),
            DecodePoll::End => Ok(DecodePoll::End),
        }
    }
}

impl<D> NormalizingDecoder<D> {
    fn normalize_chunk(&mut self, mut chunk: DecodedChunk) -> Result<DecodedChunk> {
        if chunk.sample_rate != self.target.sample_rate {
            return Err(Error::ResampleError(format!(
                "decoded sample rate {} does not match target {}",
                chunk.sample_rate, self.target.sample_rate
            )));
        }

        if chunk.channels == self.target.channels {
            return Ok(chunk);
        }

        if self.target.channels != 2 {
            return Err(Error::Unsupported(format!(
                "channel normalization to {} channels is not supported",
                self.target.channels
            )));
        }

        let mut normalized = Vec::new();
        to_stereo_interleaved(&chunk.samples_interleaved, chunk.channels, &mut normalized)?;
        chunk.channels = self.target.channels;
        chunk.samples_interleaved = normalized;
        Ok(chunk)
    }
}

#[derive(Clone, Debug)]
pub struct MemoryDecoder {
    chunks: VecDeque<DecodedChunk>,
}

impl MemoryDecoder {
    #[must_use]
    pub fn new(chunks: impl IntoIterator<Item = DecodedChunk>) -> Self {
        Self {
            chunks: chunks.into_iter().collect(),
        }
    }
}

impl DecoderBackend for MemoryDecoder {
    fn poll_decode(&mut self) -> Result<DecodePoll> {
        if let Some(chunk) = self.chunks.pop_front() {
            return Ok(DecodePoll::Chunk(chunk));
        }

        Ok(DecodePoll::End)
    }
}

#[derive(Debug)]
pub struct StreamingPcmDecoder {
    shared: Arc<Mutex<StreamingPcmState>>,
}

#[derive(Clone, Debug)]
pub struct StreamingPcmWriter {
    shared: Arc<Mutex<StreamingPcmState>>,
}

#[derive(Debug)]
struct StreamingPcmState {
    chunks: VecDeque<DecodedChunk>,
    max_buffered_ms: u64,
    buffered_ms: u64,
    finished: bool,
}

impl StreamingPcmDecoder {
    pub fn new(max_buffered_ms: u64) -> Result<Self> {
        if max_buffered_ms == 0 {
            return Err(Error::InvalidConfig(
                "streaming decoder max_buffered_ms must be greater than zero".to_owned(),
            ));
        }

        Ok(Self::from_shared(Arc::new(Mutex::new(StreamingPcmState {
            chunks: VecDeque::new(),
            max_buffered_ms,
            buffered_ms: 0,
            finished: false,
        }))))
    }

    fn from_shared(shared: Arc<Mutex<StreamingPcmState>>) -> Self {
        Self { shared }
    }

    #[must_use]
    pub fn writer(&self) -> StreamingPcmWriter {
        StreamingPcmWriter {
            shared: Arc::clone(&self.shared),
        }
    }

    #[must_use]
    pub fn max_buffered_ms(&self) -> u64 {
        self.snapshot().max_buffered_ms
    }

    #[must_use]
    pub fn buffered_ms(&self) -> u64 {
        self.snapshot().buffered_ms
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.snapshot().len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.snapshot().finished
    }

    #[must_use]
    pub fn can_accept(&self, chunk: &DecodedChunk) -> bool {
        self.writer().can_accept(chunk)
    }

    pub fn try_push(&self, chunk: DecodedChunk) -> Result<()> {
        self.writer().try_push(chunk)
    }

    pub fn finish(&self) -> Result<()> {
        self.writer().finish()
    }

    fn snapshot(&self) -> StreamingPcmSnapshot {
        self.shared
            .lock()
            .map(|state| state.snapshot())
            .unwrap_or_default()
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, StreamingPcmState>> {
        self.shared
            .lock()
            .map_err(|_| Error::Internal("streaming decoder lock poisoned".to_owned()))
    }
}

impl StreamingPcmWriter {
    #[must_use]
    pub fn max_buffered_ms(&self) -> u64 {
        self.snapshot().max_buffered_ms
    }

    #[must_use]
    pub fn buffered_ms(&self) -> u64 {
        self.snapshot().buffered_ms
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.snapshot().len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.snapshot().finished
    }

    #[must_use]
    pub fn can_accept(&self, chunk: &DecodedChunk) -> bool {
        let Ok(duration_ms) = StreamingPcmState::validated_duration_ms(chunk) else {
            return false;
        };
        self.shared
            .lock()
            .is_ok_and(|state| state.can_accept_duration(duration_ms))
    }

    pub fn try_push(&self, chunk: DecodedChunk) -> Result<()> {
        let mut state = self.lock_state()?;
        state.try_push(chunk)
    }

    pub fn finish(&self) -> Result<()> {
        let mut state = self.lock_state()?;
        state.finished = true;
        Ok(())
    }

    fn snapshot(&self) -> StreamingPcmSnapshot {
        self.shared
            .lock()
            .map(|state| state.snapshot())
            .unwrap_or_default()
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, StreamingPcmState>> {
        self.shared
            .lock()
            .map_err(|_| Error::Internal("streaming decoder lock poisoned".to_owned()))
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct StreamingPcmSnapshot {
    max_buffered_ms: u64,
    buffered_ms: u64,
    len: usize,
    finished: bool,
}

impl StreamingPcmState {
    fn snapshot(&self) -> StreamingPcmSnapshot {
        StreamingPcmSnapshot {
            max_buffered_ms: self.max_buffered_ms,
            buffered_ms: self.buffered_ms,
            len: self.chunks.len(),
            finished: self.finished,
        }
    }

    fn try_push(&mut self, chunk: DecodedChunk) -> Result<()> {
        if self.finished {
            return Err(Error::StreamClosed(
                "streaming decoder has already been finished".to_owned(),
            ));
        }

        let duration_ms = Self::validated_duration_ms(&chunk)?;
        if !self.can_accept_duration(duration_ms) {
            return Err(Error::Busy(
                "streaming decoder buffer high watermark reached".to_owned(),
            ));
        }

        self.buffered_ms = self.buffered_ms.saturating_add(duration_ms);
        self.chunks.push_back(chunk);
        Ok(())
    }

    fn pop_poll(&mut self) -> DecodePoll {
        if let Some(chunk) = self.chunks.pop_front() {
            self.buffered_ms = self.buffered_ms.saturating_sub(chunk.duration_ms());
            return DecodePoll::Chunk(chunk);
        }

        if self.finished {
            DecodePoll::End
        } else {
            DecodePoll::NeedMore
        }
    }

    fn can_accept_duration(&self, duration_ms: u64) -> bool {
        self.buffered_ms.saturating_add(duration_ms) <= self.max_buffered_ms
    }

    fn validated_duration_ms(chunk: &DecodedChunk) -> Result<u64> {
        validate_decoded_chunk(chunk)?;
        let duration_ms = chunk.duration_ms();
        if duration_ms == 0 {
            return Err(Error::InvalidSource(
                "streaming decoded chunk duration must be at least 1ms".to_owned(),
            ));
        }
        Ok(duration_ms)
    }
}

impl DecoderBackend for StreamingPcmDecoder {
    fn poll_decode(&mut self) -> Result<DecodePoll> {
        Ok(self.lock_state()?.pop_poll())
    }
}

fn validate_decoded_chunk(chunk: &DecodedChunk) -> Result<()> {
    if chunk.sample_rate == 0 {
        return Err(Error::InvalidSource(
            "decoded chunk sample_rate must be greater than zero".to_owned(),
        ));
    }
    if chunk.channels == 0 {
        return Err(Error::InvalidSource(
            "decoded chunk channels must be greater than zero".to_owned(),
        ));
    }
    if chunk.samples_interleaved.is_empty() {
        return Err(Error::InvalidSource(
            "decoded chunk samples must not be empty".to_owned(),
        ));
    }
    if !chunk
        .samples_interleaved
        .len()
        .is_multiple_of(usize::from(chunk.channels))
    {
        return Err(Error::InvalidSource(
            "decoded chunk sample count must be divisible by channel count".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod streaming_tests {
    use super::*;

    fn chunk(frames_per_channel: usize) -> DecodedChunk {
        DecodedChunk {
            sample_rate: 48_000,
            channels: 2,
            samples_interleaved: vec![0.0; frames_per_channel * 2],
        }
    }

    #[test]
    fn streaming_decoder_returns_need_more_until_finished() {
        let mut decoder = StreamingPcmDecoder::new(100).expect("decoder");

        assert_eq!(decoder.poll_decode().expect("poll"), DecodePoll::NeedMore);

        decoder.try_push(chunk(960)).expect("push");
        assert_eq!(decoder.buffered_ms(), 20);
        assert!(matches!(
            decoder.poll_decode().expect("chunk"),
            DecodePoll::Chunk(_)
        ));
        assert_eq!(decoder.buffered_ms(), 0);
        assert_eq!(
            decoder.poll_decode().expect("need more"),
            DecodePoll::NeedMore
        );

        decoder.finish().expect("finish");
        assert_eq!(decoder.poll_decode().expect("end"), DecodePoll::End);
    }

    #[test]
    fn streaming_decoder_enforces_millisecond_buffer_budget() {
        let decoder = StreamingPcmDecoder::new(40).expect("decoder");
        decoder.try_push(chunk(960)).expect("push 20ms");
        decoder.try_push(chunk(960)).expect("push 40ms");

        let error = decoder.try_push(chunk(960)).expect_err("full");
        assert_eq!(error.code(), crate::error::ErrorCode::Busy);
        assert_eq!(decoder.buffered_ms(), 40);
        assert_eq!(decoder.len(), 2);
    }

    #[test]
    fn streaming_decoder_rejects_push_after_finish() {
        let decoder = StreamingPcmDecoder::new(40).expect("decoder");
        decoder.finish().expect("finish");

        let error = decoder.try_push(chunk(960)).expect_err("closed");
        assert_eq!(error.code(), crate::error::ErrorCode::StreamClosed);
    }

    #[test]
    fn streaming_writer_can_feed_decoder_after_split() {
        let mut decoder = StreamingPcmDecoder::new(40).expect("decoder");
        let writer = decoder.writer();

        writer.try_push(chunk(960)).expect("writer push");
        assert_eq!(writer.buffered_ms(), 20);
        assert!(matches!(
            decoder.poll_decode().expect("decode"),
            DecodePoll::Chunk(_)
        ));
        assert_eq!(writer.buffered_ms(), 0);
        writer.finish().expect("finish");
        assert_eq!(decoder.poll_decode().expect("end"), DecodePoll::End);
    }

    #[test]
    fn streaming_decoder_rejects_invalid_chunks() {
        let decoder = StreamingPcmDecoder::new(40).expect("decoder");
        let mut invalid = chunk(960);
        invalid.channels = 0;

        let error = decoder.try_push(invalid).expect_err("invalid");
        assert_eq!(error.code(), crate::error::ErrorCode::InvalidSource);
    }
}

#[cfg(feature = "decoder-symphonia")]
pub struct SymphoniaFileDecoder {
    format: SymphoniaFormatReader,
    decoder: SymphoniaAudioDecoder,
    track_id: u32,
    scratch: Vec<f32>,
}

#[cfg(feature = "decoder-symphonia")]
impl std::fmt::Debug for SymphoniaFileDecoder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SymphoniaFileDecoder")
            .field("track_id", &self.track_id)
            .field("scratch_len", &self.scratch.len())
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "decoder-symphonia")]
pub struct SymphoniaStreamDecoder {
    format: SymphoniaFormatReader,
    decoder: SymphoniaAudioDecoder,
    track_id: u32,
    scratch: Vec<f32>,
}

#[cfg(feature = "decoder-symphonia")]
impl std::fmt::Debug for SymphoniaStreamDecoder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SymphoniaStreamDecoder")
            .field("track_id", &self.track_id)
            .field("scratch_len", &self.scratch.len())
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "decoder-symphonia")]
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
            scratch: Vec::new(),
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

#[cfg(feature = "decoder-symphonia")]
impl DecoderBackend for SymphoniaFileDecoder {
    fn poll_decode(&mut self) -> Result<DecodePoll> {
        poll_symphonia_decode(
            &mut self.format,
            &mut self.decoder,
            self.track_id,
            &mut self.scratch,
        )
    }
}

#[cfg(feature = "decoder-symphonia")]
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
            scratch: Vec::new(),
        })
    }
}

#[cfg(feature = "decoder-symphonia")]
impl DecoderBackend for SymphoniaStreamDecoder {
    fn poll_decode(&mut self) -> Result<DecodePoll> {
        poll_symphonia_decode(
            &mut self.format,
            &mut self.decoder,
            self.track_id,
            &mut self.scratch,
        )
    }
}

#[cfg(feature = "decoder-symphonia")]
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

#[cfg(feature = "decoder-symphonia")]
fn poll_symphonia_decode(
    format: &mut SymphoniaFormatReader,
    decoder: &mut SymphoniaAudioDecoder,
    track_id: u32,
    scratch: &mut Vec<f32>,
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
                scratch.resize(audio_buf.samples_interleaved(), 0.0);
                audio_buf.copy_to_slice_interleaved(&mut *scratch);
                let spec = audio_buf.spec();
                return Ok(DecodePoll::Chunk(DecodedChunk {
                    sample_rate: spec.rate(),
                    channels: spec.channels().count() as u16,
                    samples_interleaved: scratch.clone(),
                }));
            }
            Err(SymphoniaError::IoError(_)) | Err(SymphoniaError::DecodeError(_)) => {
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

#[cfg(feature = "decoder-symphonia")]
type SymphoniaFormatReader = Box<dyn symphonia::core::formats::FormatReader>;

#[cfg(feature = "decoder-symphonia")]
type SymphoniaAudioDecoder = Box<dyn symphonia::core::codecs::audio::AudioDecoder>;

#[cfg(feature = "decoder-symphonia")]
type SymphoniaDecoderParts = (SymphoniaFormatReader, SymphoniaAudioDecoder, u32);

#[cfg(feature = "decoder-symphonia")]
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
    fn memory_decoder_yields_chunks_then_end() {
        let chunk = DecodedChunk {
            sample_rate: 48_000,
            channels: 2,
            samples_interleaved: vec![0.0; 960 * 2],
        };
        let mut decoder = MemoryDecoder::new([chunk.clone()]);

        assert_eq!(
            decoder.poll_decode().expect("chunk"),
            DecodePoll::Chunk(chunk)
        );
        assert_eq!(decoder.poll_decode().expect("end"), DecodePoll::End);
        assert_eq!(decoder.poll_decode().expect("end again"), DecodePoll::End);
    }

    #[test]
    fn normalizing_decoder_expands_mono_to_target_stereo() {
        let source = DecodedChunk {
            sample_rate: 48_000,
            channels: 1,
            samples_interleaved: vec![0.25, -0.5],
        };
        let mut decoder = NormalizingDecoder::new(
            MemoryDecoder::new([source]),
            AudioFormat {
                sample_rate: 48_000,
                channels: 2,
            },
        )
        .expect("normalizer");

        let chunk = match decoder.poll_decode().expect("decode") {
            DecodePoll::Chunk(chunk) => chunk,
            other => panic!("unexpected decode poll: {other:?}"),
        };

        assert_eq!(chunk.sample_rate, 48_000);
        assert_eq!(chunk.channels, 2);
        assert_eq!(chunk.samples_interleaved, vec![0.25, 0.25, -0.5, -0.5]);
    }

    #[test]
    fn normalizing_decoder_rejects_sample_rate_mismatch_until_resampler_is_inserted() {
        let source = DecodedChunk {
            sample_rate: 44_100,
            channels: 2,
            samples_interleaved: vec![0.0; 2],
        };
        let mut decoder = NormalizingDecoder::new(
            MemoryDecoder::new([source]),
            AudioFormat {
                sample_rate: 48_000,
                channels: 2,
            },
        )
        .expect("normalizer");

        let error = decoder.poll_decode().expect_err("sample rate mismatch");
        assert_eq!(error.code(), crate::error::ErrorCode::ResampleError);
    }

    #[cfg(feature = "decoder-symphonia")]
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

    #[cfg(feature = "decoder-symphonia")]
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

    #[cfg(feature = "decoder-symphonia")]
    #[test]
    fn symphonia_stream_decoder_reads_non_seekable_streaming_bytes() {
        let temp = tempfile::Builder::new()
            .suffix(".wav")
            .tempfile()
            .expect("temp wav");
        write_test_wav(temp.path(), 960).expect("write wav");
        let bytes = std::fs::read(temp.path()).expect("read wav bytes");
        let (writer, reader) =
            crate::source::StreamingByteReader::new(bytes.len() + 1024).expect("byte pipe");
        writer
            .try_push(bytes::Bytes::from(bytes))
            .expect("push wav bytes");
        writer.finish().expect("finish byte stream");

        let mut decoder = SymphoniaStreamDecoder::open(reader, Some("wav")).expect("decoder");
        let chunk = match decoder.poll_decode().expect("decode") {
            DecodePoll::Chunk(chunk) => chunk,
            other => panic!("expected chunk, got {other:?}"),
        };

        assert_eq!(chunk.sample_rate, 48_000);
        assert_eq!(chunk.channels, 2);
        assert_eq!(chunk.samples_per_channel(), 960);
        assert_eq!(decoder.poll_decode().expect("end"), DecodePoll::End);
    }

    #[cfg(feature = "decoder-symphonia")]
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
