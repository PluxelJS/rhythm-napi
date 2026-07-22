//! Audio demuxing and decoding backends.

#[cfg(test)]
use std::collections::VecDeque;
use std::io::Read;
use std::path::Path;

use crate::Result;
use crate::error::MusicStreamError;

const MAX_CONSECUTIVE_DECODE_ERRORS: usize = 32;

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
    decoder: PacketAudioDecoder,
    track_id: u32,
    decode_errors: DecodeErrorBudget,
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
    decoder: PacketAudioDecoder,
    track_id: u32,
    decode_errors: DecodeErrorBudget,
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
        self.decoder.reset()?;
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
        )
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
        )
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

    let decoder = if codec_params.codec == symphonia::core::codecs::audio::well_known::CODEC_ID_OPUS
    {
        PacketAudioDecoder::Opus(LibOpusPacketDecoder::new(&codec_params)?)
    } else {
        PacketAudioDecoder::Symphonia(
            symphonia::default::get_codecs()
                .make_audio_decoder(&codec_params, &AudioDecoderOptions::default())
                .map_err(map_symphonia_error)?,
        )
    };

    Ok((format, decoder, track_id))
}
fn poll_symphonia_decode(
    format: &mut SymphoniaFormatReader,
    decoder: &mut PacketAudioDecoder,
    track_id: u32,
    decode_errors: &mut DecodeErrorBudget,
) -> Result<DecodePoll> {
    use symphonia::core::errors::Error as SymphoniaError;

    loop {
        let packet = match format.next_packet() {
            Ok(Some(packet)) => packet,
            Ok(None) => return Ok(DecodePoll::End),
            Err(SymphoniaError::ResetRequired) => {
                decoder.reset()?;
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

        if let PacketAudioDecoder::Opus(decoder) = decoder {
            match decoder.decode(&packet) {
                Ok(Some(chunk)) => {
                    decode_errors.reset();
                    return Ok(DecodePoll::Chunk(chunk));
                }
                Ok(None) => continue,
                Err(error) => {
                    decode_errors.record_message(&error.to_string())?;
                    continue;
                }
            }
        }

        let PacketAudioDecoder::Symphonia(decoder) = decoder else {
            unreachable!("Opus packets are handled above");
        };
        match decoder.decode(&packet) {
            Ok(audio_buf) => {
                decode_errors.reset();
                let mut samples_interleaved = vec![0.0; audio_buf.samples_interleaved()];
                audio_buf.copy_to_slice_interleaved(&mut samples_interleaved);
                let spec = audio_buf.spec();
                return Ok(DecodePoll::Chunk(DecodedChunk {
                    sample_rate: spec.rate(),
                    channels: spec.channels().count() as u16,
                    samples_interleaved,
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

enum PacketAudioDecoder {
    Symphonia(SymphoniaAudioDecoder),
    Opus(LibOpusPacketDecoder),
}

impl std::fmt::Debug for PacketAudioDecoder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Symphonia(_) => "PacketAudioDecoder::Symphonia",
            Self::Opus(_) => "PacketAudioDecoder::Opus",
        })
    }
}

impl PacketAudioDecoder {
    fn reset(&mut self) -> Result<()> {
        match self {
            Self::Symphonia(decoder) => {
                decoder.reset();
                Ok(())
            }
            Self::Opus(decoder) => decoder.reset(),
        }
    }
}

struct LibOpusPacketDecoder {
    inner: opus::Decoder,
    channels: u16,
    output: Vec<f32>,
}

impl std::fmt::Debug for LibOpusPacketDecoder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LibOpusPacketDecoder")
            .field("channels", &self.channels)
            .finish_non_exhaustive()
    }
}

impl LibOpusPacketDecoder {
    const SAMPLE_RATE: u32 = 48_000;
    const MAX_SAMPLES_PER_CHANNEL: usize = 5_760;

    fn new(params: &symphonia::core::codecs::audio::AudioCodecParameters) -> Result<Self> {
        let channels = params
            .channels
            .as_ref()
            .map_or(0, |channels| channels.count());
        let opus_channels = match channels {
            1 => opus::Channels::Mono,
            2 => opus::Channels::Stereo,
            _ => {
                return Err(MusicStreamError::Unsupported(
                    "Ogg Opus input currently supports mono or stereo streams".to_owned(),
                ));
            }
        };
        let inner = opus::Decoder::new(Self::SAMPLE_RATE, opus_channels)
            .map_err(|error| MusicStreamError::DecodeError(error.to_string()))?;
        Ok(Self {
            inner,
            channels: channels as u16,
            output: vec![0.0; Self::MAX_SAMPLES_PER_CHANNEL * channels],
        })
    }

    fn reset(&mut self) -> Result<()> {
        self.inner
            .reset_state()
            .map_err(|error| MusicStreamError::DecodeError(error.to_string()))
    }

    fn decode(&mut self, packet: &symphonia::core::packet::Packet) -> Result<Option<DecodedChunk>> {
        if packet.data.is_empty() {
            return Err(MusicStreamError::DecodeError(
                "Ogg Opus packet is empty".to_owned(),
            ));
        }
        let samples_per_channel = self
            .inner
            .decode_float(&packet.data, &mut self.output, false)
            .map_err(|error| MusicStreamError::DecodeError(error.to_string()))?;
        let trim_start = usize::try_from(packet.trim_start.get())
            .unwrap_or(usize::MAX)
            .min(samples_per_channel);
        let trim_end = usize::try_from(packet.trim_end.get())
            .unwrap_or(usize::MAX)
            .min(samples_per_channel.saturating_sub(trim_start));
        let retained = samples_per_channel.saturating_sub(trim_start + trim_end);
        if retained == 0 {
            return Ok(None);
        }
        let channels = usize::from(self.channels);
        let start = trim_start * channels;
        let end = start + retained * channels;
        Ok(Some(DecodedChunk {
            sample_rate: Self::SAMPLE_RATE,
            channels: self.channels,
            samples_interleaved: self.output[start..end].to_vec(),
        }))
    }
}

#[derive(Debug, Default)]
struct DecodeErrorBudget {
    consecutive: usize,
}

impl DecodeErrorBudget {
    fn record(&mut self, error: &symphonia::core::errors::Error) -> Result<()> {
        self.record_message(&error.to_string())
    }

    fn record_message(&mut self, error: &str) -> Result<()> {
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
type SymphoniaDecoderParts = (SymphoniaFormatReader, PacketAudioDecoder, u32);
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

    #[tokio::test]
    async fn symphonia_stream_decoder_reads_ogg_opus_with_libopus() {
        let bytes = make_test_ogg_opus(2);
        let (writer, reader) =
            crate::source::StreamingByteReader::new(bytes.len() + 1024).expect("byte pipe");
        writer
            .push(bytes::Bytes::from(bytes))
            .await
            .expect("push Ogg Opus bytes");
        drop(writer);

        tokio::task::spawn_blocking(move || {
            let mut decoder = SymphoniaStreamDecoder::open(reader, Some("opus")).expect("decoder");
            let mut decoded_samples = 0;
            loop {
                match decoder.poll_decode().expect("decode") {
                    DecodePoll::Chunk(chunk) => {
                        assert_eq!(chunk.sample_rate, 48_000);
                        assert_eq!(chunk.channels, 2);
                        assert!(
                            chunk
                                .samples_interleaved
                                .iter()
                                .any(|sample| *sample != 0.0)
                        );
                        decoded_samples += chunk.samples_per_channel();
                    }
                    DecodePoll::End => break,
                    DecodePoll::NeedMore => continue,
                }
            }
            assert_eq!(decoded_samples, 1_920);
        })
        .await
        .expect("decode task");
    }

    #[tokio::test]
    async fn symphonia_stream_decoder_reads_adts_aac() {
        use base64::Engine as _;

        const ADTS_AAC: &str = "//FQgBGf/N4CAExhdmM2Mi4yOC4xMDIAQlCf///4BNWeCo3UDGW1nCZJa5aSO2SLkDqdrqgsZRWRqR7GlgJMJd93HV+r47GuRKmtRUiUiRaW0sDAwMiRIgY2bBkSJEiNmzYMDIkSI2bNm0SJFKbNmzaJEilmhzqBjLazhMktctJHbJFyAAAAAAAAADj/8VCAHd/8IUps/h+H4fhNWUuyynVlOrJdOf59urbPF67/9P3641xq9fb/9v588a41ev0//D+fPGutasN/rZbZ48tyKaxDj2CkzBEddkySYZPm0lTrN5TG3OAznAYGdwmYGBncJCQYGdwkJBgYGd3CQlYMDAzu4SEnMg58lf29EutTuxIugJ4Pbup85cr0pW9KE3szcoSVJZg0oSEkrwNKEhJK84MbNhISVBs3BgY2EhJUmaES6fONRRRQaiiiijcbXRRNDtg74O+Dvjn+fbq2zxq+//T9+uNBf/7fz540D/8P588aAAAAAAAAAAHA//FQgBHf/CFK2P4fgAAATdFinNoeQ7Dt1xxxrjV6//s8cOJq70KHoEB8y9et4NpTQ4Mo4DOcBnCQYk7hISDAzuEhIMDO4SEgwMDO7hISEgwMDO7hISVxvNzT445VlOY2ASnb/HyIZWn0B64B8IBhwAeHJjEDzYss2O+Dvgdh264440H+vHDiAAAAAAAAOP/xUIAB3/whQNpGCMHA";
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(ADTS_AAC)
            .expect("AAC fixture");
        let (writer, reader) =
            crate::source::StreamingByteReader::new(bytes.len() + 1024).expect("byte pipe");
        writer
            .push(bytes::Bytes::from(bytes))
            .await
            .expect("push ADTS AAC bytes");
        drop(writer);

        tokio::task::spawn_blocking(move || {
            let mut decoder = SymphoniaStreamDecoder::open(reader, Some("aac")).expect("decoder");
            let chunk = match decoder.poll_decode().expect("decode") {
                DecodePoll::Chunk(chunk) => chunk,
                other => panic!("expected AAC chunk, got {other:?}"),
            };
            assert_eq!(chunk.sample_rate, 44_100);
            assert_eq!(chunk.channels, 2);
            assert!(!chunk.samples_interleaved.is_empty());
        })
        .await
        .expect("decode task");
    }

    fn make_test_ogg_opus(channels: u8) -> Vec<u8> {
        use ogg::writing::{PacketWriteEndInfo, PacketWriter};

        let opus_channels = match channels {
            1 => opus::Channels::Mono,
            2 => opus::Channels::Stereo,
            _ => panic!("test helper supports mono or stereo"),
        };
        let mut encoder = opus::Encoder::new(48_000, opus_channels, opus::Application::Audio)
            .expect("Opus encoder");
        let mut writer = PacketWriter::new(Vec::new());
        let serial = 0x5248_5954;

        let mut head = Vec::with_capacity(19);
        head.extend_from_slice(b"OpusHead");
        head.push(1);
        head.push(channels);
        head.extend_from_slice(&0_u16.to_le_bytes());
        head.extend_from_slice(&48_000_u32.to_le_bytes());
        head.extend_from_slice(&0_i16.to_le_bytes());
        head.push(0);
        writer
            .write_packet(head, serial, PacketWriteEndInfo::EndPage, 0)
            .expect("OpusHead");

        let mut tags = Vec::with_capacity(16);
        tags.extend_from_slice(b"OpusTags");
        tags.extend_from_slice(&0_u32.to_le_bytes());
        tags.extend_from_slice(&0_u32.to_le_bytes());
        writer
            .write_packet(tags, serial, PacketWriteEndInfo::EndPage, 0)
            .expect("OpusTags");

        for packet_index in 0..2 {
            let mut pcm = Vec::with_capacity(960 * usize::from(channels));
            for sample in 0..960 {
                let value = ((sample + packet_index * 960) as f32 * std::f32::consts::TAU * 440.0
                    / 48_000.0)
                    .sin()
                    * 0.25;
                pcm.extend(std::iter::repeat_n(value, usize::from(channels)));
            }
            let mut packet = vec![0_u8; 1_500];
            let packet_len = encoder
                .encode_float(&pcm, &mut packet)
                .expect("encode Opus packet");
            packet.truncate(packet_len);
            let end = if packet_index == 1 {
                PacketWriteEndInfo::EndStream
            } else {
                PacketWriteEndInfo::EndPage
            };
            writer
                .write_packet(packet, serial, end, ((packet_index + 1) * 960) as u64)
                .expect("audio packet");
        }

        writer.into_inner()
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
