use std::collections::VecDeque;
use std::hint::black_box;
use std::io::Write as _;
use std::path::Path;

use base64::Engine as _;
use music_stream::{
    AudioFormat, DecodePoll, DecodedChunk, DecoderBackend, LibOpusEncoder, LibOpusEncoderConfig,
    OpusEncoderBackend, PcmFrame, PipelineConfig, PlayoutPipeline, Result, RubatoResamplerConfig,
    RubatoResamplingDecoder, SymphoniaFileDecoder,
};

pub const OUTPUT_SAMPLE_RATE: u32 = 48_000;
pub const OUTPUT_CHANNELS: u16 = 2;
pub const OPUS_FRAME_SAMPLES: u32 = 960;
pub const SYNTHETIC_SECONDS: usize = 5;
const INPUT_CHUNK_FRAMES: usize = 1_024;

pub struct Fixture {
    pub name: &'static str,
    suffix: &'static str,
    base64: &'static str,
}

pub const FIXTURES: &[Fixture] = &[
    Fixture {
        name: "mp3_mono_44100",
        suffix: ".mp3",
        base64: include_str!("../../testdata/sine-mono.mp3.b64"),
    },
    Fixture {
        name: "flac_mono_44100",
        suffix: ".flac",
        base64: include_str!("../../testdata/sine-mono.flac.b64"),
    },
    Fixture {
        name: "vorbis_mono_44100",
        suffix: ".ogg",
        base64: include_str!("../../testdata/sine-mono-vorbis.ogg.b64"),
    },
    Fixture {
        name: "alac_mono_44100",
        suffix: ".m4a",
        base64: include_str!("../../testdata/sine-mono-alac.m4a.b64"),
    },
    Fixture {
        name: "aac_mono_44100",
        suffix: ".m4a",
        base64: include_str!("../../testdata/faststart-aac.m4a.b64"),
    },
];

pub fn materialize(fixture: &Fixture) -> tempfile::NamedTempFile {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(fixture.base64.split_whitespace().collect::<String>())
        .expect("valid base64 fixture");
    let mut file = tempfile::Builder::new()
        .suffix(fixture.suffix)
        .tempfile()
        .expect("fixture tempfile");
    file.write_all(&bytes).expect("write fixture");
    file.flush().expect("flush fixture");
    file
}

pub fn decode_all(path: &Path) -> Result<(usize, f32)> {
    drain_decoder(SymphoniaFileDecoder::open(path)?)
}

pub fn drain_decoder<D>(mut decoder: D) -> Result<(usize, f32)>
where
    D: DecoderBackend,
{
    let mut frames = 0_usize;
    let mut checksum = 0.0_f32;
    loop {
        match decoder.poll_decode()? {
            DecodePoll::Chunk(chunk) => {
                frames = frames.saturating_add(chunk.samples_per_channel());
                checksum += chunk
                    .samples_interleaved
                    .first()
                    .copied()
                    .unwrap_or_default();
                decoder.recycle(chunk);
            }
            DecodePoll::NeedMore => continue,
            DecodePoll::End => return Ok((frames, checksum)),
        }
    }
}

pub fn synthetic_resampler() -> RubatoResamplingDecoder<FixtureDecoder> {
    let input_frames = 44_100 * SYNTHETIC_SECONDS;
    let mut remaining = input_frames;
    let mut chunks = VecDeque::new();
    while remaining > 0 {
        let frames = remaining.min(INPUT_CHUNK_FRAMES);
        let samples_interleaved = (0..frames)
            .map(|sample| ((sample % 128) as f32 / 64.0) - 1.0)
            .collect();
        chunks.push_back(DecodedChunk {
            sample_rate: 44_100,
            channels: 1,
            samples_interleaved,
        });
        remaining -= frames;
    }
    RubatoResamplingDecoder::new(
        FixtureDecoder { chunks },
        RubatoResamplerConfig::new(output_format()),
    )
    .expect("resampler")
}

pub fn opus_workload() -> (LibOpusEncoder, Vec<f32>) {
    let encoder = LibOpusEncoder::new(LibOpusEncoderConfig::default()).expect("encoder");
    let samples = (0..OPUS_FRAME_SAMPLES as usize * 2)
        .map(|sample| ((sample % 256) as f32 / 128.0) - 1.0)
        .collect();
    (encoder, samples)
}

pub fn encode_opus_5s(mut encoder: LibOpusEncoder, mut samples: Vec<f32>) -> Result<usize> {
    let frames = OUTPUT_SAMPLE_RATE as usize * SYNTHETIC_SECONDS / OPUS_FRAME_SAMPLES as usize;
    let mut bytes = 0_usize;
    for index in 0..frames {
        let frame = PcmFrame {
            generation: 1,
            samples_per_channel: OPUS_FRAME_SAMPLES,
            sample_rate: OUTPUT_SAMPLE_RATE,
            channels: OUTPUT_CHANNELS,
            track_position_samples: (index * OPUS_FRAME_SAMPLES as usize) as u64,
            samples: &mut samples,
        };
        bytes += encoder.encode(&frame)?.payload.len();
    }
    Ok(bytes)
}

pub fn run_media_pipeline(path: &Path) -> Result<(usize, usize)> {
    let decoder = SymphoniaFileDecoder::open(path)?;
    let decoder =
        RubatoResamplingDecoder::new(decoder, RubatoResamplerConfig::new(output_format()))?;
    let encoder = LibOpusEncoder::new(LibOpusEncoderConfig::default())?;
    let mut pipeline = PlayoutPipeline::new(
        decoder,
        encoder,
        PipelineConfig {
            generation: 1,
            sample_rate: OUTPUT_SAMPLE_RATE,
            channels: OUTPUT_CHANNELS,
            frame_samples_per_channel: OPUS_FRAME_SAMPLES,
            decode_batch_ms: 100,
        },
    )?;
    let mut frames = 0_usize;
    let mut bytes = 0_usize;
    loop {
        let report = pipeline.process_turn(|frame| {
            frames += 1;
            bytes += frame.payload.len();
            black_box(frame);
            Ok(())
        })?;
        if report.source_ended {
            return Ok((frames, bytes));
        }
    }
}

const fn output_format() -> AudioFormat {
    AudioFormat {
        sample_rate: OUTPUT_SAMPLE_RATE,
        channels: OUTPUT_CHANNELS,
    }
}

#[derive(Debug)]
pub struct FixtureDecoder {
    chunks: VecDeque<DecodedChunk>,
}

impl DecoderBackend for FixtureDecoder {
    fn poll_decode(&mut self) -> Result<DecodePoll> {
        Ok(self
            .chunks
            .pop_front()
            .map_or(DecodePoll::End, DecodePoll::Chunk))
    }
}
