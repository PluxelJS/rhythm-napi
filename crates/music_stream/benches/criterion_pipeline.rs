use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use music_stream::{
    DecodePoll, DecodedChunk, DecoderBackend, LibOpusEncoder, LibOpusEncoderConfig,
    OpusEncoderBackend, OpusFrame, PcmFrame, PipelineConfig, PlayoutPipeline, Result,
};

const SAMPLE_RATE: u32 = 48_000;
const CHANNELS: u16 = 2;
const FRAME_SAMPLES_PER_CHANNEL: u32 = 960;
const FRAME_MS: u64 = 20;
const BENCH_SECONDS: u64 = 5;
const CHUNK_FRAMES: usize = 12;

fn criterion_pipeline(c: &mut Criterion) {
    c.bench_function("pipeline/fake_worker_and_drain_5s", |b| {
        b.iter(|| run_fake_pipeline().expect("fake pipeline benchmark"))
    });
    c.bench_function("pipeline/libopus_worker_and_drain_5s", |b| {
        b.iter(|| run_libopus_pipeline().expect("libopus pipeline benchmark"))
    });
}

fn run_libopus_pipeline() -> Result<(usize, usize)> {
    let total_frames = usize::try_from(BENCH_SECONDS * 1_000 / FRAME_MS).unwrap_or(usize::MAX);
    let decoder = ChunkedPcmDecoder::new(total_frames, CHUNK_FRAMES);
    let encoder = LibOpusEncoder::new(LibOpusEncoderConfig::default())?;
    let mut pipeline = PlayoutPipeline::new(decoder, encoder, pipeline_config())?;
    let mut frames = 0;
    let mut bytes = 0;
    loop {
        let report = pipeline.process_turn(|frame| {
            frames += 1;
            bytes += frame.payload.len();
            black_box(frame);
            Ok(())
        })?;
        if report.source_ended {
            break;
        }
    }
    Ok((frames, bytes))
}

fn run_fake_pipeline() -> Result<(usize, usize)> {
    let total_frames = usize::try_from(BENCH_SECONDS * 1_000 / FRAME_MS).unwrap_or(usize::MAX);
    let decoder = ChunkedPcmDecoder::new(total_frames, CHUNK_FRAMES);
    let mut pipeline = PlayoutPipeline::new(decoder, BenchEncoder, pipeline_config())?;
    let mut sent_frames = 0;
    let mut payload_bytes = 0;

    loop {
        let report = pipeline.process_turn(|frame| {
            payload_bytes += frame.payload.len();
            sent_frames += 1;
            black_box(frame);
            Ok(())
        })?;
        if report.source_ended {
            break;
        }
    }

    Ok((sent_frames, payload_bytes))
}

fn pipeline_config() -> PipelineConfig {
    PipelineConfig {
        generation: 1,
        sample_rate: SAMPLE_RATE,
        channels: CHANNELS,
        frame_samples_per_channel: FRAME_SAMPLES_PER_CHANNEL,
        decode_batch_ms: 100,
    }
}

#[derive(Debug)]
struct ChunkedPcmDecoder {
    frames_remaining: usize,
    chunk_frames: usize,
}

impl ChunkedPcmDecoder {
    fn new(total_frames: usize, chunk_frames: usize) -> Self {
        Self {
            frames_remaining: total_frames,
            chunk_frames: chunk_frames.max(1),
        }
    }
}

impl DecoderBackend for ChunkedPcmDecoder {
    fn poll_decode(&mut self) -> Result<DecodePoll> {
        if self.frames_remaining == 0 {
            return Ok(DecodePoll::End);
        }

        let frames = self.frames_remaining.min(self.chunk_frames);
        self.frames_remaining -= frames;
        Ok(DecodePoll::Chunk(decoded_chunk(frames)))
    }
}

#[derive(Debug)]
struct BenchEncoder;

impl OpusEncoderBackend for BenchEncoder {
    fn encode(&mut self, frame: &PcmFrame<'_>) -> Result<OpusFrame> {
        let mut payload = Vec::with_capacity(16);
        payload.extend_from_slice(&frame.track_position_samples.to_le_bytes());
        payload.extend_from_slice(&frame.samples_per_channel.to_le_bytes());
        payload.extend_from_slice(&frame.generation.to_le_bytes()[..4]);
        Ok(OpusFrame {
            generation: frame.generation,
            payload: Bytes::from(payload),
            samples_per_channel: frame.samples_per_channel,
            duration_ms: frame.duration_ms(),
            marker: frame.track_position_samples == 0,
            track_position_samples: frame.track_position_samples,
        })
    }
}

fn decoded_chunk(frames: usize) -> DecodedChunk {
    let samples_per_frame = FRAME_SAMPLES_PER_CHANNEL as usize * usize::from(CHANNELS);
    let sample_count = frames * samples_per_frame;
    let samples_interleaved = (0..sample_count)
        .map(|sample| {
            let phase = (sample % 256) as f32 / 256.0;
            (phase * 2.0) - 1.0
        })
        .collect();
    DecodedChunk {
        sample_rate: SAMPLE_RATE,
        channels: CHANNELS,
        samples_interleaved,
    }
}

criterion_group!(benches, criterion_pipeline);
criterion_main!(benches);
