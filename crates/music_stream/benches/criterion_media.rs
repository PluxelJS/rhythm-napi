use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

mod support;

fn criterion_media(c: &mut Criterion) {
    benchmark_decode(c);
    benchmark_resample(c);
    benchmark_opus(c);
    benchmark_full_pipeline(c);
}

fn benchmark_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("media/decode_open_and_drain");
    for fixture in support::FIXTURES {
        let file = support::materialize(fixture);
        let decoded_frames = support::decode_all(file.path())
            .expect("fixture must decode")
            .0;
        group.throughput(Throughput::Elements(decoded_frames as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(fixture.name),
            &file,
            |b, file| {
                b.iter(|| black_box(support::decode_all(file.path()).expect("decode benchmark")));
            },
        );
    }
    group.finish();
}

fn benchmark_resample(c: &mut Criterion) {
    let output_frames = support::OUTPUT_SAMPLE_RATE as usize * support::SYNTHETIC_SECONDS;
    let mut group = c.benchmark_group("media/resample");
    group.throughput(Throughput::Elements(output_frames as u64));
    group.bench_function("mono_44100_to_stereo_48000_5s", |b| {
        b.iter_batched(
            support::synthetic_resampler,
            |decoder| black_box(support::drain_decoder(decoder).expect("resample benchmark")),
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn benchmark_opus(c: &mut Criterion) {
    let frames = support::OUTPUT_SAMPLE_RATE as usize * support::SYNTHETIC_SECONDS
        / support::OPUS_FRAME_SAMPLES as usize;
    let mut group = c.benchmark_group("media/opus");
    group.throughput(Throughput::Elements(frames as u64));
    group.bench_function("stereo_48000_5s", |b| {
        b.iter_batched(
            support::opus_workload,
            |(encoder, samples)| {
                black_box(support::encode_opus_5s(encoder, samples).expect("encode benchmark"))
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn benchmark_full_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("media/open_decode_resample_opus");
    for fixture in support::FIXTURES {
        let file = support::materialize(fixture);
        let decoded_frames = support::decode_all(file.path())
            .expect("fixture must decode")
            .0;
        group.throughput(Throughput::Elements(decoded_frames as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(fixture.name),
            &file,
            |b, file| {
                b.iter(|| {
                    black_box(support::run_media_pipeline(file.path()).expect("pipeline benchmark"))
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, criterion_media);
criterion_main!(benches);
