#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use music_stream::SymphoniaFileDecoder;

mod support;

static MEASURING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;

// SAFETY: Every operation delegates to `System` with the exact original pointer/layout contract.
// The additional relaxed atomics only observe successful allocation requests and do not affect
// allocator behavior.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Delegating the caller-provided valid layout to `System` preserves its contract.
        let pointer = unsafe { System.alloc(layout) };
        record_allocation(pointer, layout.size());
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Delegating the caller-provided valid layout to `System` preserves its contract.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        record_allocation(pointer, layout.size());
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The pointer and layout came from the matching `System` allocation above.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: The pointer/layout pair came from `System`; `new_size` is forwarded unchanged.
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        record_allocation(resized, new_size);
        resized
    }
}

fn record_allocation(pointer: *mut u8, bytes: usize) {
    if !pointer.is_null() && MEASURING.load(Ordering::Relaxed) {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug)]
struct AllocationStats {
    allocations: u64,
    bytes: u64,
}

fn main() {
    let mut rows = Vec::new();

    for fixture in support::FIXTURES {
        let file = support::materialize(fixture);
        support::decode_all(file.path()).expect("decode warmup");
        rows.push((
            format!("decode_open_and_drain/{}", fixture.name),
            median_profile(|| {
                black_box(support::decode_all(file.path()).expect("decode profile"));
            }),
        ));
        rows.push((
            format!("decode_drain_only/{}", fixture.name),
            median_profile_with_setup(
                || SymphoniaFileDecoder::open(file.path()).expect("decoder setup"),
                |decoder| {
                    black_box(
                        support::drain_decoder(decoder).expect("decode-only allocation profile"),
                    );
                },
            ),
        ));

        support::run_media_pipeline(file.path()).expect("pipeline warmup");
        rows.push((
            format!("pipeline/{}", fixture.name),
            median_profile(|| {
                black_box(
                    support::run_media_pipeline(file.path()).expect("pipeline allocation profile"),
                );
            }),
        ));
    }

    rows.push((
        "resample/mono_44100_to_stereo_48000_5s".to_owned(),
        median_profile_with_setup(support::synthetic_resampler, |decoder| {
            black_box(support::drain_decoder(decoder).expect("resample allocation profile"));
        }),
    ));
    rows.push((
        "opus/stereo_48000_5s".to_owned(),
        median_profile_with_setup(support::opus_workload, |(encoder, samples)| {
            black_box(support::encode_opus_5s(encoder, samples).expect("Opus allocation profile"));
        }),
    ));

    println!("| workload | allocations/run | allocated bytes/run |");
    println!("| --- | ---: | ---: |");
    for (name, stats) in rows {
        println!("| {name} | {} | {} |", stats.allocations, stats.bytes);
    }
}

fn median_profile(mut workload: impl FnMut()) -> AllocationStats {
    let mut samples = Vec::with_capacity(7);
    for _ in 0..7 {
        samples.push(measure(&mut workload));
    }
    median(samples)
}

fn median_profile_with_setup<T>(
    mut setup: impl FnMut() -> T,
    mut workload: impl FnMut(T),
) -> AllocationStats {
    let mut samples = Vec::with_capacity(7);
    for _ in 0..7 {
        let input = setup();
        samples.push(measure(|| workload(input)));
    }
    median(samples)
}

fn measure(workload: impl FnOnce()) -> AllocationStats {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    MEASURING.store(true, Ordering::SeqCst);
    workload();
    MEASURING.store(false, Ordering::SeqCst);
    AllocationStats {
        allocations: ALLOCATIONS.load(Ordering::Relaxed),
        bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
    }
}

fn median(mut samples: Vec<AllocationStats>) -> AllocationStats {
    samples.sort_unstable_by_key(|sample| (sample.allocations, sample.bytes));
    samples[samples.len() / 2]
}
