//! Per-stream asynchronous media runtime.
//!
//! A stream owns one persistent output. Tracks are replaceable Opus producers;
//! decode/resample/encode always runs on blocking CPU workers and can never delay output pacing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};

mod external_pull;
mod opus_queue;
mod playout_clock;
mod producer;
mod sender;

use external_pull::ExternalPullHandle;
pub use external_pull::{ExternalFrameAck, ExternalFrameOutcome, ExternalOpusFrame};
use producer::{ProducerHandle, ProducerRole, ProducerSpec};
use sender::SenderHandle;

use crate::audio::opus::{
    LibOpusEncoderConfig, OPUS_CHANNELS as CHANNELS, OPUS_FRAME_SAMPLES as FRAME_SAMPLES,
    OPUS_MAX_BITRATE_BPS, OPUS_MAX_PACKET_BYTES, OPUS_MIN_BITRATE_BPS, OPUS_MUSIC_BITRATE_BPS,
    OPUS_SAMPLE_RATE_HZ as SAMPLE_RATE,
};
use crate::error::{MusicStreamError, Result};
use crate::event::{SourceRole, StreamEvent};
use crate::model::{
    GainLevel, MediaBufferConfig, PlayState, StreamStatus, TrackSource, VolumeLevel,
};
use crate::session::{ActorOutput, StreamActor, StreamCommand, TaskAction, WorkerEvent};
use crate::source::{
    FileSourceResolver, LiveByteBudget, SharedSourceArtifactCache, SharedSourceDownloadRegistry,
    SourceArtifactCache, SourceDownloadRegistry, SourceResolverConfig, SourceRuntimeResources,
    flush_temp_cleanup,
};
use crate::transport::{RtcpReceiverReportSnapshot, RtpTransportConfig};

const ATTEMPT_START_TIMEOUT: Duration = Duration::from_secs(45);
const MAX_ATTEMPT_START_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const RTCP_INTERVAL: Duration = Duration::from_secs(5);
const MAX_STREAMS: usize = 1_024;
const MAX_STREAM_ID_BYTES: usize = 512;
const MAX_CONCURRENT_HTTP_DOWNLOADS: usize = 8;
const MAX_CONCURRENT_LIVE_STREAMS: usize = 64;
const MAX_LIVE_BUFFERED_BYTES: usize = 64 * 1024 * 1024;
const MAX_TEMPFILE_BYTES: u64 = 1024 * 1024 * 1024;
const TEMPFILE_QUOTA_BYTES: u64 = 1024 * 1024;
const MIN_BLOCKING_PRODUCERS: usize = 64;
const MAX_BLOCKING_PRODUCERS: usize = 256;
const SYSTEM_CPU_HEADROOM: usize = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamOutputConfig {
    Rtp(RtpTransportConfig),
    ExternalPull,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeResourceLimits {
    pub max_streams: usize,
    pub max_cpu_workers: usize,
    pub max_blocking_producers: usize,
    pub max_blocking_preloads: usize,
    pub max_concurrent_http_downloads: usize,
    pub max_concurrent_live_streams: usize,
    pub max_live_buffered_bytes: usize,
    pub max_tempfile_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeTimingSnapshot {
    pub samples: u64,
    pub total_us: u64,
    pub max_us: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
}

const TIMING_SUB_BUCKETS: usize = 4;
const TIMING_BUCKETS: usize = u64::BITS as usize * TIMING_SUB_BUCKETS;

#[derive(Debug)]
struct RuntimeTimingCounter {
    samples: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
    buckets: [AtomicU64; TIMING_BUCKETS],
}

impl Default for RuntimeTimingCounter {
    fn default() -> Self {
        Self {
            samples: AtomicU64::new(0),
            total_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl RuntimeTimingCounter {
    fn record(&self, elapsed: Duration) {
        let elapsed_us = elapsed.as_micros().try_into().unwrap_or(u64::MAX);
        self.samples.fetch_add(1, Ordering::Relaxed);
        let _ = self
            .total_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(elapsed_us))
            });
        self.max_us.fetch_max(elapsed_us, Ordering::Relaxed);
        let bucket = if elapsed_us == 0 {
            0
        } else {
            let exponent = usize::try_from(elapsed_us.ilog2()).unwrap_or(u64::BITS as usize - 1);
            let base = 1_u64 << exponent;
            let sub_bucket = usize::try_from(
                (u128::from(elapsed_us - base) * TIMING_SUB_BUCKETS as u128) / u128::from(base),
            )
            .unwrap_or(TIMING_SUB_BUCKETS - 1)
            .min(TIMING_SUB_BUCKETS - 1);
            exponent * TIMING_SUB_BUCKETS + sub_bucket
        };
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> RuntimeTimingSnapshot {
        let buckets = self
            .buckets
            .each_ref()
            .map(|bucket| bucket.load(Ordering::Relaxed));
        let max_us = self.max_us.load(Ordering::Relaxed);
        RuntimeTimingSnapshot {
            samples: self.samples.load(Ordering::Relaxed),
            total_us: self.total_us.load(Ordering::Relaxed),
            max_us,
            p50_us: timing_percentile(&buckets, 50).min(max_us),
            p95_us: timing_percentile(&buckets, 95).min(max_us),
            p99_us: timing_percentile(&buckets, 99).min(max_us),
        }
    }
}

fn timing_percentile(buckets: &[u64; TIMING_BUCKETS], percentile: u64) -> u64 {
    let samples = buckets.iter().copied().fold(0_u64, u64::saturating_add);
    if samples == 0 {
        return 0;
    }
    let target = (u128::from(samples) * u128::from(percentile)).div_ceil(100);
    let mut seen = 0_u128;
    for (index, samples) in buckets.iter().copied().enumerate() {
        seen += u128::from(samples);
        if seen >= target {
            let exponent = index / TIMING_SUB_BUCKETS;
            let sub_bucket = index % TIMING_SUB_BUCKETS;
            let base = 1_u128 << exponent;
            let upper =
                base + (base * (sub_bucket + 1) as u128).div_ceil(TIMING_SUB_BUCKETS as u128) - 1;
            return upper.try_into().unwrap_or(u64::MAX);
        }
    }
    u64::MAX
}

#[derive(Debug, Default)]
pub(super) struct RuntimePerformanceCounters {
    blocking_current_admission_wait: RuntimeTimingCounter,
    blocking_next_admission_wait: RuntimeTimingCounter,
    blocking_start_wait: RuntimeTimingCounter,
    cpu_current_wait: RuntimeTimingCounter,
    cpu_next_wait: RuntimeTimingCounter,
    cpu_current_hold: RuntimeTimingCounter,
    cpu_next_hold: RuntimeTimingCounter,
    source_wait: RuntimeTimingCounter,
    output_wait: RuntimeTimingCounter,
}

impl RuntimePerformanceCounters {
    pub(super) fn record_blocking_admission_wait(&self, current: bool, elapsed: Duration) {
        if current {
            self.blocking_current_admission_wait.record(elapsed);
        } else {
            self.blocking_next_admission_wait.record(elapsed);
        }
    }

    pub(super) fn record_blocking_start_wait(&self, elapsed: Duration) {
        self.blocking_start_wait.record(elapsed);
    }

    pub(super) fn record_cpu_wait(&self, current: bool, elapsed: Duration) {
        if current {
            self.cpu_current_wait.record(elapsed);
        } else {
            self.cpu_next_wait.record(elapsed);
        }
    }

    pub(super) fn record_cpu_hold(&self, current: bool, elapsed: Duration) {
        if current {
            self.cpu_current_hold.record(elapsed);
        } else {
            self.cpu_next_hold.record(elapsed);
        }
    }

    pub(super) fn record_source_wait(&self, elapsed: Duration) {
        self.source_wait.record(elapsed);
    }

    pub(super) fn record_output_wait(&self, elapsed: Duration) {
        self.output_wait.record(elapsed);
    }
}

impl Default for RuntimeResourceLimits {
    fn default() -> Self {
        let available_parallelism =
            std::thread::available_parallelism().map_or(2, |value| value.get());
        let max_cpu_workers = default_max_cpu_workers(available_parallelism);
        let (max_blocking_producers, max_blocking_preloads) =
            Self::blocking_defaults(max_cpu_workers);
        Self {
            max_streams: MAX_STREAMS,
            max_cpu_workers,
            max_blocking_producers,
            max_blocking_preloads,
            max_concurrent_http_downloads: MAX_CONCURRENT_HTTP_DOWNLOADS,
            max_concurrent_live_streams: MAX_CONCURRENT_LIVE_STREAMS,
            max_live_buffered_bytes: MAX_LIVE_BUFFERED_BYTES,
            max_tempfile_bytes: MAX_TEMPFILE_BYTES,
        }
    }
}

fn default_max_cpu_workers(available_parallelism: usize) -> usize {
    available_parallelism
        .saturating_sub(SYSTEM_CPU_HEADROOM)
        .clamp(1, MAX_BLOCKING_PRODUCERS)
}

impl RuntimeResourceLimits {
    fn blocking_defaults(max_cpu_workers: usize) -> (usize, usize) {
        let producers = max_cpu_workers
            .saturating_mul(4)
            .clamp(MIN_BLOCKING_PRODUCERS, MAX_BLOCKING_PRODUCERS);
        (producers, (producers / 4).max(1))
    }

    /// Updates the CPU limit and recomputes blocking defaults that depend on it.
    pub fn set_max_cpu_workers_with_blocking_defaults(&mut self, max_cpu_workers: usize) {
        self.max_cpu_workers = max_cpu_workers;
        (self.max_blocking_producers, self.max_blocking_preloads) =
            Self::blocking_defaults(max_cpu_workers);
    }

    /// Updates the blocking producer limit and resets its preload sub-limit to one quarter.
    pub fn set_max_blocking_producers_with_preload_default(
        &mut self,
        max_blocking_producers: usize,
    ) {
        self.max_blocking_producers = max_blocking_producers;
        self.max_blocking_preloads = (max_blocking_producers / 4).max(1);
    }
}

#[derive(Debug)]
pub struct RuntimeResources {
    limits: RuntimeResourceLimits,
    cpu_parallelism: usize,
    streams: Arc<Semaphore>,
    source_cache: SharedSourceArtifactCache,
    source_downloads: SharedSourceDownloadRegistry,
    http_downloads: Arc<Semaphore>,
    http_preloads: Arc<Semaphore>,
    live_streams: Arc<Semaphore>,
    live_byte_budget: LiveByteBudget,
    tempfile_budget: Arc<Semaphore>,
    tempfile_preloads: Arc<Semaphore>,
    performance: Arc<RuntimePerformanceCounters>,
    cpu_scheduler: Arc<producer::CpuScheduler>,
    blocking_producers: Arc<Semaphore>,
    blocking_preloads: Arc<Semaphore>,
}

/// On-demand resource accounting for diagnosing admission stalls. Reading this snapshot does not
/// touch sender/producer hot loops and does not reveal source URLs or headers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeResourceSnapshot {
    pub streams_available: usize,
    pub http_downloads_available: usize,
    pub http_preloads_available: usize,
    pub live_streams_available: usize,
    pub live_bytes_available: usize,
    pub tempfile_quota_units_available: usize,
    pub tempfile_preload_units_available: usize,
    pub blocking_producers_available: usize,
    pub blocking_preloads_available: usize,
    pub cpu_active: usize,
    pub cpu_parallelism: usize,
    pub cpu_workers_maximum: usize,
    pub cpu_system_headroom: usize,
    pub cpu_current_waiters: usize,
    pub cpu_next_waiters: usize,
    pub blocking_current_admission_wait: RuntimeTimingSnapshot,
    pub blocking_next_admission_wait: RuntimeTimingSnapshot,
    pub blocking_start_wait: RuntimeTimingSnapshot,
    pub cpu_current_wait: RuntimeTimingSnapshot,
    pub cpu_next_wait: RuntimeTimingSnapshot,
    pub cpu_current_hold: RuntimeTimingSnapshot,
    pub cpu_next_hold: RuntimeTimingSnapshot,
    pub source_wait: RuntimeTimingSnapshot,
    pub output_wait: RuntimeTimingSnapshot,
    pub artifact_cache_entries: usize,
    pub artifact_cache_retained_quota_bytes: u64,
    pub download_registry_entries: usize,
    pub live_download_flights: usize,
}

impl Default for RuntimeResources {
    fn default() -> Self {
        Self::new(RuntimeResourceLimits::default())
            .expect("default runtime resource limits are valid")
    }
}

impl RuntimeResources {
    pub fn new(limits: RuntimeResourceLimits) -> Result<Self> {
        if limits.max_streams == 0
            || limits.max_streams > Semaphore::MAX_PERMITS
            || limits.max_cpu_workers == 0
            || limits.max_blocking_producers == 0
            || limits.max_cpu_workers > limits.max_blocking_producers
            || limits.max_blocking_preloads == 0
            || limits.max_blocking_preloads >= limits.max_blocking_producers
            || limits.max_blocking_producers > Semaphore::MAX_PERMITS
            || limits.max_blocking_preloads > Semaphore::MAX_PERMITS
            || limits.max_concurrent_http_downloads < 2
            || limits.max_concurrent_http_downloads > Semaphore::MAX_PERMITS
            || limits.max_concurrent_live_streams == 0
            || limits.max_concurrent_live_streams > Semaphore::MAX_PERMITS
            || limits.max_live_buffered_bytes == 0
            || limits.max_live_buffered_bytes > u32::MAX as usize
            || limits.max_live_buffered_bytes > Semaphore::MAX_PERMITS
            || limits.max_tempfile_bytes < TEMPFILE_QUOTA_BYTES * 2
            || limits.max_tempfile_bytes / TEMPFILE_QUOTA_BYTES > Semaphore::MAX_PERMITS as u64
        {
            return Err(MusicStreamError::InvalidConfig(
                "stream, CPU, blocking producer, HTTP/live connection, live byte, and tempfile limits are invalid".to_owned(),
            ));
        }
        let cpu_parallelism = std::thread::available_parallelism().map_or(1, |value| value.get());
        let tempfile_permits = usize::try_from(limits.max_tempfile_bytes / TEMPFILE_QUOTA_BYTES)
            .map_err(|_| {
                MusicStreamError::InvalidConfig("tempfile byte limit is too large".to_owned())
            })?;
        let performance = Arc::new(RuntimePerformanceCounters::default());
        Ok(Self {
            cpu_parallelism,
            streams: Arc::new(Semaphore::new(limits.max_streams)),
            http_downloads: Arc::new(Semaphore::new(limits.max_concurrent_http_downloads)),
            http_preloads: Arc::new(Semaphore::new(limits.max_concurrent_http_downloads - 1)),
            live_streams: Arc::new(Semaphore::new(limits.max_concurrent_live_streams)),
            live_byte_budget: LiveByteBudget::new(limits.max_live_buffered_bytes)?,
            tempfile_budget: Arc::new(Semaphore::new(tempfile_permits)),
            tempfile_preloads: Arc::new(Semaphore::new((tempfile_permits / 4).max(1))),
            cpu_scheduler: Arc::new(producer::CpuScheduler::with_maximum_and_counters(
                limits.max_cpu_workers,
                Arc::clone(&performance),
            )),
            performance,
            blocking_producers: Arc::new(Semaphore::new(limits.max_blocking_producers)),
            blocking_preloads: Arc::new(Semaphore::new(limits.max_blocking_preloads)),
            source_cache: Arc::new(std::sync::Mutex::new(SourceArtifactCache::new(
                limits.max_tempfile_bytes / 2,
            ))),
            source_downloads: Arc::new(SourceDownloadRegistry::default()),
            limits,
        })
    }

    #[must_use]
    pub fn limits(&self) -> &RuntimeResourceLimits {
        &self.limits
    }

    pub fn take_source_cache(&self) -> Result<SourceArtifactCache> {
        Ok(self
            .source_cache
            .lock()
            .map_err(|_| MusicStreamError::Internal("source cache poisoned".to_owned()))?
            .take())
    }

    pub async fn flush_source_cleanup(&self) -> Result<()> {
        flush_temp_cleanup().await
    }

    pub fn snapshot(&self) -> Result<RuntimeResourceSnapshot> {
        let (artifact_cache_entries, artifact_cache_retained_quota_bytes) = self
            .source_cache
            .lock()
            .map_err(|_| MusicStreamError::Internal("source cache poisoned".to_owned()))?
            .diagnostics();
        let (download_registry_entries, live_download_flights) =
            self.source_downloads.diagnostics()?;
        let (cpu_active, cpu_current_waiters, cpu_next_waiters) = self.cpu_scheduler.diagnostics();
        Ok(RuntimeResourceSnapshot {
            streams_available: self.streams.available_permits(),
            http_downloads_available: self.http_downloads.available_permits(),
            http_preloads_available: self.http_preloads.available_permits(),
            live_streams_available: self.live_streams.available_permits(),
            live_bytes_available: self.live_byte_budget.available_bytes(),
            tempfile_quota_units_available: self.tempfile_budget.available_permits(),
            tempfile_preload_units_available: self.tempfile_preloads.available_permits(),
            blocking_producers_available: self.blocking_producers.available_permits(),
            blocking_preloads_available: self.blocking_preloads.available_permits(),
            cpu_active,
            cpu_parallelism: self.cpu_parallelism,
            cpu_workers_maximum: self.limits.max_cpu_workers,
            cpu_system_headroom: self
                .cpu_parallelism
                .saturating_sub(self.limits.max_cpu_workers),
            cpu_current_waiters,
            cpu_next_waiters,
            blocking_current_admission_wait: self
                .performance
                .blocking_current_admission_wait
                .snapshot(),
            blocking_next_admission_wait: self.performance.blocking_next_admission_wait.snapshot(),
            blocking_start_wait: self.performance.blocking_start_wait.snapshot(),
            cpu_current_wait: self.performance.cpu_current_wait.snapshot(),
            cpu_next_wait: self.performance.cpu_next_wait.snapshot(),
            cpu_current_hold: self.performance.cpu_current_hold.snapshot(),
            cpu_next_hold: self.performance.cpu_next_hold.snapshot(),
            source_wait: self.performance.source_wait.snapshot(),
            output_wait: self.performance.output_wait.snapshot(),
            artifact_cache_entries,
            artifact_cache_retained_quota_bytes,
            download_registry_entries,
            live_download_flights,
        })
    }
}

#[derive(Clone)]
pub struct StreamRuntimeConfig {
    pub output: StreamOutputConfig,
    /// Target bitrate for the shared Opus producer, independent of output kind.
    pub opus_bitrate_bps: u32,
    pub source: SourceResolverConfig,
    pub resources: Arc<RuntimeResources>,
    pub buffer: MediaBufferConfig,
    pub rtcp_interval: Duration,
    /// Maximum active wall-clock time for one current/next attempt to reach its ready fact.
    pub attempt_start_timeout: Duration,
    pub on_event: Option<Arc<dyn Fn(StreamEvent) + Send + Sync>>,
}

impl std::fmt::Debug for StreamRuntimeConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamRuntimeConfig")
            .field("output", &self.output)
            .field("opus_bitrate_bps", &self.opus_bitrate_bps)
            .field("source", &self.source)
            .field("buffer", &self.buffer)
            .field("rtcp_interval", &self.rtcp_interval)
            .field("attempt_start_timeout", &self.attempt_start_timeout)
            .finish_non_exhaustive()
    }
}

impl StreamRuntimeConfig {
    #[must_use]
    pub fn new(transport: RtpTransportConfig, source: SourceResolverConfig) -> Self {
        Self {
            output: StreamOutputConfig::Rtp(transport),
            opus_bitrate_bps: OPUS_MUSIC_BITRATE_BPS,
            source,
            resources: Arc::new(RuntimeResources::default()),
            buffer: MediaBufferConfig::default(),
            rtcp_interval: RTCP_INTERVAL,
            attempt_start_timeout: ATTEMPT_START_TIMEOUT,
            on_event: None,
        }
    }

    #[must_use]
    pub fn new_external_pull(source: SourceResolverConfig) -> Self {
        Self {
            output: StreamOutputConfig::ExternalPull,
            opus_bitrate_bps: OPUS_MUSIC_BITRATE_BPS,
            source,
            resources: Arc::new(RuntimeResources::default()),
            buffer: MediaBufferConfig::default(),
            rtcp_interval: RTCP_INTERVAL,
            attempt_start_timeout: ATTEMPT_START_TIMEOUT,
            on_event: None,
        }
    }

    pub fn validate(&self) -> Result<()> {
        match &self.output {
            StreamOutputConfig::Rtp(transport) => transport.validate()?,
            StreamOutputConfig::ExternalPull => {}
        }
        if !(OPUS_MIN_BITRATE_BPS..=OPUS_MAX_BITRATE_BPS).contains(&self.opus_bitrate_bps) {
            return Err(MusicStreamError::InvalidConfig(
                "Opus bitrate must be between 500 and 512000 bps".to_owned(),
            ));
        }
        self.source.validate()?;
        self.buffer.validate()?;
        if self.source.live_http.max_buffered_bytes > self.resources.limits.max_live_buffered_bytes
        {
            return Err(MusicStreamError::InvalidConfig(
                "per-stream live buffer must fit the runtime-wide streaming byte budget".to_owned(),
            ));
        }
        let source_tempfile_units = self.source.http.max_bytes.div_ceil(TEMPFILE_QUOTA_BYTES);
        let runtime_tempfile_units =
            self.resources.limits.max_tempfile_bytes / TEMPFILE_QUOTA_BYTES;
        if source_tempfile_units > runtime_tempfile_units / 4 {
            return Err(MusicStreamError::InvalidConfig(
                "per-source HTTP max bytes must not exceed one quarter of the runtime tempfile budget"
                    .to_owned(),
            ));
        }
        if self.rtcp_interval.is_zero() {
            return Err(MusicStreamError::InvalidConfig(
                "RTCP interval must be greater than zero".to_owned(),
            ));
        }
        if self.attempt_start_timeout.is_zero()
            || self.attempt_start_timeout > MAX_ATTEMPT_START_TIMEOUT
        {
            return Err(MusicStreamError::InvalidConfig(
                "attempt startup timeout must be between 1 millisecond and 15 minutes".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamRuntimeProgress {
    pub generation: u64,
    pub start_position_ms: u64,
    pub media_sent_ms: u64,
    pub packets_sent: u64,
    pub bytes_sent: u64,
    pub dropped_frames: u64,
    pub dropped_media_ms: u64,
    pub latency_recoveries: u64,
    pub underruns: u64,
    pub buffered_ms: u64,
    pub max_lateness_ms: u64,
    pub sequence: u16,
    pub rtp_timestamp: u32,
    pub latest_receiver_report: Option<RtcpReceiverReportSnapshot>,
}

impl StreamRuntimeProgress {
    #[must_use]
    pub fn stream_position_ms(self) -> u64 {
        self.start_position_ms.saturating_add(self.media_sent_ms)
    }
}

#[derive(Clone, Debug)]
pub struct StreamRuntimeSnapshot {
    pub status: StreamStatus,
    pub progress: StreamRuntimeProgress,
}

#[derive(Clone)]
pub struct StreamRuntime {
    inner: Arc<StreamRuntimeInner>,
}

impl std::fmt::Debug for StreamRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamRuntime")
            .finish_non_exhaustive()
    }
}

struct StreamRuntimeInner {
    stream_permit: Mutex<Option<OwnedSemaphorePermit>>,
    actor: Mutex<StreamActor>,
    orchestration: Mutex<()>,
    output: OutputHandle,
    current: Mutex<Option<ProducerHandle>>,
    next: Mutex<Option<ProducerHandle>>,
    config: StreamRuntimeConfig,
    worker_events: mpsc::Sender<WorkerEvent>,
    startup_deadlines: std::sync::Mutex<HashMap<StartupDeadlineKey, tokio::task::AbortHandle>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct StartupDeadlineKey {
    source_role: SourceRole,
    generation: u64,
    watchdog_epoch: u64,
}

impl Drop for StreamRuntimeInner {
    fn drop(&mut self) {
        if let Ok(deadlines) = self.startup_deadlines.get_mut() {
            for (_, task) in deadlines.drain() {
                task.abort();
            }
        }
    }
}

#[derive(Clone, Debug)]
enum OutputHandle {
    Rtp(SenderHandle),
    ExternalPull(ExternalPullHandle),
}

impl OutputHandle {
    fn progress(&self) -> StreamRuntimeProgress {
        match self {
            Self::Rtp(sender) => sender.progress(),
            Self::ExternalPull(output) => output.progress(),
        }
    }

    async fn activate(
        &self,
        generation: u64,
        start_position_ms: u64,
        paused: bool,
        receiver: opus_queue::OpusQueueReceiver,
    ) -> Result<()> {
        match self {
            Self::Rtp(sender) => {
                sender
                    .activate(generation, start_position_ms, paused, receiver)
                    .await
            }
            Self::ExternalPull(output) => {
                output
                    .activate(generation, start_position_ms, paused, receiver)
                    .await
            }
        }
    }

    async fn deactivate(&self, generation: u64) -> Result<()> {
        match self {
            Self::Rtp(sender) => sender.deactivate(generation).await,
            Self::ExternalPull(output) => output.deactivate(generation).await,
        }
    }

    async fn pause(&self, generation: u64) -> Result<()> {
        match self {
            Self::Rtp(sender) => sender.pause(generation).await,
            Self::ExternalPull(output) => output.pause(generation).await,
        }
    }

    async fn resume(&self, generation: u64) -> Result<()> {
        match self {
            Self::Rtp(sender) => sender.resume(generation).await,
            Self::ExternalPull(output) => output.resume(generation).await,
        }
    }

    async fn shutdown(&self) -> Result<()> {
        match self {
            Self::Rtp(sender) => sender.shutdown().await,
            Self::ExternalPull(output) => output.shutdown().await,
        }
    }
}

struct ProducerRequest {
    role: ProducerRole,
    generation: u64,
    track: TrackSource,
    start_position_ms: u64,
    volume: VolumeLevel,
    gain: GainLevel,
    initial_paused: bool,
}

impl StreamRuntime {
    pub fn validate_stream_id(stream_id: &str) -> Result<()> {
        if stream_id.trim().is_empty() || stream_id.len() > MAX_STREAM_ID_BYTES {
            return Err(MusicStreamError::InvalidConfig(
                "stream id must contain 1 to 512 bytes".to_owned(),
            ));
        }
        Ok(())
    }

    pub async fn start(
        stream_id: String,
        current: TrackSource,
        config: StreamRuntimeConfig,
        volume: VolumeLevel,
        gain: GainLevel,
    ) -> Result<Self> {
        let current = current.with_normalized_capabilities();
        Self::validate_stream_id(&stream_id)?;
        config.validate()?;
        current.validate()?;
        let stream_permit = Arc::clone(&config.resources.streams)
            .try_acquire_owned()
            .map_err(|_| {
                MusicStreamError::Busy(format!(
                    "stream limit {} is exhausted",
                    config.resources.limits.max_streams
                ))
            })?;
        let (worker_tx, worker_rx) = mpsc::channel(64);
        let output = match &config.output {
            StreamOutputConfig::Rtp(transport) => OutputHandle::Rtp(
                SenderHandle::spawn(
                    transport.clone(),
                    config.buffer.prebuffer_ms,
                    config.buffer.max_playout_lateness_ms,
                    config.rtcp_interval,
                    worker_tx.clone(),
                )
                .await?,
            ),
            StreamOutputConfig::ExternalPull => {
                OutputHandle::ExternalPull(ExternalPullHandle::spawn(
                    config.buffer.prebuffer_ms,
                    config.buffer.max_playout_lateness_ms,
                    worker_tx.clone(),
                ))
            }
        };
        let inner = Arc::new(StreamRuntimeInner {
            stream_permit: Mutex::new(Some(stream_permit)),
            actor: Mutex::new(StreamActor::new(stream_id, Some(current))),
            orchestration: Mutex::new(()),
            output,
            current: Mutex::new(None),
            next: Mutex::new(None),
            config,
            worker_events: worker_tx,
            startup_deadlines: std::sync::Mutex::new(HashMap::new()),
        });
        spawn_worker_event_loop(Arc::downgrade(&inner), worker_rx);
        let runtime = Self { inner };
        if volume != VolumeLevel::default() {
            runtime.command(StreamCommand::SetVolume { volume }).await?;
        }
        if gain != GainLevel::default() {
            runtime.command(StreamCommand::SetGain { gain }).await?;
        }
        runtime.command(StreamCommand::Play).await?;
        Ok(runtime)
    }

    pub async fn command(&self, mut command: StreamCommand) -> Result<StreamRuntimeSnapshot> {
        normalize_command_sources(&mut command);
        validate_command_sources(&command)?;
        let _guard = self.inner.orchestration.lock().await;
        let (planned, output) = {
            let actor = self.inner.actor.lock().await;
            let mut planned = actor.clone();
            let output = planned.handle_command(command)?;
            (planned, output)
        };
        if let Err(error) = self.inner.execute_output_actions(&output).await {
            self.inner.fail_runtime(&error).await;
            return Err(error);
        }
        *self.inner.actor.lock().await = planned;
        Ok(self.inner.publish_output(output))
    }

    pub async fn snapshot(&self) -> StreamRuntimeSnapshot {
        let mut status = self.inner.actor.lock().await.status();
        let progress = self.inner.output.progress();
        if progress.generation == status.generation {
            status.time_played_ms = progress.stream_position_ms();
        }
        StreamRuntimeSnapshot { status, progress }
    }

    pub async fn shutdown(&self) -> Result<StreamRuntimeSnapshot> {
        self.command(StreamCommand::Stop).await
    }

    pub async fn pull_external_frame(
        &self,
        previous: Option<ExternalFrameAck>,
    ) -> Result<Option<ExternalOpusFrame>> {
        match &self.inner.output {
            OutputHandle::ExternalPull(output) => output.pull(previous).await,
            OutputHandle::Rtp(_) => Err(MusicStreamError::Unsupported(
                "RTP streams do not expose external Opus frames".to_owned(),
            )),
        }
    }

    pub async fn finish_external_frame(&self, ack: ExternalFrameAck) -> Result<()> {
        match &self.inner.output {
            OutputHandle::ExternalPull(output) => output.finish(ack).await,
            OutputHandle::Rtp(_) => Err(MusicStreamError::Unsupported(
                "RTP streams do not expose external Opus frames".to_owned(),
            )),
        }
    }

    pub async fn cancel_external_pull(&self) -> Result<()> {
        match &self.inner.output {
            OutputHandle::ExternalPull(output) => output.cancel_pull().await,
            OutputHandle::Rtp(_) => Err(MusicStreamError::Unsupported(
                "RTP streams do not expose external Opus frames".to_owned(),
            )),
        }
    }
}

fn normalize_command_sources(command: &mut StreamCommand) {
    match command {
        StreamCommand::RefreshCurrentSource { current } => {
            *current = current.clone().with_normalized_capabilities();
        }
        StreamCommand::ReconcilePlan { current, next, .. } => {
            *current = current
                .take()
                .map(TrackSource::with_normalized_capabilities);
            *next = next.take().map(TrackSource::with_normalized_capabilities);
        }
        StreamCommand::Play
        | StreamCommand::Pause
        | StreamCommand::Stop
        | StreamCommand::Seek { .. }
        | StreamCommand::SetVolume { .. }
        | StreamCommand::SetGain { .. } => {}
    }
}

fn validate_command_sources(command: &StreamCommand) -> Result<()> {
    match command {
        StreamCommand::RefreshCurrentSource { current } => current.validate(),
        StreamCommand::ReconcilePlan {
            version,
            current,
            next,
        } => {
            if *version == 0 {
                return Err(MusicStreamError::InvalidConfig(
                    "desired playback plan version must be positive".to_owned(),
                ));
            }
            if let Some(current) = current {
                current.validate()?;
            }
            if let Some(next) = next {
                validate_next_source(next)?;
            }
            Ok(())
        }
        StreamCommand::Play
        | StreamCommand::Pause
        | StreamCommand::Stop
        | StreamCommand::Seek { .. }
        | StreamCommand::SetVolume { .. }
        | StreamCommand::SetGain { .. } => Ok(()),
    }
}

fn validate_next_source(next: &TrackSource) -> Result<()> {
    next.validate()?;
    if next.is_live() || next.is_hls() {
        return Err(MusicStreamError::Unsupported(
            "live and HLS sources cannot be preloaded as next without a timeshift model".to_owned(),
        ));
    }
    Ok(())
}

fn spawn_worker_event_loop(
    inner: Weak<StreamRuntimeInner>,
    mut receiver: mpsc::Receiver<WorkerEvent>,
) {
    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            let Some(inner) = inner.upgrade() else {
                return;
            };
            let _guard = inner.orchestration.lock().await;
            inner.observe_worker_event_deadline(&event);
            if let WorkerEvent::OutputFailed { code, message } = event {
                inner.fail_output(code, message).await;
                continue;
            }
            let (planned, output) = {
                let actor = inner.actor.lock().await;
                let mut planned = actor.clone();
                let output = planned.handle_worker_event(event);
                (planned, output)
            };
            if let Err(error) = inner.execute_output_actions(&output).await {
                inner.fail_runtime(&error).await;
                tracing::warn!(error = %error, "worker event orchestration failed");
                continue;
            }
            *inner.actor.lock().await = planned;
            inner.publish_output(output);
        }
    });
}

impl StreamRuntimeInner {
    async fn execute_output_actions(&self, output: &ActorOutput) -> Result<()> {
        let start_position_ms = output.status.time_played_ms;
        let paused = output.status.play_state == PlayState::Paused;
        for action in output.actions.iter().cloned() {
            self.execute_action(
                action,
                output.status.volume,
                output.status.gain,
                start_position_ms,
                paused,
            )
            .await?;
        }
        Ok(())
    }

    fn publish_output(&self, output: ActorOutput) -> StreamRuntimeSnapshot {
        if let Some(callback) = &self.config.on_event {
            for event in output.events {
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(event)))
                    .is_err()
                {
                    metrics::counter!("music_stream.runtime.event_callback_panics").increment(1);
                    tracing::error!("stream event callback panicked");
                }
            }
        }
        let mut status = output.status;
        let progress = self.output.progress();
        if progress.generation == status.generation {
            status.time_played_ms = progress.stream_position_ms();
        }
        StreamRuntimeSnapshot { status, progress }
    }

    async fn fail_runtime(&self, error: &MusicStreamError) {
        self.shutdown_resources().await;
        let output = self.actor.lock().await.handle_runtime_failure(error);
        self.publish_output(output);
    }

    async fn fail_output(&self, code: crate::error::ErrorCode, message: String) {
        self.shutdown_resources().await;
        let output = self.actor.lock().await.handle_output_failure(code, message);
        self.publish_output(output);
    }

    async fn shutdown_resources(&self) {
        self.cancel_all_startup_deadlines();
        let current = self.current.lock().await.take();
        let next = self.next.lock().await.take();
        let (current_result, next_result, sender_result) = tokio::join!(
            stop_producer(current),
            stop_producer(next),
            self.output.shutdown(),
        );
        self.stream_permit.lock().await.take();
        for cleanup_error in [current_result, next_result, sender_result]
            .into_iter()
            .filter_map(std::result::Result::err)
        {
            tracing::warn!(error = %cleanup_error, "runtime failure cleanup failed");
        }
    }

    async fn execute_action(
        &self,
        action: TaskAction,
        volume: VolumeLevel,
        gain: GainLevel,
        start_position_ms: u64,
        paused: bool,
    ) -> Result<()> {
        match action {
            TaskAction::StartCurrent {
                generation,
                watchdog_epoch,
                track,
            } => {
                self.cancel_startup_deadlines(SourceRole::Current, None);
                self.cancel_startup_deadlines(SourceRole::Next, Some(generation));
                if let Some(mut producer) = take_generation(&self.next, generation).await {
                    producer.promote_to_current();
                    let receiver = producer.take_receiver()?;
                    self.output
                        .activate(generation, start_position_ms, paused, receiver)
                        .await?;
                    replace_producer(&self.current, producer).await?;
                } else {
                    let mut producer = self
                        .spawn_producer(ProducerRequest {
                            role: ProducerRole::Current,
                            generation,
                            track,
                            start_position_ms,
                            volume,
                            gain,
                            initial_paused: paused,
                        })
                        .await?;
                    let receiver = producer.take_receiver()?;
                    self.output
                        .activate(generation, start_position_ms, paused, receiver)
                        .await?;
                    replace_producer(&self.current, producer).await?;
                }
                self.arm_startup_deadline(SourceRole::Current, generation, watchdog_epoch);
            }
            TaskAction::PrepareNext {
                generation,
                watchdog_epoch,
                track,
            } => {
                self.cancel_startup_deadlines(SourceRole::Next, None);
                let producer = self
                    .spawn_producer(ProducerRequest {
                        role: ProducerRole::Next,
                        generation,
                        track,
                        start_position_ms: 0,
                        volume,
                        gain,
                        initial_paused: paused,
                    })
                    .await?;
                replace_producer(&self.next, producer).await?;
                self.arm_startup_deadline(SourceRole::Next, generation, watchdog_epoch);
            }
            TaskAction::CancelCurrent { generation } => {
                self.cancel_startup_deadlines(SourceRole::Current, Some(generation));
                let sender_result = self.output.deactivate(generation).await;
                let producer_result = cancel_generation(&self.current, generation).await;
                sender_result?;
                producer_result?;
            }
            TaskAction::CancelNext { generation } => {
                self.cancel_startup_deadlines(SourceRole::Next, Some(generation));
                cancel_generation(&self.next, generation).await?;
            }
            TaskAction::PauseCurrent { generation } => {
                self.cancel_startup_deadlines(SourceRole::Current, Some(generation));
                self.cancel_startup_deadlines(SourceRole::Next, None);
                pause_generation(&self.current, generation).await;
                pause_slot(&self.next).await;
                self.output.pause(generation).await?;
            }
            TaskAction::PauseNext { generation } => {
                self.cancel_startup_deadlines(SourceRole::Next, Some(generation));
                pause_generation(&self.next, generation).await;
            }
            TaskAction::ResumeCurrent { generation } => {
                resume_generation(&self.current, generation).await;
                resume_slot(&self.next).await;
                self.output.resume(generation).await?;
            }
            TaskAction::ResumeNext { generation } => {
                resume_generation(&self.next, generation).await;
            }
            TaskAction::ArmStartupDeadline {
                source_role,
                generation,
                watchdog_epoch,
            } => self.arm_startup_deadline(source_role, generation, watchdog_epoch),
            TaskAction::SetCurrentVolume { generation, volume } => {
                set_volume(&self.current, generation, volume).await;
            }
            TaskAction::SetNextVolume { generation, volume } => {
                set_volume(&self.next, generation, volume).await;
            }
            TaskAction::SetCurrentGain { generation, gain } => {
                set_gain(&self.current, generation, gain).await;
            }
            TaskAction::SetNextGain { generation, gain } => {
                set_gain(&self.next, generation, gain).await;
            }
            TaskAction::StopSender => {
                self.cancel_all_startup_deadlines();
                let current = self.current.lock().await.take();
                let next = self.next.lock().await.take();
                let (current_result, next_result) =
                    tokio::join!(stop_producer(current), stop_producer(next));
                let sender_result = self.output.shutdown().await;
                self.stream_permit.lock().await.take();
                current_result?;
                next_result?;
                sender_result?;
            }
        }
        Ok(())
    }

    fn arm_startup_deadline(&self, source_role: SourceRole, generation: u64, watchdog_epoch: u64) {
        let key = StartupDeadlineKey {
            source_role,
            generation,
            watchdog_epoch,
        };
        let timeout = self.config.attempt_start_timeout;
        let events = self.worker_events.clone();
        let task = tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            let _ = events
                .send(WorkerEvent::StartupTimedOut {
                    source_role,
                    generation,
                    watchdog_epoch,
                })
                .await;
        });
        let mut deadlines = self
            .startup_deadlines
            .lock()
            .expect("startup deadline registry poisoned");
        if let Some(old) = deadlines.insert(key, task.abort_handle()) {
            old.abort();
        }
    }

    fn observe_worker_event_deadline(&self, event: &WorkerEvent) {
        match event {
            WorkerEvent::CurrentPrebufferReady { generation }
            | WorkerEvent::CurrentEnded { generation }
            | WorkerEvent::CurrentFailed { generation, .. } => {
                self.cancel_startup_deadlines(SourceRole::Current, Some(*generation));
            }
            WorkerEvent::NextReady { generation } | WorkerEvent::NextFailed { generation, .. } => {
                self.cancel_startup_deadlines(SourceRole::Next, Some(*generation));
            }
            WorkerEvent::StartupTimedOut {
                source_role,
                generation,
                watchdog_epoch,
            } => {
                self.startup_deadlines
                    .lock()
                    .expect("startup deadline registry poisoned")
                    .remove(&StartupDeadlineKey {
                        source_role: *source_role,
                        generation: *generation,
                        watchdog_epoch: *watchdog_epoch,
                    });
            }
            WorkerEvent::CurrentSourceClassified { .. }
            | WorkerEvent::CurrentNetworkQualityChanged { .. }
            | WorkerEvent::OutputFailed { .. } => {}
        }
    }

    fn cancel_startup_deadlines(&self, source_role: SourceRole, generation: Option<u64>) {
        self.startup_deadlines
            .lock()
            .expect("startup deadline registry poisoned")
            .retain(|key, task| {
                let remove = key.source_role == source_role
                    && generation.is_none_or(|generation| key.generation == generation);
                if remove {
                    task.abort();
                }
                !remove
            });
    }

    fn cancel_all_startup_deadlines(&self) {
        let mut deadlines = self
            .startup_deadlines
            .lock()
            .expect("startup deadline registry poisoned");
        for (_, task) in deadlines.drain() {
            task.abort();
        }
    }

    async fn spawn_producer(&self, request: ProducerRequest) -> Result<ProducerHandle> {
        let resolver = FileSourceResolver::new(
            self.config.source.clone(),
            SourceRuntimeResources {
                cache: Arc::clone(&self.config.resources.source_cache),
                http_downloads: Arc::clone(&self.config.resources.http_downloads),
                http_preloads: Arc::clone(&self.config.resources.http_preloads),
                tempfile_budget: Arc::clone(&self.config.resources.tempfile_budget),
                tempfile_preloads: Arc::clone(&self.config.resources.tempfile_preloads),
                downloads: Arc::clone(&self.config.resources.source_downloads),
            },
            matches!(request.role, ProducerRole::Next),
        );
        let max_packet_bytes = match &self.config.output {
            StreamOutputConfig::Rtp(transport) => transport.mtu.saturating_sub(12),
            StreamOutputConfig::ExternalPull => OPUS_MAX_PACKET_BYTES,
        };
        let opus = LibOpusEncoderConfig {
            max_packet_bytes,
            bitrate_bps: self.config.opus_bitrate_bps,
            ..LibOpusEncoderConfig::default()
        };
        Ok(producer::spawn(ProducerSpec {
            role: request.role,
            generation: request.generation,
            track: request.track,
            start_position_ms: request.start_position_ms,
            buffer: self.config.buffer.clone(),
            opus,
            volume: request.volume,
            gain: request.gain,
            initial_paused: request.initial_paused,
            resolver,
            source: self.config.source.clone(),
            live_byte_budget: self.config.resources.live_byte_budget.clone(),
            live_streams: Arc::clone(&self.config.resources.live_streams),
            performance: Arc::clone(&self.config.resources.performance),
            cpu_scheduler: Arc::clone(&self.config.resources.cpu_scheduler),
            blocking_producers: Arc::clone(&self.config.resources.blocking_producers),
            blocking_preloads: Arc::clone(&self.config.resources.blocking_preloads),
            events: self.worker_events.clone(),
        }))
    }
}

async fn replace_producer(
    slot: &Mutex<Option<ProducerHandle>>,
    producer: ProducerHandle,
) -> Result<()> {
    let old = slot.lock().await.replace(producer);
    if let Some(old) = old {
        old.stop().await?;
    }
    Ok(())
}

async fn stop_producer(producer: Option<ProducerHandle>) -> Result<()> {
    if let Some(producer) = producer {
        producer.stop().await?;
    }
    Ok(())
}

async fn take_generation(
    slot: &Mutex<Option<ProducerHandle>>,
    generation: u64,
) -> Option<ProducerHandle> {
    let mut slot = slot.lock().await;
    if slot
        .as_ref()
        .is_some_and(|producer| producer.generation() == generation)
    {
        slot.take()
    } else {
        None
    }
}

async fn cancel_generation(slot: &Mutex<Option<ProducerHandle>>, generation: u64) -> Result<()> {
    if let Some(producer) = take_generation(slot, generation).await {
        producer.stop().await?;
    }
    Ok(())
}

async fn pause_generation(slot: &Mutex<Option<ProducerHandle>>, generation: u64) {
    if let Some(producer) = slot.lock().await.as_ref()
        && producer.generation() == generation
    {
        producer.pause();
    }
}

async fn resume_generation(slot: &Mutex<Option<ProducerHandle>>, generation: u64) {
    if let Some(producer) = slot.lock().await.as_ref()
        && producer.generation() == generation
    {
        producer.resume();
    }
}

async fn pause_slot(slot: &Mutex<Option<ProducerHandle>>) {
    if let Some(producer) = slot.lock().await.as_ref() {
        producer.pause();
    }
}

async fn resume_slot(slot: &Mutex<Option<ProducerHandle>>) {
    if let Some(producer) = slot.lock().await.as_ref() {
        producer.resume();
    }
}

async fn set_volume(slot: &Mutex<Option<ProducerHandle>>, generation: u64, volume: VolumeLevel) {
    if let Some(producer) = slot.lock().await.as_ref()
        && producer.generation() == generation
    {
        producer.set_volume(volume);
    }
}

async fn set_gain(slot: &Mutex<Option<ProducerHandle>>, generation: u64, gain: GainLevel) {
    if let Some(producer) = slot.lock().await.as_ref()
        && producer.generation() == generation
    {
        producer.set_gain(gain);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_cpu_budget_reserves_system_headroom() {
        assert_eq!(default_max_cpu_workers(1), 1);
        assert_eq!(default_max_cpu_workers(2), 1);
        assert_eq!(default_max_cpu_workers(4), 3);
        assert_eq!(default_max_cpu_workers(12), 11);
        assert_eq!(default_max_cpu_workers(512), 256);
    }

    #[test]
    fn runtime_resources_reject_zero_download_concurrency() {
        let error = RuntimeResources::new(RuntimeResourceLimits {
            max_concurrent_http_downloads: 0,
            ..RuntimeResourceLimits::default()
        })
        .expect_err("zero download slots must fail");

        assert_eq!(error.code(), crate::error::ErrorCode::InvalidConfig);
    }

    #[test]
    fn runtime_timing_counter_exposes_bounded_percentiles() {
        let counter = RuntimeTimingCounter::default();
        for elapsed_us in 1..=100 {
            counter.record(Duration::from_micros(elapsed_us));
        }

        assert_eq!(
            counter.snapshot(),
            RuntimeTimingSnapshot {
                samples: 100,
                total_us: 5_050,
                max_us: 100,
                p50_us: 55,
                p95_us: 95,
                p99_us: 100,
            }
        );
    }

    #[test]
    fn stream_admission_is_hard_bounded_and_reusable() {
        let resources = RuntimeResources::new(RuntimeResourceLimits {
            max_streams: 1,
            ..RuntimeResourceLimits::default()
        })
        .expect("resources");
        let first = Arc::clone(&resources.streams)
            .try_acquire_owned()
            .expect("first stream");
        assert!(Arc::clone(&resources.streams).try_acquire_owned().is_err());
        drop(first);
        assert!(Arc::clone(&resources.streams).try_acquire_owned().is_ok());
    }

    #[test]
    fn resource_snapshot_reports_effective_cpu_headroom() {
        let resources = RuntimeResources::new(RuntimeResourceLimits {
            max_cpu_workers: 1,
            ..RuntimeResourceLimits::default()
        })
        .expect("resources");
        let snapshot = resources.snapshot().expect("resource snapshot");

        assert_eq!(snapshot.cpu_workers_maximum, 1);
        assert!(snapshot.cpu_parallelism >= 1);
        assert_eq!(
            snapshot.cpu_system_headroom,
            snapshot.cpu_parallelism.saturating_sub(1)
        );
    }

    #[test]
    fn default_runtime_uses_twenty_millisecond_opus_frames() {
        assert_eq!(FRAME_SAMPLES * 1_000 / SAMPLE_RATE, 20);
        assert_eq!(
            StreamRuntimeConfig::new_external_pull(SourceResolverConfig::default())
                .opus_bitrate_bps,
            320_000
        );
    }

    #[test]
    fn opus_bitrate_is_validated_independently_of_output_kind() {
        let mut rtp = StreamRuntimeConfig::new(
            RtpTransportConfig::new("127.0.0.1", 5_000, 42),
            SourceResolverConfig::default(),
        );
        rtp.opus_bitrate_bps = 499;
        assert_eq!(
            rtp.validate().expect_err("low RTP bitrate").code(),
            crate::error::ErrorCode::InvalidConfig
        );

        let mut external = StreamRuntimeConfig::new_external_pull(SourceResolverConfig::default());
        external.opus_bitrate_bps = 512_001;
        assert_eq!(
            external
                .validate()
                .expect_err("high external bitrate")
                .code(),
            crate::error::ErrorCode::InvalidConfig
        );
    }
}
