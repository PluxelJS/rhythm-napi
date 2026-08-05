use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicI16, AtomicU16, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::opus_queue::{self, OpusQueueDepth, OpusQueueReceiver, OpusQueueSender};
use super::{CHANNELS, FRAME_SAMPLES, RuntimePerformanceCounters, SAMPLE_RATE};
use crate::audio::AudioFormat;
use crate::audio::decode::{DecoderBackend, SymphoniaFileDecoder, SymphoniaStreamDecoder};
use crate::audio::dsp::VolumeConfig;
use crate::audio::opus::{LibOpusEncoder, LibOpusEncoderConfig};
use crate::audio::pipeline::{PipelineConfig, PlayoutPipeline};
use crate::audio::resample::{RubatoResamplerConfig, RubatoResamplingDecoder};
use crate::control::PauseGate;
use crate::error::{MusicStreamError, Result};
use crate::model::{GainLevel, MediaBufferConfig, TrackKind, TrackSource, VolumeLevel};
use crate::session::WorkerEvent;
use crate::source::{
    BlockingReadObserver, FileSourceResolver, HlsPlaylistKind, LiveByteBudget,
    ProgressiveUrlSource, SharedUrlControl, SourceArtifact, SourceResolverConfig,
    UrlPlaybackSource, spawn_http_hls_stream, spawn_http_live_stream, supports_progressive_url,
};

#[derive(Debug)]
pub(super) struct CpuScheduler {
    state: std::sync::Mutex<CpuSchedulerState>,
    maximum: usize,
    performance: Arc<RuntimePerformanceCounters>,
}

#[derive(Debug, Default)]
struct CpuSchedulerState {
    active: usize,
    current_waiters: VecDeque<Arc<CpuWaiter>>,
    next_waiters: VecDeque<Arc<CpuWaiter>>,
}

#[derive(Debug, Default)]
struct CpuWaiter {
    changed: Condvar,
    depth: Option<OpusQueueDepth>,
}

impl CpuWaiter {
    fn with_depth(depth: OpusQueueDepth) -> Self {
        Self {
            changed: Condvar::new(),
            depth: Some(depth),
        }
    }

    fn buffered_ms(&self) -> u64 {
        self.depth
            .as_ref()
            .map_or(u64::MAX, OpusQueueDepth::buffered_ms)
    }
}

impl CpuScheduler {
    #[cfg(test)]
    pub(super) fn with_maximum(maximum: usize) -> Self {
        Self::with_maximum_and_counters(maximum, Arc::new(RuntimePerformanceCounters::default()))
    }

    pub(super) fn with_maximum_and_counters(
        maximum: usize,
        performance: Arc<RuntimePerformanceCounters>,
    ) -> Self {
        Self {
            state: std::sync::Mutex::new(CpuSchedulerState::default()),
            maximum: maximum.max(1),
            performance,
        }
    }

    fn notify_eligible_waiter(&self, state: &CpuSchedulerState) {
        if state.active >= self.maximum {
            return;
        }
        if let Some(waiter) = most_urgent_current_waiter(state) {
            waiter.changed.notify_one();
            return;
        }
        let next_limit = self.maximum.saturating_sub(1).max(1);
        if state.active < next_limit
            && let Some(waiter) = state.next_waiters.front()
        {
            waiter.changed.notify_one();
        }
    }

    #[cfg(test)]
    fn acquire(
        self: &Arc<Self>,
        role: ProducerRole,
        cancellation: &CancellationToken,
    ) -> Option<CpuPermit> {
        let waiter = Arc::new(CpuWaiter::default());
        self.acquire_with_waiter(role, cancellation, &waiter)
    }

    fn acquire_with_waiter(
        self: &Arc<Self>,
        role: ProducerRole,
        cancellation: &CancellationToken,
        waiter: &Arc<CpuWaiter>,
    ) -> Option<CpuPermit> {
        let mut state = self.state.lock().expect("CPU scheduler lock poisoned");
        let current = matches!(role, ProducerRole::Current);
        let started = std::time::Instant::now();
        if cancellation.is_cancelled() {
            drop(state);
            record_cpu_wait(&self.performance, current, started.elapsed());
            return None;
        }

        let next_limit = self.maximum.saturating_sub(1).max(1);
        let can_acquire_immediately = if current {
            state.current_waiters.is_empty() && state.active < self.maximum
        } else {
            state.current_waiters.is_empty()
                && state.next_waiters.is_empty()
                && state.active < next_limit
        };
        if can_acquire_immediately {
            state.active += 1;
            drop(state);
            record_cpu_wait(&self.performance, current, started.elapsed());
            return Some(CpuPermit {
                scheduler: Arc::clone(self),
                current,
                acquired_at: std::time::Instant::now(),
            });
        }

        if current {
            state.current_waiters.push_back(Arc::clone(waiter));
        } else {
            state.next_waiters.push_back(Arc::clone(waiter));
        }
        loop {
            if cancellation.is_cancelled() {
                remove_cpu_waiter(&mut state, current, waiter);
                self.notify_eligible_waiter(&state);
                drop(state);
                record_cpu_wait(&self.performance, current, started.elapsed());
                return None;
            }
            let first_in_role = if current {
                most_urgent_current_waiter(&state)
                    .is_some_and(|selected| Arc::ptr_eq(selected, waiter))
            } else {
                state
                    .next_waiters
                    .front()
                    .is_some_and(|first| Arc::ptr_eq(first, waiter))
            };
            let allowed = first_in_role
                && if current {
                    state.active < self.maximum
                } else {
                    state.current_waiters.is_empty() && state.active < next_limit
                };
            if allowed {
                if current {
                    debug_assert!(remove_cpu_waiter(&mut state, current, waiter));
                } else {
                    let acquired = state.next_waiters.pop_front();
                    debug_assert!(
                        acquired
                            .as_ref()
                            .is_some_and(|first| Arc::ptr_eq(first, waiter))
                    );
                }
                state.active += 1;
                self.notify_eligible_waiter(&state);
                drop(state);
                record_cpu_wait(&self.performance, current, started.elapsed());
                return Some(CpuPermit {
                    scheduler: Arc::clone(self),
                    current,
                    acquired_at: std::time::Instant::now(),
                });
            }
            if state.active < self.maximum {
                // Queue depth can change between notifying a waiter and that waiter reacquiring
                // the scheduler lock. Redirect the wake to the newly most-urgent current instead
                // of waiting for the cancellation poll timeout or another permit release.
                self.notify_eligible_waiter(&state);
            }
            let (next, _) = waiter
                .changed
                .wait_timeout(state, Duration::from_millis(20))
                .expect("CPU scheduler lock poisoned");
            state = next;
        }
    }

    pub(super) fn diagnostics(&self) -> (usize, usize, usize) {
        let state = self.state.lock().expect("CPU scheduler lock poisoned");
        (
            state.active,
            state.current_waiters.len(),
            state.next_waiters.len(),
        )
    }
}

fn most_urgent_current_waiter(state: &CpuSchedulerState) -> Option<&Arc<CpuWaiter>> {
    // `min_by_key` retains the first minimum, so equal queue depths remain FIFO. A waiting
    // producer cannot replenish its queue while the sender continues draining it; reading the
    // live depth therefore naturally ages a healthy current toward urgency without a wall-clock
    // priority boost or a scheduler timer.
    state
        .current_waiters
        .iter()
        .min_by_key(|waiter| waiter.buffered_ms())
}

fn remove_cpu_waiter(
    state: &mut CpuSchedulerState,
    current: bool,
    waiter: &Arc<CpuWaiter>,
) -> bool {
    let waiters = if current {
        &mut state.current_waiters
    } else {
        &mut state.next_waiters
    };
    if let Some(index) = waiters
        .iter()
        .position(|queued| Arc::ptr_eq(queued, waiter))
    {
        waiters.remove(index);
        true
    } else {
        false
    }
}

#[derive(Debug)]
struct CpuPermit {
    scheduler: Arc<CpuScheduler>,
    current: bool,
    acquired_at: std::time::Instant,
}

#[derive(Debug)]
struct CpuLease {
    current_role: Arc<AtomicBool>,
    cancellation: CancellationToken,
    scheduler: Arc<CpuScheduler>,
    waiter: Arc<CpuWaiter>,
    permit: std::sync::Mutex<Option<CpuPermit>>,
    source_wait_started: std::sync::Mutex<Option<std::time::Instant>>,
}

impl CpuLease {
    fn new(
        current_role: Arc<AtomicBool>,
        cancellation: CancellationToken,
        scheduler: Arc<CpuScheduler>,
        depth: OpusQueueDepth,
    ) -> Self {
        Self {
            current_role,
            cancellation,
            scheduler,
            waiter: Arc::new(CpuWaiter::with_depth(depth)),
            permit: std::sync::Mutex::new(None),
            source_wait_started: std::sync::Mutex::new(None),
        }
    }

    fn acquire(&self) -> bool {
        let mut permit = self.permit.lock().expect("CPU lease lock poisoned");
        if permit.is_some() {
            return true;
        }
        let role = if self.current_role.load(Ordering::Acquire) {
            ProducerRole::Current
        } else {
            ProducerRole::Next
        };
        *permit = self
            .scheduler
            .acquire_with_waiter(role, &self.cancellation, &self.waiter);
        permit.is_some()
    }

    fn release(&self) {
        self.permit.lock().expect("CPU lease lock poisoned").take();
    }
}

impl BlockingReadObserver for CpuLease {
    fn before_wait(&self) {
        self.release();
        self.source_wait_started
            .lock()
            .expect("source wait lock poisoned")
            .replace(std::time::Instant::now());
    }

    fn after_wait(&self) {
        if let Some(started) = self
            .source_wait_started
            .lock()
            .expect("source wait lock poisoned")
            .take()
        {
            let elapsed = started.elapsed();
            self.scheduler.performance.record_source_wait(elapsed);
            metrics::histogram!(
                "music_stream.runtime.source_wait_us",
                "role" => role_name(if self.current_role.load(Ordering::Acquire) {
                    ProducerRole::Current
                } else {
                    ProducerRole::Next
                })
            )
            .record(elapsed.as_micros() as f64);
        }
        let _ = self.acquire();
    }
}

impl Drop for CpuPermit {
    fn drop(&mut self) {
        let elapsed = self.acquired_at.elapsed();
        self.scheduler
            .performance
            .record_cpu_hold(self.current, elapsed);
        metrics::histogram!(
            "music_stream.runtime.cpu_lease_hold_us",
            "role" => if self.current { "current" } else { "next" }
        )
        .record(elapsed.as_micros() as f64);
        let mut state = self
            .scheduler
            .state
            .lock()
            .expect("CPU scheduler lock poisoned");
        state.active = state.active.saturating_sub(1);
        self.scheduler.notify_eligible_waiter(&state);
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ProducerRole {
    Current,
    Next,
}

pub(super) struct ProducerSpec {
    pub role: ProducerRole,
    pub generation: u64,
    pub track: TrackSource,
    pub start_position_ms: u64,
    pub buffer: MediaBufferConfig,
    pub opus: LibOpusEncoderConfig,
    pub volume: VolumeLevel,
    pub gain: GainLevel,
    pub initial_paused: bool,
    pub resolver: FileSourceResolver,
    pub source: SourceResolverConfig,
    pub live_byte_budget: LiveByteBudget,
    pub live_streams: Arc<Semaphore>,
    pub performance: Arc<RuntimePerformanceCounters>,
    pub cpu_scheduler: Arc<CpuScheduler>,
    pub blocking_producers: Arc<Semaphore>,
    pub blocking_preloads: Arc<Semaphore>,
    pub events: mpsc::Sender<WorkerEvent>,
}

#[derive(Debug)]
pub(super) struct ProducerHandle {
    role: ProducerRole,
    generation: u64,
    receiver: Option<OpusQueueReceiver>,
    control: Arc<MediaControl>,
    cancellation: CancellationToken,
    task: Option<ProducerTask>,
}

#[derive(Debug)]
struct ProducerTask {
    supervisor: JoinHandle<()>,
    worker_abort: tokio::task::AbortHandle,
}

#[derive(Debug)]
struct MediaControl {
    volume: AtomicU16,
    gain: AtomicI16,
    gate: Arc<PauseGate>,
    promotion_gate: PauseGate,
    current_role: Arc<AtomicBool>,
    current_role_changed: tokio::sync::watch::Sender<bool>,
    preload_admission: Mutex<Option<OwnedSemaphorePermit>>,
    shared_url: Mutex<Option<SharedUrlControl>>,
}

impl MediaControl {
    fn new(volume: VolumeLevel, gain: GainLevel, role: ProducerRole) -> Self {
        let promotion_gate = PauseGate::default();
        if matches!(role, ProducerRole::Next) {
            promotion_gate.pause();
        }
        let is_current = matches!(role, ProducerRole::Current);
        let (current_role_changed, _) = tokio::sync::watch::channel(is_current);
        Self {
            volume: AtomicU16::new(volume.units()),
            gain: AtomicI16::new(gain.centibels()),
            gate: Arc::new(PauseGate::default()),
            promotion_gate,
            current_role: Arc::new(AtomicBool::new(is_current)),
            current_role_changed,
            preload_admission: Mutex::new(None),
            shared_url: Mutex::new(None),
        }
    }

    fn role(&self) -> ProducerRole {
        if self.current_role.load(Ordering::Acquire) {
            ProducerRole::Current
        } else {
            ProducerRole::Next
        }
    }

    fn attach_preload_admission(&self, permit: OwnedSemaphorePermit) {
        if self.current_role.load(Ordering::Acquire) {
            return;
        }
        let mut admission = self
            .preload_admission
            .lock()
            .expect("preload admission lock poisoned");
        if self.current_role.load(Ordering::Acquire) {
            return;
        }
        admission.replace(permit);
    }

    fn promote_to_current(&self) {
        if self.current_role.swap(true, Ordering::AcqRel) {
            return;
        }
        self.current_role_changed.send_replace(true);
        self.preload_admission
            .lock()
            .expect("preload admission lock poisoned")
            .take();
        if let Some(shared) = self
            .shared_url
            .lock()
            .expect("shared URL control lock poisoned")
            .as_ref()
        {
            shared.promote_to_current();
        }
        self.promotion_gate.resume();
    }

    fn volume(&self) -> VolumeLevel {
        VolumeLevel::from_units(self.volume.load(Ordering::Relaxed))
            .expect("stored volume units are validated")
    }

    fn gain(&self) -> GainLevel {
        GainLevel::from_centibels(self.gain.load(Ordering::Relaxed))
            .expect("stored gain units are validated")
    }

    fn attach_shared_url(&self, control: SharedUrlControl) {
        if self.current_role.load(Ordering::Acquire) {
            control.promote_to_current();
        }
        if self.gate.is_paused() {
            control.pause();
        } else {
            control.resume();
        }
        self.shared_url
            .lock()
            .expect("shared URL control lock poisoned")
            .replace(control);
    }

    fn detach_shared_url(&self) {
        self.shared_url
            .lock()
            .expect("shared URL control lock poisoned")
            .take();
    }
}

impl ProducerHandle {
    #[must_use]
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) fn take_receiver(&mut self) -> Result<OpusQueueReceiver> {
        self.receiver.take().ok_or_else(|| {
            MusicStreamError::Internal("producer receiver was already transferred".to_owned())
        })
    }

    pub(super) fn promote_to_current(&mut self) {
        self.role = ProducerRole::Current;
        self.control.promote_to_current();
    }

    pub(super) fn set_volume(&self, volume: VolumeLevel) {
        self.control.volume.store(volume.units(), Ordering::Relaxed);
    }

    pub(super) fn set_gain(&self, gain: GainLevel) {
        self.control.gain.store(gain.centibels(), Ordering::Relaxed);
    }

    pub(super) fn pause(&self) {
        self.control.gate.pause();
        if let Some(shared) = self
            .control
            .shared_url
            .lock()
            .expect("shared URL control lock poisoned")
            .as_ref()
        {
            shared.pause();
        }
    }

    pub(super) fn resume(&self) {
        self.control.gate.resume();
        if let Some(shared) = self
            .control
            .shared_url
            .lock()
            .expect("shared URL control lock poisoned")
            .as_ref()
        {
            shared.resume();
        }
    }

    pub(super) async fn stop(mut self) -> Result<()> {
        self.cancellation.cancel();
        self.receiver.take();
        let Some(mut task) = self.task.take() else {
            return Ok(());
        };
        match tokio::time::timeout(Duration::from_secs(2), &mut task.supervisor).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(MusicStreamError::Internal(format!(
                "{:?} producer {} supervisor failed: {error}",
                self.role, self.generation
            ))),
            Err(_) => {
                task.worker_abort.abort();
                task.supervisor.abort();
                let _ = task.supervisor.await;
                Err(MusicStreamError::Internal(format!(
                    "{:?} producer {} did not stop within 2 seconds",
                    self.role, self.generation
                )))
            }
        }
    }
}

impl Drop for ProducerHandle {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = &self.task {
            task.worker_abort.abort();
            task.supervisor.abort();
        }
    }
}

pub(super) fn spawn(spec: ProducerSpec) -> ProducerHandle {
    let (output, receiver) = opus_queue::bounded(spec.buffer.encoded_capacity_ms);
    let output_lifetime = output.clone();
    let cancellation = CancellationToken::new();
    let worker_cancellation = cancellation.child_token();
    let task_cancellation = cancellation.clone();
    let role = spec.role;
    let control = Arc::new(MediaControl::new(spec.volume, spec.gain, role));
    if spec.initial_paused {
        control.gate.pause();
    }
    let worker_control = Arc::clone(&control);
    let generation = spec.generation;
    let event_tx = spec.events.clone();
    let worker_events = event_tx.clone();
    let worker = async move {
        let job = ProducerJob {
            activation_started: std::time::Instant::now(),
            generation,
            track: spec.track,
            start_position_ms: spec.start_position_ms,
            decode_batch_ms: spec.buffer.decode_batch_ms,
            opus: spec.opus,
            control: worker_control,
            output,
            cancellation: worker_cancellation,
            role,
            next_prime_ms: matches!(role, ProducerRole::Next).then_some(spec.buffer.next_prime_ms),
            events: worker_events,
            performance: spec.performance,
            cpu_scheduler: spec.cpu_scheduler,
            blocking_producers: spec.blocking_producers,
            blocking_preloads: spec.blocking_preloads,
        };
        if job.track.is_hls() {
            run_hls(job, spec.source, spec.live_byte_budget, spec.live_streams).await
        } else if job.track.is_live() {
            run_live(job, spec.source, spec.live_byte_budget, spec.live_streams).await
        } else {
            run_artifact(
                job,
                spec.resolver,
                spec.source,
                spec.live_byte_budget,
                spec.live_streams,
            )
            .await
        }
    };
    let task = supervise_producer(
        worker,
        role,
        Arc::clone(&control.current_role),
        generation,
        task_cancellation,
        event_tx,
        output_lifetime,
    );
    ProducerHandle {
        role,
        generation,
        receiver: Some(receiver),
        control,
        cancellation,
        task: Some(task),
    }
}

fn supervise_producer<F>(
    future: F,
    initial_role: ProducerRole,
    current_role: Arc<AtomicBool>,
    generation: u64,
    cancellation: CancellationToken,
    events: mpsc::Sender<WorkerEvent>,
    output_lifetime: OpusQueueSender,
) -> ProducerTask
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    let worker = tokio::spawn(future);
    let worker_abort = worker.abort_handle();
    let supervisor = tokio::spawn(async move {
        let result = match worker.await {
            Ok(result) => result,
            Err(error) => Err(MusicStreamError::Internal(format!(
                "{initial_role:?} producer {generation} task failed: {error}"
            ))),
        };
        if let Err(error) = &result
            && !cancellation.is_cancelled()
        {
            let event = match current_role.load(Ordering::Acquire) {
                true => WorkerEvent::CurrentFailed {
                    generation,
                    code: error.code(),
                    message: error.to_string(),
                },
                false => WorkerEvent::NextFailed {
                    generation,
                    code: error.code(),
                    message: error.to_string(),
                },
            };
            let _ = events.send(event).await;
        }
        drop(output_lifetime);
    });
    ProducerTask {
        supervisor,
        worker_abort,
    }
}

struct ProducerJob {
    activation_started: std::time::Instant,
    generation: u64,
    track: TrackSource,
    start_position_ms: u64,
    decode_batch_ms: u64,
    opus: LibOpusEncoderConfig,
    control: Arc<MediaControl>,
    output: OpusQueueSender,
    cancellation: CancellationToken,
    role: ProducerRole,
    next_prime_ms: Option<u64>,
    events: mpsc::Sender<WorkerEvent>,
    performance: Arc<RuntimePerformanceCounters>,
    cpu_scheduler: Arc<CpuScheduler>,
    blocking_producers: Arc<Semaphore>,
    blocking_preloads: Arc<Semaphore>,
}

#[derive(Debug)]
struct AdmissionPermit {
    _global: OwnedSemaphorePermit,
}

async fn run_artifact(
    job: ProducerJob,
    resolver: FileSourceResolver,
    source_config: SourceResolverConfig,
    live_byte_budget: LiveByteBudget,
    live_streams: Arc<Semaphore>,
) -> Result<()> {
    if job.track.kind == TrackKind::Url
        && job.start_position_ms == 0
        && supports_progressive_url(&job.track)
    {
        let source = match resolver
            .resolve_url_playback(&job.track, Arc::clone(&job.control.gate), &job.cancellation)
            .await
        {
            Ok(source) => source,
            Err(MusicStreamError::DetectedLiveSource(_)) => {
                return run_detected_live(job, source_config, live_byte_budget, live_streams).await;
            }
            Err(MusicStreamError::DetectedHlsSource(_)) => {
                return run_detected_hls(job, source_config, live_byte_budget, live_streams).await;
            }
            Err(error) => return Err(error),
        };
        record_source_ready(&job);
        let admission = acquire_blocking_job(&job).await?;
        record_codec_start(&job);
        return match source {
            UrlPlaybackSource::Cached(artifact) => {
                run_file_artifact(job, artifact, admission).await
            }
            UrlPlaybackSource::Progressive(source) => {
                run_progressive_url(job, source, admission).await
            }
        };
    }
    let artifact = match resolver
        .resolve(&job.track, &job.control.gate, &job.cancellation)
        .await
    {
        Ok(artifact) => artifact,
        Err(MusicStreamError::DetectedLiveSource(_)) => {
            return run_detected_live(job, source_config, live_byte_budget, live_streams).await;
        }
        Err(MusicStreamError::DetectedHlsSource(_)) => {
            return run_detected_hls(job, source_config, live_byte_budget, live_streams).await;
        }
        Err(error) => return Err(error),
    };
    record_source_ready(&job);
    let admission = acquire_blocking_job(&job).await?;
    record_codec_start(&job);
    run_file_artifact(job, artifact, admission).await
}

async fn run_detected_live(
    mut job: ProducerJob,
    source: SourceResolverConfig,
    live_byte_budget: LiveByteBudget,
    live_streams: Arc<Semaphore>,
) -> Result<()> {
    if matches!(job.control.role(), ProducerRole::Next) {
        return Err(MusicStreamError::Unsupported(
            "detected live HTTP source cannot be preloaded as next without a timeshift model"
                .to_owned(),
        ));
    }
    metrics::counter!("music_stream.source.detected_live_fallbacks").increment(1);
    job.track.kind = TrackKind::Live;
    job.track.seekable = Some(false);
    job.role = ProducerRole::Current;
    job.next_prime_ms = None;
    job.events
        .send(WorkerEvent::CurrentSourceClassified {
            generation: job.generation,
            kind: TrackKind::Live,
            seekable: false,
        })
        .await
        .map_err(|_| MusicStreamError::StreamClosed("worker event loop closed".to_owned()))?;
    run_live(job, source, live_byte_budget, live_streams).await
}

async fn run_detected_hls(
    mut job: ProducerJob,
    source: SourceResolverConfig,
    live_byte_budget: LiveByteBudget,
    live_streams: Arc<Semaphore>,
) -> Result<()> {
    if matches!(job.control.role(), ProducerRole::Next) {
        return Err(MusicStreamError::Unsupported(
            "detected HLS source cannot be preloaded as next yet".to_owned(),
        ));
    }
    metrics::counter!("music_stream.source.detected_hls_fallbacks").increment(1);
    job.track.seekable = Some(false);
    job.track.format_hint = Some("m3u8".to_owned());
    job.role = ProducerRole::Current;
    job.next_prime_ms = None;
    run_hls(job, source, live_byte_budget, live_streams).await
}

async fn run_file_artifact(
    job: ProducerJob,
    artifact: SourceArtifact,
    admission: AdmissionPermit,
) -> Result<()> {
    let lease = Arc::new(CpuLease::new(
        Arc::clone(&job.control.current_role),
        job.cancellation.clone(),
        Arc::clone(&job.cpu_scheduler),
        job.output.depth(),
    ));
    let submitted_at = std::time::Instant::now();
    tokio::task::spawn_blocking(move || {
        let _admission = admission;
        record_blocking_start(&job, submitted_at);
        if !lease.acquire() {
            return Ok(());
        }
        let decoder_started = std::time::Instant::now();
        let decoder = SymphoniaFileDecoder::open_at(artifact.path(), job.start_position_ms)?;
        record_decoder_open(&job, decoder_started);
        let decoder = RubatoResamplingDecoder::new(
            decoder,
            RubatoResamplerConfig::new(AudioFormat {
                sample_rate: SAMPLE_RATE,
                channels: CHANNELS,
            }),
        )?;
        run_cpu(decoder, job, lease)
    })
    .await
    .map_err(|error| MusicStreamError::Internal(error.to_string()))?
}

async fn run_progressive_url(
    job: ProducerJob,
    source: ProgressiveUrlSource,
    admission: AdmissionPermit,
) -> Result<()> {
    let hint = decoder_hint(&job.track);
    let mut terminal = source.terminal;
    let subscription = source.subscription;
    let mut reader = source.reader;
    let cancellation = job.cancellation.clone();
    let control = Arc::clone(&job.control);
    control.attach_shared_url(subscription.control());
    let lease = Arc::new(CpuLease::new(
        Arc::clone(&job.control.current_role),
        job.cancellation.clone(),
        Arc::clone(&job.cpu_scheduler),
        job.output.depth(),
    ));
    reader.set_wait_observer(lease.clone());
    let submitted_at = std::time::Instant::now();
    let mut cpu = tokio::task::spawn_blocking(move || {
        let _admission = admission;
        record_blocking_start(&job, submitted_at);
        if !lease.acquire() {
            return Ok(());
        }
        let decoder_started = std::time::Instant::now();
        let decoder = SymphoniaStreamDecoder::open(reader, hint.as_deref())?;
        record_decoder_open(&job, decoder_started);
        let decoder = RubatoResamplingDecoder::new(
            decoder,
            RubatoResamplerConfig::new(AudioFormat {
                sample_rate: SAMPLE_RATE,
                channels: CHANNELS,
            }),
        )?;
        run_cpu(decoder, job, lease)
    });
    let result = tokio::select! {
        _ = cancellation.cancelled() => {
            let _ = (&mut cpu).await;
            Ok(())
        }
        result = &mut cpu => {
            let cpu_result = result
                .map_err(|error| MusicStreamError::Internal(error.to_string()))
                .and_then(std::convert::identity);
            let source_result = tokio::time::timeout(
                Duration::from_millis(250),
                wait_for_shared_terminal(&mut terminal),
            ).await;
            match source_result {
                Ok(Err(error)) if error.code() != crate::error::ErrorCode::StreamClosed => {
                    Err(error)
                }
                _ => cpu_result,
            }
        }
        source_result = wait_for_shared_terminal(&mut terminal) => {
            match source_result {
                Ok(_artifact) => (&mut cpu)
                    .await
                    .map_err(|error| MusicStreamError::Internal(error.to_string()))?,
                Err(error) => {
                    cancellation.cancel();
                    stop_cpu_after_source_failure(&mut cpu).await;
                    Err(error)
                }
            }
        }
    };
    control.detach_shared_url();
    drop(subscription);
    result
}

async fn wait_for_shared_terminal(
    terminal: &mut tokio::sync::watch::Receiver<Option<Result<SourceArtifact>>>,
) -> Result<SourceArtifact> {
    loop {
        if let Some(result) = terminal.borrow().clone() {
            return result;
        }
        terminal.changed().await.map_err(|_| {
            MusicStreamError::StreamClosed("shared URL terminal state closed".to_owned())
        })?;
    }
}

async fn run_live(
    job: ProducerJob,
    source: SourceResolverConfig,
    live_byte_budget: LiveByteBudget,
    live_streams: Arc<Semaphore>,
) -> Result<()> {
    if matches!(job.role, ProducerRole::Next) {
        return Err(MusicStreamError::Unsupported(
            "live sources cannot run as preload producers".to_owned(),
        ));
    }
    let _live_admission =
        acquire_job_slot(live_streams, &job.control.gate, &job.cancellation).await?;
    let admission = acquire_blocking_job(&job).await?;
    record_codec_start(&job);
    let stream = spawn_http_live_stream(&job.track, source.live_http, live_byte_budget)?;
    let hint = decoder_hint(&job.track);
    run_http_stream(job, stream, hint, admission).await
}

async fn run_hls(
    job: ProducerJob,
    source: SourceResolverConfig,
    live_byte_budget: LiveByteBudget,
    live_streams: Arc<Semaphore>,
) -> Result<()> {
    if matches!(job.role, ProducerRole::Next) {
        return Err(MusicStreamError::Unsupported(
            "HLS sources cannot run as preload producers yet".to_owned(),
        ));
    }
    let _live_admission =
        acquire_job_slot(live_streams, &job.control.gate, &job.cancellation).await?;
    let admission = acquire_blocking_job(&job).await?;
    record_codec_start(&job);
    let (stream, playlist_kind) =
        spawn_http_hls_stream(&job.track, source.live_http, live_byte_budget).await?;
    let mut job = job;
    let kind = match playlist_kind {
        HlsPlaylistKind::Vod => TrackKind::Url,
        HlsPlaylistKind::Live => TrackKind::Live,
    };
    job.track.kind = kind.clone();
    job.track.seekable = Some(false);
    job.events
        .send(WorkerEvent::CurrentSourceClassified {
            generation: job.generation,
            kind,
            seekable: false,
        })
        .await
        .map_err(|_| MusicStreamError::StreamClosed("worker event loop closed".to_owned()))?;
    run_http_stream(job, stream, None, admission).await
}

async fn run_http_stream(
    job: ProducerJob,
    stream: crate::source::HttpLiveStream,
    hint: Option<String>,
    admission: AdmissionPermit,
) -> Result<()> {
    let source_cancellation = stream.cancellation.clone();
    let mut source_task = stream.task;
    let mut reader = stream.reader;
    let cancellation = job.cancellation.clone();
    let lease = Arc::new(CpuLease::new(
        Arc::clone(&job.control.current_role),
        job.cancellation.clone(),
        Arc::clone(&job.cpu_scheduler),
        job.output.depth(),
    ));
    reader.set_wait_observer(lease.clone());
    let submitted_at = std::time::Instant::now();
    let mut cpu = tokio::task::spawn_blocking(move || {
        let _admission = admission;
        record_blocking_start(&job, submitted_at);
        if !lease.acquire() {
            return Ok(());
        }
        let decoder_started = std::time::Instant::now();
        let decoder = SymphoniaStreamDecoder::open(reader, hint.as_deref())?;
        record_decoder_open(&job, decoder_started);
        let decoder = RubatoResamplingDecoder::new(
            decoder,
            RubatoResamplerConfig::new(AudioFormat {
                sample_rate: SAMPLE_RATE,
                channels: CHANNELS,
            }),
        )?;
        run_cpu(decoder, job, lease)
    });
    tokio::select! {
        _ = cancellation.cancelled() => {
            source_cancellation.cancel();
            let _ = (&mut source_task).await;
            let _ = (&mut cpu).await;
            Ok(())
        }
        result = &mut cpu => {
            let cpu_result = result
                .map_err(|error| MusicStreamError::Internal(error.to_string()))
                .and_then(std::convert::identity);
            let source_result = match tokio::time::timeout(
                Duration::from_millis(250),
                &mut source_task,
            ).await {
                Ok(result) => result,
                Err(_) => {
                    source_cancellation.cancel();
                    (&mut source_task).await
                }
            };
            match source_result {
                Ok(Err(error)) if error.code() != crate::error::ErrorCode::StreamClosed => {
                    Err(error)
                }
                Err(error) => Err(MusicStreamError::Internal(format!(
                    "live HTTP task failed: {error}"
                ))),
                _ => cpu_result,
            }
        }
        source_result = &mut source_task => {
            match source_result.map_err(|error| MusicStreamError::Internal(error.to_string()))? {
                Ok(_) => (&mut cpu)
                    .await
                    .map_err(|error| MusicStreamError::Internal(error.to_string()))?,
                Err(error) => {
                    cancellation.cancel();
                    stop_cpu_after_source_failure(&mut cpu).await;
                    Err(error)
                }
            }
        }
    }
}

async fn stop_cpu_after_source_failure(task: &mut JoinHandle<Result<()>>) {
    if tokio::time::timeout(Duration::from_millis(250), &mut *task)
        .await
        .is_err()
    {
        task.abort();
    }
}

async fn acquire_blocking_job(job: &ProducerJob) -> Result<AdmissionPermit> {
    let started = std::time::Instant::now();
    let preload = acquire_preload_job_slot(job).await?;
    if let Some(preload) = preload {
        job.control.attach_preload_admission(preload);
    }
    let producer = acquire_job_slot(
        Arc::clone(&job.blocking_producers),
        &job.control.gate,
        &job.cancellation,
    )
    .await?;
    let elapsed = started.elapsed();
    // Attribute the whole wait to the role that entered admission. A next producer may be
    // promoted while waiting; relabeling that historical preload wait as current would make
    // current capacity look blocked even though promotion is what released the preload gate.
    let current = matches!(job.role, ProducerRole::Current);
    job.performance
        .record_blocking_admission_wait(current, elapsed);
    metrics::histogram!(
        "music_stream.runtime.blocking_admission_wait_us",
        "role" => role_name(job.role)
    )
    .record(elapsed.as_micros() as f64);
    Ok(AdmissionPermit { _global: producer })
}

async fn acquire_preload_job_slot(job: &ProducerJob) -> Result<Option<OwnedSemaphorePermit>> {
    let mut role = job.control.current_role_changed.subscribe();
    loop {
        if *role.borrow_and_update() {
            return Ok(None);
        }
        if !job.control.gate.wait_async(&job.cancellation).await {
            return Err(MusicStreamError::StreamClosed(
                "producer cancelled before preload admission".to_owned(),
            ));
        }
        let permit = Arc::clone(&job.blocking_preloads).acquire_owned();
        tokio::pin!(permit);
        tokio::select! {
            _ = job.cancellation.cancelled() => {
                return Err(MusicStreamError::StreamClosed(
                    "producer cancelled before preload admission".to_owned(),
                ));
            }
            _ = job.control.gate.wait_for_pause(&job.cancellation) => {}
            changed = role.changed() => {
                changed.map_err(|_| MusicStreamError::StreamClosed(
                    "producer role state closed".to_owned(),
                ))?;
            }
            result = &mut permit => {
                let permit = result.map_err(|_| MusicStreamError::StreamClosed(
                    "blocking preload admission was closed".to_owned(),
                ))?;
                return if job.control.current_role.load(Ordering::Acquire) {
                    Ok(None)
                } else {
                    Ok(Some(permit))
                };
            }
        }
    }
}

async fn acquire_job_slot(
    slots: Arc<Semaphore>,
    gate: &PauseGate,
    cancellation: &CancellationToken,
) -> Result<OwnedSemaphorePermit> {
    loop {
        if !gate.wait_async(cancellation).await {
            return Err(MusicStreamError::StreamClosed(
                "producer cancelled before blocking admission".to_owned(),
            ));
        }
        let permit = Arc::clone(&slots).acquire_owned();
        tokio::pin!(permit);
        tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(MusicStreamError::StreamClosed(
                    "producer cancelled before blocking admission".to_owned(),
                ));
            }
            _ = gate.wait_for_pause(cancellation) => {}
            result = &mut permit => {
                return result.map_err(|_| MusicStreamError::StreamClosed(
                    "blocking producer admission was closed".to_owned(),
                ));
            }
        }
    }
}

fn run_cpu<D>(decoder: D, job: ProducerJob, lease: Arc<CpuLease>) -> Result<()>
where
    D: DecoderBackend,
{
    let encoder = LibOpusEncoder::new(job.opus)?;
    let mut pipeline = PlayoutPipeline::new(
        decoder,
        encoder,
        PipelineConfig {
            generation: job.generation,
            sample_rate: SAMPLE_RATE,
            channels: CHANNELS,
            frame_samples_per_channel: FRAME_SAMPLES,
            decode_batch_ms: job.decode_batch_ms,
        },
    )?;
    let mut last_volume = None;
    let mut last_gain = None;
    let mut produced_ms = 0_u64;
    let mut ready_sent = false;
    let mut first_opus_recorded = false;
    loop {
        if job.cancellation.is_cancelled() {
            return Ok(());
        }
        if !job.control.gate.wait_blocking(&job.cancellation) {
            return Ok(());
        }
        let volume = job.control.volume();
        let gain = job.control.gain();
        if last_volume != Some(volume) || last_gain != Some(gain) {
            pipeline.set_volume(VolumeConfig {
                level: volume,
                extra_gain_db: gain.as_db(),
                ..VolumeConfig::default()
            })?;
            last_volume = Some(volume);
            last_gain = Some(gain);
        }
        if !lease.acquire() {
            return Ok(());
        }
        let turn_started = std::time::Instant::now();
        let report = match pipeline.process_turn(|frame| {
            let duration_ms = frame.duration_ms;
            // A full playout queue is I/O backpressure, not CPU work. Release the scarce CPU
            // slot before waiting so paced current streams cannot starve other decoders.
            lease.release();
            if !job.control.gate.wait_blocking(&job.cancellation) {
                return Err(MusicStreamError::StreamClosed(
                    "producer cancelled while paused".to_owned(),
                ));
            }
            let output_started = std::time::Instant::now();
            let output_result = job.output.send_blocking(frame, &job.cancellation);
            let output_elapsed = output_started.elapsed();
            job.performance.record_output_wait(output_elapsed);
            metrics::histogram!(
                "music_stream.runtime.output_wait_us",
                "role" => role_name(job.control.role())
            )
            .record(output_elapsed.as_micros() as f64);
            output_result?;
            if !first_opus_recorded {
                metrics::histogram!(
                    "music_stream.runtime.activation_to_first_opus_us",
                    "role" => role_name(job.control.role()),
                    "source" => source_kind_name(&job.track),
                )
                .record(job.activation_started.elapsed().as_micros() as f64);
                first_opus_recorded = true;
            }
            if !lease.acquire() {
                return Err(MusicStreamError::StreamClosed(
                    "producer cancelled while waiting for CPU".to_owned(),
                ));
            }
            produced_ms = produced_ms.saturating_add(duration_ms);
            metrics::counter!(
                "music_stream.runtime.opus_frames",
                "role" => role_name(job.control.role())
            )
            .increment(1);
            if !ready_sent
                && job
                    .next_prime_ms
                    .is_some_and(|prime_ms| produced_ms >= prime_ms)
            {
                lease.release();
                job.events
                    .blocking_send(WorkerEvent::NextReady {
                        generation: job.generation,
                    })
                    .map_err(|_| {
                        MusicStreamError::StreamClosed("worker event loop closed".to_owned())
                    })?;
                ready_sent = true;
                if !job.control.promotion_gate.wait_blocking(&job.cancellation) {
                    return Err(MusicStreamError::StreamClosed(
                        "preloaded producer was cancelled before promotion".to_owned(),
                    ));
                }
                if !lease.acquire() {
                    return Err(MusicStreamError::StreamClosed(
                        "producer cancelled while waiting for CPU".to_owned(),
                    ));
                }
            }
            Ok(())
        }) {
            Ok(report) => report,
            Err(MusicStreamError::StreamClosed(_)) => return Ok(()),
            Err(error) => return Err(error),
        };
        lease.release();
        metrics::histogram!(
            "music_stream.runtime.worker_turn_us",
            "role" => role_name(job.control.role())
        )
        .record(turn_started.elapsed().as_micros() as f64);
        if report.source_ended {
            if produced_ms == 0 {
                return Err(MusicStreamError::DecodeError(
                    "source ended without decodable PCM samples".to_owned(),
                ));
            }
            if !ready_sent && produced_ms > 0 && job.next_prime_ms.is_some() {
                let _ = job.events.blocking_send(WorkerEvent::NextReady {
                    generation: job.generation,
                });
            }
            return Ok(());
        }
        if !report.made_progress() {
            std::thread::yield_now();
        }
    }
}

fn record_cpu_wait(performance: &RuntimePerformanceCounters, current: bool, elapsed: Duration) {
    performance.record_cpu_wait(current, elapsed);
    metrics::histogram!(
        "music_stream.runtime.cpu_lease_wait_us",
        "role" => if current { "current" } else { "next" }
    )
    .record(elapsed.as_micros() as f64);
}

fn record_blocking_start(job: &ProducerJob, submitted_at: std::time::Instant) {
    let elapsed = submitted_at.elapsed();
    job.performance.record_blocking_start_wait(elapsed);
    metrics::histogram!(
        "music_stream.runtime.blocking_start_wait_us",
        "role" => role_name(job.control.role()),
        "source" => source_kind_name(&job.track),
    )
    .record(elapsed.as_micros() as f64);
}

fn role_name(role: ProducerRole) -> &'static str {
    match role {
        ProducerRole::Current => "current",
        ProducerRole::Next => "next",
    }
}

fn source_kind_name(source: &TrackSource) -> &'static str {
    if source.is_hls() {
        return "hls";
    }
    match source.kind {
        TrackKind::File => "file",
        TrackKind::Url => "url",
        TrackKind::Live => "live",
    }
}

fn record_source_ready(job: &ProducerJob) {
    metrics::histogram!(
        "music_stream.runtime.activation_to_source_ready_us",
        "role" => role_name(job.role),
        "source" => source_kind_name(&job.track),
    )
    .record(job.activation_started.elapsed().as_micros() as f64);
}

fn record_codec_start(job: &ProducerJob) {
    metrics::histogram!(
        "music_stream.runtime.activation_to_codec_start_us",
        "role" => role_name(job.role),
        "source" => source_kind_name(&job.track),
    )
    .record(job.activation_started.elapsed().as_micros() as f64);
}

fn record_decoder_open(job: &ProducerJob, started: std::time::Instant) {
    metrics::histogram!(
        "music_stream.runtime.decoder_open_us",
        "role" => role_name(job.role),
        "source" => source_kind_name(&job.track),
    )
    .record(started.elapsed().as_micros() as f64);
}

fn decoder_hint(source: &TrackSource) -> Option<String> {
    source.media_format_hint().map(str::to_ascii_lowercase)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::audio::decode::{DecodedChunk, MemoryDecoder};
    use crate::audio::frame::OpusFrame;
    use crate::source::{SourceArtifactCache, SourceDownloadRegistry, SourceRuntimeResources};

    #[tokio::test]
    async fn producer_panic_is_reported_to_actor_and_supervised() {
        let (output, receiver) = opus_queue::bounded(40);
        let cancellation = CancellationToken::new();
        let (events, mut event_rx) = mpsc::channel(1);
        let control = Arc::new(MediaControl::new(
            VolumeLevel::default(),
            GainLevel::default(),
            ProducerRole::Current,
        ));
        let task = supervise_producer(
            async { panic!("injected producer panic") },
            ProducerRole::Current,
            Arc::clone(&control.current_role),
            9,
            cancellation.clone(),
            events,
            output,
        );
        let handle = ProducerHandle {
            role: ProducerRole::Current,
            generation: 9,
            receiver: Some(receiver),
            control,
            cancellation,
            task: Some(task),
        };

        let event = event_rx.recv().await.expect("failure event");
        assert!(matches!(
            event,
            WorkerEvent::CurrentFailed {
                generation: 9,
                code: crate::error::ErrorCode::Internal,
                ..
            }
        ));

        handle
            .stop()
            .await
            .expect("reported panic is already handled");
    }

    #[tokio::test]
    async fn promoted_producer_failure_is_reported_as_current() {
        let (output, _receiver) = opus_queue::bounded(40);
        let cancellation = CancellationToken::new();
        let (events, mut event_rx) = mpsc::channel(1);
        let (release, released) = tokio::sync::oneshot::channel();
        let control = Arc::new(MediaControl::new(
            VolumeLevel::default(),
            GainLevel::default(),
            ProducerRole::Next,
        ));
        let task = supervise_producer(
            async move {
                let _ = released.await;
                Err(MusicStreamError::DecodeError("injected".to_owned()))
            },
            ProducerRole::Next,
            Arc::clone(&control.current_role),
            10,
            cancellation,
            events,
            output,
        );

        control.promote_to_current();
        release.send(()).expect("release producer");
        assert!(matches!(
            event_rx.recv().await.expect("failure event"),
            WorkerEvent::CurrentFailed { generation: 10, .. }
        ));
        task.supervisor.await.expect("supervisor");
    }

    #[tokio::test]
    async fn promotion_releases_preload_admission_and_promotes_shared_download() {
        let preloads = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&preloads)
            .acquire_owned()
            .await
            .expect("preload permit");
        let flight = crate::source::SharedUrlFlight::new();
        let subscription = flight.subscribe(false, false).expect("subscription");
        let control = MediaControl::new(
            VolumeLevel::default(),
            GainLevel::default(),
            ProducerRole::Next,
        );
        control.attach_preload_admission(permit);
        control.attach_shared_url(subscription.control());
        assert_eq!(preloads.available_permits(), 0);
        assert!(!*flight.current_priority.borrow());

        control.promote_to_current();

        assert_eq!(preloads.available_permits(), 1);
        assert!(*flight.current_priority.borrow());
        assert!(
            control
                .promotion_gate
                .wait_async(&CancellationToken::new())
                .await
        );
    }

    #[tokio::test]
    async fn next_stops_encoding_at_prime_until_promotion() {
        let (output, receiver) = opus_queue::bounded(400);
        let cancellation = CancellationToken::new();
        let control = Arc::new(MediaControl::new(
            VolumeLevel::default(),
            GainLevel::default(),
            ProducerRole::Next,
        ));
        let (events, mut event_rx) = mpsc::channel(1);
        let job = ProducerJob {
            activation_started: std::time::Instant::now(),
            generation: 11,
            track: TrackSource {
                attempt_id: "attempt-prime-test".to_owned(),
                id: "prime-test".to_owned(),
                kind: TrackKind::File,
                url: None,
                path: Some("/unused".to_owned()),
                format_hint: None,
                seekable: Some(true),
                headers: Default::default(),
                network_policy: crate::model::NetworkPolicy::Provider,
            },
            start_position_ms: 0,
            decode_batch_ms: 400,
            opus: LibOpusEncoderConfig::default(),
            control: Arc::clone(&control),
            output,
            cancellation: cancellation.clone(),
            role: ProducerRole::Next,
            next_prime_ms: Some(100),
            events,
            performance: Arc::new(RuntimePerformanceCounters::default()),
            cpu_scheduler: Arc::new(CpuScheduler::with_maximum(1)),
            blocking_producers: Arc::new(Semaphore::new(1)),
            blocking_preloads: Arc::new(Semaphore::new(1)),
        };
        let decoder = MemoryDecoder::new([DecodedChunk {
            sample_rate: SAMPLE_RATE,
            channels: CHANNELS,
            samples_interleaved: vec![0.0; 20 * FRAME_SAMPLES as usize * CHANNELS as usize],
        }]);
        let lease = Arc::new(CpuLease::new(
            Arc::clone(&control.current_role),
            cancellation.clone(),
            Arc::clone(&job.cpu_scheduler),
            job.output.depth(),
        ));
        let worker = tokio::task::spawn_blocking(move || run_cpu(decoder, job, lease));

        assert!(matches!(
            event_rx.recv().await.expect("next ready"),
            WorkerEvent::NextReady { generation: 11 }
        ));
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(receiver.buffered_ms(), 100);

        cancellation.cancel();
        worker
            .await
            .expect("worker join")
            .expect("cancelled worker");
    }

    #[tokio::test]
    async fn preload_admission_preserves_blocking_capacity_for_current() {
        let producers = Arc::new(Semaphore::new(2));
        let preloads = Arc::new(Semaphore::new(1));
        let gate = Arc::new(PauseGate::default());
        let cancellation = CancellationToken::new();
        let _first_preload = acquire_job_slot(Arc::clone(&preloads), &gate, &cancellation)
            .await
            .expect("first preload slot");
        let _first_producer = acquire_job_slot(Arc::clone(&producers), &gate, &cancellation)
            .await
            .expect("first producer slot");
        let waiting_gate = Arc::clone(&gate);
        let waiting_cancellation = CancellationToken::new();
        let waiter_cancellation = waiting_cancellation.clone();
        let waiting_preloads = Arc::clone(&preloads);
        let waiting_producers = Arc::clone(&producers);
        let blocked_preload = tokio::spawn(async move {
            let _preload =
                acquire_job_slot(waiting_preloads, &waiting_gate, &waiting_cancellation).await?;
            acquire_job_slot(waiting_producers, &waiting_gate, &waiting_cancellation).await
        });
        tokio::task::yield_now().await;
        assert!(!blocked_preload.is_finished());

        let _current = tokio::time::timeout(
            Duration::from_millis(100),
            acquire_job_slot(Arc::clone(&producers), &gate, &cancellation),
        )
        .await
        .expect("current admission timeout")
        .expect("current producer slot");

        waiter_cancellation.cancel();
        assert!(blocked_preload.await.expect("preload waiter task").is_err());
    }

    #[tokio::test]
    async fn progressive_url_wait_does_not_occupy_a_blocking_worker_slot() {
        let blocking_producers = Arc::new(Semaphore::new(1));
        let blocking_preloads = Arc::new(Semaphore::new(1));
        let cancellation = CancellationToken::new();
        let (output, _receiver) = opus_queue::bounded(40);
        let (events, _event_rx) = mpsc::channel(4);
        let resolver = FileSourceResolver::new(
            SourceResolverConfig::default(),
            SourceRuntimeResources {
                cache: Arc::new(std::sync::Mutex::new(SourceArtifactCache::new(1024))),
                http_downloads: Arc::new(Semaphore::new(0)),
                http_preloads: Arc::new(Semaphore::new(0)),
                tempfile_budget: Arc::new(Semaphore::new(256)),
                tempfile_preloads: Arc::new(Semaphore::new(256)),
                downloads: Arc::new(SourceDownloadRegistry::default()),
            },
            false,
        );
        let job = ProducerJob {
            activation_started: std::time::Instant::now(),
            generation: 1,
            track: TrackSource {
                attempt_id: "attempt-slow-url".to_owned(),
                id: "slow-url".to_owned(),
                kind: TrackKind::Url,
                url: Some("http://127.0.0.1:9/audio.mp3".to_owned()),
                path: None,
                format_hint: None,
                seekable: Some(true),
                headers: Default::default(),
                network_policy: crate::model::NetworkPolicy::Provider,
            },
            start_position_ms: 0,
            decode_batch_ms: 80,
            opus: LibOpusEncoderConfig::default(),
            control: Arc::new(MediaControl::new(
                VolumeLevel::default(),
                GainLevel::default(),
                ProducerRole::Current,
            )),
            output,
            cancellation: cancellation.clone(),
            role: ProducerRole::Current,
            next_prime_ms: None,
            events,
            performance: Arc::new(RuntimePerformanceCounters::default()),
            cpu_scheduler: Arc::new(CpuScheduler::with_maximum(1)),
            blocking_producers: Arc::clone(&blocking_producers),
            blocking_preloads,
        };
        let task = tokio::spawn(run_artifact(
            job,
            resolver,
            SourceResolverConfig::default(),
            LiveByteBudget::new(1).expect("live byte budget"),
            Arc::new(Semaphore::new(1)),
        ));
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert_eq!(blocking_producers.available_permits(), 1);

        cancellation.cancel();
        assert!(task.await.expect("artifact task").is_err());
    }

    #[test]
    fn producer_controls_update_shared_download_pause_aggregation() {
        let flight = crate::source::SharedUrlFlight::new();
        let first_subscription = flight.subscribe(false, true).expect("first subscription");
        let second_subscription = flight.subscribe(false, true).expect("second subscription");
        let first_control = Arc::new(MediaControl::new(
            VolumeLevel::default(),
            GainLevel::default(),
            ProducerRole::Current,
        ));
        let second_control = Arc::new(MediaControl::new(
            VolumeLevel::default(),
            GainLevel::default(),
            ProducerRole::Current,
        ));
        first_control.attach_shared_url(first_subscription.control());
        second_control.attach_shared_url(second_subscription.control());
        let (_first_output, first_receiver) = opus_queue::bounded(20);
        let (_second_output, second_receiver) = opus_queue::bounded(20);
        let first = ProducerHandle {
            role: ProducerRole::Current,
            generation: 1,
            receiver: Some(first_receiver),
            control: first_control,
            cancellation: CancellationToken::new(),
            task: None,
        };
        let second = ProducerHandle {
            role: ProducerRole::Current,
            generation: 2,
            receiver: Some(second_receiver),
            control: second_control,
            cancellation: CancellationToken::new(),
            task: None,
        };

        first.pause();
        assert!(!flight.transfer_gate.is_paused());
        second.pause();
        assert!(flight.transfer_gate.is_paused());
        first.resume();
        assert!(!flight.transfer_gate.is_paused());

        drop((first_subscription, second_subscription, first, second));
    }

    #[test]
    fn progressive_url_policy_prefers_explicit_format_hint_and_includes_mp4_candidates() {
        let source = |url: &str, format_hint: Option<&str>| TrackSource {
            attempt_id: format!("attempt-{url}"),
            id: url.to_owned(),
            kind: TrackKind::Url,
            url: Some(url.to_owned()),
            path: None,
            format_hint: format_hint.map(str::to_owned),
            seekable: Some(true),
            headers: Default::default(),
            network_policy: crate::model::NetworkPolicy::Provider,
        };

        assert!(supports_progressive_url(&source(
            "https://cdn.test/audio.mp3?sig=1",
            None,
        )));
        assert!(supports_progressive_url(&source(
            "https://cdn.test/audio.flac",
            None,
        )));
        assert!(supports_progressive_url(&source(
            "https://cdn.test/audio.m4a",
            None,
        )));
        assert!(!supports_progressive_url(&source(
            "https://cdn.test/opaque",
            None,
        )));
        assert!(supports_progressive_url(&source(
            "https://cdn.test/opaque?sig=1",
            Some("MP3"),
        )));
        assert!(supports_progressive_url(&source(
            "https://cdn.test/audio.mp3",
            Some("m4a"),
        )));
    }

    #[test]
    fn next_workers_cannot_consume_the_reserved_current_cpu_slot() {
        let scheduler = Arc::new(CpuScheduler::with_maximum(4));
        let cancellation = CancellationToken::new();
        let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
        let mut workers = Vec::new();
        for _ in 0..8 {
            let worker_scheduler = Arc::clone(&scheduler);
            let worker_cancellation = cancellation.clone();
            let worker_tx = acquired_tx.clone();
            workers.push(std::thread::spawn(move || {
                let Some(permit) =
                    worker_scheduler.acquire(ProducerRole::Next, &worker_cancellation)
                else {
                    return;
                };
                worker_tx.send(()).expect("report next acquisition");
                while !worker_cancellation.is_cancelled() {
                    std::thread::yield_now();
                }
                drop(permit);
            }));
        }
        drop(acquired_tx);

        for _ in 0..3 {
            acquired_rx
                .recv_timeout(Duration::from_millis(100))
                .expect("reserved next capacity");
        }
        assert!(acquired_rx.recv_timeout(Duration::from_millis(30)).is_err());
        assert_eq!(scheduler.diagnostics().0, 3);
        let current = scheduler
            .acquire(ProducerRole::Current, &CancellationToken::new())
            .expect("reserved current permit");
        assert_eq!(scheduler.diagnostics().0, 4);
        drop(current);

        cancellation.cancel();
        for worker in workers {
            worker.join().expect("next worker");
        }
        assert_eq!(scheduler.diagnostics(), (0, 0, 0));
    }

    #[test]
    fn cancelled_cpu_waiter_leaves_the_scheduler() {
        let scheduler = Arc::new(CpuScheduler::with_maximum(1));
        let active_cancellation = CancellationToken::new();
        let active = scheduler
            .acquire(ProducerRole::Current, &active_cancellation)
            .expect("active permit");
        let waiting_cancellation = CancellationToken::new();
        let worker_cancellation = waiting_cancellation.clone();
        let worker_scheduler = Arc::clone(&scheduler);
        let waiter = std::thread::spawn(move || {
            worker_scheduler.acquire(ProducerRole::Next, &worker_cancellation)
        });

        std::thread::sleep(Duration::from_millis(30));
        waiting_cancellation.cancel();
        assert!(waiter.join().expect("waiter").is_none());
        assert_eq!(scheduler.diagnostics(), (1, 0, 0));
        drop(active);
        assert_eq!(scheduler.diagnostics(), (0, 0, 0));
    }

    #[test]
    fn current_waiter_is_woken_before_an_older_next_waiter() {
        let scheduler = Arc::new(CpuScheduler::with_maximum(1));
        let active_cancellation = CancellationToken::new();
        let active = scheduler
            .acquire(ProducerRole::Current, &active_cancellation)
            .expect("active permit");
        let next_cancellation = CancellationToken::new();
        let next_worker_cancellation = next_cancellation.clone();
        let next_scheduler = Arc::clone(&scheduler);
        let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
        let next_tx = acquired_tx.clone();
        let next_waiter = std::thread::spawn(move || {
            let permit = next_scheduler.acquire(ProducerRole::Next, &next_worker_cancellation);
            if let Some(permit) = permit {
                next_tx.send("next").expect("report next acquisition");
                drop(permit);
            }
        });
        while scheduler.diagnostics().2 == 0 {
            std::thread::yield_now();
        }

        let current_cancellation = CancellationToken::new();
        let current_worker_cancellation = current_cancellation.clone();
        let current_scheduler = Arc::clone(&scheduler);
        let current_waiter = std::thread::spawn(move || {
            let permit = current_scheduler
                .acquire(ProducerRole::Current, &current_worker_cancellation)
                .expect("current permit");
            acquired_tx
                .send("current")
                .expect("report current acquisition");
            std::thread::sleep(Duration::from_millis(20));
            drop(permit);
        });
        while scheduler.diagnostics().1 == 0 {
            std::thread::yield_now();
        }

        drop(active);
        assert_eq!(
            acquired_rx
                .recv_timeout(Duration::from_millis(100))
                .expect("first waiter acquisition"),
            "current"
        );

        current_waiter.join().expect("current waiter");
        next_cancellation.cancel();
        next_waiter.join().expect("next waiter");
        assert_eq!(scheduler.diagnostics(), (0, 0, 0));
    }

    #[test]
    fn current_waiters_acquire_cpu_in_fifo_order() {
        const WAITERS: usize = 8;

        let scheduler = Arc::new(CpuScheduler::with_maximum(1));
        let active = scheduler
            .acquire(ProducerRole::Current, &CancellationToken::new())
            .expect("active permit");
        let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
        let mut waiters = Vec::new();
        for index in 0..WAITERS {
            let worker_scheduler = Arc::clone(&scheduler);
            let acquired_tx = acquired_tx.clone();
            waiters.push(std::thread::spawn(move || {
                let permit = worker_scheduler
                    .acquire(ProducerRole::Current, &CancellationToken::new())
                    .expect("current permit");
                acquired_tx.send(index).expect("report acquisition");
                drop(permit);
            }));
            while scheduler.diagnostics().1 != index + 1 {
                std::thread::yield_now();
            }
        }
        drop(acquired_tx);

        drop(active);
        let acquired = acquired_rx.iter().collect::<Vec<_>>();
        assert_eq!(acquired, (0..WAITERS).collect::<Vec<_>>());
        for waiter in waiters {
            waiter.join().expect("current waiter");
        }
        assert_eq!(scheduler.diagnostics(), (0, 0, 0));
    }

    #[test]
    fn next_waiters_acquire_cpu_in_fifo_order() {
        const WAITERS: usize = 8;

        let scheduler = Arc::new(CpuScheduler::with_maximum(2));
        let active = scheduler
            .acquire(ProducerRole::Current, &CancellationToken::new())
            .expect("active permit");
        let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
        let mut waiters = Vec::new();
        for index in 0..WAITERS {
            let worker_scheduler = Arc::clone(&scheduler);
            let acquired_tx = acquired_tx.clone();
            waiters.push(std::thread::spawn(move || {
                let permit = worker_scheduler
                    .acquire(ProducerRole::Next, &CancellationToken::new())
                    .expect("next permit");
                acquired_tx.send(index).expect("report acquisition");
                drop(permit);
            }));
            while scheduler.diagnostics().2 != index + 1 {
                std::thread::yield_now();
            }
        }
        drop(acquired_tx);

        drop(active);
        let acquired = acquired_rx.iter().collect::<Vec<_>>();
        assert_eq!(acquired, (0..WAITERS).collect::<Vec<_>>());
        for waiter in waiters {
            waiter.join().expect("next waiter");
        }
        assert_eq!(scheduler.diagnostics(), (0, 0, 0));
    }

    #[test]
    fn least_buffered_current_is_scheduled_before_a_healthy_waiter() {
        let scheduler = Arc::new(CpuScheduler::with_maximum(1));
        let active = scheduler
            .acquire(ProducerRole::Current, &CancellationToken::new())
            .expect("active permit");
        let cancellation = CancellationToken::new();
        let (healthy_output, _healthy_receiver) = opus_queue::bounded(400);
        for index in 0..10 {
            healthy_output
                .send_blocking(
                    OpusFrame {
                        generation: 1,
                        payload: Bytes::from_static(b"opus"),
                        samples_per_channel: FRAME_SAMPLES,
                        duration_ms: 20,
                        marker: false,
                        track_position_samples: index * u64::from(FRAME_SAMPLES),
                    },
                    &cancellation,
                )
                .expect("buffer healthy current");
        }
        let (starved_output, _starved_receiver) = opus_queue::bounded(400);
        let healthy_waiter = Arc::new(CpuWaiter::with_depth(healthy_output.depth()));
        let starved_waiter = Arc::new(CpuWaiter::with_depth(starved_output.depth()));
        let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();

        let healthy_scheduler = Arc::clone(&scheduler);
        let healthy_tx = acquired_tx.clone();
        let healthy = std::thread::spawn(move || {
            let permit = healthy_scheduler
                .acquire_with_waiter(
                    ProducerRole::Current,
                    &CancellationToken::new(),
                    &healthy_waiter,
                )
                .expect("healthy current permit");
            healthy_tx.send("healthy").expect("report acquisition");
            drop(permit);
        });
        while scheduler.diagnostics().1 != 1 {
            std::thread::yield_now();
        }

        let starved_scheduler = Arc::clone(&scheduler);
        let starved = std::thread::spawn(move || {
            let permit = starved_scheduler
                .acquire_with_waiter(
                    ProducerRole::Current,
                    &CancellationToken::new(),
                    &starved_waiter,
                )
                .expect("starved current permit");
            acquired_tx.send("starved").expect("report acquisition");
            drop(permit);
        });
        while scheduler.diagnostics().1 != 2 {
            std::thread::yield_now();
        }

        drop(active);
        assert_eq!(
            acquired_rx
                .recv_timeout(Duration::from_millis(100))
                .expect("urgent acquisition"),
            "starved"
        );
        healthy.join().expect("healthy waiter");
        starved.join().expect("starved waiter");
        assert_eq!(scheduler.diagnostics(), (0, 0, 0));
    }
}
