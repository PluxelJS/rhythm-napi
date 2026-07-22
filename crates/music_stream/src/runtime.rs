//! Per-stream asynchronous media runtime.
//!
//! A stream owns one persistent RTP session. Tracks are replaceable Opus producers;
//! decode/resample/encode always runs on blocking CPU workers and can never delay RTP pacing.

use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};

mod opus_queue;
mod producer;
mod sender;

use producer::{ProducerHandle, ProducerRole, ProducerSpec};
use sender::SenderHandle;

use crate::audio::opus::LibOpusEncoderConfig;
use crate::error::{MusicStreamError, Result};
use crate::event::StreamEvent;
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

const SAMPLE_RATE: u32 = 48_000;
const CHANNELS: u16 = 2;
const FRAME_SAMPLES: u32 = 960;
const PREBUFFER_MS: u64 = 100;
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

impl Default for RuntimeResourceLimits {
    fn default() -> Self {
        let max_cpu_workers = std::thread::available_parallelism()
            .map_or(2, |value| value.get())
            .min(MAX_BLOCKING_PRODUCERS);
        let max_blocking_producers = max_cpu_workers
            .saturating_mul(4)
            .clamp(MIN_BLOCKING_PRODUCERS, MAX_BLOCKING_PRODUCERS);
        Self {
            max_streams: MAX_STREAMS,
            max_cpu_workers,
            max_blocking_producers,
            max_blocking_preloads: (max_blocking_producers / 4).max(1),
            max_concurrent_http_downloads: MAX_CONCURRENT_HTTP_DOWNLOADS,
            max_concurrent_live_streams: MAX_CONCURRENT_LIVE_STREAMS,
            max_live_buffered_bytes: MAX_LIVE_BUFFERED_BYTES,
            max_tempfile_bytes: MAX_TEMPFILE_BYTES,
        }
    }
}

#[derive(Debug)]
pub struct RuntimeResources {
    limits: RuntimeResourceLimits,
    streams: Arc<Semaphore>,
    source_cache: SharedSourceArtifactCache,
    source_downloads: SharedSourceDownloadRegistry,
    http_downloads: Arc<Semaphore>,
    http_preloads: Arc<Semaphore>,
    live_streams: Arc<Semaphore>,
    live_byte_budget: LiveByteBudget,
    tempfile_budget: Arc<Semaphore>,
    tempfile_preloads: Arc<Semaphore>,
    cpu_scheduler: Arc<producer::CpuScheduler>,
    blocking_producers: Arc<Semaphore>,
    blocking_preloads: Arc<Semaphore>,
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
        let tempfile_permits = usize::try_from(limits.max_tempfile_bytes / TEMPFILE_QUOTA_BYTES)
            .map_err(|_| {
                MusicStreamError::InvalidConfig("tempfile byte limit is too large".to_owned())
            })?;
        Ok(Self {
            streams: Arc::new(Semaphore::new(limits.max_streams)),
            http_downloads: Arc::new(Semaphore::new(limits.max_concurrent_http_downloads)),
            http_preloads: Arc::new(Semaphore::new(limits.max_concurrent_http_downloads - 1)),
            live_streams: Arc::new(Semaphore::new(limits.max_concurrent_live_streams)),
            live_byte_budget: LiveByteBudget::new(limits.max_live_buffered_bytes)?,
            tempfile_budget: Arc::new(Semaphore::new(tempfile_permits)),
            tempfile_preloads: Arc::new(Semaphore::new((tempfile_permits / 4).max(1))),
            cpu_scheduler: Arc::new(producer::CpuScheduler::with_maximum(limits.max_cpu_workers)),
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
}

#[derive(Clone)]
pub struct StreamRuntimeConfig {
    pub transport: RtpTransportConfig,
    pub source: SourceResolverConfig,
    pub resources: Arc<RuntimeResources>,
    pub buffer: MediaBufferConfig,
    pub rtcp_interval: Duration,
    pub on_event: Option<Arc<dyn Fn(StreamEvent) + Send + Sync>>,
}

impl std::fmt::Debug for StreamRuntimeConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamRuntimeConfig")
            .field("transport", &self.transport)
            .field("source", &self.source)
            .field("buffer", &self.buffer)
            .field("rtcp_interval", &self.rtcp_interval)
            .finish_non_exhaustive()
    }
}

impl StreamRuntimeConfig {
    #[must_use]
    pub fn new(transport: RtpTransportConfig, source: SourceResolverConfig) -> Self {
        Self {
            transport,
            source,
            resources: Arc::new(RuntimeResources::default()),
            buffer: MediaBufferConfig {
                prebuffer_ms: PREBUFFER_MS,
                ..MediaBufferConfig::default()
            },
            rtcp_interval: RTCP_INTERVAL,
            on_event: None,
        }
    }

    pub fn validate(&self) -> Result<()> {
        self.transport.validate()?;
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
    sender: SenderHandle,
    current: Mutex<Option<ProducerHandle>>,
    next: Mutex<Option<ProducerHandle>>,
    config: StreamRuntimeConfig,
    worker_events: mpsc::Sender<WorkerEvent>,
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
        next: Option<TrackSource>,
        config: StreamRuntimeConfig,
        volume: VolumeLevel,
        gain: GainLevel,
    ) -> Result<Self> {
        let current = current.with_detected_kind();
        let next = next.map(TrackSource::with_detected_kind);
        Self::validate_stream_id(&stream_id)?;
        config.validate()?;
        current.validate()?;
        if let Some(next) = &next {
            validate_next_source(next)?;
        }
        let stream_permit = Arc::clone(&config.resources.streams)
            .try_acquire_owned()
            .map_err(|_| {
                MusicStreamError::Busy(format!(
                    "stream limit {} is exhausted",
                    config.resources.limits.max_streams
                ))
            })?;
        let (worker_tx, worker_rx) = mpsc::channel(64);
        let sender = SenderHandle::spawn(
            config.transport.clone(),
            config.buffer.prebuffer_ms,
            config.buffer.max_playout_lateness_ms,
            config.rtcp_interval,
            worker_tx.clone(),
        )
        .await?;
        let inner = Arc::new(StreamRuntimeInner {
            stream_permit: Mutex::new(Some(stream_permit)),
            actor: Mutex::new(StreamActor::new(stream_id, Some(current), next)),
            orchestration: Mutex::new(()),
            sender,
            current: Mutex::new(None),
            next: Mutex::new(None),
            config,
            worker_events: worker_tx,
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
        let progress = self.inner.sender.progress();
        if progress.generation == status.generation {
            status.time_played_ms = progress.stream_position_ms();
        }
        StreamRuntimeSnapshot { status, progress }
    }

    pub async fn shutdown(&self) -> Result<StreamRuntimeSnapshot> {
        self.command(StreamCommand::Stop).await
    }
}

fn normalize_command_sources(command: &mut StreamCommand) {
    match command {
        StreamCommand::SetNext(next) => {
            *next = next.take().map(TrackSource::with_detected_kind);
        }
        StreamCommand::SwitchTrack { current, next } => {
            *current = current.clone().with_detected_kind();
            *next = next.take().map(TrackSource::with_detected_kind);
        }
        StreamCommand::RefreshCurrentSource { current } => {
            *current = current.clone().with_detected_kind();
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
        StreamCommand::SetNext(Some(next)) => validate_next_source(next),
        StreamCommand::SwitchTrack { current, next } => {
            current.validate()?;
            if let Some(next) = next {
                validate_next_source(next)?;
            }
            Ok(())
        }
        StreamCommand::RefreshCurrentSource { current } => current.validate(),
        StreamCommand::Play
        | StreamCommand::Pause
        | StreamCommand::Stop
        | StreamCommand::Seek { .. }
        | StreamCommand::SetNext(None)
        | StreamCommand::SetVolume { .. }
        | StreamCommand::SetGain { .. } => Ok(()),
    }
}

fn validate_next_source(next: &TrackSource) -> Result<()> {
    next.validate()?;
    if next.is_live() {
        return Err(MusicStreamError::Unsupported(
            "live sources cannot be preloaded as next without a timeshift model".to_owned(),
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
        let progress = self.sender.progress();
        if progress.generation == status.generation {
            status.time_played_ms = progress.stream_position_ms();
        }
        StreamRuntimeSnapshot { status, progress }
    }

    async fn fail_runtime(&self, error: &MusicStreamError) {
        let current = self.current.lock().await.take();
        let next = self.next.lock().await.take();
        let (current_result, next_result, sender_result) = tokio::join!(
            stop_producer(current),
            stop_producer(next),
            self.sender.shutdown(),
        );
        self.stream_permit.lock().await.take();
        for cleanup_error in [current_result, next_result, sender_result]
            .into_iter()
            .filter_map(std::result::Result::err)
        {
            tracing::warn!(error = %cleanup_error, "runtime failure cleanup failed");
        }
        let output = self.actor.lock().await.handle_runtime_failure(error);
        self.publish_output(output);
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
            TaskAction::StartCurrent { generation, track } => {
                if let Some(mut producer) = take_generation(&self.next, generation).await {
                    producer.promote_to_current();
                    let receiver = producer.take_receiver()?;
                    self.sender
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
                    self.sender
                        .activate(generation, start_position_ms, paused, receiver)
                        .await?;
                    replace_producer(&self.current, producer).await?;
                }
            }
            TaskAction::PrepareNext { generation, track } => {
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
            }
            TaskAction::CancelCurrent { generation } => {
                let sender_result = self.sender.deactivate(generation).await;
                let producer_result = cancel_generation(&self.current, generation).await;
                sender_result?;
                producer_result?;
            }
            TaskAction::CancelNext { generation } => {
                cancel_generation(&self.next, generation).await?;
            }
            TaskAction::PauseCurrent { generation } => {
                pause_generation(&self.current, generation).await;
                pause_slot(&self.next).await;
                self.sender.pause(generation).await?;
            }
            TaskAction::PauseNext { generation } => {
                pause_generation(&self.next, generation).await;
            }
            TaskAction::ResumeCurrent { generation } => {
                resume_generation(&self.current, generation).await;
                resume_slot(&self.next).await;
                self.sender.resume(generation).await?;
            }
            TaskAction::ResumeNext { generation } => {
                resume_generation(&self.next, generation).await;
            }
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
                let current = self.current.lock().await.take();
                let next = self.next.lock().await.take();
                let (current_result, next_result) =
                    tokio::join!(stop_producer(current), stop_producer(next));
                let sender_result = self.sender.shutdown().await;
                self.stream_permit.lock().await.take();
                current_result?;
                next_result?;
                sender_result?;
            }
        }
        Ok(())
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
        let opus = LibOpusEncoderConfig {
            max_packet_bytes: self.config.transport.mtu.saturating_sub(12),
            bitrate_bps: self
                .config
                .transport
                .opus_bitrate_bps
                .map(|value| value as i32),
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
    fn runtime_resources_reject_zero_download_concurrency() {
        let error = RuntimeResources::new(RuntimeResourceLimits {
            max_concurrent_http_downloads: 0,
            ..RuntimeResourceLimits::default()
        })
        .expect_err("zero download slots must fail");

        assert_eq!(error.code(), crate::error::ErrorCode::InvalidConfig);
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
    fn default_runtime_uses_twenty_millisecond_opus_frames() {
        assert_eq!(FRAME_SAMPLES * 1_000 / SAMPLE_RATE, 20);
    }
}
