//! Async source resolution and bounded live byte delivery.

use std::collections::HashMap;
use std::future::Future;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak, mpsc};
use std::time::{Duration, Instant};

use lru::LruCache;
use tokio::io::AsyncWriteExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio_util::sync::CancellationToken;

use crate::control::PauseGate;
use crate::error::{MusicStreamError, Result};
use crate::model::{NetworkPolicy, TrackKind, TrackSource, is_public_ip};

mod hls;
mod live;
mod mp4;
mod spool;
pub(crate) use hls::{HlsPlaylistKind, spawn_http_hls_stream};
pub use live::HttpLiveStreamConfig;
#[cfg(test)]
pub(crate) use live::StreamingByteReader;
pub(crate) use live::{
    BlockingReadObserver, HttpLiveStream, LiveByteBudget, spawn_http_live_stream,
};
use mp4::{FastStartDecision, FastStartProbe};
pub(crate) use spool::GrowingSpoolReader;
use spool::{GrowingSpool, GrowingSpoolWriter, growing_spool};

const HTTP_SOURCE_IO_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_SOURCE_MAX_BYTES: u64 = 256 * 1024 * 1024;
const HTTP_SOURCE_MAX_RETRIES: u8 = 2;
const HTTP_SOURCE_RETRY_BACKOFF: Duration = Duration::from_millis(250);
const DEFAULT_ARTIFACT_CACHE_BYTES: u64 = 512 * 1024 * 1024;
const TEMPFILE_QUOTA_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HttpSourceConfig {
    pub io_timeout: Duration,
    pub max_bytes: u64,
    pub cache_temp_files: bool,
    pub max_retries: u8,
    pub retry_backoff: Duration,
}

impl Default for HttpSourceConfig {
    fn default() -> Self {
        Self {
            io_timeout: HTTP_SOURCE_IO_TIMEOUT,
            max_bytes: HTTP_SOURCE_MAX_BYTES,
            cache_temp_files: false,
            max_retries: HTTP_SOURCE_MAX_RETRIES,
            retry_backoff: HTTP_SOURCE_RETRY_BACKOFF,
        }
    }
}

impl HttpSourceConfig {
    pub fn validate(&self) -> Result<()> {
        if self.io_timeout.is_zero()
            || self.max_bytes == 0
            || self.max_bytes > u64::from(u32::MAX) * TEMPFILE_QUOTA_BYTES
            || (self.max_retries > 0 && self.retry_backoff.is_zero())
        {
            return Err(MusicStreamError::InvalidConfig(
                "HTTP I/O timeout and max bytes must fit positive limits".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SourceResolverConfig {
    pub http: HttpSourceConfig,
    pub live_http: HttpLiveStreamConfig,
}

impl SourceResolverConfig {
    pub fn validate(&self) -> Result<()> {
        self.http.validate()?;
        self.live_http.validate()
    }
}

#[derive(Debug)]
pub struct SourceArtifactCache {
    max_bytes: u64,
    retained_quota_bytes: u64,
    entries: LruCache<String, SourceArtifact>,
}

impl Default for SourceArtifactCache {
    fn default() -> Self {
        Self::new(DEFAULT_ARTIFACT_CACHE_BYTES)
    }
}

impl SourceArtifactCache {
    #[must_use]
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            retained_quota_bytes: 0,
            entries: LruCache::unbounded(),
        }
    }

    #[must_use]
    fn get(&mut self, key: &str, max_bytes: u64) -> Option<SourceArtifact> {
        if self
            .entries
            .peek(key)
            .is_none_or(|artifact| artifact.len_bytes > max_bytes)
        {
            return None;
        }
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, artifact: SourceArtifact) -> bool {
        let retained_quota_bytes = artifact_tempfile_quota_bytes(artifact.len_bytes);
        if !artifact.cacheable || retained_quota_bytes > self.max_bytes {
            return false;
        }
        if let Some(old) = self
            .entries
            .put(artifact.stable_key.clone(), artifact.clone())
        {
            self.retained_quota_bytes = self
                .retained_quota_bytes
                .saturating_sub(artifact_tempfile_quota_bytes(old.len_bytes));
        }
        self.retained_quota_bytes = self
            .retained_quota_bytes
            .saturating_add(retained_quota_bytes);
        while self.retained_quota_bytes > self.max_bytes {
            let Some((_, old)) = self.entries.pop_lru() else {
                break;
            };
            self.retained_quota_bytes = self
                .retained_quota_bytes
                .saturating_sub(artifact_tempfile_quota_bytes(old.len_bytes));
        }
        true
    }

    #[must_use]
    pub fn take(&mut self) -> Self {
        Self {
            max_bytes: self.max_bytes,
            retained_quota_bytes: std::mem::take(&mut self.retained_quota_bytes),
            entries: std::mem::replace(&mut self.entries, LruCache::unbounded()),
        }
    }
}

fn artifact_tempfile_quota_bytes(bytes: u64) -> u64 {
    u64::from(tempfile_quota_units(bytes)).saturating_mul(TEMPFILE_QUOTA_BYTES)
}

pub type SharedSourceArtifactCache = Arc<Mutex<SourceArtifactCache>>;

#[derive(Debug, Default)]
pub struct SourceDownloadRegistry {
    flights: Mutex<HashMap<String, Weak<SharedUrlFlight>>>,
    prune_counter: AtomicU64,
}

impl SourceDownloadRegistry {
    fn prune_dead_if_due(&self, flights: &mut HashMap<String, Weak<SharedUrlFlight>>) {
        if self
            .prune_counter
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(64)
        {
            flights.retain(|_, flight| flight.strong_count() > 0);
        }
    }
}

fn download_flight_key(source: &TrackSource, config: &HttpSourceConfig) -> String {
    let stable_key = source.stable_key();
    let mut request_hasher = DefaultHasher::new();
    source.url.hash(&mut request_hasher);
    source.headers.hash(&mut request_hasher);
    source.network_policy.hash(&mut request_hasher);
    format!(
        "{}:{stable_key}:{:016x}:{}:{:?}:{}:{:?}:{}",
        stable_key.len(),
        request_hasher.finish(),
        config.max_bytes,
        config.io_timeout,
        config.max_retries,
        config.retry_backoff,
        config.cache_temp_files,
    )
}

pub type SharedSourceDownloadRegistry = Arc<SourceDownloadRegistry>;

#[derive(Debug, Default)]
struct SubscriberState {
    subscribers: HashMap<u64, SubscriberEntry>,
}

#[derive(Clone, Copy, Debug)]
struct SubscriberEntry {
    paused: bool,
    current: bool,
}

#[derive(Debug)]
pub(crate) struct SharedUrlFlight {
    reader: watch::Sender<Option<GrowingSpool>>,
    terminal: watch::Sender<Option<Result<SourceArtifact>>>,
    subscribers: Mutex<SubscriberState>,
    next_subscriber_id: AtomicU64,
    pub(crate) current_priority: watch::Sender<bool>,
    pub(crate) transfer_gate: Arc<PauseGate>,
    cancellation: CancellationToken,
    task: Mutex<Option<SharedUrlTask>>,
}

#[derive(Debug)]
struct SharedUrlTask {
    supervisor: tokio::task::JoinHandle<()>,
    worker_abort: tokio::task::AbortHandle,
}

#[derive(Clone, Debug)]
pub(crate) struct SharedUrlControl {
    flight: Weak<SharedUrlFlight>,
    subscriber_id: u64,
}

#[derive(Debug)]
pub(crate) struct SharedUrlSubscription {
    flight: Arc<SharedUrlFlight>,
    subscriber_id: u64,
}

impl SharedUrlFlight {
    pub(crate) fn new() -> Arc<Self> {
        let (reader, _) = watch::channel(None);
        let (terminal, _) = watch::channel(None);
        let (current_priority, _) = watch::channel(false);
        let transfer_gate = Arc::new(PauseGate::default());
        transfer_gate.pause();
        Arc::new(Self {
            reader,
            terminal,
            subscribers: Mutex::new(SubscriberState::default()),
            next_subscriber_id: AtomicU64::new(1),
            current_priority,
            transfer_gate,
            cancellation: CancellationToken::new(),
            task: Mutex::new(None),
        })
    }

    pub(crate) fn subscribe(
        self: &Arc<Self>,
        paused: bool,
        current: bool,
    ) -> SharedUrlSubscription {
        let subscriber_id = self.next_subscriber_id.fetch_add(1, Ordering::Relaxed);
        self.subscribers
            .lock()
            .expect("shared URL subscriber lock poisoned")
            .subscribers
            .insert(subscriber_id, SubscriberEntry { paused, current });
        self.apply_aggregate_state();
        SharedUrlSubscription {
            flight: Arc::clone(self),
            subscriber_id,
        }
    }

    fn set_paused(&self, subscriber_id: u64, paused: bool) {
        let mut subscribers = self
            .subscribers
            .lock()
            .expect("shared URL subscriber lock poisoned");
        if let Some(state) = subscribers.subscribers.get_mut(&subscriber_id) {
            state.paused = paused;
        }
        drop(subscribers);
        self.apply_aggregate_state();
    }

    fn promote_to_current(&self, subscriber_id: u64) {
        let mut subscribers = self
            .subscribers
            .lock()
            .expect("shared URL subscriber lock poisoned");
        if let Some(state) = subscribers.subscribers.get_mut(&subscriber_id) {
            state.current = true;
        }
        drop(subscribers);
        self.apply_aggregate_state();
    }

    fn unsubscribe(&self, subscriber_id: u64) {
        let empty = {
            let mut subscribers = self
                .subscribers
                .lock()
                .expect("shared URL subscriber lock poisoned");
            subscribers.subscribers.remove(&subscriber_id);
            subscribers.subscribers.is_empty()
        };
        if empty {
            self.cancellation.cancel();
        } else {
            self.apply_aggregate_state();
        }
    }

    fn apply_aggregate_state(&self) {
        let subscribers = self
            .subscribers
            .lock()
            .expect("shared URL subscriber lock poisoned");
        let any_active = subscribers
            .subscribers
            .values()
            .any(|subscriber| !subscriber.paused);
        let has_current = subscribers
            .subscribers
            .values()
            .any(|subscriber| subscriber.current);
        drop(subscribers);
        // Promotion is intentionally sticky for this finite transfer. Downgrading
        // after a current subscriber leaves would require reacquiring preload
        // quota mid-response and could deadlock a partially written artifact.
        if has_current {
            self.current_priority.send_replace(true);
        }
        if any_active {
            self.transfer_gate.resume();
        } else {
            self.transfer_gate.pause();
        }
    }
}

impl Drop for SharedUrlFlight {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Ok(task) = self.task.get_mut()
            && let Some(task) = task.take()
        {
            task.worker_abort.abort();
            task.supervisor.abort();
        }
    }
}

impl SharedUrlSubscription {
    pub(crate) fn control(&self) -> SharedUrlControl {
        SharedUrlControl {
            flight: Arc::downgrade(&self.flight),
            subscriber_id: self.subscriber_id,
        }
    }

    fn set_paused(&self, paused: bool) {
        self.flight.set_paused(self.subscriber_id, paused);
    }
}

impl Drop for SharedUrlSubscription {
    fn drop(&mut self) {
        self.flight.unsubscribe(self.subscriber_id);
    }
}

impl SharedUrlControl {
    pub(crate) fn pause(&self) {
        if let Some(flight) = self.flight.upgrade() {
            flight.set_paused(self.subscriber_id, true);
        }
    }

    pub(crate) fn resume(&self) {
        if let Some(flight) = self.flight.upgrade() {
            flight.set_paused(self.subscriber_id, false);
        }
    }

    pub(crate) fn promote_to_current(&self) {
        if let Some(flight) = self.flight.upgrade() {
            flight.promote_to_current(self.subscriber_id);
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SourceArtifact {
    stable_key: String,
    path: PathBuf,
    len_bytes: u64,
    cacheable: bool,
    _cleanup: Option<Arc<TempArtifactCleanup>>,
}

#[derive(Debug)]
pub(crate) enum UrlPlaybackSource {
    Cached(SourceArtifact),
    Progressive(ProgressiveUrlSource),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProgressiveSourceMode {
    Immediate,
    FastStartMp4,
}

fn progressive_source_mode(source: &TrackSource) -> Option<ProgressiveSourceMode> {
    let hint = source.media_format_hint()?;
    if is_mp4_format(hint) || source.url_extension().is_some_and(is_mp4_format) {
        return Some(ProgressiveSourceMode::FastStartMp4);
    }
    if ["aac", "flac", "mp3", "oga", "ogg", "opus", "wav", "wave"]
        .iter()
        .any(|supported| hint.eq_ignore_ascii_case(supported))
    {
        return Some(ProgressiveSourceMode::Immediate);
    }
    None
}

fn is_mp4_format(hint: &str) -> bool {
    hint.eq_ignore_ascii_case("m4a") || hint.eq_ignore_ascii_case("mp4")
}

pub(crate) fn supports_progressive_url(source: &TrackSource) -> bool {
    progressive_source_mode(source).is_some()
}

#[derive(Debug)]
pub(crate) struct ProgressiveUrlSource {
    pub reader: GrowingSpoolReader,
    pub terminal: watch::Receiver<Option<Result<SourceArtifact>>>,
    pub subscription: SharedUrlSubscription,
}

#[derive(Debug)]
struct TempArtifactCleanup {
    path: Option<tempfile::TempPath>,
    quota: Mutex<Option<TempfileQuota>>,
}

impl TempArtifactCleanup {
    fn new(path: tempfile::TempPath, quota: TempfileQuota) -> Self {
        Self {
            path: Some(path),
            quota: Mutex::new(Some(quota)),
        }
    }

    fn shrink_quota_to(&self, bytes: u64) {
        let retained = tempfile_quota_units(bytes);
        let mut quota = self.quota.lock().expect("tempfile quota lock poisoned");
        let Some(permit) = quota.as_mut() else {
            return;
        };
        let excess = permit
            .global
            .num_permits()
            .saturating_sub(retained as usize);
        if excess > 0 {
            drop(permit.global.split(excess));
            if let Some(preload) = permit.preload.as_mut() {
                drop(preload.split(excess.min(preload.num_permits())));
            }
        }
    }
}

impl Drop for TempArtifactCleanup {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        let quota = self
            .quota
            .get_mut()
            .expect("tempfile quota lock poisoned")
            .take();
        enqueue_temp_cleanup(path, quota);
    }
}

#[derive(Debug)]
struct TempCleanupJob {
    path: tempfile::TempPath,
    _quota: Option<TempfileQuota>,
}

enum TempCleanupCommand {
    Delete(TempCleanupJob),
    Flush(mpsc::Sender<()>),
}

fn temp_cleanup_sender() -> Option<&'static mpsc::Sender<TempCleanupCommand>> {
    static CLEANUP: OnceLock<Option<mpsc::Sender<TempCleanupCommand>>> = OnceLock::new();
    CLEANUP
        .get_or_init(|| {
            let (sender, receiver) = mpsc::channel::<TempCleanupCommand>();
            std::thread::Builder::new()
                .name("music-temp-cleanup".to_owned())
                .spawn(move || {
                    for command in receiver {
                        match command {
                            TempCleanupCommand::Delete(job) => {
                                if let Err(error) = job.path.close() {
                                    tracing::warn!(%error, "failed to remove source artifact");
                                }
                                // The quota is released only after deletion, so admitted
                                // disk usage cannot race asynchronous cleanup.
                                drop(job._quota);
                            }
                            TempCleanupCommand::Flush(reply) => {
                                let _ = reply.send(());
                            }
                        }
                    }
                })
                .ok()
                .map(|_| sender)
        })
        .as_ref()
}

fn enqueue_temp_cleanup(path: tempfile::TempPath, quota: Option<TempfileQuota>) {
    let job = TempCleanupJob {
        path,
        _quota: quota,
    };
    let job = match temp_cleanup_sender() {
        Some(sender) => match sender.send(TempCleanupCommand::Delete(job)) {
            Ok(()) => return,
            Err(error) => match error.0 {
                TempCleanupCommand::Delete(job) => job,
                TempCleanupCommand::Flush(_) => unreachable!("sent a delete command"),
            },
        },
        None => job,
    };
    if let Err(error) = job.path.close() {
        tracing::warn!(%error, "failed to remove source artifact");
    }
}

pub(crate) async fn flush_temp_cleanup() -> Result<()> {
    tokio::task::spawn_blocking(|| {
        let Some(sender) = temp_cleanup_sender() else {
            return Ok(());
        };
        let (reply, receiver) = mpsc::channel();
        sender
            .send(TempCleanupCommand::Flush(reply))
            .map_err(|_| MusicStreamError::Internal("tempfile cleanup worker closed".to_owned()))?;
        receiver.recv_timeout(Duration::from_secs(2)).map_err(|_| {
            MusicStreamError::Internal(
                "tempfile cleanup worker did not flush within 2 seconds".to_owned(),
            )
        })
    })
    .await
    .map_err(|error| MusicStreamError::Internal(format!("tempfile cleanup task failed: {error}")))?
}

impl SourceArtifact {
    #[must_use]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Clone)]
pub struct FileSourceResolver {
    config: SourceResolverConfig,
    resources: SourceRuntimeResources,
    preload: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct SourceRuntimeResources {
    pub cache: SharedSourceArtifactCache,
    pub http_downloads: Arc<Semaphore>,
    pub http_preloads: Arc<Semaphore>,
    pub tempfile_budget: Arc<Semaphore>,
    pub tempfile_preloads: Arc<Semaphore>,
    pub downloads: SharedSourceDownloadRegistry,
}

impl std::fmt::Debug for FileSourceResolver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileSourceResolver")
            .field("config", &self.config)
            .finish()
    }
}

impl FileSourceResolver {
    #[must_use]
    pub fn new(
        config: SourceResolverConfig,
        resources: SourceRuntimeResources,
        preload: bool,
    ) -> Self {
        Self {
            config,
            resources,
            preload,
        }
    }

    pub async fn resolve(
        &self,
        source: &TrackSource,
        gate: &PauseGate,
        cancellation: &CancellationToken,
    ) -> Result<SourceArtifact> {
        if !gate.wait_async(cancellation).await {
            return Err(cancelled_transfer());
        }
        let started = Instant::now();
        let result = match source.kind {
            TrackKind::File => resolve_local_file(source).await,
            TrackKind::Url => self.resolve_url(source, gate, cancellation).await,
            TrackKind::Live => Err(MusicStreamError::Unsupported(
                "live sources are consumed as streams, not artifacts".to_owned(),
            )),
        };
        metrics::histogram!("music_stream.source.resolve_us")
            .record(started.elapsed().as_micros() as f64);
        if result.is_err() {
            metrics::counter!("music_stream.source.resolve_errors").increment(1);
        }
        result
    }

    async fn resolve_url(
        &self,
        source: &TrackSource,
        gate: &PauseGate,
        cancellation: &CancellationToken,
    ) -> Result<SourceArtifact> {
        let key = source.stable_key();
        if let Some(hit) = self
            .resources
            .cache
            .lock()
            .map_err(|_| MusicStreamError::Internal("source cache poisoned".to_owned()))?
            .get(key, self.config.http.max_bytes)
        {
            metrics::counter!("music_stream.source.cache_hit").increment(1);
            return Ok(hit);
        }
        metrics::counter!("music_stream.source.cache_miss").increment(1);
        let flight = self.shared_url_flight(source)?;
        let subscription = flight.subscribe(gate.is_paused(), !self.preload);
        wait_for_shared_artifact(&subscription, gate, cancellation).await
    }

    pub(crate) async fn resolve_url_playback(
        &self,
        source: &TrackSource,
        gate: Arc<PauseGate>,
        cancellation: &CancellationToken,
    ) -> Result<UrlPlaybackSource> {
        let key = source.stable_key();
        if let Some(hit) = self
            .resources
            .cache
            .lock()
            .map_err(|_| MusicStreamError::Internal("source cache poisoned".to_owned()))?
            .get(key, self.config.http.max_bytes)
        {
            metrics::counter!("music_stream.source.cache_hit").increment(1);
            return Ok(UrlPlaybackSource::Cached(hit));
        }
        metrics::counter!("music_stream.source.cache_miss").increment(1);
        let flight = self.shared_url_flight(source)?;
        let subscription = flight.subscribe(gate.is_paused(), !self.preload);
        match wait_for_shared_reader(&subscription, &gate, cancellation).await? {
            SharedReaderReady::Cached(artifact) => Ok(UrlPlaybackSource::Cached(artifact)),
            SharedReaderReady::Reader(reader) => {
                Ok(UrlPlaybackSource::Progressive(ProgressiveUrlSource {
                    reader,
                    terminal: flight.terminal.subscribe(),
                    subscription,
                }))
            }
        }
    }

    fn shared_url_flight(&self, source: &TrackSource) -> Result<Arc<SharedUrlFlight>> {
        let key = download_flight_key(source, &self.config.http);
        let (flight, created) = {
            let mut flights = self.resources.downloads.flights.lock().map_err(|_| {
                MusicStreamError::Internal("source download registry poisoned".to_owned())
            })?;
            self.resources.downloads.prune_dead_if_due(&mut flights);
            if let Some(flight) = flights.get(&key).and_then(Weak::upgrade) {
                metrics::counter!("music_stream.source.shared_download_followers").increment(1);
                (flight, false)
            } else {
                flights.remove(&key);
                let flight = SharedUrlFlight::new();
                flights.insert(key, Arc::downgrade(&flight));
                (flight, true)
            }
        };
        if created {
            spawn_shared_url_transfer(
                &flight,
                SharedUrlTransferSpec {
                    source: source.clone(),
                    config: self.config.http.clone(),
                    resources: self.resources.clone(),
                },
            );
        }
        Ok(flight)
    }
}

enum SharedReaderReady {
    Cached(SourceArtifact),
    Reader(GrowingSpoolReader),
}

async fn wait_for_shared_artifact(
    subscription: &SharedUrlSubscription,
    gate: &PauseGate,
    cancellation: &CancellationToken,
) -> Result<SourceArtifact> {
    let mut terminal = subscription.flight.terminal.subscribe();
    loop {
        sync_shared_subscription(subscription, gate, cancellation).await?;
        if let Some(result) = terminal.borrow().clone() {
            return result;
        }
        tokio::select! {
            _ = cancellation.cancelled() => return Err(cancelled_transfer()),
            _ = gate.wait_for_pause(cancellation) => {}
            changed = terminal.changed() => changed.map_err(|_| {
                MusicStreamError::StreamClosed("shared URL terminal state closed".to_owned())
            })?,
        }
    }
}

async fn wait_for_shared_reader(
    subscription: &SharedUrlSubscription,
    gate: &PauseGate,
    cancellation: &CancellationToken,
) -> Result<SharedReaderReady> {
    let mut reader = subscription.flight.reader.subscribe();
    let mut terminal = subscription.flight.terminal.subscribe();
    loop {
        sync_shared_subscription(subscription, gate, cancellation).await?;
        if let Some(spool) = reader.borrow().clone() {
            let reader = spool
                .open_reader(cancellation.child_token())
                .map_err(|error| MusicStreamError::InvalidSource(error.to_string()))?;
            return Ok(SharedReaderReady::Reader(reader));
        }
        if let Some(result) = terminal.borrow().clone() {
            return result.map(SharedReaderReady::Cached);
        }
        tokio::select! {
            _ = cancellation.cancelled() => return Err(cancelled_transfer()),
            _ = gate.wait_for_pause(cancellation) => {}
            changed = reader.changed() => changed.map_err(|_| {
                MusicStreamError::StreamClosed("shared URL reader state closed".to_owned())
            })?,
            changed = terminal.changed() => changed.map_err(|_| {
                MusicStreamError::StreamClosed("shared URL terminal state closed".to_owned())
            })?,
        }
    }
}

async fn sync_shared_subscription(
    subscription: &SharedUrlSubscription,
    gate: &PauseGate,
    cancellation: &CancellationToken,
) -> Result<()> {
    if gate.is_paused() {
        subscription.set_paused(true);
        if !gate.wait_async(cancellation).await {
            return Err(cancelled_transfer());
        }
    }
    subscription.set_paused(false);
    Ok(())
}

struct SharedUrlTransferSpec {
    source: TrackSource,
    config: HttpSourceConfig,
    resources: SourceRuntimeResources,
}

struct SharedUrlTransferRuntime {
    gate: Arc<PauseGate>,
    cancellation: CancellationToken,
    priority: watch::Receiver<bool>,
    reader: watch::Sender<Option<GrowingSpool>>,
}

fn spawn_shared_url_transfer(flight: &Arc<SharedUrlFlight>, spec: SharedUrlTransferSpec) {
    let reader = flight.reader.clone();
    let terminal = flight.terminal.clone();
    let gate = Arc::clone(&flight.transfer_gate);
    let cancellation = flight.cancellation.clone();
    let priority = flight.current_priority.subscribe();
    let task = supervise_shared_url_transfer(
        run_shared_url_transfer(
            spec,
            SharedUrlTransferRuntime {
                gate,
                cancellation,
                priority,
                reader,
            },
        ),
        terminal,
    );
    flight
        .task
        .lock()
        .expect("shared URL task lock poisoned")
        .replace(task);
}

fn supervise_shared_url_transfer<F>(
    future: F,
    terminal: watch::Sender<Option<Result<SourceArtifact>>>,
) -> SharedUrlTask
where
    F: Future<Output = Result<SourceArtifact>> + Send + 'static,
{
    let worker = tokio::spawn(future);
    let worker_abort = worker.abort_handle();
    let supervisor = tokio::spawn(async move {
        let result = match worker.await {
            Ok(result) => result,
            Err(error) => Err(MusicStreamError::Internal(format!(
                "shared URL transfer task failed: {error}"
            ))),
        };
        terminal.send_replace(Some(result));
    });
    SharedUrlTask {
        supervisor,
        worker_abort,
    }
}

async fn run_shared_url_transfer(
    spec: SharedUrlTransferSpec,
    mut runtime: SharedUrlTransferRuntime,
) -> Result<SourceArtifact> {
    let SharedUrlTransferSpec {
        source,
        config,
        resources,
    } = spec;
    let tempfile_quota = acquire_tempfile_quota_with_priority(
        Arc::clone(&resources.tempfile_budget),
        Arc::clone(&resources.tempfile_preloads),
        config.max_bytes,
        &runtime.gate,
        &runtime.cancellation,
        &mut runtime.priority,
    )
    .await?;
    let admission_started = Instant::now();
    let _download_slot = acquire_download_slot_with_priority(
        Arc::clone(&resources.http_downloads),
        Arc::clone(&resources.http_preloads),
        &runtime.gate,
        &runtime.cancellation,
        &mut runtime.priority,
    )
    .await?;
    metrics::histogram!("music_stream.source.http_admission_wait_us")
        .record(admission_started.elapsed().as_micros() as f64);
    if let Some(hit) = resources
        .cache
        .lock()
        .map_err(|_| MusicStreamError::Internal("source cache poisoned".to_owned()))?
        .get(source.stable_key(), config.max_bytes)
    {
        metrics::counter!("music_stream.source.cache_hit_after_wait").increment(1);
        return Ok(hit);
    }
    let artifact = download_http_artifact_with_writer_and_budget(
        &source,
        &config,
        http_client_for(&source),
        &runtime.gate,
        &runtime.cancellation,
        Some(runtime.reader),
        TempfileAdmission {
            budget: resources.tempfile_budget,
            initial_quota: Some(tempfile_quota),
        },
    )
    .await?;
    metrics::counter!("music_stream.source.http_bytes").increment(artifact.len_bytes);
    if resources
        .cache
        .lock()
        .map_err(|_| MusicStreamError::Internal("source cache poisoned".to_owned()))?
        .insert(artifact.clone())
    {
        metrics::counter!("music_stream.source.cache_inserted").increment(1);
    }
    Ok(artifact)
}

async fn acquire_download_slot(
    slots: Arc<Semaphore>,
    gate: &PauseGate,
    cancellation: &CancellationToken,
) -> Result<OwnedSemaphorePermit> {
    loop {
        if !gate.wait_async(cancellation).await {
            return Err(cancelled_transfer());
        }
        let slot = Arc::clone(&slots).acquire_owned();
        tokio::pin!(slot);
        tokio::select! {
            _ = cancellation.cancelled() => return Err(cancelled_transfer()),
            _ = gate.wait_for_pause(cancellation) => {}
            result = &mut slot => {
                return result.map_err(|_| {
                    MusicStreamError::StreamClosed(
                        "HTTP download admission was closed".to_owned(),
                    )
                });
            }
        }
    }
}

#[derive(Debug)]
struct DownloadSlotPermit {
    _global: OwnedSemaphorePermit,
    _preload: Option<PromotablePreloadPermit>,
}

/// A preload-only admission permit must stop counting against preload capacity
/// as soon as its shared transfer is promoted to current playback. Keeping the
/// permit for the lifetime of the downloaded artifact creates a circular wait:
/// the following preload waits for quota held by the current artifact, while
/// that artifact is retained until the following preload can be promoted.
#[derive(Debug)]
struct PromotablePreloadPermit {
    permit: Arc<Mutex<Option<OwnedSemaphorePermit>>>,
    cancellation: CancellationToken,
}

impl PromotablePreloadPermit {
    fn new(permit: OwnedSemaphorePermit, mut priority: watch::Receiver<bool>) -> Option<Self> {
        if *priority.borrow_and_update() {
            return None;
        }
        let permit = Arc::new(Mutex::new(Some(permit)));
        let cancellation = CancellationToken::new();
        let watcher_permit = Arc::clone(&permit);
        let watcher_cancellation = cancellation.clone();
        tokio::spawn(async move {
            loop {
                if *priority.borrow_and_update() {
                    watcher_permit
                        .lock()
                        .expect("preload permit lock poisoned")
                        .take();
                    return;
                }
                tokio::select! {
                    _ = watcher_cancellation.cancelled() => return,
                    changed = priority.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
            }
        });
        Some(Self {
            permit,
            cancellation,
        })
    }

    fn split(&self, permits: usize) -> Option<OwnedSemaphorePermit> {
        self.permit
            .lock()
            .expect("preload permit lock poisoned")
            .as_mut()
            .and_then(|permit| permit.split(permits))
    }

    fn num_permits(&self) -> usize {
        self.permit
            .lock()
            .expect("preload permit lock poisoned")
            .as_ref()
            .map_or(0, OwnedSemaphorePermit::num_permits)
    }
}

impl Drop for PromotablePreloadPermit {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.permit
            .lock()
            .expect("preload permit lock poisoned")
            .take();
    }
}

async fn acquire_download_slot_with_priority(
    global: Arc<Semaphore>,
    preloads: Arc<Semaphore>,
    gate: &PauseGate,
    cancellation: &CancellationToken,
    priority: &mut watch::Receiver<bool>,
) -> Result<DownloadSlotPermit> {
    let mut preload = if *priority.borrow() {
        None
    } else {
        acquire_preload_permit(preloads, 1, gate, cancellation, priority).await?
    };
    if *priority.borrow() {
        preload = None;
    }
    let global = acquire_download_slot(global, gate, cancellation).await?;
    Ok(DownloadSlotPermit {
        _global: global,
        _preload: preload,
    })
}

async fn acquire_tempfile_quota_with_priority(
    global: Arc<Semaphore>,
    preloads: Arc<Semaphore>,
    bytes: u64,
    gate: &PauseGate,
    cancellation: &CancellationToken,
    priority: &mut watch::Receiver<bool>,
) -> Result<TempfileQuota> {
    let units = tempfile_quota_units(bytes);
    let mut preload = if *priority.borrow() {
        None
    } else {
        acquire_preload_permit(preloads, units, gate, cancellation, priority).await?
    };
    if *priority.borrow() {
        preload = None;
    }
    let global = acquire_tempfile_quota(global, bytes, gate, cancellation)
        .await?
        .global;
    Ok(TempfileQuota { global, preload })
}

async fn acquire_preload_permit(
    slots: Arc<Semaphore>,
    permits: u32,
    gate: &PauseGate,
    cancellation: &CancellationToken,
    priority: &mut watch::Receiver<bool>,
) -> Result<Option<PromotablePreloadPermit>> {
    loop {
        if *priority.borrow_and_update() {
            return Ok(None);
        }
        if !gate.wait_async(cancellation).await {
            return Err(cancelled_transfer());
        }
        let permit = Arc::clone(&slots).acquire_many_owned(permits);
        tokio::pin!(permit);
        tokio::select! {
            _ = cancellation.cancelled() => return Err(cancelled_transfer()),
            _ = gate.wait_for_pause(cancellation) => {}
            changed = priority.changed() => {
                changed.map_err(|_| MusicStreamError::StreamClosed(
                    "shared URL priority state closed".to_owned(),
                ))?;
            }
            result = &mut permit => {
                let permit = result.map_err(|_| MusicStreamError::StreamClosed(
                    "preload source admission was closed".to_owned(),
                ))?;
                return Ok(PromotablePreloadPermit::new(permit, priority.clone()));
            }
        }
    }
}

fn tempfile_quota_units(bytes: u64) -> u32 {
    let units = bytes.div_ceil(TEMPFILE_QUOTA_BYTES);
    u32::try_from(units).expect("validated tempfile byte limit must fit quota units")
}

#[cfg(test)]
fn standalone_tempfile_budget(config: &HttpSourceConfig) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(
        tempfile_quota_units(config.max_bytes) as usize
    ))
}

async fn acquire_tempfile_quota(
    budget: Arc<Semaphore>,
    bytes: u64,
    gate: &PauseGate,
    cancellation: &CancellationToken,
) -> Result<TempfileQuota> {
    let units = tempfile_quota_units(bytes);
    let started = Instant::now();
    loop {
        if !gate.wait_async(cancellation).await {
            return Err(cancelled_transfer());
        }
        let quota = Arc::clone(&budget).acquire_many_owned(units);
        tokio::pin!(quota);
        tokio::select! {
            _ = cancellation.cancelled() => return Err(cancelled_transfer()),
            _ = gate.wait_for_pause(cancellation) => {}
            result = &mut quota => {
                let permit = result.map_err(|_| {
                    MusicStreamError::StreamClosed(
                        "tempfile byte admission was closed".to_owned(),
                    )
                })?;
                metrics::histogram!("music_stream.source.tempfile_admission_wait_us")
                    .record(started.elapsed().as_micros() as f64);
                return Ok(TempfileQuota {
                    global: permit,
                    preload: None,
                });
            }
        }
    }
}

pub(super) fn shared_http_client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new).clone()
}

pub(super) fn http_client_for(source: &TrackSource) -> reqwest::Client {
    match source.network_policy {
        NetworkPolicy::Provider => shared_http_client(),
        NetworkPolicy::PublicOnly => public_http_client(),
    }
}

fn public_http_client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .no_proxy()
                .dns_resolver(PublicDnsResolver)
                .redirect(reqwest::redirect::Policy::custom(|attempt| {
                    if attempt.previous().len() >= 10 {
                        attempt.error("too many public media redirects")
                    } else if is_public_https_url(attempt.url()) {
                        attempt.follow()
                    } else {
                        attempt.error("public media redirect target is not allowed")
                    }
                }))
                .build()
                .expect("public-only HTTP client configuration must be valid")
        })
        .clone()
}

#[derive(Debug)]
struct PublicDnsResolver;

impl reqwest::dns::Resolve for PublicDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let hostname = name.as_str().to_owned();
        Box::pin(async move {
            let addresses = tokio::net::lookup_host((hostname.as_str(), 0))
                .await
                .map_err(boxed_dns_error)?
                .collect::<Vec<_>>();
            if addresses.is_empty() || addresses.iter().any(|address| !is_public_ip(address.ip())) {
                return Err(boxed_dns_error(std::io::Error::other(
                    "public media DNS resolved to a non-global address",
                )));
            }
            Ok(Box::new(addresses.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

fn boxed_dns_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(error)
}

fn is_public_https_url(url: &reqwest::Url) -> bool {
    if url.scheme() != "https"
        || url.username() != ""
        || url.password().is_some()
        || url.port().is_some()
    {
        return false;
    }
    match url
        .host_str()
        .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok())
    {
        Some(address) => is_public_ip(address),
        None => url.host_str().is_some(),
    }
}

pub async fn resolve_local_file(source: &TrackSource) -> Result<SourceArtifact> {
    let path =
        PathBuf::from(source.path.as_deref().ok_or_else(|| {
            MusicStreamError::InvalidSource("file source requires path".to_owned())
        })?);
    let metadata = tokio::fs::metadata(&path)
        .await
        .map_err(|error| MusicStreamError::InvalidSource(error.to_string()))?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(MusicStreamError::InvalidSource(
            "file source must reference a non-empty regular file".to_owned(),
        ));
    }
    Ok(SourceArtifact {
        stable_key: source.stable_key().to_owned(),
        path,
        len_bytes: metadata.len(),
        cacheable: false,
        _cleanup: None,
    })
}

struct TempfileAdmission {
    budget: Arc<Semaphore>,
    initial_quota: Option<TempfileQuota>,
}

#[derive(Debug)]
struct TempfileQuota {
    global: OwnedSemaphorePermit,
    preload: Option<PromotablePreloadPermit>,
}

enum ArtifactFileWriter {
    File(tokio::fs::File),
    Growing(GrowingSpoolWriter),
}

impl ArtifactFileWriter {
    async fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        match self {
            Self::File(file) => file.write_all(bytes).await,
            Self::Growing(writer) => writer.write_all(bytes).await,
        }
    }

    async fn finish(self) -> std::io::Result<()> {
        match self {
            Self::File(mut file) => file.flush().await,
            Self::Growing(writer) => writer.finish().await,
        }
    }
}

#[cfg(test)]
async fn download_http_artifact(
    source: &TrackSource,
    config: &HttpSourceConfig,
    client: reqwest::Client,
    gate: &PauseGate,
    cancellation: &CancellationToken,
) -> Result<SourceArtifact> {
    download_http_artifact_with_budget(
        source,
        config,
        client,
        gate,
        cancellation,
        TempfileAdmission {
            budget: standalone_tempfile_budget(config),
            initial_quota: None,
        },
    )
    .await
}

#[cfg(test)]
async fn download_http_artifact_with_budget(
    source: &TrackSource,
    config: &HttpSourceConfig,
    client: reqwest::Client,
    gate: &PauseGate,
    cancellation: &CancellationToken,
    tempfile: TempfileAdmission,
) -> Result<SourceArtifact> {
    download_http_artifact_with_writer_and_budget(
        source,
        config,
        client,
        gate,
        cancellation,
        None,
        tempfile,
    )
    .await
}

#[cfg(test)]
async fn download_http_artifact_with_writer(
    source: &TrackSource,
    config: &HttpSourceConfig,
    client: reqwest::Client,
    gate: &PauseGate,
    cancellation: &CancellationToken,
    reader_sender: Option<watch::Sender<Option<GrowingSpool>>>,
) -> Result<SourceArtifact> {
    download_http_artifact_with_writer_and_budget(
        source,
        config,
        client,
        gate,
        cancellation,
        reader_sender,
        TempfileAdmission {
            budget: standalone_tempfile_budget(config),
            initial_quota: None,
        },
    )
    .await
}

async fn download_http_artifact_with_writer_and_budget(
    source: &TrackSource,
    config: &HttpSourceConfig,
    client: reqwest::Client,
    gate: &PauseGate,
    cancellation: &CancellationToken,
    reader_sender: Option<watch::Sender<Option<GrowingSpool>>>,
    mut tempfile: TempfileAdmission,
) -> Result<SourceArtifact> {
    let mut attempt = 0_u8;
    loop {
        if !gate.wait_async(cancellation).await {
            return Err(cancelled_transfer());
        }
        let quota = match tempfile.initial_quota.take() {
            Some(quota) => quota,
            None => {
                acquire_tempfile_quota(
                    Arc::clone(&tempfile.budget),
                    config.max_bytes,
                    gate,
                    cancellation,
                )
                .await?
            }
        };
        match download_http_artifact_once(
            source,
            config,
            client.clone(),
            gate,
            cancellation,
            reader_sender.as_ref(),
            quota,
        )
        .await
        {
            Ok(artifact) => return Ok(artifact),
            Err(error)
                if attempt < config.max_retries && error.retryable && !error.reader_published =>
            {
                attempt += 1;
                metrics::counter!("music_stream.source.http_retries").increment(1);
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(cancelled_transfer()),
                    _ = tokio::time::sleep(config.retry_backoff) => {}
                }
            }
            Err(error) => {
                return Err(error.error);
            }
        }
    }
}

async fn download_http_artifact_once(
    source: &TrackSource,
    config: &HttpSourceConfig,
    client: reqwest::Client,
    gate: &PauseGate,
    cancellation: &CancellationToken,
    reader_sender: Option<&watch::Sender<Option<GrowingSpool>>>,
    quota: TempfileQuota,
) -> std::result::Result<SourceArtifact, HttpAttemptError> {
    let url = source.url.as_deref().ok_or_else(|| {
        HttpAttemptError::terminal(MusicStreamError::InvalidSource(
            "URL source requires url".to_owned(),
        ))
    })?;
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(HttpAttemptError::terminal(MusicStreamError::InvalidSource(
            "URL source requires HTTP or HTTPS".to_owned(),
        )));
    }
    if !gate.wait_async(cancellation).await {
        return Err(HttpAttemptError::terminal(cancelled_transfer()));
    }
    let progressive_mode = reader_sender.and_then(|_| progressive_source_mode(source));
    let request_started = Instant::now();
    let transfer_mode = match progressive_mode {
        Some(ProgressiveSourceMode::Immediate) => "progressive",
        Some(ProgressiveSourceMode::FastStartMp4) => "faststart_probe",
        None => "artifact",
    };
    let response = {
        let mut request = client.get(url);
        for (name, value) in &source.headers {
            request = request.header(name, value);
        }
        let response = request.send();
        tokio::pin!(response);
        let deadline = tokio::time::sleep(config.io_timeout);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    return Err(HttpAttemptError::terminal(cancelled_transfer()));
                }
                _ = gate.wait_for_pause(cancellation) => {
                    if !gate.wait_async(cancellation).await {
                        return Err(HttpAttemptError::terminal(cancelled_transfer()));
                    }
                    deadline.as_mut().reset(tokio::time::Instant::now() + config.io_timeout);
                }
                result = &mut response => break result.map_err(HttpAttemptError::from_http)?,
                _ = &mut deadline => {
                    return Err(HttpAttemptError::timeout(
                        "HTTP response did not open before the I/O deadline",
                    ));
                }
            }
        }
    };
    let mut response = response
        .error_for_status()
        .map_err(HttpAttemptError::from_http)?;
    metrics::histogram!(
        "music_stream.source.http_open_us",
        "mode" => transfer_mode
    )
    .record(request_started.elapsed().as_micros() as f64);
    if is_strong_live_http_response(response.headers()) {
        return Err(HttpAttemptError::terminal(
            MusicStreamError::DetectedLiveSource(
                "response contains Icecast/ICY headers".to_owned(),
            ),
        ));
    }
    if is_hls_playlist_response(response.headers(), response.url()) {
        return Err(HttpAttemptError::terminal(
            MusicStreamError::DetectedHlsSource(
                "response is an HLS playlist by content type or final URL".to_owned(),
            ),
        ));
    }
    let declared_length = response.content_length();
    if declared_length.is_some_and(|length| length > config.max_bytes) {
        return Err(HttpAttemptError::terminal(MusicStreamError::InvalidSource(
            "HTTP source exceeds configured byte limit".to_owned(),
        )));
    }

    let suffix = source_suffix(source);
    let tempfile_started = Instant::now();
    let named =
        tokio::task::spawn_blocking(move || tempfile::Builder::new().suffix(&suffix).tempfile())
            .await
            .map_err(|error| {
                HttpAttemptError::terminal(MusicStreamError::Internal(format!(
                    "temporary file worker failed: {error}"
                )))
            })?
            .map_err(|error| {
                HttpAttemptError::terminal(MusicStreamError::InvalidSource(error.to_string()))
            })?;
    metrics::histogram!(
        "music_stream.source.tempfile_create_us",
        "mode" => transfer_mode
    )
    .record(tempfile_started.elapsed().as_micros() as f64);
    let (std_file, temp_path) = named.into_parts();
    let path = temp_path.to_path_buf();
    let cleanup = Arc::new(TempArtifactCleanup::new(temp_path, quota));
    if let Some(declared_length) = declared_length {
        cleanup.shrink_quota_to(declared_length);
    }
    let mut pending_reader = None;
    let mut faststart_probe = None;
    let mut progressive_reader_published = false;
    let mut file = match (reader_sender, progressive_mode) {
        (Some(sender), Some(mode)) => {
            let (writer, spool) = growing_spool(std_file, path.clone(), Arc::clone(&cleanup));
            match mode {
                ProgressiveSourceMode::Immediate => {
                    sender.send_replace(Some(spool));
                    progressive_reader_published = true;
                    metrics::histogram!("music_stream.source.http_to_spool_ready_us")
                        .record(request_started.elapsed().as_micros() as f64);
                }
                ProgressiveSourceMode::FastStartMp4 => {
                    pending_reader = Some((sender, spool));
                    faststart_probe = Some(FastStartProbe::default());
                }
            }
            ArtifactFileWriter::Growing(writer)
        }
        _ => ArtifactFileWriter::File(tokio::fs::File::from_std(std_file)),
    };
    let mut length = 0_u64;
    let mut first_body_byte_recorded = false;
    loop {
        if !gate.wait_async(cancellation).await {
            return Err(HttpAttemptError::terminal(cancelled_transfer()));
        }
        let chunk = {
            let chunk = response.chunk();
            tokio::pin!(chunk);
            let deadline = tokio::time::sleep(config.io_timeout);
            tokio::pin!(deadline);
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        return Err(HttpAttemptError::terminal(cancelled_transfer()));
                    }
                    _ = gate.wait_for_pause(cancellation) => {
                        if !gate.wait_async(cancellation).await {
                            return Err(HttpAttemptError::terminal(cancelled_transfer()));
                        }
                        deadline.as_mut().reset(tokio::time::Instant::now() + config.io_timeout);
                    }
                    result = &mut chunk => {
                        break result.map_err(|error| {
                            HttpAttemptError::from_http(error)
                                .after_reader_published(progressive_reader_published)
                        })?;
                    }
                    _ = &mut deadline => {
                        return Err(HttpAttemptError::timeout(
                            "HTTP body stalled past the I/O deadline",
                        ).after_reader_published(progressive_reader_published));
                    }
                }
            }
        };
        let Some(chunk) = chunk else {
            break;
        };
        if !first_body_byte_recorded && !chunk.is_empty() {
            metrics::histogram!(
                "music_stream.source.http_first_body_byte_us",
                "mode" => transfer_mode
            )
            .record(request_started.elapsed().as_micros() as f64);
            first_body_byte_recorded = true;
        }
        length = length.saturating_add(chunk.len().try_into().unwrap_or(u64::MAX));
        if declared_length.is_some_and(|declared| length > declared) {
            return Err(HttpAttemptError::terminal(MusicStreamError::InvalidSource(
                "HTTP source exceeds its declared Content-Length".to_owned(),
            ))
            .after_reader_published(progressive_reader_published));
        }
        if length > config.max_bytes {
            return Err(HttpAttemptError::terminal(MusicStreamError::InvalidSource(
                "HTTP source exceeds configured byte limit".to_owned(),
            ))
            .after_reader_published(progressive_reader_published));
        }
        file.write_all(&chunk).await.map_err(|error| {
            HttpAttemptError::terminal(MusicStreamError::InvalidSource(error.to_string()))
                .after_reader_published(progressive_reader_published)
        })?;
        if let Some(probe) = faststart_probe.as_mut() {
            match probe.push(&chunk) {
                FastStartDecision::Pending => {}
                FastStartDecision::Progressive => {
                    if let Some((sender, spool)) = pending_reader.take() {
                        sender.send_replace(Some(spool));
                        progressive_reader_published = true;
                        metrics::counter!("music_stream.source.http_faststart_progressive")
                            .increment(1);
                        metrics::histogram!("music_stream.source.http_to_spool_ready_us")
                            .record(request_started.elapsed().as_micros() as f64);
                    }
                    faststart_probe = None;
                }
                FastStartDecision::ArtifactOnly => {
                    pending_reader = None;
                    faststart_probe = None;
                    metrics::counter!("music_stream.source.http_faststart_fallbacks").increment(1);
                }
            }
        }
    }
    if faststart_probe.is_some() {
        metrics::counter!("music_stream.source.http_faststart_fallbacks").increment(1);
    }
    file.finish().await.map_err(|error| {
        HttpAttemptError::terminal(MusicStreamError::InvalidSource(error.to_string()))
            .after_reader_published(progressive_reader_published)
    })?;
    if length == 0 {
        return Err(HttpAttemptError::terminal(MusicStreamError::InvalidSource(
            "HTTP source is empty".to_owned(),
        )));
    }
    cleanup.shrink_quota_to(length);
    Ok(SourceArtifact {
        stable_key: source.stable_key().to_owned(),
        path,
        len_bytes: length,
        cacheable: config.cache_temp_files,
        _cleanup: Some(cleanup),
    })
}

fn source_suffix(source: &TrackSource) -> String {
    source
        .media_format_hint()
        .map_or_else(String::new, |hint| format!(".{hint}"))
}

#[derive(Debug)]
struct HttpAttemptError {
    error: MusicStreamError,
    retryable: bool,
    reader_published: bool,
}

impl HttpAttemptError {
    fn from_http(error: reqwest::Error) -> Self {
        Self {
            retryable: is_retryable_http(&error),
            error: map_http_error(error),
            reader_published: false,
        }
    }

    fn terminal(error: MusicStreamError) -> Self {
        Self {
            error,
            retryable: false,
            reader_published: false,
        }
    }

    fn timeout(message: &str) -> Self {
        Self {
            error: MusicStreamError::SourceTimeout(message.to_owned()),
            retryable: true,
            reader_published: false,
        }
    }

    fn after_reader_published(mut self, reader_published: bool) -> Self {
        self.reader_published = reader_published;
        self
    }
}

fn cancelled_transfer() -> MusicStreamError {
    MusicStreamError::StreamClosed("source transfer was cancelled".to_owned())
}

fn map_http_error(error: reqwest::Error) -> MusicStreamError {
    if error.is_timeout() {
        MusicStreamError::SourceTimeout("HTTP source request timed out".to_owned())
    } else if error
        .status()
        .is_some_and(|status| matches!(status.as_u16(), 401 | 403))
    {
        MusicStreamError::SourceAuthExpired("HTTP source authorization failed".to_owned())
    } else if let Some(status) = error.status() {
        MusicStreamError::InvalidSource(format!("HTTP source returned status {}", status.as_u16()))
    } else if error.is_connect() {
        MusicStreamError::InvalidSource("HTTP source connection failed".to_owned())
    } else if error.is_body() || error.is_decode() {
        MusicStreamError::InvalidSource("HTTP source body failed".to_owned())
    } else {
        MusicStreamError::InvalidSource("HTTP source request failed".to_owned())
    }
}

fn is_retryable_http(error: &reqwest::Error) -> bool {
    if error.is_timeout() {
        return true;
    }
    match error.status().map(|status| status.as_u16()) {
        Some(408 | 429 | 500..=599) | None => true,
        Some(_) => false,
    }
}

fn is_strong_live_http_response(headers: &reqwest::header::HeaderMap) -> bool {
    if headers.keys().any(|name| name.as_str().starts_with("icy-")) {
        return true;
    }
    let served_by_icecast = headers
        .get(reqwest::header::SERVER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|server| server.to_ascii_lowercase().contains("icecast"));
    served_by_icecast
        && headers
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|content_type| {
                let content_type = content_type.to_ascii_lowercase();
                content_type.starts_with("audio/")
                    || content_type.starts_with("application/ogg")
                    || content_type.starts_with("application/octet-stream")
            })
}

fn is_hls_playlist_response(
    headers: &reqwest::header::HeaderMap,
    final_url: &reqwest::Url,
) -> bool {
    let path_is_m3u8 = final_url
        .path_segments()
        .and_then(Iterator::last)
        .and_then(|name| name.rsplit_once('.'))
        .is_some_and(|(_, extension)| extension.eq_ignore_ascii_case("m3u8"));
    let hls_content_type = headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .is_some_and(|mime| {
            matches!(
                mime.to_ascii_lowercase().as_str(),
                "application/vnd.apple.mpegurl"
                    | "application/x-mpegurl"
                    | "application/mpegurl"
                    | "audio/mpegurl"
                    | "audio/x-mpegurl"
            )
        });
    path_is_m3u8 || hls_content_type
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    async fn public_dns_rejects_localhost_resolution() {
        let name = "localhost".parse::<reqwest::dns::Name>().expect("DNS name");
        let result = reqwest::dns::Resolve::resolve(&PublicDnsResolver, name).await;
        assert!(result.is_err());
    }

    #[test]
    fn public_redirect_policy_accepts_only_default_port_https_targets() {
        assert!(is_public_https_url(
            &reqwest::Url::parse("https://media.example/live").expect("public URL")
        ));
        for value in [
            "http://media.example/live",
            "https://127.0.0.1/live",
            "https://media.example:8443/live",
        ] {
            assert!(!is_public_https_url(
                &reqwest::Url::parse(value).expect("test URL")
            ));
        }
    }

    #[test]
    fn live_detection_requires_icecast_or_icy_evidence() {
        let mut icy = reqwest::header::HeaderMap::new();
        icy.insert("icy-name", "Test Radio".parse().expect("icy header"));
        assert!(is_strong_live_http_response(&icy));

        let mut icecast = reqwest::header::HeaderMap::new();
        icecast.insert(
            reqwest::header::SERVER,
            "Icecast 2.4.4".parse().expect("server header"),
        );
        icecast.insert(
            reqwest::header::CONTENT_TYPE,
            "audio/mpeg".parse().expect("content type"),
        );
        assert!(is_strong_live_http_response(&icecast));

        icecast.insert(
            reqwest::header::CONTENT_TYPE,
            "text/html".parse().expect("content type"),
        );
        assert!(!is_strong_live_http_response(&icecast));

        let mut ambiguous = reqwest::header::HeaderMap::new();
        ambiguous.insert(
            reqwest::header::CONTENT_TYPE,
            "audio/mpeg".parse().expect("content type"),
        );
        ambiguous.insert(
            reqwest::header::CACHE_CONTROL,
            "no-cache".parse().expect("cache control"),
        );
        assert!(!is_strong_live_http_response(&ambiguous));
    }

    #[test]
    fn hls_detection_requires_a_playlist_mime_or_final_extension() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            "application/vnd.apple.mpegurl; charset=utf-8"
                .parse()
                .expect("content type"),
        );
        assert!(is_hls_playlist_response(
            &headers,
            &reqwest::Url::parse("https://media.test/signed").expect("URL")
        ));
        assert!(is_hls_playlist_response(
            &reqwest::header::HeaderMap::new(),
            &reqwest::Url::parse("https://cdn.test/audio/INDEX.M3U8?token=1").expect("URL")
        ));

        headers.insert(
            reqwest::header::CONTENT_TYPE,
            "audio/mpeg".parse().expect("content type"),
        );
        assert!(!is_hls_playlist_response(
            &headers,
            &reqwest::Url::parse("https://media.test/signed").expect("URL")
        ));
    }

    #[test]
    fn progressive_policy_uses_mp4_safe_mode_when_hint_and_url_disagree() {
        let mut mp4_url = url_source("https://media.test/audio.mp4".to_owned());
        mp4_url.format_hint = Some("mp3".to_owned());
        assert_eq!(
            progressive_source_mode(&mp4_url),
            Some(ProgressiveSourceMode::FastStartMp4)
        );

        let mut mp4_hint = url_source("https://media.test/audio.mp3".to_owned());
        mp4_hint.format_hint = Some("m4a".to_owned());
        assert_eq!(
            progressive_source_mode(&mp4_hint),
            Some(ProgressiveSourceMode::FastStartMp4)
        );
    }

    #[tokio::test]
    async fn faststart_m4a_decodes_before_http_download_completes() {
        use crate::audio::decode::{DecodePoll, DecoderBackend, SymphoniaStreamDecoder};

        let fixture = faststart_m4a_fixture();
        let split = 1_800;
        assert!(fixture.len() > split);

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let server_fixture = fixture.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = [0_u8; 1_024];
            let _ = stream.read(&mut request).await.expect("request");
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                server_fixture.len()
            );
            stream.write_all(headers.as_bytes()).await.expect("headers");
            stream
                .write_all(&server_fixture[..split])
                .await
                .expect("faststart prefix");
            stream.flush().await.expect("flush prefix");
            let _ = release_rx.await;
            stream
                .write_all(&server_fixture[split..])
                .await
                .expect("faststart suffix");
        });

        let source = url_source(format!("http://{address}/audio.m4a"));
        let config = HttpSourceConfig {
            io_timeout: Duration::from_secs(2),
            max_bytes: TEMPFILE_QUOTA_BYTES,
            max_retries: 0,
            ..HttpSourceConfig::default()
        };
        let gate = Arc::new(PauseGate::default());
        let cancellation = CancellationToken::new();
        let transfer_gate = Arc::clone(&gate);
        let transfer_cancellation = cancellation.clone();
        let (reader_sender, mut reader_receiver) = watch::channel(None);
        let transfer = tokio::spawn(async move {
            download_http_artifact_with_writer(
                &source,
                &config,
                shared_http_client().clone(),
                &transfer_gate,
                &transfer_cancellation,
                Some(reader_sender),
            )
            .await
        });

        tokio::time::timeout(Duration::from_secs(2), reader_receiver.changed())
            .await
            .expect("faststart reader publication timeout")
            .expect("faststart reader channel");
        let spool = reader_receiver
            .borrow()
            .clone()
            .expect("faststart reader was published");
        let reader = spool
            .open_reader(cancellation.child_token())
            .expect("faststart reader");
        let mut decoder = tokio::task::spawn_blocking(move || {
            let mut decoder = SymphoniaStreamDecoder::open(reader, Some("m4a"))?;
            loop {
                match decoder.poll_decode()? {
                    DecodePoll::Chunk(chunk) => return Ok(chunk),
                    DecodePoll::NeedMore => {}
                    DecodePoll::End => {
                        return Err(MusicStreamError::DecodeError(
                            "faststart fixture ended before decoded audio".to_owned(),
                        ));
                    }
                }
            }
        });
        let decoded = tokio::time::timeout(Duration::from_secs(2), &mut decoder).await;
        let _ = release_tx.send(());
        let chunk = decoded
            .expect("M4A decode waited for the HTTP suffix")
            .expect("decoder task")
            .expect("decode faststart M4A");
        assert_eq!(chunk.sample_rate, 44_100);
        assert_eq!(chunk.channels, 1);
        assert!(!chunk.samples_interleaved.is_empty());

        let artifact = transfer.await.expect("transfer task").expect("artifact");
        assert_eq!(artifact.len_bytes, fixture.len() as u64);
        server.await.expect("server");
    }

    #[tokio::test]
    async fn faststart_m4a_retries_only_before_reader_publication() {
        let fixture = faststart_m4a_fixture();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server_fixture = fixture.clone();
        let server = tokio::spawn(async move {
            for attempt in 0..2 {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let mut request = [0_u8; 1_024];
                let _ = stream.read(&mut request).await.expect("request");
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    server_fixture.len()
                );
                stream.write_all(headers.as_bytes()).await.expect("headers");
                if attempt == 0 {
                    stream
                        .write_all(&server_fixture[..4])
                        .await
                        .expect("incomplete prefix");
                } else {
                    stream
                        .write_all(&server_fixture)
                        .await
                        .expect("complete retry");
                }
            }
        });

        let source = url_source(format!("http://{address}/audio.m4a"));
        let config = HttpSourceConfig {
            max_bytes: TEMPFILE_QUOTA_BYTES,
            max_retries: 1,
            retry_backoff: Duration::from_millis(1),
            ..HttpSourceConfig::default()
        };
        let (reader_sender, reader_receiver) = watch::channel(None);
        let artifact = download_http_artifact_with_writer(
            &source,
            &config,
            shared_http_client().clone(),
            &PauseGate::default(),
            &CancellationToken::new(),
            Some(reader_sender),
        )
        .await
        .expect("faststart retry");

        assert!(reader_receiver.borrow().is_some());
        assert_eq!(artifact.len_bytes, fixture.len() as u64);
        server.await.expect("server");
    }

    fn faststart_m4a_fixture() -> Vec<u8> {
        use base64::Engine as _;

        // A 0.5 s mono AAC file generated by FFmpeg with `-movflags +faststart`.
        base64::engine::general_purpose::STANDARD
            .decode(include_str!("../../testdata/faststart-aac.m4a.b64").trim())
            .expect("faststart M4A fixture")
    }

    #[test]
    fn download_registry_periodically_prunes_dead_flight_keys() {
        let registry = SourceDownloadRegistry::default();
        let flight = SharedUrlFlight::new();
        registry
            .flights
            .lock()
            .expect("registry")
            .insert("expired".to_owned(), Arc::downgrade(&flight));
        drop(flight);
        registry.prune_counter.store(64, Ordering::Relaxed);

        let mut flights = registry.flights.lock().expect("registry");
        registry.prune_dead_if_due(&mut flights);
        assert!(flights.is_empty());
    }

    #[test]
    fn download_flights_are_isolated_by_transfer_policy() {
        let source = url_source("https://example.test/audio.mp3".to_owned());
        let first = HttpSourceConfig::default();
        let mut second = first.clone();
        second.max_bytes /= 2;

        assert_ne!(
            download_flight_key(&source, &first),
            download_flight_key(&source, &second)
        );
        let mut authenticated = source.clone();
        authenticated
            .headers
            .insert("referer".to_owned(), "https://example.test/".to_owned());
        assert_ne!(
            download_flight_key(&source, &first),
            download_flight_key(&authenticated, &first)
        );
        let mut public_only = source.clone();
        public_only.network_policy = NetworkPolicy::PublicOnly;
        assert_ne!(
            download_flight_key(&source, &first),
            download_flight_key(&public_only, &first)
        );
        assert_eq!(
            download_flight_key(&source, &first),
            download_flight_key(&source, &first)
        );
    }

    #[test]
    fn artifact_cache_evicts_lru_entries_by_retained_tempfile_quota() {
        let mut cache = SourceArtifactCache::new(2 * TEMPFILE_QUOTA_BYTES);
        assert!(cache.insert(test_artifact("first", TEMPFILE_QUOTA_BYTES + 1)));
        assert!(cache.insert(test_artifact("second", 1)));

        assert_eq!(cache.retained_quota_bytes, TEMPFILE_QUOTA_BYTES);
        assert!(cache.get("first", u64::MAX).is_none());
        assert_eq!(cache.get("second", u64::MAX).expect("cached").len_bytes, 1);
    }

    #[test]
    fn artifact_cache_hit_respects_the_callers_byte_limit() {
        let mut cache = SourceArtifactCache::new(TEMPFILE_QUOTA_BYTES);
        assert!(cache.insert(test_artifact("entry", 6)));

        assert!(cache.get("entry", 5).is_none());
        assert_eq!(cache.get("entry", 6).expect("within limit").len_bytes, 6);
    }

    #[test]
    fn taking_artifact_cache_preserves_capacity_and_empties_source() {
        let mut cache = SourceArtifactCache::new(TEMPFILE_QUOTA_BYTES);
        assert!(cache.insert(test_artifact("entry", 4)));

        let mut taken = cache.take();

        assert_eq!(cache.max_bytes, TEMPFILE_QUOTA_BYTES);
        assert_eq!(cache.retained_quota_bytes, 0);
        assert!(cache.get("entry", u64::MAX).is_none());
        assert_eq!(taken.retained_quota_bytes, TEMPFILE_QUOTA_BYTES);
        assert!(taken.get("entry", u64::MAX).is_some());
    }

    #[tokio::test]
    async fn resolves_non_empty_local_file() {
        let file = tempfile::NamedTempFile::new().expect("temp");
        tokio::fs::write(file.path(), b"audio")
            .await
            .expect("write");
        let source = TrackSource {
            attempt_id: None,
            id: "a".to_owned(),
            kind: TrackKind::File,
            url: None,
            path: Some(file.path().display().to_string()),
            format_hint: None,
            seekable: Some(true),
            headers: Default::default(),
            network_policy: NetworkPolicy::Provider,
        };
        let artifact = resolve_local_file(&source).await.expect("resolve");
        assert_eq!(artifact.len_bytes, 5);
    }

    #[tokio::test]
    async fn tempfile_quota_is_released_only_after_async_file_deletion() {
        let budget = Arc::new(Semaphore::new(1));
        let quota = acquire_tempfile_quota(
            Arc::clone(&budget),
            TEMPFILE_QUOTA_BYTES,
            &PauseGate::default(),
            &CancellationToken::new(),
        )
        .await
        .expect("initial quota");
        let named = tempfile::NamedTempFile::new().expect("tempfile");
        let (file, path) = named.into_parts();
        drop(file);
        let filesystem_path = path.to_path_buf();
        let cleanup = TempArtifactCleanup::new(path, quota);

        let waiting_budget = Arc::clone(&budget);
        let waiter = tokio::spawn(async move {
            acquire_tempfile_quota(
                waiting_budget,
                TEMPFILE_QUOTA_BYTES,
                &PauseGate::default(),
                &CancellationToken::new(),
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        drop(cleanup);
        let second_quota = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("quota was not released")
            .expect("waiter task")
            .expect("second quota");
        assert!(!filesystem_path.exists());
        drop(second_quota);
    }

    #[tokio::test]
    async fn tempfile_cleanup_flush_waits_for_deletion_and_quota_release() {
        let budget = Arc::new(Semaphore::new(1));
        let quota = acquire_tempfile_quota(
            Arc::clone(&budget),
            TEMPFILE_QUOTA_BYTES,
            &PauseGate::default(),
            &CancellationToken::new(),
        )
        .await
        .expect("quota");
        let named = tempfile::NamedTempFile::new().expect("tempfile");
        let (file, path) = named.into_parts();
        drop(file);
        let filesystem_path = path.to_path_buf();

        drop(TempArtifactCleanup::new(path, quota));
        flush_temp_cleanup().await.expect("cleanup flush");

        assert!(!filesystem_path.exists());
        assert_eq!(budget.available_permits(), 1);
    }

    #[tokio::test]
    async fn declared_content_length_releases_excess_worst_case_quota_early() {
        let budget = Arc::new(Semaphore::new(8));
        let quota = acquire_tempfile_quota(
            Arc::clone(&budget),
            8 * TEMPFILE_QUOTA_BYTES,
            &PauseGate::default(),
            &CancellationToken::new(),
        )
        .await
        .expect("quota");
        assert_eq!(budget.available_permits(), 0);
        let named = tempfile::NamedTempFile::new().expect("tempfile");
        let (_file, path) = named.into_parts();
        let cleanup = TempArtifactCleanup::new(path, quota);

        cleanup.shrink_quota_to(TEMPFILE_QUOTA_BYTES);
        assert_eq!(budget.available_permits(), 7);

        drop(cleanup);
        flush_temp_cleanup().await.expect("cleanup");
        assert_eq!(budget.available_permits(), 8);
    }

    #[tokio::test]
    async fn tempfile_pressure_does_not_occupy_an_http_download_slot() {
        let http_downloads = Arc::new(Semaphore::new(1));
        let resolver = FileSourceResolver::new(
            SourceResolverConfig {
                http: HttpSourceConfig {
                    max_bytes: TEMPFILE_QUOTA_BYTES,
                    ..HttpSourceConfig::default()
                },
                ..SourceResolverConfig::default()
            },
            SourceRuntimeResources {
                cache: Arc::new(Mutex::new(SourceArtifactCache::new(TEMPFILE_QUOTA_BYTES))),
                http_downloads: Arc::clone(&http_downloads),
                http_preloads: Arc::new(Semaphore::new(1)),
                tempfile_budget: Arc::new(Semaphore::new(0)),
                tempfile_preloads: Arc::new(Semaphore::new(1)),
                downloads: Arc::new(SourceDownloadRegistry::default()),
            },
            false,
        );
        let source = url_source("http://127.0.0.1:9/audio.mp3".to_owned());
        let gate = Arc::new(PauseGate::default());
        let cancellation = CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            resolver
                .resolve_url_playback(&source, gate, &worker_cancellation)
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert_eq!(http_downloads.available_permits(), 1);

        cancellation.cancel();
        assert!(task.await.expect("resolver task").is_err());
    }

    #[tokio::test]
    async fn bounded_http_does_not_retry_terminal_status() {
        let (url, server) = status_server(vec![(404, b"missing".to_vec())]).await;
        let source = url_source(url);
        let config = HttpSourceConfig {
            max_retries: 2,
            retry_backoff: Duration::from_millis(1),
            ..HttpSourceConfig::default()
        };
        let client = reqwest::Client::new();
        let gate = PauseGate::default();
        let cancellation = CancellationToken::new();

        let error = download_http_artifact(&source, &config, client, &gate, &cancellation)
            .await
            .expect_err("404 must fail");
        assert_eq!(error.code(), crate::error::ErrorCode::InvalidSource);
        assert_eq!(server.await.expect("server"), 1);
    }

    #[tokio::test]
    async fn bounded_http_retries_transient_status_with_a_fresh_artifact() {
        let (url, server) = status_server(vec![(500, Vec::new()), (200, b"audio".to_vec())]).await;
        let source = url_source(url);
        let config = HttpSourceConfig {
            max_retries: 2,
            retry_backoff: Duration::from_millis(1),
            ..HttpSourceConfig::default()
        };
        let client = reqwest::Client::new();
        let gate = PauseGate::default();
        let cancellation = CancellationToken::new();

        let artifact = download_http_artifact(&source, &config, client, &gate, &cancellation)
            .await
            .expect("retry succeeds");
        assert_eq!(
            tokio::fs::read(artifact.path()).await.expect("artifact"),
            b"audio"
        );
        assert_eq!(server.await.expect("server"), 2);
    }

    #[tokio::test]
    async fn progressive_http_never_retries_after_delivering_body_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let connections = Arc::new(AtomicUsize::new(0));
        let server_connections = Arc::clone(&connections);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            server_connections.fetch_add(1, Ordering::Relaxed);
            let mut request = [0_u8; 1_024];
            let _ = stream.read(&mut request).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nopus")
                .await
                .expect("partial body");
            drop(stream);
            if tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_ok()
            {
                server_connections.fetch_add(1, Ordering::Relaxed);
            }
        });
        let source = url_source(format!("http://{address}/audio.opus"));
        let config = HttpSourceConfig {
            max_retries: 2,
            retry_backoff: Duration::from_millis(1),
            ..HttpSourceConfig::default()
        };
        let (reader_tx, reader_rx) = watch::channel(None);

        let result = download_http_artifact_with_writer(
            &source,
            &config,
            reqwest::Client::new(),
            &PauseGate::default(),
            &CancellationToken::new(),
            Some(reader_tx),
        )
        .await;

        assert!(result.is_err());
        let mut reader = reader_rx
            .borrow()
            .clone()
            .expect("spool")
            .open_reader(CancellationToken::new())
            .expect("spool reader");
        let reader_task = tokio::task::spawn_blocking(move || {
            let mut output = Vec::new();
            let result = reader.read_to_end(&mut output);
            (output, result)
        });
        let (output, read_result) = reader_task.await.expect("reader");
        assert_eq!(output, b"opus");
        assert!(read_result.is_err());
        server.await.expect("server");
        assert_eq!(connections.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn concurrent_artifact_resolvers_share_one_flight() {
        let (url, server) = status_server(vec![(200, b"audio".to_vec())]).await;
        let source = url_source(url);
        let config = SourceResolverConfig {
            http: HttpSourceConfig {
                cache_temp_files: true,
                max_retries: 0,
                ..HttpSourceConfig::default()
            },
            ..SourceResolverConfig::default()
        };
        let resolver = FileSourceResolver::new(
            config,
            SourceRuntimeResources {
                cache: Arc::new(Mutex::new(SourceArtifactCache::new(TEMPFILE_QUOTA_BYTES))),
                http_downloads: Arc::new(Semaphore::new(2)),
                http_preloads: Arc::new(Semaphore::new(1)),
                // One worst-case reservation remains available while the first
                // completed artifact is retained by the cache/caller.
                tempfile_budget: Arc::new(Semaphore::new(512)),
                tempfile_preloads: Arc::new(Semaphore::new(256)),
                downloads: Arc::new(SourceDownloadRegistry::default()),
            },
            false,
        );
        let gate = PauseGate::default();
        let first_cancellation = CancellationToken::new();
        let second_cancellation = CancellationToken::new();

        let (first, second) = tokio::join!(
            resolver.resolve(&source, &gate, &first_cancellation),
            resolver.resolve(&source, &gate, &second_cancellation),
        );

        assert_eq!(
            tokio::fs::read(first.expect("first artifact").path())
                .await
                .expect("first bytes"),
            b"audio"
        );
        assert_eq!(
            tokio::fs::read(second.expect("second artifact").path())
                .await
                .expect("second bytes"),
            b"audio"
        );
        assert_eq!(server.await.expect("server"), 1);
    }

    #[tokio::test]
    async fn completed_progressive_download_is_promoted_to_artifact_cache() {
        let (url, server) = status_server(vec![(200, b"audio".to_vec())]).await;
        let mut source = url_source(url);
        source.format_hint = Some("mp3".to_owned());
        let config = SourceResolverConfig {
            http: HttpSourceConfig {
                cache_temp_files: true,
                max_retries: 0,
                ..HttpSourceConfig::default()
            },
            ..SourceResolverConfig::default()
        };
        let resolver = FileSourceResolver::new(
            config,
            SourceRuntimeResources {
                cache: Arc::new(Mutex::new(SourceArtifactCache::new(TEMPFILE_QUOTA_BYTES))),
                http_downloads: Arc::new(Semaphore::new(1)),
                http_preloads: Arc::new(Semaphore::new(1)),
                tempfile_budget: Arc::new(Semaphore::new(256)),
                tempfile_preloads: Arc::new(Semaphore::new(256)),
                downloads: Arc::new(SourceDownloadRegistry::default()),
            },
            false,
        );
        let gate = Arc::new(PauseGate::default());
        let cancellation = CancellationToken::new();

        let progressive = resolver
            .resolve_url_playback(&source, Arc::clone(&gate), &cancellation)
            .await
            .expect("progressive source");
        let UrlPlaybackSource::Progressive(progressive) = progressive else {
            panic!("first resolution must stream");
        };
        let mut terminal = progressive.terminal;
        let artifact = loop {
            if let Some(result) = terminal.borrow().clone() {
                break result.expect("download");
            }
            terminal.changed().await.expect("terminal state");
        };
        assert_eq!(
            tokio::fs::read(artifact.path()).await.expect("artifact"),
            b"audio"
        );

        let cached = resolver
            .resolve_url_playback(&source, gate, &CancellationToken::new())
            .await
            .expect("cached source");
        assert!(matches!(cached, UrlPlaybackSource::Cached(_)));
        assert_eq!(server.await.expect("server"), 1);
    }

    #[test]
    fn shared_transfer_pauses_only_when_every_subscriber_is_paused() {
        let flight = SharedUrlFlight::new();
        let first = flight.subscribe(false, true);
        let second = flight.subscribe(false, true);
        assert!(!flight.transfer_gate.is_paused());

        first.control().pause();
        assert!(!flight.transfer_gate.is_paused());
        second.control().pause();
        assert!(flight.transfer_gate.is_paused());

        first.control().resume();
        assert!(!flight.transfer_gate.is_paused());
        drop(first);
        assert!(flight.transfer_gate.is_paused());
        assert!(!flight.cancellation.is_cancelled());
        drop(second);
        assert!(flight.cancellation.is_cancelled());
    }

    #[tokio::test]
    async fn current_download_keeps_an_http_slot_when_preloads_are_saturated() {
        let global = Arc::new(Semaphore::new(2));
        let preloads = Arc::new(Semaphore::new(1));
        let gate = PauseGate::default();
        let cancellation = CancellationToken::new();
        let (_preload_priority_tx, mut preload_priority) = watch::channel(false);
        let _first_preload = acquire_download_slot_with_priority(
            Arc::clone(&global),
            Arc::clone(&preloads),
            &gate,
            &cancellation,
            &mut preload_priority,
        )
        .await
        .expect("first preload");
        let waiting_global = Arc::clone(&global);
        let waiting_preloads = Arc::clone(&preloads);
        let waiting_cancellation = CancellationToken::new();
        let cancel_waiter = waiting_cancellation.clone();
        let blocked_preload = tokio::spawn(async move {
            let (_priority_tx, mut priority) = watch::channel(false);
            acquire_download_slot_with_priority(
                waiting_global,
                waiting_preloads,
                &PauseGate::default(),
                &waiting_cancellation,
                &mut priority,
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!blocked_preload.is_finished());

        let (_current_priority_tx, mut current_priority) = watch::channel(true);
        let _current = tokio::time::timeout(
            Duration::from_millis(100),
            acquire_download_slot_with_priority(
                global,
                preloads,
                &gate,
                &cancellation,
                &mut current_priority,
            ),
        )
        .await
        .expect("current HTTP admission timeout")
        .expect("current HTTP admission");

        cancel_waiter.cancel();
        assert!(blocked_preload.await.expect("preload task").is_err());
    }

    #[tokio::test]
    async fn promoted_download_releases_preload_admission_before_artifact_drop() {
        let global = Arc::new(Semaphore::new(2));
        let preloads = Arc::new(Semaphore::new(1));
        let gate = PauseGate::default();
        let cancellation = CancellationToken::new();
        let (priority_tx, mut priority) = watch::channel(false);
        let admission = acquire_download_slot_with_priority(
            Arc::clone(&global),
            Arc::clone(&preloads),
            &gate,
            &cancellation,
            &mut priority,
        )
        .await
        .expect("preload admission");
        assert_eq!(global.available_permits(), 1);
        assert_eq!(preloads.available_permits(), 0);

        priority_tx.send_replace(true);
        tokio::time::timeout(Duration::from_millis(100), async {
            while preloads.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("promotion did not release preload admission");

        assert_eq!(preloads.available_permits(), 1);
        assert_eq!(global.available_permits(), 1);
        drop(admission);
        assert_eq!(global.available_permits(), 2);
    }

    #[tokio::test]
    async fn joining_current_promotes_a_preload_waiting_for_tempfile_quota() {
        let global = Arc::new(Semaphore::new(1));
        let preloads = Arc::new(Semaphore::new(0));
        let gate = Arc::new(PauseGate::default());
        let cancellation = CancellationToken::new();
        let (priority_tx, mut priority) = watch::channel(false);
        let waiting_gate = Arc::clone(&gate);
        let waiting_cancellation = cancellation.clone();
        let waiter = tokio::spawn(async move {
            acquire_tempfile_quota_with_priority(
                global,
                preloads,
                TEMPFILE_QUOTA_BYTES,
                &waiting_gate,
                &waiting_cancellation,
                &mut priority,
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        priority_tx.send_replace(true);
        let quota = tokio::time::timeout(Duration::from_millis(100), waiter)
            .await
            .expect("promoted tempfile admission timeout")
            .expect("waiter task")
            .expect("promoted tempfile admission");
        assert!(quota.preload.is_none());
    }

    #[tokio::test]
    async fn shared_transfer_panic_is_published_to_subscribers() {
        let (terminal, receiver) = watch::channel(None);
        let task = supervise_shared_url_transfer(
            async { panic!("injected shared transfer panic") },
            terminal,
        );
        let SharedUrlTask {
            supervisor,
            worker_abort: _,
        } = task;
        supervisor.await.expect("supervisor task");

        let error = receiver
            .borrow()
            .clone()
            .expect("terminal result")
            .expect_err("panic must fail");
        assert_eq!(error.code(), crate::error::ErrorCode::Internal);
        assert!(
            error
                .to_string()
                .contains("shared URL transfer task failed")
        );
    }

    #[tokio::test]
    async fn concurrent_progressive_subscribers_share_one_transfer_and_cancel_independently() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let connections = Arc::new(AtomicUsize::new(0));
        let server_connections = Arc::clone(&connections);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            server_connections.fetch_add(1, Ordering::Relaxed);
            let mut request = [0_u8; 1_024];
            let _ = stream.read(&mut request).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nabc")
                .await
                .expect("prefix");
            let _ = release_rx.await;
            stream.write_all(b"def").await.expect("suffix");
            drop(stream);
            if tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_ok()
            {
                server_connections.fetch_add(1, Ordering::Relaxed);
            }
        });
        let source = url_source(format!("http://{address}/audio.mp3"));
        let registry = Arc::new(SourceDownloadRegistry::default());
        let resolver = FileSourceResolver::new(
            SourceResolverConfig {
                http: HttpSourceConfig {
                    max_bytes: TEMPFILE_QUOTA_BYTES,
                    cache_temp_files: true,
                    max_retries: 0,
                    ..HttpSourceConfig::default()
                },
                ..SourceResolverConfig::default()
            },
            SourceRuntimeResources {
                cache: Arc::new(Mutex::new(SourceArtifactCache::new(TEMPFILE_QUOTA_BYTES))),
                http_downloads: Arc::new(Semaphore::new(2)),
                http_preloads: Arc::new(Semaphore::new(2)),
                tempfile_budget: Arc::new(Semaphore::new(2)),
                tempfile_preloads: Arc::new(Semaphore::new(1)),
                downloads: registry,
            },
            false,
        );
        let first_gate = Arc::new(PauseGate::default());
        let second_gate = Arc::new(PauseGate::default());
        let cancellation = CancellationToken::new();
        let (first, second) = tokio::join!(
            resolver.resolve_url_playback(&source, Arc::clone(&first_gate), &cancellation,),
            resolver.resolve_url_playback(&source, Arc::clone(&second_gate), &cancellation,),
        );
        let UrlPlaybackSource::Progressive(first) = first.expect("first subscriber") else {
            panic!("first subscriber must be progressive");
        };
        let UrlPlaybackSource::Progressive(mut second) = second.expect("second subscriber") else {
            panic!("second subscriber must be progressive");
        };

        first.subscription.control().pause();
        drop(first);
        release_tx.send(()).expect("release server");
        let bytes = tokio::task::spawn_blocking(move || {
            let mut bytes = Vec::new();
            second
                .reader
                .read_to_end(&mut bytes)
                .expect("shared reader");
            bytes
        })
        .await
        .expect("reader task");

        assert_eq!(bytes, b"abcdef");
        server.await.expect("server");
        assert_eq!(connections.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn paused_bounded_http_opens_no_connection_until_resume() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let source = url_source(format!("http://{address}/audio"));
        let config = HttpSourceConfig {
            io_timeout: Duration::from_secs(1),
            max_retries: 0,
            ..HttpSourceConfig::default()
        };
        let gate = Arc::new(PauseGate::default());
        gate.pause();
        let transfer_gate = Arc::clone(&gate);
        let cancellation = CancellationToken::new();
        let transfer_cancellation = cancellation.clone();
        let transfer = tokio::spawn(async move {
            download_http_artifact(
                &source,
                &config,
                reqwest::Client::new(),
                &transfer_gate,
                &transfer_cancellation,
            )
            .await
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(40), listener.accept())
                .await
                .is_err()
        );
        gate.resume();
        let (mut stream, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("connection timeout")
            .expect("accept");
        let mut request = [0_u8; 1_024];
        let _ = stream.read(&mut request).await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\naudio")
            .await
            .expect("response");
        drop(stream);

        let artifact = transfer.await.expect("transfer task").expect("artifact");
        assert_eq!(
            tokio::fs::read(artifact.path()).await.expect("read"),
            b"audio"
        );
    }

    #[tokio::test]
    async fn bounded_http_sends_validated_per_source_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = [0_u8; 2_048];
            let length = stream.read(&mut request).await.expect("request");
            let request = String::from_utf8_lossy(&request[..length]).to_ascii_lowercase();
            assert!(request.contains("referer: https://www.example.test/"));
            assert!(request.contains("user-agent: rhythm-test"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\naudio")
                .await
                .expect("response");
        });
        let mut source = url_source(format!("http://{address}/audio"));
        source.headers.extend([
            ("referer".to_owned(), "https://www.example.test/".to_owned()),
            ("user-agent".to_owned(), "rhythm-test".to_owned()),
        ]);
        source.validate().expect("source headers");

        let artifact = download_http_artifact(
            &source,
            &HttpSourceConfig::default(),
            reqwest::Client::new(),
            &PauseGate::default(),
            &CancellationToken::new(),
        )
        .await
        .expect("download");

        assert_eq!(
            tokio::fs::read(artifact.path()).await.expect("read"),
            b"audio"
        );
        server.await.expect("server");
    }

    fn url_source(url: String) -> TrackSource {
        TrackSource {
            attempt_id: None,
            id: "http-test".to_owned(),
            kind: TrackKind::Url,
            url: Some(url),
            path: None,
            format_hint: None,
            seekable: Some(true),
            headers: Default::default(),
            network_policy: NetworkPolicy::Provider,
        }
    }

    fn test_artifact(key: &str, len_bytes: u64) -> SourceArtifact {
        SourceArtifact {
            stable_key: key.to_owned(),
            path: PathBuf::from(key),
            len_bytes,
            cacheable: true,
            _cleanup: None,
        }
    }

    async fn status_server(
        responses: Vec<(u16, Vec<u8>)>,
    ) -> (String, tokio::task::JoinHandle<usize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let task = tokio::spawn(async move {
            let mut connections = 0;
            loop {
                let accepted =
                    tokio::time::timeout(Duration::from_millis(100), listener.accept()).await;
                let Ok(Ok((mut stream, _))) = accepted else {
                    return connections;
                };
                let index = connections.min(responses.len().saturating_sub(1));
                let (status, body) = &responses[index];
                connections += 1;
                let mut request = [0_u8; 1_024];
                let _ = stream.read(&mut request).await;
                let response = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("response headers");
                stream.write_all(body).await.expect("response body");
            }
        });
        (format!("http://{address}/audio"), task)
    }
}
