use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio_util::sync::CancellationToken;

mod convert;
mod events;
mod types;
use convert::{replay_gain_from_input, source_config_from_input};
use events::{
    EventCallback, event_belongs_to, event_output_from_parts, internal_error_event, push_events,
    push_events_checked,
};
use types::*;

use music_stream::{
    ActorOutput, ErrorCode, FileSourceResolver, GainLevel, GenerationTaskSlot, LocalFilePreload,
    LocalFilePreloadCompletion, LocalFileRtpPlayback, LocalFileRtpPlaybackConfig,
    LocalFileRtpPlaybackProgress, MusicStreamError, PlayState, RtpTransportConfig,
    SharedSourceArtifactCache, SourceArtifactCache, SourceResolverConfig, StreamActorMailbox,
    StreamActorMailboxHandle, StreamActorMailboxReply, StreamCommand, StreamEvent, TaskAction,
    TrackSource, VolumeLevel, WorkerEvent, recommend_replay_gain, spawn_live_stream_rtp_playback,
    spawn_local_file_preload, spawn_local_file_rtp_playback,
    spawn_local_file_rtp_playback_from_driver,
};

type Result<T> = std::result::Result<T, Error<String>>;
type ActorRegistry = HashMap<String, StreamActorMailbox>;
type InactiveStatusRegistry = HashMap<String, music_stream::StreamStatus>;
type TaskRegistry<T> = HashMap<String, GenerationTaskSlot<T>>;
type PlaybackRegistry = TaskRegistry<LocalFileRtpPlayback>;
type PreloadRegistry = TaskRegistry<PreloadRuntime>;
type PromotionRegistry = TaskRegistry<PromotionRuntime>;

const PLAYBACK_STOP_JOIN_US_METRIC: &str = "music_stream.napi.playback.stop_join_us";
const PLAYBACK_STOP_JOIN_ERRORS_METRIC: &str = "music_stream.napi.playback.stop_join_errors";
const PLAYBACK_REAP_JOIN_US_METRIC: &str = "music_stream.napi.playback.reap_join_us";
const PLAYBACK_REAP_JOIN_ERRORS_METRIC: &str = "music_stream.napi.playback.reap_join_errors";
const PRELOAD_STOP_JOIN_US_METRIC: &str = "music_stream.napi.preload.stop_join_us";
const PRELOAD_STOP_JOIN_ERRORS_METRIC: &str = "music_stream.napi.preload.stop_join_errors";
const WORKER_EVENT_ENQUEUE_ERRORS_METRIC: &str = "music_stream.napi.worker_event.enqueue_errors";
const WORKER_EVENT_REPLY_ERRORS_METRIC: &str = "music_stream.napi.worker_event.reply_errors";
const WORKER_EVENT_STALE_OUTPUTS_METRIC: &str = "music_stream.napi.worker_event.stale_outputs";
const PROMOTION_WAITER_DUPLICATES_METRIC: &str = "music_stream.napi.promotion.waiter_duplicates";

#[derive(Debug)]
struct PreloadRuntime {
    preload: LocalFilePreload,
    completion: LocalFilePreloadCompletion,
    config: LocalFileRtpPlaybackConfig,
}

#[derive(Debug)]
struct PromotionRuntime {
    token: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl PromotionRuntime {
    fn new(token: CancellationToken, task: tokio::task::JoinHandle<()>) -> Self {
        Self { token, task }
    }

    fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    fn abort(self) {
        self.token.cancel();
        self.task.abort();
    }
}

impl PreloadRuntime {
    fn stop(&self) {
        self.preload.stop();
    }

    fn join_for_promotion(
        self,
    ) -> Result<(
        music_stream::LocalFilePreloadReport,
        LocalFileRtpPlaybackConfig,
    )> {
        let Self {
            preload, config, ..
        } = self;
        let report = preload.join().map_err(to_napi_error)?;
        Ok((report, config))
    }

    fn stop_join(self) -> Result<()> {
        self.preload.stop();
        let started = Instant::now();
        let result = self.preload.join().map_err(to_napi_error);
        record_join_latency(PRELOAD_STOP_JOIN_US_METRIC, started.elapsed());
        if result.is_err() {
            metrics::counter!(PRELOAD_STOP_JOIN_ERRORS_METRIC).increment(1);
        }
        result.map(|_| ())
    }

    fn stop_join_best_effort(self) {
        let started = Instant::now();
        if self.preload.join().is_err() {
            metrics::counter!(PRELOAD_STOP_JOIN_ERRORS_METRIC).increment(1);
        }
        record_join_latency(PRELOAD_STOP_JOIN_US_METRIC, started.elapsed());
    }
}

struct RuntimeHandles<'a> {
    tokio: &'a Arc<tokio::runtime::Runtime>,
    actor_handle: StreamActorMailboxHandle,
    playbacks: &'a Arc<RwLock<PlaybackRegistry>>,
    preloads: &'a Arc<RwLock<PreloadRegistry>>,
    promotions: &'a Arc<RwLock<PromotionRegistry>>,
    events: &'a Arc<RwLock<Vec<StreamEvent>>>,
    event_callback: &'a Arc<RwLock<Option<EventCallback>>>,
    transports: &'a Arc<RwLock<HashMap<String, RtpTransportConfig>>>,
    source_configs: &'a Arc<RwLock<HashMap<String, SourceResolverConfig>>>,
    source_cache: &'a SharedSourceArtifactCache,
    transport: &'a RtpTransportConfig,
    source_config: &'a SourceResolverConfig,
}

#[derive(Clone)]
struct RuntimeCallbackContext {
    stream_id: String,
    tokio: Arc<tokio::runtime::Runtime>,
    actor_handle: StreamActorMailboxHandle,
    playbacks: Arc<RwLock<PlaybackRegistry>>,
    preloads: Arc<RwLock<PreloadRegistry>>,
    promotions: Arc<RwLock<PromotionRegistry>>,
    events: Arc<RwLock<Vec<StreamEvent>>>,
    event_callback: Arc<RwLock<Option<EventCallback>>>,
    transports: Arc<RwLock<HashMap<String, RtpTransportConfig>>>,
    source_configs: Arc<RwLock<HashMap<String, SourceResolverConfig>>>,
    source_cache: SharedSourceArtifactCache,
    transport: RtpTransportConfig,
    source_config: SourceResolverConfig,
}

impl RuntimeCallbackContext {
    fn from_handles(stream_id: &str, handles: &RuntimeHandles<'_>) -> Self {
        Self {
            stream_id: stream_id.to_owned(),
            tokio: Arc::clone(handles.tokio),
            actor_handle: handles.actor_handle.clone(),
            playbacks: Arc::clone(handles.playbacks),
            preloads: Arc::clone(handles.preloads),
            promotions: Arc::clone(handles.promotions),
            events: Arc::clone(handles.events),
            event_callback: Arc::clone(handles.event_callback),
            transports: Arc::clone(handles.transports),
            source_configs: Arc::clone(handles.source_configs),
            source_cache: Arc::clone(handles.source_cache),
            transport: handles.transport.clone(),
            source_config: handles.source_config.clone(),
        }
    }

    fn handle_worker_event(&self, event: WorkerEvent) {
        let reply = match self.actor_handle.try_send_worker_event(event) {
            Ok(reply) => reply,
            Err(_) => {
                metrics::counter!(WORKER_EVENT_ENQUEUE_ERRORS_METRIC).increment(1);
                return;
            }
        };
        self.clone().spawn_actor_reply(reply);
    }

    fn spawn_actor_reply(self, reply: StreamActorMailboxReply<ActorOutput>) {
        let tokio = Arc::clone(&self.tokio);
        tokio.spawn(async move {
            match reply.receive().await {
                Ok(output) => self.handle_actor_output_if_current(output).await,
                Err(_) => metrics::counter!(WORKER_EVENT_REPLY_ERRORS_METRIC).increment(1),
            }
        });
    }

    async fn handle_actor_output_if_current(&self, output: ActorOutput) {
        match self.actor_handle.status().await {
            Ok(status)
                if status.generation == output.status.generation
                    && status.play_state == output.status.play_state =>
            {
                self.handle_actor_output(with_current_status(output, status));
            }
            Ok(_) => {
                metrics::counter!(WORKER_EVENT_STALE_OUTPUTS_METRIC).increment(1);
            }
            Err(_) => {
                metrics::counter!(WORKER_EVENT_REPLY_ERRORS_METRIC).increment(1);
            }
        }
    }

    fn handle_actor_output(&self, output: ActorOutput) {
        let handles = RuntimeHandles {
            tokio: &self.tokio,
            actor_handle: self.actor_handle.clone(),
            playbacks: &self.playbacks,
            preloads: &self.preloads,
            promotions: &self.promotions,
            events: &self.events,
            event_callback: &self.event_callback,
            transports: &self.transports,
            source_configs: &self.source_configs,
            source_cache: &self.source_cache,
            transport: &self.transport,
            source_config: &self.source_config,
        };
        handle_actor_output(&self.stream_id, output, &handles);
    }
}

fn with_current_status(output: ActorOutput, status: music_stream::StreamStatus) -> ActorOutput {
    ActorOutput {
        actions: output.actions,
        events: output
            .events
            .into_iter()
            .map(|event| match event {
                StreamEvent::StateChanged { .. } => StreamEvent::StateChanged {
                    status: status.clone(),
                },
                event => event,
            })
            .collect(),
        status,
    }
}

#[napi]
pub struct Streamer {
    tokio: Arc<tokio::runtime::Runtime>,
    actors: Arc<RwLock<ActorRegistry>>,
    inactive_statuses: Arc<RwLock<InactiveStatusRegistry>>,
    playbacks: Arc<RwLock<PlaybackRegistry>>,
    preloads: Arc<RwLock<PreloadRegistry>>,
    promotions: Arc<RwLock<PromotionRegistry>>,
    transports: Arc<RwLock<HashMap<String, RtpTransportConfig>>>,
    source_configs: Arc<RwLock<HashMap<String, SourceResolverConfig>>>,
    source_cache: SharedSourceArtifactCache,
    events: Arc<RwLock<Vec<StreamEvent>>>,
    event_callback: Arc<RwLock<Option<EventCallback>>>,
}

#[napi]
impl Streamer {
    #[napi(constructor)]
    pub fn new() -> Result<Self> {
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("music-stream-tokio")
            .build()
            .map_err(|error| {
                to_napi_error(MusicStreamError::Internal(format!(
                    "failed to create music stream Tokio runtime: {error}"
                )))
            })?;
        Ok(Self {
            tokio: Arc::new(tokio),
            actors: Arc::new(RwLock::new(HashMap::new())),
            inactive_statuses: Arc::new(RwLock::new(HashMap::new())),
            playbacks: Arc::new(RwLock::new(HashMap::new())),
            preloads: Arc::new(RwLock::new(HashMap::new())),
            promotions: Arc::new(RwLock::new(HashMap::new())),
            transports: Arc::new(RwLock::new(HashMap::new())),
            source_configs: Arc::new(RwLock::new(HashMap::new())),
            source_cache: Arc::new(Mutex::new(SourceArtifactCache::default())),
            events: Arc::new(RwLock::new(Vec::new())),
            event_callback: Arc::new(RwLock::new(None)),
        })
    }

    #[napi]
    pub fn start_stream(&self, options: StartStreamInput) -> Result<StreamStatusOutput> {
        self.reap_finished_playbacks()?;
        let stream_id = options.stream_id;
        let current = TrackSource::try_from(options.current).map_err(to_napi_error)?;
        let next = options
            .next
            .map(TrackSource::try_from)
            .transpose()
            .map_err(to_napi_error)?;
        let transport = RtpTransportConfig::try_from(options.transport).map_err(to_napi_error)?;
        let source_config = source_config_from_input(options.source).map_err(to_napi_error)?;
        let volume =
            VolumeLevel::from_unit(options.volume.unwrap_or(1.0) as f32).map_err(to_napi_error)?;
        let gain =
            GainLevel::from_db(options.gain_db.unwrap_or(0.0) as f32).map_err(to_napi_error)?;

        self.remove_reusable_inactive_stream(&stream_id)?;
        self.inactive_statuses
            .write()
            .map_err(lock_error)?
            .remove(&stream_id);
        if self
            .actors
            .read()
            .map_err(lock_error)?
            .contains_key(&stream_id)
        {
            return Err(to_napi_error(MusicStreamError::StreamAlreadyExists(
                stream_id.clone(),
            )));
        }

        let mailbox = self
            .spawn_actor_mailbox(stream_id.clone(), Some(current), next)
            .inspect_err(|_| {
                let _ = self.remove_actor_best_effort(&stream_id);
            })?;
        let actor_handle = mailbox.handle();
        self.actors
            .write()
            .map_err(lock_error)?
            .insert(stream_id.clone(), mailbox);
        let mut startup_events = Vec::new();
        if volume != VolumeLevel::default() {
            match self.actor_command_with_handle(&actor_handle, StreamCommand::SetVolume { volume })
            {
                Ok(output) => startup_events.extend(output.events),
                Err(error) => {
                    let _ = self.remove_actor_best_effort(&stream_id);
                    return Err(to_napi_error(error));
                }
            }
        }
        if gain != GainLevel::default() {
            match self.actor_command_with_handle(&actor_handle, StreamCommand::SetGain { gain }) {
                Ok(output) => startup_events.extend(output.events),
                Err(error) => {
                    let _ = self.remove_actor_best_effort(&stream_id);
                    return Err(to_napi_error(error));
                }
            }
        }
        let play_output = match self.actor_command_with_handle(&actor_handle, StreamCommand::Play) {
            Ok(output) => output,
            Err(error) => {
                let _ = self.remove_actor_best_effort(&stream_id);
                return Err(to_napi_error(error));
            }
        };

        let (generation, track) = match find_start_current(&play_output.actions) {
            Some(start) => start,
            None => {
                let _ = self.remove_actor_best_effort(&stream_id);
                return Err(to_napi_error(MusicStreamError::Internal(
                    "play command did not start a current track".to_owned(),
                )));
            }
        };

        let track_id = track.id.clone();
        let mut config = playback_config(
            generation,
            transport.clone(),
            source_config.clone(),
            &self.source_cache,
        );
        config.gain = gain;
        let handles = match self.runtime_handles(&stream_id, &transport, &source_config) {
            Ok(handles) => handles,
            Err(error) => {
                let _ = self.remove_actor_best_effort(&stream_id);
                return Err(error);
            }
        };
        let playback =
            match spawn_current_playback_handle(&stream_id, track, config, volume, &handles) {
                Ok(playback) => playback,
                Err(error) => {
                    let _ = self.remove_actor_best_effort(&stream_id);
                    self.queue_source_refresh_if_auth_expired(&stream_id, &track_id, &error)?;
                    return Err(to_napi_error(error));
                }
            };

        if let Some(old_playback) =
            insert_current_playback(&stream_id, generation, playback, &handles)?
        {
            stop_replaced_playback(old_playback, &self.tokio);
        }
        self.transports
            .write()
            .map_err(lock_error)?
            .insert(stream_id.clone(), transport.clone());
        self.source_configs
            .write()
            .map_err(lock_error)?
            .insert(stream_id.clone(), source_config.clone());
        let preload_actions = play_output
            .actions
            .iter()
            .filter(|action| matches!(action, TaskAction::PrepareNext { .. }))
            .cloned()
            .collect::<Vec<_>>();
        startup_events.extend(play_output.events);
        self.queue_events(startup_events)?;
        execute_runtime_actions(&stream_id, preload_actions, volume, gain, 0, &handles)?;
        self.status_output(&stream_id)
    }

    #[napi]
    pub fn start_placeholder_stream(
        &self,
        stream_id: String,
        current: Option<TrackSourceInput>,
        next: Option<TrackSourceInput>,
    ) -> Result<StreamStatusOutput> {
        let current = current
            .map(TrackSource::try_from)
            .transpose()
            .map_err(to_napi_error)?;
        let next = next
            .map(TrackSource::try_from)
            .transpose()
            .map_err(to_napi_error)?;
        self.remove_reusable_inactive_stream(&stream_id)?;
        self.inactive_statuses
            .write()
            .map_err(lock_error)?
            .remove(&stream_id);
        if self
            .actors
            .read()
            .map_err(lock_error)?
            .contains_key(&stream_id)
        {
            return Err(to_napi_error(MusicStreamError::StreamAlreadyExists(
                stream_id.clone(),
            )));
        }
        let status = self
            .spawn_actor_mailbox(stream_id.clone(), current, next)
            .and_then(|mailbox| {
                let handle = mailbox.handle();
                self.actors
                    .write()
                    .map_err(lock_error)?
                    .insert(stream_id.clone(), mailbox);
                self.actor_status_with_handle(&handle)
            })?;

        Ok(status.into())
    }

    #[napi]
    pub fn get_status(&self, stream_id: String) -> Result<StreamStatusOutput> {
        self.status_output(&stream_id)
    }

    #[napi]
    pub fn get_statuses(
        &self,
        stream_ids: Option<Vec<String>>,
    ) -> Result<Vec<StreamStatusBatchItemOutput>> {
        let stream_ids = match stream_ids {
            Some(stream_ids) => stream_ids,
            None => self.status_stream_ids()?,
        };

        Ok(stream_ids
            .into_iter()
            .map(|stream_id| match self.status_output(&stream_id) {
                Ok(status) => StreamStatusBatchItemOutput {
                    stream_id,
                    ok: true,
                    status: Some(status),
                    code: None,
                    message: None,
                },
                Err(error) => StreamStatusBatchItemOutput {
                    stream_id,
                    ok: false,
                    status: None,
                    code: Some(error.status.clone()),
                    message: Some(error.reason.clone()),
                },
            })
            .collect())
    }

    #[napi]
    pub fn set_next(
        &self,
        stream_id: String,
        next: Option<TrackSourceInput>,
    ) -> Result<StreamStatusOutput> {
        let next = next
            .map(TrackSource::try_from)
            .transpose()
            .map_err(to_napi_error)?;
        let output = self
            .actor_command(&stream_id, StreamCommand::SetNext(next))
            .map_err(to_napi_error)?;
        let current_start_position_ms = actor_output_start_position_ms(&output);
        self.apply_actor_output_and_return_current_status(
            &stream_id,
            output,
            current_start_position_ms,
            true,
        )
    }

    #[napi]
    pub fn switch_track(
        &self,
        stream_id: String,
        current: TrackSourceInput,
        next: Option<TrackSourceInput>,
    ) -> Result<StreamStatusOutput> {
        self.reap_finished_playbacks()?;
        let current = TrackSource::try_from(current).map_err(to_napi_error)?;
        let next = next
            .map(TrackSource::try_from)
            .transpose()
            .map_err(to_napi_error)?;
        let output = self
            .actor_command(&stream_id, StreamCommand::SwitchTrack { current, next })
            .map_err(to_napi_error)?;
        self.apply_actor_output_and_return_current_status(&stream_id, output, 0, true)
    }

    #[napi]
    pub fn refresh_current_source(
        &self,
        stream_id: String,
        current: TrackSourceInput,
    ) -> Result<StreamStatusOutput> {
        self.reap_finished_playbacks()?;
        let current = TrackSource::try_from(current).map_err(to_napi_error)?;
        let output = self
            .actor_command(&stream_id, StreamCommand::RefreshCurrentSource { current })
            .map_err(to_napi_error)?;
        self.apply_actor_output_and_return_current_status(&stream_id, output, 0, true)
    }

    #[napi]
    pub fn seek_stream(&self, stream_id: String, seconds: u32) -> Result<StreamStatusOutput> {
        self.reap_finished_playbacks()?;
        let output = self
            .actor_command(
                &stream_id,
                StreamCommand::Seek {
                    seconds: u64::from(seconds),
                },
            )
            .map_err(to_napi_error)?;
        let current_start_position_ms = actor_output_start_position_ms(&output);
        self.apply_actor_output_and_return_current_status(
            &stream_id,
            output,
            current_start_position_ms,
            true,
        )
    }

    #[napi]
    pub fn set_volume(&self, stream_id: String, volume: f64) -> Result<StreamStatusOutput> {
        let volume_level = VolumeLevel::from_unit(volume as f32).map_err(to_napi_error)?;
        let output = self
            .actor_command(
                &stream_id,
                StreamCommand::SetVolume {
                    volume: volume_level,
                },
            )
            .map_err(to_napi_error)?;
        let current_start_position_ms = actor_output_start_position_ms(&output);
        let has_runtime_config = self.has_runtime_config(&stream_id)?;
        self.apply_actor_output_and_return_current_status(
            &stream_id,
            output,
            current_start_position_ms,
            has_runtime_config,
        )
    }

    #[napi]
    pub fn set_gain(&self, stream_id: String, gain_db: f64) -> Result<StreamStatusOutput> {
        let gain = GainLevel::from_db(gain_db as f32).map_err(to_napi_error)?;
        let output = self
            .actor_command(&stream_id, StreamCommand::SetGain { gain })
            .map_err(to_napi_error)?;
        let current_start_position_ms = actor_output_start_position_ms(&output);
        let has_runtime_config = self.has_runtime_config(&stream_id)?;
        self.apply_actor_output_and_return_current_status(
            &stream_id,
            output,
            current_start_position_ms,
            has_runtime_config,
        )
    }

    #[napi]
    pub fn pause_stream(&self, stream_id: String) -> Result<StreamStatusOutput> {
        let output = self
            .actor_command(&stream_id, StreamCommand::Pause)
            .map_err(to_napi_error)?;
        let current_start_position_ms = actor_output_start_position_ms(&output);
        let has_runtime_config = self.has_runtime_config(&stream_id)?;
        self.apply_actor_output_and_return_current_status(
            &stream_id,
            output,
            current_start_position_ms,
            has_runtime_config,
        )
    }

    #[napi]
    pub fn resume_stream(&self, stream_id: String) -> Result<StreamStatusOutput> {
        let output = self
            .actor_command(&stream_id, StreamCommand::Play)
            .map_err(to_napi_error)?;
        let current_start_position_ms = actor_output_start_position_ms(&output);
        let has_runtime_config = self.has_runtime_config(&stream_id)?;
        self.apply_actor_output_and_return_current_status(
            &stream_id,
            output,
            current_start_position_ms,
            has_runtime_config,
        )
    }

    #[napi]
    pub fn stop_stream(&self, stream_id: String) -> Result<StreamStatusOutput> {
        if !self
            .actors
            .read()
            .map_err(lock_error)?
            .contains_key(&stream_id)
            && let Some(status) = self
                .inactive_statuses
                .read()
                .map_err(lock_error)?
                .get(&stream_id)
                .cloned()
        {
            return Ok(status.into());
        }

        let progress = self.playback_progress(&stream_id)?;
        let output = self
            .actor_command(&stream_id, StreamCommand::Stop)
            .map_err(to_napi_error)?;
        self.cleanup_runtime_handles_for_stream(&stream_id)?;

        let final_status = actor_output_status(&output);
        self.inactive_statuses
            .write()
            .map_err(lock_error)?
            .insert(stream_id.clone(), final_status.clone());
        self.shutdown_actor_mailbox(&stream_id)?;

        let mut status: StreamStatusOutput = final_status.into();
        if let Some(progress) = progress {
            status.apply_progress(progress);
        }
        self.queue_events(output.events)?;
        Ok(status)
    }

    #[napi]
    pub fn drain_events(&self, stream_id: Option<String>) -> Result<Vec<StreamEventOutput>> {
        let drained = {
            let mut events = self.events.write().map_err(lock_error)?;
            let mut kept = Vec::new();
            let mut drained = Vec::new();
            for event in events.drain(..) {
                if stream_id
                    .as_deref()
                    .is_none_or(|stream_id| event_belongs_to(&event, stream_id))
                {
                    drained.push(event);
                } else {
                    kept.push(event);
                }
            }
            *events = kept;
            drained
        };

        drained
            .into_iter()
            .map(|event| self.event_output(event))
            .collect()
    }

    #[napi]
    pub fn set_event_callback(
        &self,
        callback: Option<Function<'_, StreamEventOutput, ()>>,
    ) -> Result<()> {
        let callback = callback
            .map(|callback| {
                callback
                    .build_threadsafe_function::<StreamEventOutput>()
                    .callee_handled::<false>()
                    .weak::<true>()
                    .max_queue_size::<1024>()
                    .build()
                    .map(Arc::new)
            })
            .transpose()
            .map_err(napi_internal_error)?;
        *self.event_callback.write().map_err(lock_error)? = callback;
        Ok(())
    }

    #[napi]
    pub fn validate_rtp_transport_config(
        &self,
        config: RtpTransportConfigInput,
    ) -> Result<RtpTransportConfigOutput> {
        let config = RtpTransportConfig::try_from(config).map_err(to_napi_error)?;
        config.validate().map_err(to_napi_error)?;
        Ok(config.into())
    }

    #[napi]
    pub fn validate_source_resolver_config(
        &self,
        config: SourceResolverConfigInput,
    ) -> Result<SourceResolverConfigOutput> {
        let config = source_config_from_input(Some(config)).map_err(to_napi_error)?;
        config.validate().map_err(to_napi_error)?;
        Ok(config.into())
    }

    #[napi]
    pub fn recommend_replay_gain(
        &self,
        input: ReplayGainInput,
    ) -> Result<ReplayGainRecommendationOutput> {
        let (metadata, config) = replay_gain_from_input(input).map_err(to_napi_error)?;
        recommend_replay_gain(metadata, config)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn shutdown(&self) -> Result<()> {
        self.shutdown_inner()
    }
}

impl std::fmt::Debug for Streamer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Streamer")
            .field("actors", &lock_len(&self.actors))
            .field("inactive_statuses", &lock_len(&self.inactive_statuses))
            .field("playbacks", &lock_len(&self.playbacks))
            .field("preloads", &lock_len(&self.preloads))
            .field("promotions", &lock_len(&self.promotions))
            .field("transports", &lock_len(&self.transports))
            .field("source_configs", &lock_len(&self.source_configs))
            .field(
                "source_cache_entries",
                &self
                    .source_cache
                    .lock()
                    .map(|cache| cache.len())
                    .unwrap_or(0),
            )
            .field("queued_events", &lock_len(&self.events))
            .field(
                "has_event_callback",
                &self
                    .event_callback
                    .read()
                    .map(|callback| callback.is_some())
                    .unwrap_or(false),
            )
            .finish()
    }
}

impl Drop for Streamer {
    fn drop(&mut self) {
        let _ = self.shutdown_inner();
    }
}

fn playback_config(
    generation: u64,
    transport: RtpTransportConfig,
    source_config: SourceResolverConfig,
    source_cache: &SharedSourceArtifactCache,
) -> LocalFileRtpPlaybackConfig {
    let mut config = LocalFileRtpPlaybackConfig::new(generation, transport);
    config.live_http = source_config.live_http.clone();
    config.source_resolver = if source_config.http.cache_temp_files {
        FileSourceResolver::with_cache(source_config, Arc::clone(source_cache))
    } else {
        FileSourceResolver::new(source_config)
    };
    config
}

impl Streamer {
    fn spawn_actor_mailbox(
        &self,
        stream_id: String,
        current: Option<TrackSource>,
        next: Option<TrackSource>,
    ) -> Result<StreamActorMailbox> {
        let _guard = self.tokio.enter();
        StreamActorMailbox::spawn(stream_id, current, next).map_err(to_napi_error)
    }

    fn actor_handle(
        &self,
        stream_id: &str,
    ) -> std::result::Result<StreamActorMailboxHandle, MusicStreamError> {
        self.actors
            .read()
            .map_err(|_| MusicStreamError::Internal("streamer lock poisoned".to_owned()))?
            .get(stream_id)
            .map(StreamActorMailbox::handle)
            .ok_or_else(|| MusicStreamError::StreamNotFound(stream_id.to_owned()))
    }

    fn actor_command(
        &self,
        stream_id: &str,
        command: StreamCommand,
    ) -> std::result::Result<ActorOutput, MusicStreamError> {
        let handle = self.actor_handle(stream_id)?;
        self.actor_command_with_handle(&handle, command)
    }

    fn actor_command_with_handle(
        &self,
        handle: &StreamActorMailboxHandle,
        command: StreamCommand,
    ) -> std::result::Result<ActorOutput, MusicStreamError> {
        self.tokio.block_on(handle.command(command))
    }

    fn actor_status_with_handle(
        &self,
        handle: &StreamActorMailboxHandle,
    ) -> Result<music_stream::StreamStatus> {
        self.tokio.block_on(handle.status()).map_err(to_napi_error)
    }

    fn actor_status(&self, stream_id: &str) -> Result<music_stream::StreamStatus> {
        let handle = self.actor_handle(stream_id).map_err(to_napi_error)?;
        self.actor_status_with_handle(&handle)
    }

    fn status_output(&self, stream_id: &str) -> Result<StreamStatusOutput> {
        let status = match self.actor_status(stream_id) {
            Ok(status) => status,
            Err(error) => self
                .inactive_statuses
                .read()
                .map_err(lock_error)?
                .get(stream_id)
                .cloned()
                .ok_or(error)?,
        };
        let mut status: StreamStatusOutput = status.into();
        if let Some(progress) = self.playback_progress(stream_id)? {
            status.apply_progress(progress);
        }
        Ok(status)
    }

    fn status_stream_ids(&self) -> Result<Vec<String>> {
        let mut stream_ids = self
            .actors
            .read()
            .map_err(lock_error)?
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let inactive_stream_ids = self
            .inactive_statuses
            .read()
            .map_err(lock_error)?
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for stream_id in inactive_stream_ids {
            if !stream_ids.contains(&stream_id) {
                stream_ids.push(stream_id);
            }
        }
        stream_ids.sort();
        Ok(stream_ids)
    }

    fn playback_progress(&self, stream_id: &str) -> Result<Option<LocalFileRtpPlaybackProgress>> {
        Ok(self
            .playbacks
            .read()
            .map_err(lock_error)?
            .get(stream_id)
            .and_then(GenerationTaskSlot::get)
            .map(LocalFileRtpPlayback::progress))
    }

    fn runtime_handles<'a>(
        &'a self,
        stream_id: &str,
        transport: &'a RtpTransportConfig,
        source_config: &'a SourceResolverConfig,
    ) -> Result<RuntimeHandles<'a>> {
        let actor_handle = self.actor_handle(stream_id).map_err(to_napi_error)?;
        Ok(RuntimeHandles {
            tokio: &self.tokio,
            actor_handle,
            playbacks: &self.playbacks,
            preloads: &self.preloads,
            promotions: &self.promotions,
            events: &self.events,
            event_callback: &self.event_callback,
            transports: &self.transports,
            source_configs: &self.source_configs,
            source_cache: &self.source_cache,
            transport,
            source_config,
        })
    }

    fn apply_actor_output_and_return_current_status(
        &self,
        stream_id: &str,
        output: ActorOutput,
        current_start_position_ms: u64,
        run_runtime_actions: bool,
    ) -> Result<StreamStatusOutput> {
        let ActorOutput {
            actions,
            events,
            status,
        } = output;
        let volume = status.volume;
        let gain = status.gain;

        self.queue_events(events)?;
        if run_runtime_actions {
            let transport = self.transport_for_stream(stream_id)?;
            let source_config = self.source_config_for_stream(stream_id)?;
            let handles = self.runtime_handles(stream_id, &transport, &source_config)?;
            execute_runtime_actions(
                stream_id,
                actions,
                volume,
                gain,
                current_start_position_ms,
                &handles,
            )?;
        }

        self.status_output(stream_id)
    }

    fn transport_for_stream(&self, stream_id: &str) -> Result<RtpTransportConfig> {
        self.transports
            .read()
            .map_err(lock_error)?
            .get(stream_id)
            .cloned()
            .ok_or_else(|| {
                to_napi_error(MusicStreamError::Internal(
                    "missing RTP transport config for stream".to_owned(),
                ))
            })
    }

    fn has_runtime_config(&self, stream_id: &str) -> Result<bool> {
        let has_transport = self
            .transports
            .read()
            .map_err(lock_error)?
            .contains_key(stream_id);
        let has_source_config = self
            .source_configs
            .read()
            .map_err(lock_error)?
            .contains_key(stream_id);
        Ok(has_transport && has_source_config)
    }

    fn source_config_for_stream(&self, stream_id: &str) -> Result<SourceResolverConfig> {
        self.source_configs
            .read()
            .map_err(lock_error)?
            .get(stream_id)
            .cloned()
            .ok_or_else(|| {
                to_napi_error(MusicStreamError::Internal(
                    "missing source resolver config for stream".to_owned(),
                ))
            })
    }

    fn queue_events(&self, events: Vec<StreamEvent>) -> Result<()> {
        push_events_checked(&self.events, &self.playbacks, &self.event_callback, events)
    }

    fn queue_source_refresh_if_auth_expired(
        &self,
        stream_id: &str,
        track_id: &str,
        error: &MusicStreamError,
    ) -> Result<()> {
        if error.code() == ErrorCode::SourceAuthExpired {
            self.queue_events(vec![StreamEvent::SourceRefreshNeeded {
                stream_id: stream_id.to_owned(),
                track_id: track_id.to_owned(),
            }])?;
        }
        Ok(())
    }

    fn event_output(&self, event: StreamEvent) -> Result<StreamEventOutput> {
        event_output_from_parts(event, &self.playbacks)
    }

    fn remove_reusable_inactive_stream(&self, stream_id: &str) -> Result<()> {
        if self
            .inactive_statuses
            .write()
            .map_err(lock_error)?
            .remove(stream_id)
            .is_some()
        {
            return Ok(());
        }

        let Ok(status) = self.actor_status(stream_id) else {
            return Ok(());
        };

        if matches!(
            status.play_state,
            PlayState::Idle | PlayState::Stopped | PlayState::Error
        ) {
            self.remove_actor(stream_id)?;
            return Ok(());
        }

        Err(to_napi_error(MusicStreamError::StreamAlreadyExists(
            stream_id.to_owned(),
        )))
    }

    fn reap_finished_playbacks(&self) -> Result<()> {
        let finished = {
            let playbacks = self.playbacks.read().map_err(lock_error)?;
            playbacks
                .iter()
                .filter_map(|(stream_id, slot)| {
                    let generation = slot.generation()?;
                    slot.get()
                        .is_some_and(LocalFileRtpPlayback::is_finished)
                        .then(|| (stream_id.clone(), generation))
                })
                .collect::<Vec<_>>()
        };

        if finished.is_empty() {
            return Ok(());
        }

        let finished_playbacks = {
            let mut playbacks = self.playbacks.write().map_err(lock_error)?;
            finished
                .into_iter()
                .filter_map(|(stream_id, generation)| {
                    let playback = playbacks
                        .get_mut(&stream_id)
                        .and_then(|slot| slot.take_task_if_generation(generation));
                    if playbacks
                        .get(&stream_id)
                        .is_some_and(GenerationTaskSlot::is_empty)
                    {
                        playbacks.remove(&stream_id);
                    }
                    playback
                })
                .collect::<Vec<_>>()
        };
        for playback in finished_playbacks {
            join_reaped_playback(playback)?;
        }
        Ok(())
    }

    fn shutdown_inner(&self) -> Result<()> {
        let promotions = drain_task_registry(&self.promotions)?;
        for promotion in promotions {
            promotion.abort();
        }

        let playbacks = drain_task_registry(&self.playbacks)?;
        for playback in playbacks {
            stop_join_playback(playback)?;
        }

        let preloads = drain_task_registry(&self.preloads)?;
        for preload in preloads {
            preload.stop_join()?;
        }

        self.transports.write().map_err(lock_error)?.clear();
        self.source_configs.write().map_err(lock_error)?.clear();
        self.inactive_statuses.write().map_err(lock_error)?.clear();
        self.events.write().map_err(lock_error)?.clear();
        self.event_callback.write().map_err(lock_error)?.take();
        self.source_cache.lock().map_err(lock_error)?.clear();
        let actors = {
            let mut actors = self.actors.write().map_err(lock_error)?;
            actors.drain().map(|(_, actor)| actor).collect::<Vec<_>>()
        };
        for actor in actors {
            self.shutdown_actor(actor)?;
        }
        Ok(())
    }

    fn remove_actor(&self, stream_id: &str) -> Result<()> {
        self.cleanup_runtime_handles_for_stream(stream_id)?;
        self.shutdown_actor_mailbox(stream_id)
    }

    fn shutdown_actor_mailbox(&self, stream_id: &str) -> Result<()> {
        if let Some(actor) = self.actors.write().map_err(lock_error)?.remove(stream_id) {
            self.shutdown_actor(actor)?;
        }
        Ok(())
    }

    fn shutdown_actor(&self, actor: StreamActorMailbox) -> Result<()> {
        let report = self.tokio.block_on(actor.shutdown(Duration::from_secs(1)));
        if report.timed_out || report.panicked > 0 || !report.failed.is_empty() {
            return Err(to_napi_error(MusicStreamError::Internal(format!(
                "stream actor shutdown failed: {report:?}"
            ))));
        }
        Ok(())
    }

    fn remove_actor_best_effort(&self, stream_id: &str) -> Result<()> {
        self.remove_actor(stream_id)
    }

    fn abort_promotions_for_stream(&self, stream_id: &str) -> Result<()> {
        if let Some(promotion) = take_stream_task(&self.promotions, stream_id)? {
            promotion.abort();
        }
        Ok(())
    }

    fn cleanup_runtime_handles_for_stream(&self, stream_id: &str) -> Result<()> {
        self.abort_promotions_for_stream(stream_id)?;
        if let Some(playback) = take_stream_task(&self.playbacks, stream_id)? {
            stop_join_playback_best_effort(playback, &self.tokio);
        }
        if let Some(preload) = take_stream_task(&self.preloads, stream_id)? {
            stop_join_preload_best_effort(preload, &self.tokio);
        }
        self.transports
            .write()
            .map_err(lock_error)?
            .remove(stream_id);
        self.source_configs
            .write()
            .map_err(lock_error)?
            .remove(stream_id);
        Ok(())
    }
}

fn find_start_current(actions: &[TaskAction]) -> Option<(u64, TrackSource)> {
    actions.iter().find_map(|action| match action {
        TaskAction::StartCurrent { generation, track } => Some((*generation, track.clone())),
        _ => None,
    })
}

fn actor_output_volume(output: &ActorOutput) -> VolumeLevel {
    output.status.volume
}

fn actor_output_gain(output: &ActorOutput) -> GainLevel {
    output.status.gain
}

fn actor_output_start_position_ms(output: &ActorOutput) -> u64 {
    output.status.time_played_ms
}

fn actor_output_status(output: &ActorOutput) -> music_stream::StreamStatus {
    output.status.clone()
}

fn handle_actor_output(stream_id: &str, output: ActorOutput, handles: &RuntimeHandles<'_>) {
    let volume = actor_output_volume(&output);
    let gain = actor_output_gain(&output);
    let actions = output.actions.clone();
    push_events(
        handles.events,
        handles.playbacks,
        handles.event_callback,
        output.events,
    );

    let current_start_position_ms = output.status.time_played_ms;
    if let Err(error) = execute_runtime_actions(
        stream_id,
        actions,
        volume,
        gain,
        current_start_position_ms,
        handles,
    ) {
        push_events(
            handles.events,
            handles.playbacks,
            handles.event_callback,
            vec![internal_error_event(stream_id, &error.to_string())],
        );
    }
}

fn execute_runtime_actions(
    stream_id: &str,
    actions: Vec<TaskAction>,
    volume: VolumeLevel,
    gain: GainLevel,
    current_start_position_ms: u64,
    handles: &RuntimeHandles<'_>,
) -> Result<()> {
    for action in actions {
        match action {
            TaskAction::CancelCurrent { generation } => {
                cancel_current_playback(stream_id, generation, handles)?;
            }
            TaskAction::CancelNext { generation } => {
                cancel_next_preload(stream_id, generation, handles)?;
            }
            TaskAction::StartCurrent { generation, track } => {
                if promote_preload_if_ready(stream_id, generation, volume, handles)? {
                    continue;
                }
                start_current_playback(
                    stream_id,
                    generation,
                    track,
                    volume,
                    gain,
                    current_start_position_ms,
                    handles,
                )?;
            }
            TaskAction::PrepareNext { generation, track } => {
                start_next_preload(stream_id, generation, track, volume, gain, handles)?;
            }
            TaskAction::PauseCurrent { generation } => {
                pause_current_playback(handles.playbacks, stream_id, generation)?;
            }
            TaskAction::ResumeCurrent { generation } => {
                resume_current_playback(handles.playbacks, stream_id, generation)?;
            }
            TaskAction::SetCurrentVolume { generation, volume } => {
                set_current_volume(stream_id, generation, volume, handles)?;
            }
            TaskAction::SetNextVolume { generation, volume } => {
                set_next_volume(stream_id, generation, volume, handles)?;
            }
            TaskAction::SetCurrentGain { generation, gain } => {
                set_current_gain(stream_id, generation, gain, handles)?;
            }
            TaskAction::SetNextGain { generation, gain } => {
                set_next_gain(stream_id, generation, gain, handles)?;
            }
            TaskAction::StopSender => {}
        }
    }
    Ok(())
}

fn pause_current_playback(
    playbacks: &Arc<RwLock<PlaybackRegistry>>,
    stream_id: &str,
    generation: u64,
) -> Result<()> {
    if let Some(playback) = playbacks
        .read()
        .map_err(lock_error)?
        .get(stream_id)
        .and_then(|slot| slot.get_if_generation(generation))
    {
        playback.pause();
    }
    Ok(())
}

fn resume_current_playback(
    playbacks: &Arc<RwLock<PlaybackRegistry>>,
    stream_id: &str,
    generation: u64,
) -> Result<()> {
    if let Some(playback) = playbacks
        .read()
        .map_err(lock_error)?
        .get(stream_id)
        .and_then(|slot| slot.get_if_generation(generation))
    {
        playback.resume();
    }
    Ok(())
}

fn stop_join_playback(playback: LocalFileRtpPlayback) -> Result<()> {
    playback.stop();
    let started = Instant::now();
    let result = playback.join().map_err(to_napi_error);
    record_join_latency(PLAYBACK_STOP_JOIN_US_METRIC, started.elapsed());
    if result.is_err() {
        metrics::counter!(PLAYBACK_STOP_JOIN_ERRORS_METRIC).increment(1);
    }
    result.map(|_| ())
}

fn join_reaped_playback(playback: LocalFileRtpPlayback) -> Result<()> {
    let started = Instant::now();
    let result = playback.join().map_err(to_napi_error);
    record_join_latency(PLAYBACK_REAP_JOIN_US_METRIC, started.elapsed());
    if result.is_err() {
        metrics::counter!(PLAYBACK_REAP_JOIN_ERRORS_METRIC).increment(1);
    }
    result.map(|_| ())
}

fn stop_replaced_playback(playback: LocalFileRtpPlayback, tokio: &Arc<tokio::runtime::Runtime>) {
    stop_join_playback_best_effort(playback, tokio);
}

fn stop_join_playback_best_effort(
    playback: LocalFileRtpPlayback,
    tokio: &Arc<tokio::runtime::Runtime>,
) {
    playback.stop();
    tokio.spawn_blocking(move || {
        let started = Instant::now();
        if playback.join().is_err() {
            metrics::counter!(PLAYBACK_STOP_JOIN_ERRORS_METRIC).increment(1);
        }
        record_join_latency(PLAYBACK_STOP_JOIN_US_METRIC, started.elapsed());
    });
}

fn stop_join_preload_best_effort(preload: PreloadRuntime, tokio: &Arc<tokio::runtime::Runtime>) {
    preload.stop();
    tokio.spawn_blocking(move || preload.stop_join_best_effort());
}

fn record_join_latency(metric: &'static str, elapsed: Duration) {
    metrics::histogram!(metric).record(duration_micros(elapsed) as f64);
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

fn drain_task_registry<T>(registry: &Arc<RwLock<TaskRegistry<T>>>) -> Result<Vec<T>> {
    Ok(registry
        .write()
        .map_err(lock_error)?
        .drain()
        .filter_map(|(_, mut slot)| slot.take_task())
        .collect())
}

fn take_stream_task<T>(
    registry: &Arc<RwLock<TaskRegistry<T>>>,
    stream_id: &str,
) -> Result<Option<T>> {
    Ok(registry
        .write()
        .map_err(lock_error)?
        .remove(stream_id)
        .and_then(|mut slot| slot.take_task()))
}

fn take_generation_task<T>(
    registry: &Arc<RwLock<TaskRegistry<T>>>,
    stream_id: &str,
    generation: u64,
) -> Result<Option<T>> {
    let mut registry = registry.write().map_err(lock_error)?;
    let task = registry
        .get_mut(stream_id)
        .and_then(|slot| slot.take_task_if_generation(generation));
    if registry
        .get(stream_id)
        .is_some_and(GenerationTaskSlot::is_empty)
    {
        registry.remove(stream_id);
    }
    Ok(task)
}

fn cancel_current_playback(
    stream_id: &str,
    generation: u64,
    handles: &RuntimeHandles<'_>,
) -> Result<()> {
    abort_promotion(stream_id, generation, handles)?;
    if let Some(playback) = take_generation_task(handles.playbacks, stream_id, generation)? {
        stop_join_playback_best_effort(playback, handles.tokio);
    }
    if let Some(preload) = take_preload(stream_id, generation, handles)? {
        stop_join_preload_best_effort(preload, handles.tokio);
    }
    Ok(())
}

fn handle_worker_failure_event(
    stream_id: &str,
    event: WorkerEvent,
    handles: &RuntimeHandles<'_>,
) -> Result<()> {
    if tokio::runtime::Handle::try_current().is_ok() {
        let context = RuntimeCallbackContext::from_handles(stream_id, handles);
        let reply = context
            .actor_handle
            .try_send_worker_event(event)
            .map_err(|error| {
                metrics::counter!(WORKER_EVENT_ENQUEUE_ERRORS_METRIC).increment(1);
                to_napi_error(error)
            })?;
        context.spawn_actor_reply(reply);
        return Ok(());
    }

    let output = handles
        .tokio
        .block_on(handles.actor_handle.worker_event(event))
        .map_err(to_napi_error)?;
    handle_actor_output(stream_id, output, handles);
    Ok(())
}

fn cancel_next_preload(
    stream_id: &str,
    generation: u64,
    handles: &RuntimeHandles<'_>,
) -> Result<()> {
    abort_promotion(stream_id, generation, handles)?;
    let preload = take_preload(stream_id, generation, handles)?;

    if let Some(preload) = preload {
        stop_join_preload_best_effort(preload, handles.tokio);
    }
    Ok(())
}

fn abort_promotion(stream_id: &str, generation: u64, handles: &RuntimeHandles<'_>) -> Result<()> {
    if let Some(promotion) = take_generation_task(handles.promotions, stream_id, generation)? {
        promotion.abort();
    }
    Ok(())
}

fn take_preload(
    stream_id: &str,
    generation: u64,
    handles: &RuntimeHandles<'_>,
) -> Result<Option<PreloadRuntime>> {
    take_generation_task(handles.preloads, stream_id, generation)
}

fn set_current_volume(
    stream_id: &str,
    generation: u64,
    volume: VolumeLevel,
    handles: &RuntimeHandles<'_>,
) -> Result<()> {
    if let Some(playback) = handles
        .playbacks
        .read()
        .map_err(lock_error)?
        .get(stream_id)
        .and_then(|slot| slot.get_if_generation(generation))
    {
        playback.set_volume(volume);
    }
    Ok(())
}

fn set_current_gain(
    stream_id: &str,
    generation: u64,
    gain: GainLevel,
    handles: &RuntimeHandles<'_>,
) -> Result<()> {
    if let Some(playback) = handles
        .playbacks
        .read()
        .map_err(lock_error)?
        .get(stream_id)
        .and_then(|slot| slot.get_if_generation(generation))
    {
        playback.set_gain(gain);
    }
    Ok(())
}

fn set_next_volume(
    stream_id: &str,
    generation: u64,
    volume: VolumeLevel,
    handles: &RuntimeHandles<'_>,
) -> Result<()> {
    if let Some(preload) = handles
        .preloads
        .read()
        .map_err(lock_error)?
        .get(stream_id)
        .and_then(|slot| slot.get_if_generation(generation))
    {
        preload.preload.set_volume(volume);
    }
    Ok(())
}

fn set_next_gain(
    stream_id: &str,
    generation: u64,
    gain: GainLevel,
    handles: &RuntimeHandles<'_>,
) -> Result<()> {
    if let Some(preload) = handles
        .preloads
        .read()
        .map_err(lock_error)?
        .get(stream_id)
        .and_then(|slot| slot.get_if_generation(generation))
    {
        preload.preload.set_gain(gain);
    }
    Ok(())
}

fn start_next_preload(
    stream_id: &str,
    generation: u64,
    track: TrackSource,
    volume: VolumeLevel,
    gain: GainLevel,
    handles: &RuntimeHandles<'_>,
) -> Result<()> {
    if !track.can_preload_as_next() {
        handle_worker_failure_event(
            stream_id,
            WorkerEvent::NextFailed {
                generation,
                code: ErrorCode::Unsupported,
                message: "live sources cannot be preloaded as next tracks".to_owned(),
            },
            handles,
        )?;
        return Ok(());
    }

    let mut config = playback_config(
        generation,
        handles.transport.clone(),
        handles.source_config.clone(),
        handles.source_cache,
    );
    config.gain = gain;
    let callback = RuntimeCallbackContext::from_handles(stream_id, handles);
    let preload = match spawn_local_file_preload(track, config.clone(), volume, move |event| {
        callback.handle_worker_event(event);
    }) {
        Ok(preload) => preload,
        Err(error) => {
            handle_worker_failure_event(
                stream_id,
                WorkerEvent::NextFailed {
                    generation,
                    code: error.code(),
                    message: error.to_string(),
                },
                handles,
            )?;
            return Ok(());
        }
    };

    let completion = preload.completion();
    let old_preload = {
        handles
            .preloads
            .write()
            .map_err(lock_error)?
            .entry(stream_id.to_owned())
            .or_default()
            .insert(
                generation,
                PreloadRuntime {
                    preload,
                    completion,
                    config,
                },
            )
    };
    if let Some(old_preload) = old_preload {
        abort_promotion(stream_id, old_preload.generation(), handles)?;
        stop_join_preload_best_effort(old_preload.into_task(), handles.tokio);
    }
    Ok(())
}

fn start_current_playback(
    stream_id: &str,
    generation: u64,
    track: TrackSource,
    volume: VolumeLevel,
    gain: GainLevel,
    start_position_ms: u64,
    handles: &RuntimeHandles<'_>,
) -> Result<()> {
    let mut config = playback_config(
        generation,
        handles.transport.clone(),
        handles.source_config.clone(),
        handles.source_cache,
    );
    config.start_position_ms = start_position_ms;
    config.gain = gain;

    let playback = match spawn_current_playback_handle(stream_id, track, config, volume, handles) {
        Ok(playback) => playback,
        Err(error) => {
            handle_worker_failure_event(
                stream_id,
                WorkerEvent::CurrentFailed {
                    generation,
                    code: error.code(),
                    message: error.to_string(),
                },
                handles,
            )?;
            return Ok(());
        }
    };

    if let Some(old_playback) = insert_current_playback(stream_id, generation, playback, handles)? {
        stop_replaced_playback(old_playback, handles.tokio);
    }
    Ok(())
}

fn spawn_current_playback_handle(
    stream_id: &str,
    track: TrackSource,
    config: LocalFileRtpPlaybackConfig,
    volume: VolumeLevel,
    handles: &RuntimeHandles<'_>,
) -> std::result::Result<LocalFileRtpPlayback, MusicStreamError> {
    let callback = RuntimeCallbackContext::from_handles(stream_id, handles);

    if track.is_live() {
        spawn_live_stream_rtp_playback(track, config, volume, move |event| {
            callback.handle_worker_event(event);
        })
    } else {
        spawn_local_file_rtp_playback(track, config, volume, move |event| {
            callback.handle_worker_event(event);
        })
    }
}

fn insert_current_playback(
    stream_id: &str,
    generation: u64,
    playback: LocalFileRtpPlayback,
    handles: &RuntimeHandles<'_>,
) -> Result<Option<LocalFileRtpPlayback>> {
    Ok(handles
        .playbacks
        .write()
        .map_err(lock_error)?
        .entry(stream_id.to_owned())
        .or_default()
        .insert_task(generation, playback))
}

fn promote_preload_if_ready(
    stream_id: &str,
    generation: u64,
    volume: VolumeLevel,
    handles: &RuntimeHandles<'_>,
) -> Result<bool> {
    let has_preload = {
        let preloads = handles.preloads.read().map_err(lock_error)?;
        preloads
            .get(stream_id)
            .and_then(|slot| slot.get_if_generation(generation))
            .is_some()
    };

    if has_preload {
        start_preload_promotion_waiter(stream_id, generation, volume, handles)?;
    }
    Ok(has_preload)
}

fn start_preload_promotion_waiter(
    stream_id: &str,
    generation: u64,
    volume: VolumeLevel,
    handles: &RuntimeHandles<'_>,
) -> Result<()> {
    let completion = {
        let preloads = handles.preloads.read().map_err(lock_error)?;
        let Some(preload) = preloads
            .get(stream_id)
            .and_then(|slot| slot.get_if_generation(generation))
        else {
            return Ok(());
        };
        preload.completion.clone()
    };

    let waiter_already_active = {
        let promotions = handles.promotions.read().map_err(lock_error)?;
        promotions
            .get(stream_id)
            .and_then(|slot| slot.get_if_generation(generation))
            .is_some_and(|promotion| !promotion.is_finished())
    };
    if waiter_already_active {
        metrics::counter!(PROMOTION_WAITER_DUPLICATES_METRIC).increment(1);
        return Ok(());
    }

    let stream_id = stream_id.to_owned();
    let tokio = Arc::clone(handles.tokio);
    let actor_handle = handles.actor_handle.clone();
    let playbacks = Arc::clone(handles.playbacks);
    let preloads = Arc::clone(handles.preloads);
    let promotions = Arc::clone(handles.promotions);
    let events = Arc::clone(handles.events);
    let event_callback = Arc::clone(handles.event_callback);
    let transports = Arc::clone(handles.transports);
    let source_configs = Arc::clone(handles.source_configs);
    let source_cache = Arc::clone(handles.source_cache);
    let transport = handles.transport.clone();
    let source_config = handles.source_config.clone();
    let registry_stream_id = stream_id.clone();
    let promotion_token = CancellationToken::new();
    let waiter_promotion_token = promotion_token.clone();
    let blocking_promotion_token = promotion_token.clone();
    let (registered_tx, registered_rx) = tokio::sync::oneshot::channel();

    let handle = {
        let _guard = tokio.enter();
        tokio::spawn(async move {
            if registered_rx.await.is_err() {
                return;
            }
            completion.wait().await;
            if waiter_promotion_token.is_cancelled() {
                return;
            }

            let join_error_stream_id = stream_id.clone();
            let join_error_playbacks = Arc::clone(&playbacks);
            let join_error_events = Arc::clone(&events);
            let join_error_callback = Arc::clone(&event_callback);
            let blocking_tokio = Arc::clone(&tokio);
            let blocking = tokio.spawn_blocking(move || {
                let result = (|| -> Result<()> {
                    if blocking_promotion_token.is_cancelled() {
                        return Ok(());
                    }

                    let still_present = preloads
                        .read()
                        .map_err(lock_error)?
                        .get(&stream_id)
                        .and_then(|slot| slot.get_if_generation(generation))
                        .is_some();
                    if !still_present || blocking_promotion_token.is_cancelled() {
                        return Ok(());
                    }

                    let handles = RuntimeHandles {
                        tokio: &blocking_tokio,
                        actor_handle,
                        playbacks: &playbacks,
                        preloads: &preloads,
                        promotions: &promotions,
                        events: &events,
                        event_callback: &event_callback,
                        transports: &transports,
                        source_configs: &source_configs,
                        source_cache: &source_cache,
                        transport: &transport,
                        source_config: &source_config,
                    };
                    match take_preload(&stream_id, generation, &handles)? {
                        Some(preload) => {
                            preload.join_for_promotion().and_then(|(report, config)| {
                                if blocking_promotion_token.is_cancelled() {
                                    return Ok(());
                                }
                                start_promoted_playback(
                                    &stream_id,
                                    generation,
                                    report,
                                    config,
                                    volume,
                                    &blocking_promotion_token,
                                    &handles,
                                )
                            })
                        }
                        None => Ok(()),
                    }
                })();
                if let Err(error) = result {
                    push_events(
                        &events,
                        &playbacks,
                        &event_callback,
                        vec![internal_error_event(&stream_id, &error.to_string())],
                    );
                }
                let _ = take_generation_task(&promotions, &stream_id, generation);
            });

            if let Err(error) = blocking.await {
                push_events(
                    &join_error_events,
                    &join_error_playbacks,
                    &join_error_callback,
                    vec![internal_error_event(
                        &join_error_stream_id,
                        &format!("preload promotion blocking task failed: {error}"),
                    )],
                );
            }
        })
    };
    let old_promotion = {
        let mut promotions = handles.promotions.write().map_err(lock_error)?;
        let slot = promotions.entry(registry_stream_id).or_default();
        if slot
            .get_if_generation(generation)
            .is_some_and(|promotion| !promotion.is_finished())
        {
            metrics::counter!(PROMOTION_WAITER_DUPLICATES_METRIC).increment(1);
            return Ok(());
        }
        slot.insert_task(generation, PromotionRuntime::new(promotion_token, handle))
    };
    if let Some(old_promotion) = old_promotion {
        old_promotion.abort();
    }
    let _ = registered_tx.send(());
    Ok(())
}

fn start_promoted_playback(
    stream_id: &str,
    generation: u64,
    report: music_stream::LocalFilePreloadReport,
    config: LocalFileRtpPlaybackConfig,
    volume: VolumeLevel,
    cancellation_token: &CancellationToken,
    handles: &RuntimeHandles<'_>,
) -> Result<()> {
    if !report.ready || report.generation != generation {
        return Err(lock_error_from_message(
            "preloaded next track was not ready for promotion",
        ));
    }

    if cancellation_token.is_cancelled() {
        return Ok(());
    }

    let callback = RuntimeCallbackContext::from_handles(stream_id, handles);
    let playback = match spawn_local_file_rtp_playback_from_driver(
        report.into_current_driver(),
        config,
        volume,
        move |event| {
            callback.handle_worker_event(event);
        },
    ) {
        Ok(playback) => playback,
        Err(error) => return Err(to_napi_error(error)),
    };

    if cancellation_token.is_cancelled() {
        stop_join_playback_best_effort(playback, handles.tokio);
        return Ok(());
    }

    if let Some(old_playback) = insert_current_playback(stream_id, generation, playback, handles)? {
        stop_replaced_playback(old_playback, handles.tokio);
    }
    if cancellation_token.is_cancelled()
        && let Some(playback) = take_generation_task(handles.playbacks, stream_id, generation)?
    {
        stop_join_playback_best_effort(playback, handles.tokio);
    }
    Ok(())
}

fn lock_error<T>(_: std::sync::PoisonError<T>) -> Error<String> {
    to_napi_error(MusicStreamError::Internal(
        "streamer lock poisoned".to_owned(),
    ))
}

trait RegistryLen {
    fn registry_len(&self) -> usize;
}

impl<K, V> RegistryLen for HashMap<K, V> {
    fn registry_len(&self) -> usize {
        self.len()
    }
}

impl<T> RegistryLen for Vec<T> {
    fn registry_len(&self) -> usize {
        self.len()
    }
}

fn lock_len<T: RegistryLen>(lock: &RwLock<T>) -> Option<usize> {
    lock.read().ok().map(|value| value.registry_len())
}

fn lock_error_from_message(message: &str) -> Error<String> {
    to_napi_error(MusicStreamError::Internal(message.to_owned()))
}

fn napi_internal_error(error: Error) -> Error<String> {
    to_napi_error(MusicStreamError::Internal(error.reason.clone()))
}

fn to_napi_error(error: music_stream::MusicStreamError) -> Error<String> {
    let code = error.code().as_str().to_owned();
    Error::new(code, error.to_string())
}
