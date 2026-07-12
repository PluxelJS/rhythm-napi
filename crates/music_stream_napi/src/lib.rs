use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use napi::bindgen_prelude::*;
use napi_derive::napi;

mod convert;
mod events;
mod types;

use convert::{
    media_buffer_config_from_input, replay_gain_from_input, runtime_resource_limits_from_input,
    source_config_from_input,
};
use events::{EventCallback, EventQueue, event_output};
use types::*;

use music_stream::{
    GainLevel, MusicStreamError, RtpTransportConfig, RuntimeResources, StreamCommand,
    StreamRuntime, StreamRuntimeConfig, StreamRuntimeSnapshot, TrackSource, VolumeLevel,
    recommend_replay_gain,
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
enum RuntimeEntry {
    Starting,
    Active(StreamRuntime),
}

#[napi]
pub struct Streamer {
    runtimes: Arc<tokio::sync::RwLock<HashMap<String, RuntimeEntry>>>,
    lifecycle: tokio::sync::RwLock<()>,
    inactive: Arc<tokio::sync::RwLock<HashMap<String, StreamStatusOutput>>>,
    resources: Arc<RuntimeResources>,
    events: EventQueue,
    event_callback: Arc<RwLock<Option<EventCallback>>>,
}

#[napi]
impl Streamer {
    #[napi(constructor)]
    pub fn new(options: Option<RuntimeResourceLimitsInput>) -> Result<Self> {
        let limits = runtime_resource_limits_from_input(options).map_err(to_napi_error)?;
        let resources = RuntimeResources::new(limits).map_err(to_napi_error)?;
        Ok(Self {
            runtimes: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            lifecycle: tokio::sync::RwLock::new(()),
            inactive: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            resources: Arc::new(resources),
            events: EventQueue::default(),
            event_callback: Arc::new(RwLock::new(None)),
        })
    }

    #[napi]
    pub async fn start_stream(&self, options: StartStreamInput) -> Result<StreamStatusOutput> {
        let _lifecycle = self.lifecycle.read().await;
        let stream_id = options.stream_id;
        let current = TrackSource::try_from(options.current).map_err(to_napi_error)?;
        let next = options
            .next
            .map(TrackSource::try_from)
            .transpose()
            .map_err(to_napi_error)?;
        let transport = RtpTransportConfig::try_from(options.transport).map_err(to_napi_error)?;
        let source = source_config_from_input(options.source).map_err(to_napi_error)?;
        let buffer = media_buffer_config_from_input(options.buffer).map_err(to_napi_error)?;
        let volume =
            VolumeLevel::from_unit(options.volume.unwrap_or(1.0) as f32).map_err(to_napi_error)?;
        let gain =
            GainLevel::from_db(options.gain_db.unwrap_or(0.0) as f32).map_err(to_napi_error)?;

        {
            let mut runtimes = self.runtimes.write().await;
            if runtimes.contains_key(&stream_id) {
                return Err(to_napi_error(MusicStreamError::StreamAlreadyExists(
                    stream_id,
                )));
            }
            runtimes.insert(stream_id.clone(), RuntimeEntry::Starting);
            self.inactive.write().await.remove(&stream_id);
        }
        let events = self.events.clone();
        let callback = Arc::clone(&self.event_callback);
        let mut config = StreamRuntimeConfig::new(transport, source);
        config.buffer = buffer;
        config.resources = Arc::clone(&self.resources);
        config.on_event = Some(Arc::new(move |event| events.publish(&callback, event)));
        let runtime = match StreamRuntime::start(
            stream_id.clone(),
            current,
            next,
            config,
            volume,
            gain,
        )
        .await
        {
            Ok(runtime) => runtime,
            Err(error) => {
                let mut runtimes = self.runtimes.write().await;
                if matches!(runtimes.get(&stream_id), Some(RuntimeEntry::Starting)) {
                    runtimes.remove(&stream_id);
                }
                return Err(to_napi_error(error));
            }
        };
        let snapshot = runtime.snapshot().await;
        let committed = {
            let mut runtimes = self.runtimes.write().await;
            if matches!(runtimes.get(&stream_id), Some(RuntimeEntry::Starting)) {
                runtimes.insert(stream_id.clone(), RuntimeEntry::Active(runtime.clone()));
                true
            } else {
                false
            }
        };
        if !committed {
            let _ = runtime.shutdown().await;
            return Err(to_napi_error(MusicStreamError::StreamClosed(format!(
                "stream {stream_id} was removed while starting"
            ))));
        }
        Ok(status_output(snapshot))
    }

    #[napi]
    pub async fn get_status(&self, stream_id: String) -> Result<StreamStatusOutput> {
        match self.runtimes.read().await.get(&stream_id).cloned() {
            Some(RuntimeEntry::Active(runtime)) => {
                return Ok(status_output(runtime.snapshot().await));
            }
            Some(RuntimeEntry::Starting) => {
                return Err(to_napi_error(MusicStreamError::Busy(format!(
                    "stream {stream_id} is still starting"
                ))));
            }
            None => {}
        }
        self.inactive
            .read()
            .await
            .get(&stream_id)
            .cloned()
            .ok_or_else(|| to_napi_error(MusicStreamError::StreamNotFound(stream_id)))
    }

    #[napi]
    pub async fn get_statuses(&self, stream_ids: Vec<String>) -> Vec<StreamStatusBatchItemOutput> {
        let mut output = Vec::with_capacity(stream_ids.len());
        for stream_id in stream_ids {
            match self.get_status(stream_id.clone()).await {
                Ok(status) => output.push(StreamStatusBatchItemOutput {
                    stream_id,
                    ok: true,
                    status: Some(status),
                    code: None,
                    message: None,
                }),
                Err(error) => output.push(StreamStatusBatchItemOutput {
                    stream_id,
                    ok: false,
                    status: None,
                    code: Some("INTERNAL".to_owned()),
                    message: Some(error.reason.clone()),
                }),
            }
        }
        output
    }

    #[napi]
    pub async fn set_next(
        &self,
        stream_id: String,
        next: Option<TrackSourceInput>,
    ) -> Result<StreamStatusOutput> {
        let next = next
            .map(TrackSource::try_from)
            .transpose()
            .map_err(to_napi_error)?;
        self.command(&stream_id, StreamCommand::SetNext(next)).await
    }

    #[napi]
    pub async fn switch_track(
        &self,
        stream_id: String,
        current: TrackSourceInput,
        next: Option<TrackSourceInput>,
    ) -> Result<StreamStatusOutput> {
        let current = TrackSource::try_from(current).map_err(to_napi_error)?;
        let next = next
            .map(TrackSource::try_from)
            .transpose()
            .map_err(to_napi_error)?;
        self.command(&stream_id, StreamCommand::SwitchTrack { current, next })
            .await
    }

    #[napi]
    pub async fn refresh_current_source(
        &self,
        stream_id: String,
        current: TrackSourceInput,
    ) -> Result<StreamStatusOutput> {
        let current = TrackSource::try_from(current).map_err(to_napi_error)?;
        self.command(&stream_id, StreamCommand::RefreshCurrentSource { current })
            .await
    }

    #[napi]
    pub async fn seek_stream(&self, stream_id: String, seconds: u32) -> Result<StreamStatusOutput> {
        self.command(
            &stream_id,
            StreamCommand::Seek {
                seconds: u64::from(seconds),
            },
        )
        .await
    }

    #[napi]
    pub async fn set_volume(&self, stream_id: String, volume: f64) -> Result<StreamStatusOutput> {
        let volume = VolumeLevel::from_unit(volume as f32).map_err(to_napi_error)?;
        self.command(&stream_id, StreamCommand::SetVolume { volume })
            .await
    }

    #[napi]
    pub async fn set_gain(&self, stream_id: String, gain_db: f64) -> Result<StreamStatusOutput> {
        let gain = GainLevel::from_db(gain_db as f32).map_err(to_napi_error)?;
        self.command(&stream_id, StreamCommand::SetGain { gain })
            .await
    }

    #[napi]
    pub async fn pause_stream(&self, stream_id: String) -> Result<StreamStatusOutput> {
        self.command(&stream_id, StreamCommand::Pause).await
    }

    #[napi]
    pub async fn resume_stream(&self, stream_id: String) -> Result<StreamStatusOutput> {
        self.command(&stream_id, StreamCommand::Play).await
    }

    #[napi]
    pub async fn stop_stream(&self, stream_id: String) -> Result<StreamStatusOutput> {
        let _lifecycle = self.lifecycle.read().await;
        let runtime = {
            let mut runtimes = self.runtimes.write().await;
            match runtimes.get(&stream_id) {
                Some(RuntimeEntry::Starting) => {
                    return Err(to_napi_error(MusicStreamError::Busy(format!(
                        "stream {stream_id} is still starting"
                    ))));
                }
                Some(RuntimeEntry::Active(_)) => match runtimes.remove(&stream_id) {
                    Some(RuntimeEntry::Active(runtime)) => Some(runtime),
                    _ => unreachable!("active runtime changed under registry write lock"),
                },
                None => None,
            }
        };
        if let Some(runtime) = runtime {
            let output = status_output(runtime.shutdown().await.map_err(to_napi_error)?);
            self.inactive
                .write()
                .await
                .insert(stream_id, output.clone());
            return Ok(output);
        }
        self.inactive
            .read()
            .await
            .get(&stream_id)
            .cloned()
            .ok_or_else(|| to_napi_error(MusicStreamError::StreamNotFound(stream_id)))
    }

    #[napi]
    pub fn drain_events(&self, stream_id: Option<String>) -> Result<Vec<StreamEventOutput>> {
        Ok(self
            .events
            .drain(stream_id.as_deref())?
            .into_iter()
            .map(event_output)
            .collect())
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
    pub async fn shutdown(&self) -> Result<()> {
        let _lifecycle = self.lifecycle.write().await;
        let entries = std::mem::take(&mut *self.runtimes.write().await);
        let mut shutdowns = tokio::task::JoinSet::new();
        for (stream_id, entry) in entries {
            if let RuntimeEntry::Active(runtime) = entry {
                shutdowns.spawn(async move { (stream_id, runtime.shutdown().await) });
            }
        }
        let mut failure_count = 0_usize;
        let mut failures = Vec::new();
        while let Some(result) = shutdowns.join_next().await {
            let failure = match result {
                Ok((_, Ok(_))) => None,
                Ok((stream_id, Err(error))) => Some(format!("{stream_id}: {error}")),
                Err(error) => Some(format!("shutdown task failed: {error}")),
            };
            if let Some(failure) = failure {
                failure_count += 1;
                if failures.len() < 8 {
                    failures.push(failure);
                }
            }
        }
        self.inactive.write().await.clear();
        self.events.clear()?;
        let stale_cache = self.resources.take_source_cache().map_err(to_napi_error)?;
        tokio::task::spawn_blocking(move || drop(stale_cache))
            .await
            .map_err(|error| {
                to_napi_error(MusicStreamError::Internal(format!(
                    "source cache cleanup worker failed: {error}"
                )))
            })?;
        self.resources
            .flush_source_cleanup()
            .await
            .map_err(to_napi_error)?;
        if failure_count > 0 {
            let omitted = failure_count.saturating_sub(failures.len());
            let suffix = if omitted == 0 {
                String::new()
            } else {
                format!("; {omitted} additional failures omitted")
            };
            return Err(to_napi_error(MusicStreamError::Internal(format!(
                "{failure_count} stream shutdown operation(s) failed: {}{suffix}",
                failures.join("; "),
            ))));
        }
        Ok(())
    }
}

impl Streamer {
    async fn command(&self, stream_id: &str, command: StreamCommand) -> Result<StreamStatusOutput> {
        let _lifecycle = self.lifecycle.read().await;
        let runtime = match self.runtimes.read().await.get(stream_id).cloned() {
            Some(RuntimeEntry::Active(runtime)) => runtime,
            Some(RuntimeEntry::Starting) => {
                return Err(to_napi_error(MusicStreamError::Busy(format!(
                    "stream {stream_id} is still starting"
                ))));
            }
            None => {
                return Err(to_napi_error(MusicStreamError::StreamNotFound(
                    stream_id.to_owned(),
                )));
            }
        };
        runtime
            .command(command)
            .await
            .map(status_output)
            .map_err(to_napi_error)
    }
}

impl Default for Streamer {
    fn default() -> Self {
        Self::new(None).expect("default runtime resource limits are valid")
    }
}

impl std::fmt::Debug for Streamer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Streamer").finish_non_exhaustive()
    }
}

fn status_output(snapshot: StreamRuntimeSnapshot) -> StreamStatusOutput {
    let generation_matches = snapshot.status.generation == snapshot.progress.generation;
    let mut output = StreamStatusOutput::from(snapshot.status);
    if generation_matches {
        output.apply_progress(snapshot.progress);
    }
    output
}

fn lock_error<T>(_: std::sync::PoisonError<T>) -> Error {
    to_napi_error(MusicStreamError::Internal(
        "streamer lock poisoned".to_owned(),
    ))
}

fn napi_internal_error(error: napi::Error) -> Error {
    Error::new(Status::GenericFailure, error.to_string())
}

fn to_napi_error(error: MusicStreamError) -> Error {
    Error::new(
        Status::GenericFailure,
        format!("{}: {error}", error.code().as_str()),
    )
}
