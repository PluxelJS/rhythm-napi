//! Per-stream actor state machine, playout slots, and generation control.
//!
//! Playlist policy stays in TypeScript. A session only manages current and next
//! track slots for one realtime stream.

use crate::error::{ErrorCode, MusicStreamError, Result};
use crate::event::{SourceRole, StreamEvent};
use crate::model::{GainLevel, PlayState, StreamStatus, TrackKind, TrackSource, VolumeLevel};
use crate::quality::{RtcpNetworkQualityLevel, RtcpQualityWindowSnapshot};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamCommand {
    Play,
    Pause,
    Stop,
    Seek {
        seconds: u64,
    },
    RefreshCurrentSource {
        current: TrackSource,
    },
    ReconcilePlan {
        version: u64,
        current: Option<TrackSource>,
        next: Option<TrackSource>,
    },
    SetVolume {
        volume: VolumeLevel,
    },
    SetGain {
        gain: GainLevel,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum WorkerEvent {
    CurrentSourceClassified {
        generation: u64,
        kind: TrackKind,
        seekable: bool,
    },
    CurrentPrebufferReady {
        generation: u64,
    },
    CurrentEnded {
        generation: u64,
    },
    CurrentFailed {
        generation: u64,
        code: ErrorCode,
        message: String,
    },
    OutputFailed {
        code: ErrorCode,
        message: String,
    },
    CurrentNetworkQualityChanged {
        generation: u64,
        quality: RtcpNetworkQualityLevel,
        snapshot: RtcpQualityWindowSnapshot,
    },
    NextReady {
        generation: u64,
    },
    NextFailed {
        generation: u64,
        code: ErrorCode,
        message: String,
    },
    StartupTimedOut {
        source_role: SourceRole,
        generation: u64,
        watchdog_epoch: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskAction {
    StartCurrent {
        generation: u64,
        watchdog_epoch: u64,
        track: TrackSource,
    },
    CancelCurrent {
        generation: u64,
    },
    PrepareNext {
        generation: u64,
        watchdog_epoch: u64,
        track: TrackSource,
    },
    CancelNext {
        generation: u64,
    },
    PauseCurrent {
        generation: u64,
    },
    PauseNext {
        generation: u64,
    },
    ResumeCurrent {
        generation: u64,
    },
    ResumeNext {
        generation: u64,
    },
    ArmStartupDeadline {
        source_role: SourceRole,
        generation: u64,
        watchdog_epoch: u64,
    },
    SetCurrentVolume {
        generation: u64,
        volume: VolumeLevel,
    },
    SetNextVolume {
        generation: u64,
        volume: VolumeLevel,
    },
    SetCurrentGain {
        generation: u64,
        gain: GainLevel,
    },
    SetNextGain {
        generation: u64,
        gain: GainLevel,
    },
    StopSender,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActorOutput {
    pub actions: Vec<TaskAction>,
    pub events: Vec<StreamEvent>,
    pub status: StreamStatus,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct ActorEffects {
    actions: Vec<TaskAction>,
    events: Vec<StreamEvent>,
}

impl ActorEffects {
    fn into_output(self, status: StreamStatus) -> ActorOutput {
        let mut facts = Vec::with_capacity(self.events.len() + 1);
        let mut requests = Vec::new();
        for event in self.events {
            if matches!(
                event,
                StreamEvent::NextNeeded { .. } | StreamEvent::SourceRefreshNeeded { .. }
            ) {
                requests.push(event);
            } else {
                facts.push(event);
            }
        }
        facts.push(StreamEvent::StateChanged {
            status: Box::new(status.clone()),
        });
        facts.extend(requests);
        ActorOutput {
            actions: self.actions,
            events: facts,
            status,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AttemptState {
    Dormant,
    Starting,
    Ready,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PlaybackAttempt {
    source: TrackSource,
    generation: u64,
    state: AttemptState,
    watchdog_epoch: u64,
}

impl PlaybackAttempt {
    fn new(source: TrackSource, generation: u64) -> Self {
        Self {
            source,
            generation,
            state: AttemptState::Dormant,
            watchdog_epoch: 0,
        }
    }

    fn start(&mut self) -> u64 {
        self.state = AttemptState::Starting;
        self.watchdog_epoch = self.watchdog_epoch.saturating_add(1);
        self.watchdog_epoch
    }

    fn suspend_watchdog(&mut self) {
        if self.state == AttemptState::Starting {
            self.watchdog_epoch = self.watchdog_epoch.saturating_add(1);
        }
    }

    fn resume_watchdog(&mut self) -> Option<u64> {
        (self.state == AttemptState::Starting).then(|| {
            self.watchdog_epoch = self.watchdog_epoch.saturating_add(1);
            self.watchdog_epoch
        })
    }

    fn is_active(&self) -> bool {
        self.state != AttemptState::Dormant
    }

    fn is_ready(&self) -> bool {
        self.state == AttemptState::Ready
    }
}

#[derive(Clone, Debug)]
pub struct StreamActor {
    stream_id: String,
    current: Option<PlaybackAttempt>,
    next: Option<PlaybackAttempt>,
    refreshable_current_key: Option<String>,
    play_state: PlayState,
    generation: u64,
    plan_version: u64,
    time_played_ms: u64,
    volume: VolumeLevel,
    gain: GainLevel,
}

impl StreamActor {
    #[must_use]
    pub fn new(stream_id: String, current: Option<TrackSource>) -> Self {
        let mut generation = 0;
        let current = current.map(|source| {
            generation += 1;
            PlaybackAttempt::new(source, generation)
        });

        Self {
            stream_id,
            current,
            next: None,
            refreshable_current_key: None,
            play_state: PlayState::Idle,
            generation,
            plan_version: 0,
            time_played_ms: 0,
            volume: VolumeLevel::default(),
            gain: GainLevel::default(),
        }
    }

    #[must_use]
    pub fn status(&self) -> StreamStatus {
        StreamStatus {
            stream_id: self.stream_id.clone(),
            current: self.current.as_ref().map(|slot| slot.source.clone()),
            next: self.next.as_ref().map(|slot| slot.source.clone()),
            play_state: self.play_state.clone(),
            time_played_ms: self.time_played_ms,
            generation: self.current_generation(),
            plan_version: self.plan_version,
            volume: self.volume,
            gain: self.gain,
        }
    }

    #[must_use]
    pub fn current_generation(&self) -> u64 {
        self.current
            .as_ref()
            .map_or(self.generation, |slot| slot.generation)
    }

    pub fn handle_command(&mut self, command: StreamCommand) -> Result<ActorOutput> {
        let mut output = ActorEffects::default();

        match command {
            StreamCommand::Play => self.play(&mut output)?,
            StreamCommand::Pause => self.pause(&mut output)?,
            StreamCommand::Stop => self.stop(&mut output),
            StreamCommand::Seek { seconds } => self.seek(seconds, &mut output)?,
            StreamCommand::RefreshCurrentSource { current } => {
                self.refresh_current_source(current, &mut output)?;
            }
            StreamCommand::ReconcilePlan {
                version,
                current,
                next,
            } => self.reconcile_plan(version, current, next, &mut output)?,
            StreamCommand::SetVolume { volume } => self.set_volume(volume, &mut output),
            StreamCommand::SetGain { gain } => self.set_gain(gain, &mut output),
        }

        Ok(output.into_output(self.status()))
    }

    pub fn handle_worker_event(&mut self, event: WorkerEvent) -> ActorOutput {
        let mut output = ActorEffects::default();

        match event {
            WorkerEvent::CurrentSourceClassified {
                generation,
                kind,
                seekable,
            } => {
                if self.is_current_generation(generation)
                    && let Some(current) = self.current.as_mut()
                {
                    current.source.kind = kind;
                    current.source.seekable = Some(seekable);
                }
            }
            WorkerEvent::CurrentPrebufferReady { generation } => {
                if self.is_current_generation(generation) {
                    if let Some(current) = self.current.as_mut() {
                        current.state = AttemptState::Ready;
                    }
                    if self.play_state != PlayState::Paused {
                        self.play_state = PlayState::Playing;
                    }
                }
            }
            WorkerEvent::CurrentEnded { generation } => {
                if self.is_current_generation(generation) {
                    self.promote_next_or_wait(&mut output, true);
                }
            }
            WorkerEvent::CurrentFailed {
                generation,
                code,
                message,
            } => {
                if self.is_current_generation(generation) {
                    self.handle_track_failure(code, message, &mut output);
                }
            }
            WorkerEvent::OutputFailed { code, message } => {
                return self.handle_output_failure(code, message);
            }
            WorkerEvent::CurrentNetworkQualityChanged {
                generation,
                quality,
                snapshot,
            } => {
                if self.is_current_generation(generation) {
                    output.events.push(StreamEvent::NetworkQualityChanged {
                        stream_id: self.stream_id.clone(),
                        quality,
                        snapshot,
                    });
                }
            }
            WorkerEvent::NextReady { generation } => {
                if let Some(next) = self.next.as_mut()
                    && next.generation == generation
                    && next.is_active()
                {
                    next.state = AttemptState::Ready;
                    if self.current.is_none() {
                        self.promote_ready_next(&mut output);
                    }
                }
            }
            WorkerEvent::NextFailed {
                generation,
                code,
                message,
            } => self.handle_next_failure(generation, code, message, &mut output),
            WorkerEvent::StartupTimedOut {
                source_role,
                generation,
                watchdog_epoch,
            } => self.handle_startup_timeout(source_role, generation, watchdog_epoch, &mut output),
        }

        output.into_output(self.status())
    }

    /// Moves the state machine to its only safe state after orchestration has
    /// partially failed. Runtime actions are not generally reversible (an RTP
    /// receiver or producer may already have been replaced), so pretending the
    /// previous logical state is still operational would leave a stuck stream.
    pub fn handle_runtime_failure(&mut self, error: &MusicStreamError) -> ActorOutput {
        self.handle_output_failure(error.code(), error.to_string())
    }

    pub fn handle_output_failure(&mut self, code: ErrorCode, message: String) -> ActorOutput {
        self.current = None;
        self.next = None;
        self.refreshable_current_key = None;
        self.play_state = PlayState::Stopped;
        let status = self.status();
        ActorOutput {
            actions: Vec::new(),
            events: vec![
                StreamEvent::Error {
                    stream_id: self.stream_id.clone(),
                    code,
                    message,
                },
                StreamEvent::StreamStopped {
                    stream_id: self.stream_id.clone(),
                },
                StreamEvent::StateChanged {
                    status: Box::new(status.clone()),
                },
            ],
            status,
        }
    }

    fn play(&mut self, output: &mut ActorEffects) -> Result<()> {
        if self.play_state == PlayState::Stopped {
            return Err(MusicStreamError::Unsupported(
                "cannot play a stopped stream".to_owned(),
            ));
        }

        let was_paused = self.play_state == PlayState::Paused;
        if self.current.is_none() {
            if let Some(next) = self.next.as_ref() {
                if was_paused && next.is_active() {
                    output.actions.push(TaskAction::ResumeNext {
                        generation: next.generation,
                    });
                }
                self.play_state = PlayState::Buffering;
            } else {
                self.play_state = PlayState::Idle;
                output.events.push(StreamEvent::NextNeeded {
                    stream_id: self.stream_id.clone(),
                });
            }
            if was_paused {
                self.arm_resumed_watchdogs(output);
            }
            self.prepare_next_if_needed(output);
            return Ok(());
        }

        let resume_existing_current = was_paused
            && self
                .current
                .as_ref()
                .is_some_and(PlaybackAttempt::is_active);
        let current_ready = self.current.as_ref().is_some_and(PlaybackAttempt::is_ready);
        if self.play_state == PlayState::Paused && current_ready {
            self.play_state = PlayState::Playing;
        } else if self.play_state != PlayState::Playing {
            self.play_state = PlayState::Buffering;
            if let Some(current) = self.current.as_mut()
                && !current.is_active()
            {
                let watchdog_epoch = current.start();
                output.actions.push(TaskAction::StartCurrent {
                    generation: current.generation,
                    watchdog_epoch,
                    track: current.source.clone(),
                });
            }
        }
        if resume_existing_current && let Some(current) = self.current.as_ref() {
            output.actions.push(TaskAction::ResumeCurrent {
                generation: current.generation,
            });
        }
        if was_paused {
            self.arm_resumed_watchdogs(output);
        }

        self.prepare_next_if_needed(output);
        Ok(())
    }

    fn pause(&mut self, output: &mut ActorEffects) -> Result<()> {
        if self
            .current
            .as_ref()
            .is_some_and(|current| current.source.is_live())
        {
            return Err(MusicStreamError::Unsupported(
                "live sources cannot be paused without a timeshift store".to_owned(),
            ));
        }
        if matches!(self.play_state, PlayState::Playing | PlayState::Buffering) {
            if let Some(current) = self.current.as_ref()
                && current.is_active()
            {
                output.actions.push(TaskAction::PauseCurrent {
                    generation: current.generation,
                });
            } else if let Some(next) = self.next.as_ref()
                && next.is_active()
            {
                output.actions.push(TaskAction::PauseNext {
                    generation: next.generation,
                });
            }
            if let Some(current) = self.current.as_mut() {
                current.suspend_watchdog();
            }
            if let Some(next) = self.next.as_mut() {
                next.suspend_watchdog();
            }
            self.play_state = PlayState::Paused;
        }
        Ok(())
    }

    fn stop(&mut self, output: &mut ActorEffects) {
        if self.play_state == PlayState::Stopped {
            return;
        }

        self.current = None;
        self.next = None;
        output.actions.push(TaskAction::StopSender);
        self.refreshable_current_key = None;
        self.play_state = PlayState::Stopped;
        output.events.push(StreamEvent::StreamStopped {
            stream_id: self.stream_id.clone(),
        });
    }

    fn seek(&mut self, seconds: u64, output: &mut ActorEffects) -> Result<()> {
        let preserve_pause = self.play_state == PlayState::Paused;
        let Some(current) = self.current.as_mut() else {
            return Err(MusicStreamError::Unsupported(
                "cannot seek without a current track".to_owned(),
            ));
        };

        if !current.source.is_seekable() {
            return Err(MusicStreamError::NotSeekable(current.source.id.clone()));
        }

        let old_generation = current.generation;
        self.generation += 1;
        current.generation = self.generation;
        current.state = AttemptState::Dormant;
        current.watchdog_epoch = 0;
        self.time_played_ms = seconds.saturating_mul(1_000);
        self.play_state = if preserve_pause {
            PlayState::Paused
        } else {
            PlayState::Buffering
        };

        if !preserve_pause {
            let watchdog_epoch = current.start();
            output.actions.push(TaskAction::StartCurrent {
                generation: current.generation,
                watchdog_epoch,
                track: current.source.clone(),
            });
        } else {
            output.actions.push(TaskAction::CancelCurrent {
                generation: old_generation,
            });
        }

        Ok(())
    }

    fn reconcile_next_attempt(
        &mut self,
        next: Option<TrackSource>,
        output: &mut ActorEffects,
    ) -> Result<()> {
        if next.as_ref().is_some_and(TrackSource::is_live) {
            return Err(MusicStreamError::Unsupported(
                "live sources cannot be preloaded as next without a timeshift model".to_owned(),
            ));
        }
        match (self.next.as_ref(), next) {
            (None, None) => {}
            (Some(old), None) => {
                output.actions.push(TaskAction::CancelNext {
                    generation: old.generation,
                });
                self.next = None;
            }
            (Some(old), Some(new_source)) if old.source.same_attempt_as(&new_source) => {
                if let Some(next) = self.next.as_mut() {
                    next.source = new_source;
                }
            }
            (old, Some(new_source)) => {
                let old_generation = old.map(|slot| slot.generation);
                self.generation += 1;
                let mut slot = PlaybackAttempt::new(new_source.clone(), self.generation);
                if self.play_state != PlayState::Paused {
                    let watchdog_epoch = slot.start();
                    output.actions.push(TaskAction::PrepareNext {
                        generation: slot.generation,
                        watchdog_epoch,
                        track: new_source,
                    });
                } else if let Some(generation) = old_generation {
                    output.actions.push(TaskAction::CancelNext { generation });
                }
                self.next = Some(slot);
            }
        }
        Ok(())
    }

    fn refresh_current_source(
        &mut self,
        current: TrackSource,
        output: &mut ActorEffects,
    ) -> Result<()> {
        let preserve_pause = self.play_state == PlayState::Paused;
        let mut old_generation = None;
        if let Some(old_current) = self.current.take() {
            if !old_current.source.same_attempt_as(&current) {
                self.current = Some(old_current);
                return Err(MusicStreamError::InvalidSource(
                    "refreshed current source must keep the current track id".to_owned(),
                ));
            }
            old_generation = Some(old_current.generation);
        } else if !self.can_refresh_current_source(&current) {
            return Err(MusicStreamError::InvalidSource(
                "refreshed current source must keep the current track id".to_owned(),
            ));
        }

        self.refreshable_current_key = None;
        self.generation += 1;
        let mut current_slot = PlaybackAttempt::new(current.clone(), self.generation);
        if !preserve_pause {
            let watchdog_epoch = current_slot.start();
            output.actions.push(TaskAction::StartCurrent {
                generation: current_slot.generation,
                watchdog_epoch,
                track: current,
            });
        } else if let Some(generation) = old_generation {
            output
                .actions
                .push(TaskAction::CancelCurrent { generation });
        }
        self.current = Some(current_slot);
        self.time_played_ms = 0;
        self.play_state = if preserve_pause {
            PlayState::Paused
        } else {
            PlayState::Buffering
        };

        Ok(())
    }

    fn reconcile_plan(
        &mut self,
        version: u64,
        current: Option<TrackSource>,
        next: Option<TrackSource>,
        output: &mut ActorEffects,
    ) -> Result<()> {
        if version <= self.plan_version {
            return Ok(());
        }
        if next.as_ref().is_some_and(TrackSource::is_live) {
            return Err(MusicStreamError::Unsupported(
                "live sources cannot be preloaded as next without a timeshift model".to_owned(),
            ));
        }

        let Some(current) = current else {
            if let Some(current) = self.current.take() {
                output.actions.push(TaskAction::CancelCurrent {
                    generation: current.generation,
                });
            }
            if let Some(next) = self.next.take() {
                output.actions.push(TaskAction::CancelNext {
                    generation: next.generation,
                });
            }
            self.refreshable_current_key = None;
            self.time_played_ms = 0;
            if self.play_state != PlayState::Paused {
                self.play_state = PlayState::Idle;
            }
            self.plan_version = version;
            return Ok(());
        };

        let current_unchanged = self
            .current
            .as_ref()
            .is_some_and(|attempt| attempt.source.same_attempt_as(&current));
        if current_unchanged {
            if let Some(active) = self.current.as_mut() {
                active.source = current;
            }
            self.reconcile_next_attempt(next, output)?;
            self.plan_version = version;
            return Ok(());
        }

        let promote_planned_next = self
            .next
            .as_ref()
            .is_some_and(|attempt| attempt.source.same_attempt_as(&current));
        if promote_planned_next {
            let preserve_pause = self.play_state == PlayState::Paused;
            self.remember_refreshable_current();
            let mut promoted = self.next.take().expect("matched planned next");
            promoted.source = current;
            let watchdog_epoch = promoted.start();
            let generation = promoted.generation;
            let track = promoted.source.clone();
            self.current = Some(promoted);
            self.time_played_ms = 0;
            self.play_state = if preserve_pause {
                PlayState::Paused
            } else {
                PlayState::Buffering
            };
            output.actions.push(TaskAction::StartCurrent {
                generation,
                watchdog_epoch,
                track,
            });
            self.reconcile_next_attempt(next, output)?;
            self.plan_version = version;
            return Ok(());
        }

        self.replace_current(current, next, output)?;
        self.plan_version = version;
        Ok(())
    }

    fn replace_current(
        &mut self,
        current: TrackSource,
        next: Option<TrackSource>,
        output: &mut ActorEffects,
    ) -> Result<()> {
        let preserve_pause = self.play_state == PlayState::Paused;
        let old_current_generation = self.current.take().map(|attempt| attempt.generation);
        let old_next_generation = self.next.take().map(|attempt| attempt.generation);

        self.refreshable_current_key = None;
        self.generation += 1;
        let mut current_attempt = PlaybackAttempt::new(current.clone(), self.generation);
        if !preserve_pause {
            let watchdog_epoch = current_attempt.start();
            output.actions.push(TaskAction::StartCurrent {
                generation: current_attempt.generation,
                watchdog_epoch,
                track: current,
            });
        } else if let Some(generation) = old_current_generation {
            output
                .actions
                .push(TaskAction::CancelCurrent { generation });
        }
        self.current = Some(current_attempt);

        if let Some(next_source) = next {
            self.generation += 1;
            let mut next_attempt = PlaybackAttempt::new(next_source.clone(), self.generation);
            if !preserve_pause {
                let watchdog_epoch = next_attempt.start();
                output.actions.push(TaskAction::PrepareNext {
                    generation: next_attempt.generation,
                    watchdog_epoch,
                    track: next_source,
                });
            }
            self.next = Some(next_attempt);
        } else if let Some(generation) = old_next_generation {
            output.actions.push(TaskAction::CancelNext { generation });
        }

        self.time_played_ms = 0;
        self.play_state = if preserve_pause {
            PlayState::Paused
        } else {
            PlayState::Buffering
        };
        Ok(())
    }

    fn set_volume(&mut self, volume: VolumeLevel, output: &mut ActorEffects) {
        self.volume = volume;
        if let Some(current) = self.current.as_ref()
            && current.is_active()
        {
            output.actions.push(TaskAction::SetCurrentVolume {
                generation: current.generation,
                volume,
            });
        }
        if let Some(next) = self.next.as_ref()
            && next.is_active()
        {
            output.actions.push(TaskAction::SetNextVolume {
                generation: next.generation,
                volume,
            });
        }
    }

    fn set_gain(&mut self, gain: GainLevel, output: &mut ActorEffects) {
        self.gain = gain;
        if let Some(current) = self.current.as_ref()
            && current.is_active()
        {
            output.actions.push(TaskAction::SetCurrentGain {
                generation: current.generation,
                gain,
            });
        }
        if let Some(next) = self.next.as_ref()
            && next.is_active()
        {
            output.actions.push(TaskAction::SetNextGain {
                generation: next.generation,
                gain,
            });
        }
    }

    fn promote_next_or_wait(&mut self, output: &mut ActorEffects, request_next_when_empty: bool) {
        let preserve_pause = self.play_state == PlayState::Paused;
        if self.promote_ready_next(output) {
            return;
        }

        self.remember_refreshable_current();
        self.current = None;
        if self.next.is_some() {
            self.play_state = if preserve_pause {
                PlayState::Paused
            } else {
                PlayState::Buffering
            };
        } else {
            self.play_state = PlayState::Idle;
            if request_next_when_empty {
                output.events.push(StreamEvent::NextNeeded {
                    stream_id: self.stream_id.clone(),
                });
            }
        }
    }

    fn promote_ready_next(&mut self, output: &mut ActorEffects) -> bool {
        if !self.next.as_ref().is_some_and(PlaybackAttempt::is_ready) {
            return false;
        }

        let preserve_pause = self.play_state == PlayState::Paused;
        let Some(next) = self.next.take() else {
            return false;
        };

        self.refreshable_current_key = None;
        self.current = Some(next);
        self.time_played_ms = 0;
        self.play_state = if preserve_pause {
            PlayState::Paused
        } else {
            PlayState::Buffering
        };
        if let Some(current) = self.current.as_mut() {
            let watchdog_epoch = current.start();
            output.actions.push(TaskAction::StartCurrent {
                generation: current.generation,
                watchdog_epoch,
                track: current.source.clone(),
            });
        }

        true
    }

    fn handle_track_failure(
        &mut self,
        code: ErrorCode,
        message: String,
        output: &mut ActorEffects,
    ) {
        if let Some(current) = self.current.as_ref() {
            output.actions.push(TaskAction::CancelCurrent {
                generation: current.generation,
            });
        }
        let refresh_current = code == ErrorCode::SourceAuthExpired && self.next.is_none();
        if refresh_current && let Some(current) = self.current.as_ref() {
            output.events.push(StreamEvent::SourceRefreshNeeded {
                stream_id: self.stream_id.clone(),
                attempt_id: current.source.attempt_key().to_owned(),
                track_id: current.source.id.clone(),
                source_role: SourceRole::Current,
                generation: current.generation,
            });
        }
        if let Some(current) = self.current.as_ref() {
            output.events.push(StreamEvent::AttemptFailed {
                stream_id: self.stream_id.clone(),
                attempt_id: current.source.attempt_key().to_owned(),
                track_id: current.source.id.clone(),
                source_role: SourceRole::Current,
                generation: current.generation,
                code,
                message,
            });
        }

        self.promote_next_or_wait(output, !refresh_current);
    }

    fn handle_next_failure(
        &mut self,
        generation: u64,
        code: ErrorCode,
        message: String,
        output: &mut ActorEffects,
    ) {
        let Some(failed) = self
            .next
            .as_ref()
            .filter(|attempt| attempt.generation == generation && attempt.is_active())
        else {
            return;
        };
        let attempt_id = failed.source.attempt_key().to_owned();
        let track_id = failed.source.id.clone();
        if code == ErrorCode::SourceAuthExpired && self.current.is_some() {
            output.events.push(StreamEvent::SourceRefreshNeeded {
                stream_id: self.stream_id.clone(),
                attempt_id: attempt_id.clone(),
                track_id: track_id.clone(),
                source_role: SourceRole::Next,
                generation,
            });
        }
        output.actions.push(TaskAction::CancelNext { generation });
        self.next = None;
        if self.current.is_none() {
            if self.play_state != PlayState::Paused {
                self.play_state = PlayState::Idle;
            }
            output.events.push(StreamEvent::NextNeeded {
                stream_id: self.stream_id.clone(),
            });
        }
        output.events.push(StreamEvent::AttemptFailed {
            stream_id: self.stream_id.clone(),
            attempt_id,
            track_id,
            source_role: SourceRole::Next,
            generation,
            code,
            message,
        });
    }

    fn handle_startup_timeout(
        &mut self,
        source_role: SourceRole,
        generation: u64,
        watchdog_epoch: u64,
        output: &mut ActorEffects,
    ) {
        if self.play_state == PlayState::Paused {
            return;
        }
        let is_current_timeout = self.current.as_ref().is_some_and(|attempt| {
            source_role == SourceRole::Current
                && attempt.generation == generation
                && attempt.state == AttemptState::Starting
                && attempt.watchdog_epoch == watchdog_epoch
        });
        if is_current_timeout {
            self.handle_track_failure(
                ErrorCode::SourceTimeout,
                "playback attempt did not become ready before its startup deadline".to_owned(),
                output,
            );
            return;
        }
        let is_next_timeout = self.next.as_ref().is_some_and(|attempt| {
            source_role == SourceRole::Next
                && attempt.generation == generation
                && attempt.state == AttemptState::Starting
                && attempt.watchdog_epoch == watchdog_epoch
        });
        if is_next_timeout {
            self.handle_next_failure(
                generation,
                ErrorCode::SourceTimeout,
                "preload attempt did not become ready before its startup deadline".to_owned(),
                output,
            );
        }
    }

    fn arm_resumed_watchdogs(&mut self, output: &mut ActorEffects) {
        for (source_role, attempt) in [
            (SourceRole::Current, self.current.as_mut()),
            (SourceRole::Next, self.next.as_mut()),
        ] {
            let Some(attempt) = attempt else {
                continue;
            };
            if let Some(watchdog_epoch) = attempt.resume_watchdog() {
                output.actions.push(TaskAction::ArmStartupDeadline {
                    source_role,
                    generation: attempt.generation,
                    watchdog_epoch,
                });
            }
        }
    }

    fn is_current_generation(&self, generation: u64) -> bool {
        self.current
            .as_ref()
            .is_some_and(|slot| slot.generation == generation)
    }

    fn remember_refreshable_current(&mut self) {
        self.refreshable_current_key = self
            .current
            .as_ref()
            .map(|slot| slot.source.attempt_key().to_owned());
    }

    fn can_refresh_current_source(&self, current: &TrackSource) -> bool {
        self.refreshable_current_key
            .as_deref()
            .is_some_and(|key| key == current.attempt_key())
    }

    fn prepare_next_if_needed(&mut self, output: &mut ActorEffects) {
        if let Some(next) = self.next.as_mut()
            && !next.is_active()
        {
            let watchdog_epoch = next.start();
            output.actions.push(TaskAction::PrepareNext {
                generation: next.generation,
                watchdog_epoch,
                track: next.source.clone(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TrackKind;

    fn track(id: &str) -> TrackSource {
        TrackSource {
            attempt_id: format!("attempt-{id}"),
            id: id.to_owned(),
            kind: TrackKind::File,
            url: None,
            path: Some(format!("/tmp/{id}.mp3")),
            format_hint: None,
            seekable: Some(true),
            headers: Default::default(),
            network_policy: crate::model::NetworkPolicy::Provider,
        }
    }

    fn url_track(id: &str) -> TrackSource {
        TrackSource {
            attempt_id: format!("attempt-{id}"),
            id: id.to_owned(),
            kind: TrackKind::Url,
            url: Some(format!("https://example.test/{id}.mp3")),
            path: None,
            format_hint: None,
            seekable: None,
            headers: Default::default(),
            network_policy: crate::model::NetworkPolicy::Provider,
        }
    }

    fn live_track(id: &str, url: &str) -> TrackSource {
        TrackSource {
            attempt_id: format!("attempt-{id}"),
            id: id.to_owned(),
            kind: TrackKind::Live,
            url: Some(url.to_owned()),
            path: None,
            format_hint: None,
            seekable: Some(false),
            headers: Default::default(),
            network_policy: crate::model::NetworkPolicy::Provider,
        }
    }

    fn malformed_seekable_live_track(id: &str, url: &str) -> TrackSource {
        TrackSource {
            attempt_id: format!("attempt-{id}"),
            id: id.to_owned(),
            kind: TrackKind::Live,
            url: Some(url.to_owned()),
            path: None,
            format_hint: None,
            seekable: Some(true),
            headers: Default::default(),
            network_policy: crate::model::NetworkPolicy::Provider,
        }
    }

    fn actor(
        stream_id: String,
        current: Option<TrackSource>,
        next: Option<TrackSource>,
    ) -> StreamActor {
        let mut actor = StreamActor::new(stream_id, current);
        if let Some(next) = next {
            actor.generation += 1;
            actor.next = Some(PlaybackAttempt::new(next, actor.generation));
        }
        actor
    }

    #[test]
    fn desired_plan_replacement_increments_generation_and_drops_stale_worker_events() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), None);
        let old_generation = actor.current_generation();

        let output = actor
            .handle_command(StreamCommand::ReconcilePlan {
                version: 1,
                current: Some(track("b")),
                next: None,
            })
            .expect("switch should succeed");

        let new_generation = actor.current_generation();
        assert!(new_generation > old_generation);
        assert!(output.actions.contains(&TaskAction::StartCurrent {
            generation: new_generation,
            watchdog_epoch: 1,
            track: track("b"),
        }));
        assert!(!output.actions.iter().any(|action| matches!(
            action,
            TaskAction::CancelCurrent { generation } if *generation == old_generation
        )));

        let stale = actor.handle_worker_event(WorkerEvent::CurrentEnded {
            generation: old_generation,
        });
        assert!(
            !stale
                .events
                .iter()
                .any(|event| matches!(event, StreamEvent::NextNeeded { .. }))
        );
        assert_eq!(actor.current_generation(), new_generation);
    }

    #[test]
    fn runtime_action_failure_forces_a_clean_terminal_state() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), Some(track("b")));
        actor.handle_command(StreamCommand::Play).expect("play");

        let output = actor.handle_runtime_failure(&MusicStreamError::RtpOutputError(
            "sender closed".to_owned(),
        ));

        assert_eq!(output.status.play_state, PlayState::Stopped);
        assert!(output.status.current.is_none());
        assert!(output.status.next.is_none());
        assert!(output.actions.is_empty());
        assert!(output.events.iter().any(|event| matches!(
            event,
            StreamEvent::Error {
                code: ErrorCode::OutputError,
                ..
            }
        )));
    }

    #[test]
    fn next_never_promotes_before_ready() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), Some(track("b")));
        let current_generation = actor.current_generation();
        let output = actor.handle_worker_event(WorkerEvent::CurrentEnded {
            generation: current_generation,
        });

        assert!(matches!(output.status.play_state, PlayState::Buffering));
        assert!(actor.current.is_none());
        assert_eq!(actor.next.as_ref().expect("next retained").source.id, "b");
    }

    #[test]
    fn pause_during_cross_track_gap_freezes_next_and_survives_promotion() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), Some(track("b")));
        actor.handle_command(StreamCommand::Play).expect("play");
        let current_generation = actor.current_generation();
        let next_generation = actor.next.as_ref().expect("next").generation;
        actor.handle_worker_event(WorkerEvent::CurrentEnded {
            generation: current_generation,
        });

        let paused = actor.handle_command(StreamCommand::Pause).expect("pause");
        assert_eq!(paused.status.play_state, PlayState::Paused);
        assert_eq!(
            paused.actions,
            vec![TaskAction::PauseNext {
                generation: next_generation
            }]
        );

        let promoted = actor.handle_worker_event(WorkerEvent::NextReady {
            generation: next_generation,
        });
        assert_eq!(promoted.status.play_state, PlayState::Paused);
        assert_eq!(promoted.status.current.expect("promoted").id, "b");
        assert!(promoted.actions.contains(&TaskAction::StartCurrent {
            generation: next_generation,
            watchdog_epoch: 3,
            track: track("b")
        }));

        let resumed = actor.handle_command(StreamCommand::Play).expect("resume");
        assert_eq!(resumed.status.play_state, PlayState::Buffering);
        assert!(resumed.actions.contains(&TaskAction::ResumeCurrent {
            generation: next_generation
        }));
        assert!(resumed.actions.contains(&TaskAction::ArmStartupDeadline {
            source_role: SourceRole::Current,
            generation: next_generation,
            watchdog_epoch: 4,
        }));
    }

    #[test]
    fn resume_during_cross_track_gap_restarts_next_without_requesting_another_track() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), Some(track("b")));
        actor.handle_command(StreamCommand::Play).expect("play");
        let current_generation = actor.current_generation();
        let next_generation = actor.next.as_ref().expect("next").generation;
        actor.handle_worker_event(WorkerEvent::CurrentEnded {
            generation: current_generation,
        });
        actor.handle_command(StreamCommand::Pause).expect("pause");

        let resumed = actor.handle_command(StreamCommand::Play).expect("resume");

        assert_eq!(resumed.status.play_state, PlayState::Buffering);
        assert_eq!(
            resumed.actions,
            vec![
                TaskAction::ResumeNext {
                    generation: next_generation
                },
                TaskAction::ArmStartupDeadline {
                    source_role: SourceRole::Next,
                    generation: next_generation,
                    watchdog_epoch: 3,
                }
            ]
        );
        assert!(
            !resumed
                .events
                .iter()
                .any(|event| matches!(event, StreamEvent::NextNeeded { .. }))
        );
    }

    #[test]
    fn live_track_seek_is_rejected_even_when_input_marks_seekable() {
        let mut actor = actor(
            "s1".to_owned(),
            Some(malformed_seekable_live_track(
                "live-a",
                "https://example.test/live.wav",
            )),
            None,
        );

        let error = actor
            .handle_command(StreamCommand::Seek { seconds: 5 })
            .expect_err("live seek should be rejected");

        assert_eq!(error.code(), ErrorCode::NotSeekable);
        assert_eq!(actor.current_generation(), 1);
    }

    #[test]
    fn ready_next_promotes_on_current_end() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), Some(track("b")));
        actor.handle_command(StreamCommand::Play).expect("play");
        let current_generation = actor.current_generation();
        let next_generation = actor.next.as_ref().expect("next").generation;

        actor.handle_worker_event(WorkerEvent::NextReady {
            generation: next_generation,
        });
        let output = actor.handle_worker_event(WorkerEvent::CurrentEnded {
            generation: current_generation,
        });

        assert_eq!(output.status.current.expect("current").id, "b");
        assert!(output.actions.contains(&TaskAction::StartCurrent {
            generation: next_generation,
            watchdog_epoch: 2,
            track: track("b")
        }));
    }

    #[test]
    fn next_failure_after_current_end_exits_buffering_and_requests_replacement() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), Some(track("b")));
        actor.handle_command(StreamCommand::Play).expect("play");
        let current_generation = actor.current_generation();
        let next_generation = actor.next.as_ref().expect("next").generation;
        actor.handle_worker_event(WorkerEvent::CurrentEnded {
            generation: current_generation,
        });

        let output = actor.handle_worker_event(WorkerEvent::NextFailed {
            generation: next_generation,
            code: ErrorCode::SourceTimeout,
            message: "preload timed out".to_owned(),
        });

        assert_eq!(output.status.play_state, PlayState::Idle);
        assert!(output.status.current.is_none());
        assert!(output.status.next.is_none());
        assert!(output.events.iter().any(
            |event| matches!(event, StreamEvent::NextNeeded { stream_id } if stream_id == "s1")
        ));
        let state_index = output
            .events
            .iter()
            .position(|event| matches!(event, StreamEvent::StateChanged { .. }))
            .expect("state event");
        let request_index = output
            .events
            .iter()
            .position(|event| matches!(event, StreamEvent::NextNeeded { .. }))
            .expect("next request");
        assert!(state_index < request_index);
    }

    #[test]
    fn next_startup_timeout_exits_buffering_with_exact_attempt_identity() {
        let mut next = track("media-b");
        next.attempt_id = "queue-entry-b".to_owned();
        let mut actor = actor("s1".to_owned(), Some(track("a")), Some(next));
        actor.handle_command(StreamCommand::Play).expect("play");
        let current_generation = actor.current_generation();
        let next = actor.next.as_ref().expect("next");
        let next_generation = next.generation;
        let watchdog_epoch = next.watchdog_epoch;
        actor.handle_worker_event(WorkerEvent::CurrentEnded {
            generation: current_generation,
        });

        let output = actor.handle_worker_event(WorkerEvent::StartupTimedOut {
            source_role: SourceRole::Next,
            generation: next_generation,
            watchdog_epoch,
        });

        assert_eq!(output.status.play_state, PlayState::Idle);
        assert!(output.status.current.is_none());
        assert!(output.status.next.is_none());
        assert!(output.events.iter().any(|event| matches!(
            event,
            StreamEvent::AttemptFailed {
                attempt_id,
                track_id,
                source_role: SourceRole::Next,
                generation,
                code: ErrorCode::SourceTimeout,
                ..
            } if attempt_id == "queue-entry-b"
                && track_id == "media-b"
                && *generation == next_generation
        )));
        let state_index = output
            .events
            .iter()
            .position(|event| matches!(event, StreamEvent::StateChanged { .. }))
            .expect("state event");
        let request_index = output
            .events
            .iter()
            .position(|event| matches!(event, StreamEvent::NextNeeded { .. }))
            .expect("next request");
        assert!(state_index < request_index);
    }

    #[test]
    fn paused_and_superseded_startup_deadlines_cannot_fail_a_resumed_attempt() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), None);
        let started = actor.handle_command(StreamCommand::Play).expect("play");
        let generation = actor.current_generation();
        let first_epoch = actor.current.as_ref().expect("current").watchdog_epoch;
        assert_eq!(started.status.play_state, PlayState::Buffering);
        actor.handle_command(StreamCommand::Pause).expect("pause");

        let paused_timeout = actor.handle_worker_event(WorkerEvent::StartupTimedOut {
            source_role: SourceRole::Current,
            generation,
            watchdog_epoch: first_epoch,
        });
        assert_eq!(paused_timeout.status.play_state, PlayState::Paused);
        assert!(
            paused_timeout
                .events
                .iter()
                .all(|event| !matches!(event, StreamEvent::AttemptFailed { .. }))
        );

        actor.handle_command(StreamCommand::Play).expect("resume");
        let stale_timeout = actor.handle_worker_event(WorkerEvent::StartupTimedOut {
            source_role: SourceRole::Current,
            generation,
            watchdog_epoch: first_epoch,
        });
        assert_eq!(stale_timeout.status.play_state, PlayState::Buffering);
        assert!(
            stale_timeout
                .events
                .iter()
                .all(|event| !matches!(event, StreamEvent::AttemptFailed { .. }))
        );
    }

    #[test]
    fn desired_plan_is_versioned_and_promotes_the_existing_attempt() {
        let mut current = track("media-a");
        current.attempt_id = "entry-a".to_owned();
        let mut next = track("media-b");
        next.attempt_id = "entry-b".to_owned();
        let mut actor = actor("s1".to_owned(), Some(current), Some(next.clone()));
        actor.handle_command(StreamCommand::Play).expect("play");
        let next_generation = actor.next.as_ref().expect("next").generation;

        let output = actor
            .handle_command(StreamCommand::ReconcilePlan {
                version: 2,
                current: Some(next),
                next: Some(track("media-c")),
            })
            .expect("plan");
        assert_eq!(output.status.plan_version, 2);
        assert_eq!(
            output
                .status
                .current
                .as_ref()
                .map(|track| track.attempt_id.as_str()),
            Some("entry-b")
        );
        assert_eq!(output.status.generation, next_generation);
        assert!(output.actions.iter().any(|action| matches!(
            action,
            TaskAction::StartCurrent { generation, track, .. }
                if *generation == next_generation && track.attempt_key() == "entry-b"
        )));

        let stale = actor
            .handle_command(StreamCommand::ReconcilePlan {
                version: 1,
                current: Some({
                    let mut stale = track("stale");
                    stale.attempt_id = "stale-entry".to_owned();
                    stale
                }),
                next: None,
            })
            .expect("stale plan");
        assert!(stale.actions.is_empty());
        assert_eq!(stale.status.plan_version, 2);
        assert_eq!(
            stale.status.current.expect("current").attempt_key(),
            "entry-b"
        );
    }

    #[test]
    fn paused_next_failure_preserves_pause_while_requesting_replacement() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), Some(track("b")));
        actor.handle_command(StreamCommand::Play).expect("play");
        let current_generation = actor.current_generation();
        let next_generation = actor.next.as_ref().expect("next").generation;
        actor.handle_worker_event(WorkerEvent::CurrentEnded {
            generation: current_generation,
        });
        actor.handle_command(StreamCommand::Pause).expect("pause");

        let output = actor.handle_worker_event(WorkerEvent::NextFailed {
            generation: next_generation,
            code: ErrorCode::SourceTimeout,
            message: "preload timed out".to_owned(),
        });

        assert_eq!(output.status.play_state, PlayState::Paused);
        assert!(output.events.iter().any(
            |event| matches!(event, StreamEvent::NextNeeded { stream_id } if stream_id == "s1")
        ));
    }

    #[test]
    fn seek_requires_seekable_current_and_creates_generation() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), None);
        let old_generation = actor.current_generation();
        let output = actor
            .handle_command(StreamCommand::Seek { seconds: 42 })
            .expect("seek should succeed");

        assert!(actor.current_generation() > old_generation);
        assert_eq!(output.status.time_played_ms, 42_000);
    }

    #[test]
    fn generation_replacements_preserve_explicit_pause() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), None);
        actor.handle_command(StreamCommand::Play).expect("play");
        actor.handle_command(StreamCommand::Pause).expect("pause");

        let seeked = actor
            .handle_command(StreamCommand::Seek { seconds: 7 })
            .expect("seek");
        assert_eq!(seeked.status.play_state, PlayState::Paused);
        assert!(
            !seeked
                .actions
                .iter()
                .any(|action| matches!(action, TaskAction::StartCurrent { .. }))
        );

        let switched = actor
            .handle_command(StreamCommand::ReconcilePlan {
                version: 1,
                current: Some(track("b")),
                next: None,
            })
            .expect("switch");
        assert_eq!(switched.status.play_state, PlayState::Paused);
        assert!(
            !switched
                .actions
                .iter()
                .any(|action| matches!(action, TaskAction::StartCurrent { .. }))
        );

        let refreshed = actor
            .handle_command(StreamCommand::RefreshCurrentSource {
                current: track("b"),
            })
            .expect("refresh");
        assert_eq!(refreshed.status.play_state, PlayState::Paused);
        assert!(
            !refreshed
                .actions
                .iter()
                .any(|action| matches!(action, TaskAction::StartCurrent { .. }))
        );

        let resumed = actor.handle_command(StreamCommand::Play).expect("resume");
        assert!(resumed.actions.iter().any(
            |action| matches!(action, TaskAction::StartCurrent { track, .. } if track.id == "b")
        ));
    }

    #[test]
    fn bounded_url_tracks_are_seekable_by_default() {
        let mut actor = actor("s1".to_owned(), Some(url_track("url-a")), None);
        actor.handle_command(StreamCommand::Play).expect("play");

        let output = actor
            .handle_command(StreamCommand::Seek { seconds: 3 })
            .expect("URL seek");

        let status = output.status;
        assert_eq!(status.time_played_ms, 3_000);
        assert!(output.actions.iter().any(
            |action| matches!(action, TaskAction::StartCurrent { track, .. } if track.id == "url-a")
        ));
    }

    #[test]
    fn live_pause_is_rejected_without_timeshift_storage() {
        let mut actor = actor(
            "s1".to_owned(),
            Some(live_track("live", "https://example.test/live")),
            None,
        );
        actor.handle_command(StreamCommand::Play).expect("play");

        let error = actor
            .handle_command(StreamCommand::Pause)
            .expect_err("live pause must fail");
        assert_eq!(error.code(), crate::error::ErrorCode::Unsupported);
    }

    #[test]
    fn live_next_is_rejected_without_a_timeshift_model() {
        let live = live_track("live-next", "https://example.test/live");
        let mut actor = actor("s1".to_owned(), Some(track("current")), None);
        actor.handle_command(StreamCommand::Play).expect("play");
        let error = actor
            .handle_command(StreamCommand::ReconcilePlan {
                version: 1,
                current: Some(track("current")),
                next: Some(live),
            })
            .expect_err("live next must be rejected");
        assert_eq!(error.code(), ErrorCode::Unsupported);
        assert!(actor.next.is_none());
    }

    #[test]
    fn playing_with_only_a_pending_next_starts_its_preload() {
        let next = track("next");
        let mut actor = actor("s1".to_owned(), None, Some(next.clone()));
        let next_generation = actor.next.as_ref().expect("next").generation;

        let output = actor.handle_command(StreamCommand::Play).expect("play");

        assert_eq!(output.status.play_state, PlayState::Buffering);
        assert_eq!(
            output.actions,
            vec![TaskAction::PrepareNext {
                generation: next_generation,
                watchdog_epoch: 1,
                track: next,
            }]
        );
    }

    #[test]
    fn set_volume_updates_status_and_current_slot_without_restarting_tasks() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), Some(track("b")));
        let generation = actor.current_generation();
        let next_generation = actor.next.as_ref().expect("next").generation;
        actor.handle_command(StreamCommand::Play).expect("play");
        let volume = VolumeLevel::from_unit(0.5).expect("volume");
        let output = actor
            .handle_command(StreamCommand::SetVolume { volume })
            .expect("set volume");

        assert_eq!(output.status.volume, volume);
        assert!(
            output
                .actions
                .contains(&TaskAction::SetCurrentVolume { generation, volume })
        );
        assert!(output.actions.contains(&TaskAction::SetNextVolume {
            generation: next_generation,
            volume
        }));
        assert!(
            output
                .actions
                .iter()
                .all(|action| !matches!(action, TaskAction::StartCurrent { .. }))
        );
    }

    #[test]
    fn set_volume_before_play_updates_status_without_runtime_action() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), None);
        let volume = VolumeLevel::from_unit(0.25).expect("volume");

        let output = actor
            .handle_command(StreamCommand::SetVolume { volume })
            .expect("set volume");

        assert_eq!(output.status.volume, volume);
        assert!(output.actions.is_empty());
    }

    #[test]
    fn set_gain_updates_status_and_current_slot_without_restarting_tasks() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), None);
        actor.handle_command(StreamCommand::Play).expect("play");
        let generation = actor.current_generation();

        let output = actor
            .handle_command(StreamCommand::SetGain {
                gain: GainLevel::from_db(3.0).expect("gain"),
            })
            .expect("gain");

        assert_eq!(output.status.gain, GainLevel::from_db(3.0).expect("gain"));
        assert_eq!(
            output.actions,
            vec![TaskAction::SetCurrentGain {
                generation,
                gain: GainLevel::from_db(3.0).expect("gain"),
            }]
        );
    }

    #[test]
    fn prebuffer_ready_while_paused_is_remembered_for_resume() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), None);
        let generation = actor.current_generation();
        actor.handle_command(StreamCommand::Play).expect("play");
        let paused = actor.handle_command(StreamCommand::Pause).expect("pause");
        assert_eq!(paused.status.play_state, PlayState::Paused);
        assert_eq!(
            paused.actions,
            vec![TaskAction::PauseCurrent { generation }]
        );

        let ready = actor.handle_worker_event(WorkerEvent::CurrentPrebufferReady { generation });
        assert_eq!(ready.status.play_state, PlayState::Paused);

        let resumed = actor.handle_command(StreamCommand::Play).expect("resume");
        assert_eq!(resumed.status.play_state, PlayState::Playing);
        assert!(
            resumed
                .actions
                .iter()
                .all(|action| !matches!(action, TaskAction::StartCurrent { .. }))
        );
        assert!(
            resumed
                .actions
                .contains(&TaskAction::ResumeCurrent { generation })
        );
    }

    #[test]
    fn current_network_quality_change_is_generation_filtered() {
        let mut actor = actor("s1".to_owned(), Some(track("a")), None);
        let generation = actor.current_generation();

        let stale = actor.handle_worker_event(WorkerEvent::CurrentNetworkQualityChanged {
            generation: generation + 1,
            quality: RtcpNetworkQualityLevel::Poor,
            snapshot: RtcpQualityWindowSnapshot::default(),
        });
        assert!(
            stale
                .events
                .iter()
                .all(|event| !matches!(event, StreamEvent::NetworkQualityChanged { .. }))
        );

        let current = actor.handle_worker_event(WorkerEvent::CurrentNetworkQualityChanged {
            generation,
            quality: RtcpNetworkQualityLevel::Degraded,
            snapshot: RtcpQualityWindowSnapshot {
                samples: 1,
                level: RtcpNetworkQualityLevel::Degraded,
                latest_fraction_lost: 13,
                latest_loss_percent: 5.078125,
                average_loss_percent: 5.078125,
                max_loss_percent: 5.078125,
                average_jitter_micros: 2_562,
                max_jitter_micros: 2_562,
                average_round_trip_time_micros: None,
                max_round_trip_time_micros: None,
            },
        });
        assert!(current.events.iter().any(|event| matches!(
            event,
            StreamEvent::NetworkQualityChanged {
                stream_id,
                quality: RtcpNetworkQualityLevel::Degraded,
                snapshot,
            } if stream_id == "s1"
                && snapshot.samples == 1
                && snapshot.latest_fraction_lost == 13
        )));
    }

    #[test]
    fn current_auth_expiry_requests_source_refresh_with_stable_error_code() {
        let mut actor = actor("s1".to_owned(), Some(url_track("current")), None);
        let generation = actor.current_generation();

        let output = actor.handle_worker_event(WorkerEvent::CurrentFailed {
            generation,
            code: ErrorCode::SourceAuthExpired,
            message: "expired".to_owned(),
        });

        assert!(output.events.iter().any(|event| {
            matches!(
                event,
                StreamEvent::SourceRefreshNeeded { stream_id, track_id, source_role, .. }
                    if stream_id == "s1"
                        && track_id == "current"
                        && *source_role == SourceRole::Current
            )
        }));
        assert!(output.events.iter().any(|event| {
            matches!(
                event,
                StreamEvent::AttemptFailed { code, message, .. }
                    if *code == ErrorCode::SourceAuthExpired && message == "expired"
            )
        }));
        assert!(
            !output
                .events
                .iter()
                .any(|event| matches!(event, StreamEvent::NextNeeded { .. }))
        );
        let state_index = output
            .events
            .iter()
            .position(|event| matches!(event, StreamEvent::StateChanged { .. }))
            .expect("state event");
        let refresh_index = output
            .events
            .iter()
            .position(|event| matches!(event, StreamEvent::SourceRefreshNeeded { .. }))
            .expect("refresh request");
        assert!(state_index < refresh_index);
    }

    #[test]
    fn current_source_failure_without_next_reports_error_and_next_needed() {
        let mut actor = actor("s1".to_owned(), Some(url_track("current")), None);
        let generation = actor.current_generation();

        let output = actor.handle_worker_event(WorkerEvent::CurrentFailed {
            generation,
            code: ErrorCode::InvalidSource,
            message: "retry exhausted".to_owned(),
        });

        let status = output.status;
        assert_eq!(status.play_state, PlayState::Idle);
        assert!(status.current.is_none());
        assert!(
            output
                .actions
                .contains(&TaskAction::CancelCurrent { generation })
        );
        assert!(output.events.iter().any(|event| {
            matches!(
                event,
                StreamEvent::AttemptFailed { code, message, .. }
                    if *code == ErrorCode::InvalidSource && message == "retry exhausted"
            )
        }));
        assert!(output.events.iter().any(|event| {
            matches!(event, StreamEvent::NextNeeded { stream_id } if stream_id == "s1")
        }));
    }

    #[test]
    fn refresh_current_source_rejects_track_id_change_after_current_was_cleared() {
        let mut actor = actor("s1".to_owned(), Some(url_track("current")), None);
        let generation = actor.current_generation();
        actor.handle_worker_event(WorkerEvent::CurrentFailed {
            generation,
            code: ErrorCode::InvalidSource,
            message: "retry exhausted".to_owned(),
        });

        let error = actor
            .handle_command(StreamCommand::RefreshCurrentSource {
                current: live_track("other", "https://new.example.test/live.wav"),
            })
            .expect_err("refresh must keep the cleared current identity");

        assert_eq!(error.code(), ErrorCode::InvalidSource);
        assert!(actor.status().current.is_none());
    }

    #[test]
    fn refresh_current_source_requires_active_or_recent_current_identity() {
        let mut actor = actor("s1".to_owned(), None, None);

        let error = actor
            .handle_command(StreamCommand::RefreshCurrentSource {
                current: live_track("current", "https://new.example.test/live.wav"),
            })
            .expect_err("refresh without a current identity should be rejected");

        assert_eq!(error.code(), ErrorCode::InvalidSource);
        assert!(actor.status().current.is_none());
    }

    #[test]
    fn next_auth_expiry_requests_source_refresh_for_next_track() {
        let mut actor = actor(
            "s1".to_owned(),
            Some(track("current")),
            Some(url_track("next")),
        );
        actor.handle_command(StreamCommand::Play).expect("play");
        let generation = actor.next.as_ref().expect("next").generation;

        let output = actor.handle_worker_event(WorkerEvent::NextFailed {
            generation,
            code: ErrorCode::SourceAuthExpired,
            message: "expired".to_owned(),
        });

        assert!(output.events.iter().any(|event| {
            matches!(
                event,
                StreamEvent::SourceRefreshNeeded { stream_id, track_id, source_role, .. }
                    if stream_id == "s1"
                        && track_id == "next"
                        && *source_role == SourceRole::Next
            )
        }));
        assert!(actor.next.is_none());
    }

    #[test]
    fn refresh_current_source_restarts_idle_stream_and_preserves_next() {
        let mut actor = actor(
            "s1".to_owned(),
            Some(live_track("current", "https://old.example.test/live.wav")),
            Some(track("next")),
        );
        let failed_generation = actor.current_generation();
        actor.handle_worker_event(WorkerEvent::CurrentFailed {
            generation: failed_generation,
            code: ErrorCode::SourceAuthExpired,
            message: "expired".to_owned(),
        });

        let output = actor
            .handle_command(StreamCommand::RefreshCurrentSource {
                current: live_track("current", "https://new.example.test/live.wav"),
            })
            .expect("refresh current source");

        let status = output.status;
        assert_eq!(status.play_state, PlayState::Buffering);
        assert_eq!(
            status.current.expect("current").url.as_deref(),
            Some("https://new.example.test/live.wav")
        );
        assert_eq!(status.next.expect("next").id, "next");
        assert!(output.actions.iter().any(
            |action| matches!(action, TaskAction::StartCurrent { track, .. } if track.url.as_deref() == Some("https://new.example.test/live.wav"))
        ));
    }

    #[test]
    fn refresh_current_source_can_restart_same_track_after_current_end() {
        let mut actor = actor(
            "s1".to_owned(),
            Some(live_track("current", "https://old.example.test/live.wav")),
            None,
        );
        let ended_generation = actor.current_generation();
        actor.handle_worker_event(WorkerEvent::CurrentEnded {
            generation: ended_generation,
        });

        let output = actor
            .handle_command(StreamCommand::RefreshCurrentSource {
                current: live_track("current", "https://new.example.test/live.wav"),
            })
            .expect("same current identity can be refreshed after end");

        let status = output.status;
        assert_eq!(status.play_state, PlayState::Buffering);
        assert_eq!(
            status.current.expect("current").url.as_deref(),
            Some("https://new.example.test/live.wav")
        );
        assert!(output.actions.iter().any(
            |action| matches!(action, TaskAction::StartCurrent { track, .. } if track.id == "current")
        ));
    }

    #[test]
    fn refresh_current_source_rejects_track_id_change_while_current_is_active() {
        let mut actor = actor(
            "s1".to_owned(),
            Some(live_track("current", "https://old.example.test/live.wav")),
            None,
        );

        let error = actor
            .handle_command(StreamCommand::RefreshCurrentSource {
                current: live_track("other", "https://new.example.test/live.wav"),
            })
            .expect_err("track id change should be rejected");

        assert_eq!(error.code(), ErrorCode::InvalidSource);
        assert_eq!(
            actor.status().current.expect("current").id,
            "current".to_owned()
        );
    }
}
