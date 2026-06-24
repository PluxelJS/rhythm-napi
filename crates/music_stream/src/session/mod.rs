//! Per-stream actor state machine, playout slots, and generation control.
//!
//! Playlist policy stays in TypeScript. A session only manages current and next
//! track slots for one realtime stream.

mod mailbox;

use crate::error::{ErrorCode, MusicStreamError, Result};
use crate::event::StreamEvent;
use crate::model::{GainLevel, PlayState, StreamStatus, TrackSource, VolumeLevel};
use crate::quality::{RtcpNetworkQualityLevel, RtcpQualityWindowSnapshot};

pub use mailbox::{
    DEFAULT_STREAM_ACTOR_MAILBOX_CAPACITY, StreamActorMailbox, StreamActorMailboxHandle,
    StreamActorMailboxReply,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamCommand {
    Play,
    Pause,
    Stop,
    Seek {
        seconds: u64,
    },
    SetNext(Option<TrackSource>),
    SwitchTrack {
        current: TrackSource,
        next: Option<TrackSource>,
    },
    RefreshCurrentSource {
        current: TrackSource,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskAction {
    StartCurrent {
        generation: u64,
        track: TrackSource,
    },
    CancelCurrent {
        generation: u64,
    },
    PrepareNext {
        generation: u64,
        track: TrackSource,
    },
    CancelNext {
        generation: u64,
    },
    PauseCurrent {
        generation: u64,
    },
    ResumeCurrent {
        generation: u64,
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
        ActorOutput {
            actions: self.actions,
            events: self.events,
            status,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TrackSlot {
    source: TrackSource,
    generation: u64,
    ready: bool,
    task_active: bool,
}

impl TrackSlot {
    fn new(source: TrackSource, generation: u64) -> Self {
        Self {
            source,
            generation,
            ready: false,
            task_active: false,
        }
    }
}

#[derive(Debug)]
pub struct StreamActor {
    stream_id: String,
    current: Option<TrackSlot>,
    next: Option<TrackSlot>,
    refreshable_current_key: Option<String>,
    play_state: PlayState,
    generation: u64,
    time_played_ms: u64,
    time_total_ms: Option<u64>,
    volume: VolumeLevel,
    gain: GainLevel,
    consecutive_track_errors: u8,
    max_consecutive_track_errors: u8,
}

impl StreamActor {
    #[must_use]
    pub fn new(stream_id: String, current: Option<TrackSource>, next: Option<TrackSource>) -> Self {
        let mut generation = 0;
        let current = current.map(|source| {
            generation += 1;
            TrackSlot::new(source, generation)
        });
        let next = next.map(|source| {
            generation += 1;
            TrackSlot::new(source, generation)
        });

        Self {
            stream_id,
            current,
            next,
            refreshable_current_key: None,
            play_state: PlayState::Idle,
            generation,
            time_played_ms: 0,
            time_total_ms: None,
            volume: VolumeLevel::default(),
            gain: GainLevel::default(),
            consecutive_track_errors: 0,
            max_consecutive_track_errors: 3,
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
            time_total_ms: self.time_total_ms,
            generation: self.current_generation(),
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
            StreamCommand::Pause => self.pause(&mut output),
            StreamCommand::Stop => self.stop(&mut output),
            StreamCommand::Seek { seconds } => self.seek(seconds, &mut output)?,
            StreamCommand::SetNext(next) => self.set_next(next, &mut output),
            StreamCommand::SwitchTrack { current, next } => {
                self.switch_track(current, next, &mut output);
            }
            StreamCommand::RefreshCurrentSource { current } => {
                self.refresh_current_source(current, &mut output)?;
            }
            StreamCommand::SetVolume { volume } => self.set_volume(volume, &mut output),
            StreamCommand::SetGain { gain } => self.set_gain(gain, &mut output),
        }

        let status = self.status();
        output.events.push(StreamEvent::StateChanged {
            status: status.clone(),
        });
        Ok(output.into_output(status))
    }

    pub fn handle_worker_event(&mut self, event: WorkerEvent) -> ActorOutput {
        let mut output = ActorEffects::default();

        match event {
            WorkerEvent::CurrentPrebufferReady { generation } => {
                if self.is_current_generation(generation) {
                    if let Some(current) = self.current.as_mut() {
                        current.ready = true;
                    }
                    if self.play_state != PlayState::Paused {
                        self.play_state = PlayState::Playing;
                    }
                    self.consecutive_track_errors = 0;
                }
            }
            WorkerEvent::CurrentEnded { generation } => {
                if self.is_current_generation(generation) {
                    self.promote_next_or_wait(&mut output);
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
                {
                    next.ready = true;
                    if self.current.is_none() {
                        self.promote_ready_next(&mut output);
                    }
                }
            }
            WorkerEvent::NextFailed {
                generation,
                code,
                message,
            } => {
                if self
                    .next
                    .as_ref()
                    .is_some_and(|slot| slot.generation == generation)
                {
                    if code == ErrorCode::SourceAuthExpired
                        && let Some(next) = self.next.as_ref()
                    {
                        output.events.push(StreamEvent::SourceRefreshNeeded {
                            stream_id: self.stream_id.clone(),
                            track_id: next.source.id.clone(),
                        });
                    }
                    self.next = None;
                    output.events.push(StreamEvent::Error {
                        stream_id: self.stream_id.clone(),
                        code,
                        message,
                    });
                }
            }
        }

        let status = self.status();
        output.events.push(StreamEvent::StateChanged {
            status: status.clone(),
        });
        output.into_output(status)
    }

    fn play(&mut self, output: &mut ActorEffects) -> Result<()> {
        if self.play_state == PlayState::Stopped {
            return Err(MusicStreamError::Unsupported(
                "cannot play a stopped stream".to_owned(),
            ));
        }

        if self.current.is_none() {
            self.play_state = PlayState::Idle;
            output.events.push(StreamEvent::NextNeeded {
                stream_id: self.stream_id.clone(),
            });
            return Ok(());
        }

        let was_paused = self.play_state == PlayState::Paused;
        let current_ready = self.current.as_ref().is_some_and(|slot| slot.ready);
        if self.play_state == PlayState::Paused && current_ready {
            self.play_state = PlayState::Playing;
        } else if self.play_state != PlayState::Playing {
            self.play_state = PlayState::Buffering;
            if let Some(current) = self.current.as_mut()
                && !current.task_active
            {
                current.task_active = true;
                output.actions.push(TaskAction::StartCurrent {
                    generation: current.generation,
                    track: current.source.clone(),
                });
            }
        }
        if was_paused
            && let Some(current) = self.current.as_ref()
            && current.task_active
        {
            output.actions.push(TaskAction::ResumeCurrent {
                generation: current.generation,
            });
        }

        self.prepare_next_if_needed(output);
        Ok(())
    }

    fn pause(&mut self, output: &mut ActorEffects) {
        if matches!(self.play_state, PlayState::Playing | PlayState::Buffering) {
            if let Some(current) = self.current.as_ref()
                && current.task_active
            {
                output.actions.push(TaskAction::PauseCurrent {
                    generation: current.generation,
                });
            }
            self.play_state = PlayState::Paused;
        }
    }

    fn stop(&mut self, output: &mut ActorEffects) {
        if self.play_state == PlayState::Stopped {
            return;
        }

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

        output.actions.push(TaskAction::StopSender);
        self.refreshable_current_key = None;
        self.play_state = PlayState::Stopped;
        output.events.push(StreamEvent::StreamStopped {
            stream_id: self.stream_id.clone(),
        });
    }

    fn seek(&mut self, seconds: u64, output: &mut ActorEffects) -> Result<()> {
        let Some(current) = self.current.as_mut() else {
            return Err(MusicStreamError::Unsupported(
                "cannot seek without a current track".to_owned(),
            ));
        };

        if !current.source.is_seekable() {
            return Err(MusicStreamError::NotSeekable(current.source.id.clone()));
        }

        output.actions.push(TaskAction::CancelCurrent {
            generation: current.generation,
        });

        self.generation += 1;
        current.generation = self.generation;
        current.ready = false;
        current.task_active = true;
        self.time_played_ms = seconds.saturating_mul(1_000);
        self.play_state = PlayState::Buffering;

        output.actions.push(TaskAction::StartCurrent {
            generation: current.generation,
            track: current.source.clone(),
        });

        Ok(())
    }

    fn set_next(&mut self, next: Option<TrackSource>, output: &mut ActorEffects) {
        match (self.next.as_ref(), next) {
            (None, None) => {}
            (Some(old), None) => {
                output.actions.push(TaskAction::CancelNext {
                    generation: old.generation,
                });
                self.next = None;
            }
            (Some(old), Some(new_source)) if old.source.same_identity_as(&new_source) => {
                if let Some(next) = self.next.as_mut() {
                    next.source = new_source;
                }
            }
            (old, Some(new_source)) => {
                if let Some(old) = old {
                    output.actions.push(TaskAction::CancelNext {
                        generation: old.generation,
                    });
                }
                self.generation += 1;
                let mut slot = TrackSlot::new(new_source.clone(), self.generation);
                slot.task_active = true;
                output.actions.push(TaskAction::PrepareNext {
                    generation: slot.generation,
                    track: new_source,
                });
                self.next = Some(slot);
            }
        }
    }

    fn switch_track(
        &mut self,
        current: TrackSource,
        next: Option<TrackSource>,
        output: &mut ActorEffects,
    ) {
        if let Some(old_current) = self.current.take() {
            output.actions.push(TaskAction::CancelCurrent {
                generation: old_current.generation,
            });
        }

        if let Some(old_next) = self.next.take() {
            output.actions.push(TaskAction::CancelNext {
                generation: old_next.generation,
            });
        }

        self.refreshable_current_key = None;
        self.generation += 1;
        let mut current_slot = TrackSlot::new(current.clone(), self.generation);
        current_slot.task_active = true;
        output.actions.push(TaskAction::StartCurrent {
            generation: current_slot.generation,
            track: current,
        });
        self.current = Some(current_slot);

        if let Some(next_source) = next {
            self.generation += 1;
            let mut next_slot = TrackSlot::new(next_source.clone(), self.generation);
            next_slot.task_active = true;
            output.actions.push(TaskAction::PrepareNext {
                generation: next_slot.generation,
                track: next_source,
            });
            self.next = Some(next_slot);
        }

        self.time_played_ms = 0;
        self.time_total_ms = None;
        self.play_state = PlayState::Buffering;
    }

    fn refresh_current_source(
        &mut self,
        current: TrackSource,
        output: &mut ActorEffects,
    ) -> Result<()> {
        if let Some(old_current) = self.current.take() {
            if !old_current.source.same_identity_as(&current) {
                self.current = Some(old_current);
                return Err(MusicStreamError::InvalidSource(
                    "refreshed current source must keep the current track id".to_owned(),
                ));
            }
            output.actions.push(TaskAction::CancelCurrent {
                generation: old_current.generation,
            });
        } else if !self.can_refresh_current_source(&current) {
            return Err(MusicStreamError::InvalidSource(
                "refreshed current source must keep the current track id".to_owned(),
            ));
        }

        self.refreshable_current_key = None;
        self.generation += 1;
        let mut current_slot = TrackSlot::new(current.clone(), self.generation);
        current_slot.task_active = true;
        output.actions.push(TaskAction::StartCurrent {
            generation: current_slot.generation,
            track: current,
        });
        self.current = Some(current_slot);
        self.time_played_ms = 0;
        self.time_total_ms = None;
        self.play_state = PlayState::Buffering;
        self.consecutive_track_errors = 0;

        Ok(())
    }

    fn set_volume(&mut self, volume: VolumeLevel, output: &mut ActorEffects) {
        self.volume = volume;
        if let Some(current) = self.current.as_ref()
            && current.task_active
        {
            output.actions.push(TaskAction::SetCurrentVolume {
                generation: current.generation,
                volume,
            });
        }
        if let Some(next) = self.next.as_ref()
            && next.task_active
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
            && current.task_active
        {
            output.actions.push(TaskAction::SetCurrentGain {
                generation: current.generation,
                gain,
            });
        }
        if let Some(next) = self.next.as_ref()
            && next.task_active
        {
            output.actions.push(TaskAction::SetNextGain {
                generation: next.generation,
                gain,
            });
        }
    }

    fn promote_next_or_wait(&mut self, output: &mut ActorEffects) {
        if self.promote_ready_next(output) {
            return;
        }

        self.remember_refreshable_current();
        self.current = None;
        if self.next.is_some() {
            self.play_state = PlayState::Buffering;
        } else {
            self.play_state = PlayState::Idle;
            output.events.push(StreamEvent::NextNeeded {
                stream_id: self.stream_id.clone(),
            });
        }
    }

    fn promote_ready_next(&mut self, output: &mut ActorEffects) -> bool {
        if !self.next.as_ref().is_some_and(|slot| slot.ready) {
            return false;
        }

        let Some(next) = self.next.take() else {
            return false;
        };

        self.refreshable_current_key = None;
        self.current = Some(next);
        self.time_played_ms = 0;
        self.time_total_ms = None;
        self.play_state = PlayState::Buffering;
        if let Some(current) = self.current.as_ref() {
            output.actions.push(TaskAction::StartCurrent {
                generation: current.generation,
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
        self.consecutive_track_errors = self.consecutive_track_errors.saturating_add(1);
        if code == ErrorCode::SourceAuthExpired
            && let Some(current) = self.current.as_ref()
        {
            output.events.push(StreamEvent::SourceRefreshNeeded {
                stream_id: self.stream_id.clone(),
                track_id: current.source.id.clone(),
            });
        }
        output.events.push(StreamEvent::Error {
            stream_id: self.stream_id.clone(),
            code,
            message,
        });

        if self.consecutive_track_errors >= self.max_consecutive_track_errors {
            self.play_state = PlayState::Error;
            return;
        }

        self.promote_next_or_wait(output);
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
            .map(|slot| slot.source.stable_key().to_owned());
    }

    fn can_refresh_current_source(&self, current: &TrackSource) -> bool {
        self.refreshable_current_key
            .as_deref()
            .is_some_and(|key| key == current.stable_key())
    }

    fn prepare_next_if_needed(&mut self, output: &mut ActorEffects) {
        if let Some(next) = self.next.as_mut()
            && !next.ready
            && !next.task_active
        {
            next.task_active = true;
            output.actions.push(TaskAction::PrepareNext {
                generation: next.generation,
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
            id: id.to_owned(),
            kind: TrackKind::File,
            url: None,
            path: Some(format!("/tmp/{id}.mp3")),
            seekable: Some(true),
        }
    }

    fn url_track(id: &str) -> TrackSource {
        TrackSource {
            id: id.to_owned(),
            kind: TrackKind::Url,
            url: Some(format!("https://example.test/{id}.mp3")),
            path: None,
            seekable: None,
        }
    }

    fn live_track(id: &str, url: &str) -> TrackSource {
        TrackSource {
            id: id.to_owned(),
            kind: TrackKind::Live,
            url: Some(url.to_owned()),
            path: None,
            seekable: Some(false),
        }
    }

    fn malformed_seekable_live_track(id: &str, url: &str) -> TrackSource {
        TrackSource {
            id: id.to_owned(),
            kind: TrackKind::Live,
            url: Some(url.to_owned()),
            path: None,
            seekable: Some(true),
        }
    }

    #[test]
    fn switch_track_increments_generation_and_drops_stale_worker_events() {
        let mut actor = StreamActor::new("s1".to_owned(), Some(track("a")), None);
        let old_generation = actor.current_generation();

        let output = actor
            .handle_command(StreamCommand::SwitchTrack {
                current: track("b"),
                next: None,
            })
            .expect("switch should succeed");

        let new_generation = actor.current_generation();
        assert!(new_generation > old_generation);
        assert!(output.actions.contains(&TaskAction::CancelCurrent {
            generation: old_generation
        }));

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
    fn next_never_promotes_before_ready() {
        let mut actor = StreamActor::new("s1".to_owned(), Some(track("a")), Some(track("b")));
        let current_generation = actor.current_generation();
        let output = actor.handle_worker_event(WorkerEvent::CurrentEnded {
            generation: current_generation,
        });

        assert!(matches!(output.status.play_state, PlayState::Buffering));
        assert!(actor.current.is_none());
        assert_eq!(actor.next.as_ref().expect("next retained").source.id, "b");
    }

    #[test]
    fn live_track_seek_is_rejected_even_when_input_marks_seekable() {
        let mut actor = StreamActor::new(
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
        let mut actor = StreamActor::new("s1".to_owned(), Some(track("a")), Some(track("b")));
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
            track: track("b")
        }));
    }

    #[test]
    fn seek_requires_seekable_current_and_creates_generation() {
        let mut actor = StreamActor::new("s1".to_owned(), Some(track("a")), None);
        let old_generation = actor.current_generation();
        let output = actor
            .handle_command(StreamCommand::Seek { seconds: 42 })
            .expect("seek should succeed");

        assert!(actor.current_generation() > old_generation);
        assert_eq!(output.status.time_played_ms, 42_000);
    }

    #[test]
    fn bounded_url_tracks_are_seekable_by_default() {
        let mut actor = StreamActor::new("s1".to_owned(), Some(url_track("url-a")), None);
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
    fn set_volume_updates_status_and_current_slot_without_restarting_tasks() {
        let mut actor = StreamActor::new("s1".to_owned(), Some(track("a")), Some(track("b")));
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
        let mut actor = StreamActor::new("s1".to_owned(), Some(track("a")), None);
        let volume = VolumeLevel::from_unit(0.25).expect("volume");

        let output = actor
            .handle_command(StreamCommand::SetVolume { volume })
            .expect("set volume");

        assert_eq!(output.status.volume, volume);
        assert!(output.actions.is_empty());
    }

    #[test]
    fn set_gain_updates_status_and_current_slot_without_restarting_tasks() {
        let mut actor = StreamActor::new("s1".to_owned(), Some(track("a")), None);
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
        let mut actor = StreamActor::new("s1".to_owned(), Some(track("a")), None);
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
        let mut actor = StreamActor::new("s1".to_owned(), Some(track("a")), None);
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
        let mut actor = StreamActor::new("s1".to_owned(), Some(url_track("current")), None);
        let generation = actor.current_generation();

        let output = actor.handle_worker_event(WorkerEvent::CurrentFailed {
            generation,
            code: ErrorCode::SourceAuthExpired,
            message: "expired".to_owned(),
        });

        assert!(output.events.iter().any(|event| {
            matches!(
                event,
                StreamEvent::SourceRefreshNeeded { stream_id, track_id }
                    if stream_id == "s1" && track_id == "current"
            )
        }));
        assert!(output.events.iter().any(|event| {
            matches!(
                event,
                StreamEvent::Error { code, message, .. }
                    if *code == ErrorCode::SourceAuthExpired && message == "expired"
            )
        }));
    }

    #[test]
    fn current_source_failure_without_next_reports_error_and_next_needed() {
        let mut actor = StreamActor::new("s1".to_owned(), Some(url_track("current")), None);
        let generation = actor.current_generation();

        let output = actor.handle_worker_event(WorkerEvent::CurrentFailed {
            generation,
            code: ErrorCode::InvalidSource,
            message: "retry exhausted".to_owned(),
        });

        let status = output.status;
        assert_eq!(status.play_state, PlayState::Idle);
        assert!(status.current.is_none());
        assert!(output.events.iter().any(|event| {
            matches!(
                event,
                StreamEvent::Error { code, message, .. }
                    if *code == ErrorCode::InvalidSource && message == "retry exhausted"
            )
        }));
        assert!(output.events.iter().any(|event| {
            matches!(event, StreamEvent::NextNeeded { stream_id } if stream_id == "s1")
        }));
    }

    #[test]
    fn refresh_current_source_rejects_track_id_change_after_current_was_cleared() {
        let mut actor = StreamActor::new("s1".to_owned(), Some(url_track("current")), None);
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
        let mut actor = StreamActor::new("s1".to_owned(), None, None);

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
        let mut actor = StreamActor::new(
            "s1".to_owned(),
            Some(track("current")),
            Some(url_track("next")),
        );
        let generation = actor.next.as_ref().expect("next").generation;

        let output = actor.handle_worker_event(WorkerEvent::NextFailed {
            generation,
            code: ErrorCode::SourceAuthExpired,
            message: "expired".to_owned(),
        });

        assert!(output.events.iter().any(|event| {
            matches!(
                event,
                StreamEvent::SourceRefreshNeeded { stream_id, track_id }
                    if stream_id == "s1" && track_id == "next"
            )
        }));
        assert!(actor.next.is_none());
    }

    #[test]
    fn refresh_current_source_restarts_idle_stream_and_preserves_next() {
        let mut actor = StreamActor::new(
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
        let mut actor = StreamActor::new(
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
        let mut actor = StreamActor::new(
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
