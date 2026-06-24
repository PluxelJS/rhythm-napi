use std::collections::HashMap;
use std::sync::RwLock;

use crate::error::{MusicStreamError, Result};
use crate::model::{GainLevel, StreamStatus, TrackSource, VolumeLevel};
use crate::session::{ActorOutput, StreamActor, StreamCommand, WorkerEvent};

#[derive(Debug, Default)]
pub struct Engine {
    streams: RwLock<HashMap<String, StreamActor>>,
}

impl Engine {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_status(&self, stream_id: &str) -> Result<StreamStatus> {
        let streams = self.read_streams()?;

        streams
            .get(stream_id)
            .map(StreamActor::status)
            .ok_or_else(|| MusicStreamError::StreamNotFound(stream_id.to_owned()))
    }

    pub fn insert_placeholder_stream(
        &self,
        stream_id: String,
        current: Option<TrackSource>,
        next: Option<TrackSource>,
    ) -> Result<StreamStatus> {
        self.create_stream(stream_id, current, next)
    }

    pub fn create_stream(
        &self,
        stream_id: String,
        current: Option<TrackSource>,
        next: Option<TrackSource>,
    ) -> Result<StreamStatus> {
        let mut streams = self.write_streams()?;

        if streams.contains_key(&stream_id) {
            return Err(MusicStreamError::StreamAlreadyExists(stream_id));
        }

        let actor = StreamActor::new(stream_id.clone(), current, next);
        let status = actor.status();
        streams.insert(stream_id, actor);
        Ok(status)
    }

    pub fn remove_stream(&self, stream_id: &str) -> Result<Option<StreamStatus>> {
        let mut streams = self.write_streams()?;
        Ok(streams.remove(stream_id).map(|actor| actor.status()))
    }

    pub fn clear(&self) -> Result<Vec<StreamStatus>> {
        let mut streams = self.write_streams()?;
        Ok(streams.drain().map(|(_, actor)| actor.status()).collect())
    }

    pub fn command(&self, stream_id: &str, command: StreamCommand) -> Result<ActorOutput> {
        let mut streams = self.write_streams()?;
        let actor = streams
            .get_mut(stream_id)
            .ok_or_else(|| MusicStreamError::StreamNotFound(stream_id.to_owned()))?;

        actor.handle_command(command)
    }

    pub fn worker_event(&self, stream_id: &str, event: WorkerEvent) -> Result<ActorOutput> {
        let mut streams = self.write_streams()?;
        let actor = streams
            .get_mut(stream_id)
            .ok_or_else(|| MusicStreamError::StreamNotFound(stream_id.to_owned()))?;

        Ok(actor.handle_worker_event(event))
    }

    pub fn play(&self, stream_id: &str) -> Result<ActorOutput> {
        self.command(stream_id, StreamCommand::Play)
    }

    pub fn pause(&self, stream_id: &str) -> Result<ActorOutput> {
        self.command(stream_id, StreamCommand::Pause)
    }

    pub fn stop(&self, stream_id: &str) -> Result<ActorOutput> {
        self.command(stream_id, StreamCommand::Stop)
    }

    pub fn seek(&self, stream_id: &str, seconds: u64) -> Result<ActorOutput> {
        self.command(stream_id, StreamCommand::Seek { seconds })
    }

    pub fn set_volume(&self, stream_id: &str, volume: f32) -> Result<ActorOutput> {
        self.command(
            stream_id,
            StreamCommand::SetVolume {
                volume: VolumeLevel::from_unit(volume)?,
            },
        )
    }

    pub fn set_gain(&self, stream_id: &str, gain_db: f32) -> Result<ActorOutput> {
        self.command(
            stream_id,
            StreamCommand::SetGain {
                gain: GainLevel::from_db(gain_db)?,
            },
        )
    }

    pub fn set_next(&self, stream_id: &str, next: Option<TrackSource>) -> Result<ActorOutput> {
        self.command(stream_id, StreamCommand::SetNext(next))
    }

    pub fn switch_track(
        &self,
        stream_id: &str,
        current: TrackSource,
        next: Option<TrackSource>,
    ) -> Result<ActorOutput> {
        self.command(stream_id, StreamCommand::SwitchTrack { current, next })
    }

    fn read_streams(&self) -> Result<std::sync::RwLockReadGuard<'_, HashMap<String, StreamActor>>> {
        self.streams
            .read()
            .map_err(|_| MusicStreamError::Internal("stream registry lock poisoned".to_owned()))
    }

    fn write_streams(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<'_, HashMap<String, StreamActor>>> {
        self.streams
            .write()
            .map_err(|_| MusicStreamError::Internal("stream registry lock poisoned".to_owned()))
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

    #[test]
    fn engine_routes_commands_through_actor() {
        let engine = Engine::new();
        let status = engine
            .create_stream("s1".to_owned(), Some(track("a")), None)
            .expect("create");
        assert_eq!(status.generation, 1);

        let output = engine.play("s1").expect("play");
        let status = output.status;
        assert_eq!(status.stream_id, "s1");
        assert!(matches!(
            status.play_state,
            crate::model::PlayState::Buffering
        ));
    }

    #[test]
    fn clear_removes_all_streams_and_returns_last_statuses() {
        let engine = Engine::new();
        engine
            .create_stream("s1".to_owned(), Some(track("a")), None)
            .expect("create s1");
        engine
            .create_stream("s2".to_owned(), Some(track("b")), None)
            .expect("create s2");

        let cleared = engine.clear().expect("clear");
        assert_eq!(cleared.len(), 2);
        assert!(engine.get_status("s1").is_err());
        assert!(engine.get_status("s2").is_err());
    }
}
