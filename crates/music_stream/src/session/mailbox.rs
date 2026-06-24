use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::lifecycle::{RuntimeTaskGroup, RuntimeTaskShutdownReport};
use crate::model::{StreamStatus, TrackSource};
use crate::session::{ActorOutput, StreamActor, StreamCommand, WorkerEvent};
use crate::{MusicStreamError, Result};

pub const DEFAULT_STREAM_ACTOR_MAILBOX_CAPACITY: usize = 32;
const MAILBOX_ACCEPTED_METRIC: &str = "music_stream.session.mailbox.accepted";
const MAILBOX_BUSY_METRIC: &str = "music_stream.session.mailbox.busy";
const MAILBOX_CLOSED_METRIC: &str = "music_stream.session.mailbox.closed";
const MAILBOX_QUEUE_DEPTH_METRIC: &str = "music_stream.session.mailbox.queue_depth";

#[derive(Debug)]
pub struct StreamActorMailbox {
    handle: StreamActorMailboxHandle,
    tasks: RuntimeTaskGroup,
}

impl StreamActorMailbox {
    pub fn spawn(
        stream_id: String,
        current: Option<TrackSource>,
        next: Option<TrackSource>,
    ) -> Result<Self> {
        Self::spawn_with_capacity(
            stream_id,
            current,
            next,
            DEFAULT_STREAM_ACTOR_MAILBOX_CAPACITY,
        )
    }

    pub fn spawn_with_capacity(
        stream_id: String,
        current: Option<TrackSource>,
        next: Option<TrackSource>,
        capacity: usize,
    ) -> Result<Self> {
        if capacity == 0 {
            return Err(MusicStreamError::InvalidConfig(
                "stream actor mailbox capacity must be greater than zero".to_owned(),
            ));
        }

        let (sender, receiver) = mpsc::channel(capacity);
        let handle = StreamActorMailboxHandle {
            stream_id: stream_id.clone(),
            sender,
        };
        let mut tasks = RuntimeTaskGroup::new();
        tasks.spawn_with_token(format!("stream-actor:{stream_id}"), move |token| {
            run_actor_mailbox(token, StreamActor::new(stream_id, current, next), receiver)
        });

        Ok(Self { handle, tasks })
    }

    #[must_use]
    pub fn handle(&self) -> StreamActorMailboxHandle {
        self.handle.clone()
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.tasks.is_cancelled()
    }

    pub async fn shutdown(self, timeout: Duration) -> RuntimeTaskShutdownReport {
        self.tasks.shutdown(timeout).await
    }
}

#[derive(Clone, Debug)]
pub struct StreamActorMailboxHandle {
    stream_id: String,
    sender: mpsc::Sender<StreamActorMailboxMessage>,
}

impl StreamActorMailboxHandle {
    pub async fn command(&self, command: StreamCommand) -> Result<ActorOutput> {
        let (reply, response) = oneshot::channel();
        self.try_send(StreamActorMailboxMessage::Command { command, reply })?;
        receive_actor_reply(&self.stream_id, response).await
    }

    pub async fn worker_event(&self, event: WorkerEvent) -> Result<ActorOutput> {
        let (reply, response) = oneshot::channel();
        self.try_send(StreamActorMailboxMessage::WorkerEvent { event, reply })?;
        receive_actor_reply(&self.stream_id, response).await
    }

    pub fn try_send_worker_event(
        &self,
        event: WorkerEvent,
    ) -> Result<StreamActorMailboxReply<ActorOutput>> {
        let (reply, response) = oneshot::channel();
        self.try_send(StreamActorMailboxMessage::WorkerEvent { event, reply })?;
        Ok(StreamActorMailboxReply {
            stream_id: self.stream_id.clone(),
            response,
        })
    }

    pub async fn status(&self) -> Result<StreamStatus> {
        let (reply, response) = oneshot::channel();
        self.try_send(StreamActorMailboxMessage::Status { reply })?;
        receive_actor_reply(&self.stream_id, response).await
    }

    fn try_send(&self, message: StreamActorMailboxMessage) -> Result<()> {
        match self.sender.try_send(message) {
            Ok(()) => {
                metrics::counter!(MAILBOX_ACCEPTED_METRIC).increment(1);
                record_queue_depth(&self.sender);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                metrics::counter!(MAILBOX_BUSY_METRIC).increment(1);
                record_queue_depth(&self.sender);
                Err(MusicStreamError::Busy(format!(
                    "stream actor mailbox is full: {}",
                    self.stream_id
                )))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                metrics::counter!(MAILBOX_CLOSED_METRIC).increment(1);
                Err(MusicStreamError::StreamClosed(format!(
                    "stream actor mailbox is closed: {}",
                    self.stream_id
                )))
            }
        }
    }
}

#[derive(Debug)]
pub struct StreamActorMailboxReply<T> {
    stream_id: String,
    response: oneshot::Receiver<Result<T>>,
}

impl<T> StreamActorMailboxReply<T> {
    pub async fn receive(self) -> Result<T> {
        receive_actor_reply(&self.stream_id, self.response).await
    }
}

fn record_queue_depth(sender: &mpsc::Sender<StreamActorMailboxMessage>) {
    let depth = sender.max_capacity().saturating_sub(sender.capacity());
    metrics::gauge!(MAILBOX_QUEUE_DEPTH_METRIC).set(depth as f64);
}

#[derive(Debug)]
enum StreamActorMailboxMessage {
    Command {
        command: StreamCommand,
        reply: oneshot::Sender<Result<ActorOutput>>,
    },
    WorkerEvent {
        event: WorkerEvent,
        reply: oneshot::Sender<Result<ActorOutput>>,
    },
    Status {
        reply: oneshot::Sender<Result<StreamStatus>>,
    },
}

async fn run_actor_mailbox(
    token: CancellationToken,
    mut actor: StreamActor,
    mut receiver: mpsc::Receiver<StreamActorMailboxMessage>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = token.cancelled() => return Ok(()),
            message = receiver.recv() => {
                let Some(message) = message else {
                    return Ok(());
                };
                handle_mailbox_message(&mut actor, message);
            }
        }
    }
}

fn handle_mailbox_message(actor: &mut StreamActor, message: StreamActorMailboxMessage) {
    match message {
        StreamActorMailboxMessage::Command { command, reply } => {
            let _ = reply.send(actor.handle_command(command));
        }
        StreamActorMailboxMessage::WorkerEvent { event, reply } => {
            let _ = reply.send(Ok(actor.handle_worker_event(event)));
        }
        StreamActorMailboxMessage::Status { reply } => {
            let _ = reply.send(Ok(actor.status()));
        }
    }
}

async fn receive_actor_reply<T>(
    stream_id: &str,
    response: oneshot::Receiver<Result<T>>,
) -> Result<T> {
    response.await.map_err(|_| {
        MusicStreamError::StreamClosed(format!("stream actor mailbox is closed: {stream_id}"))
    })?
}

#[cfg(test)]
mod tests {
    use metrics_util::CompositeKey;
    use metrics_util::MetricKind;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::error::ErrorCode;
    use crate::event::StreamEvent;
    use crate::model::{PlayState, TrackKind};
    use crate::session::TaskAction;

    type MetricSnapshot = Vec<(
        CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    )>;

    fn metric_counter_sum(snapshot: &MetricSnapshot, name: &str) -> u64 {
        snapshot
            .iter()
            .filter_map(|(key, _, _, value)| {
                if key.kind() == MetricKind::Counter && key.key().name() == name {
                    match value {
                        DebugValue::Counter(count) => Some(*count),
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .sum()
    }

    fn metric_has_gauge(snapshot: &MetricSnapshot, name: &str) -> bool {
        snapshot.iter().any(|(key, _, _, value)| {
            key.kind() == MetricKind::Gauge
                && key.key().name() == name
                && matches!(value, DebugValue::Gauge(_))
        })
    }

    fn track(id: &str) -> TrackSource {
        TrackSource {
            id: id.to_owned(),
            kind: TrackKind::File,
            url: None,
            path: Some(format!("/tmp/{id}.mp3")),
            seekable: Some(true),
        }
    }

    #[tokio::test]
    async fn bounded_command_queue_returns_busy_when_full() {
        let (sender, _receiver) = mpsc::channel(1);
        let (reply, _response) = oneshot::channel();
        sender
            .try_send(StreamActorMailboxMessage::Status { reply })
            .expect("fill mailbox");
        let handle = StreamActorMailboxHandle {
            stream_id: "s1".to_owned(),
            sender,
        };

        let error = handle
            .command(StreamCommand::Play)
            .await
            .expect_err("full mailbox should reject immediately");

        assert_eq!(error.code(), ErrorCode::Busy);
    }

    #[test]
    fn try_send_records_mailbox_pressure_metrics() {
        let metrics = DebuggingRecorder::new();
        let snapshotter = metrics.snapshotter();

        metrics::with_local_recorder(&metrics, || {
            let (sender, _receiver) = mpsc::channel(1);
            let handle = StreamActorMailboxHandle {
                stream_id: "s1".to_owned(),
                sender,
            };
            let (first_reply, _first_response) = oneshot::channel();
            handle
                .try_send(StreamActorMailboxMessage::Status { reply: first_reply })
                .expect("first message should fit");

            let (second_reply, _second_response) = oneshot::channel();
            let error = handle
                .try_send(StreamActorMailboxMessage::Command {
                    command: StreamCommand::Play,
                    reply: second_reply,
                })
                .expect_err("full mailbox should be busy");
            assert_eq!(error.code(), ErrorCode::Busy);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        assert_eq!(metric_counter_sum(&snapshot, MAILBOX_ACCEPTED_METRIC), 1);
        assert_eq!(metric_counter_sum(&snapshot, MAILBOX_BUSY_METRIC), 1);
        assert!(metric_has_gauge(&snapshot, MAILBOX_QUEUE_DEPTH_METRIC));
    }

    #[tokio::test]
    async fn commands_and_worker_events_are_processed_serially() {
        let mailbox =
            StreamActorMailbox::spawn_with_capacity("s1".to_owned(), Some(track("a")), None, 8)
                .expect("mailbox");
        let handle = mailbox.handle();

        let play = handle
            .command(StreamCommand::Play)
            .await
            .expect("play command");
        let generation = play.status.generation;
        assert_eq!(
            play.actions,
            vec![TaskAction::StartCurrent {
                generation,
                track: track("a"),
            }]
        );
        assert_eq!(play.status.play_state, PlayState::Buffering);

        let ready = handle
            .worker_event(WorkerEvent::CurrentPrebufferReady { generation })
            .await
            .expect("ready event");
        assert_eq!(ready.status.play_state, PlayState::Playing);

        let report = mailbox.shutdown(Duration::from_secs(1)).await;
        assert_eq!(report.completed, 1);
        assert!(!report.timed_out);
    }

    #[tokio::test]
    async fn try_send_worker_event_enqueues_before_waiting_for_reply() {
        let mailbox =
            StreamActorMailbox::spawn_with_capacity("s1".to_owned(), Some(track("a")), None, 8)
                .expect("mailbox");
        let handle = mailbox.handle();

        let play = handle
            .command(StreamCommand::Play)
            .await
            .expect("play command");
        let generation = play.status.generation;

        let ready_reply = handle
            .try_send_worker_event(WorkerEvent::CurrentPrebufferReady { generation })
            .expect("ready event should enqueue");
        let ended_reply = handle
            .try_send_worker_event(WorkerEvent::CurrentEnded { generation })
            .expect("ended event should enqueue after ready");

        let ready = ready_reply.receive().await.expect("ready reply");
        assert_eq!(ready.status.play_state, PlayState::Playing);
        let ended = ended_reply.receive().await.expect("ended reply");
        assert_eq!(ended.status.play_state, PlayState::Idle);

        let report = mailbox.shutdown(Duration::from_secs(1)).await;
        assert_eq!(report.completed, 1);
        assert!(!report.timed_out);
    }

    #[tokio::test]
    async fn worker_event_path_keeps_actor_generation_filtering() {
        let mailbox =
            StreamActorMailbox::spawn_with_capacity("s1".to_owned(), Some(track("a")), None, 8)
                .expect("mailbox");
        let handle = mailbox.handle();
        let generation = handle.status().await.expect("status").generation;

        let stale = handle
            .worker_event(WorkerEvent::CurrentEnded {
                generation: generation + 1,
            })
            .await
            .expect("stale event");
        assert!(stale.events.iter().all(
            |event| !matches!(event, StreamEvent::NextNeeded { stream_id } if stream_id == "s1")
        ));
        assert_eq!(stale.status.current, Some(track("a")));

        let current = handle
            .worker_event(WorkerEvent::CurrentEnded { generation })
            .await
            .expect("current end");
        assert!(current.events.iter().any(
            |event| matches!(event, StreamEvent::NextNeeded { stream_id } if stream_id == "s1")
        ));

        let report = mailbox.shutdown(Duration::from_secs(1)).await;
        assert_eq!(report.completed, 1);
    }

    #[tokio::test]
    async fn shutdown_cancels_actor_task_deterministically() {
        let mailbox = StreamActorMailbox::spawn_with_capacity("s1".to_owned(), None, None, 8)
            .expect("mailbox");

        let report = mailbox.shutdown(Duration::from_secs(1)).await;

        assert_eq!(report.completed, 1);
        assert!(report.failed.is_empty());
        assert_eq!(report.aborted, 0);
        assert!(!report.timed_out);
    }
}
