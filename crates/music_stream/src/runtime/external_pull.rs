use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

use super::StreamRuntimeProgress;
use super::opus_queue::OpusQueueReceiver;
use crate::error::{MusicStreamError, Result};
use crate::session::WorkerEvent;

const COMMAND_TIMEOUT: Duration = Duration::from_secs(2);
const STOP_TIMEOUT: Duration = Duration::from_secs(2);
const LEASE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExternalFrameOutcome {
    Sent,
    Late,
    Cancelled,
    OutputUnavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalFrameAck {
    pub lease_id: u32,
    pub generation: u64,
    pub outcome: ExternalFrameOutcome,
}

#[derive(Clone, Debug)]
pub struct ExternalOpusFrame {
    pub lease_id: u32,
    pub generation: u64,
    pub payload: Bytes,
    pub samples_per_channel: u32,
    pub media_position_ms: u64,
    deadline: Instant,
}

impl ExternalOpusFrame {
    /// Time left on the native playout deadline at the instant this method is called.
    ///
    /// Keeping the absolute deadline in native code ensures time spent waiting for the
    /// JavaScript event loop is not accidentally added back during N-API conversion.
    #[must_use]
    pub fn deadline_remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

#[derive(Clone, Debug)]
pub(super) struct ExternalPullHandle {
    commands: mpsc::Sender<Command>,
    progress: watch::Receiver<StreamRuntimeProgress>,
    task: Arc<ExternalPullTask>,
}

#[derive(Debug)]
struct ExternalPullTask {
    supervisor: Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
    worker_abort: tokio::task::AbortHandle,
}

impl Drop for ExternalPullTask {
    fn drop(&mut self) {
        self.worker_abort.abort();
        if let Ok(slot) = self.supervisor.get_mut()
            && let Some(supervisor) = slot.take()
        {
            supervisor.abort();
        }
    }
}

impl ExternalPullHandle {
    pub(super) fn spawn(
        prebuffer_ms: u64,
        max_playout_lateness_ms: u64,
        events: mpsc::Sender<WorkerEvent>,
    ) -> Self {
        let (commands, command_rx) = mpsc::channel(32);
        let (progress_tx, progress) = watch::channel(StreamRuntimeProgress::default());
        let worker = tokio::spawn(run_worker(
            prebuffer_ms,
            Duration::from_millis(max_playout_lateness_ms),
            command_rx,
            progress_tx,
            events,
        ));
        let worker_abort = worker.abort_handle();
        let supervisor = tokio::spawn(async move {
            worker.await.map_err(|error| {
                MusicStreamError::Internal(format!("external pull worker failed: {error}"))
            })?
        });
        Self {
            commands,
            progress,
            task: Arc::new(ExternalPullTask {
                supervisor: Mutex::new(Some(supervisor)),
                worker_abort,
            }),
        }
    }

    pub(super) fn progress(&self) -> StreamRuntimeProgress {
        *self.progress.borrow()
    }

    pub(super) async fn activate(
        &self,
        generation: u64,
        start_position_ms: u64,
        paused: bool,
        receiver: OpusQueueReceiver,
    ) -> Result<()> {
        self.request_ack(|reply| Command::Activate {
            generation,
            start_position_ms,
            paused,
            receiver,
            reply,
        })
        .await
    }

    pub(super) async fn deactivate(&self, generation: u64) -> Result<()> {
        self.request_ack(|reply| Command::Deactivate { generation, reply })
            .await
    }

    pub(super) async fn pause(&self, generation: u64) -> Result<()> {
        self.request_ack(|reply| Command::Pause { generation, reply })
            .await
    }

    pub(super) async fn resume(&self, generation: u64) -> Result<()> {
        self.request_ack(|reply| Command::Resume { generation, reply })
            .await
    }

    pub(super) async fn pull(
        &self,
        previous: Option<ExternalFrameAck>,
    ) -> Result<Option<ExternalOpusFrame>> {
        let (reply, receiver) = oneshot::channel();
        tokio::time::timeout(
            COMMAND_TIMEOUT,
            self.commands.send(Command::Pull { previous, reply }),
        )
        .await
        .map_err(|_| MusicStreamError::StreamClosed("external pull command timed out".to_owned()))?
        .map_err(|_| MusicStreamError::StreamClosed("external pull output closed".to_owned()))?;
        receiver.await.map_err(|_| {
            MusicStreamError::StreamClosed(
                "external pull output closed before delivering a frame".to_owned(),
            )
        })?
    }

    pub(super) async fn finish(&self, ack: ExternalFrameAck) -> Result<()> {
        self.request_ack(|reply| Command::Finish { ack, reply })
            .await
    }

    pub(super) async fn cancel_pull(&self) -> Result<()> {
        self.request_ack(|reply| Command::CancelPull { reply })
            .await
    }

    pub(super) async fn shutdown(&self) -> Result<()> {
        let command_result = self.request_ack(|reply| Command::Shutdown { reply }).await;
        let task = self
            .task
            .supervisor
            .lock()
            .map_err(|_| MusicStreamError::Internal("external pull task lock poisoned".to_owned()))?
            .take();
        let task_result = match task {
            Some(mut supervisor) => {
                match tokio::time::timeout(STOP_TIMEOUT, &mut supervisor).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(error)) => Err(MusicStreamError::Internal(format!(
                        "external pull supervisor failed: {error}"
                    ))),
                    Err(_) => {
                        self.task.worker_abort.abort();
                        supervisor.abort();
                        let _ = supervisor.await;
                        Err(MusicStreamError::Internal(
                            "external pull output did not stop within 2 seconds".to_owned(),
                        ))
                    }
                }
            }
            None => Ok(()),
        };
        match (command_result, task_result) {
            (_, Err(error)) | (Err(error), Ok(())) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    async fn request_ack(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<()>>) -> Command,
    ) -> Result<()> {
        let (reply, receiver) = oneshot::channel();
        tokio::time::timeout(COMMAND_TIMEOUT, self.commands.send(command(reply)))
            .await
            .map_err(|_| {
                MusicStreamError::StreamClosed("external pull command timed out".to_owned())
            })?
            .map_err(|_| {
                MusicStreamError::StreamClosed("external pull output closed".to_owned())
            })?;
        receiver.await.map_err(|_| {
            MusicStreamError::StreamClosed(
                "external pull output closed before command acknowledgement".to_owned(),
            )
        })?
    }
}

#[derive(Debug)]
enum Command {
    Activate {
        generation: u64,
        start_position_ms: u64,
        paused: bool,
        receiver: OpusQueueReceiver,
        reply: oneshot::Sender<Result<()>>,
    },
    Deactivate {
        generation: u64,
        reply: oneshot::Sender<Result<()>>,
    },
    Pause {
        generation: u64,
        reply: oneshot::Sender<Result<()>>,
    },
    Resume {
        generation: u64,
        reply: oneshot::Sender<Result<()>>,
    },
    Pull {
        previous: Option<ExternalFrameAck>,
        reply: oneshot::Sender<Result<Option<ExternalOpusFrame>>>,
    },
    Finish {
        ack: ExternalFrameAck,
        reply: oneshot::Sender<Result<()>>,
    },
    CancelPull {
        reply: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<()>>,
    },
}

#[derive(Debug)]
struct ActiveMedia {
    generation: u64,
    start_position_ms: u64,
    receiver: OpusQueueReceiver,
    started: bool,
    prebuffer_reported: bool,
    deadline: Option<Instant>,
    media_sent_ms: u64,
}

#[derive(Debug)]
struct Lease {
    id: u32,
    generation: u64,
    duration_ms: u64,
    payload_len: usize,
    delivered_at: Instant,
    expires_at: Instant,
}

async fn run_worker(
    prebuffer_ms: u64,
    max_playout_lateness: Duration,
    mut commands: mpsc::Receiver<Command>,
    progress_tx: watch::Sender<StreamRuntimeProgress>,
    events: mpsc::Sender<WorkerEvent>,
) -> Result<()> {
    let mut active: Option<ActiveMedia> = None;
    let mut paused = false;
    let mut pending_pull: Option<oneshot::Sender<Result<Option<ExternalOpusFrame>>>> = None;
    let mut lease: Option<Lease> = None;
    let mut next_lease_id = 1_u32;
    let mut frames_delivered = 0_u64;
    let mut bytes_delivered = 0_u64;
    let mut dropped_frames = 0_u64;
    let mut dropped_media_ms = 0_u64;
    let mut latency_recoveries = 0_u64;
    let mut underruns = 0_u64;
    let mut max_lateness_ms = 0_u64;
    let mut pending_events = VecDeque::new();

    loop {
        flush_events(&events, &mut pending_events);
        if let Some(media) = active.as_mut() {
            let buffered_ms = media.receiver.buffered_ms();
            let source_closed = media.receiver.is_closed();
            if !media.started
                && !paused
                && (buffered_ms >= prebuffer_ms || (source_closed && buffered_ms > 0))
            {
                media.started = true;
                media.deadline = Some(Instant::now());
                if !media.prebuffer_reported {
                    emit_event(
                        &events,
                        &mut pending_events,
                        WorkerEvent::CurrentPrebufferReady {
                            generation: media.generation,
                        },
                    );
                    media.prebuffer_reported = true;
                }
            }
            if media.receiver.is_drained() && lease.is_none() {
                let generation = media.generation;
                active = None;
                // Keep an outstanding pull parked while the actor promotes `next` (or the host
                // supplies a replacement after `NextNeeded`). `None` is the output-lifetime
                // sentinel, so returning it at a track boundary makes the consumer stop pulling
                // just before the next generation is activated.
                emit_event(
                    &events,
                    &mut pending_events,
                    WorkerEvent::CurrentEnded { generation },
                );
                continue;
            }
        }

        let delivery_deadline = active
            .as_ref()
            .filter(|media| media.started && !paused)
            .and_then(|media| media.deadline)
            .filter(|_| pending_pull.is_some() && lease.is_none());
        let lease_deadline = lease.as_ref().map(|lease| lease.expires_at);
        let has_pending_events = !pending_events.is_empty();

        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(Command::Activate { generation, start_position_ms, paused: initial_paused, receiver, reply }) => {
                        lease = None;
                        active = Some(ActiveMedia {
                            generation,
                            start_position_ms,
                            receiver,
                            started: false,
                            prebuffer_reported: false,
                            deadline: None,
                            media_sent_ms: 0,
                        });
                        paused = initial_paused;
                        let _ = reply.send(Ok(()));
                    }
                    Some(Command::Deactivate { generation, reply }) => {
                        if active.as_ref().is_some_and(|media| media.generation == generation) {
                            active = None;
                            lease = None;
                            if let Some(waiter) = pending_pull.take() {
                                let _ = waiter.send(Ok(None));
                            }
                        }
                        let _ = reply.send(Ok(()));
                    }
                    Some(Command::Pause { generation, reply }) => {
                        if active.as_ref().is_some_and(|media| media.generation == generation) {
                            paused = true;
                        }
                        let _ = reply.send(Ok(()));
                    }
                    Some(Command::Resume { generation, reply }) => {
                        if let Some(media) = active.as_mut().filter(|media| media.generation == generation) {
                            paused = false;
                            media.deadline = Some(Instant::now());
                        }
                        let _ = reply.send(Ok(()));
                    }
                    Some(Command::Pull { previous, reply }) => {
                        if pending_pull.is_some() {
                            let _ = reply.send(Err(MusicStreamError::Busy(
                                "only one external pull may be pending per stream".to_owned(),
                            )));
                            continue;
                        }
                        if let Some(ack) = previous {
                            let unavailable = ack.outcome == ExternalFrameOutcome::OutputUnavailable;
                            if let Err(error) = apply_ack(
                                ack,
                                &mut active,
                                &mut lease,
                                &mut dropped_frames,
                                &mut dropped_media_ms,
                                &mut frames_delivered,
                                &mut bytes_delivered,
                            ) {
                                let _ = reply.send(Err(error));
                                continue;
                            }
                            if unavailable {
                                emit_output_unavailable(&events, &mut pending_events, ack.generation);
                                let _ = reply.send(Ok(None));
                                continue;
                            }
                        }
                        if lease.is_some() {
                            let _ = reply.send(Err(MusicStreamError::Busy(
                                "the previous external Opus frame is still outstanding".to_owned(),
                            )));
                            continue;
                        }
                        if active.is_none() {
                            let _ = reply.send(Ok(None));
                        } else {
                            pending_pull = Some(reply);
                        }
                    }
                    Some(Command::Finish { ack, reply }) => {
                        let unavailable = ack.outcome == ExternalFrameOutcome::OutputUnavailable;
                        let result = apply_ack(
                            ack,
                            &mut active,
                            &mut lease,
                            &mut dropped_frames,
                            &mut dropped_media_ms,
                            &mut frames_delivered,
                            &mut bytes_delivered,
                        );
                        if unavailable && result.is_ok() {
                            emit_output_unavailable(&events, &mut pending_events, ack.generation);
                        }
                        let _ = reply.send(result);
                    }
                    Some(Command::CancelPull { reply }) => {
                        if let Some(waiter) = pending_pull.take() {
                            let _ = waiter.send(Ok(None));
                        }
                        let _ = reply.send(Ok(()));
                    }
                    Some(Command::Shutdown { reply }) => {
                        if let Some(waiter) = pending_pull.take() {
                            let _ = waiter.send(Ok(None));
                        }
                        let _ = reply.send(Ok(()));
                        return Ok(());
                    }
                    None => return Ok(()),
                }
                publish_progress(
                    &progress_tx,
                    active.as_ref(),
                    frames_delivered,
                    bytes_delivered,
                    dropped_frames,
                    dropped_media_ms,
                    latency_recoveries,
                    underruns,
                    max_lateness_ms,
                );
            }
            _ = async {
                match active.as_mut() {
                    Some(media) => media.receiver.changed().await,
                    None => std::future::pending().await,
                }
            } => {}
            _ = async {
                match delivery_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                let Some(media) = active.as_mut() else { continue; };
                let Some(reply) = pending_pull.take() else { continue; };
                let mut scheduled_deadline = media.deadline.unwrap_or_else(Instant::now);
                let observed_lateness = Instant::now().saturating_duration_since(scheduled_deadline);
                max_lateness_ms = max_lateness_ms.max(
                    u64::try_from(observed_lateness.as_millis()).unwrap_or(u64::MAX),
                );
                let mut recovered = false;
                while Instant::now().saturating_duration_since(scheduled_deadline)
                    > max_playout_lateness
                {
                    let Some(stale) = media.receiver.try_drop_oldest_if_followed() else {
                        break;
                    };
                    recovered = true;
                    dropped_frames = dropped_frames.saturating_add(1);
                    dropped_media_ms = dropped_media_ms.saturating_add(stale.duration_ms);
                    media.media_sent_ms = media.media_sent_ms.saturating_add(stale.duration_ms);
                    scheduled_deadline += Duration::from_millis(stale.duration_ms.max(1));
                }
                if recovered {
                    latency_recoveries = latency_recoveries.saturating_add(1);
                }
                let Some(frame) = media.receiver.try_recv() else {
                    underruns = underruns.saturating_add(1);
                    media.started = false;
                    media.deadline = None;
                    pending_pull = Some(reply);
                    publish_progress(
                        &progress_tx,
                        active.as_ref(),
                        frames_delivered,
                        bytes_delivered,
                        dropped_frames,
                        dropped_media_ms,
                        latency_recoveries,
                        underruns,
                        max_lateness_ms,
                    );
                    continue;
                };
                let lease_id = next_lease_id;
                next_lease_id = next_lease_id.wrapping_add(1).max(1);
                let now = Instant::now();
                let send_deadline = scheduled_deadline + max_playout_lateness;
                let duration_ms = frame.duration_ms;
                let payload_len = frame.payload.len();
                let output = ExternalOpusFrame {
                    lease_id,
                    generation: media.generation,
                    payload: frame.payload,
                    samples_per_channel: frame.samples_per_channel,
                    media_position_ms: media
                        .start_position_ms
                        .saturating_add(media.media_sent_ms)
                        .saturating_add(duration_ms),
                    deadline: send_deadline,
                };
                lease = Some(Lease {
                    id: lease_id,
                    generation: media.generation,
                    duration_ms,
                    payload_len,
                    delivered_at: now,
                    expires_at: now + LEASE_TIMEOUT,
                });
                media.deadline = Some(scheduled_deadline);
                if reply.send(Ok(Some(output))).is_err() {
                    lease = None;
                }
                publish_progress(
                    &progress_tx,
                    active.as_ref(),
                    frames_delivered,
                    bytes_delivered,
                    dropped_frames,
                    dropped_media_ms,
                    latency_recoveries,
                    underruns,
                    max_lateness_ms,
                );
            }
            _ = async {
                match lease_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                let generation = lease.as_ref().map_or(0, |lease| lease.generation);
                lease = None;
                active = None;
                if let Some(waiter) = pending_pull.take() {
                    let _ = waiter.send(Err(MusicStreamError::StreamClosed(
                        "external Opus frame lease timed out".to_owned(),
                    )));
                }
                emit_event(
                    &events,
                    &mut pending_events,
                    WorkerEvent::CurrentFailed {
                        generation,
                        code: crate::error::ErrorCode::StreamClosed,
                        message: "external Opus frame lease timed out".to_owned(),
                    },
                );
            }
            permit = async {
                if has_pending_events {
                    events.reserve().await.ok()
                } else {
                    std::future::pending().await
                }
            } => {
                if let (Some(permit), Some(event)) = (permit, pending_events.pop_front()) {
                    permit.send(event);
                }
            }
        }
    }
}

fn apply_ack(
    ack: ExternalFrameAck,
    active: &mut Option<ActiveMedia>,
    lease: &mut Option<Lease>,
    dropped_frames: &mut u64,
    dropped_media_ms: &mut u64,
    frames_delivered: &mut u64,
    bytes_delivered: &mut u64,
) -> Result<()> {
    let Some(current) = lease.as_ref() else {
        return Err(MusicStreamError::InvalidConfig(
            "external Opus frame ack has no outstanding lease".to_owned(),
        ));
    };
    if current.id != ack.lease_id || current.generation != ack.generation {
        return Err(MusicStreamError::InvalidConfig(
            "external Opus frame ack does not match the outstanding lease".to_owned(),
        ));
    }
    let current = lease.take().expect("outstanding lease was checked");
    if ack.outcome == ExternalFrameOutcome::OutputUnavailable {
        active.take();
        return Ok(());
    }
    let Some(media) = active
        .as_mut()
        .filter(|media| media.generation == ack.generation)
    else {
        return Ok(());
    };
    match ack.outcome {
        ExternalFrameOutcome::Sent | ExternalFrameOutcome::Late => {
            media.media_sent_ms = media.media_sent_ms.saturating_add(current.duration_ms);
            if ack.outcome == ExternalFrameOutcome::Late {
                *dropped_frames = dropped_frames.saturating_add(1);
                *dropped_media_ms = dropped_media_ms.saturating_add(current.duration_ms);
            } else {
                *frames_delivered = frames_delivered.saturating_add(1);
                *bytes_delivered = bytes_delivered.saturating_add(current.payload_len as u64);
            }
            let duration = Duration::from_millis(current.duration_ms.max(1));
            let anchored = media.deadline.unwrap_or_else(Instant::now) + duration;
            let now = Instant::now();
            let acknowledgement_late =
                now.saturating_duration_since(current.delivered_at) > duration;
            // A delayed consumer must expose its accumulated lateness to the next pull so stale
            // queued media is discarded. Once the consumer is prompt again, re-anchor exactly as
            // the RTP sender does to avoid a burst of catch-up packets.
            media.deadline = Some(if acknowledgement_late {
                anchored
            } else if anchored <= now {
                now + duration
            } else {
                anchored
            });
        }
        ExternalFrameOutcome::Cancelled => {}
        ExternalFrameOutcome::OutputUnavailable => unreachable!("handled before media lookup"),
    }
    Ok(())
}

fn emit_output_unavailable(
    events: &mpsc::Sender<WorkerEvent>,
    pending: &mut VecDeque<WorkerEvent>,
    generation: u64,
) {
    emit_event(
        events,
        pending,
        WorkerEvent::CurrentFailed {
            generation,
            code: crate::error::ErrorCode::StreamClosed,
            message: "external Opus output is unavailable".to_owned(),
        },
    );
}

#[allow(clippy::too_many_arguments)]
fn publish_progress(
    progress: &watch::Sender<StreamRuntimeProgress>,
    active: Option<&ActiveMedia>,
    frames_delivered: u64,
    bytes_delivered: u64,
    dropped_frames: u64,
    dropped_media_ms: u64,
    latency_recoveries: u64,
    underruns: u64,
    max_lateness_ms: u64,
) {
    let Some(media) = active else {
        return;
    };
    progress.send_replace(StreamRuntimeProgress {
        generation: media.generation,
        start_position_ms: media.start_position_ms,
        media_sent_ms: media.media_sent_ms,
        packets_sent: frames_delivered,
        bytes_sent: bytes_delivered,
        dropped_frames,
        dropped_media_ms,
        latency_recoveries,
        underruns,
        buffered_ms: media.receiver.buffered_ms(),
        max_lateness_ms,
        sequence: 0,
        rtp_timestamp: 0,
        latest_receiver_report: None,
    });
}

fn emit_event(
    sender: &mpsc::Sender<WorkerEvent>,
    pending: &mut VecDeque<WorkerEvent>,
    event: WorkerEvent,
) {
    match sender.try_send(event) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(event)) => pending.push_back(event),
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

fn flush_events(sender: &mpsc::Sender<WorkerEvent>, pending: &mut VecDeque<WorkerEvent>) {
    while let Some(event) = pending.pop_front() {
        match sender.try_send(event) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(event)) => {
                pending.push_front(event);
                return;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return,
        }
    }
}
