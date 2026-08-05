use tokio::sync::mpsc;

use crate::session::WorkerEvent;

/// Bounded outbox for events produced by a persistent output worker.
///
/// The actor channel is deliberately bounded. Output workers must keep pacing and accepting
/// control commands while that channel is full, so pending state is represented by semantic
/// slots instead of a second, hidden queue. Generation-scoped snapshots coalesce to the newest
/// value; an output failure supersedes all media state because the output can no longer recover.
#[derive(Debug, Default)]
pub(super) struct PendingWorkerEvents {
    output_failure: Option<WorkerEvent>,
    prebuffer: Option<WorkerEvent>,
    terminal: Option<WorkerEvent>,
    quality: Option<WorkerEvent>,
}

impl PendingWorkerEvents {
    fn store(&mut self, event: WorkerEvent) {
        match event {
            event @ WorkerEvent::OutputFailed { .. } => {
                if self.output_failure.is_none() {
                    self.output_failure = Some(event);
                }
                self.prebuffer = None;
                self.terminal = None;
                self.quality = None;
            }
            _ if self.output_failure.is_some() => {}
            event @ WorkerEvent::CurrentPrebufferReady { .. } => self.prebuffer = Some(event),
            event @ (WorkerEvent::CurrentEnded { .. } | WorkerEvent::CurrentFailed { .. }) => {
                self.terminal = Some(event);
            }
            event @ WorkerEvent::CurrentNetworkQualityChanged { .. } => {
                self.quality = Some(event);
            }
            WorkerEvent::CurrentSourceClassified { .. }
            | WorkerEvent::NextReady { .. }
            | WorkerEvent::NextFailed { .. }
            | WorkerEvent::StartupTimedOut { .. } => {
                unreachable!("producer events never originate from an output worker");
            }
        }
    }

    pub(super) fn pop(&mut self) -> Option<WorkerEvent> {
        self.output_failure
            .take()
            .or_else(|| self.prebuffer.take())
            .or_else(|| self.terminal.take())
            .or_else(|| self.quality.take())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.output_failure.is_none()
            && self.prebuffer.is_none()
            && self.terminal.is_none()
            && self.quality.is_none()
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

pub(super) fn emit_worker_event(
    sender: &mpsc::Sender<WorkerEvent>,
    pending: &mut PendingWorkerEvents,
    event: WorkerEvent,
) {
    // Once an older event is pending, never let a newly produced event take a channel slot first.
    // The receiver can free capacity between the worker's flush and its next event.
    if !pending.is_empty() {
        pending.store(event);
        return;
    }
    match sender.try_send(event) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(event)) => pending.store(event),
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

pub(super) fn flush_worker_events(
    sender: &mpsc::Sender<WorkerEvent>,
    pending: &mut PendingWorkerEvents,
) {
    while let Some(event) = pending.pop() {
        match sender.try_send(event) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(event)) => {
                pending.store(event);
                return;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Clearing is required: reserve() on a closed sender completes immediately, so a
                // retained event would otherwise turn the output actor into a hot loop.
                pending.clear();
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::quality::{RtcpNetworkQualityLevel, RtcpQualityWindowSnapshot};

    #[tokio::test]
    async fn pending_event_cannot_be_overtaken_when_channel_capacity_returns() {
        let (sender, mut receiver) = mpsc::channel(1);
        sender
            .try_send(WorkerEvent::CurrentPrebufferReady { generation: 0 })
            .expect("fill channel");
        let mut pending = PendingWorkerEvents::default();
        emit_worker_event(
            &sender,
            &mut pending,
            WorkerEvent::CurrentPrebufferReady { generation: 1 },
        );

        assert!(matches!(
            receiver.recv().await,
            Some(WorkerEvent::CurrentPrebufferReady { generation: 0 })
        ));
        emit_worker_event(
            &sender,
            &mut pending,
            WorkerEvent::CurrentEnded { generation: 1 },
        );
        assert!(receiver.try_recv().is_err(), "new event bypassed backlog");

        flush_worker_events(&sender, &mut pending);
        assert!(matches!(
            receiver.recv().await,
            Some(WorkerEvent::CurrentPrebufferReady { generation: 1 })
        ));
        flush_worker_events(&sender, &mut pending);
        assert!(matches!(
            receiver.recv().await,
            Some(WorkerEvent::CurrentEnded { generation: 1 })
        ));
        assert!(pending.is_empty());
    }

    #[test]
    fn output_failure_supersedes_all_pending_media_state() {
        let mut pending = PendingWorkerEvents::default();
        pending.store(WorkerEvent::CurrentPrebufferReady { generation: 1 });
        pending.store(WorkerEvent::CurrentEnded { generation: 1 });
        pending.store(WorkerEvent::CurrentNetworkQualityChanged {
            generation: 1,
            quality: RtcpNetworkQualityLevel::Good,
            snapshot: RtcpQualityWindowSnapshot::default(),
        });
        pending.store(WorkerEvent::OutputFailed {
            code: ErrorCode::OutputError,
            message: "socket closed".to_owned(),
        });
        pending.store(WorkerEvent::CurrentPrebufferReady { generation: 2 });

        assert!(matches!(
            pending.pop(),
            Some(WorkerEvent::OutputFailed {
                code: ErrorCode::OutputError,
                ..
            })
        ));
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn closed_channel_discards_pending_events() {
        let (sender, receiver) = mpsc::channel(1);
        let mut pending = PendingWorkerEvents::default();
        pending.store(WorkerEvent::CurrentEnded { generation: 1 });
        drop(receiver);

        flush_worker_events(&sender, &mut pending);

        assert!(pending.is_empty());
    }
}
