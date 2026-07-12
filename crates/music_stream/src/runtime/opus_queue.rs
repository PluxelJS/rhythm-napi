use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::audio::frame::OpusFrame;
use crate::error::{MusicStreamError, Result};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct QueueSnapshot {
    buffered_ms: u64,
    sender_alive: bool,
}

#[derive(Debug)]
struct QueueState {
    frames: VecDeque<OpusFrame>,
    buffered_ms: u64,
    sender_handles: usize,
    sender_alive: bool,
    receiver_alive: bool,
}

#[derive(Debug)]
struct QueueInner {
    capacity_ms: u64,
    state: Mutex<QueueState>,
    space_available: Condvar,
    changed: watch::Sender<QueueSnapshot>,
}

#[derive(Debug)]
pub(super) struct OpusQueueSender {
    inner: Arc<QueueInner>,
}

#[derive(Debug)]
pub(super) struct OpusQueueReceiver {
    inner: Arc<QueueInner>,
    changed: watch::Receiver<QueueSnapshot>,
}

pub(super) fn bounded(capacity_ms: u64) -> (OpusQueueSender, OpusQueueReceiver) {
    let initial = QueueSnapshot {
        buffered_ms: 0,
        sender_alive: true,
    };
    let (changed, receiver) = watch::channel(initial);
    let inner = Arc::new(QueueInner {
        capacity_ms,
        state: Mutex::new(QueueState {
            frames: VecDeque::new(),
            buffered_ms: 0,
            sender_handles: 1,
            sender_alive: true,
            receiver_alive: true,
        }),
        space_available: Condvar::new(),
        changed,
    });
    (
        OpusQueueSender {
            inner: Arc::clone(&inner),
        },
        OpusQueueReceiver {
            inner,
            changed: receiver,
        },
    )
}

impl OpusQueueSender {
    pub(super) fn send_blocking(
        &self,
        frame: OpusFrame,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if frame.duration_ms > self.inner.capacity_ms {
            return Err(MusicStreamError::InvalidConfig(
                "Opus frame duration exceeds the encoded queue capacity".to_owned(),
            ));
        }

        let mut state = self.inner.state.lock().expect("Opus queue lock poisoned");
        while state
            .buffered_ms
            .saturating_add(frame.duration_ms)
            .gt(&self.inner.capacity_ms)
        {
            if !state.receiver_alive || cancellation.is_cancelled() {
                return Err(MusicStreamError::StreamClosed(
                    "Opus receiver was replaced or stopped".to_owned(),
                ));
            }
            let (next, _) = self
                .inner
                .space_available
                .wait_timeout(state, Duration::from_millis(20))
                .expect("Opus queue lock poisoned");
            state = next;
        }
        if !state.receiver_alive || cancellation.is_cancelled() {
            return Err(MusicStreamError::StreamClosed(
                "Opus receiver was replaced or stopped".to_owned(),
            ));
        }

        state.buffered_ms = state.buffered_ms.saturating_add(frame.duration_ms);
        state.frames.push_back(frame);
        self.publish(&state);
        Ok(())
    }

    fn publish(&self, state: &QueueState) {
        self.inner.changed.send_replace(QueueSnapshot {
            buffered_ms: state.buffered_ms,
            sender_alive: state.sender_alive,
        });
    }
}

impl Clone for OpusQueueSender {
    fn clone(&self) -> Self {
        let mut state = self.inner.state.lock().expect("Opus queue lock poisoned");
        state.sender_handles = state.sender_handles.saturating_add(1);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for OpusQueueSender {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().expect("Opus queue lock poisoned");
        state.sender_handles = state.sender_handles.saturating_sub(1);
        if state.sender_handles == 0 {
            state.sender_alive = false;
            self.publish(&state);
        }
    }
}

impl OpusQueueReceiver {
    #[must_use]
    pub(super) fn buffered_ms(&self) -> u64 {
        self.changed.borrow().buffered_ms
    }

    #[must_use]
    pub(super) fn is_closed(&self) -> bool {
        !self.changed.borrow().sender_alive
    }

    #[must_use]
    pub(super) fn is_drained(&self) -> bool {
        let snapshot = *self.changed.borrow();
        !snapshot.sender_alive && snapshot.buffered_ms == 0
    }

    pub(super) fn try_recv(&self) -> Option<OpusFrame> {
        let mut state = self.inner.state.lock().expect("Opus queue lock poisoned");
        let frame = state.frames.pop_front()?;
        state.buffered_ms = state.buffered_ms.saturating_sub(frame.duration_ms);
        self.inner.changed.send_replace(QueueSnapshot {
            buffered_ms: state.buffered_ms,
            sender_alive: state.sender_alive,
        });
        self.inner.space_available.notify_one();
        Some(frame)
    }

    /// Removes an obsolete frame only when another frame is immediately
    /// available. The sender must never discard the sole playable frame while
    /// a producer is temporarily starved.
    pub(super) fn try_drop_oldest_if_followed(&self) -> Option<OpusFrame> {
        let mut state = self.inner.state.lock().expect("Opus queue lock poisoned");
        if state.frames.len() < 2 {
            return None;
        }
        let frame = state.frames.pop_front().expect("queue length was checked");
        state.buffered_ms = state.buffered_ms.saturating_sub(frame.duration_ms);
        self.inner.changed.send_replace(QueueSnapshot {
            buffered_ms: state.buffered_ms,
            sender_alive: state.sender_alive,
        });
        self.inner.space_available.notify_one();
        Some(frame)
    }

    pub(super) async fn changed(&mut self) {
        let _ = self.changed.changed().await;
    }
}

impl Drop for OpusQueueReceiver {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().expect("Opus queue lock poisoned");
        state.receiver_alive = false;
        state.frames.clear();
        state.buffered_ms = 0;
        self.inner.space_available.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn frame(duration_ms: u64) -> OpusFrame {
        OpusFrame {
            generation: 1,
            payload: Bytes::from_static(b"opus"),
            samples_per_channel: 960,
            duration_ms,
            marker: false,
            track_position_samples: 0,
        }
    }

    #[test]
    fn queue_is_bounded_by_media_duration() {
        let (sender, receiver) = bounded(40);
        let cancellation = CancellationToken::new();
        sender
            .send_blocking(frame(20), &cancellation)
            .expect("first frame");
        sender
            .send_blocking(frame(20), &cancellation)
            .expect("second frame");
        assert_eq!(receiver.buffered_ms(), 40);
        assert_eq!(receiver.try_recv().expect("queued frame").duration_ms, 20);
        assert_eq!(receiver.buffered_ms(), 20);
    }

    #[test]
    fn stale_drop_preserves_the_only_playable_frame() {
        let (sender, receiver) = bounded(40);
        let cancellation = CancellationToken::new();
        sender
            .send_blocking(frame(20), &cancellation)
            .expect("first frame");
        assert!(receiver.try_drop_oldest_if_followed().is_none());

        sender
            .send_blocking(frame(20), &cancellation)
            .expect("second frame");
        assert_eq!(
            receiver
                .try_drop_oldest_if_followed()
                .expect("stale frame")
                .duration_ms,
            20
        );
        assert_eq!(receiver.buffered_ms(), 20);
    }

    #[tokio::test]
    async fn receiver_change_notification_observes_close() {
        let (sender, mut receiver) = bounded(40);
        drop(sender);
        receiver.changed().await;
        assert!(receiver.is_drained());
    }

    #[tokio::test]
    async fn queue_closes_only_after_the_last_sender_guard_drops() {
        let (sender, mut receiver) = bounded(40);
        let lifetime_guard = sender.clone();
        drop(sender);
        assert!(!receiver.is_closed());

        drop(lifetime_guard);
        receiver.changed().await;

        assert!(receiver.is_drained());
    }
}
