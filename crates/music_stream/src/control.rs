use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub(crate) struct PauseGate {
    paused: AtomicBool,
    blocking_lock: Mutex<()>,
    blocking_changed: Condvar,
    async_state: watch::Sender<bool>,
}

impl Default for PauseGate {
    fn default() -> Self {
        let (async_state, _) = watch::channel(false);
        Self {
            paused: AtomicBool::new(false),
            blocking_lock: Mutex::new(()),
            blocking_changed: Condvar::new(),
            async_state,
        }
    }
}

impl PauseGate {
    pub(crate) fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    pub(crate) fn pause(&self) {
        let _guard = self.blocking_lock.lock().expect("pause gate lock poisoned");
        self.paused.store(true, Ordering::Release);
        self.async_state.send_replace(true);
    }

    pub(crate) fn resume(&self) {
        let _guard = self.blocking_lock.lock().expect("pause gate lock poisoned");
        self.paused.store(false, Ordering::Release);
        self.async_state.send_replace(false);
        self.blocking_changed.notify_all();
    }

    pub(crate) async fn wait_async(&self, cancellation: &CancellationToken) -> bool {
        let mut state = self.async_state.subscribe();
        while *state.borrow_and_update() {
            tokio::select! {
                _ = cancellation.cancelled() => return false,
                changed = state.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                }
            }
        }
        !cancellation.is_cancelled()
    }

    pub(crate) async fn wait_for_pause(&self, cancellation: &CancellationToken) -> bool {
        let mut state = self.async_state.subscribe();
        loop {
            if *state.borrow_and_update() {
                return true;
            }
            tokio::select! {
                _ = cancellation.cancelled() => return false,
                changed = state.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                }
            }
        }
    }

    pub(crate) fn wait_blocking(&self, cancellation: &CancellationToken) -> bool {
        let mut guard = self.blocking_lock.lock().expect("pause gate lock poisoned");
        while self.paused.load(Ordering::Acquire) {
            if cancellation.is_cancelled() {
                return false;
            }
            let (next, _) = self
                .blocking_changed
                .wait_timeout(guard, Duration::from_millis(20))
                .expect("pause gate lock poisoned");
            guard = next;
        }
        !cancellation.is_cancelled()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn pause_gate_resumes_async_and_blocking_waiters() {
        let gate = Arc::new(PauseGate::default());
        gate.pause();
        let cancellation = CancellationToken::new();
        let async_gate = Arc::clone(&gate);
        let async_cancellation = cancellation.clone();
        let async_waiter =
            tokio::spawn(async move { async_gate.wait_async(&async_cancellation).await });
        let blocking_gate = Arc::clone(&gate);
        let blocking_cancellation = cancellation.clone();
        let blocking_waiter = tokio::task::spawn_blocking(move || {
            blocking_gate.wait_blocking(&blocking_cancellation)
        });

        tokio::task::yield_now().await;
        gate.resume();
        assert!(async_waiter.await.expect("async waiter"));
        assert!(blocking_waiter.await.expect("blocking waiter"));
    }
}
