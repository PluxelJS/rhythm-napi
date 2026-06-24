use std::future::Future;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{ErrorCode, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeTaskFailure {
    pub name: String,
    pub code: ErrorCode,
    pub message: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeTaskShutdownReport {
    pub completed: usize,
    pub failed: Vec<RuntimeTaskFailure>,
    pub panicked: usize,
    pub aborted: usize,
    pub timed_out: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenerationScopedTask<T> {
    generation: u64,
    task: T,
}

impl<T> GenerationScopedTask<T> {
    #[must_use]
    pub fn new(generation: u64, task: T) -> Self {
        Self { generation, task }
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn task(&self) -> &T {
        &self.task
    }

    #[must_use]
    pub fn task_mut(&mut self) -> &mut T {
        &mut self.task
    }

    #[must_use]
    pub fn into_task(self) -> T {
        self.task
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenerationTaskSlot<T> {
    active: Option<GenerationScopedTask<T>>,
}

impl<T> Default for GenerationTaskSlot<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GenerationTaskSlot<T> {
    #[must_use]
    pub fn new() -> Self {
        Self { active: None }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.active.is_none()
    }

    #[must_use]
    pub fn generation(&self) -> Option<u64> {
        self.active.as_ref().map(GenerationScopedTask::generation)
    }

    #[must_use]
    pub fn get(&self) -> Option<&T> {
        self.active.as_ref().map(GenerationScopedTask::task)
    }

    #[must_use]
    pub fn get_mut(&mut self) -> Option<&mut T> {
        self.active.as_mut().map(GenerationScopedTask::task_mut)
    }

    #[must_use]
    pub fn get_if_generation(&self, generation: u64) -> Option<&T> {
        self.active
            .as_ref()
            .filter(|active| active.generation() == generation)
            .map(GenerationScopedTask::task)
    }

    #[must_use]
    pub fn get_mut_if_generation(&mut self, generation: u64) -> Option<&mut T> {
        self.active
            .as_mut()
            .filter(|active| active.generation() == generation)
            .map(GenerationScopedTask::task_mut)
    }

    pub fn insert(&mut self, generation: u64, task: T) -> Option<GenerationScopedTask<T>> {
        self.active
            .replace(GenerationScopedTask::new(generation, task))
    }

    pub fn insert_task(&mut self, generation: u64, task: T) -> Option<T> {
        self.insert(generation, task)
            .map(GenerationScopedTask::into_task)
    }

    pub fn take(&mut self) -> Option<GenerationScopedTask<T>> {
        self.active.take()
    }

    pub fn take_task(&mut self) -> Option<T> {
        self.take().map(GenerationScopedTask::into_task)
    }

    pub fn take_if_generation(&mut self, generation: u64) -> Option<GenerationScopedTask<T>> {
        if self.generation() == Some(generation) {
            self.active.take()
        } else {
            None
        }
    }

    pub fn take_task_if_generation(&mut self, generation: u64) -> Option<T> {
        self.take_if_generation(generation)
            .map(GenerationScopedTask::into_task)
    }
}

#[derive(Debug)]
pub struct RuntimeTaskGroup {
    token: CancellationToken,
    tasks: JoinSet<(String, Result<()>)>,
}

impl Default for RuntimeTaskGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeTaskGroup {
    #[must_use]
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            tasks: JoinSet::new(),
        }
    }

    #[must_use]
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    #[must_use]
    pub fn child_token(&self) -> CancellationToken {
        self.token.child_token()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub fn cancel(&self) {
        self.token.cancel();
    }

    pub fn spawn<F>(&mut self, name: impl Into<String>, future: F)
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let name = name.into();
        self.tasks.spawn(async move {
            let result = future.await;
            (name, result)
        });
    }

    pub fn spawn_with_token<F, Fut>(&mut self, name: impl Into<String>, task: F)
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let token = self.child_token();
        self.spawn(name, task(token));
    }

    pub async fn shutdown(mut self, timeout: Duration) -> RuntimeTaskShutdownReport {
        self.cancel();
        let mut report = RuntimeTaskShutdownReport::default();

        if tokio::time::timeout(timeout, join_all(&mut self.tasks, &mut report))
            .await
            .is_err()
        {
            report.timed_out = true;
            self.tasks.abort_all();
            join_all(&mut self.tasks, &mut report).await;
        }

        report
    }
}

impl Drop for RuntimeTaskGroup {
    fn drop(&mut self) {
        self.token.cancel();
        self.tasks.abort_all();
    }
}

async fn join_all(
    tasks: &mut JoinSet<(String, Result<()>)>,
    report: &mut RuntimeTaskShutdownReport,
) {
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((_, Ok(()))) => {
                report.completed += 1;
            }
            Ok((name, Err(error))) => {
                report.failed.push(RuntimeTaskFailure {
                    name,
                    code: error.code(),
                    message: error.to_string(),
                });
            }
            Err(error) if error.is_cancelled() => {
                report.aborted += 1;
            }
            Err(error) if error.is_panic() => {
                report.panicked += 1;
            }
            Err(error) => {
                report.failed.push(RuntimeTaskFailure {
                    name: "<unknown>".to_owned(),
                    code: ErrorCode::Internal,
                    message: error.to_string(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::MusicStreamError;

    #[test]
    fn generation_task_slot_only_takes_matching_generation() {
        let mut slot = GenerationTaskSlot::new();
        assert!(slot.is_empty());

        assert!(slot.insert(7, "current").is_none());
        assert_eq!(slot.generation(), Some(7));
        assert_eq!(slot.get_if_generation(7), Some(&"current"));
        assert_eq!(slot.get_if_generation(8), None);

        assert!(slot.take_if_generation(8).is_none());
        assert_eq!(slot.generation(), Some(7));

        let task = slot
            .take_if_generation(7)
            .expect("matching generation should be removed");
        assert_eq!(task.generation(), 7);
        assert_eq!(task.into_task(), "current");
        assert!(slot.is_empty());
    }

    #[test]
    fn generation_task_slot_can_take_inner_task_directly() {
        let mut slot = GenerationTaskSlot::new();
        slot.insert(9, "current");

        assert_eq!(slot.take_task_if_generation(8), None);
        assert_eq!(slot.take_task_if_generation(9), Some("current"));
        assert!(slot.is_empty());

        slot.insert(10, "next");
        assert_eq!(slot.take_task(), Some("next"));
        assert!(slot.is_empty());
    }

    #[test]
    fn generation_task_slot_returns_displaced_task_on_replace() {
        let mut slot = GenerationTaskSlot::new();
        assert!(slot.insert(1, "old").is_none());

        let old = slot.insert(2, "new").expect("old task");

        assert_eq!(old.generation(), 1);
        assert_eq!(old.into_task(), "old");
        assert_eq!(slot.generation(), Some(2));
        assert_eq!(slot.get(), Some(&"new"));
    }

    #[test]
    fn generation_task_slot_can_return_displaced_inner_task_directly() {
        let mut slot = GenerationTaskSlot::new();
        assert_eq!(slot.insert_task(1, "old"), None);
        assert_eq!(slot.insert_task(2, "new"), Some("old"));
        assert_eq!(slot.generation(), Some(2));
        assert_eq!(slot.get(), Some(&"new"));
    }

    #[tokio::test]
    async fn shutdown_cancels_and_joins_cooperative_tasks() {
        let observed_cancel = Arc::new(AtomicBool::new(false));
        let observed_cancel_task = Arc::clone(&observed_cancel);
        let mut group = RuntimeTaskGroup::new();
        group.spawn_with_token("cooperative", move |token| async move {
            token.cancelled().await;
            observed_cancel_task.store(true, Ordering::SeqCst);
            Ok(())
        });

        let report = group.shutdown(Duration::from_secs(1)).await;

        assert_eq!(report.completed, 1);
        assert!(report.failed.is_empty());
        assert_eq!(report.aborted, 0);
        assert!(!report.timed_out);
        assert!(observed_cancel.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_reports_task_failures_without_hiding_successes() {
        let mut group = RuntimeTaskGroup::new();
        group.spawn("success", async { Ok(()) });
        group.spawn("failure", async {
            Err(MusicStreamError::InvalidConfig("bad config".to_owned()))
        });

        let report = group.shutdown(Duration::from_secs(1)).await;

        assert_eq!(report.completed, 1);
        assert_eq!(report.failed.len(), 1);
        assert_eq!(report.failed[0].name, "failure");
        assert_eq!(report.failed[0].code, ErrorCode::InvalidConfig);
        assert_eq!(report.aborted, 0);
        assert!(!report.timed_out);
    }

    #[tokio::test]
    async fn shutdown_aborts_uncooperative_tasks_after_timeout() {
        let mut group = RuntimeTaskGroup::new();
        group.spawn("uncooperative", async {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });

        let report = group.shutdown(Duration::from_millis(10)).await;

        assert_eq!(report.completed, 0);
        assert!(report.failed.is_empty());
        assert_eq!(report.aborted, 1);
        assert!(report.timed_out);
    }
}
