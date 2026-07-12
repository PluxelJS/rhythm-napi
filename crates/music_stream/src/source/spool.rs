use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use super::{BlockingReadObserver, TempArtifactCleanup};

#[derive(Debug)]
enum SpoolTerminal {
    Complete,
    Failed(String),
}

#[derive(Debug, Default)]
struct SpoolState {
    available_bytes: u64,
    terminal: Option<SpoolTerminal>,
}

#[derive(Debug)]
struct GrowingSpoolInner {
    path: PathBuf,
    _cleanup: Arc<TempArtifactCleanup>,
    state: Mutex<SpoolState>,
    changed: Condvar,
}

#[derive(Debug)]
pub(super) struct GrowingSpoolWriter {
    file: Option<tokio::fs::File>,
    inner: Arc<GrowingSpoolInner>,
}

#[derive(Clone, Debug)]
pub(crate) struct GrowingSpool {
    inner: Arc<GrowingSpoolInner>,
}

#[derive(Debug)]
pub(crate) struct GrowingSpoolReader {
    file: std::fs::File,
    inner: Arc<GrowingSpoolInner>,
    position: u64,
    cancellation: CancellationToken,
    wait_observer: Option<Arc<dyn BlockingReadObserver>>,
}

pub(super) fn growing_spool(
    file: std::fs::File,
    path: PathBuf,
    cleanup: Arc<TempArtifactCleanup>,
) -> (GrowingSpoolWriter, GrowingSpool) {
    let inner = Arc::new(GrowingSpoolInner {
        path,
        _cleanup: cleanup,
        state: Mutex::new(SpoolState::default()),
        changed: Condvar::new(),
    });
    (
        GrowingSpoolWriter {
            file: Some(tokio::fs::File::from_std(file)),
            inner: Arc::clone(&inner),
        },
        GrowingSpool { inner },
    )
}

impl GrowingSpoolWriter {
    pub(super) async fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.file
            .as_mut()
            .expect("growing spool writer already finished")
            .write_all(bytes)
            .await?;
        let mut state = self
            .inner
            .state
            .lock()
            .expect("growing spool lock poisoned");
        state.available_bytes = state
            .available_bytes
            .saturating_add(bytes.len().try_into().unwrap_or(u64::MAX));
        self.inner.changed.notify_all();
        Ok(())
    }

    pub(super) async fn finish(mut self) -> std::io::Result<()> {
        if let Some(mut file) = self.file.take() {
            file.flush().await?;
        }
        let mut state = self
            .inner
            .state
            .lock()
            .expect("growing spool lock poisoned");
        state.terminal = Some(SpoolTerminal::Complete);
        self.inner.changed.notify_all();
        Ok(())
    }

    pub(super) fn fail(&mut self, message: impl Into<String>) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("growing spool lock poisoned");
        if state.terminal.is_none() {
            state.terminal = Some(SpoolTerminal::Failed(message.into()));
            self.inner.changed.notify_all();
        }
    }
}

impl Drop for GrowingSpoolWriter {
    fn drop(&mut self) {
        self.fail("growing spool writer closed before completion");
    }
}

impl GrowingSpool {
    pub(crate) fn open_reader(
        &self,
        cancellation: CancellationToken,
    ) -> std::io::Result<GrowingSpoolReader> {
        Ok(GrowingSpoolReader {
            file: std::fs::File::open(&self.inner.path)?,
            inner: Arc::clone(&self.inner),
            position: 0,
            cancellation,
            wait_observer: None,
        })
    }
}

impl GrowingSpoolReader {
    pub(crate) fn set_wait_observer(&mut self, observer: Arc<dyn BlockingReadObserver>) {
        self.wait_observer = Some(observer);
    }

    fn wait_for_bytes(&self) -> std::io::Result<()> {
        if let Some(observer) = &self.wait_observer {
            observer.before_wait();
        }
        let result = loop {
            if self.cancellation.is_cancelled() {
                break Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "growing spool reader cancelled",
                ));
            }
            let state = self
                .inner
                .state
                .lock()
                .expect("growing spool lock poisoned");
            if self.position < state.available_bytes || state.terminal.is_some() {
                break Ok(());
            }
            let (_state, _) = self
                .inner
                .changed
                .wait_timeout(state, Duration::from_millis(20))
                .expect("growing spool lock poisoned");
        };
        if let Some(observer) = &self.wait_observer {
            observer.after_wait();
        }
        result
    }
}

impl Read for GrowingSpoolReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            let (available_bytes, terminal) = {
                let state = self
                    .inner
                    .state
                    .lock()
                    .expect("growing spool lock poisoned");
                let terminal = match state.terminal.as_ref() {
                    Some(SpoolTerminal::Complete) => Some(Ok(())),
                    Some(SpoolTerminal::Failed(message)) => Some(Err(message.clone())),
                    None => None,
                };
                (state.available_bytes, terminal)
            };
            if self.position < available_bytes {
                let readable = usize::try_from(available_bytes - self.position)
                    .unwrap_or(usize::MAX)
                    .min(output.len());
                let read = self.file.read(&mut output[..readable])?;
                self.position = self.position.saturating_add(read as u64);
                if read > 0 {
                    return Ok(read);
                }
            }
            match terminal {
                Some(Ok(())) => return Ok(0),
                Some(Err(message)) => return Err(std::io::Error::other(message)),
                None => self.wait_for_bytes()?,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::Semaphore;

    use super::*;

    #[tokio::test]
    async fn readers_joining_the_same_growing_spool_each_start_at_zero() {
        let named = tempfile::NamedTempFile::new().expect("tempfile");
        let (file, path) = named.into_parts();
        let filesystem_path = path.to_path_buf();
        let quota = Arc::new(Semaphore::new(1))
            .acquire_owned()
            .await
            .expect("quota");
        let cleanup = Arc::new(TempArtifactCleanup::new(
            path,
            crate::source::TempfileQuota {
                global: quota,
                preload: None,
            },
        ));
        let (mut writer, spool) = growing_spool(file, filesystem_path, cleanup);
        let first = spool
            .open_reader(CancellationToken::new())
            .expect("first reader");
        let second = spool
            .open_reader(CancellationToken::new())
            .expect("second reader");
        let first_task = tokio::task::spawn_blocking(move || {
            let mut reader = first;
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).expect("first read");
            bytes
        });
        let second_task = tokio::task::spawn_blocking(move || {
            let mut reader = second;
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).expect("second read");
            bytes
        });

        writer.write_all(b"abc").await.expect("prefix");
        tokio::task::yield_now().await;
        assert!(!first_task.is_finished());
        assert!(!second_task.is_finished());
        writer.write_all(b"def").await.expect("suffix");
        writer.finish().await.expect("finish");

        assert_eq!(first_task.await.expect("first task"), b"abcdef");
        assert_eq!(second_task.await.expect("second task"), b"abcdef");
    }
}
