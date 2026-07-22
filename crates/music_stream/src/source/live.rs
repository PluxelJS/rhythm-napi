use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{MusicStreamError, Result};
use crate::model::TrackSource;

use super::{is_retryable_http, map_http_error, shared_http_client};

const LIVE_HTTP_OPEN_TIMEOUT: Duration = Duration::from_secs(30);
const LIVE_HTTP_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const LIVE_HTTP_BUFFER_BYTES: usize = 512 * 1024;
const LIVE_HTTP_MAX_RETRIES: u8 = 2;
const LIVE_HTTP_RETRY_BACKOFF: Duration = Duration::from_millis(250);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpLiveStreamConfig {
    pub open_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_buffered_bytes: usize,
    pub max_retries: u8,
    pub retry_backoff: Duration,
}

#[derive(Clone, Debug)]
pub(crate) struct LiveByteBudget {
    semaphore: Arc<Semaphore>,
    capacity: usize,
}

impl LiveByteBudget {
    pub(crate) fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 || capacity > u32::MAX as usize || capacity > Semaphore::MAX_PERMITS {
            return Err(MusicStreamError::InvalidConfig(
                "global live byte budget must fit in a positive u32".to_owned(),
            ));
        }
        Ok(Self {
            semaphore: Arc::new(Semaphore::new(capacity)),
            capacity,
        })
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(super) async fn acquire(&self, bytes: usize) -> Result<OwnedSemaphorePermit> {
        let permits = u32::try_from(bytes).map_err(|_| {
            MusicStreamError::InvalidSource("live HTTP chunk is too large".to_owned())
        })?;
        Arc::clone(&self.semaphore)
            .acquire_many_owned(permits)
            .await
            .map_err(|_| {
                MusicStreamError::StreamClosed("global live byte budget closed".to_owned())
            })
    }
}

impl Default for HttpLiveStreamConfig {
    fn default() -> Self {
        Self {
            open_timeout: LIVE_HTTP_OPEN_TIMEOUT,
            idle_timeout: LIVE_HTTP_IDLE_TIMEOUT,
            max_buffered_bytes: LIVE_HTTP_BUFFER_BYTES,
            max_retries: LIVE_HTTP_MAX_RETRIES,
            retry_backoff: LIVE_HTTP_RETRY_BACKOFF,
        }
    }
}

impl HttpLiveStreamConfig {
    pub fn validate(&self) -> Result<()> {
        if self.open_timeout.is_zero()
            || self.idle_timeout.is_zero()
            || self.max_buffered_bytes == 0
        {
            return Err(MusicStreamError::InvalidConfig(
                "live HTTP open timeout, idle timeout, and buffer size must be greater than zero"
                    .to_owned(),
            ));
        }
        if self.max_retries > 0 && self.retry_backoff.is_zero() {
            return Err(MusicStreamError::InvalidConfig(
                "live HTTP retry backoff must be non-zero when retries are enabled".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HttpLiveStreamReport {
    pub bytes_read: u64,
    pub retries: u8,
    pub completed: bool,
    pub stopped: bool,
}

#[derive(Debug)]
struct ByteChunk {
    bytes: Bytes,
    _stream_budget: OwnedSemaphorePermit,
    _global_budget: Arc<OwnedSemaphorePermit>,
}

pub(crate) trait BlockingReadObserver: std::fmt::Debug + Send + Sync {
    fn before_wait(&self);
    fn after_wait(&self);
}

#[derive(Debug)]
enum ByteMessage {
    Data(ByteChunk),
    Failed(String),
}

#[derive(Debug)]
pub struct StreamingByteReader {
    receiver: mpsc::Receiver<ByteMessage>,
    current: Option<ByteChunk>,
    offset: usize,
    wait_observer: Option<Arc<dyn BlockingReadObserver>>,
}

#[derive(Clone, Debug)]
pub struct StreamingByteWriter {
    sender: mpsc::Sender<ByteMessage>,
    stream_budget: Arc<Semaphore>,
    global_budget: LiveByteBudget,
    max_chunk_bytes: usize,
}

impl StreamingByteReader {
    #[cfg(test)]
    pub fn new(max_buffered_bytes: usize) -> Result<(StreamingByteWriter, Self)> {
        Self::with_global_budget(max_buffered_bytes, LiveByteBudget::new(max_buffered_bytes)?)
    }

    pub(crate) fn with_global_budget(
        max_buffered_bytes: usize,
        global_budget: LiveByteBudget,
    ) -> Result<(StreamingByteWriter, Self)> {
        if max_buffered_bytes == 0 || max_buffered_bytes > u32::MAX as usize {
            return Err(MusicStreamError::InvalidConfig(
                "streaming byte budget must fit in a positive u32".to_owned(),
            ));
        }
        let (sender, receiver) = mpsc::channel(64);
        Ok((
            StreamingByteWriter {
                sender,
                stream_budget: Arc::new(Semaphore::new(max_buffered_bytes)),
                global_budget,
                max_chunk_bytes: max_buffered_bytes,
            },
            Self {
                receiver,
                current: None,
                offset: 0,
                wait_observer: None,
            },
        ))
    }
}

impl StreamingByteReader {
    pub(crate) fn set_wait_observer(&mut self, observer: Arc<dyn BlockingReadObserver>) {
        self.wait_observer = Some(observer);
    }
}

impl StreamingByteWriter {
    pub(super) fn global_byte_budget(&self) -> LiveByteBudget {
        self.global_budget.clone()
    }

    pub async fn push(&self, bytes: Bytes) -> Result<()> {
        let mut offset = 0;
        while offset < bytes.len() {
            let end = offset.saturating_add(self.max_chunk_bytes).min(bytes.len());
            self.push_one(bytes.slice(offset..end)).await?;
            offset = end;
        }
        Ok(())
    }

    async fn push_one(&self, bytes: Bytes) -> Result<()> {
        let global_wait_started = std::time::Instant::now();
        let global_permit = self.global_budget.acquire(bytes.len()).await?;
        metrics::histogram!("music_stream.source.live_global_budget_wait_us")
            .record(global_wait_started.elapsed().as_micros() as f64);
        self.push_one_with_global_permit(bytes, Arc::new(global_permit))
            .await
    }

    async fn push_one_with_global_permit(
        &self,
        bytes: Bytes,
        global_permit: Arc<OwnedSemaphorePermit>,
    ) -> Result<()> {
        if global_permit.num_permits() < bytes.len()
            || !Arc::ptr_eq(global_permit.semaphore(), &self.global_budget.semaphore)
        {
            return Err(MusicStreamError::Internal(
                "live byte permit does not match its payload".to_owned(),
            ));
        }
        let permits = u32::try_from(bytes.len()).map_err(|_| {
            MusicStreamError::InvalidSource("live HTTP chunk is too large".to_owned())
        })?;
        let stream_permit = Arc::clone(&self.stream_budget)
            .acquire_many_owned(permits)
            .await
            .map_err(|_| MusicStreamError::StreamClosed("live byte bridge closed".to_owned()))?;
        self.sender
            .send(ByteMessage::Data(ByteChunk {
                bytes,
                _stream_budget: stream_permit,
                _global_budget: global_permit,
            }))
            .await
            .map_err(|_| MusicStreamError::StreamClosed("live byte bridge closed".to_owned()))
    }

    pub(super) async fn push_with_global_permit(
        &self,
        bytes: Bytes,
        global_permit: OwnedSemaphorePermit,
    ) -> Result<()> {
        if global_permit.num_permits() < bytes.len()
            || !Arc::ptr_eq(global_permit.semaphore(), &self.global_budget.semaphore)
        {
            return Err(MusicStreamError::Internal(
                "live byte permit does not match its payload".to_owned(),
            ));
        }
        let global_permit = Arc::new(global_permit);
        let mut offset = 0;
        while offset < bytes.len() {
            let end = offset.saturating_add(self.max_chunk_bytes).min(bytes.len());
            self.push_one_with_global_permit(bytes.slice(offset..end), Arc::clone(&global_permit))
                .await?;
            offset = end;
        }
        Ok(())
    }

    pub async fn fail(&self, message: impl Into<String>, cancellation: &CancellationToken) {
        tokio::select! {
            _ = cancellation.cancelled() => {}
            _ = self.sender.send(ByteMessage::Failed(message.into())) => {}
        }
    }
}

impl Read for StreamingByteReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            if let Some(current) = self.current.as_ref() {
                let remaining = &current.bytes[self.offset..];
                let copied = remaining.len().min(output.len());
                output[..copied].copy_from_slice(&remaining[..copied]);
                self.offset += copied;
                if self.offset == current.bytes.len() {
                    self.current = None;
                    self.offset = 0;
                }
                return Ok(copied);
            }
            if let Some(observer) = &self.wait_observer {
                observer.before_wait();
            }
            let message = self.receiver.blocking_recv();
            if let Some(observer) = &self.wait_observer {
                observer.after_wait();
            }
            match message {
                Some(ByteMessage::Data(chunk)) => self.current = Some(chunk),
                Some(ByteMessage::Failed(message)) => {
                    return Err(std::io::Error::other(message));
                }
                None => return Ok(0),
            }
        }
    }
}

#[derive(Debug)]
pub struct HttpLiveStream {
    pub reader: StreamingByteReader,
    pub cancellation: CancellationToken,
    pub task: JoinHandle<Result<HttpLiveStreamReport>>,
}

pub fn spawn_http_live_stream(
    source: &TrackSource,
    config: HttpLiveStreamConfig,
    global_byte_budget: LiveByteBudget,
) -> Result<HttpLiveStream> {
    config.validate()?;
    if !source.is_live() {
        return Err(MusicStreamError::InvalidSource(
            "live HTTP stream requires a live source".to_owned(),
        ));
    }
    let url = source
        .url
        .clone()
        .ok_or_else(|| MusicStreamError::InvalidSource("live source requires a URL".to_owned()))?;
    let headers = source.headers.clone();
    let (writer, reader) =
        StreamingByteReader::with_global_budget(config.max_buffered_bytes, global_byte_budget)?;
    let cancellation = CancellationToken::new();
    let worker_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        run_http_live_stream(url, headers, config, writer, worker_cancellation).await
    });
    Ok(HttpLiveStream {
        reader,
        cancellation,
        task,
    })
}

async fn run_http_live_stream(
    url: String,
    headers: std::collections::BTreeMap<String, String>,
    config: HttpLiveStreamConfig,
    writer: StreamingByteWriter,
    cancellation: CancellationToken,
) -> Result<HttpLiveStreamReport> {
    let client = shared_http_client();
    let mut report = HttpLiveStreamReport::default();

    loop {
        if cancellation.is_cancelled() {
            report.stopped = true;
            return Ok(report);
        }
        let attempt_started = Instant::now();
        let response = tokio::select! {
            _ = cancellation.cancelled() => {
                report.stopped = true;
                return Ok(report);
            }
            response = open_live_response(&client, &url, &headers, config.open_timeout) => response,
        };
        let mut response = match response {
            Ok(response) => {
                metrics::histogram!("music_stream.source.live_http_open_us")
                    .record(attempt_started.elapsed().as_micros() as f64);
                response
            }
            Err(error) if error.retryable && report.retries < config.max_retries => {
                report.retries += 1;
                metrics::counter!("music_stream.source.live_http_retries").increment(1);
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        report.stopped = true;
                        return Ok(report);
                    }
                    _ = tokio::time::sleep(config.retry_backoff) => {}
                }
                continue;
            }
            Err(error) => {
                writer.fail(error.error.to_string(), &cancellation).await;
                return Err(error.error);
            }
        };

        loop {
            let chunk = tokio::select! {
                _ = cancellation.cancelled() => {
                    report.stopped = true;
                    return Ok(report);
                }
                chunk = tokio::time::timeout(config.idle_timeout, response.chunk()) => chunk,
            };
            match chunk {
                Ok(Ok(Some(bytes))) => {
                    if report.bytes_read == 0 && !bytes.is_empty() {
                        metrics::histogram!("music_stream.source.live_http_first_body_byte_us")
                            .record(attempt_started.elapsed().as_micros() as f64);
                    }
                    report.bytes_read = report
                        .bytes_read
                        .saturating_add(bytes.len().try_into().unwrap_or(u64::MAX));
                    tokio::select! {
                        _ = cancellation.cancelled() => {
                            report.stopped = true;
                            return Ok(report);
                        }
                        pushed = writer.push(bytes) => pushed?,
                    }
                }
                Ok(Ok(None)) => {
                    if report.bytes_read == 0 && report.retries < config.max_retries {
                        report.retries += 1;
                        metrics::counter!("music_stream.source.live_http_retries").increment(1);
                        tokio::select! {
                            _ = cancellation.cancelled() => {
                                report.stopped = true;
                                return Ok(report);
                            }
                            _ = tokio::time::sleep(config.retry_backoff) => {}
                        }
                        break;
                    }
                    if report.bytes_read == 0 {
                        let error = MusicStreamError::InvalidSource(
                            "live HTTP response ended before media bytes".to_owned(),
                        );
                        writer.fail(error.to_string(), &cancellation).await;
                        return Err(error);
                    }
                    report.completed = true;
                    return Ok(report);
                }
                Ok(Err(_error))
                    if report.bytes_read == 0 && report.retries < config.max_retries =>
                {
                    report.retries += 1;
                    metrics::counter!("music_stream.source.live_http_retries").increment(1);
                    tokio::select! {
                        _ = cancellation.cancelled() => {
                            report.stopped = true;
                            return Ok(report);
                        }
                        _ = tokio::time::sleep(config.retry_backoff) => {}
                    }
                    break;
                }
                Ok(Err(error)) => {
                    let mapped = map_http_error(error);
                    writer.fail(mapped.to_string(), &cancellation).await;
                    return Err(mapped);
                }
                Err(_) if report.bytes_read == 0 && report.retries < config.max_retries => {
                    report.retries += 1;
                    metrics::counter!("music_stream.source.live_http_retries").increment(1);
                    tokio::select! {
                        _ = cancellation.cancelled() => {
                            report.stopped = true;
                            return Ok(report);
                        }
                        _ = tokio::time::sleep(config.retry_backoff) => {}
                    }
                    break;
                }
                Err(_) => {
                    let error = MusicStreamError::SourceTimeout(
                        "live HTTP body stalled past the idle deadline".to_owned(),
                    );
                    writer.fail(error.to_string(), &cancellation).await;
                    return Err(error);
                }
            }
        }
    }
}

#[derive(Debug)]
struct LiveOpenError {
    error: MusicStreamError,
    retryable: bool,
}

async fn open_live_response(
    client: &reqwest::Client,
    url: &str,
    headers: &std::collections::BTreeMap<String, String>,
    timeout: Duration,
) -> std::result::Result<reqwest::Response, LiveOpenError> {
    let mut request = client.get(url);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = tokio::time::timeout(timeout, request.send())
        .await
        .map_err(|_| LiveOpenError {
            error: MusicStreamError::SourceTimeout(
                "live HTTP response did not open before the deadline".to_owned(),
            ),
            retryable: true,
        })?
        .map_err(|error| LiveOpenError {
            retryable: is_retryable_http(&error),
            error: map_http_error(error),
        })?;
    response.error_for_status().map_err(|error| LiveOpenError {
        retryable: is_retryable_http(&error),
        error: map_http_error(error),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[derive(Debug, Default)]
    struct CountingWaitObserver {
        before: AtomicUsize,
        after: AtomicUsize,
    }

    impl BlockingReadObserver for CountingWaitObserver {
        fn before_wait(&self) {
            self.before.fetch_add(1, Ordering::Relaxed);
        }

        fn after_wait(&self) {
            self.after.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn byte_bridge_splits_oversized_chunks_and_preserves_bytes() {
        let (writer, mut reader) = StreamingByteReader::new(4).expect("bridge");
        let blocked = tokio::spawn(async move { writer.push(Bytes::from_static(b"abcdef")).await });
        tokio::task::spawn_blocking(move || {
            let mut output = [0_u8; 6];
            reader.read_exact(&mut output).expect("read");
            assert_eq!(&output, b"abcdef");
        })
        .await
        .expect("reader task");
        blocked.await.expect("writer task").expect("unblocked push");
    }

    #[tokio::test]
    async fn live_bridges_share_the_runtime_wide_byte_budget() {
        let global = LiveByteBudget::new(4).expect("global budget");
        let (first_writer, mut first_reader) =
            StreamingByteReader::with_global_budget(4, global.clone()).expect("first bridge");
        let (second_writer, _second_reader) =
            StreamingByteReader::with_global_budget(4, global).expect("second bridge");
        first_writer
            .push(Bytes::from_static(b"abcd"))
            .await
            .expect("fill global budget");
        let blocked =
            tokio::spawn(async move { second_writer.push(Bytes::from_static(b"e")).await });
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished());

        tokio::task::spawn_blocking(move || {
            let mut output = [0_u8; 4];
            first_reader.read_exact(&mut output).expect("release bytes");
            assert_eq!(&output, b"abcd");
        })
        .await
        .expect("reader task");

        tokio::time::timeout(Duration::from_secs(1), blocked)
            .await
            .expect("global budget did not release")
            .expect("writer task")
            .expect("second push");
    }

    #[tokio::test]
    async fn wait_observer_wraps_only_blocking_channel_receives() {
        let (writer, mut reader) = StreamingByteReader::new(4).expect("bridge");
        let observer = Arc::new(CountingWaitObserver::default());
        reader.set_wait_observer(observer.clone());
        writer.push(Bytes::from_static(b"ab")).await.expect("push");
        tokio::task::spawn_blocking(move || {
            let mut byte = [0_u8; 1];
            reader.read_exact(&mut byte).expect("first byte");
            assert_eq!(byte, [b'a']);
            assert_eq!(observer.before.load(Ordering::Relaxed), 1);
            assert_eq!(observer.after.load(Ordering::Relaxed), 1);
            reader.read_exact(&mut byte).expect("second byte");
            assert_eq!(byte, [b'b']);
            assert_eq!(observer.before.load(Ordering::Relaxed), 1);
            assert_eq!(observer.after.load(Ordering::Relaxed), 1);
        })
        .await
        .expect("reader");
    }

    #[tokio::test]
    async fn live_body_may_outlive_the_open_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = [0_u8; 1_024];
            let _ = stream.read(&mut request).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\na")
                .await
                .expect("first body byte");
            stream.flush().await.expect("flush");
            tokio::time::sleep(Duration::from_millis(60)).await;
            stream.write_all(b"b").await.expect("second body byte");
        });
        let (writer, mut reader) = StreamingByteReader::new(16).expect("bridge");
        let cancellation = CancellationToken::new();
        let reader_task = tokio::task::spawn_blocking(move || {
            let mut output = Vec::new();
            reader.read_to_end(&mut output).expect("read body");
            output
        });
        let report = run_http_live_stream(
            format!("http://{address}/live"),
            Default::default(),
            HttpLiveStreamConfig {
                open_timeout: Duration::from_millis(20),
                idle_timeout: Duration::from_secs(1),
                max_buffered_bytes: 16,
                max_retries: 0,
                retry_backoff: Duration::from_millis(1),
            },
            writer,
            cancellation,
        )
        .await
        .expect("live stream");

        assert!(report.completed);
        assert_eq!(report.bytes_read, 2);
        assert_eq!(reader_task.await.expect("reader"), b"ab");
        server.await.expect("server");
    }

    #[tokio::test]
    async fn live_body_idle_timeout_reports_source_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = [0_u8; 1_024];
            let _ = stream.read(&mut request).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n")
                .await
                .expect("headers");
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        let (writer, mut reader) = StreamingByteReader::new(16).expect("bridge");
        let cancellation = CancellationToken::new();
        let reader_task = tokio::task::spawn_blocking(move || {
            let mut output = Vec::new();
            reader.read_to_end(&mut output)
        });

        let error = run_http_live_stream(
            format!("http://{address}/live"),
            Default::default(),
            HttpLiveStreamConfig {
                open_timeout: Duration::from_secs(1),
                idle_timeout: Duration::from_millis(20),
                max_buffered_bytes: 16,
                max_retries: 0,
                retry_backoff: Duration::from_millis(1),
            },
            writer,
            cancellation,
        )
        .await
        .expect_err("idle body must time out");

        assert_eq!(error.code(), crate::error::ErrorCode::SourceTimeout);
        assert!(reader_task.await.expect("reader").is_err());
        server.await.expect("server");
    }

    #[tokio::test]
    async fn partial_live_body_failure_is_not_concatenated_with_a_retry() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let connections = Arc::new(AtomicUsize::new(0));
        let server_connections = Arc::clone(&connections);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            server_connections.fetch_add(1, Ordering::Relaxed);
            let mut request = [0_u8; 1_024];
            let _ = stream.read(&mut request).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nopus")
                .await
                .expect("partial body");
            drop(stream);
            if tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_ok()
            {
                server_connections.fetch_add(1, Ordering::Relaxed);
            }
        });
        let (writer, mut reader) = StreamingByteReader::new(16).expect("bridge");
        let cancellation = CancellationToken::new();
        let reader_task = tokio::task::spawn_blocking(move || {
            let mut output = Vec::new();
            let result = reader.read_to_end(&mut output);
            (output, result)
        });
        let result = run_http_live_stream(
            format!("http://{address}/live"),
            Default::default(),
            HttpLiveStreamConfig {
                open_timeout: Duration::from_secs(1),
                idle_timeout: Duration::from_secs(1),
                max_buffered_bytes: 16,
                max_retries: 2,
                retry_backoff: Duration::from_millis(1),
            },
            writer,
            cancellation,
        )
        .await;

        assert!(result.is_err());
        let (output, read_result) = reader_task.await.expect("reader");
        assert_eq!(output, b"opus");
        assert!(read_result.is_err());
        server.await.expect("server");
        assert_eq!(connections.load(Ordering::Relaxed), 1);
    }
}
