use std::collections::VecDeque;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::error::{MusicStreamError, Result};
use crate::model::TrackSource;

const LIVE_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const LIVE_HTTP_BUFFER_BYTES: usize = 512 * 1024;
const LIVE_HTTP_READ_CHUNK_BYTES: usize = 16 * 1024;
const LIVE_HTTP_MAX_RETRIES: u8 = 2;
const LIVE_HTTP_RETRY_BACKOFF: Duration = Duration::from_millis(250);
const LIVE_HTTP_RETRIES_METRIC: &str = "music_stream.source.live_http_retries";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpLiveStreamConfig {
    pub timeout: Duration,
    pub max_buffered_bytes: usize,
    pub read_chunk_bytes: usize,
    pub max_retries: u8,
    pub retry_backoff: Duration,
}

impl Default for HttpLiveStreamConfig {
    fn default() -> Self {
        Self {
            timeout: LIVE_HTTP_TIMEOUT,
            max_buffered_bytes: LIVE_HTTP_BUFFER_BYTES,
            read_chunk_bytes: LIVE_HTTP_READ_CHUNK_BYTES,
            max_retries: LIVE_HTTP_MAX_RETRIES,
            retry_backoff: LIVE_HTTP_RETRY_BACKOFF,
        }
    }
}

impl HttpLiveStreamConfig {
    pub fn validate(&self) -> Result<()> {
        if self.timeout.is_zero() {
            return Err(MusicStreamError::InvalidConfig(
                "live HTTP source timeout must be greater than zero".to_owned(),
            ));
        }
        if self.max_buffered_bytes == 0 {
            return Err(MusicStreamError::InvalidConfig(
                "live HTTP source max_buffered_bytes must be greater than zero".to_owned(),
            ));
        }
        if self.read_chunk_bytes == 0 {
            return Err(MusicStreamError::InvalidConfig(
                "live HTTP source read_chunk_bytes must be greater than zero".to_owned(),
            ));
        }
        if self.read_chunk_bytes > self.max_buffered_bytes {
            return Err(MusicStreamError::InvalidConfig(
                "live HTTP source read_chunk_bytes must not exceed max_buffered_bytes".to_owned(),
            ));
        }
        if self.max_retries > 0 && self.retry_backoff.is_zero() {
            return Err(MusicStreamError::InvalidConfig(
                "live HTTP source retry_backoff must be greater than zero when retries are enabled"
                    .to_owned(),
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
pub struct HttpLiveStream {
    reader: Option<StreamingByteReader>,
    writer: StreamingByteWriter,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<Result<HttpLiveStreamReport>>>,
}

#[derive(Clone, Debug)]
pub struct HttpLiveStreamStopHandle {
    writer: StreamingByteWriter,
    stop: Arc<AtomicBool>,
}

impl HttpLiveStream {
    #[must_use]
    pub fn reader(&self) -> Option<&StreamingByteReader> {
        self.reader.as_ref()
    }

    #[must_use]
    pub fn take_reader(&mut self) -> Option<StreamingByteReader> {
        self.reader.take()
    }

    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.join.as_ref().is_some_and(JoinHandle::is_finished)
    }

    #[must_use]
    pub fn stop_handle(&self) -> HttpLiveStreamStopHandle {
        HttpLiveStreamStopHandle {
            writer: self.writer.clone(),
            stop: Arc::clone(&self.stop),
        }
    }

    pub fn stop(&self) {
        self.stop_handle().stop();
    }

    pub fn join(mut self) -> Result<HttpLiveStreamReport> {
        let Some(join) = self.join.take() else {
            return Err(MusicStreamError::Internal(
                "live HTTP stream join handle already consumed".to_owned(),
            ));
        };
        join.join().map_err(|_| {
            MusicStreamError::Internal("live HTTP stream worker panicked".to_owned())
        })?
    }
}

impl HttpLiveStreamStopHandle {
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        let _ = self.writer.close();
    }
}

impl Drop for HttpLiveStream {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = self.writer.close();
    }
}

pub fn spawn_http_live_stream(
    source: &TrackSource,
    config: HttpLiveStreamConfig,
) -> Result<HttpLiveStream> {
    config.validate()?;
    if !source.is_live() {
        return Err(MusicStreamError::InvalidSource(
            "live HTTP stream requires a live track source".to_owned(),
        ));
    }
    let url = source.url.clone().ok_or_else(|| {
        MusicStreamError::InvalidSource("live track source requires url".to_owned())
    })?;
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(MusicStreamError::InvalidSource(
            "live track source requires http or https URL".to_owned(),
        ));
    }

    let (writer, reader) = StreamingByteReader::new(config.max_buffered_bytes)?;
    let worker_writer = writer.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let worker_stop = Arc::clone(&stop);
    let handle = thread::Builder::new()
        .name("music-stream-live-http".to_owned())
        .spawn(move || run_http_live_stream(url, config, worker_writer, worker_stop))
        .map_err(|error| MusicStreamError::Internal(error.to_string()))?;

    Ok(HttpLiveStream {
        reader: Some(reader),
        writer,
        stop,
        join: Some(handle),
    })
}

#[derive(Debug)]
pub struct StreamingByteReader {
    shared: Arc<StreamingByteShared>,
}

#[derive(Clone, Debug)]
pub struct StreamingByteWriter {
    shared: Arc<StreamingByteShared>,
}

#[derive(Debug)]
struct StreamingByteShared {
    state: Mutex<StreamingByteState>,
    readable: Condvar,
    writable: Condvar,
}

#[derive(Debug)]
struct StreamingByteState {
    chunks: VecDeque<Bytes>,
    front_offset: usize,
    max_buffered_bytes: usize,
    buffered_bytes: usize,
    finished: bool,
    closed: bool,
    failure: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamingByteSnapshot {
    pub max_buffered_bytes: usize,
    pub buffered_bytes: usize,
    pub chunks: usize,
    pub finished: bool,
    pub closed: bool,
    pub failed: bool,
}

impl StreamingByteReader {
    pub fn new(max_buffered_bytes: usize) -> Result<(StreamingByteWriter, Self)> {
        if max_buffered_bytes == 0 {
            return Err(MusicStreamError::InvalidConfig(
                "streaming byte source max_buffered_bytes must be greater than zero".to_owned(),
            ));
        }

        let shared = Arc::new(StreamingByteShared {
            state: Mutex::new(StreamingByteState {
                chunks: VecDeque::new(),
                front_offset: 0,
                max_buffered_bytes,
                buffered_bytes: 0,
                finished: false,
                closed: false,
                failure: None,
            }),
            readable: Condvar::new(),
            writable: Condvar::new(),
        });

        Ok((
            StreamingByteWriter {
                shared: Arc::clone(&shared),
            },
            Self { shared },
        ))
    }

    #[must_use]
    pub fn snapshot(&self) -> StreamingByteSnapshot {
        self.shared
            .state
            .lock()
            .map(|state| state.snapshot())
            .unwrap_or_default()
    }

    pub fn close(&self) -> Result<()> {
        let mut state = self.lock_state()?;
        state.closed = true;
        drop(state);
        self.shared.readable.notify_all();
        self.shared.writable.notify_all();
        Ok(())
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, StreamingByteState>> {
        self.shared.state.lock().map_err(|_| {
            MusicStreamError::Internal("streaming byte source lock poisoned".to_owned())
        })
    }
}

impl StreamingByteWriter {
    #[must_use]
    pub fn snapshot(&self) -> StreamingByteSnapshot {
        self.shared
            .state
            .lock()
            .map(|state| state.snapshot())
            .unwrap_or_default()
    }

    pub fn try_push(&self, chunk: impl Into<Bytes>) -> Result<()> {
        let chunk = chunk.into();
        validate_streaming_byte_chunk(&chunk)?;
        let mut state = self.lock_state()?;
        state.ensure_open_for_write()?;
        state.ensure_chunk_fits(chunk.len())?;
        if !state.can_accept(chunk.len()) {
            return Err(MusicStreamError::Busy(
                "streaming byte source buffer high watermark reached".to_owned(),
            ));
        }

        state.push(chunk);
        drop(state);
        self.shared.readable.notify_all();
        Ok(())
    }

    pub fn push_blocking(&self, chunk: impl Into<Bytes>) -> Result<()> {
        let chunk = chunk.into();
        validate_streaming_byte_chunk(&chunk)?;
        let len = chunk.len();
        let mut state = self.lock_state()?;
        state.ensure_chunk_fits(len)?;
        while !state.can_accept(len) {
            state.ensure_open_for_write()?;
            state = self.shared.writable.wait(state).map_err(|_| {
                MusicStreamError::Internal("streaming byte source lock poisoned".to_owned())
            })?;
        }
        state.ensure_open_for_write()?;
        state.push(chunk);
        drop(state);
        self.shared.readable.notify_all();
        Ok(())
    }

    pub fn finish(&self) -> Result<()> {
        let mut state = self.lock_state()?;
        state.finished = true;
        drop(state);
        self.shared.readable.notify_all();
        self.shared.writable.notify_all();
        Ok(())
    }

    pub fn fail(&self, message: impl Into<String>) -> Result<()> {
        let mut state = self.lock_state()?;
        state.failure = Some(message.into());
        drop(state);
        self.shared.readable.notify_all();
        self.shared.writable.notify_all();
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        let mut state = self.lock_state()?;
        state.closed = true;
        drop(state);
        self.shared.readable.notify_all();
        self.shared.writable.notify_all();
        Ok(())
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, StreamingByteState>> {
        self.shared.state.lock().map_err(|_| {
            MusicStreamError::Internal("streaming byte source lock poisoned".to_owned())
        })
    }
}

impl Read for StreamingByteReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }

        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| std::io::Error::other("streaming byte source lock poisoned"))?;
        loop {
            if let Some(copied) = state.read_available(output) {
                self.shared.writable.notify_all();
                return Ok(copied);
            }
            if let Some(message) = state.failure.as_ref() {
                return Err(std::io::Error::other(message.clone()));
            }
            if state.finished || state.closed {
                return Ok(0);
            }

            state = self
                .shared
                .readable
                .wait(state)
                .map_err(|_| std::io::Error::other("streaming byte source lock poisoned"))?;
        }
    }
}

impl StreamingByteState {
    fn snapshot(&self) -> StreamingByteSnapshot {
        StreamingByteSnapshot {
            max_buffered_bytes: self.max_buffered_bytes,
            buffered_bytes: self.buffered_bytes,
            chunks: self.chunks.len(),
            finished: self.finished,
            closed: self.closed,
            failed: self.failure.is_some(),
        }
    }

    fn can_accept(&self, len: usize) -> bool {
        self.buffered_bytes.saturating_add(len) <= self.max_buffered_bytes
    }

    fn ensure_chunk_fits(&self, len: usize) -> Result<()> {
        if len > self.max_buffered_bytes {
            return Err(MusicStreamError::InvalidSource(format!(
                "streaming byte chunk exceeds buffer capacity of {} bytes",
                self.max_buffered_bytes
            )));
        }
        Ok(())
    }

    fn ensure_open_for_write(&self) -> Result<()> {
        if self.closed || self.finished || self.failure.is_some() {
            return Err(MusicStreamError::StreamClosed(
                "streaming byte source is closed".to_owned(),
            ));
        }
        Ok(())
    }

    fn push(&mut self, chunk: Bytes) {
        self.buffered_bytes = self.buffered_bytes.saturating_add(chunk.len());
        self.chunks.push_back(chunk);
    }

    fn read_available(&mut self, output: &mut [u8]) -> Option<usize> {
        let front = self.chunks.front()?;
        let available = front.len().saturating_sub(self.front_offset);
        let copy_len = available.min(output.len());
        output[..copy_len].copy_from_slice(&front[self.front_offset..self.front_offset + copy_len]);
        self.front_offset += copy_len;
        self.buffered_bytes = self.buffered_bytes.saturating_sub(copy_len);
        if self.front_offset == front.len() {
            self.chunks.pop_front();
            self.front_offset = 0;
        }
        Some(copy_len)
    }
}

fn validate_streaming_byte_chunk(chunk: &Bytes) -> Result<()> {
    if chunk.is_empty() {
        return Err(MusicStreamError::InvalidSource(
            "streaming byte chunk must not be empty".to_owned(),
        ));
    }
    Ok(())
}

fn run_http_live_stream(
    url: String,
    config: HttpLiveStreamConfig,
    writer: StreamingByteWriter,
    stop: Arc<AtomicBool>,
) -> Result<HttpLiveStreamReport> {
    let client = reqwest::blocking::Client::builder()
        .timeout(config.timeout)
        .build()
        .map_err(|error| MusicStreamError::InvalidSource(error.to_string()))?;

    let mut bytes_read = 0_u64;
    let mut retries = 0_u8;
    let mut buffer = vec![0_u8; config.read_chunk_bytes];
    loop {
        if stop.load(Ordering::Acquire) {
            let _ = writer.close();
            return Ok(HttpLiveStreamReport {
                bytes_read,
                retries,
                completed: false,
                stopped: true,
            });
        }

        let mut response = match client
            .get(&url)
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
        {
            Ok(response) => response,
            Err(error) => {
                let retryable = is_retryable_live_http_request_error(&error);
                let mapped = map_live_http_error(error);
                if retryable && retries < config.max_retries {
                    if wait_live_retry(&stop, config.retry_backoff) {
                        retries += 1;
                        metrics::counter!(LIVE_HTTP_RETRIES_METRIC).increment(1);
                        continue;
                    }
                    let _ = writer.close();
                    return Ok(HttpLiveStreamReport {
                        bytes_read,
                        retries,
                        completed: false,
                        stopped: true,
                    });
                }
                let _ = writer.fail(mapped.to_string());
                return Err(mapped);
            }
        };

        loop {
            if stop.load(Ordering::Acquire) {
                let _ = writer.close();
                return Ok(HttpLiveStreamReport {
                    bytes_read,
                    retries,
                    completed: false,
                    stopped: true,
                });
            }

            let read = match response.read(&mut buffer) {
                Ok(read) => read,
                Err(error) => {
                    let mapped = MusicStreamError::InvalidSource(error.to_string());
                    if retries < config.max_retries {
                        if wait_live_retry(&stop, config.retry_backoff) {
                            retries += 1;
                            metrics::counter!(LIVE_HTTP_RETRIES_METRIC).increment(1);
                            break;
                        }
                        let _ = writer.close();
                        return Ok(HttpLiveStreamReport {
                            bytes_read,
                            retries,
                            completed: false,
                            stopped: true,
                        });
                    }
                    let _ = writer.fail(mapped.to_string());
                    return Err(mapped);
                }
            };
            if read == 0 {
                writer.finish()?;
                return Ok(HttpLiveStreamReport {
                    bytes_read,
                    retries,
                    completed: true,
                    stopped: false,
                });
            }

            match writer.push_blocking(Bytes::copy_from_slice(&buffer[..read])) {
                Ok(()) => {
                    bytes_read = bytes_read.saturating_add(read.try_into().unwrap_or(u64::MAX));
                }
                Err(error)
                    if stop.load(Ordering::Acquire)
                        || matches!(error, MusicStreamError::StreamClosed(_)) =>
                {
                    return Ok(HttpLiveStreamReport {
                        bytes_read,
                        retries,
                        completed: false,
                        stopped: true,
                    });
                }
                Err(error) => {
                    let _ = writer.fail(error.to_string());
                    return Err(error);
                }
            }
        }
    }
}

fn map_live_http_error(error: reqwest::Error) -> MusicStreamError {
    if error.is_timeout() {
        return MusicStreamError::SourceTimeout(error.to_string());
    }
    if error
        .status()
        .is_some_and(|status| matches!(status.as_u16(), 401 | 403))
    {
        return MusicStreamError::SourceAuthExpired(error.to_string());
    }
    MusicStreamError::InvalidSource(error.to_string())
}

fn is_retryable_live_http_request_error(error: &reqwest::Error) -> bool {
    if error.is_timeout() {
        return true;
    }

    match error.status().map(|status| status.as_u16()) {
        Some(401 | 403) => false,
        Some(408 | 429) => true,
        Some(500..=599) => true,
        Some(400..=499) => false,
        Some(_) => false,
        None => true,
    }
}

fn wait_live_retry(stop: &AtomicBool, backoff: Duration) -> bool {
    if backoff.is_zero() {
        return !stop.load(Ordering::Acquire);
    }

    let deadline = Instant::now() + backoff;
    while Instant::now() < deadline {
        if stop.load(Ordering::Acquire) {
            return false;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        thread::sleep(remaining.min(Duration::from_millis(10)));
    }
    !stop.load(Ordering::Acquire)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{TrackKind, TrackSource};
    use bytes::Bytes;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;

    fn live_track(url: impl Into<String>) -> TrackSource {
        TrackSource {
            id: "live-a".to_owned(),
            kind: TrackKind::Live,
            url: Some(url.into()),
            path: None,
            seekable: Some(false),
        }
    }

    fn serve_live_http_chunks(chunks: Vec<Vec<u8>>) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind live HTTP test server");
        let addr = listener
            .local_addr()
            .expect("live HTTP test server address");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept live HTTP request");
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request);
            let headers = "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n";
            stream
                .write_all(headers.as_bytes())
                .expect("write live headers");
            for chunk in chunks {
                stream.write_all(&chunk).expect("write live chunk");
                stream.flush().expect("flush live chunk");
            }
        });
        (format!("http://{addr}/live"), handle)
    }

    fn serve_live_http_status(status: u16) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind live HTTP test server");
        let addr = listener
            .local_addr()
            .expect("live HTTP test server address");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept live HTTP request");
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request);
            let response = format!("HTTP/1.1 {status} test\r\nContent-Length: 0\r\n\r\n");
            stream
                .write_all(response.as_bytes())
                .expect("write live error");
            stream.flush().expect("flush live error");
        });
        (format!("http://{addr}/live"), handle)
    }

    fn serve_live_http_status_then_chunks(
        status: u16,
        chunks: Vec<Vec<u8>>,
    ) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind live HTTP test server");
        let addr = listener
            .local_addr()
            .expect("live HTTP test server address");
        let handle = thread::spawn(move || {
            let (mut first, _) = listener.accept().expect("accept first live HTTP request");
            let mut request = [0_u8; 1024];
            let _ = first.read(&mut request);
            let response = format!("HTTP/1.1 {status} test\r\nContent-Length: 0\r\n\r\n");
            first
                .write_all(response.as_bytes())
                .expect("write retryable live error");
            first.flush().expect("flush retryable live error");
            drop(first);

            let (mut second, _) = listener.accept().expect("accept retry live HTTP request");
            let _ = second.set_read_timeout(Some(Duration::from_secs(2)));
            let mut request = [0_u8; 1024];
            let _ = second.read(&mut request);
            second
                .write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n")
                .expect("write retry live headers");
            for chunk in chunks {
                second.write_all(&chunk).expect("write retry live chunk");
                second.flush().expect("flush retry live chunk");
            }
        });
        (format!("http://{addr}/live"), handle)
    }

    #[test]
    fn streaming_byte_source_try_push_enforces_byte_budget() {
        let (writer, mut reader) = StreamingByteReader::new(4).expect("streaming bytes");
        writer
            .try_push(Bytes::from_static(b"abcd"))
            .expect("push fills buffer");

        let error = writer
            .try_push(Bytes::from_static(b"e"))
            .expect_err("buffer full");
        assert_eq!(error.code(), crate::error::ErrorCode::Busy);
        assert_eq!(writer.snapshot().buffered_bytes, 4);

        let mut output = [0_u8; 2];
        reader.read_exact(&mut output).expect("read partial");
        assert_eq!(&output, b"ab");
        assert_eq!(writer.snapshot().buffered_bytes, 2);

        writer
            .try_push(Bytes::from_static(b"ef"))
            .expect("push after drain");
        assert_eq!(writer.snapshot().buffered_bytes, 4);
    }

    #[test]
    fn streaming_byte_reader_blocks_until_writer_pushes_and_finishes() {
        let (writer, mut reader) = StreamingByteReader::new(16).expect("streaming bytes");
        let (ready_tx, ready_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            ready_tx.send(()).expect("ready");
            let mut output = [0_u8; 5];
            reader.read_exact(&mut output).expect("read pushed bytes");
            assert_eq!(&output, b"hello");
            let mut eof = [0_u8; 1];
            assert_eq!(reader.read(&mut eof).expect("eof"), 0);
        });

        ready_rx.recv().expect("reader waiting");
        thread::sleep(Duration::from_millis(20));
        writer
            .try_push(Bytes::from_static(b"hello"))
            .expect("push bytes");
        writer.finish().expect("finish");
        handle.join().expect("reader thread");
    }

    #[test]
    fn streaming_byte_blocking_push_waits_for_reader_capacity() {
        let (writer, mut reader) = StreamingByteReader::new(4).expect("streaming bytes");
        writer
            .try_push(Bytes::from_static(b"abcd"))
            .expect("fill buffer");
        let blocking_writer = writer.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            started_tx.send(()).expect("started");
            blocking_writer
                .push_blocking(Bytes::from_static(b"ef"))
                .expect("blocking push");
        });

        started_rx.recv().expect("writer started");
        thread::sleep(Duration::from_millis(20));
        assert_eq!(writer.snapshot().buffered_bytes, 4);

        let mut output = [0_u8; 2];
        reader.read_exact(&mut output).expect("drain capacity");
        assert_eq!(&output, b"ab");
        handle.join().expect("blocking writer");
        assert_eq!(writer.snapshot().buffered_bytes, 4);

        let mut rest = [0_u8; 4];
        reader.read_exact(&mut rest).expect("read rest");
        assert_eq!(&rest, b"cdef");
    }

    #[test]
    fn streaming_byte_reader_reports_writer_failure() {
        let (writer, mut reader) = StreamingByteReader::new(16).expect("streaming bytes");
        writer.fail("upstream disconnected").expect("fail");

        let mut output = [0_u8; 1];
        let error = reader.read(&mut output).expect_err("failure");
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert!(error.to_string().contains("upstream disconnected"));
        assert!(writer.snapshot().failed);
    }

    #[test]
    fn http_live_stream_reads_chunks_into_bounded_reader() {
        let (url, server) = serve_live_http_chunks(vec![b"hel".to_vec(), b"lo".to_vec()]);
        let mut live = spawn_http_live_stream(
            &live_track(url),
            HttpLiveStreamConfig {
                timeout: Duration::from_secs(2),
                max_buffered_bytes: 16,
                read_chunk_bytes: 4,
                max_retries: 0,
                retry_backoff: Duration::from_millis(1),
            },
        )
        .expect("spawn live stream");
        let mut reader = live.take_reader().expect("reader");
        let mut output = Vec::new();
        reader.read_to_end(&mut output).expect("read live stream");
        let report = live.join().expect("join live stream");
        server.join().expect("live server");

        assert_eq!(output, b"hello");
        assert_eq!(report.bytes_read, 5);
        assert!(report.completed);
        assert!(!report.stopped);
    }

    #[test]
    fn http_live_stream_retries_retryable_status_before_failing_reader() {
        let (url, server) =
            serve_live_http_status_then_chunks(503, vec![b"re".to_vec(), b"try".to_vec()]);
        let mut live = spawn_http_live_stream(
            &live_track(url),
            HttpLiveStreamConfig {
                timeout: Duration::from_secs(2),
                max_buffered_bytes: 16,
                read_chunk_bytes: 4,
                max_retries: 1,
                retry_backoff: Duration::from_millis(1),
            },
        )
        .expect("spawn live stream");
        let mut reader = live.take_reader().expect("reader");
        let mut output = Vec::new();
        reader
            .read_to_end(&mut output)
            .expect("read retried live stream");
        let report = live.join().expect("join retried live stream");
        server.join().expect("live retry server");

        assert_eq!(output, b"retry");
        assert_eq!(report.bytes_read, 5);
        assert_eq!(report.retries, 1);
        assert!(report.completed);
        assert!(!report.stopped);
    }

    #[test]
    fn http_live_stream_stop_unblocks_backpressured_writer() {
        let (url, server) = serve_live_http_chunks(vec![vec![b'x'; 64 * 1024]]);
        let live = spawn_http_live_stream(
            &live_track(url),
            HttpLiveStreamConfig {
                timeout: Duration::from_secs(2),
                max_buffered_bytes: 1024,
                read_chunk_bytes: 1024,
                max_retries: 0,
                retry_backoff: Duration::from_millis(1),
            },
        )
        .expect("spawn live stream");

        wait_for_condition(Duration::from_secs(2), || {
            live.reader()
                .is_some_and(|reader| reader.snapshot().buffered_bytes == 1024)
        });
        live.stop();
        let report = live.join().expect("join stopped live stream");
        server.join().expect("live server");

        assert!(report.stopped);
        assert!(!report.completed);
    }

    #[test]
    fn http_live_stream_maps_auth_expiry() {
        let (url, server) = serve_live_http_status(403);
        let live = spawn_http_live_stream(&live_track(url), HttpLiveStreamConfig::default())
            .expect("spawn live stream");
        let error = live.join().expect_err("auth failure");
        server.join().expect("live server");

        assert_eq!(error.code(), crate::error::ErrorCode::SourceAuthExpired);
    }

    fn wait_for_condition(timeout: Duration, condition: impl Fn() -> bool) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if condition() {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
        assert!(condition(), "condition was not met before timeout");
    }
}
