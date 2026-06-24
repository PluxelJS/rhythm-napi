//! Source resolution and byte access for files, HTTP objects, and live streams.
//!
//! Source artifacts are separate from track slots so preloaded data can be
//! reused or discarded without leaking playlist semantics into Rust.

use std::fmt;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::error::{MusicStreamError, Result};
use crate::model::{TrackKind, TrackSource};
use lru::LruCache;
use reqwest::StatusCode;
use reqwest::header::{CONTENT_RANGE, RANGE};

mod live;
pub use live::{
    HttpLiveStream, HttpLiveStreamConfig, HttpLiveStreamReport, HttpLiveStreamStopHandle,
    StreamingByteReader, StreamingByteSnapshot, StreamingByteWriter, spawn_http_live_stream,
};

const HTTP_SOURCE_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_SOURCE_MAX_BYTES: u64 = 256 * 1024 * 1024;
const HTTP_SOURCE_MAX_RESUME_ATTEMPTS: u8 = 2;
const DEFAULT_ARTIFACT_CACHE_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_ARTIFACT_CACHE_ITEM_BYTES: u64 = HTTP_SOURCE_MAX_BYTES;
const SOURCE_CACHE_HIT_METRIC: &str = "music_stream.source.cache_hit";
const SOURCE_CACHE_MISS_METRIC: &str = "music_stream.source.cache_miss";
const SOURCE_CACHE_INSERTED_METRIC: &str = "music_stream.source.cache_inserted";
const SOURCE_CACHE_INSERT_SKIPPED_METRIC: &str = "music_stream.source.cache_insert_skipped";
const SOURCE_RESOLVE_ERRORS_METRIC: &str = "music_stream.source.resolve_errors";
const SOURCE_RESOLVE_US_METRIC: &str = "music_stream.source.resolve_us";
const SOURCE_HTTP_BYTES_METRIC: &str = "music_stream.source.http_bytes";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpSourceConfig {
    pub timeout: Duration,
    pub max_bytes: u64,
    pub cache_temp_files: bool,
}

impl Default for HttpSourceConfig {
    fn default() -> Self {
        Self {
            timeout: HTTP_SOURCE_TIMEOUT,
            max_bytes: HTTP_SOURCE_MAX_BYTES,
            cache_temp_files: false,
        }
    }
}

impl HttpSourceConfig {
    pub fn validate(&self) -> Result<()> {
        if self.timeout.is_zero() {
            return Err(MusicStreamError::InvalidConfig(
                "HTTP source timeout must be greater than zero".to_owned(),
            ));
        }
        if self.max_bytes == 0 {
            return Err(MusicStreamError::InvalidConfig(
                "HTTP source max_bytes must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SourceResolverConfig {
    pub http: HttpSourceConfig,
    pub live_http: HttpLiveStreamConfig,
}

impl SourceResolverConfig {
    pub fn validate(&self) -> Result<()> {
        self.http.validate()?;
        self.live_http.validate()
    }
}

#[derive(Debug)]
pub struct SourceArtifactCache {
    max_bytes: u64,
    max_item_bytes: u64,
    total_bytes: u64,
    entries: LruCache<String, SourceArtifact>,
}

impl Default for SourceArtifactCache {
    fn default() -> Self {
        Self::new(
            DEFAULT_ARTIFACT_CACHE_BYTES,
            DEFAULT_ARTIFACT_CACHE_ITEM_BYTES,
        )
    }
}

impl SourceArtifactCache {
    #[must_use]
    pub fn new(max_bytes: u64, max_item_bytes: u64) -> Self {
        Self {
            max_bytes,
            max_item_bytes,
            total_bytes: 0,
            entries: LruCache::unbounded(),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn get(&mut self, stable_key: &str) -> Option<SourceArtifact> {
        if !self.entries.peek(stable_key)?.path().exists() {
            self.remove(stable_key);
            return None;
        }
        self.entries.get(stable_key).cloned()
    }

    pub fn insert(&mut self, artifact: SourceArtifact) -> bool {
        if !artifact.cacheable
            || artifact.len_bytes > self.max_item_bytes
            || artifact.len_bytes > self.max_bytes
        {
            return false;
        }

        let key = artifact.stable_key.clone();
        let len_bytes = artifact.len_bytes;
        if let Some(replaced) = self.entries.put(key, artifact) {
            self.total_bytes = self.total_bytes.saturating_sub(replaced.len_bytes);
        }
        self.total_bytes = self.total_bytes.saturating_add(len_bytes);
        self.evict_to_budget();
        true
    }

    pub fn remove(&mut self, stable_key: &str) -> Option<SourceArtifact> {
        let removed = self.entries.pop(stable_key)?;
        self.total_bytes = self.total_bytes.saturating_sub(removed.len_bytes);
        Some(removed)
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.total_bytes = 0;
    }

    fn evict_to_budget(&mut self) {
        while self.total_bytes > self.max_bytes {
            let Some((_, removed)) = self.entries.pop_lru() else {
                self.total_bytes = 0;
                return;
            };
            self.total_bytes = self.total_bytes.saturating_sub(removed.len_bytes);
        }
    }
}

pub type SharedSourceArtifactCache = Arc<Mutex<SourceArtifactCache>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceArtifactKind {
    LocalFile,
    HttpTempFile,
}

#[derive(Clone, Debug)]
pub struct SourceArtifact {
    pub track_id: String,
    pub stable_key: String,
    pub kind: SourceArtifactKind,
    pub path: PathBuf,
    pub len_bytes: u64,
    pub seekable: bool,
    pub cacheable: bool,
    cleanup: Option<Arc<tempfile::TempPath>>,
}

impl SourceArtifact {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len_bytes == 0
    }

    #[must_use]
    pub fn is_temporary(&self) -> bool {
        self.cleanup.is_some()
    }
}

impl PartialEq for SourceArtifact {
    fn eq(&self, other: &Self) -> bool {
        self.track_id == other.track_id
            && self.stable_key == other.stable_key
            && self.kind == other.kind
            && self.path == other.path
            && self.len_bytes == other.len_bytes
            && self.seekable == other.seekable
            && self.cacheable == other.cacheable
            && self.is_temporary() == other.is_temporary()
    }
}

impl Eq for SourceArtifact {}

pub trait SourceResolver {
    fn resolve(&self, source: &TrackSource) -> Result<SourceArtifact>;
}

#[derive(Clone, Default)]
pub struct FileSourceResolver {
    config: SourceResolverConfig,
    cache: Option<SharedSourceArtifactCache>,
    http_client: Arc<OnceLock<reqwest::blocking::Client>>,
}

impl fmt::Debug for FileSourceResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileSourceResolver")
            .field("config", &self.config)
            .field("cache", &self.cache.as_ref().map(|_| "<cache>"))
            .finish()
    }
}

impl FileSourceResolver {
    #[must_use]
    pub fn new(config: SourceResolverConfig) -> Self {
        Self {
            config,
            cache: None,
            http_client: Arc::new(OnceLock::new()),
        }
    }

    #[must_use]
    pub fn with_cache(mut config: SourceResolverConfig, cache: SharedSourceArtifactCache) -> Self {
        config.http.cache_temp_files = true;
        Self {
            config,
            cache: Some(cache),
            http_client: Arc::new(OnceLock::new()),
        }
    }

    pub fn validate(&self) -> Result<()> {
        self.config.validate()
    }

    #[must_use]
    pub fn config(&self) -> &SourceResolverConfig {
        &self.config
    }

    fn http_client(&self) -> Result<reqwest::blocking::Client> {
        if let Some(client) = self.http_client.get() {
            return Ok(client.clone());
        }

        let client = build_http_client(&self.config.http)?;
        if self.http_client.set(client.clone()).is_ok() {
            return Ok(client);
        }
        self.http_client.get().cloned().ok_or_else(|| {
            MusicStreamError::Internal("HTTP source client initialization failed".to_owned())
        })
    }

    fn resolve_url(&self, source: &TrackSource) -> Result<SourceArtifact> {
        let stable_key = source.stable_key();
        let started = Instant::now();
        if let Some(cache) = &self.cache {
            let cached = cache
                .lock()
                .map_err(|_| MusicStreamError::Internal("source cache lock poisoned".to_owned()))
                .map_err(|error| record_source_resolve_error(started, error))?
                .get(stable_key);
            if let Some(artifact) = cached {
                metrics::counter!(SOURCE_CACHE_HIT_METRIC).increment(1);
                record_source_resolve_duration(started);
                return Ok(artifact);
            }
        }
        if self.cache.is_some() {
            metrics::counter!(SOURCE_CACHE_MISS_METRIC).increment(1);
        }

        let client = self
            .http_client()
            .map_err(|error| record_source_resolve_error(started, error))?;
        let artifact = resolve_http_temp_file_with_client(source, &self.config.http, &client)
            .map_err(|error| record_source_resolve_error(started, error))?;
        metrics::counter!(SOURCE_HTTP_BYTES_METRIC).increment(artifact.len_bytes);
        if artifact.cacheable
            && let Some(cache) = &self.cache
        {
            if cache
                .lock()
                .map_err(|_| MusicStreamError::Internal("source cache lock poisoned".to_owned()))
                .map_err(|error| record_source_resolve_error(started, error))?
                .insert(artifact.clone())
            {
                metrics::counter!(SOURCE_CACHE_INSERTED_METRIC).increment(1);
            } else {
                metrics::counter!(SOURCE_CACHE_INSERT_SKIPPED_METRIC).increment(1);
            }
        }
        record_source_resolve_duration(started);
        Ok(artifact)
    }
}

impl SourceResolver for FileSourceResolver {
    fn resolve(&self, source: &TrackSource) -> Result<SourceArtifact> {
        let _span = tracing::debug_span!(
            "music_stream.source.resolve",
            track_id = %source.id,
            kind = ?source.kind,
            seekable = ?source.seekable,
        )
        .entered();
        match source.kind {
            TrackKind::File => resolve_local_file(source),
            TrackKind::Url => self.resolve_url(source),
            TrackKind::Live => Err(MusicStreamError::Unsupported(
                "live sources bypass the artifact resolver".to_owned(),
            )),
        }
    }
}

pub fn resolve_local_file(source: &TrackSource) -> Result<SourceArtifact> {
    let path = source.path.as_ref().ok_or_else(|| {
        MusicStreamError::InvalidSource("file track source requires path".to_owned())
    })?;
    let path = PathBuf::from(path);
    let metadata = std::fs::metadata(&path)
        .map_err(|error| MusicStreamError::InvalidSource(error.to_string()))?;

    if !metadata.is_file() {
        return Err(MusicStreamError::InvalidSource(
            "file track source path must point to a regular file".to_owned(),
        ));
    }
    if metadata.len() == 0 {
        return Err(MusicStreamError::InvalidSource(
            "file track source path must not be empty".to_owned(),
        ));
    }

    Ok(SourceArtifact {
        track_id: source.id.clone(),
        stable_key: source.stable_key().to_owned(),
        kind: SourceArtifactKind::LocalFile,
        path,
        len_bytes: metadata.len(),
        seekable: source.is_seekable(),
        cacheable: false,
        cleanup: None,
    })
}

pub fn resolve_http_temp_file(source: &TrackSource) -> Result<SourceArtifact> {
    resolve_http_temp_file_with_config(source, &HttpSourceConfig::default())
}

pub fn resolve_http_temp_file_with_config(
    source: &TrackSource,
    config: &HttpSourceConfig,
) -> Result<SourceArtifact> {
    let client = build_http_client(config)?;
    resolve_http_temp_file_with_client(source, config, &client)
}

fn resolve_http_temp_file_with_client(
    source: &TrackSource,
    config: &HttpSourceConfig,
    client: &reqwest::blocking::Client,
) -> Result<SourceArtifact> {
    config.validate()?;
    let url = source.url.as_ref().ok_or_else(|| {
        MusicStreamError::InvalidSource("url track source requires url".to_owned())
    })?;
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(MusicStreamError::InvalidSource(
            "url track source requires http or https URL".to_owned(),
        ));
    }

    let suffix = http_temp_suffix(url);
    let mut temp = tempfile::Builder::new()
        .prefix("music-stream-http-")
        .suffix(&suffix)
        .tempfile()
        .map_err(|error| MusicStreamError::InvalidSource(error.to_string()))?;
    let len_bytes = download_http_temp_file(client, url, &mut temp, config)?;

    if len_bytes == 0 {
        return Err(MusicStreamError::InvalidSource(
            "HTTP source must not be empty".to_owned(),
        ));
    }
    if len_bytes > config.max_bytes {
        return Err(MusicStreamError::InvalidSource(format!(
            "HTTP source exceeds max size of {} bytes",
            config.max_bytes
        )));
    }

    temp.flush()
        .map_err(|error| MusicStreamError::InvalidSource(error.to_string()))?;
    let temp_path = temp.into_temp_path();
    let path = temp_path.to_path_buf();

    Ok(SourceArtifact {
        track_id: source.id.clone(),
        stable_key: source.stable_key().to_owned(),
        kind: SourceArtifactKind::HttpTempFile,
        path,
        len_bytes,
        seekable: source.is_seekable(),
        cacheable: config.cache_temp_files && source.is_seekable(),
        cleanup: Some(Arc::new(temp_path)),
    })
}

fn build_http_client(config: &HttpSourceConfig) -> Result<reqwest::blocking::Client> {
    config.validate()?;
    reqwest::blocking::Client::builder()
        .timeout(config.timeout)
        .build()
        .map_err(|error| MusicStreamError::InvalidSource(error.to_string()))
}

fn download_http_temp_file(
    client: &reqwest::blocking::Client,
    url: &str,
    temp: &mut tempfile::NamedTempFile,
    config: &HttpSourceConfig,
) -> Result<u64> {
    let mut downloaded = 0_u64;
    let mut resume_attempts = 0_u8;
    let mut expected_total = None;

    loop {
        let mut request = client.get(url);
        if downloaded > 0 {
            request = request.header(RANGE, format!("bytes={downloaded}-"));
        }

        let mut response = request
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(map_http_source_error)?;
        let status = response.status();
        if downloaded > 0 && status != StatusCode::PARTIAL_CONTENT {
            return Err(MusicStreamError::InvalidSource(
                "HTTP source interrupted and server did not honor range resume".to_owned(),
            ));
        }
        if downloaded > 0 {
            validate_http_content_range_start(&response, downloaded)?;
        }

        validate_http_response_size(downloaded, response.content_length(), config.max_bytes)?;
        if let Some(content_length) = response.content_length() {
            expected_total = Some(downloaded.saturating_add(content_length));
        }

        let copy_limit = config
            .max_bytes
            .saturating_sub(downloaded)
            .saturating_add(1);
        match std::io::copy(&mut response.by_ref().take(copy_limit), temp) {
            Ok(written) => {
                downloaded = downloaded.saturating_add(written);
                break;
            }
            Err(error) => {
                downloaded = temp
                    .as_file()
                    .metadata()
                    .map_err(|metadata_error| {
                        MusicStreamError::InvalidSource(metadata_error.to_string())
                    })?
                    .len();
                if expected_total.is_some_and(|total| downloaded == total) {
                    break;
                }
                if downloaded == 0
                    || downloaded > config.max_bytes
                    || resume_attempts >= HTTP_SOURCE_MAX_RESUME_ATTEMPTS
                {
                    return Err(MusicStreamError::InvalidSource(error.to_string()));
                }

                resume_attempts = resume_attempts.saturating_add(1);
                temp.seek(SeekFrom::End(0)).map_err(|seek_error| {
                    MusicStreamError::InvalidSource(seek_error.to_string())
                })?;
            }
        }
    }

    Ok(downloaded)
}

fn validate_http_response_size(
    downloaded: u64,
    content_length: Option<u64>,
    max_bytes: u64,
) -> Result<()> {
    if let Some(content_length) = content_length
        && downloaded.saturating_add(content_length) > max_bytes
    {
        return Err(MusicStreamError::InvalidSource(format!(
            "HTTP source exceeds max size of {} bytes",
            max_bytes
        )));
    }
    Ok(())
}

fn validate_http_content_range_start(
    response: &reqwest::blocking::Response,
    expected_start: u64,
) -> Result<()> {
    let range_start = response
        .headers()
        .get(CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_http_content_range_start)
        .ok_or_else(|| {
            MusicStreamError::InvalidSource(
                "HTTP range resume response is missing a valid Content-Range".to_owned(),
            )
        })?;
    if range_start != expected_start {
        return Err(MusicStreamError::InvalidSource(format!(
            "HTTP range resume started at byte {range_start}, expected {expected_start}"
        )));
    }
    Ok(())
}

fn parse_http_content_range_start(value: &str) -> Option<u64> {
    value
        .trim()
        .strip_prefix("bytes ")
        .and_then(|range| range.split_once('-'))
        .and_then(|(start, _)| start.parse::<u64>().ok())
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

fn record_source_resolve_duration(started: Instant) {
    metrics::histogram!(SOURCE_RESOLVE_US_METRIC).record(duration_micros(started.elapsed()) as f64);
}

fn record_source_resolve_error(started: Instant, error: MusicStreamError) -> MusicStreamError {
    metrics::counter!(SOURCE_RESOLVE_ERRORS_METRIC).increment(1);
    record_source_resolve_duration(started);
    error
}

fn map_http_source_error(error: reqwest::Error) -> MusicStreamError {
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

fn http_temp_suffix(url: &str) -> String {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let file_name = path.rsplit('/').next().unwrap_or_default();
    let Some((_, extension)) = file_name.rsplit_once('.') else {
        return ".bin".to_owned();
    };
    if extension.is_empty()
        || extension.len() > 8
        || !extension.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return ".bin".to_owned();
    }
    format!(".{}", extension.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use metrics_util::{CompositeKey, MetricKind};
    use std::io::Read;
    use std::net::TcpListener;
    use std::thread;

    type MetricSnapshot = Vec<(
        CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    )>;

    fn counter_sum(snapshot: &MetricSnapshot, name: &str) -> u64 {
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

    fn has_histogram(snapshot: &MetricSnapshot, name: &str) -> bool {
        snapshot.iter().any(|(key, _, _, value)| {
            key.kind() == MetricKind::Histogram
                && key.key().name() == name
                && matches!(value, DebugValue::Histogram(values) if !values.is_empty())
        })
    }

    fn file_track(path: Option<String>) -> TrackSource {
        TrackSource {
            id: "track-a".to_owned(),
            kind: TrackKind::File,
            url: None,
            path,
            seekable: None,
        }
    }

    fn url_track(url: impl Into<String>) -> TrackSource {
        TrackSource {
            id: "url-a".to_owned(),
            kind: TrackKind::Url,
            url: Some(url.into()),
            path: None,
            seekable: None,
        }
    }

    fn serve_http_once(path: &'static str, body: Vec<u8>, content_length: Option<u64>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP test server");
        let addr = listener.local_addr().expect("HTTP test server address");
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept HTTP request");
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request);

            let content_length = content_length.unwrap_or(body.len() as u64);
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n"
            );
            let _ = stream.write_all(headers.as_bytes());
            let _ = stream.write_all(&body);
            let _ = stream.flush();
        });
        format!("http://{addr}{path}")
    }

    fn serve_resumable_http_once(path: &'static str, body: Vec<u8>, cutoff: usize) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP test server");
        let addr = listener.local_addr().expect("HTTP test server address");
        thread::spawn(move || {
            let (mut first, _) = listener.accept().expect("accept first HTTP request");
            let _ = first.set_read_timeout(Some(Duration::from_secs(2)));
            let mut request = [0_u8; 2048];
            let _ = first.read(&mut request);
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = first.write_all(headers.as_bytes());
            let _ = first.write_all(&body[..cutoff]);
            let _ = first.flush();
            drop(first);

            let (mut second, _) = listener.accept().expect("accept range HTTP request");
            let _ = second.set_read_timeout(Some(Duration::from_secs(2)));
            let mut request = [0_u8; 2048];
            let count = second.read(&mut request).expect("read range request");
            let request = String::from_utf8_lossy(&request[..count]);
            let range_start = request
                .lines()
                .find_map(|line| {
                    line.split_once(':')
                        .filter(|(name, _)| name.eq_ignore_ascii_case("range"))
                        .map(|(_, value)| value.trim())
                })
                .and_then(|value| value.strip_prefix("bytes="))
                .and_then(|range| range.strip_suffix('-'))
                .and_then(|start| start.parse::<usize>().ok())
                .expect("range resume start");
            assert_eq!(range_start, cutoff);

            let remaining = &body[range_start..];
            let headers = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                remaining.len(),
                range_start,
                body.len() - 1,
                body.len()
            );
            let _ = second.write_all(headers.as_bytes());
            let _ = second.write_all(remaining);
            let _ = second.flush();
        });
        format!("http://{addr}{path}")
    }

    fn serve_non_resumable_http_once(path: &'static str, body: Vec<u8>, cutoff: usize) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP test server");
        let addr = listener.local_addr().expect("HTTP test server address");
        thread::spawn(move || {
            let (mut first, _) = listener.accept().expect("accept first HTTP request");
            let _ = first.set_read_timeout(Some(Duration::from_secs(2)));
            let mut request = [0_u8; 2048];
            let _ = first.read(&mut request);
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = first.write_all(headers.as_bytes());
            let _ = first.write_all(&body[..cutoff]);
            let _ = first.flush();
            drop(first);

            let (mut second, _) = listener.accept().expect("accept retry HTTP request");
            let _ = second.set_read_timeout(Some(Duration::from_secs(2)));
            let mut request = [0_u8; 2048];
            let _ = second.read(&mut request);
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = second.write_all(headers.as_bytes());
            let _ = second.write_all(&body);
            let _ = second.flush();
        });
        format!("http://{addr}{path}")
    }

    fn serve_wrong_content_range_http_once(
        path: &'static str,
        body: Vec<u8>,
        cutoff: usize,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP test server");
        let addr = listener.local_addr().expect("HTTP test server address");
        thread::spawn(move || {
            let (mut first, _) = listener.accept().expect("accept first HTTP request");
            let _ = first.set_read_timeout(Some(Duration::from_secs(2)));
            let mut request = [0_u8; 2048];
            let _ = first.read(&mut request);
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = first.write_all(headers.as_bytes());
            let _ = first.write_all(&body[..cutoff]);
            let _ = first.flush();
            drop(first);

            let (mut second, _) = listener.accept().expect("accept retry HTTP request");
            let _ = second.set_read_timeout(Some(Duration::from_secs(2)));
            let mut request = [0_u8; 2048];
            let _ = second.read(&mut request);
            let remaining = &body[cutoff..];
            let headers = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes 0-{}/{}\r\nConnection: close\r\n\r\n",
                remaining.len(),
                body.len() - 1,
                body.len()
            );
            let _ = second.write_all(headers.as_bytes());
            let _ = second.write_all(remaining);
            let _ = second.flush();
        });
        format!("http://{addr}{path}")
    }

    #[test]
    fn resolves_local_file_artifact_without_binding_it_to_slot_lifetime() {
        let temp = tempfile::NamedTempFile::new().expect("temp file");
        std::fs::write(temp.path(), b"audio bytes").expect("write file");

        let artifact = resolve_local_file(&file_track(Some(
            temp.path().to_string_lossy().into_owned(),
        )))
        .expect("artifact");

        assert_eq!(artifact.track_id, "track-a");
        assert_eq!(artifact.stable_key, "track-a");
        assert_eq!(artifact.kind, SourceArtifactKind::LocalFile);
        assert_eq!(artifact.len_bytes, 11);
        assert!(artifact.seekable);
        assert!(!artifact.cacheable);
        assert_eq!(artifact.path(), temp.path());
    }

    #[test]
    fn local_file_source_requires_path() {
        let error = resolve_local_file(&file_track(None)).expect_err("missing path");
        assert_eq!(error.code(), crate::error::ErrorCode::InvalidSource);
    }

    #[test]
    fn local_file_source_rejects_directories() {
        let temp = tempfile::tempdir().expect("temp dir");
        let error = resolve_local_file(&file_track(Some(
            temp.path().to_string_lossy().into_owned(),
        )))
        .expect_err("directory is not file");

        assert_eq!(error.code(), crate::error::ErrorCode::InvalidSource);
    }

    #[test]
    fn local_file_source_rejects_empty_files() {
        let temp = tempfile::NamedTempFile::new().expect("temp file");
        let error = resolve_local_file(&file_track(Some(
            temp.path().to_string_lossy().into_owned(),
        )))
        .expect_err("empty file");

        assert_eq!(error.code(), crate::error::ErrorCode::InvalidSource);
    }

    #[test]
    fn resolves_url_artifact_as_bounded_temp_file() {
        let url = serve_http_once("/audio.Track.WAV?token=abc", b"audio bytes".to_vec(), None);
        let artifact = resolve_http_temp_file(&url_track(url)).expect("HTTP temp artifact");
        let path = artifact.path().to_path_buf();

        assert_eq!(artifact.track_id, "url-a");
        assert_eq!(artifact.stable_key, "url-a");
        assert_eq!(artifact.kind, SourceArtifactKind::HttpTempFile);
        assert_eq!(artifact.len_bytes, 11);
        assert!(artifact.seekable);
        assert!(!artifact.cacheable);
        assert!(artifact.is_temporary());
        assert_eq!(
            path.extension().and_then(|value| value.to_str()),
            Some("wav")
        );
        assert_eq!(
            std::fs::read(&path).expect("temp file body"),
            b"audio bytes"
        );

        drop(artifact);
        assert!(!path.exists());
    }

    #[test]
    fn url_source_resumes_interrupted_download_when_server_honors_range() {
        let body = b"abcdefghijklmnopqrstuvwxyz".to_vec();
        let url = serve_resumable_http_once("/resume.mp3", body.clone(), 10);
        let artifact = resolve_http_temp_file(&url_track(url)).expect("resumed HTTP temp artifact");

        assert_eq!(artifact.kind, SourceArtifactKind::HttpTempFile);
        assert_eq!(artifact.len_bytes, body.len() as u64);
        assert_eq!(std::fs::read(artifact.path()).expect("artifact body"), body);
    }

    #[test]
    fn url_source_rejects_interrupted_download_when_server_ignores_range() {
        let body = b"abcdefghijklmnopqrstuvwxyz".to_vec();
        let url = serve_non_resumable_http_once("/no-resume.mp3", body, 10);
        let error = resolve_http_temp_file(&url_track(url)).expect_err("range ignored");

        assert_eq!(error.code(), crate::error::ErrorCode::InvalidSource);
    }

    #[test]
    fn url_source_rejects_range_resume_with_wrong_content_range() {
        let body = b"abcdefghijklmnopqrstuvwxyz".to_vec();
        let url = serve_wrong_content_range_http_once("/wrong-range.mp3", body, 10);
        let error = resolve_http_temp_file(&url_track(url)).expect_err("wrong content range");

        assert_eq!(error.code(), crate::error::ErrorCode::InvalidSource);
    }

    #[test]
    fn parses_http_content_range_start() {
        assert_eq!(parse_http_content_range_start("bytes 10-25/26"), Some(10));
        assert_eq!(parse_http_content_range_start("bytes 0-0/*"), Some(0));
        assert_eq!(parse_http_content_range_start("items 0-1/2"), None);
        assert_eq!(parse_http_content_range_start("bytes */26"), None);
    }

    #[test]
    fn url_source_rejects_non_http_urls() {
        let metrics = DebuggingRecorder::new();
        let snapshotter = metrics.snapshotter();
        let resolver = FileSourceResolver::default();
        let error = metrics::with_local_recorder(&metrics, || {
            resolver
                .resolve(&url_track("file:///tmp/audio.wav"))
                .expect_err("bad scheme")
        });

        assert_eq!(error.code(), crate::error::ErrorCode::InvalidSource);
        let snapshot = snapshotter.snapshot().into_vec();
        assert_eq!(counter_sum(&snapshot, SOURCE_RESOLVE_ERRORS_METRIC), 1);
        assert!(has_histogram(&snapshot, SOURCE_RESOLVE_US_METRIC));
    }

    #[test]
    fn url_source_rejects_oversized_content_length_before_download() {
        let url = serve_http_once(
            "/too-large.mp3",
            Vec::new(),
            Some(HTTP_SOURCE_MAX_BYTES + 1),
        );
        let error = resolve_http_temp_file(&url_track(url)).expect_err("too large");
        assert_eq!(error.code(), crate::error::ErrorCode::InvalidSource);
    }

    #[test]
    fn http_temp_suffix_requires_real_file_extension() {
        assert_eq!(http_temp_suffix("https://example.test/audio"), ".bin");
        assert_eq!(http_temp_suffix("https://example.test/path/"), ".bin");
        assert_eq!(
            http_temp_suffix("https://example.test/audio.MP3?signature=abc"),
            ".mp3"
        );
        assert_eq!(
            http_temp_suffix("https://example.test/audio.tar.gz#frag"),
            ".gz"
        );
    }

    #[test]
    fn resolver_keeps_live_source_unsupported_explicit() {
        let resolver = FileSourceResolver::default();
        let source = TrackSource {
            id: "live-a".to_owned(),
            kind: TrackKind::Live,
            url: Some("rtmp://example.test/live".to_owned()),
            path: None,
            seekable: None,
        };

        let error = resolver.resolve(&source).expect_err("live unsupported");
        assert_eq!(error.code(), crate::error::ErrorCode::Unsupported);
    }

    #[test]
    fn cached_url_artifact_is_reused_without_redownload() {
        let cache = Arc::new(Mutex::new(SourceArtifactCache::new(1_024, 1_024)));
        let metrics = DebuggingRecorder::new();
        let snapshotter = metrics.snapshotter();
        let resolver =
            FileSourceResolver::with_cache(SourceResolverConfig::default(), Arc::clone(&cache));
        let url = serve_http_once("/cached.wav", b"audio bytes".to_vec(), None);
        let source = url_track(url);

        let (first_path, second) = metrics::with_local_recorder(&metrics, || {
            let first = resolver.resolve(&source).expect("first artifact");
            let first_path = first.path().to_path_buf();
            drop(first);

            let second = resolver.resolve(&source).expect("cached artifact");
            (first_path, second)
        });
        assert_eq!(second.path(), first_path);
        assert!(second.is_temporary());
        assert!(second.cacheable);
        assert_eq!(cache.lock().expect("cache").len(), 1);
        let snapshot = snapshotter.snapshot().into_vec();
        assert_eq!(counter_sum(&snapshot, SOURCE_CACHE_MISS_METRIC), 1);
        assert_eq!(counter_sum(&snapshot, SOURCE_CACHE_INSERTED_METRIC), 1);
        assert_eq!(counter_sum(&snapshot, SOURCE_CACHE_HIT_METRIC), 1);
        assert_eq!(counter_sum(&snapshot, SOURCE_HTTP_BYTES_METRIC), 11);
        assert!(has_histogram(&snapshot, SOURCE_RESOLVE_US_METRIC));

        drop(second);
        assert!(first_path.exists());
        drop(resolver);
        drop(cache);
        assert!(!first_path.exists());
    }

    #[test]
    fn artifact_cache_evicts_lru_entries_to_budget() {
        let mut cache = SourceArtifactCache::new(12, 12);
        let first_temp = tempfile::NamedTempFile::new().expect("first temp");
        std::fs::write(first_temp.path(), b"first bytes").expect("write first");
        let first_path = first_temp.path().to_path_buf();
        let first_temp_path = first_temp.into_temp_path();
        let first = SourceArtifact {
            track_id: "first".to_owned(),
            stable_key: "first".to_owned(),
            kind: SourceArtifactKind::HttpTempFile,
            path: first_path.clone(),
            len_bytes: 11,
            seekable: true,
            cacheable: true,
            cleanup: Some(Arc::new(first_temp_path)),
        };

        let second_temp = tempfile::NamedTempFile::new().expect("second temp");
        std::fs::write(second_temp.path(), b"second").expect("write second");
        let second_path = second_temp.path().to_path_buf();
        let second_temp_path = second_temp.into_temp_path();
        let second = SourceArtifact {
            track_id: "second".to_owned(),
            stable_key: "second".to_owned(),
            kind: SourceArtifactKind::HttpTempFile,
            path: second_path.clone(),
            len_bytes: 6,
            seekable: true,
            cacheable: true,
            cleanup: Some(Arc::new(second_temp_path)),
        };

        assert!(cache.insert(first));
        assert!(first_path.exists());
        assert!(cache.insert(second));

        assert!(cache.get("first").is_none());
        assert!(!first_path.exists());
        assert!(cache.get("second").is_some());
        assert!(second_path.exists());
    }

    #[test]
    fn artifact_cache_clear_releases_temporary_files() {
        let mut cache = SourceArtifactCache::new(64, 64);
        let temp = tempfile::NamedTempFile::new().expect("temp");
        std::fs::write(temp.path(), b"cached").expect("write");
        let path = temp.path().to_path_buf();
        let temp_path = temp.into_temp_path();
        let artifact = SourceArtifact {
            track_id: "cached".to_owned(),
            stable_key: "cached".to_owned(),
            kind: SourceArtifactKind::HttpTempFile,
            path: path.clone(),
            len_bytes: 6,
            seekable: true,
            cacheable: true,
            cleanup: Some(Arc::new(temp_path)),
        };

        assert!(cache.insert(artifact));
        assert!(path.exists());
        cache.clear();

        assert!(cache.is_empty());
        assert_eq!(cache.total_bytes(), 0);
        assert!(!path.exists());
    }
}
