//! Bounded audio HLS playlist and segment delivery.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use m3u8_rs::{AlternativeMediaType, KeyMethod, MediaPlaylist, Playlist};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;

use crate::error::{MusicStreamError, Result};
use crate::model::TrackSource;

use super::live::{
    HttpLiveStream, HttpLiveStreamConfig, HttpLiveStreamReport, LiveByteBudget,
    StreamingByteReader, StreamingByteWriter,
};
use super::{is_retryable_http, map_http_error, shared_http_client};

const MAX_PLAYLIST_BYTES: usize = 1024 * 1024;
const MAX_SEGMENT_BYTES: usize = 16 * 1024 * 1024;
const MAX_MASTER_DEPTH: usize = 3;
const LIVE_EDGE_SEGMENTS: usize = 3;

pub(crate) fn spawn_http_hls_stream(
    source: &TrackSource,
    config: HttpLiveStreamConfig,
    global_byte_budget: LiveByteBudget,
) -> Result<HttpLiveStream> {
    config.validate()?;
    if !source.is_hls() {
        return Err(MusicStreamError::InvalidSource(
            "HLS source requires a .m3u8 URL or m3u8 format hint".to_owned(),
        ));
    }
    let url = source
        .url
        .as_deref()
        .ok_or_else(|| MusicStreamError::InvalidSource("HLS source requires a URL".to_owned()))?;
    let url = reqwest::Url::parse(url)
        .map_err(|error| MusicStreamError::InvalidSource(format!("invalid HLS URL: {error}")))?;
    let headers = source.headers.clone();
    let (writer, reader) =
        StreamingByteReader::with_global_budget(config.max_buffered_bytes, global_byte_budget)?;
    let cancellation = CancellationToken::new();
    let worker_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        let result = run_http_hls_stream(
            url,
            headers,
            config,
            writer.clone(),
            worker_cancellation.clone(),
        )
        .await;
        if let Err(error) = &result {
            writer.fail(error.to_string(), &worker_cancellation).await;
        }
        result
    });
    Ok(HttpLiveStream {
        reader,
        cancellation,
        task,
    })
}

async fn run_http_hls_stream(
    url: reqwest::Url,
    headers: BTreeMap<String, String>,
    config: HttpLiveStreamConfig,
    writer: StreamingByteWriter,
    cancellation: CancellationToken,
) -> Result<HttpLiveStreamReport> {
    let client = shared_http_client();
    let byte_budget = writer.global_byte_budget();
    let http = HlsHttp {
        client: &client,
        headers: &headers,
        config: &config,
        cancellation: &cancellation,
        byte_budget: &byte_budget,
    };
    let (mut media_url, mut playlist) = resolve_media_playlist(&http, url).await?;
    let mut report = HttpLiveStreamReport::default();
    let mut next_sequence = initial_sequence(&playlist);
    let mut segment_kind = None;
    let mut delivered_segments = 0_u64;

    loop {
        if cancellation.is_cancelled() {
            report.stopped = true;
            return Ok(report);
        }

        let playlist_end = playlist
            .media_sequence
            .saturating_add(playlist.segments.len() as u64);
        if next_sequence < playlist.media_sequence {
            metrics::counter!("music_stream.source.hls_live_edge_skips")
                .increment(playlist.media_sequence - next_sequence);
            next_sequence = playlist.media_sequence;
        }

        while next_sequence < playlist_end {
            let index = usize::try_from(next_sequence - playlist.media_sequence)
                .map_err(|_| MusicStreamError::InvalidSource("HLS sequence overflow".to_owned()))?;
            let segment = playlist.segments.get(index).ok_or_else(|| {
                MusicStreamError::InvalidSource("HLS playlist sequence is inconsistent".to_owned())
            })?;
            validate_segment(segment, delivered_segments > 0)?;
            let segment_url = media_url.join(&segment.uri).map_err(|error| {
                MusicStreamError::InvalidSource(format!("invalid HLS segment URL: {error}"))
            })?;
            let response = http
                .fetch(segment_url.clone(), config.idle_timeout, MAX_SEGMENT_BYTES)
                .await?;
            report.bytes_read = report
                .bytes_read
                .saturating_add(response.bytes.len() as u64);
            let (bytes, global_permit) = response.into_parts();
            let (bytes, kind) = normalize_segment(bytes, &segment_url)?;
            if let Some(kind) = kind {
                if segment_kind.is_some_and(|current| current != kind) {
                    return Err(MusicStreamError::Unsupported(
                        "HLS audio codec changes require a new media generation".to_owned(),
                    ));
                }
                segment_kind = Some(kind);
            }
            tokio::select! {
                _ = cancellation.cancelled() => {
                    report.stopped = true;
                    return Ok(report);
                }
                pushed = writer.push_with_global_permit(Bytes::from(bytes), global_permit) => {
                    pushed?;
                }
            }
            delivered_segments = delivered_segments.saturating_add(1);
            next_sequence = next_sequence.saturating_add(1);
            metrics::counter!("music_stream.source.hls_segments").increment(1);
        }

        if playlist.end_list {
            report.completed = true;
            return Ok(report);
        }

        let reload_delay = Duration::from_millis(
            playlist
                .target_duration
                .saturating_mul(500)
                .clamp(500, 5_000),
        );
        tokio::select! {
            _ = cancellation.cancelled() => {
                report.stopped = true;
                return Ok(report);
            }
            _ = tokio::time::sleep(reload_delay) => {}
        }
        (media_url, playlist) = fetch_media_playlist(&http, media_url.clone()).await?;
    }
}

async fn resolve_media_playlist(
    http: &HlsHttp<'_>,
    mut url: reqwest::Url,
) -> Result<(reqwest::Url, MediaPlaylist)> {
    for _ in 0..MAX_MASTER_DEPTH {
        let response = http
            .fetch(url.clone(), http.config.open_timeout, MAX_PLAYLIST_BYTES)
            .await?;
        let playlist = m3u8_rs::parse_playlist_res(&response.bytes).map_err(|error| {
            MusicStreamError::InvalidSource(format!("invalid HLS playlist: {error:?}"))
        })?;
        match playlist {
            Playlist::MediaPlaylist(playlist) => return Ok((response.final_url, playlist)),
            Playlist::MasterPlaylist(master) => {
                let selected = select_variant(&master).ok_or_else(|| {
                    MusicStreamError::Unsupported(
                        "HLS master playlist has no playable media variant".to_owned(),
                    )
                })?;
                url = response.final_url.join(selected).map_err(|error| {
                    MusicStreamError::InvalidSource(format!("invalid HLS variant URL: {error}"))
                })?;
            }
        }
    }
    Err(MusicStreamError::InvalidSource(
        "HLS master playlist nesting exceeds the supported limit".to_owned(),
    ))
}

async fn fetch_media_playlist(
    http: &HlsHttp<'_>,
    url: reqwest::Url,
) -> Result<(reqwest::Url, MediaPlaylist)> {
    let response = http
        .fetch(url, http.config.open_timeout, MAX_PLAYLIST_BYTES)
        .await?;
    match m3u8_rs::parse_playlist_res(&response.bytes).map_err(|error| {
        MusicStreamError::InvalidSource(format!("invalid HLS playlist: {error:?}"))
    })? {
        Playlist::MediaPlaylist(playlist) => Ok((response.final_url, playlist)),
        Playlist::MasterPlaylist(_) => Err(MusicStreamError::InvalidSource(
            "HLS media playlist reload changed into a master playlist".to_owned(),
        )),
    }
}

fn select_variant(master: &m3u8_rs::MasterPlaylist) -> Option<&str> {
    master
        .alternatives
        .iter()
        .filter(|media| media.media_type == AlternativeMediaType::Audio && media.uri.is_some())
        .min_by_key(|media| (!media.default, !media.autoselect))
        .and_then(|media| media.uri.as_deref())
        .or_else(|| {
            master
                .variants
                .iter()
                .filter(|variant| !variant.is_i_frame)
                .min_by_key(|variant| {
                    let video = variant.resolution.is_some()
                        || variant.codecs.as_deref().is_some_and(|codecs| {
                            let codecs = codecs.to_ascii_lowercase();
                            codecs.contains("avc")
                                || codecs.contains("hvc")
                                || codecs.contains("hev")
                                || codecs.contains("vp9")
                                || codecs.contains("av01")
                        });
                    (video, variant.bandwidth)
                })
                .map(|variant| variant.uri.as_str())
        })
}

fn initial_sequence(playlist: &MediaPlaylist) -> u64 {
    if playlist.end_list {
        return playlist.media_sequence;
    }
    playlist
        .media_sequence
        .saturating_add(playlist.segments.len().saturating_sub(LIVE_EDGE_SEGMENTS) as u64)
}

fn validate_segment(segment: &m3u8_rs::MediaSegment, after_first: bool) -> Result<()> {
    if segment
        .key
        .as_ref()
        .is_some_and(|key| key.method != KeyMethod::None)
    {
        return Err(MusicStreamError::Unsupported(
            "encrypted HLS segments are not supported yet".to_owned(),
        ));
    }
    if segment.map.is_some() {
        return Err(MusicStreamError::Unsupported(
            "fMP4 HLS initialization maps are not supported yet".to_owned(),
        ));
    }
    if segment.byte_range.is_some() {
        return Err(MusicStreamError::Unsupported(
            "HLS byte-range segments are not supported yet".to_owned(),
        ));
    }
    if after_first && segment.discontinuity {
        return Err(MusicStreamError::Unsupported(
            "HLS discontinuities require a new media generation".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HlsAudioKind {
    Aac,
    MpegAudio,
}

fn normalize_segment(
    bytes: Vec<u8>,
    url: &reqwest::Url,
) -> Result<(Vec<u8>, Option<HlsAudioKind>)> {
    if is_mpeg_ts(&bytes, url) {
        let elementary = extract_ts_audio(bytes)?;
        let kind = detect_elementary_kind(&elementary).ok_or_else(|| {
            MusicStreamError::Unsupported(
                "MPEG-TS HLS currently supports ADTS AAC or MPEG audio".to_owned(),
            )
        })?;
        return Ok((elementary, Some(kind)));
    }
    if bytes.len() >= 8 && matches!(&bytes[4..8], b"ftyp" | b"styp" | b"moof") {
        return Err(MusicStreamError::Unsupported(
            "fragmented MP4 HLS is not supported yet".to_owned(),
        ));
    }
    let kind = detect_elementary_kind(&bytes);
    Ok((bytes, kind))
}

fn is_mpeg_ts(bytes: &[u8], url: &reqwest::Url) -> bool {
    let extension_is_ts = url
        .path_segments()
        .and_then(Iterator::last)
        .and_then(|name| name.rsplit_once('.'))
        .is_some_and(|(_, extension)| extension.eq_ignore_ascii_case("ts"));
    extension_is_ts || (bytes.len() >= 188 * 2 && bytes[0] == 0x47 && bytes[188] == 0x47)
}

fn extract_ts_audio(mut bytes: Vec<u8>) -> Result<Vec<u8>> {
    if !bytes.len().is_multiple_of(188) {
        return Err(MusicStreamError::DecodeError(
            "MPEG-TS segment is not aligned to 188-byte packets".to_owned(),
        ));
    }
    let mut selected_pid = None;
    let mut continuity = None;
    let mut output_len = 0;
    for packet_start in (0..bytes.len()).step_by(188) {
        let packet_end = packet_start + 188;
        let payload_range = {
            let packet = &bytes[packet_start..packet_end];
            if packet[0] != 0x47 || packet[1] & 0x80 != 0 {
                return Err(MusicStreamError::DecodeError(
                    "invalid MPEG-TS packet header".to_owned(),
                ));
            }
            let pid = (u16::from(packet[1] & 0x1f) << 8) | u16::from(packet[2]);
            let unit_start = packet[1] & 0x40 != 0;
            let adaptation_control = (packet[3] >> 4) & 0x03;
            if adaptation_control == 0 || packet[3] & 0xc0 != 0 {
                return Err(MusicStreamError::Unsupported(
                    "scrambled or invalid MPEG-TS packets are not supported".to_owned(),
                ));
            }
            if adaptation_control == 2 {
                continue;
            }
            let mut offset = 4;
            if adaptation_control == 3 {
                let adaptation_len = usize::from(packet[offset]);
                offset = offset.saturating_add(1 + adaptation_len);
                if offset > packet.len() {
                    return Err(MusicStreamError::DecodeError(
                        "invalid MPEG-TS adaptation field".to_owned(),
                    ));
                }
            }
            let payload = &packet[offset..];
            if payload.is_empty() {
                continue;
            }
            if selected_pid.is_none()
                && unit_start
                && let Some(stream_id) = pes_stream_id(payload)
                && (0xc0..=0xdf).contains(&stream_id)
            {
                selected_pid = Some(pid);
            }
            if selected_pid != Some(pid) {
                continue;
            }
            let counter = packet[3] & 0x0f;
            if let Some(previous) = continuity
                && counter != (previous + 1) & 0x0f
            {
                return Err(MusicStreamError::DecodeError(
                    "MPEG-TS audio continuity counter skipped".to_owned(),
                ));
            }
            continuity = Some(counter);
            let payload_offset = if unit_start {
                offset + pes_payload_offset(payload)?
            } else {
                offset
            };
            packet_start + payload_offset..packet_end
        };
        let payload_len = payload_range.len();
        bytes.copy_within(payload_range, output_len);
        output_len += payload_len;
    }
    if selected_pid.is_none() || output_len == 0 {
        return Err(MusicStreamError::Unsupported(
            "MPEG-TS segment contains no supported audio PES stream".to_owned(),
        ));
    }
    bytes.truncate(output_len);
    Ok(bytes)
}

fn pes_stream_id(payload: &[u8]) -> Option<u8> {
    (payload.len() >= 6 && payload[..3] == [0, 0, 1]).then_some(payload[3])
}

fn pes_payload_offset(payload: &[u8]) -> Result<usize> {
    let stream_id = pes_stream_id(payload).ok_or_else(|| {
        MusicStreamError::DecodeError("invalid MPEG-TS PES start code".to_owned())
    })?;
    if !(0xc0..=0xdf).contains(&stream_id) || payload.len() < 9 {
        return Err(MusicStreamError::Unsupported(
            "MPEG-TS PES stream is not supported audio".to_owned(),
        ));
    }
    let offset = 9_usize.saturating_add(usize::from(payload[8]));
    if offset > payload.len() {
        return Err(MusicStreamError::DecodeError(
            "MPEG-TS PES header exceeds its packet".to_owned(),
        ));
    }
    Ok(offset)
}

fn detect_elementary_kind(bytes: &[u8]) -> Option<HlsAudioKind> {
    if bytes.len() >= 2 && bytes[0] == 0xff && bytes[1] & 0xf6 == 0xf0 {
        return Some(HlsAudioKind::Aac);
    }
    if bytes.len() >= 2 && bytes[0] == 0xff && bytes[1] & 0xe0 == 0xe0 {
        return Some(HlsAudioKind::MpegAudio);
    }
    None
}

struct HlsHttp<'a> {
    client: &'a reqwest::Client,
    headers: &'a BTreeMap<String, String>,
    config: &'a HttpLiveStreamConfig,
    cancellation: &'a CancellationToken,
    byte_budget: &'a LiveByteBudget,
}

impl HlsHttp<'_> {
    async fn fetch(
        &self,
        url: reqwest::Url,
        timeout: Duration,
        max_bytes: usize,
    ) -> Result<BudgetedResponse> {
        let mut attempt = 0;
        loop {
            match fetch_bounded(
                self.client,
                url.clone(),
                self.headers,
                timeout,
                max_bytes,
                self.cancellation,
                self.byte_budget,
            )
            .await
            {
                Ok(bytes) => return Ok(bytes),
                Err(error) if attempt < self.config.max_retries && error.retryable => {
                    attempt += 1;
                    metrics::counter!("music_stream.source.hls_http_retries").increment(1);
                    tokio::select! {
                        _ = self.cancellation.cancelled() => {
                            return Err(MusicStreamError::StreamClosed(
                                "HLS source was cancelled".to_owned(),
                            ));
                        }
                        _ = tokio::time::sleep(self.config.retry_backoff) => {}
                    }
                }
                Err(error) => return Err(error.error),
            }
        }
    }
}

#[derive(Debug)]
struct BudgetedResponse {
    bytes: Vec<u8>,
    permit: OwnedSemaphorePermit,
    final_url: reqwest::Url,
}

impl BudgetedResponse {
    fn into_parts(self) -> (Vec<u8>, OwnedSemaphorePermit) {
        (self.bytes, self.permit)
    }
}

#[derive(Debug)]
struct HlsFetchError {
    error: MusicStreamError,
    retryable: bool,
}

impl HlsFetchError {
    fn terminal(error: MusicStreamError) -> Self {
        Self {
            error,
            retryable: false,
        }
    }

    fn retryable(error: MusicStreamError) -> Self {
        Self {
            error,
            retryable: true,
        }
    }

    fn http(error: reqwest::Error) -> Self {
        Self {
            retryable: is_retryable_http(&error),
            error: map_http_error(error),
        }
    }
}

async fn fetch_bounded(
    client: &reqwest::Client,
    url: reqwest::Url,
    headers: &BTreeMap<String, String>,
    timeout: Duration,
    max_bytes: usize,
    cancellation: &CancellationToken,
    byte_budget: &LiveByteBudget,
) -> std::result::Result<BudgetedResponse, HlsFetchError> {
    let mut request = client.get(url);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = tokio::select! {
        _ = cancellation.cancelled() => {
            return Err(HlsFetchError::terminal(MusicStreamError::StreamClosed(
                "HLS source was cancelled".to_owned(),
            )));
        }
        response = tokio::time::timeout(timeout, request.send()) => {
            response.map_err(|_| HlsFetchError::retryable(MusicStreamError::SourceTimeout(
                "HLS request did not open before the deadline".to_owned(),
            )))?.map_err(HlsFetchError::http)?
        }
    };
    let mut response = response.error_for_status().map_err(HlsFetchError::http)?;
    let final_url = response.url().clone();
    let content_length = response.content_length();
    if content_length.is_some_and(|length| length > max_bytes as u64) {
        return Err(HlsFetchError::terminal(MusicStreamError::InvalidSource(
            "HLS response exceeds its byte limit".to_owned(),
        )));
    }
    if content_length == Some(0) {
        return Err(HlsFetchError::terminal(MusicStreamError::InvalidSource(
            "HLS response is empty".to_owned(),
        )));
    }
    let reserved_bytes = match content_length {
        Some(length) => usize::try_from(length).map_err(|_| {
            HlsFetchError::terminal(MusicStreamError::InvalidSource(
                "HLS response length does not fit this platform".to_owned(),
            ))
        })?,
        None => max_bytes.min(byte_budget.capacity()),
    };
    if reserved_bytes > byte_budget.capacity() {
        return Err(HlsFetchError::terminal(MusicStreamError::InvalidSource(
            "HLS response exceeds the runtime live byte budget".to_owned(),
        )));
    }
    // Reserve once before reading the body. Incremental acquisition can deadlock when
    // concurrent segments each hold part of the shared budget and wait for the rest.
    let budget_wait_started = std::time::Instant::now();
    let permit = tokio::select! {
        _ = cancellation.cancelled() => {
            return Err(HlsFetchError::terminal(MusicStreamError::StreamClosed(
                "HLS source was cancelled".to_owned(),
            )));
        }
        permit = byte_budget.acquire(reserved_bytes) => {
            permit.map_err(HlsFetchError::terminal)?
        }
    };
    metrics::histogram!("music_stream.source.live_global_budget_wait_us")
        .record(budget_wait_started.elapsed().as_micros() as f64);
    // Keeping allocation capacity equal to the reservation makes the byte budget
    // account for the retained allocation even after TS is compacted in place.
    let mut bytes = Vec::with_capacity(reserved_bytes);
    loop {
        let chunk = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(HlsFetchError::terminal(MusicStreamError::StreamClosed(
                    "HLS source was cancelled".to_owned(),
                )));
            }
            chunk = tokio::time::timeout(timeout, response.chunk()) => {
                chunk.map_err(|_| HlsFetchError::retryable(MusicStreamError::SourceTimeout(
                    "HLS response body stalled past the deadline".to_owned(),
                )))?.map_err(HlsFetchError::http)?
            }
        };
        let Some(chunk) = chunk else {
            break;
        };
        let next_len = bytes.len().saturating_add(chunk.len());
        if next_len > max_bytes {
            return Err(HlsFetchError::terminal(MusicStreamError::InvalidSource(
                "HLS response exceeds its byte limit".to_owned(),
            )));
        }
        if next_len > reserved_bytes {
            return Err(HlsFetchError::terminal(MusicStreamError::InvalidSource(
                "HLS response exceeds the runtime live byte budget".to_owned(),
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.is_empty() {
        return Err(HlsFetchError::terminal(MusicStreamError::InvalidSource(
            "HLS response is empty".to_owned(),
        )));
    }
    Ok(BudgetedResponse {
        bytes,
        permit,
        final_url,
    })
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn variant_selection_prefers_default_audio_rendition() {
        let master = m3u8_rs::parse_playlist_res(
            b"#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",NAME=\"main\",DEFAULT=YES,URI=\"audio/index.m3u8\"\n#EXT-X-STREAM-INF:BANDWIDTH=1000000,AUDIO=\"a\"\nvideo/index.m3u8\n",
        )
        .expect("playlist");
        let Playlist::MasterPlaylist(master) = master else {
            panic!("master playlist");
        };
        assert_eq!(select_variant(&master), Some("audio/index.m3u8"));
    }

    #[test]
    fn ts_audio_extractor_removes_transport_and_pes_headers() {
        let elementary = b"\xff\xf1\x50\x80\x00\x1f\xfcAAC";
        let segment = make_test_ts(elementary);
        assert_eq!(extract_ts_audio(segment).expect("extract"), elementary);
        assert_eq!(detect_elementary_kind(elementary), Some(HlsAudioKind::Aac));
    }

    #[tokio::test]
    async fn hls_stream_fetches_playlist_and_delivers_ts_audio() {
        let elementary = b"\xff\xf1\x50\x80\x00\x1f\xfcAAC";
        let segment = make_test_ts(elementary);
        let playlist =
            b"#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1,\nsegment.ts\n#EXT-X-ENDLIST\n";
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut request = [0_u8; 1024];
                let read = socket.read(&mut request).await.expect("request");
                let request = String::from_utf8_lossy(&request[..read]);
                if request.starts_with("GET /playlist.m3u8 ") {
                    socket
                        .write_all(
                            b"HTTP/1.1 302 Found\r\nLocation: /cdn/index.m3u8\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .await
                        .expect("redirect");
                    continue;
                }
                let body: &[u8] = if request.starts_with("GET /cdn/segment.ts ") {
                    &segment
                } else {
                    assert!(request.starts_with("GET /cdn/index.m3u8 "));
                    playlist
                };
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(headers.as_bytes()).await.expect("headers");
                socket.write_all(body).await.expect("body");
            }
        });
        let source = TrackSource {
            id: "hls-test".to_owned(),
            kind: crate::model::TrackKind::Url,
            url: Some(format!("http://{address}/playlist.m3u8")),
            path: None,
            format_hint: None,
            seekable: Some(true),
            headers: BTreeMap::new(),
        };
        let stream = spawn_http_hls_stream(
            &source,
            HttpLiveStreamConfig {
                max_buffered_bytes: 32,
                ..HttpLiveStreamConfig::default()
            },
            LiveByteBudget::new(1024).expect("budget"),
        )
        .expect("HLS stream");
        let HttpLiveStream {
            mut reader, task, ..
        } = stream;
        let reader_task = tokio::task::spawn_blocking(move || {
            let mut output = Vec::new();
            reader.read_to_end(&mut output).expect("read HLS bytes");
            output
        });

        let report = task.await.expect("HLS task").expect("HLS result");
        assert!(report.completed);
        assert_eq!(report.bytes_read, 188);
        assert_eq!(reader_task.await.expect("reader task"), elementary);
        server.await.expect("server");
    }

    fn make_test_ts(elementary: &[u8]) -> Vec<u8> {
        let mut pes = vec![0, 0, 1, 0xc0, 0, 0, 0x80, 0, 0];
        pes.extend_from_slice(elementary);
        let mut output = Vec::new();
        let mut offset = 0;
        let mut counter = 0;
        while offset < pes.len() {
            let remaining = pes.len() - offset;
            let payload_len = remaining.min(184);
            let mut packet = [0xff_u8; 188];
            packet[0] = 0x47;
            packet[1] = 0x01 | if offset == 0 { 0x40 } else { 0 };
            packet[2] = 0x00;
            if payload_len == 184 {
                packet[3] = 0x10 | counter;
                packet[4..].copy_from_slice(&pes[offset..offset + payload_len]);
            } else {
                packet[3] = 0x30 | counter;
                let adaptation_len = 183 - payload_len;
                packet[4] = adaptation_len as u8;
                if adaptation_len > 0 {
                    packet[5] = 0;
                }
                let payload_offset = 5 + adaptation_len;
                packet[payload_offset..].copy_from_slice(&pes[offset..offset + payload_len]);
            }
            output.extend_from_slice(&packet);
            offset += payload_len;
            counter = (counter + 1) & 0x0f;
        }
        output
    }
}
