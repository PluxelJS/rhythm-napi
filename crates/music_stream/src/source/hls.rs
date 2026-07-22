//! Bounded audio HLS playlist and segment delivery.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use m3u8_rs::{AlternativeMediaType, ByteRange, KeyMethod, MediaPlaylist, Playlist};
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
const MAX_INIT_SEGMENT_BYTES: usize = 4 * 1024 * 1024;
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
    let mut initialization = None;
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
            let mapped_fmp4 = segment.map.is_some();
            if let Some(map) = segment.map.as_ref() {
                let map_url = media_url.join(&map.uri).map_err(|error| {
                    MusicStreamError::InvalidSource(format!(
                        "invalid HLS initialization URL: {error}"
                    ))
                })?;
                let map_range = map
                    .byte_range
                    .as_ref()
                    .map(HlsByteRange::try_from)
                    .transpose()?;
                let map_identity = InitializationMap {
                    url: map_url,
                    range: map_range,
                };
                match initialization.as_ref() {
                    Some(current) if current != &map_identity => {
                        return Err(MusicStreamError::Unsupported(
                            "HLS initialization map changes require a new media generation"
                                .to_owned(),
                        ));
                    }
                    None if delivered_segments > 0 => {
                        return Err(MusicStreamError::Unsupported(
                            "HLS cannot introduce an initialization map midstream".to_owned(),
                        ));
                    }
                    None => {
                        let response = http
                            .fetch_range(
                                map_identity.url.clone(),
                                config.idle_timeout,
                                MAX_INIT_SEGMENT_BYTES,
                                map_identity.range,
                            )
                            .await?;
                        report.bytes_read = report
                            .bytes_read
                            .saturating_add(response.bytes.len() as u64);
                        validate_fmp4_initialization(&response.bytes)?;
                        let (bytes, permit) = response.into_parts();
                        if !push_budgeted_bytes(&writer, &cancellation, bytes, permit).await? {
                            report.stopped = true;
                            return Ok(report);
                        }
                        initialization = Some(map_identity);
                        metrics::counter!("music_stream.source.hls_initialization_maps")
                            .increment(1);
                    }
                    Some(_) => {}
                }
            } else if initialization.is_some() {
                return Err(MusicStreamError::Unsupported(
                    "HLS cannot remove an initialization map midstream".to_owned(),
                ));
            }
            let segment_url = media_url.join(&segment.uri).map_err(|error| {
                MusicStreamError::InvalidSource(format!("invalid HLS segment URL: {error}"))
            })?;
            let response = http
                .fetch_range(
                    segment_url.clone(),
                    config.idle_timeout,
                    MAX_SEGMENT_BYTES,
                    segment
                        .byte_range
                        .as_ref()
                        .map(HlsByteRange::try_from)
                        .transpose()?,
                )
                .await?;
            report.bytes_read = report
                .bytes_read
                .saturating_add(response.bytes.len() as u64);
            let (bytes, global_permit) = response.into_parts();
            let (bytes, kind) = normalize_segment(bytes, &segment_url, mapped_fmp4)?;
            if let Some(kind) = kind {
                if segment_kind.is_some_and(|current| current != kind) {
                    return Err(MusicStreamError::Unsupported(
                        "HLS audio codec changes require a new media generation".to_owned(),
                    ));
                }
                segment_kind = Some(kind);
            }
            if !push_budgeted_bytes(&writer, &cancellation, bytes, global_permit).await? {
                report.stopped = true;
                return Ok(report);
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
            Playlist::MediaPlaylist(mut playlist) => {
                normalize_media_playlist(&mut playlist)?;
                return Ok((response.final_url, playlist));
            }
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
        Playlist::MediaPlaylist(mut playlist) => {
            normalize_media_playlist(&mut playlist)?;
            Ok((response.final_url, playlist))
        }
        Playlist::MasterPlaylist(_) => Err(MusicStreamError::InvalidSource(
            "HLS media playlist reload changed into a master playlist".to_owned(),
        )),
    }
}

#[derive(Debug)]
struct ByteRangeCursor {
    uri: String,
    next_offset: u64,
}

fn normalize_media_playlist(playlist: &mut MediaPlaylist) -> Result<()> {
    let mut active_key = None;
    let mut active_map = None;
    let mut map_range_cursor = None;
    let mut segment_range_cursor = None;
    for segment in &mut playlist.segments {
        if let Some(key) = segment.key.as_ref() {
            active_key = Some(key.clone());
        } else {
            segment.key.clone_from(&active_key);
        }
        if let Some(map) = segment.map.as_mut() {
            resolve_byte_range(&map.uri, &mut map.byte_range, &mut map_range_cursor)?;
            active_map = Some(map.clone());
        } else {
            segment.map.clone_from(&active_map);
        }
        resolve_byte_range(
            &segment.uri,
            &mut segment.byte_range,
            &mut segment_range_cursor,
        )?;
    }
    Ok(())
}

fn resolve_byte_range(
    uri: &str,
    byte_range: &mut Option<ByteRange>,
    cursor: &mut Option<ByteRangeCursor>,
) -> Result<()> {
    let Some(byte_range) = byte_range.as_mut() else {
        *cursor = None;
        return Ok(());
    };
    if byte_range.length == 0 {
        return Err(MusicStreamError::InvalidSource(
            "HLS byte range must be non-empty".to_owned(),
        ));
    }
    let offset = match byte_range.offset {
        Some(offset) => offset,
        None => cursor
            .as_ref()
            .filter(|cursor| cursor.uri == uri)
            .map(|cursor| cursor.next_offset)
            .ok_or_else(|| {
                MusicStreamError::InvalidSource(
                    "HLS implicit byte range has no preceding range for the same URI".to_owned(),
                )
            })?,
    };
    let next_offset = offset.checked_add(byte_range.length).ok_or_else(|| {
        MusicStreamError::InvalidSource("HLS byte range overflows its resource".to_owned())
    })?;
    byte_range.offset = Some(offset);
    *cursor = Some(ByteRangeCursor {
        uri: uri.to_owned(),
        next_offset,
    });
    Ok(())
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
    if after_first && segment.discontinuity {
        return Err(MusicStreamError::Unsupported(
            "HLS discontinuities require a new media generation".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HlsByteRange {
    start: u64,
    length: u64,
}

impl TryFrom<&ByteRange> for HlsByteRange {
    type Error = MusicStreamError;

    fn try_from(byte_range: &ByteRange) -> Result<Self> {
        let start = byte_range.offset.ok_or_else(|| {
            MusicStreamError::Internal("HLS byte range was not normalized".to_owned())
        })?;
        start.checked_add(byte_range.length).ok_or_else(|| {
            MusicStreamError::InvalidSource("HLS byte range overflows its resource".to_owned())
        })?;
        Ok(Self {
            start,
            length: byte_range.length,
        })
    }
}

impl HlsByteRange {
    fn end_inclusive(self) -> u64 {
        self.start + self.length - 1
    }
}

#[derive(Debug, PartialEq, Eq)]
struct InitializationMap {
    url: reqwest::Url,
    range: Option<HlsByteRange>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HlsAudioKind {
    Aac,
    FragmentedMp4,
    MpegAudio,
}

fn normalize_segment(
    bytes: Vec<u8>,
    url: &reqwest::Url,
    mapped_fmp4: bool,
) -> Result<(Vec<u8>, Option<HlsAudioKind>)> {
    if mapped_fmp4 {
        validate_fmp4_fragment(&bytes)?;
        return Ok((bytes, Some(HlsAudioKind::FragmentedMp4)));
    }
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
            "fragmented MP4 HLS requires an EXT-X-MAP initialization segment".to_owned(),
        ));
    }
    let kind = detect_elementary_kind(&bytes);
    Ok((bytes, kind))
}

async fn push_budgeted_bytes(
    writer: &StreamingByteWriter,
    cancellation: &CancellationToken,
    bytes: Vec<u8>,
    permit: OwnedSemaphorePermit,
) -> Result<bool> {
    tokio::select! {
        _ = cancellation.cancelled() => Ok(false),
        pushed = writer.push_with_global_permit(Bytes::from(bytes), permit) => {
            pushed.map(|()| true)
        }
    }
}

#[derive(Default)]
struct IsoBmffBoxes {
    file_type: bool,
    media_data: Option<usize>,
    movie: bool,
    movie_fragment: Option<usize>,
}

fn inspect_iso_bmff(bytes: &[u8]) -> Result<IsoBmffBoxes> {
    let mut boxes = IsoBmffBoxes::default();
    let mut offset = 0;
    while offset < bytes.len() {
        let remaining = bytes.len() - offset;
        if remaining < 8 {
            return Err(MusicStreamError::DecodeError(
                "fragmented MP4 has a truncated box header".to_owned(),
            ));
        }
        let size32 = u32::from_be_bytes(bytes[offset..offset + 4].try_into().map_err(|_| {
            MusicStreamError::DecodeError("invalid fragmented MP4 box size".to_owned())
        })?);
        let box_type = &bytes[offset + 4..offset + 8];
        let (header_len, box_len) = match size32 {
            0 => (8, remaining),
            1 => {
                if remaining < 16 {
                    return Err(MusicStreamError::DecodeError(
                        "fragmented MP4 has a truncated extended box header".to_owned(),
                    ));
                }
                let extended = u64::from_be_bytes(
                    bytes[offset + 8..offset + 16].try_into().map_err(|_| {
                        MusicStreamError::DecodeError(
                            "invalid fragmented MP4 extended box size".to_owned(),
                        )
                    })?,
                );
                let length = usize::try_from(extended).map_err(|_| {
                    MusicStreamError::InvalidSource(
                        "fragmented MP4 box is too large for this platform".to_owned(),
                    )
                })?;
                (16, length)
            }
            length => (8, length as usize),
        };
        if box_len < header_len || box_len > remaining {
            return Err(MusicStreamError::DecodeError(
                "fragmented MP4 box exceeds its response".to_owned(),
            ));
        }
        match box_type {
            b"ftyp" => boxes.file_type = true,
            b"moov" => boxes.movie = true,
            b"moof" => {
                boxes.movie_fragment.get_or_insert(offset);
            }
            b"mdat" => {
                boxes.media_data.get_or_insert(offset);
            }
            _ => {}
        };
        offset += box_len;
    }
    Ok(boxes)
}

fn validate_fmp4_initialization(bytes: &[u8]) -> Result<()> {
    let boxes = inspect_iso_bmff(bytes)?;
    if !boxes.file_type || !boxes.movie {
        return Err(MusicStreamError::InvalidSource(
            "HLS initialization segment requires ftyp and moov boxes".to_owned(),
        ));
    }
    Ok(())
}

fn validate_fmp4_fragment(bytes: &[u8]) -> Result<()> {
    let boxes = inspect_iso_bmff(bytes)?;
    match (boxes.movie_fragment, boxes.media_data) {
        (Some(moof), Some(mdat)) if moof < mdat => Ok(()),
        _ => Err(MusicStreamError::InvalidSource(
            "HLS fMP4 segment requires a moof box followed by mdat".to_owned(),
        )),
    }
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
        self.fetch_range(url, timeout, max_bytes, None).await
    }

    async fn fetch_range(
        &self,
        url: reqwest::Url,
        timeout: Duration,
        max_bytes: usize,
        range: Option<HlsByteRange>,
    ) -> Result<BudgetedResponse> {
        let mut attempt = 0;
        loop {
            match self
                .fetch_once(url.clone(), timeout, max_bytes, range)
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

impl HlsHttp<'_> {
    async fn fetch_once(
        &self,
        url: reqwest::Url,
        timeout: Duration,
        max_bytes: usize,
        range: Option<HlsByteRange>,
    ) -> std::result::Result<BudgetedResponse, HlsFetchError> {
        let mut request = self.client.get(url);
        for (name, value) in self.headers {
            request = request.header(name, value);
        }
        if let Some(range) = range {
            request = request.header(
                reqwest::header::RANGE,
                format!("bytes={}-{}", range.start, range.end_inclusive()),
            );
        }
        let response = tokio::select! {
            _ = self.cancellation.cancelled() => {
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
        if let Some(range) = range {
            if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
                return Err(HlsFetchError::terminal(MusicStreamError::InvalidSource(
                    "HLS server ignored a byte-range request".to_owned(),
                )));
            }
            let content_range = response
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok());
            if !content_range.is_some_and(|value| content_range_matches(value, range)) {
                return Err(HlsFetchError::terminal(MusicStreamError::InvalidSource(
                    "HLS server returned an unexpected content range".to_owned(),
                )));
            }
        }
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
        let reserved_bytes = match range {
            Some(range) => {
                if range.length > max_bytes as u64
                    || content_length.is_some_and(|length| length != range.length)
                {
                    return Err(HlsFetchError::terminal(MusicStreamError::InvalidSource(
                        "HLS byte-range response has an invalid length".to_owned(),
                    )));
                }
                usize::try_from(range.length).map_err(|_| {
                    HlsFetchError::terminal(MusicStreamError::InvalidSource(
                        "HLS byte range does not fit this platform".to_owned(),
                    ))
                })?
            }
            None => match content_length {
                Some(length) => usize::try_from(length).map_err(|_| {
                    HlsFetchError::terminal(MusicStreamError::InvalidSource(
                        "HLS response length does not fit this platform".to_owned(),
                    ))
                })?,
                None => max_bytes.min(self.byte_budget.capacity()),
            },
        };
        if reserved_bytes > self.byte_budget.capacity() {
            return Err(HlsFetchError::terminal(MusicStreamError::InvalidSource(
                "HLS response exceeds the runtime live byte budget".to_owned(),
            )));
        }
        // Reserve once before reading the body. Incremental acquisition can deadlock when
        // concurrent segments each hold part of the shared budget and wait for the rest.
        let budget_wait_started = std::time::Instant::now();
        let permit = tokio::select! {
            _ = self.cancellation.cancelled() => {
                return Err(HlsFetchError::terminal(MusicStreamError::StreamClosed(
                    "HLS source was cancelled".to_owned(),
                )));
            }
            permit = self.byte_budget.acquire(reserved_bytes) => {
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
                _ = self.cancellation.cancelled() => {
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
        if range.is_some() && bytes.len() != reserved_bytes {
            return Err(HlsFetchError::terminal(MusicStreamError::InvalidSource(
                "HLS byte-range response ended at the wrong length".to_owned(),
            )));
        }
        Ok(BudgetedResponse {
            bytes,
            permit,
            final_url,
        })
    }
}

fn content_range_matches(value: &str, expected: HlsByteRange) -> bool {
    let mut parts = value.split_whitespace();
    let (Some(unit), Some(remainder), None) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    if !unit.eq_ignore_ascii_case("bytes") {
        return false;
    }
    let Some((bounds, total)) = remainder.split_once('/') else {
        return false;
    };
    let Some((start, end)) = bounds.split_once('-') else {
        return false;
    };
    let expected_end = expected.end_inclusive();
    start.parse::<u64>().ok() == Some(expected.start)
        && end.parse::<u64>().ok() == Some(expected_end)
        && (total == "*" || total.parse::<u64>().is_ok_and(|total| total > expected_end))
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

    #[test]
    fn playlist_state_inherits_map_and_encryption_key() {
        let Playlist::MediaPlaylist(mut playlist) = m3u8_rs::parse_playlist_res(
            b"#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:1,\none.m4s\n#EXTINF:1,\ntwo.m4s\n#EXT-X-ENDLIST\n",
        )
        .expect("playlist")
        else {
            panic!("media playlist");
        };
        assert!(playlist.segments[1].map.is_none());
        assert!(playlist.segments[1].key.is_none());

        normalize_media_playlist(&mut playlist).expect("normalize playlist");

        assert_eq!(
            playlist.segments[1]
                .map
                .as_ref()
                .map(|map| map.uri.as_str()),
            Some("init.mp4")
        );
        assert_eq!(
            playlist.segments[1].key.as_ref().map(|key| &key.method),
            Some(&KeyMethod::AES128)
        );
        assert!(validate_segment(&playlist.segments[1], true).is_err());
    }

    #[test]
    fn playlist_normalization_resolves_implicit_byte_ranges() {
        let Playlist::MediaPlaylist(mut playlist) = m3u8_rs::parse_playlist_res(
            b"#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1,\n#EXT-X-BYTERANGE:10@100\nmedia.mp4\n#EXTINF:1,\n#EXT-X-BYTERANGE:12\nmedia.mp4\n#EXT-X-ENDLIST\n",
        )
        .expect("playlist")
        else {
            panic!("media playlist");
        };

        normalize_media_playlist(&mut playlist).expect("normalize playlist");

        assert_eq!(
            playlist.segments[0].byte_range.as_ref().unwrap().offset,
            Some(100)
        );
        assert_eq!(
            playlist.segments[1].byte_range.as_ref().unwrap().offset,
            Some(110)
        );
        assert!(content_range_matches(
            "bytes 110-121/999",
            HlsByteRange {
                start: 110,
                length: 12,
            }
        ));
        assert!(!content_range_matches(
            "bytes 110-121/121",
            HlsByteRange {
                start: 110,
                length: 12,
            }
        ));
    }

    #[test]
    fn fragmented_mp4_validation_requires_streamable_box_order() {
        let mut initialization = make_iso_box(*b"ftyp", b"isom");
        initialization.extend(make_iso_box(*b"moov", b"metadata"));
        validate_fmp4_initialization(&initialization).expect("initialization");

        let mut fragment = make_iso_box(*b"styp", b"msdh");
        fragment.extend(make_iso_box(*b"moof", b"samples"));
        fragment.extend(make_iso_box(*b"mdat", b"media"));
        validate_fmp4_fragment(&fragment).expect("fragment");

        let mut reversed = make_iso_box(*b"mdat", b"media");
        reversed.extend(make_iso_box(*b"moof", b"samples"));
        assert!(validate_fmp4_fragment(&reversed).is_err());
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

    #[tokio::test]
    async fn hls_stream_delivers_byte_ranged_fmp4_in_stream_order() {
        let mut initialization = make_iso_box(*b"ftyp", b"isom");
        initialization.extend(make_iso_box(*b"moov", b"metadata"));
        let mut fragment = make_iso_box(*b"styp", b"msdh");
        fragment.extend(make_iso_box(*b"moof", b"samples"));
        fragment.extend(make_iso_box(*b"mdat", b"media"));
        let mut resource = initialization.clone();
        resource.extend_from_slice(&fragment);
        resource.extend_from_slice(&fragment);
        let playlist = format!(
            "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:1\n#EXT-X-MAP:URI=\"media.mp4\",BYTERANGE=\"{}@0\"\n#EXTINF:1,\n#EXT-X-BYTERANGE:{}@{}\nmedia.mp4\n#EXTINF:1,\n#EXT-X-BYTERANGE:{}\nmedia.mp4\n#EXT-X-ENDLIST\n",
            initialization.len(),
            fragment.len(),
            initialization.len(),
            fragment.len(),
        )
        .into_bytes();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let expected = resource.clone();
        let server = tokio::spawn(async move {
            for _ in 0..4 {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut request = [0_u8; 1024];
                let read = socket.read(&mut request).await.expect("request");
                let request = String::from_utf8_lossy(&request[..read]);
                if request.starts_with("GET /playlist.m3u8 ") {
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        playlist.len()
                    );
                    socket.write_all(headers.as_bytes()).await.expect("headers");
                    socket.write_all(&playlist).await.expect("playlist");
                } else {
                    assert!(request.starts_with("GET /media.mp4 "));
                    let request = request.to_ascii_lowercase();
                    let range = request
                        .lines()
                        .find_map(|line| line.strip_prefix("range: bytes="))
                        .expect("range header");
                    let (start, end) = range.split_once('-').expect("range bounds");
                    let start = start.parse::<usize>().expect("range start");
                    let end = end.parse::<usize>().expect("range end");
                    let body = &resource[start..=end];
                    let headers = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nConnection: close\r\n\r\n",
                        body.len(),
                        resource.len()
                    );
                    socket.write_all(headers.as_bytes()).await.expect("headers");
                    socket.write_all(body).await.expect("range body");
                }
            }
        });
        let source = TrackSource {
            id: "fmp4-hls-test".to_owned(),
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
        assert_eq!(report.bytes_read, expected.len() as u64);
        assert_eq!(reader_task.await.expect("reader task"), expected);
        server.await.expect("server");
    }

    fn make_iso_box(box_type: [u8; 4], payload: &[u8]) -> Vec<u8> {
        let length = u32::try_from(payload.len() + 8).expect("test box length");
        let mut output = Vec::with_capacity(length as usize);
        output.extend_from_slice(&length.to_be_bytes());
        output.extend_from_slice(&box_type);
        output.extend_from_slice(payload);
        output
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
