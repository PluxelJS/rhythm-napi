//! Bounded audio HLS playlist and segment delivery.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use aes::cipher::{BlockModeDecrypt, KeyIvInit, block_padding::Pkcs7};
use bytes::Bytes;
use m3u8_rs::{AlternativeMediaType, ByteRange, Key, KeyMethod, MediaPlaylist, Playlist};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;

use crate::error::{MusicStreamError, Result};
use crate::model::{NetworkPolicy, TrackSource, validate_network_url};

use super::live::{
    HttpLiveStream, HttpLiveStreamConfig, HttpLiveStreamReport, LiveByteBudget,
    StreamingByteReader, StreamingByteWriter,
};
use super::{http_client_for, is_retryable_http, map_http_error};

const MAX_PLAYLIST_BYTES: usize = 1024 * 1024;
const MAX_INIT_SEGMENT_BYTES: usize = 4 * 1024 * 1024;
const MAX_SEGMENT_BYTES: usize = 16 * 1024 * 1024;
const AES128_KEY_BYTES: usize = 16;
const MAX_MASTER_DEPTH: usize = 3;
const LIVE_EDGE_SEGMENTS: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HlsPlaylistKind {
    Vod,
    Live,
}

struct HlsStreamSpec {
    media_url: reqwest::Url,
    playlist: MediaPlaylist,
    headers: BTreeMap<String, String>,
    network_policy: NetworkPolicy,
    client: reqwest::Client,
    config: HttpLiveStreamConfig,
}

pub(crate) async fn spawn_http_hls_stream(
    source: &TrackSource,
    config: HttpLiveStreamConfig,
    global_byte_budget: LiveByteBudget,
) -> Result<(HttpLiveStream, HlsPlaylistKind)> {
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
    let network_policy = source.network_policy.clone();
    let client = http_client_for(source);
    let (writer, reader) =
        StreamingByteReader::with_global_budget(config.max_buffered_bytes, global_byte_budget)?;
    let cancellation = CancellationToken::new();
    let byte_budget = writer.global_byte_budget();
    let (media_url, playlist) = {
        let http = HlsHttp {
            client: &client,
            headers: &headers,
            network_policy: &network_policy,
            config: &config,
            cancellation: &cancellation,
            byte_budget: &byte_budget,
        };
        resolve_media_playlist(&http, url).await?
    };
    let playlist_kind = if playlist.end_list {
        HlsPlaylistKind::Vod
    } else {
        HlsPlaylistKind::Live
    };
    let worker_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        let result = run_http_hls_stream(
            HlsStreamSpec {
                media_url,
                playlist,
                headers,
                network_policy,
                client,
                config,
            },
            writer.clone(),
            worker_cancellation.clone(),
        )
        .await;
        if let Err(error) = &result {
            writer.fail(error.to_string(), &worker_cancellation).await;
        }
        result
    });
    Ok((
        HttpLiveStream {
            reader,
            cancellation,
            task,
        },
        playlist_kind,
    ))
}

async fn run_http_hls_stream(
    spec: HlsStreamSpec,
    writer: StreamingByteWriter,
    cancellation: CancellationToken,
) -> Result<HttpLiveStreamReport> {
    let HlsStreamSpec {
        mut media_url,
        mut playlist,
        headers,
        network_policy,
        client,
        config,
    } = spec;
    let byte_budget = writer.global_byte_budget();
    let http = HlsHttp {
        client: &client,
        headers: &headers,
        network_policy: &network_policy,
        config: &config,
        cancellation: &cancellation,
        byte_budget: &byte_budget,
    };
    let mut report = HttpLiveStreamReport::default();
    let mut next_sequence = initial_sequence(&playlist);
    let mut segment_kind = None;
    let mut initialization = None;
    let mut key_cache = None;
    let mut delivered_media = false;

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
            if segment_is_gap(segment) {
                metrics::counter!("music_stream.source.hls_gap_segments").increment(1);
                next_sequence = next_sequence.saturating_add(1);
                continue;
            }
            if segment.discontinuity && delivered_media {
                metrics::counter!("music_stream.source.hls_discontinuities").increment(1);
            }
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
                    None if delivered_media => {
                        return Err(MusicStreamError::Unsupported(
                            "HLS cannot introduce an initialization map midstream".to_owned(),
                        ));
                    }
                    None => {
                        let encryption = resolve_aes128_encryption(
                            &http,
                            &media_url,
                            segment.key.as_ref(),
                            next_sequence,
                            true,
                            &mut key_cache,
                        )
                        .await?;
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
                        let (mut bytes, permit) = response.into_parts();
                        decrypt_hls_bytes(&mut bytes, encryption)?;
                        validate_fmp4_initialization(&bytes)?;
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
            let encryption = resolve_aes128_encryption(
                &http,
                &media_url,
                segment.key.as_ref(),
                next_sequence,
                false,
                &mut key_cache,
            )
            .await?;
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
            let (mut bytes, global_permit) = response.into_parts();
            decrypt_hls_bytes(&mut bytes, encryption)?;
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
            delivered_media = true;
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

#[derive(Clone, Copy)]
struct AudioGroupProfile {
    codec_rank: AudioCodecRank,
    bandwidth: u64,
}

fn select_variant(master: &m3u8_rs::MasterPlaylist) -> Option<&str> {
    // EXT-X-MEDIA has no CODECS or BANDWIDTH, so derive a compact profile for each
    // referenced audio group in one pass over the variants.
    let mut audio_groups = HashMap::<&str, AudioGroupProfile>::new();
    for variant in &master.variants {
        let Some(group_id) = variant.audio.as_deref() else {
            continue;
        };
        let codec = codec_profile(variant.codecs.as_deref()).audio_rank;
        audio_groups
            .entry(group_id)
            .and_modify(|profile| {
                profile.codec_rank = profile.codec_rank.min(codec);
                profile.bandwidth = profile.bandwidth.min(variant.bandwidth);
            })
            .or_insert(AudioGroupProfile {
                codec_rank: codec,
                bandwidth: variant.bandwidth,
            });
    }
    master
        .alternatives
        .iter()
        .filter(|media| {
            media.media_type == AlternativeMediaType::Audio
                && media.uri.is_some()
                && (audio_groups.is_empty() || audio_groups.contains_key(media.group_id.as_str()))
        })
        .filter(|media| {
            audio_groups
                .get(media.group_id.as_str())
                .is_none_or(|profile| profile.codec_rank != AudioCodecRank::Unsupported)
        })
        .min_by_key(|media| {
            let profile = audio_groups
                .get(media.group_id.as_str())
                .copied()
                .unwrap_or(AudioGroupProfile {
                    codec_rank: AudioCodecRank::Unknown,
                    bandwidth: u64::MAX,
                });
            (
                profile.codec_rank,
                !media.default,
                !media.autoselect,
                channel_rank(media.channels.as_deref()),
                profile.bandwidth,
            )
        })
        .and_then(|media| media.uri.as_deref())
        .or_else(|| {
            master
                .variants
                .iter()
                .filter(|variant| !variant.is_i_frame)
                .min_by_key(|variant| {
                    let codec = codec_profile(variant.codecs.as_deref());
                    (
                        codec.audio_rank == AudioCodecRank::Unsupported,
                        variant.resolution.is_some() || variant.video.is_some() || codec.has_video,
                        codec.audio_rank,
                        variant.bandwidth,
                    )
                })
                .map(|variant| variant.uri.as_str())
        })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum AudioCodecRank {
    Verified,
    Unknown,
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CodecProfile {
    audio_rank: AudioCodecRank,
    has_video: bool,
}

fn codec_profile(codecs: Option<&str>) -> CodecProfile {
    let Some(codecs) = codecs else {
        return CodecProfile {
            audio_rank: AudioCodecRank::Unknown,
            has_video: false,
        };
    };
    let codecs = codecs.to_ascii_lowercase();
    let mut audio_rank = AudioCodecRank::Unknown;
    let mut has_video = false;
    for codec in codecs.split(',').map(str::trim) {
        if is_verified_audio_codec(codec) {
            audio_rank = AudioCodecRank::Verified;
        } else if is_unsupported_audio_codec(codec) && audio_rank != AudioCodecRank::Verified {
            audio_rank = AudioCodecRank::Unsupported;
        }
        has_video |= [
            "avc1", "avc3", "hvc1", "hev1", "dvh1", "dvhe", "vp09", "av01",
        ]
        .iter()
        .any(|family| codec_family(codec, family));
    }
    CodecProfile {
        audio_rank,
        has_video,
    }
}

fn is_verified_audio_codec(codec: &str) -> bool {
    matches!(
        codec,
        "mp3"
            | "mp4a.40.2"
            | "mp4a.40.5"
            | "mp4a.40.29"
            | "mp4a.40.34"
            | "mp4a.66"
            | "mp4a.67"
            | "mp4a.68"
            | "mp4a.69"
            | "mp4a.6b"
    )
}

fn is_unsupported_audio_codec(codec: &str) -> bool {
    ["ac-3", "ec-3", "ac-4", "dtsc", "dtse", "dtsh", "dtsl"]
        .iter()
        .any(|family| codec_family(codec, family))
        || matches!(
            codec,
            "mp4a.a5" | "mp4a.a6" | "mp4a.a9" | "mp4a.aa" | "mp4a.ab" | "mp4a.ac"
        )
}

fn codec_family(codec: &str, family: &str) -> bool {
    codec == family
        || codec
            .strip_prefix(family)
            .is_some_and(|suffix| suffix.starts_with('.'))
}

fn channel_rank(channels: Option<&str>) -> u16 {
    channels
        .and_then(|channels| channels.split('/').next())
        .and_then(|channels| channels.parse::<u16>().ok())
        .map_or(1, |channels| u16::from(channels > 2))
}

fn initial_sequence(playlist: &MediaPlaylist) -> u64 {
    if playlist.end_list {
        return playlist.media_sequence;
    }
    playlist
        .media_sequence
        .saturating_add(playlist.segments.len().saturating_sub(LIVE_EDGE_SEGMENTS) as u64)
}

fn segment_is_gap(segment: &m3u8_rs::MediaSegment) -> bool {
    segment
        .unknown_tags
        .iter()
        .any(|tag| tag.tag.eq_ignore_ascii_case("X-GAP"))
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

struct Aes128KeyCache {
    url: reqwest::Url,
    key: [u8; AES128_KEY_BYTES],
}

#[derive(Clone, Copy)]
struct Aes128Encryption {
    key: [u8; AES128_KEY_BYTES],
    iv: [u8; AES128_KEY_BYTES],
}

async fn resolve_aes128_encryption(
    http: &HlsHttp<'_>,
    media_url: &reqwest::Url,
    key: Option<&Key>,
    media_sequence: u64,
    initialization: bool,
    cache: &mut Option<Aes128KeyCache>,
) -> Result<Option<Aes128Encryption>> {
    let Some(key) = key else {
        *cache = None;
        return Ok(None);
    };
    match key.method {
        KeyMethod::None => {
            *cache = None;
            return Ok(None);
        }
        KeyMethod::AES128 => {}
        KeyMethod::SampleAES | KeyMethod::Other(_) => {
            return Err(MusicStreamError::Unsupported(
                "HLS encryption method is not supported".to_owned(),
            ));
        }
    }
    if key
        .keyformat
        .as_deref()
        .is_some_and(|format| !format.eq_ignore_ascii_case("identity"))
        || key
            .keyformatversions
            .as_deref()
            .is_some_and(|versions| versions.split('/').any(|version| version.trim() != "1"))
    {
        return Err(MusicStreamError::Unsupported(
            "HLS AES-128 requires the identity key format version 1".to_owned(),
        ));
    }
    let iv = match key.iv.as_deref() {
        Some(iv) => parse_aes128_iv(iv)?,
        None if initialization => {
            return Err(MusicStreamError::InvalidSource(
                "encrypted HLS initialization map requires an explicit IV".to_owned(),
            ));
        }
        None => sequence_iv(media_sequence),
    };
    let key_uri = key.uri.as_deref().ok_or_else(|| {
        MusicStreamError::InvalidSource("HLS AES-128 key requires a URI".to_owned())
    })?;
    let key_url = media_url.join(key_uri).map_err(|error| {
        MusicStreamError::InvalidSource(format!("invalid HLS key URL: {error}"))
    })?;
    if cache.as_ref().is_none_or(|cached| cached.url != key_url) {
        let response = http
            .fetch(key_url.clone(), http.config.idle_timeout, AES128_KEY_BYTES)
            .await?;
        if response.bytes.len() != AES128_KEY_BYTES {
            return Err(MusicStreamError::InvalidSource(
                "HLS AES-128 key must contain exactly 16 bytes".to_owned(),
            ));
        }
        let mut bytes = [0_u8; AES128_KEY_BYTES];
        bytes.copy_from_slice(&response.bytes);
        *cache = Some(Aes128KeyCache {
            url: key_url,
            key: bytes,
        });
        metrics::counter!("music_stream.source.hls_keys").increment(1);
    }
    let cached = cache
        .as_ref()
        .ok_or_else(|| MusicStreamError::Internal("HLS AES-128 key cache is empty".to_owned()))?;
    Ok(Some(Aes128Encryption {
        key: cached.key,
        iv,
    }))
}

fn parse_aes128_iv(value: &str) -> Result<[u8; AES128_KEY_BYTES]> {
    let digits = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .ok_or_else(|| {
            MusicStreamError::InvalidSource("HLS AES-128 IV must start with 0x".to_owned())
        })?;
    if digits.is_empty()
        || digits.len() > 32
        || !digits.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(MusicStreamError::InvalidSource(
            "HLS AES-128 IV must contain 1 to 32 hexadecimal digits".to_owned(),
        ));
    }
    u128::from_str_radix(digits, 16)
        .map(u128::to_be_bytes)
        .map_err(|_| MusicStreamError::InvalidSource("invalid HLS AES-128 IV".to_owned()))
}

fn sequence_iv(media_sequence: u64) -> [u8; AES128_KEY_BYTES] {
    let mut iv = [0_u8; AES128_KEY_BYTES];
    iv[8..].copy_from_slice(&media_sequence.to_be_bytes());
    iv
}

fn decrypt_hls_bytes(bytes: &mut Vec<u8>, encryption: Option<Aes128Encryption>) -> Result<()> {
    let Some(encryption) = encryption else {
        return Ok(());
    };
    if bytes.is_empty() || !bytes.len().is_multiple_of(AES128_KEY_BYTES) {
        return Err(MusicStreamError::DecodeError(
            "HLS AES-128 ciphertext is not block-aligned".to_owned(),
        ));
    }
    let plaintext_len =
        cbc::Decryptor::<aes::Aes128>::new(&encryption.key.into(), &encryption.iv.into())
            .decrypt_padded::<Pkcs7>(bytes)
            .map_err(|_| {
                MusicStreamError::DecodeError("HLS AES-128 padding is invalid".to_owned())
            })?
            .len();
    if plaintext_len == 0 {
        return Err(MusicStreamError::DecodeError(
            "HLS AES-128 segment decrypted to an empty body".to_owned(),
        ));
    }
    bytes.truncate(plaintext_len);
    Ok(())
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
    network_policy: &'a NetworkPolicy,
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
        validate_network_url(self.network_policy, &url)?;
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

    use aes::cipher::BlockModeEncrypt as _;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn variant_selection_avoids_unsupported_default_audio_rendition() {
        let master = m3u8_rs::parse_playlist_res(
            b"#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"surround\",NAME=\"surround\",DEFAULT=YES,AUTOSELECT=YES,CHANNELS=\"6\",URI=\"audio/ac3.m3u8\"\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"stereo\",NAME=\"stereo\",DEFAULT=NO,AUTOSELECT=YES,CHANNELS=\"2\",URI=\"audio/aac.m3u8\"\n#EXT-X-STREAM-INF:BANDWIDTH=900000,CODECS=\"avc1.4d401e,ac-3\",AUDIO=\"surround\"\nvideo/ac3.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=1000000,CODECS=\"avc1.4d401e,mp4a.40.2\",AUDIO=\"stereo\"\nvideo/aac.m3u8\n",
        )
        .expect("playlist");
        let Playlist::MasterPlaylist(master) = master else {
            panic!("master playlist");
        };
        assert_eq!(select_variant(&master), Some("audio/aac.m3u8"));
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
    }

    #[test]
    fn codec_profiles_distinguish_supported_and_unsupported_mp4a_types() {
        assert_eq!(
            codec_profile(Some("avc1.4D401E,mp4a.40.2")),
            CodecProfile {
                audio_rank: AudioCodecRank::Verified,
                has_video: true,
            }
        );
        assert_eq!(
            codec_profile(Some("mp4a.A6")),
            CodecProfile {
                audio_rank: AudioCodecRank::Unsupported,
                has_video: false,
            }
        );
        assert_eq!(
            codec_profile(Some("opus")),
            CodecProfile {
                audio_rank: AudioCodecRank::Unknown,
                has_video: false,
            }
        );
    }

    #[test]
    fn playlist_gap_tag_is_recognized() {
        let Playlist::MediaPlaylist(playlist) = m3u8_rs::parse_playlist_res(
            b"#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1,\nfirst.ts\n#EXTINF:1,\n#EXT-X-GAP\nmissing.ts\n#EXT-X-ENDLIST\n",
        )
        .expect("playlist")
        else {
            panic!("media playlist");
        };

        assert!(!segment_is_gap(&playlist.segments[0]));
        assert!(segment_is_gap(&playlist.segments[1]));
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

    #[test]
    fn aes128_iv_and_in_place_decryption_follow_hls_rules() {
        let key = [0x42; AES128_KEY_BYTES];
        let explicit = parse_aes128_iv("0x1234").expect("explicit IV");
        assert_eq!(explicit[14..], [0x12, 0x34]);
        assert_eq!(sequence_iv(7)[8..], 7_u64.to_be_bytes());

        let plaintext = b"bounded encrypted HLS segment";
        let mut ciphertext = encrypt_test_aes128(plaintext, key, explicit);
        decrypt_hls_bytes(
            &mut ciphertext,
            Some(Aes128Encryption { key, iv: explicit }),
        )
        .expect("decrypt");
        assert_eq!(ciphertext, plaintext);
    }

    #[tokio::test]
    async fn public_only_policy_is_applied_to_every_hls_request() {
        let client = reqwest::Client::new();
        let headers = BTreeMap::new();
        let network_policy = NetworkPolicy::PublicOnly;
        let config = HttpLiveStreamConfig::default();
        let cancellation = CancellationToken::new();
        let byte_budget = LiveByteBudget::new(1024).expect("budget");
        let http = HlsHttp {
            client: &client,
            headers: &headers,
            network_policy: &network_policy,
            config: &config,
            cancellation: &cancellation,
            byte_budget: &byte_budget,
        };

        for url in [
            "http://example.com/segment.ts",
            "https://127.0.0.1/segment.ts",
            "https://example.com:8443/key.bin",
            "file:///etc/passwd",
        ] {
            let error = http
                .fetch(
                    reqwest::Url::parse(url).expect("URL fixture"),
                    Duration::from_millis(10),
                    1024,
                )
                .await
                .expect_err("unsafe HLS child URL must be rejected");
            assert_eq!(error.code(), crate::ErrorCode::InvalidSource, "{url}");
        }
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
                assert!(
                    request
                        .to_ascii_lowercase()
                        .contains("x-provider-token: trusted")
                );
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
            attempt_id: None,
            id: "hls-test".to_owned(),
            kind: crate::model::TrackKind::Url,
            url: Some(format!("http://{address}/playlist.m3u8")),
            path: None,
            format_hint: None,
            seekable: Some(true),
            headers: BTreeMap::from([("x-provider-token".to_owned(), "trusted".to_owned())]),
            network_policy: crate::model::NetworkPolicy::Provider,
        };
        let (stream, playlist_kind) = spawn_http_hls_stream(
            &source,
            HttpLiveStreamConfig {
                max_buffered_bytes: 32,
                ..HttpLiveStreamConfig::default()
            },
            LiveByteBudget::new(1024).expect("budget"),
        )
        .await
        .expect("HLS stream");
        assert_eq!(playlist_kind, HlsPlaylistKind::Vod);
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
        let key = [0x11; AES128_KEY_BYTES];
        let iv = [0x12; AES128_KEY_BYTES];
        let encrypted_initialization = encrypt_test_aes128(&initialization, key, iv);
        let encrypted_fragment = encrypt_test_aes128(&fragment, key, iv);
        let mut resource = encrypted_initialization.clone();
        resource.extend_from_slice(&encrypted_fragment);
        resource.extend_from_slice(&encrypted_fragment);
        let playlist = format!(
            "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:1\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\",IV=0x12121212121212121212121212121212\n#EXT-X-MAP:URI=\"media.mp4\",BYTERANGE=\"{}@0\"\n#EXTINF:1,\n#EXT-X-BYTERANGE:{}@{}\nmedia.mp4\n#EXTINF:1,\n#EXT-X-BYTERANGE:{}\nmedia.mp4\n#EXT-X-ENDLIST\n",
            encrypted_initialization.len(),
            encrypted_fragment.len(),
            encrypted_initialization.len(),
            encrypted_fragment.len(),
        )
        .into_bytes();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let encrypted_bytes = resource.len();
        let mut expected = initialization;
        expected.extend_from_slice(&fragment);
        expected.extend_from_slice(&fragment);
        let server = tokio::spawn(async move {
            for _ in 0..5 {
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
                } else if request.starts_with("GET /key.bin ") {
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        key.len()
                    );
                    socket.write_all(headers.as_bytes()).await.expect("headers");
                    socket.write_all(&key).await.expect("key");
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
            attempt_id: None,
            id: "fmp4-hls-test".to_owned(),
            kind: crate::model::TrackKind::Url,
            url: Some(format!("http://{address}/playlist.m3u8")),
            path: None,
            format_hint: None,
            seekable: Some(true),
            headers: BTreeMap::new(),
            network_policy: crate::model::NetworkPolicy::Provider,
        };
        let (stream, playlist_kind) = spawn_http_hls_stream(
            &source,
            HttpLiveStreamConfig {
                max_buffered_bytes: 32,
                ..HttpLiveStreamConfig::default()
            },
            LiveByteBudget::new(1024).expect("budget"),
        )
        .await
        .expect("HLS stream");
        assert_eq!(playlist_kind, HlsPlaylistKind::Vod);
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
        assert_eq!(report.bytes_read, encrypted_bytes as u64);
        assert_eq!(reader_task.await.expect("reader task"), expected);
        server.await.expect("server");
    }

    #[tokio::test]
    async fn hls_stream_decrypts_inherited_aes128_key_with_sequence_ivs() {
        let elementary = b"\xff\xf1\x50\x80\x00\x1f\xfcAAC";
        let segment = make_test_ts(elementary);
        let key = [0x24; AES128_KEY_BYTES];
        let first = encrypt_test_aes128(&segment, key, sequence_iv(7));
        let second = encrypt_test_aes128(&segment, key, sequence_iv(9));
        let encrypted_bytes = first.len() + second.len();
        let playlist = b"#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:7\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n#EXTINF:1,\nfirst.ts\n#EXTINF:1,\n#EXT-X-GAP\nmissing.ts\n#EXT-X-DISCONTINUITY\n#EXTINF:1,\nsecond.ts\n#EXT-X-ENDLIST\n";
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            for _ in 0..4 {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut request = [0_u8; 1024];
                let read = socket.read(&mut request).await.expect("request");
                let request = String::from_utf8_lossy(&request[..read]);
                let body: &[u8] = if request.starts_with("GET /key.bin ") {
                    &key
                } else if request.starts_with("GET /first.ts ") {
                    &first
                } else if request.starts_with("GET /second.ts ") {
                    &second
                } else {
                    assert!(request.starts_with("GET /playlist.m3u8 "));
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
            attempt_id: None,
            id: "aes128-hls-test".to_owned(),
            kind: crate::model::TrackKind::Url,
            url: Some(format!("http://{address}/playlist.m3u8")),
            path: None,
            format_hint: None,
            seekable: Some(true),
            headers: BTreeMap::new(),
            network_policy: crate::model::NetworkPolicy::Provider,
        };
        let (stream, playlist_kind) = spawn_http_hls_stream(
            &source,
            HttpLiveStreamConfig {
                max_buffered_bytes: 32,
                ..HttpLiveStreamConfig::default()
            },
            LiveByteBudget::new(1024).expect("budget"),
        )
        .await
        .expect("HLS stream");
        assert_eq!(playlist_kind, HlsPlaylistKind::Vod);
        let HttpLiveStream {
            mut reader, task, ..
        } = stream;
        let reader_task = tokio::task::spawn_blocking(move || {
            let mut output = Vec::new();
            reader.read_to_end(&mut output).expect("read HLS bytes");
            output
        });

        let report = task.await.expect("HLS task").expect("HLS result");
        let mut expected = elementary.to_vec();
        expected.extend_from_slice(elementary);
        assert!(report.completed);
        assert_eq!(report.bytes_read, encrypted_bytes as u64);
        assert_eq!(reader_task.await.expect("reader task"), expected);
        server.await.expect("server");
    }

    fn encrypt_test_aes128(
        plaintext: &[u8],
        key: [u8; AES128_KEY_BYTES],
        iv: [u8; AES128_KEY_BYTES],
    ) -> Vec<u8> {
        let padded_len = plaintext.len().div_ceil(AES128_KEY_BYTES) * AES128_KEY_BYTES
            + usize::from(plaintext.len().is_multiple_of(AES128_KEY_BYTES)) * AES128_KEY_BYTES;
        let mut output = vec![0_u8; padded_len];
        output[..plaintext.len()].copy_from_slice(plaintext);
        let encrypted_len = cbc::Encryptor::<aes::Aes128>::new(&key.into(), &iv.into())
            .encrypt_padded::<Pkcs7>(&mut output, plaintext.len())
            .expect("encrypt test segment")
            .len();
        output.truncate(encrypted_len);
        output
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
