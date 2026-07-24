use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use music_stream::{
    NetworkPolicy, RtpTransportConfig, SourceResolverConfig, StreamCommand, StreamRuntime,
    StreamRuntimeConfig, TrackKind, TrackSource, VolumeLevel,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::UdpSocket;

fn write_wav(path: &Path, seconds: f32) {
    let sample_rate = 48_000_u32;
    let channels = 2_u16;
    let samples = (sample_rate as f32 * seconds) as usize;
    let data_bytes = samples * usize::from(channels) * 2;
    let mut bytes = Vec::with_capacity(44 + data_bytes);
    bytes.extend_from_slice(b"RIFF");
    bytes.extend_from_slice(&(36_u32 + data_bytes as u32).to_le_bytes());
    bytes.extend_from_slice(b"WAVEfmt ");
    bytes.extend_from_slice(&16_u32.to_le_bytes());
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&channels.to_le_bytes());
    bytes.extend_from_slice(&sample_rate.to_le_bytes());
    bytes.extend_from_slice(&(sample_rate * u32::from(channels) * 2).to_le_bytes());
    bytes.extend_from_slice(&(channels * 2).to_le_bytes());
    bytes.extend_from_slice(&16_u16.to_le_bytes());
    bytes.extend_from_slice(b"data");
    bytes.extend_from_slice(&(data_bytes as u32).to_le_bytes());
    for index in 0..samples {
        let value = (((index as f32 * 440.0 * std::f32::consts::TAU / sample_rate as f32).sin())
            * 12_000.0) as i16;
        for _ in 0..channels {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
    }
    std::fs::write(path, bytes).expect("write wav");
}

fn file_track(id: &str, path: &Path) -> TrackSource {
    TrackSource {
        attempt_id: format!("attempt-{id}"),
        id: id.to_owned(),
        kind: TrackKind::File,
        url: None,
        path: Some(path.display().to_string()),
        format_hint: None,
        seekable: Some(true),
        headers: Default::default(),
        network_policy: NetworkPolicy::Provider,
    }
}

async fn runtime_for(
    stream_id: &str,
    current: TrackSource,
    next: Option<TrackSource>,
    receiver: &UdpSocket,
    ssrc: u32,
) -> StreamRuntime {
    let port = receiver.local_addr().expect("receiver address").port();
    let mut transport = RtpTransportConfig::new("127.0.0.1", port, ssrc);
    transport.local_ip = "127.0.0.1".to_owned();
    transport.payload_type = 111;
    let config = StreamRuntimeConfig::new(transport, SourceResolverConfig::default());
    let mut current = current;
    current.attempt_id = format!("{stream_id}:current");
    let current_plan = current.clone();
    let runtime = StreamRuntime::start(
        stream_id.to_owned(),
        current,
        config,
        VolumeLevel::default(),
        Default::default(),
    )
    .await
    .expect("runtime");
    if let Some(mut next) = next {
        next.attempt_id = format!("{stream_id}:next");
        runtime
            .command(StreamCommand::ReconcilePlan {
                version: 1,
                current: Some(current_plan),
                next: Some(next),
            })
            .await
            .expect("initial desired plan");
    }
    runtime
}

async fn recv_rtp(socket: &UdpSocket) -> Vec<u8> {
    try_recv_rtp(socket, Duration::from_secs(2))
        .await
        .expect("RTP timeout")
}

async fn try_recv_rtp(socket: &UdpSocket, timeout: Duration) -> Option<Vec<u8>> {
    let mut bytes = vec![0_u8; 2_000];
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let len = tokio::time::timeout_at(deadline, socket.recv(&mut bytes))
            .await
            .ok()?
            .ok()?;
        if len >= 12 && bytes[1] & 0x7f != 72 && bytes[1] & 0x7f != 73 {
            bytes.truncate(len);
            return Some(bytes);
        }
    }
}

fn sequence(packet: &[u8]) -> u16 {
    u16::from_be_bytes([packet[2], packet[3]])
}

fn timestamp(packet: &[u8]) -> u32 {
    u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_decodes_encodes_and_sends_paced_rtp() {
    let wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("wav");
    write_wav(wav.path(), 0.4);
    let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
    let runtime = runtime_for("basic", file_track("a", wav.path()), None, &receiver, 7).await;

    let first = recv_rtp(&receiver).await;
    let second = recv_rtp(&receiver).await;
    assert_eq!(sequence(&second), sequence(&first).wrapping_add(1));
    assert_eq!(timestamp(&second), timestamp(&first).wrapping_add(960));

    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switch_keeps_one_monotonic_rtp_session() {
    let first_wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("first");
    let second_wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("second");
    write_wav(first_wav.path(), 1.0);
    write_wav(second_wav.path(), 1.0);
    let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
    let runtime = runtime_for(
        "switch",
        file_track("a", first_wav.path()),
        None,
        &receiver,
        8,
    )
    .await;

    let before = recv_rtp(&receiver).await;
    let mut switched = file_track("b", second_wav.path());
    switched.attempt_id = "switch:b".to_owned();
    runtime
        .command(StreamCommand::ReconcilePlan {
            version: 1,
            current: Some(switched),
            next: None,
        })
        .await
        .expect("switch");
    let after = loop {
        let packet = recv_rtp(&receiver).await;
        if packet[1] & 0x80 != 0 {
            break packet;
        }
    };
    assert!(sequence(&after).wrapping_sub(sequence(&before)) > 0);
    assert!(timestamp(&after).wrapping_sub(timestamp(&before)) >= 960);

    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_stops_playout_without_destroying_the_producer() {
    let wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("wav");
    write_wav(wav.path(), 1.0);
    let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
    let runtime = runtime_for("pause", file_track("a", wav.path()), None, &receiver, 9).await;
    let _ = recv_rtp(&receiver).await;

    runtime.command(StreamCommand::Pause).await.expect("pause");
    let mut scratch = [0_u8; 2_000];
    while tokio::time::timeout(Duration::from_millis(5), receiver.recv(&mut scratch))
        .await
        .is_ok()
    {}
    assert!(
        tokio::time::timeout(Duration::from_millis(80), receiver.recv(&mut scratch))
            .await
            .is_err()
    );

    runtime.command(StreamCommand::Play).await.expect("resume");
    let _ = recv_rtp(&receiver).await;
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preloaded_next_promotes_without_resetting_rtp_clock() {
    let first_wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("first");
    let second_wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("second");
    write_wav(first_wav.path(), 0.14);
    write_wav(second_wav.path(), 0.5);
    let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
    let runtime = runtime_for(
        "promotion",
        file_track("a", first_wav.path()),
        Some(file_track("b", second_wav.path())),
        &receiver,
        10,
    )
    .await;

    let mut previous = recv_rtp(&receiver).await;
    let promoted = loop {
        let packet = recv_rtp(&receiver).await;
        if packet[1] & 0x80 != 0 && sequence(&packet) != sequence(&previous) {
            break packet;
        }
        previous = packet;
    };
    assert_eq!(sequence(&promoted), sequence(&previous).wrapping_add(1));
    assert_eq!(timestamp(&promoted), timestamp(&previous).wrapping_add(960));
    let status = runtime.snapshot().await.status;
    assert_eq!(status.current.expect("current").id, "b");

    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preloaded_progressive_url_promotes_and_reaches_playing() {
    let first_wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("first");
    let second_wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("second");
    write_wav(first_wav.path(), 0.14);
    write_wav(second_wav.path(), 0.5);
    let body = std::fs::read(second_wav.path()).expect("next body");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("HTTP bind");
    let address = listener.local_addr().expect("HTTP address");
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut request = [0_u8; 2_048];
        let _ = stream.read(&mut request).await;
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: audio/wav\r\n\r\n",
            body.len()
        );
        stream.write_all(header.as_bytes()).await.expect("header");
        stream.write_all(&body).await.expect("body");
    });

    let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("RTP bind");
    let runtime = runtime_for(
        "url-promotion",
        file_track("a", first_wav.path()),
        Some(TrackSource {
            attempt_id: "attempt-b".to_owned(),
            id: "b".to_owned(),
            kind: TrackKind::Url,
            url: Some(format!("http://{address}/next.wav")),
            path: None,
            format_hint: Some("wav".to_owned()),
            seekable: Some(true),
            headers: Default::default(),
            network_policy: NetworkPolicy::Provider,
        }),
        &receiver,
        11,
    )
    .await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let status = runtime.snapshot().await.status;
        if status.current.as_ref().is_some_and(|track| track.id == "b")
            && status.play_state == music_stream::PlayState::Playing
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "stuck status: {status:?}"
        );
        let _ = try_recv_rtp(&receiver, Duration::from_millis(20)).await;
    }

    runtime.shutdown().await.expect("shutdown");
    server.await.expect("server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_live_decode_never_blocks_rtp_deadlines() {
    let wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("wav");
    write_wav(wav.path(), 1.0);
    let body = std::fs::read(wav.path()).expect("wav body");
    let initial_bytes = 44 + 48_000 * 2 * 2 / 2;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("HTTP bind");
    let address = listener.local_addr().expect("HTTP address");
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut request = [0_u8; 2_048];
        let _ = stream.read(&mut request).await;
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: audio/wav\r\n\r\n",
            body.len()
        );
        stream.write_all(header.as_bytes()).await.expect("header");
        stream
            .write_all(&body[..initial_bytes])
            .await
            .expect("initial live body");
        stream.flush().await.expect("flush");
        tokio::time::sleep(Duration::from_millis(600)).await;
    });

    let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("RTP bind");
    let live = TrackSource {
        attempt_id: "attempt-stalled-live".to_owned(),
        id: "stalled-live".to_owned(),
        kind: TrackKind::Live,
        url: Some(format!("http://{address}/live.wav")),
        path: None,
        format_hint: None,
        seekable: Some(false),
        headers: Default::default(),
        network_policy: NetworkPolicy::Provider,
    };
    let runtime = runtime_for("stalled-live", live, None, &receiver, 11).await;
    let started = tokio::time::Instant::now();
    let mut previous = recv_rtp(&receiver).await;
    for _ in 0..7 {
        let packet = recv_rtp(&receiver).await;
        assert_eq!(sequence(&packet), sequence(&previous).wrapping_add(1));
        previous = packet;
    }
    assert!(started.elapsed() < Duration::from_millis(350));

    runtime.shutdown().await.expect("shutdown");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_mux_rtcp_uses_the_dedicated_remote_port() {
    let wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("wav");
    write_wav(wav.path(), 0.5);
    let rtp = UdpSocket::bind("127.0.0.1:0").await.expect("RTP bind");
    let rtcp = UdpSocket::bind("127.0.0.1:0").await.expect("RTCP bind");
    let mut transport = RtpTransportConfig::new(
        "127.0.0.1",
        rtp.local_addr().expect("RTP address").port(),
        12,
    );
    transport.local_ip = "127.0.0.1".to_owned();
    transport.rtcp_mux = false;
    transport.remote_rtcp_port = Some(rtcp.local_addr().expect("RTCP address").port());
    let mut config = StreamRuntimeConfig::new(transport, SourceResolverConfig::default());
    config.rtcp_interval = Duration::from_millis(40);
    let runtime = StreamRuntime::start(
        "rtcp-non-mux".to_owned(),
        file_track("a", wav.path()),
        config,
        VolumeLevel::default(),
        Default::default(),
    )
    .await
    .expect("runtime");
    let _ = recv_rtp(&rtp).await;

    let mut bytes = [0_u8; 1_500];
    let len = tokio::time::timeout(Duration::from_secs(1), rtcp.recv(&mut bytes))
        .await
        .expect("RTCP timeout")
        .expect("RTCP receive");
    assert!(len >= 8);
    assert_eq!(bytes[1], 200);

    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_bitrate_and_mtu_configure_the_opus_producer() {
    let wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("wav");
    write_wav(wav.path(), 0.4);
    let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("RTP bind");
    let mut transport = RtpTransportConfig::new(
        "127.0.0.1",
        receiver.local_addr().expect("address").port(),
        13,
    );
    transport.local_ip = "127.0.0.1".to_owned();
    transport.mtu = 64;
    transport.opus_bitrate_bps = Some(6_000);
    let runtime = StreamRuntime::start(
        "small-mtu".to_owned(),
        file_track("a", wav.path()),
        StreamRuntimeConfig::new(transport, SourceResolverConfig::default()),
        VolumeLevel::default(),
        Default::default(),
    )
    .await
    .expect("runtime");

    let packet = recv_rtp(&receiver).await;
    assert!(packet.len() <= 64);
    assert!(packet.len() > 12);
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_currents_advance_while_next_tracks_preload() {
    let current_wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("current wav");
    let next_wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("next wav");
    write_wav(current_wav.path(), 1.0);
    write_wav(next_wav.path(), 1.0);

    let mut streams = Vec::new();
    for index in 0..4_u32 {
        let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("RTP bind");
        let runtime = runtime_for(
            &format!("concurrent-{index}"),
            file_track(&format!("current-{index}"), current_wav.path()),
            Some(file_track(&format!("next-{index}"), next_wav.path())),
            &receiver,
            100 + index,
        )
        .await;
        streams.push((runtime, receiver));
    }

    for (_, receiver) in &streams {
        let first = recv_rtp(receiver).await;
        let second = recv_rtp(receiver).await;
        assert_eq!(sequence(&second), sequence(&first).wrapping_add(1));
        assert_eq!(timestamp(&second), timestamp(&first).wrapping_add(960));
    }

    for (runtime, _) in streams {
        runtime.shutdown().await.expect("shutdown");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_shorter_than_prebuffer_is_still_played() {
    let wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("wav");
    write_wav(wav.path(), 0.04);
    let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("RTP bind");
    let runtime = runtime_for(
        "short",
        file_track("short", wav.path()),
        None,
        &receiver,
        200,
    )
    .await;

    let first = recv_rtp(&receiver).await;
    let second = recv_rtp(&receiver).await;
    assert_eq!(sequence(&second), sequence(&first).wrapping_add(1));
    assert_eq!(timestamp(&second), timestamp(&first).wrapping_add(960));

    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_during_url_download_excludes_paused_time_from_io_timeout() {
    let wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("wav");
    write_wav(wav.path(), 0.4);
    let body = std::fs::read(wav.path()).expect("wav body");
    let split = (44 + 960 * 2 * 2).min(body.len());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("HTTP bind");
    let address = listener.local_addr().expect("HTTP address");
    let (initial_sent, initial_received) = tokio::sync::oneshot::channel();
    let (continue_send, continue_receive) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut request = [0_u8; 2_048];
        let _ = stream.read(&mut request).await;
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: audio/wav\r\n\r\n",
            body.len()
        );
        stream.write_all(header.as_bytes()).await.expect("header");
        stream
            .write_all(&body[..split])
            .await
            .expect("initial body");
        stream.flush().await.expect("flush");
        let _ = initial_sent.send(());
        let _ = continue_receive.await;
        stream
            .write_all(&body[split..])
            .await
            .expect("remaining body");
    });

    let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("RTP bind");
    let mut transport = RtpTransportConfig::new(
        "127.0.0.1",
        receiver.local_addr().expect("RTP address").port(),
        201,
    );
    transport.local_ip = "127.0.0.1".to_owned();
    let mut source_config = SourceResolverConfig::default();
    source_config.http.io_timeout = Duration::from_secs(1);
    source_config.http.max_retries = 0;
    let runtime = StreamRuntime::start(
        "paused-download".to_owned(),
        TrackSource {
            attempt_id: "attempt-paused-url".to_owned(),
            id: "paused-url".to_owned(),
            kind: TrackKind::Url,
            url: Some(format!("http://{address}/opaque")),
            path: None,
            format_hint: Some("wav".to_owned()),
            seekable: Some(true),
            headers: Default::default(),
            network_policy: NetworkPolicy::Provider,
        },
        StreamRuntimeConfig::new(transport, source_config),
        VolumeLevel::default(),
        Default::default(),
    )
    .await
    .expect("runtime");

    initial_received.await.expect("initial HTTP body");
    runtime.command(StreamCommand::Pause).await.expect("pause");
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(
        runtime.snapshot().await.status.play_state,
        music_stream::PlayState::Paused
    );
    runtime.command(StreamCommand::Play).await.expect("resume");
    let _ = continue_send.send(());
    if try_recv_rtp(&receiver, Duration::from_secs(2))
        .await
        .is_none()
    {
        panic!(
            "RTP timeout after resumed download: {:?}",
            runtime.snapshot().await
        );
    }

    runtime.shutdown().await.expect("shutdown");
    server.await.expect("server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn progressive_url_sends_rtp_before_http_download_completes() {
    let wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("wav");
    write_wav(wav.path(), 1.0);
    let body = std::fs::read(wav.path()).expect("wav body");
    let split = (44 + 48_000 * 2 * 2 / 3).min(body.len());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("HTTP bind");
    let address = listener.local_addr().expect("HTTP address");
    let (initial_sent, initial_received) = tokio::sync::oneshot::channel();
    let (finish_send, finish_receive) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut request = [0_u8; 2_048];
        let _ = stream.read(&mut request).await;
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: audio/wav\r\n\r\n",
            body.len()
        );
        stream.write_all(header.as_bytes()).await.expect("header");
        stream
            .write_all(&body[..split])
            .await
            .expect("initial body");
        stream.flush().await.expect("initial flush");
        let _ = initial_sent.send(());
        let _ = finish_receive.await;
        stream
            .write_all(&body[split..])
            .await
            .expect("remaining body");
    });

    let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("RTP bind");
    let mut transport = RtpTransportConfig::new(
        "127.0.0.1",
        receiver.local_addr().expect("RTP address").port(),
        205,
    );
    transport.local_ip = "127.0.0.1".to_owned();
    let runtime = StreamRuntime::start(
        "progressive-url".to_owned(),
        TrackSource {
            attempt_id: "attempt-progressive-url".to_owned(),
            id: "progressive-url".to_owned(),
            kind: TrackKind::Url,
            url: Some(format!("http://{address}/opaque")),
            path: None,
            format_hint: Some("wav".to_owned()),
            seekable: Some(true),
            headers: Default::default(),
            network_policy: NetworkPolicy::Provider,
        },
        StreamRuntimeConfig::new(transport, SourceResolverConfig::default()),
        VolumeLevel::default(),
        Default::default(),
    )
    .await
    .expect("runtime");

    initial_received.await.expect("initial HTTP bytes");
    assert!(
        try_recv_rtp(&receiver, Duration::from_secs(1))
            .await
            .is_some(),
        "RTP must start before the server releases the remaining HTTP body"
    );

    let _ = finish_send.send(());
    server.await.expect("server");
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn next_url_added_while_paused_starts_no_download_until_resume() {
    let current_wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("current wav");
    let next_wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("next wav");
    write_wav(current_wav.path(), 1.0);
    write_wav(next_wav.path(), 0.4);
    let next_body = std::fs::read(next_wav.path()).expect("next body");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("HTTP bind");
    let address = listener.local_addr().expect("HTTP address");
    let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("RTP bind");
    let runtime = runtime_for(
        "paused-next",
        file_track("current", current_wav.path()),
        None,
        &receiver,
        202,
    )
    .await;
    let _ = recv_rtp(&receiver).await;
    runtime.command(StreamCommand::Pause).await.expect("pause");
    let mut current = file_track("current", current_wav.path());
    current.attempt_id = "paused-next:current".to_owned();
    runtime
        .command(StreamCommand::ReconcilePlan {
            version: 1,
            current: Some(current),
            next: Some(TrackSource {
                attempt_id: "paused-next:next".to_owned(),
                id: "next-url".to_owned(),
                kind: TrackKind::Url,
                url: Some(format!("http://{address}/next.wav")),
                path: None,
                format_hint: None,
                seekable: Some(true),
                headers: Default::default(),
                network_policy: NetworkPolicy::Provider,
            }),
        })
        .await
        .expect("set next");

    assert!(
        tokio::time::timeout(Duration::from_millis(80), listener.accept())
            .await
            .is_err()
    );
    runtime.command(StreamCommand::Play).await.expect("resume");
    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
        .await
        .expect("next connection timeout")
        .expect("next connection");
    let mut request = [0_u8; 2_048];
    let _ = stream.read(&mut request).await;
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: audio/wav\r\n\r\n",
        next_body.len()
    );
    stream.write_all(header.as_bytes()).await.expect("header");
    stream.write_all(&next_body).await.expect("next body");
    drop(stream);

    let _ = recv_rtp(&receiver).await;
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switching_to_url_while_paused_preserves_pause_and_defers_download() {
    let current_wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("current wav");
    let switched_wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("switched wav");
    write_wav(current_wav.path(), 1.0);
    write_wav(switched_wav.path(), 0.4);
    let switched_body = std::fs::read(switched_wav.path()).expect("switched body");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("HTTP bind");
    let address = listener.local_addr().expect("HTTP address");
    let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("RTP bind");
    let runtime = runtime_for(
        "paused-switch",
        file_track("current", current_wav.path()),
        None,
        &receiver,
        203,
    )
    .await;
    let _ = recv_rtp(&receiver).await;
    runtime.command(StreamCommand::Pause).await.expect("pause");

    let switched = runtime
        .command(StreamCommand::ReconcilePlan {
            version: 1,
            current: Some(TrackSource {
                attempt_id: "paused-switch:replacement".to_owned(),
                id: "switched-url".to_owned(),
                kind: TrackKind::Url,
                url: Some(format!("http://{address}/switched.wav")),
                path: None,
                format_hint: None,
                seekable: Some(true),
                headers: Default::default(),
                network_policy: NetworkPolicy::Provider,
            }),
            next: None,
        })
        .await
        .expect("switch");
    assert_eq!(switched.status.play_state, music_stream::PlayState::Paused);
    assert!(
        tokio::time::timeout(Duration::from_millis(80), listener.accept())
            .await
            .is_err()
    );

    runtime.command(StreamCommand::Play).await.expect("resume");
    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
        .await
        .expect("switched connection timeout")
        .expect("switched connection");
    let mut request = [0_u8; 2_048];
    let _ = stream.read(&mut request).await;
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: audio/wav\r\n\r\n",
        switched_body.len()
    );
    stream.write_all(header.as_bytes()).await.expect("header");
    stream
        .write_all(&switched_body)
        .await
        .expect("switched body");
    drop(stream);

    let _ = recv_rtp(&receiver).await;
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_callback_panic_cannot_skip_runtime_actions() {
    let wav = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("wav");
    write_wav(wav.path(), 0.4);
    let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("RTP bind");
    let mut transport = RtpTransportConfig::new(
        "127.0.0.1",
        receiver.local_addr().expect("RTP address").port(),
        203,
    );
    transport.local_ip = "127.0.0.1".to_owned();
    let panics_once = Arc::new(AtomicBool::new(true));
    let callback_state = Arc::clone(&panics_once);
    let mut config = StreamRuntimeConfig::new(transport, SourceResolverConfig::default());
    config.on_event = Some(Arc::new(move |_| {
        if callback_state.swap(false, Ordering::Relaxed) {
            panic!("injected callback panic");
        }
    }));

    let runtime = StreamRuntime::start(
        "callback-panic".to_owned(),
        file_track("callback", wav.path()),
        config,
        VolumeLevel::default(),
        Default::default(),
    )
    .await
    .expect("runtime");
    let _ = recv_rtp(&receiver).await;
    assert!(!panics_once.load(Ordering::Relaxed));
    runtime.shutdown().await.expect("shutdown");
}
