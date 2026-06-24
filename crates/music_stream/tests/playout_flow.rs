use std::collections::VecDeque;

use bytes::Bytes;
#[cfg(feature = "transport-rtp")]
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
#[cfg(feature = "transport-rtp")]
use metrics_util::{CompositeKey, MetricKind};
#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
use music_stream::ErrorCode;
#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
use music_stream::{
    AudioFormat, LocalFileSlotConfig, RubatoResamplerConfig, RubatoResamplingDecoder,
    SymphoniaFileDecoder, build_local_file_slot,
};
use music_stream::{
    DecodePoll, DecodedChunk, DecoderBackend, Engine, LibOpusEncoder, LibOpusEncoderConfig,
    MemoryDecoder, OpusEncoderBackend, OpusFrame, PcmFrame, PipelineConfig, PlayState,
    PlayoutPipeline, Result, SenderStep, SlotDriver, SlotRole, StreamCommand, TaskAction,
    TrackKind, TrackSource, WatermarkConfig, WorkerEvent,
};
#[cfg(feature = "transport-rtp")]
use music_stream::{
    LocalFileRtpPlaybackConfig, MemoryRtpPacketSink, RtpPacketizer, RtpPacketizerConfig,
    RtpSlotRunner, RtpTransportConfig, VolumeLevel, spawn_live_stream_rtp_playback,
    spawn_local_file_preload, spawn_local_file_rtp_playback,
    spawn_local_file_rtp_playback_from_driver,
};

#[derive(Debug)]
struct FakeDecoder {
    polls: VecDeque<DecodePoll>,
}

impl FakeDecoder {
    fn new(polls: impl IntoIterator<Item = DecodePoll>) -> Self {
        Self {
            polls: polls.into_iter().collect(),
        }
    }
}

impl DecoderBackend for FakeDecoder {
    fn poll_decode(&mut self) -> Result<DecodePoll> {
        Ok(self.polls.pop_front().unwrap_or(DecodePoll::End))
    }
}

#[derive(Debug, Default)]
struct FakeEncoder;

impl OpusEncoderBackend for FakeEncoder {
    fn encode(&mut self, frame: &PcmFrame) -> Result<OpusFrame> {
        Ok(OpusFrame {
            generation: frame.generation,
            payload: Bytes::copy_from_slice(&frame.track_position_samples.to_le_bytes()),
            samples_per_channel: frame.samples_per_channel,
            duration_ms: frame.duration_ms(),
            marker: frame.track_position_samples == 0,
            track_position_samples: frame.track_position_samples,
        })
    }
}

#[cfg(feature = "transport-rtp")]
type MetricSnapshot = Vec<(
    CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

#[cfg(feature = "transport-rtp")]
fn metric_snapshot(snapshotter: &Snapshotter) -> MetricSnapshot {
    snapshotter.snapshot().into_vec()
}

#[cfg(feature = "transport-rtp")]
fn metric_counter_sum(snapshot: &MetricSnapshot, name: &str) -> u64 {
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

#[cfg(feature = "transport-rtp")]
fn metric_has_gauge(snapshot: &MetricSnapshot, name: &str) -> bool {
    snapshot.iter().any(|(key, _, _, value)| {
        key.kind() == MetricKind::Gauge
            && key.key().name() == name
            && matches!(value, DebugValue::Gauge(_))
    })
}

#[cfg(feature = "transport-rtp")]
fn metric_histogram_values_u64(snapshot: &MetricSnapshot, name: &str) -> Vec<u64> {
    snapshot
        .iter()
        .find_map(|(key, _, _, value)| {
            if key.kind() == MetricKind::Histogram && key.key().name() == name {
                match value {
                    DebugValue::Histogram(values) => {
                        Some(values.iter().map(|value| value.0 as u64).collect())
                    }
                    _ => None,
                }
            } else {
                None
            }
        })
        .unwrap_or_default()
}

#[test]
fn command_worker_sender_flow_reaches_playing_and_sends_ready_frames() {
    let engine = Engine::new();
    engine
        .create_stream("s1".to_owned(), Some(track("a")), Some(track("b")))
        .expect("create stream");

    let play_output = engine
        .command("s1", StreamCommand::Play)
        .expect("play command");
    assert!(
        play_output.actions.iter().any(
            |action| matches!(action, TaskAction::PrepareNext { track, .. } if track.id == "b")
        )
    );

    let (generation, current_track) = play_output
        .actions
        .iter()
        .find_map(|action| match action {
            TaskAction::StartCurrent { generation, track } => Some((*generation, track.clone())),
            _ => None,
        })
        .expect("start current action");

    assert_eq!(current_track.id, "a");

    let pipeline = PlayoutPipeline::new(
        FakeDecoder::new([chunk(3), DecodePoll::End]),
        FakeEncoder,
        pipeline_config(generation),
    )
    .expect("pipeline");
    let mut slot = SlotDriver::new(SlotRole::Current, generation, pipeline);

    let turn = slot.worker_turn().expect("worker turn");
    let report = turn.worker;
    assert_eq!(report.encoded_frames, 3);
    assert_eq!(slot.pipeline().encoded_queue_ms(), 60);
    assert!(report.prebuffer_ready);
    assert_eq!(
        turn.event,
        Some(WorkerEvent::CurrentPrebufferReady { generation })
    );

    let ready = engine
        .worker_event("s1", turn.event.expect("ready event"))
        .expect("ready event");
    assert_eq!(ready.status.play_state, PlayState::Playing);

    let sent = drain_slot_sender(&mut slot);
    assert_eq!(sent, 3);

    let ended_turn = slot.worker_turn().expect("source end turn");
    assert!(ended_turn.worker.source_ended);
    assert!(ended_turn.worker.playout_drained);
    assert_eq!(
        ended_turn.event,
        Some(WorkerEvent::CurrentEnded { generation })
    );

    let ended = engine
        .worker_event("s1", ended_turn.event.expect("ended event"))
        .expect("current ended");
    assert_eq!(ended.status.play_state, PlayState::Buffering);
}

#[test]
fn repeated_play_does_not_duplicate_current_or_next_tasks() {
    let engine = Engine::new();
    engine
        .create_stream("s1".to_owned(), Some(track("a")), Some(track("b")))
        .expect("create stream");

    let first = engine.play("s1").expect("first play");
    assert_eq!(count_start_current(&first.actions), 1);
    assert_eq!(count_prepare_next(&first.actions), 1);

    let second = engine.play("s1").expect("second play");
    assert_eq!(count_start_current(&second.actions), 0);
    assert_eq!(count_prepare_next(&second.actions), 0);
}

#[test]
fn prepared_next_promotes_only_after_current_end() {
    let engine = Engine::new();
    engine
        .create_stream("s1".to_owned(), Some(track("a")), Some(track("b")))
        .expect("create stream");

    let play_output = engine.play("s1").expect("play");
    let current_generation = find_start_current(&play_output.actions)
        .expect("start current")
        .0;
    let (next_generation, next_track) =
        find_prepare_next(&play_output.actions).expect("prepare next");
    assert_eq!(next_track.id, "b");

    let next_ready = engine
        .worker_event(
            "s1",
            WorkerEvent::NextReady {
                generation: next_generation,
            },
        )
        .expect("next ready");
    let next_ready_status = next_ready.status;
    assert_eq!(next_ready_status.current.expect("current").id, "a");
    assert!(next_ready.actions.iter().all(
        |action| !matches!(action, TaskAction::StartCurrent { track, .. } if track.id == "b")
    ));

    let ended = engine
        .worker_event(
            "s1",
            WorkerEvent::CurrentEnded {
                generation: current_generation,
            },
        )
        .expect("current ended");

    assert_eq!(ended.status.current.expect("current").id, "b");
    assert!(ended.actions.iter().any(
        |action| matches!(action, TaskAction::StartCurrent { generation, track } if *generation == next_generation && track.id == "b")
    ));
}

#[test]
fn pipeline_can_encode_with_real_libopus_backend() {
    let mut pipeline = PlayoutPipeline::new(
        MemoryDecoder::new([decoded_chunk(2)]),
        LibOpusEncoder::new(LibOpusEncoderConfig::default()).expect("libopus encoder"),
        pipeline_config(1),
    )
    .expect("pipeline");

    let report = pipeline.worker_turn().expect("worker turn");
    assert_eq!(report.encoded_frames, 2);

    let mut payload_sizes = Vec::new();
    while let SenderStep::Send { frame, .. } = pipeline.sender_step() {
        payload_sizes.push(frame.payload.len());
    }

    assert_eq!(payload_sizes.len(), 2);
    assert!(payload_sizes.iter().all(|size| *size > 0 && *size <= 1_500));
}

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
#[test]
fn pipeline_can_decode_wav_with_symphonia_and_encode_with_libopus() {
    let temp = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("temp wav");
    write_test_wav(temp.path(), 4_410, 44_100, 1).expect("write wav");

    let mut pipeline = PlayoutPipeline::new(
        RubatoResamplingDecoder::new(
            SymphoniaFileDecoder::open(temp.path()).expect("symphonia decoder"),
            RubatoResamplerConfig::new(AudioFormat {
                sample_rate: 48_000,
                channels: 2,
            }),
        )
        .expect("resampling decoder"),
        LibOpusEncoder::new(LibOpusEncoderConfig::default()).expect("libopus encoder"),
        pipeline_config(1),
    )
    .expect("pipeline");

    let report = pipeline.worker_turn().expect("worker turn");
    assert!(report.encoded_frames > 0);
    assert!(report.prebuffer_ready);

    let sent = drain_sender(&mut pipeline);
    assert!(sent > 0);
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
#[test]
fn file_track_slot_flow_resolves_decodes_resamples_encodes_and_packetizes_rtp() {
    use util::marshal::Unmarshal;

    let temp = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("temp wav");
    write_test_wav(temp.path(), 4_410, 44_100, 1).expect("write wav");

    let track = TrackSource {
        id: "file-a".to_owned(),
        kind: TrackKind::File,
        url: None,
        path: Some(temp.path().to_string_lossy().into_owned()),
        seekable: Some(true),
    };
    let built = build_local_file_slot(
        SlotRole::Current,
        &track,
        LocalFileSlotConfig::new(pipeline_config(7)),
    )
    .expect("local file slot");
    assert_eq!(built.artifact.track_id, "file-a");
    assert!(built.artifact.seekable);
    let packetizer = RtpPacketizer::new(RtpPacketizerConfig {
        payload_type: 111,
        ssrc: 0x0102_0304,
        mtu: 1_200,
    })
    .expect("packetizer");
    let mut runner = RtpSlotRunner::new(built.driver, packetizer, MemoryRtpPacketSink::default());

    let tick = runner.tick(1).expect("runner tick");
    assert!(tick.worker.encoded_frames > 0);
    assert!(tick.worker.prebuffer_ready);
    assert_eq!(
        tick.events,
        vec![WorkerEvent::CurrentPrebufferReady { generation: 7 }]
    );
    assert_eq!(tick.drain.packets_sent, 1);
    assert!(tick.drain.bytes_sent > 0);
    let packetized = runner.sink().packets()[0].clone();

    assert_eq!(packetized.sequence, 0);
    assert_eq!(packetized.rtp_timestamp, 0);
    assert_eq!(packetized.ssrc, 0x0102_0304);
    assert_eq!(packetized.payload_type, 111);
    assert!(packetized.payload_len > 0);

    let mut raw = packetized.bytes.clone();
    let packet = rtp::packet::Packet::unmarshal(&mut raw).expect("unmarshal rtp packet");
    assert_eq!(packet.header.version, 2);
    assert_eq!(packet.header.payload_type, 111);
    assert_eq!(packet.header.sequence_number, 0);
    assert_eq!(packet.header.timestamp, 0);
    assert_eq!(packet.header.ssrc, 0x0102_0304);
    assert!(!packet.payload.is_empty());
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
#[test]
fn url_track_slot_flow_downloads_temp_file_and_packetizes_rtp() {
    let temp = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("temp wav");
    write_test_wav(temp.path(), 4_410, 44_100, 1).expect("write wav");
    let body = std::fs::read(temp.path()).expect("read wav");
    let url = serve_http_bytes_once("/tone.wav", body);

    let track = TrackSource {
        id: "url-a".to_owned(),
        kind: TrackKind::Url,
        url: Some(url),
        path: None,
        seekable: Some(true),
    };
    let built = build_local_file_slot(
        SlotRole::Current,
        &track,
        LocalFileSlotConfig::new(pipeline_config(8)),
    )
    .expect("URL slot");
    assert_eq!(built.artifact.track_id, "url-a");
    assert!(built.artifact.is_temporary());
    let artifact_path = built
        .driver
        .artifact()
        .expect("driver keeps artifact")
        .path()
        .to_path_buf();
    assert!(artifact_path.exists());

    drop(built.artifact);
    assert!(artifact_path.exists());

    let packetizer = RtpPacketizer::new(RtpPacketizerConfig {
        payload_type: 111,
        ssrc: 0x1112_1314,
        mtu: 1_200,
    })
    .expect("packetizer");
    let mut runner = RtpSlotRunner::new(built.driver, packetizer, MemoryRtpPacketSink::default());

    let run = runner.run_until_idle(8, 8).expect("run URL slot");
    assert!(run.completed);
    assert!(run.drain.packets_sent > 0);
    assert_eq!(runner.sink().len(), run.drain.packets_sent);

    drop(runner);
    assert!(!artifact_path.exists());
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
#[test]
fn mp3_file_track_slot_flow_decodes_resamples_encodes_and_packetizes_rtp() {
    use util::marshal::Unmarshal;

    let temp = tempfile::Builder::new()
        .suffix(".mp3")
        .tempfile()
        .expect("temp mp3");
    write_test_mp3(temp.path()).expect("write mp3");

    let track = file_track("mp3-a", temp.path());
    let built = build_local_file_slot(
        SlotRole::Current,
        &track,
        LocalFileSlotConfig::new(pipeline_config(11)),
    )
    .expect("local mp3 slot");
    let packetizer = RtpPacketizer::new(RtpPacketizerConfig {
        payload_type: 111,
        ssrc: 0x0a0b_0c0d,
        mtu: 1_200,
    })
    .expect("packetizer");
    let mut runner = RtpSlotRunner::new(built.driver, packetizer, MemoryRtpPacketSink::default());

    let run = runner.run_until_idle(16, 8).expect("run mp3 slot");
    assert!(run.completed);
    assert!(run.drain.packets_sent > 0);
    assert!(run.drain.media_sent_ms > 0);
    assert_eq!(runner.sink().len(), run.drain.packets_sent);
    assert_eq!(
        run.events,
        vec![
            WorkerEvent::CurrentPrebufferReady { generation: 11 },
            WorkerEvent::CurrentEnded { generation: 11 },
        ]
    );

    let first = &runner.sink().packets()[0];
    assert_eq!(first.sequence, 0);
    assert_eq!(first.rtp_timestamp, 0);
    assert_eq!(first.ssrc, 0x0a0b_0c0d);
    assert_eq!(first.payload_type, 111);
    assert!(first.payload_len > 0);
    assert!(
        runner
            .sink()
            .packets()
            .iter()
            .all(|packet| packet.payload_len > 0)
    );

    let mut raw = first.bytes.clone();
    let packet = rtp::packet::Packet::unmarshal(&mut raw).expect("unmarshal rtp packet");
    assert_eq!(packet.header.version, 2);
    assert_eq!(packet.header.payload_type, 111);
    assert_eq!(packet.header.sequence_number, 0);
    assert_eq!(packet.header.timestamp, 0);
    assert_eq!(packet.header.ssrc, 0x0a0b_0c0d);
    assert!(!packet.payload.is_empty());
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
#[test]
fn runtime_can_play_local_mp3_to_udp_with_paced_worker() {
    use std::sync::{Arc, Mutex};
    use util::marshal::Unmarshal;

    let temp = tempfile::Builder::new()
        .suffix(".mp3")
        .tempfile()
        .expect("temp mp3");
    write_test_mp3(temp.path()).expect("write mp3");

    let receiver = std::net::UdpSocket::bind("127.0.0.1:0").expect("receiver socket");
    receiver
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("read timeout");
    let mut transport = RtpTransportConfig::new(
        "127.0.0.1",
        receiver.local_addr().expect("receiver addr").port(),
        0x2122_2324,
    );
    transport.payload_type = 111;
    transport.mtu = 1_200;

    let mut config = LocalFileRtpPlaybackConfig::new(21, transport);
    config.pipeline = pipeline_config(21);
    let metrics = Arc::new(DebuggingRecorder::new());
    let snapshotter = metrics.snapshotter();
    config.metrics_recorder = Some(metrics.clone());
    let events = Arc::new(Mutex::new(Vec::new()));
    let callback_events = Arc::clone(&events);
    let playback = spawn_local_file_rtp_playback(
        file_track("mp3-runtime-a", temp.path()),
        config,
        VolumeLevel::default(),
        move |event| callback_events.lock().expect("events lock").push(event),
    )
    .expect("spawn playback");
    playback.set_volume(VolumeLevel::from_unit(0.5).expect("volume"));

    for _ in 0..100 {
        if playback.is_finished() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(playback.is_finished());

    let progress = playback.progress();
    let report = playback.join().expect("join playback");
    assert!(report.completed);
    assert!(!report.stopped);
    assert!(report.drain.packets_sent > 0);
    assert!(report.drain.media_sent_ms > 0);
    assert_eq!(progress.media_sent_ms, report.drain.media_sent_ms);
    assert_eq!(progress.packets_sent, report.drain.packets_sent);
    assert_eq!(progress.bytes_sent, report.drain.bytes_sent);
    let snapshot = metric_snapshot(&snapshotter);
    assert_eq!(
        metric_counter_sum(&snapshot, "music_stream.runtime.current.rtp_packets_sent"),
        report.drain.packets_sent as u64
    );
    assert!(metric_has_gauge(
        &snapshot,
        "music_stream.runtime.current.encoded_queue_ms"
    ));
    assert!(metric_has_gauge(
        &snapshot,
        "music_stream.runtime.current.rtp_max_pacing_late_ms"
    ));
    assert!(
        !metric_histogram_values_u64(&snapshot, "music_stream.runtime.current.worker_turn_us")
            .is_empty()
    );
    assert_eq!(
        events.lock().expect("events lock").as_slice(),
        &[
            WorkerEvent::CurrentPrebufferReady { generation: 21 },
            WorkerEvent::CurrentEnded { generation: 21 },
        ]
    );

    let mut first_packet = None;
    for _ in 0..report.drain.packets_sent {
        let mut buffer = vec![0_u8; 1_500];
        let len = receiver.recv(&mut buffer).expect("receive rtp datagram");
        buffer.truncate(len);
        let mut raw = bytes::Bytes::from(buffer);
        let packet = rtp::packet::Packet::unmarshal(&mut raw).expect("unmarshal rtp packet");
        if packet.header.sequence_number == 0 {
            first_packet = Some(packet);
        }
    }

    let first_packet = first_packet.expect("first packet");
    assert_eq!(first_packet.header.version, 2);
    assert_eq!(first_packet.header.payload_type, 111);
    assert_eq!(first_packet.header.timestamp, 0);
    assert_eq!(first_packet.header.ssrc, 0x2122_2324);
    assert!(!first_packet.payload.is_empty());
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
#[test]
fn runtime_can_play_live_http_wav_to_udp_with_paced_worker() {
    use std::sync::{Arc, Mutex};
    use util::marshal::Unmarshal;

    let temp = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("temp wav");
    write_test_wav(temp.path(), 24_000, 48_000, 2).expect("write wav");
    let body = std::fs::read(temp.path()).expect("read wav");
    let url = serve_http_bytes_once("/live.wav", body);

    let receiver = std::net::UdpSocket::bind("127.0.0.1:0").expect("receiver socket");
    receiver
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("read timeout");
    let mut transport = RtpTransportConfig::new(
        "127.0.0.1",
        receiver.local_addr().expect("receiver addr").port(),
        0x3132_3334,
    );
    transport.payload_type = 111;
    transport.mtu = 1_200;

    let mut config = LocalFileRtpPlaybackConfig::new(31, transport);
    config.pipeline = pipeline_config(31);
    let events = Arc::new(Mutex::new(Vec::new()));
    let callback_events = Arc::clone(&events);
    let playback = spawn_live_stream_rtp_playback(
        live_track("live-runtime-a", url),
        config,
        VolumeLevel::default(),
        move |event| callback_events.lock().expect("events lock").push(event),
    )
    .expect("spawn live playback");

    for _ in 0..100 {
        if playback.is_finished() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(playback.is_finished());

    let report = playback.join().expect("join live playback");
    assert!(report.completed);
    assert!(!report.stopped);
    assert!(report.drain.packets_sent > 0);
    assert!(report.drain.media_sent_ms > 0);
    assert_eq!(
        events.lock().expect("events lock").as_slice(),
        &[
            WorkerEvent::CurrentPrebufferReady { generation: 31 },
            WorkerEvent::CurrentEnded { generation: 31 },
        ]
    );

    let mut first_packet = None;
    for _ in 0..report.drain.packets_sent {
        let mut buffer = vec![0_u8; 1_500];
        let len = receiver.recv(&mut buffer).expect("receive rtp datagram");
        buffer.truncate(len);
        let mut raw = bytes::Bytes::from(buffer);
        let packet = rtp::packet::Packet::unmarshal(&mut raw).expect("unmarshal rtp packet");
        if packet.header.sequence_number == 0 {
            first_packet = Some(packet);
        }
    }

    let first_packet = first_packet.expect("first packet");
    assert_eq!(first_packet.header.version, 2);
    assert_eq!(first_packet.header.payload_type, 111);
    assert_eq!(first_packet.header.timestamp, 0);
    assert_eq!(first_packet.header.ssrc, 0x3132_3334);
    assert!(!first_packet.payload.is_empty());
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
#[test]
fn runtime_retries_live_http_before_playing_to_udp() {
    use std::sync::{Arc, Mutex};
    use util::marshal::Unmarshal;

    let temp = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("temp wav");
    write_test_wav(temp.path(), 24_000, 48_000, 2).expect("write wav");
    let body = std::fs::read(temp.path()).expect("read wav");
    let url = serve_http_status_then_bytes_once("/live-retry.wav", 503, body);

    let receiver = std::net::UdpSocket::bind("127.0.0.1:0").expect("receiver socket");
    receiver
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("read timeout");
    let mut transport = RtpTransportConfig::new(
        "127.0.0.1",
        receiver.local_addr().expect("receiver addr").port(),
        0x4142_4344,
    );
    transport.payload_type = 111;
    transport.mtu = 1_200;

    let mut config = LocalFileRtpPlaybackConfig::new(32, transport);
    config.pipeline = pipeline_config(32);
    let events = Arc::new(Mutex::new(Vec::new()));
    let callback_events = Arc::clone(&events);
    let playback = spawn_live_stream_rtp_playback(
        live_track("live-runtime-retry", url),
        config,
        VolumeLevel::default(),
        move |event| callback_events.lock().expect("events lock").push(event),
    )
    .expect("spawn retried live playback");

    for _ in 0..100 {
        if playback.is_finished() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(playback.is_finished());

    let report = playback.join().expect("join retried live playback");
    assert!(report.completed);
    assert!(!report.stopped);
    assert!(report.drain.packets_sent > 0);
    assert_eq!(
        events.lock().expect("events lock").as_slice(),
        &[
            WorkerEvent::CurrentPrebufferReady { generation: 32 },
            WorkerEvent::CurrentEnded { generation: 32 },
        ]
    );

    let mut buffer = vec![0_u8; 1_500];
    let len = receiver
        .recv(&mut buffer)
        .expect("receive retried live RTP");
    buffer.truncate(len);
    let packet = rtp::packet::Packet::unmarshal(&mut bytes::Bytes::from(buffer))
        .expect("unmarshal retried live RTP");
    assert_eq!(packet.header.payload_type, 111);
    assert_eq!(packet.header.ssrc, 0x4142_4344);
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
#[test]
fn runtime_live_http_startup_auth_failure_preserves_source_error_code() {
    let url = serve_http_status_once("/live-auth.wav", 403);
    let receiver = std::net::UdpSocket::bind("127.0.0.1:0").expect("receiver socket");
    let transport = RtpTransportConfig::new(
        "127.0.0.1",
        receiver.local_addr().expect("receiver addr").port(),
        0x5152_5354,
    );

    let mut config = LocalFileRtpPlaybackConfig::new(33, transport);
    config.pipeline = pipeline_config(33);
    let error = spawn_live_stream_rtp_playback(
        live_track("live-runtime-auth", url),
        config,
        VolumeLevel::default(),
        |_| {},
    )
    .expect_err("auth failure should reject live startup");
    assert_eq!(error.code(), ErrorCode::SourceAuthExpired);
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
#[test]
fn runtime_pause_holds_progress_and_resume_continues_pacing() {
    let temp = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("temp wav");
    write_test_wav(temp.path(), 24_000, 48_000, 2).expect("write wav");

    let receiver = std::net::UdpSocket::bind("127.0.0.1:0").expect("receiver socket");
    let transport = RtpTransportConfig::new(
        "127.0.0.1",
        receiver.local_addr().expect("receiver addr").port(),
        0x3132_3334,
    );

    let mut config = LocalFileRtpPlaybackConfig::new(31, transport);
    config.pipeline = pipeline_config(31);
    let playback = spawn_local_file_rtp_playback(
        file_track("wav-runtime-pause", temp.path()),
        config,
        VolumeLevel::default(),
        |_| {},
    )
    .expect("spawn playback");

    playback.pause();
    std::thread::sleep(std::time::Duration::from_millis(60));
    let paused_first = playback.progress();
    std::thread::sleep(std::time::Duration::from_millis(60));
    let paused_second = playback.progress();
    assert_eq!(paused_second.media_sent_ms, paused_first.media_sent_ms);
    assert_eq!(paused_second.packets_sent, paused_first.packets_sent);
    assert!(playback.is_paused());

    playback.resume();
    for _ in 0..20 {
        if playback.progress().media_sent_ms > paused_second.media_sent_ms {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(playback.progress().media_sent_ms > paused_second.media_sent_ms);

    playback.stop();
    let report = playback.join().expect("join playback");
    assert!(report.stopped);
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
#[test]
fn runtime_can_start_local_file_playback_from_seek_position() {
    let temp = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("temp wav");
    write_test_wav(temp.path(), 24_000, 48_000, 2).expect("write wav");

    let receiver = std::net::UdpSocket::bind("127.0.0.1:0").expect("receiver socket");
    let transport = RtpTransportConfig::new(
        "127.0.0.1",
        receiver.local_addr().expect("receiver addr").port(),
        0x5152_5354,
    );

    let mut config = LocalFileRtpPlaybackConfig::new(51, transport);
    config.pipeline = pipeline_config(51);
    config.start_position_ms = 250;
    let playback = spawn_local_file_rtp_playback(
        file_track("wav-runtime-seek", temp.path()),
        config,
        VolumeLevel::default(),
        |_| {},
    )
    .expect("spawn playback");

    for _ in 0..20 {
        let progress = playback.progress();
        if progress.media_sent_ms > 0 {
            assert_eq!(progress.start_position_ms, 250);
            assert_eq!(progress.stream_position_ms, 250 + progress.media_sent_ms);
            playback.stop();
            let report = playback.join().expect("join playback");
            assert!(report.stopped);
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    panic!("seeked runtime playback did not send media");
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
#[test]
fn runtime_sends_rtcp_sender_report_over_muxed_rtp_socket() {
    use util::marshal::Unmarshal;

    let temp = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("temp wav");
    write_test_wav(temp.path(), 24_000, 48_000, 2).expect("write wav");

    let receiver = std::net::UdpSocket::bind("127.0.0.1:0").expect("receiver socket");
    receiver
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("read timeout");
    let mut transport = RtpTransportConfig::new(
        "127.0.0.1",
        receiver.local_addr().expect("receiver addr").port(),
        0x6162_6364,
    );
    transport.rtcp_mux = true;

    let mut config = LocalFileRtpPlaybackConfig::new(61, transport);
    config.pipeline = pipeline_config(61);
    config.rtcp_report_interval = std::time::Duration::from_millis(20);
    let playback = spawn_local_file_rtp_playback(
        file_track("wav-runtime-rtcp", temp.path()),
        config,
        VolumeLevel::default(),
        |_| {},
    )
    .expect("spawn playback");

    let mut saw_rtp = false;
    let mut saw_rtcp = false;
    for _ in 0..80 {
        let mut buffer = vec![0_u8; 1_500];
        let len = receiver.recv(&mut buffer).expect("receive datagram");
        buffer.truncate(len);
        if len >= 2 && buffer[1] == 200 {
            let mut raw = bytes::Bytes::from(buffer);
            let packets = rtcp::packet::unmarshal(&mut raw).expect("unmarshal rtcp");
            let sender_report = packets[0]
                .as_any()
                .downcast_ref::<rtcp::sender_report::SenderReport>()
                .expect("sender report packet");
            assert_eq!(sender_report.ssrc, 0x6162_6364);
            assert!(sender_report.packet_count > 0);
            assert!(sender_report.octet_count > 0);
            saw_rtcp = true;
            break;
        }

        let mut raw = bytes::Bytes::from(buffer);
        let packet = rtp::packet::Packet::unmarshal(&mut raw).expect("unmarshal rtp packet");
        assert_eq!(packet.header.ssrc, 0x6162_6364);
        saw_rtp = true;
    }

    assert!(saw_rtp);
    assert!(saw_rtcp);
    let progress = playback.progress();
    assert!(progress.rtcp_reports_sent > 0);
    assert!(progress.rtcp_bytes_sent > 0);

    playback.stop();
    let report = playback.join().expect("join playback");
    assert!(report.stopped);
    assert!(report.rtcp_reports_sent > 0);
    assert!(report.rtcp_bytes_sent > 0);
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
#[test]
fn runtime_records_rtcp_receiver_report_feedback() {
    use std::sync::{Arc, Mutex};
    use util::marshal::Unmarshal;

    let temp = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("temp wav");
    write_test_wav(temp.path(), 24_000, 48_000, 2).expect("write wav");

    let receiver = std::net::UdpSocket::bind("127.0.0.1:0").expect("receiver socket");
    receiver
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("read timeout");
    let transport = RtpTransportConfig::new(
        "127.0.0.1",
        receiver.local_addr().expect("receiver addr").port(),
        0x7172_7374,
    );

    let mut config = LocalFileRtpPlaybackConfig::new(71, transport);
    config.pipeline = pipeline_config(71);
    config.rtcp_report_interval = std::time::Duration::from_millis(20);
    let metrics = Arc::new(DebuggingRecorder::new());
    let snapshotter = metrics.snapshotter();
    config.metrics_recorder = Some(metrics.clone());
    let events = Arc::new(Mutex::new(Vec::new()));
    let callback_events = Arc::clone(&events);
    let playback = spawn_local_file_rtp_playback(
        file_track("wav-runtime-rr", temp.path()),
        config,
        VolumeLevel::default(),
        move |event| callback_events.lock().expect("events lock").push(event),
    )
    .expect("spawn playback");

    let runtime_addr = loop {
        let mut buffer = vec![0_u8; 1_500];
        let (len, peer) = receiver.recv_from(&mut buffer).expect("receive rtp");
        buffer.truncate(len);
        if len >= 2 && buffer[1] == 200 {
            continue;
        }
        let mut raw = bytes::Bytes::from(buffer);
        let packet = rtp::packet::Packet::unmarshal(&mut raw).expect("unmarshal rtp packet");
        assert_eq!(packet.header.ssrc, 0x7172_7374);
        break peer;
    };

    let receiver_report = rtcp::receiver_report::ReceiverReport {
        ssrc: 0x0102_0304,
        reports: vec![rtcp::reception_report::ReceptionReport {
            ssrc: 0x7172_7374,
            fraction_lost: 13,
            total_lost: 2,
            last_sequence_number: 4,
            jitter: 123,
            last_sender_report: 0,
            delay: 0,
        }],
        ..Default::default()
    };
    let bytes = rtcp::packet::marshal(&[Box::new(receiver_report)]).expect("marshal rr");
    receiver.send_to(&bytes, runtime_addr).expect("send rr");

    let mut latest = None;
    for _ in 0..30 {
        latest = playback.progress().latest_receiver_report;
        if latest.is_some() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let latest = latest.expect("receiver report feedback");
    assert_eq!(latest.reports_received, 1);
    assert_eq!(latest.sender_ssrc, 0x0102_0304);
    assert_eq!(latest.source_ssrc, 0x7172_7374);
    assert_eq!(latest.fraction_lost, 13);
    assert_eq!(latest.total_lost, 2);
    assert_eq!(latest.jitter, 123);
    assert_eq!(latest.jitter_micros, 2_562);
    assert_eq!(latest.round_trip_time_micros, None);
    let snapshot = metric_snapshot(&snapshotter);
    assert!(metric_has_gauge(
        &snapshot,
        "music_stream.runtime.current.rtcp_quality.window_reports"
    ));
    assert!(metric_has_gauge(
        &snapshot,
        "music_stream.runtime.current.rtcp_quality.average_loss_percent"
    ));
    assert!(metric_has_gauge(
        &snapshot,
        "music_stream.runtime.current.rtcp_quality.average_jitter_ms"
    ));
    assert!(
        events
            .lock()
            .expect("events lock")
            .iter()
            .any(|event| matches!(
                event,
                WorkerEvent::CurrentNetworkQualityChanged {
                    generation: 71,
                    quality: music_stream::RtcpNetworkQualityLevel::Degraded,
                    ..
                }
            ))
    );

    playback.stop();
    let report = playback.join().expect("join playback");
    assert_eq!(report.latest_receiver_report, Some(latest));
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
#[test]
fn preloaded_next_driver_can_be_promoted_and_streamed_without_redecode() {
    use std::sync::{Arc, Mutex};
    use util::marshal::Unmarshal;

    let temp = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("temp wav");
    write_test_wav(temp.path(), 4_410, 44_100, 1).expect("write wav");

    let receiver = std::net::UdpSocket::bind("127.0.0.1:0").expect("receiver socket");
    receiver
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("read timeout");
    let mut transport = RtpTransportConfig::new(
        "127.0.0.1",
        receiver.local_addr().expect("receiver addr").port(),
        0x4142_4344,
    );
    transport.payload_type = 111;

    let mut config = LocalFileRtpPlaybackConfig::new(41, transport);
    config.pipeline = pipeline_config(41);
    let events = Arc::new(Mutex::new(Vec::new()));
    let preload_events = Arc::clone(&events);
    let preload = spawn_local_file_preload(
        file_track("wav-next-preload", temp.path()),
        config.clone(),
        VolumeLevel::default(),
        move |event| preload_events.lock().expect("events lock").push(event),
    )
    .expect("spawn preload");

    let preload_completion = preload.completion();
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("preload wait runtime")
        .block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(2), preload_completion.wait())
                .await
                .expect("preload completion timeout");
        });
    assert!(preload.is_finished());

    let preload_report = preload.join().expect("join preload");
    assert!(preload_report.ready);
    assert!(!preload_report.stopped);
    assert_eq!(
        events.lock().expect("events lock").as_slice(),
        &[WorkerEvent::NextReady { generation: 41 }]
    );

    let playback = spawn_local_file_rtp_playback_from_driver(
        preload_report.into_current_driver(),
        config,
        VolumeLevel::default(),
        |_| {},
    )
    .expect("spawn promoted playback");

    for _ in 0..100 {
        if playback.is_finished() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(playback.is_finished());

    let report = playback.join().expect("join playback");
    assert!(report.completed);
    assert!(report.drain.packets_sent > 0);
    assert!(
        report
            .events
            .contains(&WorkerEvent::CurrentPrebufferReady { generation: 41 })
    );
    assert!(
        report
            .events
            .contains(&WorkerEvent::CurrentEnded { generation: 41 })
    );

    let mut first_packet = None;
    for _ in 0..report.drain.packets_sent {
        let mut buffer = vec![0_u8; 1_500];
        let len = receiver.recv(&mut buffer).expect("receive rtp datagram");
        buffer.truncate(len);
        let mut raw = bytes::Bytes::from(buffer);
        let packet = rtp::packet::Packet::unmarshal(&mut raw).expect("unmarshal rtp packet");
        if packet.header.sequence_number == 0 {
            first_packet = Some(packet);
        }
    }

    let first_packet = first_packet.expect("first packet");
    assert_eq!(first_packet.header.payload_type, 111);
    assert_eq!(first_packet.header.ssrc, 0x4142_4344);
    assert!(!first_packet.payload.is_empty());
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
#[test]
fn engine_action_can_drive_local_file_slot_runner_to_completion() {
    let temp = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("temp wav");
    write_test_wav(temp.path(), 4_410, 44_100, 1).expect("write wav");

    let track = TrackSource {
        id: "file-a".to_owned(),
        kind: TrackKind::File,
        url: None,
        path: Some(temp.path().to_string_lossy().into_owned()),
        seekable: Some(true),
    };
    let engine = Engine::new();
    engine
        .create_stream("s1".to_owned(), Some(track), None)
        .expect("create stream");

    let play = engine.play("s1").expect("play");
    let (generation, track) = find_start_current(&play.actions).expect("start current");
    let built = build_local_file_slot(
        SlotRole::Current,
        &track,
        LocalFileSlotConfig::new(pipeline_config(generation)),
    )
    .expect("local file slot");
    let packetizer = RtpPacketizer::new(RtpPacketizerConfig {
        payload_type: 111,
        ssrc: 0x2024,
        mtu: 1_200,
    })
    .expect("packetizer");
    let mut runner = RtpSlotRunner::new(built.driver, packetizer, MemoryRtpPacketSink::default());

    let run = runner.run_until_idle(8, 8).expect("run slot");
    assert!(run.completed);
    assert_eq!(run.drain.packets_sent, 5);
    assert_eq!(runner.sink().len(), 5);
    assert_eq!(
        run.events,
        vec![
            WorkerEvent::CurrentPrebufferReady { generation },
            WorkerEvent::CurrentEnded { generation },
        ]
    );

    let ready = engine
        .worker_event("s1", run.events[0].clone())
        .expect("ready event");
    assert_eq!(ready.status.play_state, PlayState::Playing);

    let ended = engine
        .worker_event("s1", run.events[1].clone())
        .expect("ended event");
    let status = ended.status;
    assert_eq!(status.play_state, PlayState::Idle);
    assert!(status.current.is_none());
    assert!(
        ended
            .events
            .iter()
            .any(|event| matches!(event, music_stream::StreamEvent::NextNeeded { .. }))
    );
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
#[test]
fn runtime_soak_streams_longer_wav_with_monotonic_rtp_timestamps() {
    use util::marshal::Unmarshal;

    let temp = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("temp wav");
    write_test_wav(temp.path(), 72_000, 48_000, 2).expect("write wav");

    let receiver = std::net::UdpSocket::bind("127.0.0.1:0").expect("receiver socket");
    receiver
        .set_read_timeout(Some(std::time::Duration::from_millis(100)))
        .expect("read timeout");
    let mut transport = RtpTransportConfig::new(
        "127.0.0.1",
        receiver.local_addr().expect("receiver addr").port(),
        0x8182_8384,
    );
    transport.payload_type = 111;

    let mut config = LocalFileRtpPlaybackConfig::new(81, transport);
    config.pipeline = pipeline_config(81);
    let playback = spawn_local_file_rtp_playback(
        file_track("wav-runtime-soak", temp.path()),
        config,
        VolumeLevel::default(),
        |_| {},
    )
    .expect("spawn playback");

    let mut packets = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(4);
    while std::time::Instant::now() < deadline {
        let mut buffer = vec![0_u8; 1_500];
        match receiver.recv(&mut buffer) {
            Ok(len) => {
                buffer.truncate(len);
                if len >= 2 && matches!(buffer[1], 200 | 201) {
                    continue;
                }
                let mut raw = bytes::Bytes::from(buffer);
                let packet = rtp::packet::Packet::unmarshal(&mut raw).expect("unmarshal rtp");
                if packet.header.ssrc == 0x8182_8384 {
                    packets.push(packet);
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if playback.is_finished() {
                    break;
                }
            }
            Err(error) => panic!("receive RTP: {error}"),
        }
    }

    for _ in 0..10 {
        let mut buffer = vec![0_u8; 1_500];
        let Ok(len) = receiver.recv(&mut buffer) else {
            break;
        };
        buffer.truncate(len);
        if len >= 2 && matches!(buffer[1], 200 | 201) {
            continue;
        }
        let mut raw = bytes::Bytes::from(buffer);
        let packet = rtp::packet::Packet::unmarshal(&mut raw).expect("unmarshal rtp");
        if packet.header.ssrc == 0x8182_8384 {
            packets.push(packet);
        }
    }

    let report = playback.join().expect("join playback");
    assert!(report.completed);
    assert!(!report.stopped);
    assert!(report.drain.packets_sent >= 70);
    assert_eq!(packets.len(), report.drain.packets_sent);
    assert_eq!(report.drain.media_sent_ms, 1_500);
    assert_eq!(
        packets
            .first()
            .expect("first packet")
            .header
            .sequence_number,
        0
    );
    for pair in packets.windows(2) {
        let previous = &pair[0].header;
        let next = &pair[1].header;
        assert_eq!(
            next.sequence_number,
            previous.sequence_number.wrapping_add(1)
        );
        assert_eq!(next.timestamp, previous.timestamp.wrapping_add(960));
    }
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
#[test]
fn engine_promotes_prepared_local_file_next_after_current_finishes() {
    let current_file = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("current wav");
    let next_file = tempfile::Builder::new()
        .suffix(".wav")
        .tempfile()
        .expect("next wav");
    write_test_wav(current_file.path(), 4_410, 44_100, 1).expect("write current");
    write_test_wav(next_file.path(), 4_410, 44_100, 1).expect("write next");

    let current = file_track("file-a", current_file.path());
    let next = file_track("file-b", next_file.path());
    let engine = Engine::new();
    engine
        .create_stream("s1".to_owned(), Some(current), Some(next))
        .expect("create stream");

    let play = engine.play("s1").expect("play");
    let (current_generation, current_track) =
        find_start_current(&play.actions).expect("start current");
    let (next_generation, next_track) = find_prepare_next(&play.actions).expect("prepare next");

    let current_slot = build_local_file_slot(
        SlotRole::Current,
        &current_track,
        LocalFileSlotConfig::new(pipeline_config(current_generation)),
    )
    .expect("current slot");
    let mut current_runner = RtpSlotRunner::new(
        current_slot.driver,
        RtpPacketizer::new(RtpPacketizerConfig::default()).expect("current packetizer"),
        MemoryRtpPacketSink::default(),
    );
    let next_slot = build_local_file_slot(
        SlotRole::Next,
        &next_track,
        LocalFileSlotConfig::new(pipeline_config(next_generation)),
    )
    .expect("next slot");
    let mut next_runner = RtpSlotRunner::new(
        next_slot.driver,
        RtpPacketizer::new(RtpPacketizerConfig::default()).expect("next packetizer"),
        MemoryRtpPacketSink::default(),
    );

    let next_tick = next_runner.tick(0).expect("prime next");
    assert_eq!(
        next_tick.events,
        vec![WorkerEvent::NextReady {
            generation: next_generation,
        }]
    );
    let next_ready = engine
        .worker_event("s1", next_tick.events[0].clone())
        .expect("next ready");
    assert_eq!(next_ready.status.current.expect("current").id, "file-a");

    let current_run = current_runner.run_until_idle(8, 8).expect("current run");
    assert_eq!(
        current_run.events,
        vec![
            WorkerEvent::CurrentPrebufferReady {
                generation: current_generation,
            },
            WorkerEvent::CurrentEnded {
                generation: current_generation,
            },
        ]
    );
    engine
        .worker_event("s1", current_run.events[0].clone())
        .expect("current ready");
    let promoted = engine
        .worker_event("s1", current_run.events[1].clone())
        .expect("current ended");

    let status = promoted.status;
    assert_eq!(status.current.expect("current").id, "file-b");
    assert!(promoted.actions.iter().any(
        |action| matches!(action, TaskAction::StartCurrent { generation, track } if *generation == next_generation && track.id == "file-b")
    ));
}

fn track(id: &str) -> TrackSource {
    TrackSource {
        id: id.to_owned(),
        kind: TrackKind::File,
        url: None,
        path: Some(format!("/tmp/{id}.mp3")),
        seekable: Some(true),
    }
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
fn file_track(id: &str, path: &std::path::Path) -> TrackSource {
    TrackSource {
        id: id.to_owned(),
        kind: TrackKind::File,
        url: None,
        path: Some(path.to_string_lossy().into_owned()),
        seekable: Some(true),
    }
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
fn live_track(id: &str, url: impl Into<String>) -> TrackSource {
    TrackSource {
        id: id.to_owned(),
        kind: TrackKind::Live,
        url: Some(url.into()),
        path: None,
        seekable: Some(false),
    }
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
fn serve_http_bytes_once(path: &'static str, body: Vec<u8>) -> String {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP test server");
    let addr = listener.local_addr().expect("HTTP test server address");
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept HTTP request");
        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
        let mut request = [0_u8; 1024];
        let _ = stream.read(&mut request);

        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(headers.as_bytes());
        let _ = stream.write_all(&body);
        let _ = stream.flush();
    });
    format!("http://{addr}{path}")
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
fn serve_http_status_then_bytes_once(path: &'static str, status: u16, body: Vec<u8>) -> String {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind retry HTTP test server");
    let addr = listener
        .local_addr()
        .expect("retry HTTP test server address");
    thread::spawn(move || {
        let (mut first, _) = listener.accept().expect("accept first HTTP request");
        let _ = first.set_read_timeout(Some(Duration::from_secs(2)));
        let mut request = [0_u8; 1024];
        let _ = first.read(&mut request);
        let response = format!("HTTP/1.1 {status} retry\r\nContent-Length: 0\r\n\r\n");
        let _ = first.write_all(response.as_bytes());
        let _ = first.flush();
        drop(first);

        let (mut second, _) = listener.accept().expect("accept retry HTTP request");
        let _ = second.set_read_timeout(Some(Duration::from_secs(2)));
        let mut request = [0_u8; 1024];
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

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
fn serve_http_status_once(path: &'static str, status: u16) -> String {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind status HTTP test server");
    let addr = listener
        .local_addr()
        .expect("status HTTP test server address");
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept status HTTP request");
        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
        let mut request = [0_u8; 1024];
        let _ = stream.read(&mut request);
        let response = format!("HTTP/1.1 {status} test\r\nContent-Length: 0\r\n\r\n");
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    });
    format!("http://{addr}{path}")
}

fn pipeline_config(generation: u64) -> PipelineConfig {
    PipelineConfig {
        generation,
        sample_rate: 48_000,
        channels: 2,
        frame_samples_per_channel: 960,
        watermarks: WatermarkConfig {
            decode_batch_ms: 100,
            decoded_low_water_ms: 20,
            decoded_high_water_ms: 60,
            encoded_low_water_ms: 20,
            encoded_high_water_ms: 100,
            next_prime_ms: 60,
            pause_encoded_limit_ms: 2_000,
        },
        prebuffer_ms: 40,
    }
}

fn chunk(frames: usize) -> DecodePoll {
    DecodePoll::Chunk(decoded_chunk(frames))
}

fn decoded_chunk(frames: usize) -> DecodedChunk {
    let samples_per_frame = 960 * 2;
    let sample_count = frames * samples_per_frame;
    let samples = (0..sample_count).map(|sample| sample as f32).collect();
    DecodedChunk {
        sample_rate: 48_000,
        channels: 2,
        samples_interleaved: samples,
    }
}

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
fn drain_sender<D, E>(pipeline: &mut PlayoutPipeline<D, E>) -> usize
where
    D: DecoderBackend,
    E: OpusEncoderBackend,
{
    let mut sent = 0;
    while let SenderStep::Send { .. } = pipeline.sender_step() {
        sent += 1;
    }
    sent
}

fn drain_slot_sender<D, E>(slot: &mut SlotDriver<D, E>) -> usize
where
    D: DecoderBackend,
    E: OpusEncoderBackend,
{
    let mut sent = 0;
    while let SenderStep::Send { .. } = slot.sender_step() {
        sent += 1;
    }
    sent
}

fn count_start_current(actions: &[TaskAction]) -> usize {
    actions
        .iter()
        .filter(|action| matches!(action, TaskAction::StartCurrent { .. }))
        .count()
}

fn count_prepare_next(actions: &[TaskAction]) -> usize {
    actions
        .iter()
        .filter(|action| matches!(action, TaskAction::PrepareNext { .. }))
        .count()
}

fn find_start_current(actions: &[TaskAction]) -> Option<(u64, TrackSource)> {
    actions.iter().find_map(|action| match action {
        TaskAction::StartCurrent { generation, track } => Some((*generation, track.clone())),
        _ => None,
    })
}

fn find_prepare_next(actions: &[TaskAction]) -> Option<(u64, TrackSource)> {
    actions.iter().find_map(|action| match action {
        TaskAction::PrepareNext { generation, track } => Some((*generation, track.clone())),
        _ => None,
    })
}

#[cfg(all(feature = "decoder-symphonia", feature = "resampler-rubato"))]
fn write_test_wav(
    path: &std::path::Path,
    samples_per_channel: usize,
    sample_rate: u32,
    channels: u16,
) -> std::io::Result<()> {
    use std::io::Write;

    let bits_per_sample = 16_u16;
    let bytes_per_sample = bits_per_sample / 8;
    let data_bytes = samples_per_channel as u32 * u32::from(channels) * u32::from(bytes_per_sample);
    let byte_rate = sample_rate * u32::from(channels) * u32::from(bytes_per_sample);
    let block_align = channels * bytes_per_sample;

    let mut file = std::fs::File::create(path)?;
    file.write_all(b"RIFF")?;
    file.write_all(&(36 + data_bytes).to_le_bytes())?;
    file.write_all(b"WAVE")?;
    file.write_all(b"fmt ")?;
    file.write_all(&16_u32.to_le_bytes())?;
    file.write_all(&1_u16.to_le_bytes())?;
    file.write_all(&channels.to_le_bytes())?;
    file.write_all(&sample_rate.to_le_bytes())?;
    file.write_all(&byte_rate.to_le_bytes())?;
    file.write_all(&block_align.to_le_bytes())?;
    file.write_all(&bits_per_sample.to_le_bytes())?;
    file.write_all(b"data")?;
    file.write_all(&data_bytes.to_le_bytes())?;
    for index in 0..samples_per_channel {
        let value = (index % 1024) as i16;
        let left = value.to_le_bytes();
        let right = (-value).to_le_bytes();
        file.write_all(&left)?;
        if channels > 1 {
            file.write_all(&right)?;
        }
    }

    Ok(())
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
fn write_test_mp3(path: &std::path::Path) -> std::io::Result<()> {
    std::fs::write(
        path,
        decode_hex(TEST_MP3_HEX).expect("valid mp3 fixture hex"),
    )
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
fn decode_hex(input: &str) -> std::result::Result<Vec<u8>, String> {
    let compact = input
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    if compact.len() % 2 != 0 {
        return Err("hex string has an odd length".to_owned());
    }

    let mut bytes = Vec::with_capacity(compact.len() / 2);
    for pair in compact.chunks_exact(2) {
        let high = hex_value(pair[0])?;
        let low = hex_value(pair[1])?;
        bytes.push((high << 4) | low);
    }

    Ok(bytes)
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
fn hex_value(byte: u8) -> std::result::Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex byte: {byte}")),
    }
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
const TEST_MP3_HEX: &str = "\
49443304000000000022545353450000000e0000034c61766636312e372e3130300000000000000000000000fffb40c0
0000000000000000000000000000000000496e666f0000000f0000000600000329005a5a5a5a5a5a5a5a5a5a5a5a5a5a
5a5a7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9cbdbdbdbdbdbdbdbdbdbdbdbdbd
bdbdbdbddededededededededededededededededeffffffffffffffffffffffffffffffff000000004c61766336312e
31390000000000000000000000002404140000000000000329c45e21c50000000000fffb10c400000474135554908030
a609af371a20020001ad39400001593a3d505008060901f07c1f07ca02008061107c1fd4083b1387f883700493f6c060
381c0e000000000028892a99146408e9024816a3f78501f0131bf022942fa81a12fc240d2a0a00183000fffb12c40283
c5581d201de000289b03e341af684ccc0900bc40048600e07867eef6a663039661c411260c007e6042060605204c605e
03c59ab4958798208664f9c4b4617e28a6abd4a26a7e28a61840cc73de99f4a668f19b8e6242b894e09bbeaa30d0e30e
1d31d3fffb10c40383c5041f180dfb2240ad846281bf6c48833e8330af1be350ce1c34eb1b4309e06d35cc02066ea875
7e6b06c9a5a1cfd7f42441898899b1c1bbc698970ed1c15f5c1bfc0ee189a84f9c23219e231999f9993c18b0b3078c53
e01ffabe8a30a1130f1c31fffb12c40303c5001f180dfb2240ab042281bf6c48a3b33d8630a61c934dbe6b34b81bf309
7070349101046f2676f86b84cd67837fafe847f3140e3393537a8431351c73856e2138231c9313a09a3856533b473303
e32f7d315175d12fa80e7ecfba30310153003003fffb10c403800588211a15e00028a40e6d771a7002301207830c20e8
31642aa340ec34353f016301201f300000e2f82230e0228180751cdffbe200000968180c060301400000000218a2ca6c
408888a13f2953f86cb7e258bdbfcf6733ff163821fde6ea22810199fffb12c4020004e8455819228000000034838000
042f02ea2c25232c7b208eac40268f8c0f0aafc5c1c0bf2a1c33f080b073f36098b7f4012a4c414d45332e313030aaaa
aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
";
