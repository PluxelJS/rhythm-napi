use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use bytes::{Bytes, BytesMut};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, MissedTickBehavior};

use super::StreamRuntimeProgress;
use super::opus_queue::OpusQueueReceiver;
use crate::error::{MusicStreamError, Result};
use crate::quality::{RtcpNetworkQualityLevel, RtcpQualityWindow};
use crate::session::WorkerEvent;
use crate::transport::{
    RtpPacketizer, RtpTransportConfig, build_rtcp_sender_report, parse_rtcp_receiver_reports,
};

const SENDER_COMMAND_TIMEOUT: Duration = Duration::from_secs(2);
const DATAGRAM_SEND_TIMEOUT: Duration = Duration::from_secs(1);
const SENDER_STOP_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
pub(super) struct SenderHandle {
    commands: mpsc::Sender<SenderCommand>,
    progress: watch::Receiver<StreamRuntimeProgress>,
    task: Arc<SenderTask>,
}

#[derive(Debug)]
struct SenderTask {
    supervisor: Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
    worker_abort: tokio::task::AbortHandle,
}

impl Drop for SenderTask {
    fn drop(&mut self) {
        self.worker_abort.abort();
        if let Ok(slot) = self.supervisor.get_mut()
            && let Some(supervisor) = slot.take()
        {
            supervisor.abort();
        }
    }
}

impl SenderHandle {
    pub(super) async fn spawn(
        config: RtpTransportConfig,
        prebuffer_ms: u64,
        max_playout_lateness_ms: u64,
        rtcp_interval: Duration,
        events: mpsc::Sender<WorkerEvent>,
    ) -> Result<Self> {
        if !config.encryption.is_plaintext() {
            return Err(MusicStreamError::Unsupported(
                "RTP protection requires an installed packet protector".to_owned(),
            ));
        }
        let local = config.local_rtp_addr();
        let remote = config.remote_rtp_addr();
        let socket = UdpSocket::bind(local)
            .await
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
        socket
            .connect(remote)
            .await
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
        let rtcp_socket = if config.rtcp_mux {
            None
        } else {
            let socket = UdpSocket::bind(format!("{}:0", config.local_ip))
                .await
                .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
            socket
                .connect(config.remote_rtcp_addr())
                .await
                .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
            Some(socket)
        };
        let (commands, command_rx) = mpsc::channel(32);
        let (progress_tx, progress) = watch::channel(StreamRuntimeProgress::default());
        let active_generation = Arc::new(AtomicU64::new(0));
        let worker = tokio::spawn(run_sender(
            socket,
            rtcp_socket,
            SenderRuntime {
                config,
                prebuffer_ms,
                max_playout_lateness: Duration::from_millis(max_playout_lateness_ms),
                rtcp_interval,
                commands: command_rx,
                progress: progress_tx,
                events: events.clone(),
            },
            Arc::clone(&active_generation),
        ));
        let worker_abort = worker.abort_handle();
        let supervisor = supervise_sender(worker, active_generation, events);
        Ok(Self {
            commands,
            progress,
            task: Arc::new(SenderTask {
                supervisor: Mutex::new(Some(supervisor)),
                worker_abort,
            }),
        })
    }

    pub(super) fn progress(&self) -> StreamRuntimeProgress {
        *self.progress.borrow()
    }

    pub(super) async fn activate(
        &self,
        generation: u64,
        start_position_ms: u64,
        paused: bool,
        receiver: OpusQueueReceiver,
    ) -> Result<()> {
        self.request(|reply| SenderCommand::Activate {
            generation,
            start_position_ms,
            paused,
            receiver,
            reply,
        })
        .await
    }

    pub(super) async fn deactivate(&self, generation: u64) -> Result<()> {
        self.request(|reply| SenderCommand::Deactivate { generation, reply })
            .await
    }

    pub(super) async fn pause(&self, generation: u64) -> Result<()> {
        self.request(|reply| SenderCommand::Pause { generation, reply })
            .await
    }

    pub(super) async fn resume(&self, generation: u64) -> Result<()> {
        self.request(|reply| SenderCommand::Resume { generation, reply })
            .await
    }

    pub(super) async fn shutdown(&self) -> Result<()> {
        let command_result = self
            .request(|reply| SenderCommand::Shutdown { reply })
            .await;
        let task = self
            .task
            .supervisor
            .lock()
            .map_err(|_| MusicStreamError::Internal("sender task lock poisoned".to_owned()))?
            .take();
        let task_result = match task {
            Some(mut supervisor) => {
                match tokio::time::timeout(SENDER_STOP_TIMEOUT, &mut supervisor).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(error)) => Err(MusicStreamError::Internal(format!(
                        "RTP sender supervisor failed: {error}"
                    ))),
                    Err(_) => {
                        self.task.worker_abort.abort();
                        supervisor.abort();
                        let _ = supervisor.await;
                        Err(MusicStreamError::Internal(
                            "RTP sender did not stop within 2 seconds".to_owned(),
                        ))
                    }
                }
            }
            None => Ok(()),
        };
        match (command_result, task_result) {
            (_, Err(error)) | (Err(error), Ok(())) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    async fn request(
        &self,
        command: impl FnOnce(tokio::sync::oneshot::Sender<()>) -> SenderCommand,
    ) -> Result<()> {
        let (reply, receiver) = tokio::sync::oneshot::channel();
        tokio::time::timeout(SENDER_COMMAND_TIMEOUT, async {
            self.commands
                .send(command(reply))
                .await
                .map_err(|_| MusicStreamError::StreamClosed("RTP sender closed".to_owned()))?;
            receiver.await.map_err(|_| {
                MusicStreamError::StreamClosed(
                    "RTP sender closed before command acknowledgement".to_owned(),
                )
            })
        })
        .await
        .map_err(|_| {
            MusicStreamError::RtpSendError(
                "RTP sender command exceeded the 2 second deadline".to_owned(),
            )
        })?
    }
}

fn supervise_sender(
    worker: tokio::task::JoinHandle<Result<()>>,
    active_generation: Arc<AtomicU64>,
    events: mpsc::Sender<WorkerEvent>,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let result = match worker.await {
            Ok(result) => result,
            Err(error) => Err(MusicStreamError::Internal(format!(
                "RTP sender task failed: {error}"
            ))),
        };
        if let Err(error) = &result {
            let generation = active_generation.load(Ordering::Acquire);
            if generation != 0 {
                let _ = events
                    .send(WorkerEvent::CurrentFailed {
                        generation,
                        code: error.code(),
                        message: error.to_string(),
                    })
                    .await;
            }
        }
        result
    })
}

#[derive(Debug)]
enum SenderCommand {
    Activate {
        generation: u64,
        start_position_ms: u64,
        paused: bool,
        receiver: OpusQueueReceiver,
        reply: tokio::sync::oneshot::Sender<()>,
    },
    Deactivate {
        generation: u64,
        reply: tokio::sync::oneshot::Sender<()>,
    },
    Pause {
        generation: u64,
        reply: tokio::sync::oneshot::Sender<()>,
    },
    Resume {
        generation: u64,
        reply: tokio::sync::oneshot::Sender<()>,
    },
    Shutdown {
        reply: tokio::sync::oneshot::Sender<()>,
    },
}

struct ActiveMedia {
    generation: u64,
    start_position_ms: u64,
    receiver: OpusQueueReceiver,
    started: bool,
    prebuffer_reported: bool,
    first_packet: bool,
    deadline: Option<Instant>,
    media_sent_ms: u64,
    activation_started: Option<Instant>,
    first_packet_started: Option<Instant>,
}

struct SenderRuntime {
    config: RtpTransportConfig,
    prebuffer_ms: u64,
    max_playout_lateness: Duration,
    rtcp_interval: Duration,
    commands: mpsc::Receiver<SenderCommand>,
    progress: watch::Sender<StreamRuntimeProgress>,
    events: mpsc::Sender<WorkerEvent>,
}

async fn run_sender(
    socket: UdpSocket,
    rtcp_socket: Option<UdpSocket>,
    runtime: SenderRuntime,
    active_generation: Arc<AtomicU64>,
) -> Result<()> {
    let SenderRuntime {
        config,
        prebuffer_ms,
        max_playout_lateness,
        rtcp_interval,
        mut commands,
        progress: progress_tx,
        events,
    } = runtime;
    let packetizer = match RtpPacketizer::new(config.packetizer_config()) {
        Ok(packetizer) => packetizer,
        Err(error) => {
            tracing::error!(error = %error, "RTP packetizer initialization failed");
            return Err(error);
        }
    };
    let mut scratch = BytesMut::new();
    let mut active: Option<ActiveMedia> = None;
    let mut paused = false;
    let mut sequence = rand::random::<u16>();
    let mut rtp_timestamp = rand::random::<u32>();
    let mut packets_sent = 0_u64;
    let mut bytes_sent = 0_u64;
    let mut octets_sent = 0_u64;
    let mut dropped_frames = 0_u64;
    let mut dropped_media_ms = 0_u64;
    let mut latency_recoveries = 0_u64;
    let mut max_lateness_ms = 0_u64;
    let mut receiver_reports = 0_usize;
    let mut latest_receiver_report = None;
    let mut quality_window = RtcpQualityWindow::default();
    let mut quality_level: Option<RtcpNetworkQualityLevel> = None;
    let mut pending_events = PendingWorkerEvents::default();
    let mut rtcp = tokio::time::interval(rtcp_interval);
    rtcp.set_missed_tick_behavior(MissedTickBehavior::Skip);
    rtcp.tick().await;

    loop {
        flush_worker_events(&events, &mut pending_events);
        if let Some(media) = active.as_mut() {
            let buffered_ms = media.receiver.buffered_ms();
            let source_closed = media.receiver.is_closed();
            metrics::gauge!("music_stream.runtime.sender_buffer_ms").set(buffered_ms as f64);
            if !media.started
                && !paused
                && (buffered_ms >= prebuffer_ms || (source_closed && buffered_ms > 0))
            {
                media.started = true;
                media.deadline = Some(Instant::now());
                metrics::counter!("music_stream.runtime.prebuffer_ready").increment(1);
                if let Some(started) = media.activation_started.take() {
                    metrics::histogram!("music_stream.runtime.activation_to_prebuffer_us")
                        .record(started.elapsed().as_micros() as f64);
                }
                if !media.prebuffer_reported {
                    emit_worker_event(
                        &events,
                        &mut pending_events,
                        WorkerEvent::CurrentPrebufferReady {
                            generation: media.generation,
                        },
                    );
                    media.prebuffer_reported = true;
                }
            }
            if media.receiver.is_drained() {
                let generation = media.generation;
                active = None;
                active_generation.store(0, Ordering::Release);
                emit_worker_event(
                    &events,
                    &mut pending_events,
                    WorkerEvent::CurrentEnded { generation },
                );
                continue;
            }
        }

        let deadline = active
            .as_ref()
            .filter(|media| media.started && !paused)
            .and_then(|media| media.deadline);
        let has_pending_events = !pending_events.is_empty();
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(SenderCommand::Activate { generation, start_position_ms, paused: initial_paused, receiver, reply }) => {
                        metrics::counter!("music_stream.runtime.sender_activations").increment(1);
                        let activation_started = (!initial_paused).then(Instant::now);
                        active = Some(ActiveMedia {
                            generation,
                            start_position_ms,
                            receiver,
                            started: false,
                            prebuffer_reported: false,
                            first_packet: true,
                            deadline: None,
                            media_sent_ms: 0,
                            activation_started,
                            first_packet_started: activation_started,
                        });
                        active_generation.store(generation, Ordering::Release);
                        paused = initial_paused;
                        let _ = reply.send(());
                    }
                    Some(SenderCommand::Deactivate { generation, reply }) => {
                        if active.as_ref().is_some_and(|media| media.generation == generation) {
                            active = None;
                            active_generation.store(0, Ordering::Release);
                        }
                        let _ = reply.send(());
                    }
                    Some(SenderCommand::Pause { generation, reply }) => {
                        if let Some(media) = active.as_mut().filter(|media| media.generation == generation) {
                            paused = true;
                            if !media.started {
                                media.activation_started = None;
                                media.first_packet_started = None;
                            }
                        }
                        let _ = reply.send(());
                    }
                    Some(SenderCommand::Resume { generation, reply }) => {
                        if let Some(media) = active.as_mut().filter(|media| media.generation == generation) {
                            paused = false;
                            media.deadline = Some(Instant::now());
                            if !media.started {
                                let activation_started = Instant::now();
                                media.activation_started = Some(activation_started);
                                media.first_packet_started = Some(activation_started);
                            }
                        }
                        let _ = reply.send(());
                    }
                    Some(SenderCommand::Shutdown { reply }) => {
                        active_generation.store(0, Ordering::Release);
                        let _ = reply.send(());
                        return Ok(());
                    }
                    None => return Ok(()),
                }
            }
            _ = async {
                match active.as_mut() {
                    Some(media) => media.receiver.changed().await,
                    None => std::future::pending().await,
                }
            } => {}
            _ = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                let Some(media) = active.as_mut() else { continue; };
                let mut scheduled_deadline = media.deadline.unwrap_or_else(Instant::now);
                let observed_lateness = Instant::now().saturating_duration_since(scheduled_deadline);
                max_lateness_ms = max_lateness_ms.max(
                    u64::try_from(observed_lateness.as_millis()).unwrap_or(u64::MAX),
                );
                metrics::histogram!("music_stream.runtime.pacing_late_us")
                    .record(observed_lateness.as_micros() as f64);

                let mut recovered = false;
                while Instant::now().saturating_duration_since(scheduled_deadline)
                    > max_playout_lateness
                {
                    let Some(stale) = media.receiver.try_drop_oldest_if_followed() else {
                        break;
                    };
                    recovered = true;
                    dropped_frames = dropped_frames.saturating_add(1);
                    dropped_media_ms = dropped_media_ms.saturating_add(stale.duration_ms);
                    rtp_timestamp = rtp_timestamp.wrapping_add(stale.samples_per_channel);
                    media.media_sent_ms = media.media_sent_ms.saturating_add(stale.duration_ms);
                    scheduled_deadline += Duration::from_millis(stale.duration_ms.max(1));
                    metrics::counter!("music_stream.runtime.late_frames_dropped").increment(1);
                    metrics::counter!("music_stream.runtime.late_media_ms_dropped")
                        .increment(stale.duration_ms);
                }
                if recovered {
                    latency_recoveries = latency_recoveries.saturating_add(1);
                    metrics::counter!("music_stream.runtime.latency_recoveries").increment(1);
                }
                let Some(mut frame) = media.receiver.try_recv() else {
                    metrics::counter!("music_stream.runtime.underruns").increment(1);
                    media.started = false;
                    media.deadline = None;
                    continue;
                };
                let is_first_packet = media.first_packet;
                if is_first_packet {
                    frame.marker = true;
                    media.first_packet = false;
                }
                let duration_ms = frame.duration_ms;
                let samples = frame.samples_per_channel;
                let payload_len = frame.payload.len();
                let mut send_failure = None;
                match packetizer.packetize(frame, rtp_timestamp, sequence, &mut scratch) {
                    Ok(()) => match tokio::time::timeout(
                        DATAGRAM_SEND_TIMEOUT,
                        socket.send(&scratch),
                    ).await {
                        Ok(Ok(sent)) if sent == scratch.len() => {
                            packets_sent = packets_sent.saturating_add(1);
                            bytes_sent = bytes_sent.saturating_add(sent as u64);
                            octets_sent = octets_sent.saturating_add(payload_len as u64);
                            metrics::counter!("music_stream.runtime.rtp_packets").increment(1);
                            metrics::counter!("music_stream.runtime.rtp_bytes")
                                .increment(sent as u64);
                            if is_first_packet
                                && let Some(started) = media.first_packet_started.take()
                            {
                                metrics::histogram!(
                                    "music_stream.runtime.activation_to_first_packet_us"
                                )
                                .record(started.elapsed().as_micros() as f64);
                            }
                            sequence = sequence.wrapping_add(1);
                            rtp_timestamp = rtp_timestamp.wrapping_add(samples);
                            media.media_sent_ms = media.media_sent_ms.saturating_add(duration_ms);
                            let duration = Duration::from_millis(duration_ms.max(1));
                            let anchored = scheduled_deadline + duration;
                            let now = Instant::now();
                            media.deadline = Some(if anchored <= now { now + duration } else { anchored });
                            let _ = progress_tx.send(StreamRuntimeProgress {
                                generation: media.generation,
                                start_position_ms: media.start_position_ms,
                                media_sent_ms: media.media_sent_ms,
                                packets_sent,
                                bytes_sent,
                                dropped_frames,
                                dropped_media_ms,
                                latency_recoveries,
                                max_lateness_ms,
                                sequence,
                                rtp_timestamp,
                                latest_receiver_report,
                            });
                        }
                        Ok(Ok(sent)) => {
                            send_failure = Some(MusicStreamError::RtpSendError(format!(
                                "partial UDP datagram send: {sent} bytes"
                            )));
                        }
                        Ok(Err(error)) => {
                            send_failure = Some(MusicStreamError::RtpSendError(error.to_string()));
                        }
                        Err(_) => {
                            send_failure = Some(MusicStreamError::RtpSendError(
                                "RTP datagram send exceeded the 1 second deadline".to_owned(),
                            ));
                        }
                    },
                    Err(error) => send_failure = Some(error),
                }
                if let Some(error) = send_failure {
                    let generation = media.generation;
                    active = None;
                    active_generation.store(0, Ordering::Release);
                    emit_worker_event(
                        &events,
                        &mut pending_events,
                        WorkerEvent::CurrentFailed {
                            generation,
                            code: error.code(),
                            message: error.to_string(),
                        },
                    );
                }
            }
            permit = async {
                if has_pending_events {
                    events.reserve().await.ok()
                } else {
                    std::future::pending().await
                }
            } => {
                if let (Some(permit), Some(event)) = (permit, pending_events.pop()) {
                    permit.send(event);
                }
            }
            _ = rtcp.tick() => {
                if packets_sent > 0
                    && let Ok(report) = build_rtcp_sender_report(
                        config.ssrc,
                        rtp_timestamp,
                        packets_sent.min(u64::from(u32::MAX)) as u32,
                        octets_sent.min(u64::from(u32::MAX)) as u32,
                        SystemTime::now(),
                    )
                {
                    let target = rtcp_socket.as_ref().unwrap_or(&socket);
                    if matches!(
                        tokio::time::timeout(DATAGRAM_SEND_TIMEOUT, target.send(&report.bytes)).await,
                        Ok(Ok(_))
                    ) {
                        metrics::counter!("music_stream.runtime.rtcp_sender_reports").increment(1);
                    }
                }
            }
            received = recv_rtcp(&socket, rtcp_socket.as_ref()) => {
                if let Ok(Some(bytes)) = received
                    && let Ok(Some(snapshot)) = parse_rtcp_receiver_reports(bytes, config.ssrc, receiver_reports)
                {
                    receiver_reports = snapshot.reports_received;
                    metrics::counter!("music_stream.runtime.rtcp_receiver_reports").increment(1);
                    latest_receiver_report = Some(snapshot);
                    let quality = quality_window.observe(snapshot);
                    if quality_level != Some(quality.level) {
                        quality_level = Some(quality.level);
                        if let Some(media) = active.as_ref() {
                            emit_worker_event(
                                &events,
                                &mut pending_events,
                                WorkerEvent::CurrentNetworkQualityChanged {
                                    generation: media.generation,
                                    quality: quality.level,
                                    snapshot: quality,
                                },
                            );
                        }
                    }
                    if let Some(media) = active.as_ref() {
                        let _ = progress_tx.send(StreamRuntimeProgress {
                            generation: media.generation,
                            start_position_ms: media.start_position_ms,
                            media_sent_ms: media.media_sent_ms,
                            packets_sent,
                            bytes_sent,
                            dropped_frames,
                            dropped_media_ms,
                            latency_recoveries,
                            max_lateness_ms,
                            sequence,
                            rtp_timestamp,
                            latest_receiver_report,
                        });
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct PendingWorkerEvents {
    prebuffer: Option<WorkerEvent>,
    terminal: Option<WorkerEvent>,
    quality: Option<WorkerEvent>,
}

impl PendingWorkerEvents {
    fn store(&mut self, event: WorkerEvent) {
        match event {
            event @ WorkerEvent::CurrentPrebufferReady { .. } => self.prebuffer = Some(event),
            event @ (WorkerEvent::CurrentEnded { .. } | WorkerEvent::CurrentFailed { .. }) => {
                self.terminal = Some(event);
            }
            event @ WorkerEvent::CurrentNetworkQualityChanged { .. } => {
                self.quality = Some(event);
            }
            WorkerEvent::NextReady { .. } | WorkerEvent::NextFailed { .. } => {
                unreachable!("producer events never originate from the RTP sender");
            }
        }
    }

    fn pop(&mut self) -> Option<WorkerEvent> {
        self.prebuffer
            .take()
            .or_else(|| self.terminal.take())
            .or_else(|| self.quality.take())
    }

    fn is_empty(&self) -> bool {
        self.prebuffer.is_none() && self.terminal.is_none() && self.quality.is_none()
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

fn emit_worker_event(
    sender: &mpsc::Sender<WorkerEvent>,
    pending: &mut PendingWorkerEvents,
    event: WorkerEvent,
) {
    match sender.try_send(event) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(event)) => pending.store(event),
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

fn flush_worker_events(sender: &mpsc::Sender<WorkerEvent>, pending: &mut PendingWorkerEvents) {
    while let Some(event) = pending.pop() {
        match sender.try_send(event) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(event)) => {
                pending.store(event);
                return;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                pending.clear();
                return;
            }
        }
    }
}

async fn recv_rtcp(rtp: &UdpSocket, rtcp: Option<&UdpSocket>) -> Result<Option<Bytes>> {
    let mut buffer = [0_u8; 1_500];
    let len = rtcp
        .unwrap_or(rtp)
        .recv(&mut buffer)
        .await
        .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
    Ok((len > 0).then(|| Bytes::copy_from_slice(&buffer[..len])))
}

#[cfg(test)]
mod tests {
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::audio::frame::OpusFrame;
    use crate::runtime::opus_queue;

    #[tokio::test]
    async fn sender_panic_is_published_for_the_active_generation() {
        let active_generation = Arc::new(AtomicU64::new(42));
        let (events, mut event_rx) = mpsc::channel(1);
        let worker = tokio::spawn(async {
            panic!("injected sender panic");
            #[allow(unreachable_code)]
            Ok(())
        });
        let supervisor = supervise_sender(worker, active_generation, events);

        let event = event_rx.recv().await.expect("sender failure event");
        assert!(matches!(
            event,
            WorkerEvent::CurrentFailed {
                generation: 42,
                code: crate::ErrorCode::Internal,
                ..
            }
        ));
        assert!(supervisor.await.expect("supervisor").is_err());
    }

    #[tokio::test]
    async fn activating_paused_media_never_sends_before_resume() {
        let remote = UdpSocket::bind("127.0.0.1:0").await.expect("remote");
        let mut config = RtpTransportConfig::new(
            "127.0.0.1",
            remote.local_addr().expect("remote address").port(),
            77,
        );
        config.local_ip = "127.0.0.1".to_owned();
        let (events, _event_rx) = mpsc::channel(4);
        let sender = SenderHandle::spawn(config, 20, 100, Duration::from_secs(60), events)
            .await
            .expect("sender");
        let (output, receiver) = opus_queue::bounded(40);
        output
            .send_blocking(
                OpusFrame {
                    generation: 1,
                    payload: Bytes::from_static(b"opus"),
                    samples_per_channel: 960,
                    duration_ms: 20,
                    marker: false,
                    track_position_samples: 0,
                },
                &CancellationToken::new(),
            )
            .expect("queue frame");
        sender
            .activate(1, 0, true, receiver)
            .await
            .expect("activate paused");

        let mut packet = [0_u8; 1_500];
        assert!(
            tokio::time::timeout(Duration::from_millis(50), remote.recv(&mut packet))
                .await
                .is_err()
        );

        sender.resume(1).await.expect("resume");
        tokio::time::timeout(Duration::from_secs(1), remote.recv(&mut packet))
            .await
            .expect("RTP timeout")
            .expect("RTP receive");
        sender.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn excessive_sender_lateness_drops_media_without_consuming_sequence_numbers() {
        let remote = UdpSocket::bind("127.0.0.1:0").await.expect("remote");
        let mut config = RtpTransportConfig::new(
            "127.0.0.1",
            remote.local_addr().expect("remote address").port(),
            78,
        );
        config.local_ip = "127.0.0.1".to_owned();
        let (events, _event_rx) = mpsc::channel(4);
        let sender = SenderHandle::spawn(config, 20, 100, Duration::from_secs(60), events)
            .await
            .expect("sender");
        let (output, receiver) = opus_queue::bounded(400);
        let cancellation = CancellationToken::new();
        for position in 0..20 {
            output
                .send_blocking(
                    OpusFrame {
                        generation: 1,
                        payload: Bytes::from_static(b"opus"),
                        samples_per_channel: 960,
                        duration_ms: 20,
                        marker: false,
                        track_position_samples: position * 960,
                    },
                    &cancellation,
                )
                .expect("queue frame");
        }
        sender
            .activate(1, 0, false, receiver)
            .await
            .expect("activate");

        let mut first = [0_u8; 1_500];
        let first_len = tokio::time::timeout(Duration::from_secs(1), remote.recv(&mut first))
            .await
            .expect("first RTP timeout")
            .expect("first RTP receive");
        assert!(first_len >= 12);

        // This intentionally stalls the single-threaded test runtime, modeling
        // a scheduler pause that also prevents the sender task from running.
        std::thread::sleep(Duration::from_millis(250));

        let mut second = [0_u8; 1_500];
        let second_len = tokio::time::timeout(Duration::from_secs(1), remote.recv(&mut second))
            .await
            .expect("second RTP timeout")
            .expect("second RTP receive");
        assert!(second_len >= 12);

        let first_sequence = u16::from_be_bytes([first[2], first[3]]);
        let second_sequence = u16::from_be_bytes([second[2], second[3]]);
        let first_timestamp = u32::from_be_bytes([first[4], first[5], first[6], first[7]]);
        let second_timestamp = u32::from_be_bytes([second[4], second[5], second[6], second[7]]);
        let progress = sender.progress();
        assert!(progress.dropped_frames > 0);
        assert_eq!(second_sequence, first_sequence.wrapping_add(1));
        assert_eq!(
            second_timestamp.wrapping_sub(first_timestamp),
            u32::try_from(progress.dropped_frames + 1).expect("frame count") * 960
        );
        assert_eq!(progress.dropped_media_ms, progress.dropped_frames * 20);
        assert_eq!(progress.latency_recoveries, 1);

        sender.shutdown().await.expect("shutdown");
    }
}
