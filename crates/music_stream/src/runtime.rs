//! Runtime glue for a concrete local-file RTP playback task.
//!
//! This layer owns thread lifecycle, stop/volume controls, and sleeping until
//! the pure RTP pacer says the next packet is due. Media work stays in the
//! playout pipeline and slot runner.

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
mod local_file_rtp {
    use std::fmt;
    use std::sync::atomic::{
        AtomicBool, AtomicI16, AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering,
    };
    use std::sync::{Arc, Condvar, Mutex, OnceLock};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant, SystemTime};

    use crate::audio::decode::DecoderBackend;
    use crate::audio::opus::OpusEncoderBackend;
    use crate::audio::pipeline::{PipelineConfig, WorkerTurnReport};
    use crate::error::{MusicStreamError, Result};
    use crate::model::{GainLevel, TrackSource, VolumeLevel, WatermarkConfig};
    use crate::quality::{RtcpNetworkQualityLevel, RtcpQualityWindow, RtcpQualityWindowSnapshot};
    use crate::session::WorkerEvent;
    use crate::slot::{
        LiveStreamSlotConfig, LocalFileSlotConfig, LocalFileSlotDriver, RtpSlotDrainReport,
        RtpSlotRunner, SlotRole, SlotTurnReport,
    };
    use crate::source::{FileSourceResolver, HttpLiveStreamConfig, HttpLiveStreamStopHandle};
    use crate::transport::{
        RtcpReceiverReportSnapshot, RtcpSenderReportPacket, RtpPaceDecision, RtpPacketizer,
        RtpTransportConfig, UdpRtcpPacketSink, UdpRtpPacketSink, build_rtcp_sender_report,
        parse_rtcp_receiver_reports,
    };

    const DEFAULT_SAMPLE_RATE: u32 = 48_000;
    const DEFAULT_CHANNELS: u16 = 2;
    const DEFAULT_FRAME_SAMPLES_PER_CHANNEL: u32 = 960;
    const DEFAULT_PREBUFFER_MS: u64 = 100;
    const DEFAULT_MAX_PACKETS_PER_TICK: usize = 4;
    const DEFAULT_IDLE_SLEEP_MS: u64 = 2;
    const DEFAULT_RTCP_REPORT_INTERVAL_MS: u64 = 5_000;
    const MAX_RTCP_RECV_PER_TICK: usize = 4;
    const RTP_FIXED_HEADER_LEN: usize = 12;
    const MAX_SLEEP_MS: u64 = 20;
    const CURRENT_CPU_PERMIT_BUSY_METRIC: &str = "music_stream.runtime.current.cpu_permit_busy";
    const PRELOAD_CPU_PERMIT_BUSY_METRIC: &str = "music_stream.runtime.preload.cpu_permit_busy";
    const CURRENT_RTCP_REPORTS_SENT_METRIC: &str = "music_stream.runtime.current.rtcp_reports_sent";
    const CURRENT_RTCP_BYTES_SENT_METRIC: &str = "music_stream.runtime.current.rtcp_bytes_sent";
    const CURRENT_RTCP_RECEIVER_REPORTS_METRIC: &str =
        "music_stream.runtime.current.rtcp_receiver_reports";
    const CURRENT_RTCP_QUALITY_WINDOW_REPORTS_METRIC: &str =
        "music_stream.runtime.current.rtcp_quality.window_reports";
    const CURRENT_RTCP_QUALITY_LATEST_LOSS_PERCENT_METRIC: &str =
        "music_stream.runtime.current.rtcp_quality.latest_loss_percent";
    const CURRENT_RTCP_QUALITY_AVERAGE_LOSS_PERCENT_METRIC: &str =
        "music_stream.runtime.current.rtcp_quality.average_loss_percent";
    const CURRENT_RTCP_QUALITY_MAX_LOSS_PERCENT_METRIC: &str =
        "music_stream.runtime.current.rtcp_quality.max_loss_percent";
    const CURRENT_RTCP_QUALITY_AVERAGE_JITTER_MS_METRIC: &str =
        "music_stream.runtime.current.rtcp_quality.average_jitter_ms";
    const CURRENT_RTCP_QUALITY_MAX_JITTER_MS_METRIC: &str =
        "music_stream.runtime.current.rtcp_quality.max_jitter_ms";
    const CURRENT_RTCP_QUALITY_AVERAGE_RTT_MS_METRIC: &str =
        "music_stream.runtime.current.rtcp_quality.average_rtt_ms";
    const CURRENT_RTCP_QUALITY_MAX_RTT_MS_METRIC: &str =
        "music_stream.runtime.current.rtcp_quality.max_rtt_ms";
    const CURRENT_RTP_PACKETS_SENT_METRIC: &str = "music_stream.runtime.current.rtp_packets_sent";
    const CURRENT_RTP_BYTES_SENT_METRIC: &str = "music_stream.runtime.current.rtp_bytes_sent";
    const CURRENT_RTP_PAYLOAD_BYTES_SENT_METRIC: &str =
        "music_stream.runtime.current.rtp_payload_bytes_sent";
    const CURRENT_RTP_MEDIA_SENT_MS_METRIC: &str = "music_stream.runtime.current.rtp_media_sent_ms";
    const CURRENT_RTP_MAX_PACING_LATE_MS_METRIC: &str =
        "music_stream.runtime.current.rtp_max_pacing_late_ms";
    const CURRENT_RTP_PACING_LATE_MS_METRIC: &str =
        "music_stream.runtime.current.rtp_pacing_late_ms";
    const CURRENT_RTP_PREBUFFER_WAITS_METRIC: &str =
        "music_stream.runtime.current.rtp_prebuffer_waits";
    const CURRENT_RTP_UNDERRUNS_METRIC: &str = "music_stream.runtime.current.rtp_underruns";
    const CURRENT_RTP_PACING_WAITS_METRIC: &str = "music_stream.runtime.current.rtp_pacing_waits";

    static CURRENT_CPU_LANE: OnceLock<CpuPermitLane> = OnceLock::new();
    static PRELOAD_CPU_LANE: OnceLock<CpuPermitLane> = OnceLock::new();

    #[derive(Clone)]
    pub struct LocalFileRtpPlaybackConfig {
        pub pipeline: PipelineConfig,
        pub transport: RtpTransportConfig,
        pub source_resolver: FileSourceResolver,
        pub live_http: HttpLiveStreamConfig,
        pub metrics_recorder: Option<Arc<dyn metrics::Recorder + Send + Sync>>,
        pub gain: GainLevel,
        pub start_position_ms: u64,
        pub max_packets_per_tick: usize,
        pub rtcp_report_interval: Duration,
        pub idle_sleep: Duration,
    }

    impl fmt::Debug for LocalFileRtpPlaybackConfig {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("LocalFileRtpPlaybackConfig")
                .field("pipeline", &self.pipeline)
                .field("transport", &self.transport)
                .field("source_resolver", &self.source_resolver)
                .field("live_http", &self.live_http)
                .field(
                    "metrics_recorder",
                    &self.metrics_recorder.as_ref().map(|_| "<metrics recorder>"),
                )
                .field("gain", &self.gain)
                .field("start_position_ms", &self.start_position_ms)
                .field("max_packets_per_tick", &self.max_packets_per_tick)
                .field("rtcp_report_interval", &self.rtcp_report_interval)
                .field("idle_sleep", &self.idle_sleep)
                .finish()
        }
    }

    impl LocalFileRtpPlaybackConfig {
        #[must_use]
        pub fn new(generation: u64, transport: RtpTransportConfig) -> Self {
            Self {
                pipeline: default_pipeline_config(generation),
                transport,
                source_resolver: FileSourceResolver::default(),
                live_http: HttpLiveStreamConfig::default(),
                metrics_recorder: None,
                gain: GainLevel::default(),
                start_position_ms: 0,
                max_packets_per_tick: DEFAULT_MAX_PACKETS_PER_TICK,
                rtcp_report_interval: Duration::from_millis(DEFAULT_RTCP_REPORT_INTERVAL_MS),
                idle_sleep: Duration::from_millis(DEFAULT_IDLE_SLEEP_MS),
            }
        }

        pub fn validate(&self) -> Result<()> {
            self.pipeline.validate()?;
            self.transport.validate()?;
            self.source_resolver.validate()?;
            self.live_http.validate()?;
            if self.max_packets_per_tick == 0 {
                return Err(MusicStreamError::InvalidConfig(
                    "max_packets_per_tick must be greater than zero".to_owned(),
                ));
            }
            if self.rtcp_report_interval.is_zero() {
                return Err(MusicStreamError::InvalidConfig(
                    "rtcp_report_interval must be greater than zero".to_owned(),
                ));
            }
            if let Some(opus_bitrate_bps) = self.transport.opus_bitrate_bps
                && opus_bitrate_bps > i32::MAX as u32
            {
                return Err(MusicStreamError::InvalidConfig(
                    "Opus bitrate must fit in i32".to_owned(),
                ));
            }
            Ok(())
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct LocalFileRtpPlaybackReport {
        pub generation: u64,
        pub events: Vec<WorkerEvent>,
        pub drain: RtpSlotDrainReport,
        pub rtcp_reports_sent: usize,
        pub rtcp_bytes_sent: usize,
        pub latest_receiver_report: Option<RtcpReceiverReportSnapshot>,
        pub completed: bool,
        pub stopped: bool,
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct LocalFileRtpPlaybackProgress {
        pub start_position_ms: u64,
        pub media_sent_ms: u64,
        pub stream_position_ms: u64,
        pub packets_sent: usize,
        pub bytes_sent: usize,
        pub rtcp_reports_sent: usize,
        pub rtcp_bytes_sent: usize,
        pub latest_receiver_report: Option<RtcpReceiverReportSnapshot>,
    }

    #[derive(Debug)]
    pub struct LocalFilePreloadReport {
        pub generation: u64,
        pub events: Vec<WorkerEvent>,
        pub ready: bool,
        pub stopped: bool,
        pub driver: LocalFileSlotDriver,
    }

    impl LocalFilePreloadReport {
        #[must_use]
        pub fn into_current_driver(self) -> LocalFileSlotDriver {
            self.driver.into_role(SlotRole::Current)
        }
    }

    #[derive(Debug)]
    pub struct LocalFilePreload {
        generation: u64,
        control: Arc<PlaybackControl>,
        completion: LocalFilePreloadCompletion,
        join: Option<JoinHandle<Result<LocalFilePreloadReport>>>,
    }

    #[derive(Clone, Debug)]
    pub struct LocalFilePreloadCompletion {
        finished: Arc<AtomicBool>,
        notify: Arc<tokio::sync::Notify>,
    }

    impl Default for LocalFilePreloadCompletion {
        fn default() -> Self {
            Self::new()
        }
    }

    impl LocalFilePreloadCompletion {
        #[must_use]
        pub fn new() -> Self {
            Self {
                finished: Arc::new(AtomicBool::new(false)),
                notify: Arc::new(tokio::sync::Notify::new()),
            }
        }

        #[must_use]
        pub fn is_finished(&self) -> bool {
            self.finished.load(Ordering::Acquire)
        }

        pub async fn wait(&self) {
            if self.is_finished() {
                return;
            }

            let notified = self.notify.notified();
            tokio::pin!(notified);
            loop {
                notified.as_mut().enable();
                if self.is_finished() {
                    return;
                }
                notified.as_mut().await;
                notified.set(self.notify.notified());
            }
        }

        fn mark_finished(&self) {
            self.finished.store(true, Ordering::Release);
            self.notify.notify_waiters();
        }
    }

    #[derive(Debug)]
    struct LocalFilePreloadCompletionGuard(LocalFilePreloadCompletion);

    impl Drop for LocalFilePreloadCompletionGuard {
        fn drop(&mut self) {
            self.0.mark_finished();
        }
    }

    impl LocalFilePreload {
        #[must_use]
        pub fn generation(&self) -> u64 {
            self.generation
        }

        #[must_use]
        pub fn is_finished(&self) -> bool {
            self.completion.is_finished() || self.join.as_ref().is_some_and(JoinHandle::is_finished)
        }

        #[must_use]
        pub fn completion(&self) -> LocalFilePreloadCompletion {
            self.completion.clone()
        }

        pub fn set_volume(&self, volume: VolumeLevel) {
            self.control.set_volume(volume);
        }

        pub fn set_gain(&self, gain: GainLevel) {
            self.control.set_gain(gain);
        }

        pub fn stop(&self) {
            self.control.stop();
        }

        pub fn join(mut self) -> Result<LocalFilePreloadReport> {
            let Some(join) = self.join.take() else {
                return Err(MusicStreamError::Internal(
                    "preload join handle already consumed".to_owned(),
                ));
            };
            join.join()
                .map_err(|_| MusicStreamError::Internal("preload worker panicked".to_owned()))?
        }
    }

    impl Drop for LocalFilePreload {
        fn drop(&mut self) {
            self.stop();
        }
    }

    #[derive(Debug)]
    pub struct LocalFileRtpPlayback {
        generation: u64,
        control: Arc<PlaybackControl>,
        live_stop: Option<HttpLiveStreamStopHandle>,
        join: Option<JoinHandle<Result<LocalFileRtpPlaybackReport>>>,
    }

    impl LocalFileRtpPlayback {
        #[must_use]
        pub fn generation(&self) -> u64 {
            self.generation
        }

        #[must_use]
        pub fn is_finished(&self) -> bool {
            self.join.as_ref().is_some_and(JoinHandle::is_finished)
        }

        pub fn set_volume(&self, volume: VolumeLevel) {
            self.control.set_volume(volume);
        }

        pub fn set_gain(&self, gain: GainLevel) {
            self.control.set_gain(gain);
        }

        #[must_use]
        pub fn progress(&self) -> LocalFileRtpPlaybackProgress {
            self.control.progress()
        }

        pub fn pause(&self) {
            self.control.set_paused(true);
        }

        pub fn resume(&self) {
            self.control.set_paused(false);
        }

        #[must_use]
        pub fn is_paused(&self) -> bool {
            self.control.paused.load(Ordering::Relaxed)
        }

        pub fn stop(&self) {
            if let Some(live_stop) = &self.live_stop {
                live_stop.stop();
            }
            self.control.stop();
        }

        pub fn join(mut self) -> Result<LocalFileRtpPlaybackReport> {
            let Some(join) = self.join.take() else {
                return Err(MusicStreamError::Internal(
                    "playback join handle already consumed".to_owned(),
                ));
            };
            join.join()
                .map_err(|_| MusicStreamError::Internal("playback worker panicked".to_owned()))?
        }
    }

    impl Drop for LocalFileRtpPlayback {
        fn drop(&mut self) {
            self.stop();
        }
    }

    #[derive(Debug)]
    enum RuntimeRtcpSink {
        Muxed,
        Dedicated(UdpRtcpPacketSink),
    }

    #[derive(Debug)]
    struct RtcpSenderReportRuntime {
        ssrc: u32,
        interval_ms: u64,
        next_due_ms: u64,
        packets_sent: u32,
        payload_octets_sent: u32,
        last_rtp_timestamp: Option<u32>,
        reports_sent: usize,
        bytes_sent: usize,
        receiver_reports_received: usize,
        latest_receiver_report: Option<RtcpReceiverReportSnapshot>,
        quality_window: RtcpQualityWindow,
        last_quality_level: Option<RtcpNetworkQualityLevel>,
        sink: RuntimeRtcpSink,
    }

    impl RtcpSenderReportRuntime {
        fn new(config: &RtpTransportConfig, interval: Duration) -> Result<Self> {
            let sink = if config.rtcp_mux {
                RuntimeRtcpSink::Muxed
            } else {
                RuntimeRtcpSink::Dedicated(UdpRtcpPacketSink::connect_config(config)?)
            };

            let interval_ms = interval.as_millis().try_into().unwrap_or(u64::MAX).max(1);
            Ok(Self {
                ssrc: config.ssrc,
                interval_ms,
                next_due_ms: interval_ms,
                packets_sent: 0,
                payload_octets_sent: 0,
                last_rtp_timestamp: None,
                reports_sent: 0,
                bytes_sent: 0,
                receiver_reports_received: 0,
                latest_receiver_report: None,
                quality_window: RtcpQualityWindow::default(),
                last_quality_level: None,
                sink,
            })
        }

        fn record_drain(&mut self, drain: &RtpSlotDrainReport) {
            self.packets_sent = self
                .packets_sent
                .saturating_add(drain.packets_sent.try_into().unwrap_or(u32::MAX));
            self.payload_octets_sent = self
                .payload_octets_sent
                .saturating_add(drain.payload_bytes_sent.try_into().unwrap_or(u32::MAX));
            self.last_rtp_timestamp = drain.last_rtp_timestamp.or(self.last_rtp_timestamp);
        }

        fn due_report(
            &self,
            now_ms: u64,
            now: SystemTime,
        ) -> Result<Option<RtcpSenderReportPacket>> {
            let Some(last_rtp_timestamp) = self.last_rtp_timestamp else {
                return Ok(None);
            };
            if now_ms < self.next_due_ms {
                return Ok(None);
            }

            build_rtcp_sender_report(
                self.ssrc,
                last_rtp_timestamp,
                self.packets_sent,
                self.payload_octets_sent,
                now,
            )
            .map(Some)
        }

        fn mark_sent(&mut self, now_ms: u64, bytes_sent: usize) {
            self.reports_sent += 1;
            self.bytes_sent += bytes_sent;
            self.next_due_ms = now_ms.saturating_add(self.interval_ms);
        }

        fn record_receiver_report(
            &mut self,
            snapshot: RtcpReceiverReportSnapshot,
        ) -> RtcpQualityWindowSnapshot {
            self.receiver_reports_received = snapshot.reports_received;
            self.latest_receiver_report = Some(snapshot);
            self.quality_window.observe(snapshot)
        }

        fn note_quality_level(&mut self, level: RtcpNetworkQualityLevel) -> bool {
            if self.last_quality_level == Some(level) {
                return false;
            }
            self.last_quality_level = Some(level);
            true
        }
    }

    #[derive(Clone, Copy)]
    struct WorkerMetricNames {
        turn_us: &'static str,
        decoded_chunks: &'static str,
        decoded_frames: &'static str,
        encoded_frames: &'static str,
        decoded_queue_ms: &'static str,
        encoded_queue_ms: &'static str,
        decoded_high_water_hits: &'static str,
        encoded_high_water_hits: &'static str,
        source_need_more: &'static str,
        source_ended: &'static str,
    }

    const CURRENT_WORKER_METRICS: WorkerMetricNames = WorkerMetricNames {
        turn_us: "music_stream.runtime.current.worker_turn_us",
        decoded_chunks: "music_stream.runtime.current.decoded_chunks",
        decoded_frames: "music_stream.runtime.current.decoded_frames",
        encoded_frames: "music_stream.runtime.current.encoded_frames",
        decoded_queue_ms: "music_stream.runtime.current.decoded_queue_ms",
        encoded_queue_ms: "music_stream.runtime.current.encoded_queue_ms",
        decoded_high_water_hits: "music_stream.runtime.current.decoded_high_water_hits",
        encoded_high_water_hits: "music_stream.runtime.current.encoded_high_water_hits",
        source_need_more: "music_stream.runtime.current.source_need_more",
        source_ended: "music_stream.runtime.current.source_ended",
    };

    const PRELOAD_WORKER_METRICS: WorkerMetricNames = WorkerMetricNames {
        turn_us: "music_stream.runtime.preload.worker_turn_us",
        decoded_chunks: "music_stream.runtime.preload.decoded_chunks",
        decoded_frames: "music_stream.runtime.preload.decoded_frames",
        encoded_frames: "music_stream.runtime.preload.encoded_frames",
        decoded_queue_ms: "music_stream.runtime.preload.decoded_queue_ms",
        encoded_queue_ms: "music_stream.runtime.preload.encoded_queue_ms",
        decoded_high_water_hits: "music_stream.runtime.preload.decoded_high_water_hits",
        encoded_high_water_hits: "music_stream.runtime.preload.encoded_high_water_hits",
        source_need_more: "music_stream.runtime.preload.source_need_more",
        source_ended: "music_stream.runtime.preload.source_ended",
    };

    #[derive(Debug)]
    struct CpuPermitLane {
        permits: Mutex<usize>,
        condvar: Condvar,
    }

    impl CpuPermitLane {
        fn new(permits: usize) -> Self {
            Self {
                permits: Mutex::new(permits.max(1)),
                condvar: Condvar::new(),
            }
        }

        fn try_acquire(&'static self, control: &PlaybackControl) -> Option<CpuPermit> {
            if control.stop.load(Ordering::Relaxed) {
                return None;
            }

            let Ok(mut permits) = self.permits.lock() else {
                return Some(CpuPermit { lane: None });
            };
            if *permits > 0 {
                *permits -= 1;
                Some(CpuPermit { lane: Some(self) })
            } else {
                None
            }
        }

        fn release(&self) {
            if let Ok(mut permits) = self.permits.lock() {
                *permits = permits.saturating_add(1);
                self.condvar.notify_one();
            }
        }
    }

    #[derive(Debug)]
    struct CpuPermit {
        lane: Option<&'static CpuPermitLane>,
    }

    impl Drop for CpuPermit {
        fn drop(&mut self) {
            if let Some(lane) = self.lane {
                lane.release();
            }
        }
    }

    fn current_cpu_lane() -> &'static CpuPermitLane {
        CURRENT_CPU_LANE.get_or_init(|| CpuPermitLane::new(available_cpu_parallelism()))
    }

    fn preload_cpu_lane() -> &'static CpuPermitLane {
        PRELOAD_CPU_LANE.get_or_init(|| {
            let permits = available_cpu_parallelism().saturating_sub(1).max(1);
            CpuPermitLane::new(permits)
        })
    }

    fn available_cpu_parallelism() -> usize {
        thread::available_parallelism().map_or(2, |parallelism| parallelism.get())
    }

    #[derive(Debug)]
    struct WorkerWake {
        epoch: Mutex<u64>,
        condvar: Condvar,
    }

    impl WorkerWake {
        fn new() -> Self {
            Self {
                epoch: Mutex::new(0),
                condvar: Condvar::new(),
            }
        }

        fn notify(&self) {
            if let Ok(mut epoch) = self.epoch.lock() {
                *epoch = epoch.wrapping_add(1);
                self.condvar.notify_all();
            }
        }

        fn epoch(&self) -> u64 {
            self.epoch.lock().map_or(0, |epoch| *epoch)
        }

        fn wait_timeout_since(&self, observed: u64, timeout: Duration) {
            let Ok(mut epoch) = self.epoch.lock() else {
                thread::sleep(timeout);
                return;
            };
            if *epoch == observed {
                let Ok((guard, _)) = self.condvar.wait_timeout(epoch, timeout) else {
                    thread::sleep(timeout);
                    return;
                };
                epoch = guard;
            }
            drop(epoch);
        }
    }

    #[derive(Debug)]
    struct PlaybackControl {
        stop: AtomicBool,
        paused: AtomicBool,
        wake: WorkerWake,
        volume_units: AtomicU16,
        gain_centibels: AtomicI16,
        start_position_ms: AtomicU64,
        media_sent_ms: AtomicU64,
        packets_sent: AtomicUsize,
        bytes_sent: AtomicUsize,
        rtcp_reports_sent: AtomicUsize,
        rtcp_bytes_sent: AtomicUsize,
        rtcp_receiver_reports_received: AtomicUsize,
        rr_sender_ssrc: AtomicU32,
        rr_source_ssrc: AtomicU32,
        rr_fraction_lost: AtomicU32,
        rr_total_lost: AtomicU32,
        rr_last_sequence_number: AtomicU32,
        rr_jitter: AtomicU32,
        rr_jitter_micros: AtomicU64,
        rr_last_sender_report: AtomicU32,
        rr_delay: AtomicU32,
        rr_round_trip_time_micros: AtomicU64,
    }

    impl PlaybackControl {
        fn new(
            initial_volume: VolumeLevel,
            initial_gain: GainLevel,
            start_position_ms: u64,
        ) -> Self {
            Self {
                stop: AtomicBool::new(false),
                paused: AtomicBool::new(false),
                wake: WorkerWake::new(),
                volume_units: AtomicU16::new(initial_volume.units()),
                gain_centibels: AtomicI16::new(initial_gain.centibels()),
                start_position_ms: AtomicU64::new(start_position_ms),
                media_sent_ms: AtomicU64::new(0),
                packets_sent: AtomicUsize::new(0),
                bytes_sent: AtomicUsize::new(0),
                rtcp_reports_sent: AtomicUsize::new(0),
                rtcp_bytes_sent: AtomicUsize::new(0),
                rtcp_receiver_reports_received: AtomicUsize::new(0),
                rr_sender_ssrc: AtomicU32::new(0),
                rr_source_ssrc: AtomicU32::new(0),
                rr_fraction_lost: AtomicU32::new(0),
                rr_total_lost: AtomicU32::new(0),
                rr_last_sequence_number: AtomicU32::new(0),
                rr_jitter: AtomicU32::new(0),
                rr_jitter_micros: AtomicU64::new(0),
                rr_last_sender_report: AtomicU32::new(0),
                rr_delay: AtomicU32::new(0),
                rr_round_trip_time_micros: AtomicU64::new(u64::MAX),
            }
        }

        fn set_volume(&self, volume: VolumeLevel) {
            let previous = self.volume_units.swap(volume.units(), Ordering::Relaxed);
            if previous != volume.units() {
                self.wake.notify();
            }
        }

        fn set_gain(&self, gain: GainLevel) {
            let previous = self
                .gain_centibels
                .swap(gain.centibels(), Ordering::Relaxed);
            if previous != gain.centibels() {
                self.wake.notify();
            }
        }

        fn set_paused(&self, paused: bool) {
            if self.paused.swap(paused, Ordering::Relaxed) != paused {
                self.wake.notify();
            }
        }

        fn stop(&self) {
            if !self.stop.swap(true, Ordering::Relaxed) {
                self.wake.notify();
            }
        }

        fn wake_epoch(&self) -> u64 {
            self.wake.epoch()
        }

        fn wait_timeout_since(&self, observed: u64, timeout: Duration) {
            self.wake.wait_timeout_since(observed, timeout);
        }

        fn record_drain(&self, drain: &RtpSlotDrainReport) {
            self.media_sent_ms
                .fetch_add(drain.media_sent_ms, Ordering::Relaxed);
            self.packets_sent
                .fetch_add(drain.packets_sent, Ordering::Relaxed);
            self.bytes_sent
                .fetch_add(drain.bytes_sent, Ordering::Relaxed);
        }

        fn progress(&self) -> LocalFileRtpPlaybackProgress {
            let start_position_ms = self.start_position_ms.load(Ordering::Relaxed);
            let media_sent_ms = self.media_sent_ms.load(Ordering::Relaxed);
            LocalFileRtpPlaybackProgress {
                start_position_ms,
                media_sent_ms,
                stream_position_ms: start_position_ms.saturating_add(media_sent_ms),
                packets_sent: self.packets_sent.load(Ordering::Relaxed),
                bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
                rtcp_reports_sent: self.rtcp_reports_sent.load(Ordering::Relaxed),
                rtcp_bytes_sent: self.rtcp_bytes_sent.load(Ordering::Relaxed),
                latest_receiver_report: self.receiver_report_snapshot(),
            }
        }

        fn record_rtcp_report(&self, bytes_sent: usize) {
            self.rtcp_reports_sent.fetch_add(1, Ordering::Relaxed);
            self.rtcp_bytes_sent
                .fetch_add(bytes_sent, Ordering::Relaxed);
        }

        fn record_receiver_report(&self, snapshot: RtcpReceiverReportSnapshot) {
            self.rr_sender_ssrc
                .store(snapshot.sender_ssrc, Ordering::Relaxed);
            self.rr_source_ssrc
                .store(snapshot.source_ssrc, Ordering::Relaxed);
            self.rr_fraction_lost
                .store(u32::from(snapshot.fraction_lost), Ordering::Relaxed);
            self.rr_total_lost
                .store(snapshot.total_lost, Ordering::Relaxed);
            self.rr_last_sequence_number
                .store(snapshot.last_sequence_number, Ordering::Relaxed);
            self.rr_jitter.store(snapshot.jitter, Ordering::Relaxed);
            self.rr_jitter_micros
                .store(snapshot.jitter_micros, Ordering::Relaxed);
            self.rr_last_sender_report
                .store(snapshot.last_sender_report, Ordering::Relaxed);
            self.rr_delay.store(snapshot.delay, Ordering::Relaxed);
            self.rr_round_trip_time_micros.store(
                snapshot.round_trip_time_micros.unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            self.rtcp_receiver_reports_received
                .store(snapshot.reports_received, Ordering::Release);
        }

        fn receiver_report_snapshot(&self) -> Option<RtcpReceiverReportSnapshot> {
            let reports_received = self.rtcp_receiver_reports_received.load(Ordering::Acquire);
            if reports_received == 0 {
                return None;
            }
            Some(RtcpReceiverReportSnapshot {
                reports_received,
                sender_ssrc: self.rr_sender_ssrc.load(Ordering::Relaxed),
                source_ssrc: self.rr_source_ssrc.load(Ordering::Relaxed),
                fraction_lost: self.rr_fraction_lost.load(Ordering::Relaxed) as u8,
                total_lost: self.rr_total_lost.load(Ordering::Relaxed),
                last_sequence_number: self.rr_last_sequence_number.load(Ordering::Relaxed),
                jitter: self.rr_jitter.load(Ordering::Relaxed),
                jitter_micros: self.rr_jitter_micros.load(Ordering::Relaxed),
                last_sender_report: self.rr_last_sender_report.load(Ordering::Relaxed),
                delay: self.rr_delay.load(Ordering::Relaxed),
                round_trip_time_micros: match self.rr_round_trip_time_micros.load(Ordering::Relaxed)
                {
                    u64::MAX => None,
                    value => Some(value),
                },
            })
        }
    }

    #[derive(Debug)]
    struct RuntimeControlMirror {
        last_volume_units: u16,
        last_gain_centibels: i16,
    }

    impl RuntimeControlMirror {
        fn new() -> Self {
            Self {
                last_volume_units: u16::MAX,
                last_gain_centibels: i16::MAX,
            }
        }

        fn update<D, E, S>(
            &mut self,
            control: &PlaybackControl,
            runner: &mut RtpSlotRunner<D, E, S>,
        ) -> Result<()>
        where
            D: DecoderBackend,
            E: OpusEncoderBackend,
            S: crate::transport::RtpPacketSink,
        {
            let volume_units = control.volume_units.load(Ordering::Relaxed);
            if volume_units != self.last_volume_units {
                let volume = VolumeLevel::from_units(volume_units)?;
                runner.set_volume(volume)?;
                self.last_volume_units = volume_units;
            }

            let gain_centibels = control.gain_centibels.load(Ordering::Relaxed);
            if gain_centibels != self.last_gain_centibels {
                let gain = GainLevel::from_centibels(gain_centibels)?;
                runner.set_gain(gain)?;
                self.last_gain_centibels = gain_centibels;
            }

            Ok(())
        }

        fn update_driver<D, E>(
            &mut self,
            control: &PlaybackControl,
            driver: &mut crate::slot::SlotDriver<D, E>,
        ) -> Result<()>
        where
            D: DecoderBackend,
            E: OpusEncoderBackend,
        {
            let volume_units = control.volume_units.load(Ordering::Relaxed);
            if volume_units != self.last_volume_units {
                let volume = VolumeLevel::from_units(volume_units)?;
                driver.set_volume(volume)?;
                self.last_volume_units = volume_units;
            }

            let gain_centibels = control.gain_centibels.load(Ordering::Relaxed);
            if gain_centibels != self.last_gain_centibels {
                let gain = GainLevel::from_centibels(gain_centibels)?;
                driver.set_gain(gain)?;
                self.last_gain_centibels = gain_centibels;
            }

            Ok(())
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum RuntimeWait {
        None,
        Idle,
        Pacing(RtpPaceDecision),
    }

    impl RuntimeWait {
        fn wait(self, control: &PlaybackControl, wake_epoch: u64, idle_sleep: Duration) {
            match self {
                Self::None => {}
                Self::Idle => control.wait_timeout_since(wake_epoch, idle_sleep),
                Self::Pacing(decision) => wait_for_pacer(control, wake_epoch, decision, idle_sleep),
            }
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum PlaybackTick {
        Continue(RuntimeWait),
        Completed,
        Stopped,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum PreloadTick {
        Continue(RuntimeWait),
        Ready,
        Stopped,
    }

    #[derive(Debug)]
    enum WorkerTurnAttempt {
        Ran(SlotTurnReport),
        Busy,
        Stopped,
    }

    struct PlaybackTickContext<'a, D, E, F>
    where
        D: DecoderBackend,
        E: OpusEncoderBackend,
        F: FnMut(WorkerEvent),
    {
        runner: &'a mut RtpSlotRunner<D, E, UdpRtpPacketSink>,
        rtcp: &'a mut RtcpSenderReportRuntime,
        report: &'a mut LocalFileRtpPlaybackReport,
        control: &'a PlaybackControl,
        config: &'a LocalFileRtpPlaybackConfig,
        control_mirror: &'a mut RuntimeControlMirror,
        on_event: &'a mut F,
    }

    pub fn spawn_local_file_rtp_playback<F>(
        track: TrackSource,
        config: LocalFileRtpPlaybackConfig,
        initial_volume: VolumeLevel,
        on_event: F,
    ) -> Result<LocalFileRtpPlayback>
    where
        F: FnMut(WorkerEvent) + Send + 'static,
    {
        validate_track(&track)?;
        config.validate()?;

        let generation = config.pipeline.generation;
        let slot_config = slot_config_for_runtime(&config);
        let source_resolver = config.source_resolver.clone();
        let built = with_optional_metrics_recorder(&config.metrics_recorder, || {
            crate::slot::build_local_file_slot_with_resolver(
                SlotRole::Current,
                &track,
                slot_config,
                &source_resolver,
            )
        })?;
        spawn_local_file_rtp_playback_from_driver(built.driver, config, initial_volume, on_event)
            .map(|mut playback| {
                playback.generation = generation;
                playback
            })
    }

    pub fn spawn_local_file_rtp_playback_from_driver<F>(
        driver: LocalFileSlotDriver,
        config: LocalFileRtpPlaybackConfig,
        initial_volume: VolumeLevel,
        mut on_event: F,
    ) -> Result<LocalFileRtpPlayback>
    where
        F: FnMut(WorkerEvent) + Send + 'static,
    {
        config.validate()?;

        let generation = driver.generation();
        let mut driver = driver.into_role(SlotRole::Current);
        driver.set_volume(initial_volume)?;
        driver.set_gain(config.gain)?;
        let packetizer = RtpPacketizer::new(config.transport.packetizer_config())?;
        let sink = UdpRtpPacketSink::connect_config(&config.transport)?;
        let runner = RtpSlotRunner::new(driver, packetizer, sink);
        let control = Arc::new(PlaybackControl::new(
            initial_volume,
            config.gain,
            config.start_position_ms,
        ));
        let worker_control = Arc::clone(&control);

        let join = thread::Builder::new()
            .name(format!("music-rtp-{generation}"))
            .spawn(move || {
                let _span = tracing::debug_span!(
                    "music_stream.runtime.current_worker",
                    generation,
                    start_position_ms = config.start_position_ms,
                )
                .entered();
                let metrics_recorder = config.metrics_recorder.clone();
                let result = with_optional_metrics_recorder(&metrics_recorder, || {
                    run_rtp_playback(runner, config, worker_control, |event| {
                        on_event(event);
                    })
                });
                if let Err(error) = &result {
                    tracing::warn!(
                        generation,
                        code = ?error.code(),
                        error = %error,
                        "current RTP worker failed",
                    );
                    on_event(WorkerEvent::CurrentFailed {
                        generation,
                        code: error.code(),
                        message: error.to_string(),
                    });
                }
                result
            })
            .map_err(|error| MusicStreamError::Internal(error.to_string()))?;

        Ok(LocalFileRtpPlayback {
            generation,
            control,
            live_stop: None,
            join: Some(join),
        })
    }

    pub fn spawn_live_stream_rtp_playback<F>(
        track: TrackSource,
        config: LocalFileRtpPlaybackConfig,
        initial_volume: VolumeLevel,
        mut on_event: F,
    ) -> Result<LocalFileRtpPlayback>
    where
        F: FnMut(WorkerEvent) + Send + 'static,
    {
        validate_live_track(&track)?;
        config.validate()?;

        let generation = config.pipeline.generation;
        let built = crate::slot::build_live_stream_slot(
            SlotRole::Current,
            &track,
            live_slot_config_for_runtime(&config),
        )?;
        let crate::slot::LiveStreamSlot { stream, mut driver } = built;
        let live_stop = stream.stop_handle();
        driver.set_volume(initial_volume)?;
        driver.set_gain(config.gain)?;
        let packetizer = RtpPacketizer::new(config.transport.packetizer_config())?;
        let sink = UdpRtpPacketSink::connect_config(&config.transport)?;
        let runner = RtpSlotRunner::new(driver.into_role(SlotRole::Current), packetizer, sink);
        let control = Arc::new(PlaybackControl::new(initial_volume, config.gain, 0));
        let worker_control = Arc::clone(&control);

        let join = thread::Builder::new()
            .name(format!("music-live-rtp-{generation}"))
            .spawn(move || {
                let _span =
                    tracing::debug_span!("music_stream.runtime.live_worker", generation,).entered();
                let metrics_recorder = config.metrics_recorder.clone();
                let result = with_optional_metrics_recorder(&metrics_recorder, || {
                    run_rtp_playback(runner, config, worker_control, |event| {
                        on_event(event);
                    })
                });
                let stream_result = stream.join();
                if let Err(error) = &stream_result {
                    tracing::warn!(
                        generation,
                        code = ?error.code(),
                        error = %error,
                        "live HTTP stream worker failed",
                    );
                    on_event(WorkerEvent::CurrentFailed {
                        generation,
                        code: error.code(),
                        message: error.to_string(),
                    });
                    return stream_result.map(|_| LocalFileRtpPlaybackReport {
                        generation,
                        events: Vec::new(),
                        drain: RtpSlotDrainReport::default(),
                        rtcp_reports_sent: 0,
                        rtcp_bytes_sent: 0,
                        latest_receiver_report: None,
                        completed: false,
                        stopped: false,
                    });
                } else if let Err(error) = &result {
                    tracing::warn!(
                        generation,
                        code = ?error.code(),
                        error = %error,
                        "live RTP worker failed",
                    );
                    on_event(WorkerEvent::CurrentFailed {
                        generation,
                        code: error.code(),
                        message: error.to_string(),
                    });
                }
                result
            })
            .map_err(|error| MusicStreamError::Internal(error.to_string()))?;

        Ok(LocalFileRtpPlayback {
            generation,
            control,
            live_stop: Some(live_stop),
            join: Some(join),
        })
    }

    pub fn spawn_local_file_preload<F>(
        track: TrackSource,
        config: LocalFileRtpPlaybackConfig,
        initial_volume: VolumeLevel,
        mut on_event: F,
    ) -> Result<LocalFilePreload>
    where
        F: FnMut(WorkerEvent) + Send + 'static,
    {
        validate_track(&track)?;
        config.validate()?;

        let generation = config.pipeline.generation;
        let source_resolver = config.source_resolver.clone();
        let built = with_optional_metrics_recorder(&config.metrics_recorder, || {
            crate::slot::build_local_file_slot_with_resolver(
                SlotRole::Next,
                &track,
                slot_config_for_runtime(&config),
                &source_resolver,
            )
        })?;
        let mut driver = built.driver;
        driver.set_volume(initial_volume)?;
        driver.set_gain(config.gain)?;
        let control = Arc::new(PlaybackControl::new(initial_volume, config.gain, 0));
        let worker_control = Arc::clone(&control);
        let completion = LocalFilePreloadCompletion::new();
        let worker_completion = completion.clone();

        let join = thread::Builder::new()
            .name(format!("music-preload-{generation}"))
            .spawn(move || {
                let _completion_guard = LocalFilePreloadCompletionGuard(worker_completion);
                let _span =
                    tracing::debug_span!("music_stream.runtime.preload_worker", generation,)
                        .entered();
                let metrics_recorder = config.metrics_recorder.clone();
                let result = with_optional_metrics_recorder(&metrics_recorder, || {
                    run_local_file_preload(driver, config, worker_control, |event| {
                        on_event(event);
                    })
                });
                if let Err(error) = &result {
                    tracing::warn!(
                        generation,
                        code = ?error.code(),
                        error = %error,
                        "preload worker failed",
                    );
                    on_event(WorkerEvent::NextFailed {
                        generation,
                        code: error.code(),
                        message: error.to_string(),
                    });
                }
                result
            })
            .map_err(|error| MusicStreamError::Internal(error.to_string()))?;

        Ok(LocalFilePreload {
            generation,
            control,
            completion,
            join: Some(join),
        })
    }

    fn run_rtp_playback<D, E, F>(
        mut runner: RtpSlotRunner<D, E, UdpRtpPacketSink>,
        config: LocalFileRtpPlaybackConfig,
        control: Arc<PlaybackControl>,
        mut on_event: F,
    ) -> Result<LocalFileRtpPlaybackReport>
    where
        D: DecoderBackend,
        E: OpusEncoderBackend,
        F: FnMut(WorkerEvent),
    {
        let clock = Instant::now();
        let mut control_mirror = RuntimeControlMirror::new();
        let mut report = LocalFileRtpPlaybackReport {
            generation: config.pipeline.generation,
            events: Vec::new(),
            drain: RtpSlotDrainReport::default(),
            rtcp_reports_sent: 0,
            rtcp_bytes_sent: 0,
            latest_receiver_report: None,
            completed: false,
            stopped: false,
        };
        let mut rtcp =
            RtcpSenderReportRuntime::new(&config.transport, config.rtcp_report_interval)?;

        loop {
            let wake_epoch = control.wake_epoch();
            let tick = PlaybackTickContext {
                runner: &mut runner,
                rtcp: &mut rtcp,
                report: &mut report,
                control: &control,
                config: &config,
                control_mirror: &mut control_mirror,
                on_event: &mut on_event,
            };
            match playback_tick(tick, clock)? {
                PlaybackTick::Stopped => {
                    report.stopped = true;
                    return Ok(report);
                }
                PlaybackTick::Completed => {
                    report.completed = true;
                    return Ok(report);
                }
                PlaybackTick::Continue(wait) => {
                    wait.wait(&control, wake_epoch, config.idle_sleep);
                }
            }
        }
    }

    fn playback_tick<D, E, F>(
        context: PlaybackTickContext<'_, D, E, F>,
        clock: Instant,
    ) -> Result<PlaybackTick>
    where
        D: DecoderBackend,
        E: OpusEncoderBackend,
        F: FnMut(WorkerEvent),
    {
        let PlaybackTickContext {
            runner,
            rtcp,
            report,
            control,
            config,
            control_mirror,
            on_event,
        } = context;

        if control.stop.load(Ordering::Relaxed) {
            return Ok(PlaybackTick::Stopped);
        }

        control_mirror.update(control, runner)?;

        let worker_first =
            !runner.driver().ready_reported() || control.paused.load(Ordering::Relaxed);
        let mut worker = if worker_first {
            match run_current_worker_turn(runner, control)? {
                WorkerTurnAttempt::Ran(turn) => Some(turn),
                WorkerTurnAttempt::Busy => return Ok(PlaybackTick::Continue(RuntimeWait::Idle)),
                WorkerTurnAttempt::Stopped => return Ok(PlaybackTick::Stopped),
            }
        } else {
            None
        };
        if let Some(turn) = worker.take() {
            worker = Some(record_worker_event(turn, report, on_event));
        }
        if control.paused.load(Ordering::Relaxed) {
            return Ok(PlaybackTick::Continue(RuntimeWait::Idle));
        }

        let (now_ms, drain) =
            run_playback_io(runner, rtcp, control, report, config, clock, on_event)?;

        let turn = if let Some(turn) = worker {
            turn
        } else {
            match run_current_worker_turn(runner, control)? {
                WorkerTurnAttempt::Ran(turn) => record_worker_event(turn, report, on_event),
                WorkerTurnAttempt::Busy => SlotTurnReport {
                    worker: WorkerTurnReport::default(),
                    event: None,
                },
                WorkerTurnAttempt::Stopped => return Ok(PlaybackTick::Stopped),
            }
        };

        if runner.driver().ended_reported() {
            return Ok(PlaybackTick::Completed);
        }

        Ok(PlaybackTick::Continue(playback_wait_after_tick(
            &turn.worker,
            &drain,
            runner.pacer().poll(now_ms),
        )))
    }

    fn run_current_worker_turn<D, E>(
        runner: &mut RtpSlotRunner<D, E, UdpRtpPacketSink>,
        control: &PlaybackControl,
    ) -> Result<WorkerTurnAttempt>
    where
        D: DecoderBackend,
        E: OpusEncoderBackend,
    {
        let Some(_cpu_permit) = current_cpu_lane().try_acquire(control) else {
            if control.stop.load(Ordering::Relaxed) {
                return Ok(WorkerTurnAttempt::Stopped);
            }
            metrics::counter!(CURRENT_CPU_PERMIT_BUSY_METRIC).increment(1);
            return Ok(WorkerTurnAttempt::Busy);
        };
        let worker_started = Instant::now();
        let turn = runner.worker_turn();
        if turn.is_err() && control.stop.load(Ordering::Relaxed) {
            return Ok(WorkerTurnAttempt::Stopped);
        }
        let turn = turn?;
        record_worker_turn_metrics(
            CURRENT_WORKER_METRICS,
            &turn.worker,
            worker_started.elapsed(),
        );
        Ok(WorkerTurnAttempt::Ran(turn))
    }

    fn record_worker_event<F>(
        turn: SlotTurnReport,
        report: &mut LocalFileRtpPlaybackReport,
        on_event: &mut F,
    ) -> SlotTurnReport
    where
        F: FnMut(WorkerEvent),
    {
        if let Some(event) = turn.event {
            on_event(event.clone());
            report.events.push(event);
        }
        SlotTurnReport {
            worker: turn.worker,
            event: None,
        }
    }

    fn run_playback_io<D, E, F>(
        runner: &mut RtpSlotRunner<D, E, UdpRtpPacketSink>,
        rtcp: &mut RtcpSenderReportRuntime,
        control: &PlaybackControl,
        report: &mut LocalFileRtpPlaybackReport,
        config: &LocalFileRtpPlaybackConfig,
        clock: Instant,
        on_event: &mut F,
    ) -> Result<(u64, RtpSlotDrainReport)>
    where
        D: DecoderBackend,
        E: OpusEncoderBackend,
        F: FnMut(WorkerEvent),
    {
        let now_ms = elapsed_ms(clock);
        let drain = runner.drain_due_packets(now_ms, config.max_packets_per_tick)?;
        control.record_drain(&drain);
        record_rtp_drain_metrics(&drain);
        rtcp.record_drain(&drain);
        report.drain.merge(&drain);
        send_due_rtcp_report(runner, rtcp, now_ms, control, report)?;
        poll_rtcp_receiver_reports(
            runner,
            rtcp,
            control,
            report,
            config.pipeline.generation,
            on_event,
        )?;
        Ok((now_ms, drain))
    }

    fn send_due_rtcp_report<D, E>(
        runner: &mut RtpSlotRunner<D, E, UdpRtpPacketSink>,
        rtcp: &mut RtcpSenderReportRuntime,
        now_ms: u64,
        control: &PlaybackControl,
        report: &mut LocalFileRtpPlaybackReport,
    ) -> Result<()>
    where
        D: DecoderBackend,
        E: OpusEncoderBackend,
    {
        let Some(packet) = rtcp.due_report(now_ms, SystemTime::now())? else {
            return Ok(());
        };

        let bytes_sent = packet.bytes.len();
        match &mut rtcp.sink {
            RuntimeRtcpSink::Muxed => runner.sink_mut().send_control(&packet.bytes)?,
            RuntimeRtcpSink::Dedicated(sink) => sink.send(packet)?,
        }
        rtcp.mark_sent(now_ms, bytes_sent);
        control.record_rtcp_report(bytes_sent);
        report.rtcp_reports_sent = rtcp.reports_sent;
        report.rtcp_bytes_sent = rtcp.bytes_sent;
        metrics::counter!(CURRENT_RTCP_REPORTS_SENT_METRIC).increment(1);
        metrics::counter!(CURRENT_RTCP_BYTES_SENT_METRIC)
            .increment(bytes_sent.try_into().unwrap_or(u64::MAX));
        Ok(())
    }

    fn poll_rtcp_receiver_reports<D, E, F>(
        runner: &mut RtpSlotRunner<D, E, UdpRtpPacketSink>,
        rtcp: &mut RtcpSenderReportRuntime,
        control: &PlaybackControl,
        report: &mut LocalFileRtpPlaybackReport,
        generation: u64,
        on_event: &mut F,
    ) -> Result<()>
    where
        D: DecoderBackend,
        E: OpusEncoderBackend,
        F: FnMut(WorkerEvent),
    {
        for _ in 0..MAX_RTCP_RECV_PER_TICK {
            let bytes = match &mut rtcp.sink {
                RuntimeRtcpSink::Muxed => runner.sink_mut().try_recv_control()?,
                RuntimeRtcpSink::Dedicated(sink) => sink.try_recv()?,
            };
            let Some(bytes) = bytes else {
                return Ok(());
            };

            if let Some(snapshot) =
                parse_rtcp_receiver_reports(bytes, rtcp.ssrc, rtcp.receiver_reports_received)?
            {
                let quality = rtcp.record_receiver_report(snapshot);
                control.record_receiver_report(snapshot);
                report.latest_receiver_report = Some(snapshot);
                metrics::counter!(CURRENT_RTCP_RECEIVER_REPORTS_METRIC).increment(1);
                record_rtcp_quality_metrics(&quality);
                if rtcp.note_quality_level(quality.level) {
                    let event = WorkerEvent::CurrentNetworkQualityChanged {
                        generation,
                        quality: quality.level,
                        snapshot: quality,
                    };
                    on_event(event.clone());
                    report.events.push(event);
                }
            }
        }

        Ok(())
    }

    fn record_rtcp_quality_metrics(quality: &RtcpQualityWindowSnapshot) {
        metrics::gauge!(CURRENT_RTCP_QUALITY_WINDOW_REPORTS_METRIC).set(quality.samples as f64);
        metrics::gauge!(CURRENT_RTCP_QUALITY_LATEST_LOSS_PERCENT_METRIC)
            .set(quality.latest_loss_percent);
        metrics::gauge!(CURRENT_RTCP_QUALITY_AVERAGE_LOSS_PERCENT_METRIC)
            .set(quality.average_loss_percent);
        metrics::gauge!(CURRENT_RTCP_QUALITY_MAX_LOSS_PERCENT_METRIC).set(quality.max_loss_percent);
        metrics::gauge!(CURRENT_RTCP_QUALITY_AVERAGE_JITTER_MS_METRIC)
            .set(quality.average_jitter_micros as f64 / 1_000.0);
        metrics::gauge!(CURRENT_RTCP_QUALITY_MAX_JITTER_MS_METRIC)
            .set(quality.max_jitter_micros as f64 / 1_000.0);
        if let Some(average_rtt) = quality.average_round_trip_time_micros {
            metrics::gauge!(CURRENT_RTCP_QUALITY_AVERAGE_RTT_MS_METRIC)
                .set(average_rtt as f64 / 1_000.0);
        }
        if let Some(max_rtt) = quality.max_round_trip_time_micros {
            metrics::gauge!(CURRENT_RTCP_QUALITY_MAX_RTT_MS_METRIC).set(max_rtt as f64 / 1_000.0);
        }
    }

    fn record_worker_turn_metrics(
        names: WorkerMetricNames,
        turn: &WorkerTurnReport,
        elapsed: Duration,
    ) {
        metrics::histogram!(names.turn_us).record(duration_micros(elapsed) as f64);
        counter_if_nonzero(names.decoded_chunks, turn.decoded_chunks);
        counter_if_nonzero(names.decoded_frames, turn.decoded_frames);
        counter_if_nonzero(names.encoded_frames, turn.encoded_frames);
        metrics::gauge!(names.decoded_queue_ms).set(turn.decoded_queue_ms as f64);
        metrics::gauge!(names.encoded_queue_ms).set(turn.encoded_queue_ms as f64);
        if turn.hit_decoded_high_water {
            metrics::counter!(names.decoded_high_water_hits).increment(1);
        }
        if turn.hit_encoded_high_water {
            metrics::counter!(names.encoded_high_water_hits).increment(1);
        }
        if turn.source_need_more {
            metrics::counter!(names.source_need_more).increment(1);
        }
        if turn.source_ended {
            metrics::counter!(names.source_ended).increment(1);
        }
    }

    fn record_rtp_drain_metrics(drain: &RtpSlotDrainReport) {
        counter_if_nonzero(CURRENT_RTP_PACKETS_SENT_METRIC, drain.packets_sent);
        counter_if_nonzero(CURRENT_RTP_BYTES_SENT_METRIC, drain.bytes_sent);
        counter_if_nonzero(
            CURRENT_RTP_PAYLOAD_BYTES_SENT_METRIC,
            drain.payload_bytes_sent,
        );
        counter_if_nonzero(CURRENT_RTP_MEDIA_SENT_MS_METRIC, drain.media_sent_ms);
        metrics::gauge!(CURRENT_RTP_MAX_PACING_LATE_MS_METRIC).set(drain.max_pacing_late_ms as f64);
        counter_if_nonzero(CURRENT_RTP_PACING_LATE_MS_METRIC, drain.max_pacing_late_ms);
        if drain.stopped_on_prebuffer {
            metrics::counter!(CURRENT_RTP_PREBUFFER_WAITS_METRIC).increment(1);
        }
        if drain.stopped_on_underrun {
            metrics::counter!(CURRENT_RTP_UNDERRUNS_METRIC).increment(1);
        }
        if drain.stopped_on_pacing {
            metrics::counter!(CURRENT_RTP_PACING_WAITS_METRIC).increment(1);
        }
    }

    fn counter_if_nonzero<T>(name: &'static str, value: T)
    where
        T: TryInto<u64> + Copy + PartialEq + From<u8>,
    {
        if value != T::from(0) {
            metrics::counter!(name).increment(value.try_into().unwrap_or(u64::MAX));
        }
    }

    fn duration_micros(duration: Duration) -> u64 {
        duration.as_micros().try_into().unwrap_or(u64::MAX)
    }

    fn with_optional_metrics_recorder<T>(
        recorder: &Option<Arc<dyn metrics::Recorder + Send + Sync>>,
        f: impl FnOnce() -> T,
    ) -> T {
        if let Some(recorder) = recorder {
            metrics::with_local_recorder(recorder.as_ref(), f)
        } else {
            f()
        }
    }

    fn run_local_file_preload<F>(
        driver: LocalFileSlotDriver,
        config: LocalFileRtpPlaybackConfig,
        control: Arc<PlaybackControl>,
        mut on_event: F,
    ) -> Result<LocalFilePreloadReport>
    where
        F: FnMut(WorkerEvent),
    {
        let mut control_mirror = RuntimeControlMirror::new();
        let mut report = LocalFilePreloadReport {
            generation: config.pipeline.generation,
            events: Vec::new(),
            ready: false,
            stopped: false,
            driver,
        };

        loop {
            let wake_epoch = control.wake_epoch();
            match preload_tick(&mut report, &control, &mut control_mirror, &mut on_event)? {
                PreloadTick::Stopped => {
                    report.stopped = true;
                    return Ok(report);
                }
                PreloadTick::Ready => {
                    report.ready = true;
                    return Ok(report);
                }
                PreloadTick::Continue(wait) => {
                    wait.wait(&control, wake_epoch, config.idle_sleep);
                }
            }
        }
    }

    fn preload_tick<F>(
        report: &mut LocalFilePreloadReport,
        control: &PlaybackControl,
        control_mirror: &mut RuntimeControlMirror,
        on_event: &mut F,
    ) -> Result<PreloadTick>
    where
        F: FnMut(WorkerEvent),
    {
        if control.stop.load(Ordering::Relaxed) {
            return Ok(PreloadTick::Stopped);
        }

        control_mirror.update_driver(control, &mut report.driver)?;

        let turn = {
            let Some(_cpu_permit) = preload_cpu_lane().try_acquire(control) else {
                if control.stop.load(Ordering::Relaxed) {
                    return Ok(PreloadTick::Stopped);
                }
                metrics::counter!(PRELOAD_CPU_PERMIT_BUSY_METRIC).increment(1);
                return Ok(PreloadTick::Continue(RuntimeWait::Idle));
            };
            let worker_started = Instant::now();
            let turn = report.driver.worker_turn()?;
            record_worker_turn_metrics(
                PRELOAD_WORKER_METRICS,
                &turn.worker,
                worker_started.elapsed(),
            );
            turn
        };

        if let Some(event) = turn.event {
            on_event(event.clone());
            let ready = matches!(event, WorkerEvent::NextReady { .. });
            report.events.push(event);
            if ready {
                return Ok(PreloadTick::Ready);
            }
        }

        Ok(PreloadTick::Continue(preload_wait_after_turn(&turn.worker)))
    }

    fn slot_config_for_runtime(config: &LocalFileRtpPlaybackConfig) -> LocalFileSlotConfig {
        let mut slot_config = LocalFileSlotConfig::new(config.pipeline.clone());
        slot_config.start_position_ms = config.start_position_ms;
        if let Some(opus_bitrate_bps) = config.transport.opus_bitrate_bps {
            slot_config.opus.bitrate_bps = Some(opus_bitrate_bps as i32);
        }
        slot_config.opus.max_packet_bytes =
            config.transport.mtu.saturating_sub(RTP_FIXED_HEADER_LEN);
        slot_config
    }

    fn live_slot_config_for_runtime(config: &LocalFileRtpPlaybackConfig) -> LiveStreamSlotConfig {
        let mut slot_config = LiveStreamSlotConfig::new(config.pipeline.clone());
        slot_config.http = config.live_http.clone();
        if let Some(opus_bitrate_bps) = config.transport.opus_bitrate_bps {
            slot_config.opus.bitrate_bps = Some(opus_bitrate_bps as i32);
        }
        slot_config.opus.max_packet_bytes =
            config.transport.mtu.saturating_sub(RTP_FIXED_HEADER_LEN);
        slot_config
    }

    fn validate_track(track: &TrackSource) -> Result<()> {
        if !track.is_artifact_backed() {
            return Err(MusicStreamError::Unsupported(
                "runtime playback currently supports local file and bounded URL sources only"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_live_track(track: &TrackSource) -> Result<()> {
        if !track.is_live() {
            return Err(MusicStreamError::Unsupported(
                "live runtime playback requires a live source".to_owned(),
            ));
        }
        if track.url.is_none() {
            return Err(MusicStreamError::InvalidSource(
                "live runtime playback requires url".to_owned(),
            ));
        }
        Ok(())
    }

    fn playback_wait_after_tick(
        worker: &WorkerTurnReport,
        drain: &RtpSlotDrainReport,
        pacer: RtpPaceDecision,
    ) -> RuntimeWait {
        if drain.stopped_on_pacing {
            return RuntimeWait::Pacing(pacer);
        }

        if drain.packets_sent == 0 && worker_is_idle(worker) {
            return RuntimeWait::Idle;
        }

        RuntimeWait::None
    }

    fn preload_wait_after_turn(worker: &WorkerTurnReport) -> RuntimeWait {
        if worker_is_idle(worker) {
            RuntimeWait::Idle
        } else {
            RuntimeWait::None
        }
    }

    fn worker_is_idle(worker: &WorkerTurnReport) -> bool {
        worker.decoded_chunks == 0
            && worker.decoded_frames == 0
            && worker.encoded_frames == 0
            && !worker.source_ended
    }

    fn wait_for_pacer(
        control: &PlaybackControl,
        wake_epoch: u64,
        decision: RtpPaceDecision,
        idle_sleep: Duration,
    ) {
        match decision {
            RtpPaceDecision::Ready => control.wait_timeout_since(wake_epoch, idle_sleep),
            RtpPaceDecision::Wait { delay_ms } => {
                control.wait_timeout_since(
                    wake_epoch,
                    Duration::from_millis(delay_ms.clamp(1, MAX_SLEEP_MS)),
                );
            }
        }
    }

    fn elapsed_ms(clock: Instant) -> u64 {
        clock.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
    }

    #[must_use]
    pub fn default_pipeline_config(generation: u64) -> PipelineConfig {
        PipelineConfig {
            generation,
            sample_rate: DEFAULT_SAMPLE_RATE,
            channels: DEFAULT_CHANNELS,
            frame_samples_per_channel: DEFAULT_FRAME_SAMPLES_PER_CHANNEL,
            watermarks: WatermarkConfig::default(),
            prebuffer_ms: DEFAULT_PREBUFFER_MS,
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn worker_report() -> WorkerTurnReport {
            WorkerTurnReport::default()
        }

        #[test]
        fn playback_wait_prefers_pacer_deadline_when_drain_stopped_on_pacing() {
            let worker = worker_report();
            let drain = RtpSlotDrainReport {
                stopped_on_pacing: true,
                ..RtpSlotDrainReport::default()
            };

            assert_eq!(
                playback_wait_after_tick(&worker, &drain, RtpPaceDecision::Wait { delay_ms: 12 }),
                RuntimeWait::Pacing(RtpPaceDecision::Wait { delay_ms: 12 })
            );
        }

        #[test]
        fn playback_waits_idle_when_no_worker_or_sender_progress_was_made() {
            let worker = worker_report();
            let drain = RtpSlotDrainReport::default();

            assert_eq!(
                playback_wait_after_tick(&worker, &drain, RtpPaceDecision::Ready),
                RuntimeWait::Idle
            );
        }

        #[test]
        fn playback_keeps_spinning_after_worker_or_sender_progress() {
            let mut decoded = worker_report();
            decoded.decoded_chunks = 1;
            assert_eq!(
                playback_wait_after_tick(
                    &decoded,
                    &RtpSlotDrainReport::default(),
                    RtpPaceDecision::Ready
                ),
                RuntimeWait::None
            );

            let sent = RtpSlotDrainReport {
                packets_sent: 1,
                ..RtpSlotDrainReport::default()
            };
            assert_eq!(
                playback_wait_after_tick(&worker_report(), &sent, RtpPaceDecision::Ready),
                RuntimeWait::None
            );
        }

        #[test]
        fn playback_does_not_idle_wait_after_source_end() {
            let mut worker = worker_report();
            worker.source_ended = true;

            assert_eq!(
                playback_wait_after_tick(
                    &worker,
                    &RtpSlotDrainReport::default(),
                    RtpPaceDecision::Ready
                ),
                RuntimeWait::None
            );
        }

        #[test]
        fn preload_wait_uses_same_idle_worker_definition() {
            assert_eq!(preload_wait_after_turn(&worker_report()), RuntimeWait::Idle);

            let mut worker = worker_report();
            worker.encoded_frames = 1;
            assert_eq!(preload_wait_after_turn(&worker), RuntimeWait::None);
        }
    }
}

#[cfg(all(
    feature = "decoder-symphonia",
    feature = "resampler-rubato",
    feature = "transport-rtp"
))]
pub use local_file_rtp::{
    LocalFilePreload, LocalFilePreloadCompletion, LocalFilePreloadReport, LocalFileRtpPlayback,
    LocalFileRtpPlaybackConfig, LocalFileRtpPlaybackProgress, LocalFileRtpPlaybackReport,
    default_pipeline_config, spawn_live_stream_rtp_playback, spawn_local_file_preload,
    spawn_local_file_rtp_playback, spawn_local_file_rtp_playback_from_driver,
};
