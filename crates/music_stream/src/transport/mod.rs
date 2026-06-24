//! RTP/RTCP transport session built on tested protocol crates.
//!
//! This module must not hand-write wire format serialization. The pure sender
//! core below owns playout decisions; packetization and UDP IO can wrap it.

#[cfg(feature = "transport-rtp")]
use bytes::{Bytes, BytesMut};
#[cfg(feature = "transport-rtp")]
use std::io::ErrorKind;
#[cfg(feature = "transport-rtp")]
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
#[cfg(feature = "transport-rtp")]
use std::time::{SystemTime, UNIX_EPOCH};

use crate::audio::frame::{FrameQueue, OpusFrame};
#[cfg(feature = "transport-rtp")]
use crate::error::{MusicStreamError, Result};

#[cfg(feature = "transport-rtp")]
const RTP_FIXED_HEADER_LEN: usize = 12;
#[cfg(feature = "transport-rtp")]
const DEFAULT_RTP_MTU: usize = 1_200;
#[cfg(feature = "transport-rtp")]
const DEFAULT_BIND_IP: &str = "0.0.0.0";
#[cfg(feature = "transport-rtp")]
const NTP_UNIX_EPOCH_OFFSET_SECS: u64 = 2_208_988_800;
#[cfg(feature = "transport-rtp")]
pub const RTP_OPUS_CLOCK_RATE_HZ: u32 = 48_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SenderStep {
    WaitPrebuffer {
        queued_ms: u64,
        needed_ms: u64,
    },
    Send {
        frame: OpusFrame,
        rtp_timestamp: u32,
        sequence: u16,
    },
    Underrun {
        generation: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SenderCore {
    active_generation: u64,
    prebuffer_ms: u64,
    started: bool,
    rtp_timestamp: u32,
    sequence: u16,
}

impl SenderCore {
    #[must_use]
    pub fn new(active_generation: u64, prebuffer_ms: u64) -> Self {
        Self {
            active_generation,
            prebuffer_ms,
            started: false,
            rtp_timestamp: 0,
            sequence: 0,
        }
    }

    pub fn set_active_generation(&mut self, generation: u64) {
        if self.active_generation != generation {
            self.active_generation = generation;
            self.started = false;
        }
    }

    #[must_use]
    pub fn active_generation(&self) -> u64 {
        self.active_generation
    }

    #[must_use]
    pub fn started(&self) -> bool {
        self.started
    }

    pub fn next_step(&mut self, queue: &mut FrameQueue<OpusFrame>) -> SenderStep {
        self.next_step_with_prebuffer_ready(queue, queue.duration_ms() >= self.prebuffer_ms)
    }

    pub fn next_step_with_prebuffer_ready(
        &mut self,
        queue: &mut FrameQueue<OpusFrame>,
        prebuffer_ready: bool,
    ) -> SenderStep {
        if !self.started && !prebuffer_ready {
            return SenderStep::WaitPrebuffer {
                queued_ms: queue.duration_ms(),
                needed_ms: self.prebuffer_ms,
            };
        }

        let Some(frame) = queue.pop_active(self.active_generation) else {
            self.started = false;
            return SenderStep::Underrun {
                generation: self.active_generation,
            };
        };

        self.started = true;
        let step = SenderStep::Send {
            rtp_timestamp: self.rtp_timestamp,
            sequence: self.sequence,
            frame,
        };

        if let SenderStep::Send { frame, .. } = &step {
            self.rtp_timestamp = self.rtp_timestamp.wrapping_add(frame.samples_per_channel);
        }
        self.sequence = self.sequence.wrapping_add(1);

        step
    }
}

#[cfg(feature = "transport-rtp")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtpPacketizerConfig {
    pub payload_type: u8,
    pub ssrc: u32,
    pub mtu: usize,
}

#[cfg(feature = "transport-rtp")]
impl Default for RtpPacketizerConfig {
    fn default() -> Self {
        Self {
            payload_type: 96,
            ssrc: 1,
            mtu: DEFAULT_RTP_MTU,
        }
    }
}

#[cfg(feature = "transport-rtp")]
impl RtpPacketizerConfig {
    pub fn validate(&self) -> Result<()> {
        if self.payload_type > 127 {
            return Err(MusicStreamError::InvalidConfig(
                "RTP payload_type must fit in 7 bits".to_owned(),
            ));
        }
        if self.mtu <= RTP_FIXED_HEADER_LEN {
            return Err(MusicStreamError::InvalidConfig(
                "RTP mtu must fit the fixed header and at least one payload byte".to_owned(),
            ));
        }

        Ok(())
    }
}

#[cfg(feature = "transport-rtp")]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum RtpEncryptionConfig {
    #[default]
    None,
    External {
        mode: String,
        secret_key: Option<Vec<u8>>,
    },
}

#[cfg(feature = "transport-rtp")]
impl RtpEncryptionConfig {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::None => Ok(()),
            Self::External { mode, secret_key } => {
                if mode.trim().is_empty() {
                    return Err(MusicStreamError::InvalidConfig(
                        "RTP encryption mode must not be empty".to_owned(),
                    ));
                }
                if let Some(secret_key) = secret_key
                    && secret_key.is_empty()
                {
                    return Err(MusicStreamError::InvalidConfig(
                        "RTP encryption secret_key must not be empty when provided".to_owned(),
                    ));
                }
                Ok(())
            }
        }
    }

    #[must_use]
    pub fn is_plaintext(&self) -> bool {
        matches!(self, Self::None)
    }
}

#[cfg(feature = "transport-rtp")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtpTransportConfig {
    pub remote_ip: String,
    pub remote_rtp_port: u16,
    pub remote_rtcp_port: Option<u16>,
    pub local_ip: String,
    pub local_rtp_port: u16,
    pub payload_type: u8,
    pub ssrc: u32,
    pub mtu: usize,
    pub rtcp_mux: bool,
    pub opus_bitrate_bps: Option<u32>,
    pub encryption: RtpEncryptionConfig,
}

#[cfg(feature = "transport-rtp")]
impl RtpTransportConfig {
    #[must_use]
    pub fn new(remote_ip: impl Into<String>, remote_rtp_port: u16, ssrc: u32) -> Self {
        Self {
            remote_ip: remote_ip.into(),
            remote_rtp_port,
            remote_rtcp_port: None,
            local_ip: DEFAULT_BIND_IP.to_owned(),
            local_rtp_port: 0,
            payload_type: 96,
            ssrc,
            mtu: DEFAULT_RTP_MTU,
            rtcp_mux: true,
            opus_bitrate_bps: None,
            encryption: RtpEncryptionConfig::None,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.remote_ip.trim().is_empty() {
            return Err(MusicStreamError::InvalidConfig(
                "RTP remote_ip must not be empty".to_owned(),
            ));
        }
        if self.remote_rtp_port == 0 {
            return Err(MusicStreamError::InvalidConfig(
                "RTP remote_rtp_port must be greater than zero".to_owned(),
            ));
        }
        if let Some(remote_rtcp_port) = self.remote_rtcp_port
            && remote_rtcp_port == 0
        {
            return Err(MusicStreamError::InvalidConfig(
                "RTP remote_rtcp_port must be greater than zero when provided".to_owned(),
            ));
        }
        if !self.rtcp_mux && self.remote_rtcp_port.is_none() {
            return Err(MusicStreamError::InvalidConfig(
                "RTP remote_rtcp_port is required when rtcp_mux is false".to_owned(),
            ));
        }
        if self.local_ip.trim().is_empty() {
            return Err(MusicStreamError::InvalidConfig(
                "RTP local_ip must not be empty".to_owned(),
            ));
        }
        if let Some(opus_bitrate_bps) = self.opus_bitrate_bps
            && opus_bitrate_bps == 0
        {
            return Err(MusicStreamError::InvalidConfig(
                "Opus bitrate must be greater than zero when provided".to_owned(),
            ));
        }
        self.packetizer_config().validate()?;
        self.encryption.validate()
    }

    #[must_use]
    pub fn packetizer_config(&self) -> RtpPacketizerConfig {
        RtpPacketizerConfig {
            payload_type: self.payload_type,
            ssrc: self.ssrc,
            mtu: self.mtu,
        }
    }

    #[must_use]
    pub fn local_rtp_addr(&self) -> String {
        format!("{}:{}", self.local_ip, self.local_rtp_port)
    }

    #[must_use]
    pub fn remote_rtp_addr(&self) -> String {
        format!("{}:{}", self.remote_ip, self.remote_rtp_port)
    }

    #[must_use]
    pub fn remote_rtcp_addr(&self) -> String {
        let port = if self.rtcp_mux {
            self.remote_rtp_port
        } else {
            self.remote_rtcp_port.unwrap_or(self.remote_rtp_port)
        };
        format!("{}:{port}", self.remote_ip)
    }
}

#[cfg(feature = "transport-rtp")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtcpSenderReportPacket {
    pub bytes: Bytes,
    pub ssrc: u32,
    pub ntp_time: u64,
    pub rtp_time: u32,
    pub packet_count: u32,
    pub octet_count: u32,
}

#[cfg(feature = "transport-rtp")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RtcpReceiverReportSnapshot {
    pub reports_received: usize,
    pub sender_ssrc: u32,
    pub source_ssrc: u32,
    pub fraction_lost: u8,
    pub total_lost: u32,
    pub last_sequence_number: u32,
    pub jitter: u32,
    pub jitter_micros: u64,
    pub last_sender_report: u32,
    pub delay: u32,
    pub round_trip_time_micros: Option<u64>,
}

#[cfg(feature = "transport-rtp")]
pub fn ntp_timestamp(time: SystemTime) -> u64 {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let seconds = duration
        .as_secs()
        .saturating_add(NTP_UNIX_EPOCH_OFFSET_SECS)
        & 0xffff_ffff;
    let fractional = ((u128::from(duration.subsec_nanos()) << 32) / 1_000_000_000_u128) as u64;
    (seconds << 32) | fractional
}

#[cfg(feature = "transport-rtp")]
#[must_use]
pub fn compact_ntp_timestamp(time: SystemTime) -> u32 {
    ((ntp_timestamp(time) >> 16) & 0xffff_ffff) as u32
}

#[cfg(feature = "transport-rtp")]
#[must_use]
pub fn rtp_jitter_micros(jitter: u32, clock_rate_hz: u32) -> u64 {
    if clock_rate_hz == 0 {
        return 0;
    }
    (u64::from(jitter) * 1_000_000) / u64::from(clock_rate_hz)
}

#[cfg(feature = "transport-rtp")]
#[must_use]
pub fn rtcp_compact_duration_micros(duration: u32) -> u64 {
    (u64::from(duration) * 1_000_000) / 65_536
}

#[cfg(feature = "transport-rtp")]
#[must_use]
pub fn rtcp_round_trip_time_micros(
    received_at: SystemTime,
    last_sender_report: u32,
    delay_since_last_sender_report: u32,
) -> Option<u64> {
    if last_sender_report == 0 {
        return None;
    }

    let arrival = compact_ntp_timestamp(received_at);
    let elapsed = arrival.wrapping_sub(last_sender_report);
    if elapsed < delay_since_last_sender_report {
        return Some(0);
    }

    Some(rtcp_compact_duration_micros(
        elapsed - delay_since_last_sender_report,
    ))
}

#[cfg(feature = "transport-rtp")]
pub fn parse_rtcp_receiver_reports(
    bytes: Bytes,
    local_ssrc: u32,
    previous_reports_received: usize,
) -> Result<Option<RtcpReceiverReportSnapshot>> {
    parse_rtcp_receiver_reports_at(
        bytes,
        local_ssrc,
        previous_reports_received,
        SystemTime::now(),
    )
}

#[cfg(feature = "transport-rtp")]
pub fn parse_rtcp_receiver_reports_at(
    bytes: Bytes,
    local_ssrc: u32,
    previous_reports_received: usize,
    received_at: SystemTime,
) -> Result<Option<RtcpReceiverReportSnapshot>> {
    let mut raw = bytes;
    let packets = rtcp::packet::unmarshal(&mut raw)
        .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
    let mut reports_received = previous_reports_received;
    let mut latest = None;

    for packet in packets {
        let Some(receiver_report) = packet
            .as_any()
            .downcast_ref::<rtcp::receiver_report::ReceiverReport>()
        else {
            continue;
        };

        for report in &receiver_report.reports {
            if report.ssrc != local_ssrc {
                continue;
            }
            reports_received += 1;
            latest = Some(RtcpReceiverReportSnapshot {
                reports_received,
                sender_ssrc: receiver_report.ssrc,
                source_ssrc: report.ssrc,
                fraction_lost: report.fraction_lost,
                total_lost: report.total_lost,
                last_sequence_number: report.last_sequence_number,
                jitter: report.jitter,
                jitter_micros: rtp_jitter_micros(report.jitter, RTP_OPUS_CLOCK_RATE_HZ),
                last_sender_report: report.last_sender_report,
                delay: report.delay,
                round_trip_time_micros: rtcp_round_trip_time_micros(
                    received_at,
                    report.last_sender_report,
                    report.delay,
                ),
            });
        }
    }

    Ok(latest)
}

#[cfg(feature = "transport-rtp")]
pub fn build_rtcp_sender_report(
    ssrc: u32,
    rtp_time: u32,
    packet_count: u32,
    octet_count: u32,
    now: SystemTime,
) -> Result<RtcpSenderReportPacket> {
    let ntp_time = ntp_timestamp(now);
    let report = rtcp::sender_report::SenderReport {
        ssrc,
        ntp_time,
        rtp_time,
        packet_count,
        octet_count,
        ..Default::default()
    };
    let bytes = rtcp::packet::marshal(&[Box::new(report)])
        .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
    Ok(RtcpSenderReportPacket {
        bytes,
        ssrc,
        ntp_time,
        rtp_time,
        packet_count,
        octet_count,
    })
}

#[cfg(feature = "transport-rtp")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtpPacketized {
    pub bytes: Bytes,
    pub sequence: u16,
    pub rtp_timestamp: u32,
    pub ssrc: u32,
    pub payload_type: u8,
    pub marker: bool,
    pub payload_len: usize,
    pub duration_ms: u64,
    pub track_position_samples: u64,
}

#[cfg(feature = "transport-rtp")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RtpSenderStep {
    WaitPrebuffer { queued_ms: u64, needed_ms: u64 },
    Packet(RtpPacketized),
    Underrun { generation: u64 },
}

#[cfg(feature = "transport-rtp")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RtpPaceDecision {
    Ready,
    Wait { delay_ms: u64 },
}

#[cfg(feature = "transport-rtp")]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RtpPacer {
    next_deadline_ms: Option<u64>,
}

#[cfg(feature = "transport-rtp")]
impl RtpPacer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn next_deadline_ms(&self) -> Option<u64> {
        self.next_deadline_ms
    }

    #[must_use]
    pub fn lateness_ms(&self, now_ms: u64) -> u64 {
        self.next_deadline_ms
            .map_or(0, |deadline| now_ms.saturating_sub(deadline))
    }

    #[must_use]
    pub fn poll(&self, now_ms: u64) -> RtpPaceDecision {
        match self.next_deadline_ms {
            None => RtpPaceDecision::Ready,
            Some(deadline) if now_ms >= deadline => RtpPaceDecision::Ready,
            Some(deadline) => RtpPaceDecision::Wait {
                delay_ms: deadline - now_ms,
            },
        }
    }

    pub fn on_packet_sent(&mut self, now_ms: u64, packet_duration_ms: u64) {
        let duration_ms = packet_duration_ms.max(1);
        self.next_deadline_ms = Some(now_ms.saturating_add(duration_ms));
    }

    pub fn on_underrun(&mut self) {
        self.reset();
    }

    pub fn reset(&mut self) {
        self.next_deadline_ms = None;
    }
}

#[cfg(feature = "transport-rtp")]
pub trait RtpPacketSink {
    fn send(&mut self, packet: RtpPacketized) -> Result<()>;
}

#[cfg(feature = "transport-rtp")]
pub trait RtpPacketProtector {
    fn protect(&mut self, packet: RtpPacketized) -> Result<RtpPacketized>;
}

#[cfg(feature = "transport-rtp")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlaintextRtpPacketProtector;

#[cfg(feature = "transport-rtp")]
impl RtpPacketProtector for PlaintextRtpPacketProtector {
    fn protect(&mut self, packet: RtpPacketized) -> Result<RtpPacketized> {
        Ok(packet)
    }
}

#[cfg(feature = "transport-rtp")]
#[derive(Debug)]
pub struct ProtectedRtpPacketSink<S, P> {
    inner: S,
    protector: P,
}

#[cfg(feature = "transport-rtp")]
impl<S, P> ProtectedRtpPacketSink<S, P> {
    #[must_use]
    pub fn new(inner: S, protector: P) -> Self {
        Self { inner, protector }
    }

    #[must_use]
    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    #[must_use]
    pub fn protector(&self) -> &P {
        &self.protector
    }

    pub fn protector_mut(&mut self) -> &mut P {
        &mut self.protector
    }

    pub fn into_parts(self) -> (S, P) {
        (self.inner, self.protector)
    }
}

#[cfg(feature = "transport-rtp")]
impl<S, P> RtpPacketSink for ProtectedRtpPacketSink<S, P>
where
    S: RtpPacketSink,
    P: RtpPacketProtector,
{
    fn send(&mut self, packet: RtpPacketized) -> Result<()> {
        let packet = self.protector.protect(packet)?;
        self.inner.send(packet)
    }
}

#[cfg(feature = "transport-rtp")]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemoryRtpPacketSink {
    packets: Vec<RtpPacketized>,
}

#[cfg(feature = "transport-rtp")]
impl MemoryRtpPacketSink {
    #[must_use]
    pub fn packets(&self) -> &[RtpPacketized] {
        &self.packets
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.packets.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }

    pub fn clear(&mut self) {
        self.packets.clear();
    }
}

#[cfg(feature = "transport-rtp")]
impl RtpPacketSink for MemoryRtpPacketSink {
    fn send(&mut self, packet: RtpPacketized) -> Result<()> {
        self.packets.push(packet);
        Ok(())
    }
}

#[cfg(feature = "transport-rtp")]
#[derive(Debug)]
pub struct UdpRtpPacketSink {
    socket: UdpSocket,
    packets_sent: usize,
    bytes_sent: usize,
}

#[cfg(feature = "transport-rtp")]
impl UdpRtpPacketSink {
    pub fn connect_config(config: &RtpTransportConfig) -> Result<Self> {
        config.validate()?;
        if !config.encryption.is_plaintext() {
            return Err(MusicStreamError::Unsupported(
                "encrypted RTP is configured but no packet protector is installed".to_owned(),
            ));
        }
        Self::connect(config.local_rtp_addr(), config.remote_rtp_addr())
    }

    pub fn connect_config_with_protector<P>(
        config: &RtpTransportConfig,
        protector: P,
    ) -> Result<ProtectedRtpPacketSink<Self, P>>
    where
        P: RtpPacketProtector,
    {
        config.validate()?;
        let sink = Self::connect(config.local_rtp_addr(), config.remote_rtp_addr())?;
        Ok(ProtectedRtpPacketSink::new(sink, protector))
    }

    pub fn connect(
        local_addr: impl ToSocketAddrs,
        remote_addr: impl ToSocketAddrs,
    ) -> Result<Self> {
        let socket = UdpSocket::bind(local_addr)
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
        socket
            .connect(remote_addr)
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
        socket
            .set_nonblocking(true)
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;

        Ok(Self {
            socket,
            packets_sent: 0,
            bytes_sent: 0,
        })
    }

    pub fn from_connected_socket(socket: UdpSocket) -> Result<Self> {
        socket
            .peer_addr()
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;

        Ok(Self {
            socket,
            packets_sent: 0,
            bytes_sent: 0,
        })
    }

    #[must_use]
    pub fn socket(&self) -> &UdpSocket {
        &self.socket
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.socket
            .local_addr()
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))
    }

    pub fn peer_addr(&self) -> Result<SocketAddr> {
        self.socket
            .peer_addr()
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))
    }

    #[must_use]
    pub fn packets_sent(&self) -> usize {
        self.packets_sent
    }

    #[must_use]
    pub fn bytes_sent(&self) -> usize {
        self.bytes_sent
    }
}

#[cfg(feature = "transport-rtp")]
impl RtpPacketSink for UdpRtpPacketSink {
    fn send(&mut self, packet: RtpPacketized) -> Result<()> {
        let expected = packet.bytes.len();
        let sent = self
            .socket
            .send(&packet.bytes)
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
        if sent != expected {
            return Err(MusicStreamError::RtpSendError(format!(
                "partial udp datagram send: sent {sent} of {expected} bytes"
            )));
        }

        self.packets_sent += 1;
        self.bytes_sent += sent;
        Ok(())
    }
}

#[cfg(feature = "transport-rtp")]
impl UdpRtpPacketSink {
    pub fn send_control(&mut self, bytes: &[u8]) -> Result<()> {
        let expected = bytes.len();
        let sent = self
            .socket
            .send(bytes)
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
        if sent != expected {
            return Err(MusicStreamError::RtpSendError(format!(
                "partial rtcp udp datagram send: sent {sent} of {expected} bytes"
            )));
        }
        Ok(())
    }

    pub fn try_recv_control(&mut self) -> Result<Option<Bytes>> {
        let mut buffer = [0_u8; 1_500];
        match self.socket.recv(&mut buffer) {
            Ok(len) => Ok(Some(Bytes::copy_from_slice(&buffer[..len]))),
            Err(error) if error.kind() == ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(MusicStreamError::RtpSendError(error.to_string())),
        }
    }
}

#[cfg(feature = "transport-rtp")]
#[derive(Debug)]
pub struct UdpRtcpPacketSink {
    socket: UdpSocket,
    packets_sent: usize,
    bytes_sent: usize,
}

#[cfg(feature = "transport-rtp")]
impl UdpRtcpPacketSink {
    pub fn connect_config(config: &RtpTransportConfig) -> Result<Self> {
        config.validate()?;
        Self::connect(format!("{}:0", config.local_ip), config.remote_rtcp_addr())
    }

    pub fn connect(
        local_addr: impl ToSocketAddrs,
        remote_addr: impl ToSocketAddrs,
    ) -> Result<Self> {
        let socket = UdpSocket::bind(local_addr)
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
        socket
            .connect(remote_addr)
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
        socket
            .set_nonblocking(true)
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;

        Ok(Self {
            socket,
            packets_sent: 0,
            bytes_sent: 0,
        })
    }

    pub fn send(&mut self, packet: RtcpSenderReportPacket) -> Result<()> {
        self.send_bytes(&packet.bytes)
    }

    pub fn send_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let expected = bytes.len();
        let sent = self
            .socket
            .send(bytes)
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
        if sent != expected {
            return Err(MusicStreamError::RtpSendError(format!(
                "partial rtcp udp datagram send: sent {sent} of {expected} bytes"
            )));
        }

        self.packets_sent += 1;
        self.bytes_sent += sent;
        Ok(())
    }

    pub fn try_recv(&mut self) -> Result<Option<Bytes>> {
        let mut buffer = [0_u8; 1_500];
        match self.socket.recv(&mut buffer) {
            Ok(len) => Ok(Some(Bytes::copy_from_slice(&buffer[..len]))),
            Err(error) if error.kind() == ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(MusicStreamError::RtpSendError(error.to_string())),
        }
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.socket
            .local_addr()
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))
    }

    pub fn peer_addr(&self) -> Result<SocketAddr> {
        self.socket
            .peer_addr()
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))
    }

    #[must_use]
    pub fn packets_sent(&self) -> usize {
        self.packets_sent
    }

    #[must_use]
    pub fn bytes_sent(&self) -> usize {
        self.bytes_sent
    }
}

#[cfg(feature = "transport-rtp")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtpPacketizer {
    config: RtpPacketizerConfig,
}

#[cfg(feature = "transport-rtp")]
impl RtpPacketizer {
    pub fn new(config: RtpPacketizerConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { config })
    }

    #[must_use]
    pub fn config(&self) -> &RtpPacketizerConfig {
        &self.config
    }

    pub fn packet_from_frame(
        &self,
        frame: OpusFrame,
        rtp_timestamp: u32,
        sequence: u16,
    ) -> Result<rtp::packet::Packet> {
        if frame.payload.is_empty() {
            return Err(MusicStreamError::RtpSendError(
                "cannot packetize empty Opus payload".to_owned(),
            ));
        }

        if frame.payload.len() > self.config.mtu - RTP_FIXED_HEADER_LEN {
            return Err(MusicStreamError::RtpSendError(
                "Opus payload exceeds RTP mtu".to_owned(),
            ));
        }

        Ok(rtp::packet::Packet {
            header: rtp::header::Header {
                version: 2,
                padding: false,
                extension: false,
                marker: frame.marker,
                payload_type: self.config.payload_type,
                sequence_number: sequence,
                timestamp: rtp_timestamp,
                ssrc: self.config.ssrc,
                ..Default::default()
            },
            payload: frame.payload,
        })
    }

    pub fn marshal_packet(packet: &rtp::packet::Packet, scratch: &mut BytesMut) -> Result<Bytes> {
        use util::marshal::{Marshal, MarshalSize};

        let size = packet.marshal_size();
        scratch.clear();
        scratch.resize(size, 0);
        let written = packet
            .marshal_to(&mut scratch[..])
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
        scratch.truncate(written);
        Ok(Bytes::copy_from_slice(scratch))
    }

    pub fn packetize_send(
        &self,
        frame: OpusFrame,
        rtp_timestamp: u32,
        sequence: u16,
        scratch: &mut BytesMut,
    ) -> Result<RtpPacketized> {
        let marker = frame.marker;
        let payload_len = frame.payload.len();
        let duration_ms = frame.duration_ms;
        let track_position_samples = frame.track_position_samples;
        let packet = self.packet_from_frame(frame, rtp_timestamp, sequence)?;
        let bytes = Self::marshal_packet(&packet, scratch)?;
        Ok(RtpPacketized {
            bytes,
            sequence,
            rtp_timestamp,
            ssrc: self.config.ssrc,
            payload_type: self.config.payload_type,
            marker,
            payload_len,
            duration_ms,
            track_position_samples,
        })
    }
}

#[cfg(feature = "transport-rtp")]
#[derive(Debug)]
pub struct RtpSender {
    core: SenderCore,
    packetizer: RtpPacketizer,
    scratch: BytesMut,
}

#[cfg(feature = "transport-rtp")]
impl RtpSender {
    pub fn new(
        active_generation: u64,
        prebuffer_ms: u64,
        config: RtpPacketizerConfig,
    ) -> Result<Self> {
        Ok(Self {
            core: SenderCore::new(active_generation, prebuffer_ms),
            packetizer: RtpPacketizer::new(config)?,
            scratch: BytesMut::new(),
        })
    }

    pub fn set_active_generation(&mut self, generation: u64) {
        self.core.set_active_generation(generation);
    }

    #[must_use]
    pub fn active_generation(&self) -> u64 {
        self.core.active_generation()
    }

    #[must_use]
    pub fn started(&self) -> bool {
        self.core.started()
    }

    #[must_use]
    pub fn packetizer(&self) -> &RtpPacketizer {
        &self.packetizer
    }

    pub fn next_packet(&mut self, queue: &mut FrameQueue<OpusFrame>) -> Result<RtpSenderStep> {
        match self.core.next_step(queue) {
            SenderStep::WaitPrebuffer {
                queued_ms,
                needed_ms,
            } => Ok(RtpSenderStep::WaitPrebuffer {
                queued_ms,
                needed_ms,
            }),
            SenderStep::Send {
                frame,
                rtp_timestamp,
                sequence,
            } => self
                .packetizer
                .packetize_send(frame, rtp_timestamp, sequence, &mut self.scratch)
                .map(RtpSenderStep::Packet),
            SenderStep::Underrun { generation } => Ok(RtpSenderStep::Underrun { generation }),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::audio::frame::QueueWatermarks;

    fn frame(generation: u64) -> OpusFrame {
        OpusFrame {
            generation,
            payload: Bytes::from_static(b"opus"),
            samples_per_channel: 960,
            duration_ms: 20,
            marker: false,
            track_position_samples: 0,
        }
    }

    #[test]
    fn sender_waits_for_prebuffer_before_first_send() {
        let mut queue = FrameQueue::new(QueueWatermarks::new(20, 100).expect("watermarks"));
        let mut sender = SenderCore::new(1, 40);
        queue.push(frame(1)).expect("push");

        assert!(matches!(
            sender.next_step(&mut queue),
            SenderStep::WaitPrebuffer {
                queued_ms: 20,
                needed_ms: 40
            }
        ));

        queue.push(frame(1)).expect("push");
        assert!(matches!(
            sender.next_step(&mut queue),
            SenderStep::Send {
                rtp_timestamp: 0,
                sequence: 0,
                ..
            }
        ));
    }

    #[test]
    fn sender_drops_stale_frames_through_queue_filter() {
        let mut queue = FrameQueue::new(QueueWatermarks::new(20, 100).expect("watermarks"));
        let mut sender = SenderCore::new(2, 20);
        queue.push(frame(1)).expect("push stale");
        queue.push(frame(2)).expect("push active");

        let step = sender.next_step(&mut queue);
        assert!(matches!(
            step,
            SenderStep::Send {
                rtp_timestamp: 0,
                sequence: 0,
                ..
            }
        ));
        assert_eq!(queue.snapshot().stale_dropped, 1);
    }

    #[test]
    fn underrun_resets_started_and_requires_prebuffer_again() {
        let mut queue = FrameQueue::new(QueueWatermarks::new(20, 100).expect("watermarks"));
        let mut sender = SenderCore::new(1, 20);
        queue.push(frame(1)).expect("push");
        assert!(matches!(
            sender.next_step(&mut queue),
            SenderStep::Send { .. }
        ));
        assert!(sender.started());

        assert!(matches!(
            sender.next_step(&mut queue),
            SenderStep::Underrun { generation: 1 }
        ));
        assert!(!sender.started());
    }

    #[test]
    fn caller_can_start_sender_when_pipeline_declares_short_prebuffer_ready() {
        let mut queue = FrameQueue::new(QueueWatermarks::new(20, 100).expect("watermarks"));
        let mut sender = SenderCore::new(1, 40);
        queue.push(frame(1)).expect("push");

        assert!(matches!(
            sender.next_step(&mut queue),
            SenderStep::WaitPrebuffer {
                queued_ms: 20,
                needed_ms: 40
            }
        ));

        assert!(matches!(
            sender.next_step_with_prebuffer_ready(&mut queue, true),
            SenderStep::Send { .. }
        ));
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtp_sender_packetizes_active_opus_frame_to_wire_bytes() {
        use util::marshal::Unmarshal;

        let mut queue = FrameQueue::new(QueueWatermarks::new(20, 100).expect("watermarks"));
        let mut sender = RtpSender::new(
            1,
            20,
            RtpPacketizerConfig {
                payload_type: 111,
                ssrc: 0x1234_5678,
                mtu: 1_200,
            },
        )
        .expect("rtp sender");
        queue.push(frame(1)).expect("push");

        let packetized = match sender.next_packet(&mut queue).expect("next packet") {
            RtpSenderStep::Packet(packetized) => packetized,
            other => panic!("unexpected rtp sender step: {other:?}"),
        };

        assert_eq!(packetized.sequence, 0);
        assert_eq!(packetized.rtp_timestamp, 0);
        assert_eq!(packetized.ssrc, 0x1234_5678);
        assert_eq!(packetized.payload_type, 111);
        assert_eq!(packetized.payload_len, 4);

        let mut raw = packetized.bytes.clone();
        let packet = rtp::packet::Packet::unmarshal(&mut raw).expect("unmarshal rtp packet");
        assert_eq!(packet.header.version, 2);
        assert_eq!(packet.header.payload_type, 111);
        assert_eq!(packet.header.sequence_number, 0);
        assert_eq!(packet.header.timestamp, 0);
        assert_eq!(packet.header.ssrc, 0x1234_5678);
        assert_eq!(packet.payload, Bytes::from_static(b"opus"));
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtp_sender_preserves_sender_prebuffer_and_underrun_steps() {
        let mut queue = FrameQueue::new(QueueWatermarks::new(20, 100).expect("watermarks"));
        let mut sender = RtpSender::new(1, 40, RtpPacketizerConfig::default()).expect("rtp sender");
        queue.push(frame(1)).expect("push");

        assert!(matches!(
            sender.next_packet(&mut queue).expect("wait prebuffer"),
            RtpSenderStep::WaitPrebuffer {
                queued_ms: 20,
                needed_ms: 40
            }
        ));

        queue.push(frame(1)).expect("push");
        assert!(matches!(
            sender.next_packet(&mut queue).expect("packet"),
            RtpSenderStep::Packet(_)
        ));
        assert!(matches!(
            sender.next_packet(&mut queue).expect("packet"),
            RtpSenderStep::Packet(_)
        ));
        assert!(matches!(
            sender.next_packet(&mut queue).expect("underrun"),
            RtpSenderStep::Underrun { generation: 1 }
        ));
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtp_pacer_schedules_one_packet_per_frame_duration_without_bursting_backlog() {
        let mut pacer = RtpPacer::new();
        assert_eq!(pacer.poll(1_000), RtpPaceDecision::Ready);

        pacer.on_packet_sent(1_000, 20);
        assert_eq!(pacer.next_deadline_ms(), Some(1_020));
        assert_eq!(pacer.poll(1_000), RtpPaceDecision::Wait { delay_ms: 20 });
        assert_eq!(pacer.poll(1_019), RtpPaceDecision::Wait { delay_ms: 1 });
        assert_eq!(pacer.poll(1_020), RtpPaceDecision::Ready);
        assert_eq!(pacer.lateness_ms(1_019), 0);
        assert_eq!(pacer.lateness_ms(1_025), 5);

        pacer.on_packet_sent(1_050, 20);
        assert_eq!(pacer.next_deadline_ms(), Some(1_070));
        assert_eq!(pacer.poll(1_050), RtpPaceDecision::Wait { delay_ms: 20 });
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtp_pacer_resets_after_underrun() {
        let mut pacer = RtpPacer::new();
        pacer.on_packet_sent(10, 20);
        assert_eq!(pacer.poll(20), RtpPaceDecision::Wait { delay_ms: 10 });

        pacer.on_underrun();
        assert_eq!(pacer.next_deadline_ms(), None);
        assert_eq!(pacer.poll(20), RtpPaceDecision::Ready);
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtp_packetizer_rejects_payload_larger_than_mtu() {
        let packetizer = RtpPacketizer::new(RtpPacketizerConfig {
            payload_type: 111,
            ssrc: 1,
            mtu: RTP_FIXED_HEADER_LEN + 3,
        })
        .expect("packetizer");
        let error = packetizer
            .packet_from_frame(frame(1), 0, 0)
            .expect_err("payload exceeds mtu");

        assert_eq!(error.code(), crate::error::ErrorCode::RtpSendError);
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn memory_rtp_sink_retains_packetized_output_for_tests() {
        let mut sink = MemoryRtpPacketSink::default();
        let packet = RtpPacketized {
            bytes: Bytes::from_static(b"rtp"),
            sequence: 3,
            rtp_timestamp: 960,
            ssrc: 1,
            payload_type: 111,
            marker: false,
            payload_len: 4,
            duration_ms: 20,
            track_position_samples: 960,
        };

        sink.send(packet.clone()).expect("send packet");

        assert_eq!(sink.len(), 1);
        assert_eq!(sink.packets()[0], packet);
        sink.clear();
        assert!(sink.is_empty());
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn udp_rtp_sink_sends_wire_bytes_to_connected_peer() {
        let receiver = UdpSocket::bind("127.0.0.1:0").expect("receiver socket");
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .expect("read timeout");
        let mut sink =
            UdpRtpPacketSink::connect("127.0.0.1:0", receiver.local_addr().expect("receiver addr"))
                .expect("udp sink");
        let packet = RtpPacketized {
            bytes: Bytes::from_static(b"rtp"),
            sequence: 3,
            rtp_timestamp: 960,
            ssrc: 1,
            payload_type: 111,
            marker: false,
            payload_len: 4,
            duration_ms: 20,
            track_position_samples: 960,
        };

        sink.send(packet).expect("send packet");

        let mut buffer = [0_u8; 64];
        let (len, peer) = receiver.recv_from(&mut buffer).expect("receive datagram");
        assert_eq!(&buffer[..len], b"rtp");
        assert_eq!(peer, sink.local_addr().expect("sink addr"));
        assert_eq!(sink.packets_sent(), 1);
        assert_eq!(sink.bytes_sent(), 3);
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtp_transport_config_maps_voice_connection_fields_to_packetizer_and_udp() {
        let config = RtpTransportConfig {
            remote_ip: "127.0.0.1".to_owned(),
            remote_rtp_port: 50_000,
            remote_rtcp_port: Some(50_001),
            local_ip: "0.0.0.0".to_owned(),
            local_rtp_port: 0,
            payload_type: 111,
            ssrc: 0x0102_0304,
            mtu: 1_408,
            rtcp_mux: true,
            opus_bitrate_bps: Some(128_000),
            encryption: RtpEncryptionConfig::None,
        };

        config.validate().expect("valid config");

        let packetizer = config.packetizer_config();
        assert_eq!(packetizer.payload_type, 111);
        assert_eq!(packetizer.ssrc, 0x0102_0304);
        assert_eq!(packetizer.mtu, 1_408);
        assert_eq!(config.local_rtp_addr(), "0.0.0.0:0");
        assert_eq!(config.remote_rtp_addr(), "127.0.0.1:50000");
        assert_eq!(config.remote_rtcp_addr(), "127.0.0.1:50000");
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtp_transport_config_rejects_invalid_ports_and_payload_type() {
        let mut config = RtpTransportConfig::new("127.0.0.1", 0, 1);
        assert_eq!(
            config.validate().expect_err("zero remote port").code(),
            crate::error::ErrorCode::InvalidConfig
        );

        config.remote_rtp_port = 50_000;
        config.payload_type = 128;
        assert_eq!(
            config.validate().expect_err("invalid payload type").code(),
            crate::error::ErrorCode::InvalidConfig
        );

        config.payload_type = 96;
        config.rtcp_mux = false;
        config.remote_rtcp_port = None;
        assert_eq!(
            config
                .validate()
                .expect_err("missing rtcp port without mux")
                .code(),
            crate::error::ErrorCode::InvalidConfig
        );
        config.remote_rtcp_port = Some(50_001);
        config.validate().expect("valid non-mux rtcp config");
        assert_eq!(config.remote_rtcp_addr(), "127.0.0.1:50001");
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtcp_sender_report_uses_protocol_crate_wire_format() {
        let report = build_rtcp_sender_report(
            0x1122_3344,
            9_600,
            12,
            480,
            UNIX_EPOCH + std::time::Duration::from_millis(1_500),
        )
        .expect("sender report");

        assert_eq!(report.ssrc, 0x1122_3344);
        assert_eq!(report.rtp_time, 9_600);
        assert_eq!(report.packet_count, 12);
        assert_eq!(report.octet_count, 480);
        assert_eq!(
            report.ntp_time,
            ((NTP_UNIX_EPOCH_OFFSET_SECS + 1) << 32) + (1 << 31)
        );

        let mut raw = report.bytes.clone();
        let packets = rtcp::packet::unmarshal(&mut raw).expect("unmarshal rtcp");
        let sender_report = packets[0]
            .as_any()
            .downcast_ref::<rtcp::sender_report::SenderReport>()
            .expect("sender report packet");
        assert_eq!(sender_report.ssrc, 0x1122_3344);
        assert_eq!(sender_report.rtp_time, 9_600);
        assert_eq!(sender_report.packet_count, 12);
        assert_eq!(sender_report.octet_count, 480);
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn udp_rtcp_sink_sends_sender_report_to_configured_port() {
        let receiver = UdpSocket::bind("127.0.0.1:0").expect("receiver socket");
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .expect("read timeout");
        let mut config = RtpTransportConfig::new("127.0.0.1", 50_000, 0x0102_0304);
        config.rtcp_mux = false;
        config.remote_rtcp_port = Some(receiver.local_addr().expect("receiver addr").port());
        let mut sink = UdpRtcpPacketSink::connect_config(&config).expect("rtcp sink");
        let report =
            build_rtcp_sender_report(0x0102_0304, 0, 1, 4, UNIX_EPOCH).expect("sender report");

        sink.send(report).expect("send rtcp");

        let mut buffer = [0_u8; 128];
        let (len, peer) = receiver.recv_from(&mut buffer).expect("receive rtcp");
        assert!(len > 0);
        assert_eq!(peer, sink.local_addr().expect("sink addr"));
        assert_eq!(sink.packets_sent(), 1);
        assert_eq!(sink.bytes_sent(), len);
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn rtcp_receiver_report_parser_keeps_reports_for_local_ssrc() {
        let received_at = UNIX_EPOCH + std::time::Duration::from_millis(2_000);
        let last_sender_report =
            compact_ntp_timestamp(received_at - std::time::Duration::from_millis(150));
        let delay = ((50_u64 * 65_536) / 1_000) as u32;

        let receiver_report = rtcp::receiver_report::ReceiverReport {
            ssrc: 0xaaaa_bbbb,
            reports: vec![
                rtcp::reception_report::ReceptionReport {
                    ssrc: 0x1111_2222,
                    fraction_lost: 7,
                    total_lost: 3,
                    last_sequence_number: 44,
                    jitter: 9,
                    last_sender_report,
                    delay,
                },
                rtcp::reception_report::ReceptionReport {
                    ssrc: 0x3333_4444,
                    fraction_lost: 99,
                    total_lost: 88,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let bytes = rtcp::packet::marshal(&[Box::new(receiver_report)]).expect("marshal rr");

        let snapshot = parse_rtcp_receiver_reports_at(bytes, 0x1111_2222, 4, received_at)
            .expect("parse rr")
            .expect("matching report");

        assert_eq!(snapshot.reports_received, 5);
        assert_eq!(snapshot.sender_ssrc, 0xaaaa_bbbb);
        assert_eq!(snapshot.source_ssrc, 0x1111_2222);
        assert_eq!(snapshot.fraction_lost, 7);
        assert_eq!(snapshot.total_lost, 3);
        assert_eq!(snapshot.last_sequence_number, 44);
        assert_eq!(snapshot.jitter, 9);
        assert_eq!(snapshot.jitter_micros, 187);
        assert_eq!(snapshot.last_sender_report, last_sender_report);
        assert_eq!(snapshot.delay, delay);
        let rtt = snapshot.round_trip_time_micros.expect("rtt");
        assert!((99_900..=100_100).contains(&rtt));
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn udp_sink_from_transport_config_refuses_uninstalled_encryption() {
        let receiver = UdpSocket::bind("127.0.0.1:0").expect("receiver socket");
        let mut config = RtpTransportConfig::new(
            "127.0.0.1",
            receiver.local_addr().expect("receiver addr").port(),
            1,
        );
        config.encryption = RtpEncryptionConfig::External {
            mode: "xsalsa20_poly1305_lite".to_owned(),
            secret_key: Some(vec![7; 32]),
        };

        let error = UdpRtpPacketSink::connect_config(&config).expect_err("unsupported encryption");
        assert_eq!(error.code(), crate::error::ErrorCode::Unsupported);
    }

    #[cfg(feature = "transport-rtp")]
    #[test]
    fn udp_sink_from_transport_config_can_send_through_installed_packet_protector() {
        #[derive(Debug)]
        struct TestProtector;

        impl RtpPacketProtector for TestProtector {
            fn protect(&mut self, mut packet: RtpPacketized) -> Result<RtpPacketized> {
                let mut protected = Vec::from(packet.bytes.as_ref());
                protected.push(0xaa);
                packet.bytes = Bytes::from(protected);
                Ok(packet)
            }
        }

        let receiver = UdpSocket::bind("127.0.0.1:0").expect("receiver socket");
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .expect("read timeout");
        let mut config = RtpTransportConfig::new(
            "127.0.0.1",
            receiver.local_addr().expect("receiver addr").port(),
            1,
        );
        config.encryption = RtpEncryptionConfig::External {
            mode: "test-protector".to_owned(),
            secret_key: Some(vec![7; 32]),
        };
        let mut sink = UdpRtpPacketSink::connect_config_with_protector(&config, TestProtector)
            .expect("protected udp sink");

        sink.send(RtpPacketized {
            bytes: Bytes::from_static(b"rtp"),
            sequence: 3,
            rtp_timestamp: 960,
            ssrc: 1,
            payload_type: 111,
            marker: false,
            payload_len: 4,
            duration_ms: 20,
            track_position_samples: 960,
        })
        .expect("send packet");

        let mut buffer = [0_u8; 64];
        let len = receiver.recv(&mut buffer).expect("receive datagram");
        assert_eq!(&buffer[..len], b"rtp\xaa");
        assert_eq!(sink.inner().packets_sent(), 1);
        assert_eq!(sink.inner().bytes_sent(), 4);
    }
}
