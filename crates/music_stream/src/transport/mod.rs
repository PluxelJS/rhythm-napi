use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};

use crate::audio::frame::OpusFrame;
use crate::error::{MusicStreamError, Result};

const RTP_HEADER_BYTES: usize = 12;
const DEFAULT_MTU: usize = 1_200;
const MIN_RTP_MTU: usize = 64;
const MAX_RTP_DATAGRAM_BYTES: usize = 65_507;
const MIN_OPUS_BITRATE_BPS: u32 = 500;
const MAX_OPUS_BITRATE_BPS: u32 = 512_000;
const NTP_UNIX_EPOCH_OFFSET_SECS: u64 = 2_208_988_800;
pub const RTP_OPUS_CLOCK_RATE_HZ: u32 = 48_000;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum RtpEncryptionConfig {
    #[default]
    None,
    External {
        mode: String,
        secret_key: Option<Vec<u8>>,
    },
}

impl RtpEncryptionConfig {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::None => Ok(()),
            Self::External { mode, secret_key } => {
                if mode.trim().is_empty()
                    || mode.len() > 64
                    || secret_key
                        .as_ref()
                        .is_some_and(|key| key.is_empty() || key.len() > 4_096)
                {
                    return Err(MusicStreamError::InvalidConfig(
                        "RTP protection mode and key must fit bounded non-empty limits".to_owned(),
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

impl RtpTransportConfig {
    #[must_use]
    pub fn new(remote_ip: impl Into<String>, remote_rtp_port: u16, ssrc: u32) -> Self {
        Self {
            remote_ip: remote_ip.into(),
            remote_rtp_port,
            remote_rtcp_port: None,
            local_ip: "0.0.0.0".to_owned(),
            local_rtp_port: 0,
            payload_type: 96,
            ssrc,
            mtu: DEFAULT_MTU,
            rtcp_mux: true,
            opus_bitrate_bps: None,
            encryption: RtpEncryptionConfig::None,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.remote_ip.trim().is_empty()
            || self.local_ip.trim().is_empty()
            || self.remote_rtp_port == 0
            || (!self.rtcp_mux && self.remote_rtcp_port.is_none())
            || self.remote_rtcp_port == Some(0)
            || self.payload_type > 127
            || self.mtu < MIN_RTP_MTU
            || self.mtu > MAX_RTP_DATAGRAM_BYTES
            || self.opus_bitrate_bps.is_some_and(|bitrate| {
                !(MIN_OPUS_BITRATE_BPS..=MAX_OPUS_BITRATE_BPS).contains(&bitrate)
            })
        {
            return Err(MusicStreamError::InvalidConfig(
                "invalid RTP transport configuration".to_owned(),
            ));
        }
        self.encryption.validate()
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
        format!(
            "{}:{}",
            self.remote_ip,
            if self.rtcp_mux {
                self.remote_rtp_port
            } else {
                self.remote_rtcp_port.unwrap_or(self.remote_rtp_port)
            }
        )
    }

    #[must_use]
    pub fn packetizer_config(&self) -> RtpPacketizerConfig {
        RtpPacketizerConfig {
            payload_type: self.payload_type,
            ssrc: self.ssrc,
            mtu: self.mtu,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtpPacketizerConfig {
    pub payload_type: u8,
    pub ssrc: u32,
    pub mtu: usize,
}

impl Default for RtpPacketizerConfig {
    fn default() -> Self {
        Self {
            payload_type: 96,
            ssrc: 1,
            mtu: DEFAULT_MTU,
        }
    }
}

impl RtpPacketizerConfig {
    pub fn validate(&self) -> Result<()> {
        if self.payload_type > 127 || !(MIN_RTP_MTU..=MAX_RTP_DATAGRAM_BYTES).contains(&self.mtu) {
            return Err(MusicStreamError::InvalidConfig(
                "invalid RTP packetizer configuration".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtpPacketizer {
    config: RtpPacketizerConfig,
}

impl RtpPacketizer {
    pub fn new(config: RtpPacketizerConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { config })
    }

    pub fn packetize(
        &self,
        frame: OpusFrame,
        rtp_timestamp: u32,
        sequence: u16,
        scratch: &mut BytesMut,
    ) -> Result<()> {
        use util::marshal::{Marshal, MarshalSize};

        if frame.payload.is_empty() || frame.payload.len() > self.config.mtu - RTP_HEADER_BYTES {
            return Err(MusicStreamError::RtpSendError(
                "Opus payload does not fit RTP packet".to_owned(),
            ));
        }
        let packet = rtp::packet::Packet {
            header: rtp::header::Header {
                version: 2,
                marker: frame.marker,
                payload_type: self.config.payload_type,
                sequence_number: sequence,
                timestamp: rtp_timestamp,
                ssrc: self.config.ssrc,
                ..Default::default()
            },
            payload: frame.payload,
        };
        scratch.clear();
        scratch.resize(packet.marshal_size(), 0);
        let written = packet
            .marshal_to(&mut scratch[..])
            .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
        scratch.truncate(written);
        Ok(())
    }
}

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtcpSenderReportPacket {
    pub bytes: Bytes,
    pub ntp_time: u64,
}

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
    Ok(RtcpSenderReportPacket { bytes, ntp_time })
}

pub fn parse_rtcp_receiver_reports(
    mut bytes: Bytes,
    local_ssrc: u32,
    previous_reports_received: usize,
) -> Result<Option<RtcpReceiverReportSnapshot>> {
    let packets = rtcp::packet::unmarshal(&mut bytes)
        .map_err(|error| MusicStreamError::RtpSendError(error.to_string()))?;
    let mut count = previous_reports_received;
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
            count += 1;
            latest = Some(RtcpReceiverReportSnapshot {
                reports_received: count,
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
                    SystemTime::now(),
                    report.last_sender_report,
                    report.delay,
                ),
            });
        }
    }
    Ok(latest)
}

#[must_use]
pub fn ntp_timestamp(time: SystemTime) -> u64 {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let seconds = duration
        .as_secs()
        .saturating_add(NTP_UNIX_EPOCH_OFFSET_SECS)
        & 0xffff_ffff;
    let fraction = ((u128::from(duration.subsec_nanos()) << 32) / 1_000_000_000) as u64;
    (seconds << 32) | fraction
}

#[must_use]
pub fn compact_ntp_timestamp(time: SystemTime) -> u32 {
    ((ntp_timestamp(time) >> 16) & 0xffff_ffff) as u32
}

#[must_use]
pub fn rtp_jitter_micros(jitter: u32, clock_rate_hz: u32) -> u64 {
    if clock_rate_hz == 0 {
        0
    } else {
        u64::from(jitter) * 1_000_000 / u64::from(clock_rate_hz)
    }
}

#[must_use]
pub fn rtcp_compact_duration_micros(duration: u32) -> u64 {
    u64::from(duration) * 1_000_000 / 65_536
}

#[must_use]
pub fn rtcp_round_trip_time_micros(
    arrival: SystemTime,
    last_sender_report: u32,
    delay: u32,
) -> Option<u64> {
    if last_sender_report == 0 {
        return None;
    }
    let arrival = compact_ntp_timestamp(arrival);
    let elapsed = arrival.wrapping_sub(last_sender_report);
    (elapsed >= delay).then(|| rtcp_compact_duration_micros(elapsed - delay))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_rejects_mtu_larger_than_a_udp_datagram() {
        let mut config = RtpTransportConfig::new("127.0.0.1", 5_000, 1);
        config.mtu = MAX_RTP_DATAGRAM_BYTES + 1;
        assert_eq!(
            config.validate().expect_err("oversized MTU").code(),
            crate::ErrorCode::InvalidConfig
        );
    }
}
