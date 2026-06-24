//! RTCP quality aggregation.
//!
//! Transport parsing produces one receiver-report snapshot at a time. This
//! module keeps the small rolling window used by session/runtime metrics so the
//! protocol parser stays free of policy decisions.

use std::collections::VecDeque;

#[cfg(feature = "transport-rtp")]
use crate::transport::RtcpReceiverReportSnapshot;

const DEFAULT_RTCP_QUALITY_WINDOW_REPORTS: usize = 32;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RtcpNetworkQualityLevel {
    #[default]
    Good,
    Degraded,
    Poor,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtcpQualityWindowConfig {
    pub max_reports: usize,
}

impl Default for RtcpQualityWindowConfig {
    fn default() -> Self {
        Self {
            max_reports: DEFAULT_RTCP_QUALITY_WINDOW_REPORTS,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RtcpQualitySample {
    pub reports_received: usize,
    pub fraction_lost: u8,
    pub jitter_micros: u64,
    pub round_trip_time_micros: Option<u64>,
}

#[cfg(feature = "transport-rtp")]
impl From<RtcpReceiverReportSnapshot> for RtcpQualitySample {
    fn from(snapshot: RtcpReceiverReportSnapshot) -> Self {
        Self {
            reports_received: snapshot.reports_received,
            fraction_lost: snapshot.fraction_lost,
            jitter_micros: snapshot.jitter_micros,
            round_trip_time_micros: snapshot.round_trip_time_micros,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RtcpQualityWindowSnapshot {
    pub samples: usize,
    pub level: RtcpNetworkQualityLevel,
    pub latest_fraction_lost: u8,
    pub latest_loss_percent: f64,
    pub average_loss_percent: f64,
    pub max_loss_percent: f64,
    pub average_jitter_micros: u64,
    pub max_jitter_micros: u64,
    pub average_round_trip_time_micros: Option<u64>,
    pub max_round_trip_time_micros: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct RtcpQualityWindow {
    max_reports: usize,
    samples: VecDeque<RtcpQualitySample>,
}

impl Default for RtcpQualityWindow {
    fn default() -> Self {
        Self::new(RtcpQualityWindowConfig::default())
    }
}

impl RtcpQualityWindow {
    #[must_use]
    pub fn new(config: RtcpQualityWindowConfig) -> Self {
        Self {
            max_reports: config.max_reports.max(1),
            samples: VecDeque::with_capacity(config.max_reports.max(1)),
        }
    }

    pub fn observe(&mut self, sample: impl Into<RtcpQualitySample>) -> RtcpQualityWindowSnapshot {
        if self.samples.len() == self.max_reports {
            self.samples.pop_front();
        }
        self.samples.push_back(sample.into());
        self.snapshot()
    }

    #[must_use]
    pub fn snapshot(&self) -> RtcpQualityWindowSnapshot {
        let Some(latest) = self.samples.back().copied() else {
            return RtcpQualityWindowSnapshot::default();
        };

        let samples = self.samples.len();
        let loss_sum: u64 = self
            .samples
            .iter()
            .map(|sample| u64::from(sample.fraction_lost))
            .sum();
        let max_loss = self
            .samples
            .iter()
            .map(|sample| sample.fraction_lost)
            .max()
            .unwrap_or(0);
        let jitter_sum: u128 = self
            .samples
            .iter()
            .map(|sample| u128::from(sample.jitter_micros))
            .sum();
        let max_jitter = self
            .samples
            .iter()
            .map(|sample| sample.jitter_micros)
            .max()
            .unwrap_or(0);
        let rtt_values = self
            .samples
            .iter()
            .filter_map(|sample| sample.round_trip_time_micros)
            .collect::<Vec<_>>();
        let average_rtt = average_u64(&rtt_values);
        let max_rtt = rtt_values.into_iter().max();

        RtcpQualityWindowSnapshot {
            samples,
            level: classify_quality(
                fraction_lost_percent_average(loss_sum, samples),
                max_jitter,
                average_rtt,
            ),
            latest_fraction_lost: latest.fraction_lost,
            latest_loss_percent: fraction_lost_percent(latest.fraction_lost),
            average_loss_percent: fraction_lost_percent_average(loss_sum, samples),
            max_loss_percent: fraction_lost_percent(max_loss),
            average_jitter_micros: average_u128(jitter_sum, samples),
            max_jitter_micros: max_jitter,
            average_round_trip_time_micros: average_rtt,
            max_round_trip_time_micros: max_rtt,
        }
    }
}

fn classify_quality(
    average_loss_percent: f64,
    max_jitter_micros: u64,
    average_round_trip_time_micros: Option<u64>,
) -> RtcpNetworkQualityLevel {
    let average_rtt = average_round_trip_time_micros.unwrap_or(0);
    if average_loss_percent >= 10.0 || max_jitter_micros >= 80_000 || average_rtt >= 500_000 {
        return RtcpNetworkQualityLevel::Poor;
    }
    if average_loss_percent >= 2.0 || max_jitter_micros >= 30_000 || average_rtt >= 200_000 {
        return RtcpNetworkQualityLevel::Degraded;
    }
    RtcpNetworkQualityLevel::Good
}

fn fraction_lost_percent(fraction_lost: u8) -> f64 {
    f64::from(fraction_lost) * 100.0 / 256.0
}

fn fraction_lost_percent_average(fraction_lost_sum: u64, samples: usize) -> f64 {
    if samples == 0 {
        return 0.0;
    }
    fraction_lost_sum as f64 * 100.0 / (samples as f64 * 256.0)
}

fn average_u128(sum: u128, count: usize) -> u64 {
    if count == 0 {
        return 0;
    }
    (sum / count as u128).try_into().unwrap_or(u64::MAX)
}

fn average_u64(values: &[u64]) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    Some(average_u128(
        values.iter().map(|value| u128::from(*value)).sum(),
        values.len(),
    ))
}

#[cfg(all(test, feature = "transport-rtp"))]
mod tests {
    use super::{
        RtcpNetworkQualityLevel, RtcpQualitySample, RtcpQualityWindow, RtcpQualityWindowConfig,
    };

    #[test]
    fn quality_window_aggregates_recent_receiver_reports() {
        let mut window = RtcpQualityWindow::new(RtcpQualityWindowConfig { max_reports: 3 });

        window.observe(sample(1, 0, 1_000, Some(10_000)));
        window.observe(sample(2, 26, 2_000, None));
        let snapshot = window.observe(sample(3, 52, 3_000, Some(30_000)));

        assert_eq!(snapshot.samples, 3);
        assert_eq!(snapshot.level, RtcpNetworkQualityLevel::Poor);
        assert_eq!(snapshot.latest_fraction_lost, 52);
        assert_eq!(snapshot.latest_loss_percent, 20.3125);
        assert_eq!(snapshot.average_loss_percent, 10.15625);
        assert_eq!(snapshot.max_loss_percent, 20.3125);
        assert_eq!(snapshot.average_jitter_micros, 2_000);
        assert_eq!(snapshot.max_jitter_micros, 3_000);
        assert_eq!(snapshot.average_round_trip_time_micros, Some(20_000));
        assert_eq!(snapshot.max_round_trip_time_micros, Some(30_000));
    }

    #[test]
    fn quality_window_evicts_oldest_samples() {
        let mut window = RtcpQualityWindow::new(RtcpQualityWindowConfig { max_reports: 2 });

        window.observe(sample(1, 255, 50_000, Some(100_000)));
        window.observe(sample(2, 0, 1_000, None));
        let snapshot = window.observe(sample(3, 0, 3_000, Some(20_000)));

        assert_eq!(snapshot.samples, 2);
        assert_eq!(snapshot.level, RtcpNetworkQualityLevel::Good);
        assert_eq!(snapshot.max_loss_percent, 0.0);
        assert_eq!(snapshot.average_jitter_micros, 2_000);
        assert_eq!(snapshot.average_round_trip_time_micros, Some(20_000));
    }

    #[test]
    fn quality_window_classifies_degraded_jitter_and_rtt() {
        let mut window = RtcpQualityWindow::new(RtcpQualityWindowConfig { max_reports: 3 });

        let jitter = window.observe(sample(1, 0, 30_000, None));
        assert_eq!(jitter.level, RtcpNetworkQualityLevel::Degraded);

        let mut window = RtcpQualityWindow::new(RtcpQualityWindowConfig { max_reports: 3 });
        let rtt = window.observe(sample(1, 0, 1_000, Some(200_000)));
        assert_eq!(rtt.level, RtcpNetworkQualityLevel::Degraded);
    }

    fn sample(
        reports_received: usize,
        fraction_lost: u8,
        jitter_micros: u64,
        round_trip_time_micros: Option<u64>,
    ) -> RtcpQualitySample {
        RtcpQualitySample {
            reports_received,
            fraction_lost,
            jitter_micros,
            round_trip_time_micros,
        }
    }
}
