use std::time::Duration;

use tokio::time::Instant;

use crate::audio::opus::OPUS_SAMPLE_RATE_HZ;

/// Persistent wall-clock projection for the next media sample.
///
/// Output scheduling may rebase its next wake-up after a delayed turn to avoid
/// catch-up bursts. This clock deliberately does not: every sent or discarded
/// frame advances the projection by its media samples, so repeated delays that
/// are individually below the recovery threshold still accumulate and
/// eventually trigger stale-frame recovery.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct PlayoutClock {
    next_media_instant: Option<Instant>,
}

impl PlayoutClock {
    pub(super) fn reanchor(&mut self, now: Instant) {
        self.next_media_instant = Some(now);
    }

    pub(super) fn lateness(&self, now: Instant) -> Duration {
        self.next_media_instant.map_or(Duration::ZERO, |expected| {
            now.saturating_duration_since(expected)
        })
    }

    pub(super) fn deadline_with_tolerance(&self, now: Instant, tolerance: Duration) -> Instant {
        self.next_media_instant.unwrap_or(now) + tolerance
    }

    pub(super) fn advance_samples(&mut self, samples_per_channel: u32, now: Instant) {
        let duration = duration_for_samples(samples_per_channel);
        let expected = self.next_media_instant.get_or_insert(now);
        *expected += duration;
    }
}

fn duration_for_samples(samples: u32) -> Duration {
    Duration::from_nanos(u64::from(samples) * 1_000_000_000 / u64::from(OPUS_SAMPLE_RATE_HZ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::opus::OPUS_FRAME_SAMPLES;

    #[test]
    fn repeated_subthreshold_stalls_accumulate_against_the_media_timeline() {
        let start = Instant::now();
        let mut clock = PlayoutClock::default();
        clock.reanchor(start);
        clock.advance_samples(OPUS_FRAME_SAMPLES, start);

        // A no-burst output loop may rebase its local wake-up after every turn.
        // The media clock must nevertheless retain all three 40 ms deficits.
        for (sent_frames, now_ms) in [(2_u64, 60_u64), (3, 120), (4, 180)] {
            let now = start + Duration::from_millis(now_ms);
            assert_eq!(
                clock.lateness(now),
                Duration::from_millis(now_ms - (sent_frames - 1) * 20)
            );
            clock.advance_samples(OPUS_FRAME_SAMPLES, now);
        }

        assert_eq!(
            clock.lateness(start + Duration::from_millis(180)),
            Duration::from_millis(100)
        );
        assert_eq!(
            clock.lateness(start + Duration::from_millis(200)),
            Duration::from_millis(120)
        );
    }

    #[test]
    fn reanchor_excludes_pause_or_underrun_wall_time() {
        let start = Instant::now();
        let mut clock = PlayoutClock::default();
        clock.reanchor(start);
        clock.advance_samples(OPUS_FRAME_SAMPLES, start);
        assert_eq!(
            clock.lateness(start + Duration::from_secs(5)),
            Duration::from_millis(4_980)
        );

        let resumed = start + Duration::from_secs(5);
        clock.reanchor(resumed);
        assert_eq!(clock.lateness(resumed), Duration::ZERO);
    }
}
