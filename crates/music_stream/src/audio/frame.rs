use std::collections::VecDeque;

use bytes::Bytes;

use crate::error::{MusicStreamError, Result};

pub trait TimedFrame {
    fn generation(&self) -> u64;
    fn duration_ms(&self) -> u64;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpusFrame {
    pub generation: u64,
    pub payload: Bytes,
    pub samples_per_channel: u32,
    pub duration_ms: u64,
    pub marker: bool,
    pub track_position_samples: u64,
}

impl TimedFrame for OpusFrame {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn duration_ms(&self) -> u64 {
        self.duration_ms
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PcmFrame {
    pub generation: u64,
    pub samples_per_channel: u32,
    pub sample_rate: u32,
    pub channels: u16,
    pub track_position_samples: u64,
    pub samples: Vec<f32>,
}

#[derive(Debug)]
pub struct FrameAssembler {
    channels: u16,
    frame_samples_per_channel: u32,
    next_position_samples: u64,
    tail: Vec<f32>,
}

impl FrameAssembler {
    pub fn new(channels: u16, frame_samples_per_channel: u32) -> Result<Self> {
        Self::new_at(channels, frame_samples_per_channel, 0)
    }

    pub fn new_at(
        channels: u16,
        frame_samples_per_channel: u32,
        start_position_samples: u64,
    ) -> Result<Self> {
        if channels == 0 {
            return Err(MusicStreamError::InvalidConfig(
                "frame assembler channels must be greater than zero".to_owned(),
            ));
        }

        if frame_samples_per_channel == 0 {
            return Err(MusicStreamError::InvalidConfig(
                "frame_samples_per_channel must be greater than zero".to_owned(),
            ));
        }

        Ok(Self {
            channels,
            frame_samples_per_channel,
            next_position_samples: start_position_samples,
            tail: Vec::new(),
        })
    }

    pub fn push_interleaved(
        &mut self,
        generation: u64,
        sample_rate: u32,
        samples: &[f32],
    ) -> Result<Vec<PcmFrame>> {
        let channels = usize::from(self.channels);
        if !samples.len().is_multiple_of(channels) {
            return Err(MusicStreamError::InvalidConfig(
                "interleaved sample count must be divisible by channel count".to_owned(),
            ));
        }

        self.tail.extend_from_slice(samples);

        let frame_len = self.frame_len();
        let frame_count = self.tail.len() / frame_len;
        let mut frames = Vec::with_capacity(frame_count);

        if frame_count == 0 {
            return Ok(frames);
        }

        let ready_len = frame_count * frame_len;
        let remainder = self.tail.split_off(ready_len);
        let ready = std::mem::replace(&mut self.tail, remainder);
        for chunk in ready.chunks_exact(frame_len) {
            frames.push(PcmFrame {
                generation,
                samples_per_channel: self.frame_samples_per_channel,
                sample_rate,
                channels: self.channels,
                track_position_samples: self.next_position_samples,
                samples: chunk.to_vec(),
            });
            self.next_position_samples = self
                .next_position_samples
                .saturating_add(u64::from(self.frame_samples_per_channel));
        }

        Ok(frames)
    }

    #[must_use]
    pub fn tail_samples(&self) -> usize {
        self.tail.len()
    }

    #[must_use]
    pub fn tail_samples_per_channel(&self) -> usize {
        self.tail.len() / usize::from(self.channels)
    }

    #[must_use]
    pub fn next_position_samples(&self) -> u64 {
        self.next_position_samples
    }

    #[must_use]
    fn frame_len(&self) -> usize {
        self.frame_samples_per_channel as usize * usize::from(self.channels)
    }
}

impl PcmFrame {
    #[must_use]
    pub fn duration_ms(&self) -> u64 {
        if self.sample_rate == 0 {
            return 0;
        }

        (u64::from(self.samples_per_channel) * 1_000) / u64::from(self.sample_rate)
    }
}

impl TimedFrame for PcmFrame {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn duration_ms(&self) -> u64 {
        self.duration_ms()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueWatermarks {
    pub low_ms: u64,
    pub high_ms: u64,
}

impl QueueWatermarks {
    pub fn new(low_ms: u64, high_ms: u64) -> Result<Self> {
        if low_ms >= high_ms {
            return Err(MusicStreamError::InvalidConfig(
                "queue low watermark must be lower than high watermark".to_owned(),
            ));
        }

        Ok(Self { low_ms, high_ms })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueSnapshot {
    pub len_frames: usize,
    pub duration_ms: u64,
    pub stale_dropped: u64,
}

#[derive(Debug)]
pub struct FrameQueue<T> {
    frames: VecDeque<T>,
    watermarks: QueueWatermarks,
    duration_ms: u64,
    stale_dropped: u64,
}

impl<T> FrameQueue<T>
where
    T: TimedFrame,
{
    #[must_use]
    pub fn new(watermarks: QueueWatermarks) -> Self {
        Self {
            frames: VecDeque::new(),
            watermarks,
            duration_ms: 0,
            stale_dropped: 0,
        }
    }

    pub fn push(&mut self, frame: T) -> Result<()> {
        let frame_duration = frame.duration_ms();
        if self.duration_ms.saturating_add(frame_duration) > self.watermarks.high_ms {
            return Err(MusicStreamError::Busy(
                "frame queue high watermark reached".to_owned(),
            ));
        }

        self.duration_ms = self.duration_ms.saturating_add(frame_duration);
        self.frames.push_back(frame);
        Ok(())
    }

    pub fn pop_active(&mut self, active_generation: u64) -> Option<T> {
        loop {
            let frame = self.frames.pop_front()?;
            self.duration_ms = self.duration_ms.saturating_sub(frame.duration_ms());
            if frame.generation() == active_generation {
                return Some(frame);
            }

            self.stale_dropped = self.stale_dropped.saturating_add(1);
        }
    }

    pub fn clear(&mut self) {
        self.frames.clear();
        self.duration_ms = 0;
    }

    #[must_use]
    pub fn should_fill(&self) -> bool {
        self.duration_ms <= self.watermarks.low_ms
    }

    #[must_use]
    pub fn is_full(&self) -> bool {
        self.duration_ms >= self.watermarks.high_ms
    }

    #[must_use]
    pub fn can_accept_duration(&self, duration_ms: u64) -> bool {
        self.duration_ms.saturating_add(duration_ms) <= self.watermarks.high_ms
    }

    #[must_use]
    pub fn duration_ms(&self) -> u64 {
        self.duration_ms
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    #[must_use]
    pub fn snapshot(&self) -> QueueSnapshot {
        QueueSnapshot {
            len_frames: self.frames.len(),
            duration_ms: self.duration_ms,
            stale_dropped: self.stale_dropped,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(generation: u64, duration_ms: u64) -> OpusFrame {
        OpusFrame {
            generation,
            payload: Bytes::from_static(b"frame"),
            samples_per_channel: 960,
            duration_ms,
            marker: false,
            track_position_samples: 0,
        }
    }

    #[test]
    fn queue_uses_millisecond_high_watermark() {
        let mut queue = FrameQueue::new(QueueWatermarks::new(20, 60).expect("watermarks"));
        queue.push(frame(1, 20)).expect("push 1");
        queue.push(frame(1, 20)).expect("push 2");
        queue.push(frame(1, 20)).expect("push 3");

        let err = queue.push(frame(1, 20)).expect_err("queue should be full");
        assert_eq!(err.code(), crate::error::ErrorCode::Busy);
        assert_eq!(queue.duration_ms(), 60);
        assert!(queue.is_full());
    }

    #[test]
    fn pop_active_drops_stale_generation() {
        let mut queue = FrameQueue::new(QueueWatermarks::new(20, 100).expect("watermarks"));
        queue.push(frame(1, 20)).expect("push stale");
        queue.push(frame(2, 20)).expect("push active");

        let active = queue.pop_active(2).expect("active frame");
        assert_eq!(active.generation, 2);
        assert_eq!(queue.snapshot().stale_dropped, 1);
        assert_eq!(queue.duration_ms(), 0);
    }

    #[test]
    fn low_watermark_tells_worker_when_to_fill() {
        let mut queue = FrameQueue::new(QueueWatermarks::new(40, 100).expect("watermarks"));
        assert!(queue.should_fill());
        queue.push(frame(1, 20)).expect("push");
        queue.push(frame(1, 20)).expect("push");
        assert!(queue.should_fill());
        queue.push(frame(1, 20)).expect("push");
        assert!(!queue.should_fill());
    }

    #[test]
    fn assembler_splits_arbitrary_chunks_and_retains_tail() {
        let mut assembler = FrameAssembler::new(2, 4).expect("assembler");
        let first = assembler
            .push_interleaved(7, 48_000, &[0.0, 0.1, 1.0, 1.1, 2.0, 2.1])
            .expect("first push");
        assert!(first.is_empty());
        assert_eq!(assembler.tail_samples_per_channel(), 3);

        let second = assembler
            .push_interleaved(7, 48_000, &[3.0, 3.1, 4.0, 4.1, 5.0, 5.1])
            .expect("second push");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].generation, 7);
        assert_eq!(second[0].samples_per_channel, 4);
        assert_eq!(
            second[0].samples,
            vec![0.0, 0.1, 1.0, 1.1, 2.0, 2.1, 3.0, 3.1]
        );
        assert_eq!(assembler.tail_samples_per_channel(), 2);
    }

    #[test]
    fn assembler_rejects_partial_interleaved_sample() {
        let mut assembler = FrameAssembler::new(2, 4).expect("assembler");
        let err = assembler
            .push_interleaved(1, 48_000, &[0.0, 1.0, 2.0])
            .expect_err("invalid interleaved samples");
        assert_eq!(err.code(), crate::error::ErrorCode::InvalidConfig);
    }
}
