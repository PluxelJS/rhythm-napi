use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use napi::bindgen_prelude::Status;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};

use music_stream::{RtcpNetworkQualityLevel, SourceRole, StreamEvent};

use crate::types::{StreamEventOutput, StreamStatusOutput};
use crate::{Result, lock_error};

pub(crate) type EventCallback =
    Arc<ThreadsafeFunction<StreamEventOutput, (), StreamEventOutput, Status, false, true, 1024>>;

#[derive(Clone, Debug, Default)]
pub(crate) struct EventQueue {
    events: Arc<RwLock<VecDeque<QueuedStreamEvent>>>,
    next_sequence: Arc<AtomicU64>,
}

#[derive(Clone, Debug)]
pub(crate) struct QueuedStreamEvent {
    sequence: u64,
    event: StreamEvent,
}

impl EventQueue {
    pub(crate) fn publish(
        &self,
        callback: &Arc<RwLock<Option<EventCallback>>>,
        event: StreamEvent,
    ) {
        let queued = QueuedStreamEvent {
            sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed) + 1,
            event,
        };
        if let Ok(mut events) = self.events.write() {
            if let Some(position) = events
                .iter()
                .rposition(|existing| coalesces(&existing.event, &queued.event))
            {
                events.remove(position);
            }
            if events.len() == 4_096 {
                let removable = events
                    .iter()
                    .position(|queued| {
                        matches!(
                            queued.event,
                            StreamEvent::StateChanged { .. }
                                | StreamEvent::NetworkQualityChanged { .. }
                        )
                    })
                    .unwrap_or(0);
                events.remove(removable);
            }
            events.push_back(queued.clone());
        }
        if let Ok(callback) = callback.read()
            && let Some(callback) = callback.as_ref()
        {
            let _ = callback.call(
                event_output(queued),
                ThreadsafeFunctionCallMode::NonBlocking,
            );
        }
    }

    pub(crate) fn drain(&self, stream_id: Option<&str>) -> Result<Vec<QueuedStreamEvent>> {
        let mut events = self.events.write().map_err(lock_error)?;
        let Some(stream_id) = stream_id else {
            return Ok(std::mem::take(&mut *events).into());
        };
        let (drained, kept): (VecDeque<_>, VecDeque<_>) = std::mem::take(&mut *events)
            .into_iter()
            .partition(|queued| belongs_to(&queued.event, stream_id));
        *events = kept;
        Ok(drained.into())
    }

    pub(crate) fn clear(&self) -> Result<()> {
        self.events.write().map_err(lock_error)?.clear();
        Ok(())
    }
}

fn coalesces(existing: &StreamEvent, incoming: &StreamEvent) -> bool {
    match (existing, incoming) {
        (
            StreamEvent::StateChanged { status: left },
            StreamEvent::StateChanged { status: right },
        ) => left.stream_id == right.stream_id,
        (
            StreamEvent::NetworkQualityChanged {
                stream_id: left, ..
            },
            StreamEvent::NetworkQualityChanged {
                stream_id: right, ..
            },
        ) => left == right,
        _ => false,
    }
}

pub(crate) fn event_output(queued: QueuedStreamEvent) -> StreamEventOutput {
    let sequence = i64::try_from(queued.sequence).unwrap_or(i64::MAX);
    match queued.event {
        StreamEvent::StreamStarted { stream_id } => base(sequence, "streamStarted", stream_id),
        StreamEvent::StreamStopped { stream_id } => base(sequence, "streamStopped", stream_id),
        StreamEvent::StateChanged { status } => StreamEventOutput {
            sequence,
            r#type: "stateChanged".to_owned(),
            stream_id: Some(status.stream_id.clone()),
            status: Some(StreamStatusOutput::from(status)),
            ..empty()
        },
        StreamEvent::NextNeeded { stream_id } => base(sequence, "nextNeeded", stream_id),
        StreamEvent::SourceRefreshNeeded {
            stream_id,
            track_id,
            source_role,
        } => StreamEventOutput {
            sequence,
            r#type: "sourceRefreshNeeded".to_owned(),
            stream_id: Some(stream_id),
            track_id: Some(track_id),
            source_role: Some(
                match source_role {
                    SourceRole::Current => "current",
                    SourceRole::Next => "next",
                }
                .to_owned(),
            ),
            ..empty()
        },
        StreamEvent::NetworkQualityChanged {
            stream_id,
            quality,
            snapshot,
        } => StreamEventOutput {
            sequence,
            r#type: "networkQualityChanged".to_owned(),
            stream_id: Some(stream_id),
            quality: Some(
                match quality {
                    RtcpNetworkQualityLevel::Good => "good",
                    RtcpNetworkQualityLevel::Degraded => "degraded",
                    RtcpNetworkQualityLevel::Poor => "poor",
                }
                .to_owned(),
            ),
            quality_samples: Some(snapshot.samples.try_into().unwrap_or(u32::MAX)),
            latest_loss_percent: Some(snapshot.latest_loss_percent),
            average_loss_percent: Some(snapshot.average_loss_percent),
            max_loss_percent: Some(snapshot.max_loss_percent),
            average_jitter_ms: Some(snapshot.average_jitter_micros as f64 / 1_000.0),
            max_jitter_ms: Some(snapshot.max_jitter_micros as f64 / 1_000.0),
            average_round_trip_time_ms: snapshot
                .average_round_trip_time_micros
                .map(|v| v as f64 / 1_000.0),
            max_round_trip_time_ms: snapshot
                .max_round_trip_time_micros
                .map(|v| v as f64 / 1_000.0),
            ..empty()
        },
        StreamEvent::Error {
            stream_id,
            code,
            message,
        } => StreamEventOutput {
            sequence,
            r#type: "error".to_owned(),
            stream_id: Some(stream_id),
            code: Some(code.as_str().to_owned()),
            message: Some(message),
            ..empty()
        },
    }
}

fn belongs_to(event: &StreamEvent, stream_id: &str) -> bool {
    match event {
        StreamEvent::StateChanged { status } => status.stream_id == stream_id,
        StreamEvent::StreamStarted { stream_id: id }
        | StreamEvent::StreamStopped { stream_id: id }
        | StreamEvent::NextNeeded { stream_id: id }
        | StreamEvent::SourceRefreshNeeded { stream_id: id, .. }
        | StreamEvent::NetworkQualityChanged { stream_id: id, .. }
        | StreamEvent::Error { stream_id: id, .. } => id == stream_id,
    }
}

fn base(sequence: i64, kind: &str, stream_id: String) -> StreamEventOutput {
    StreamEventOutput {
        sequence,
        r#type: kind.to_owned(),
        stream_id: Some(stream_id),
        ..empty()
    }
}

fn empty() -> StreamEventOutput {
    StreamEventOutput {
        sequence: 0,
        r#type: String::new(),
        stream_id: None,
        track_id: None,
        source_role: None,
        quality: None,
        quality_samples: None,
        latest_loss_percent: None,
        average_loss_percent: None,
        max_loss_percent: None,
        average_jitter_ms: None,
        max_jitter_ms: None,
        average_round_trip_time_ms: None,
        max_round_trip_time_ms: None,
        code: None,
        message: None,
        status: None,
    }
}
