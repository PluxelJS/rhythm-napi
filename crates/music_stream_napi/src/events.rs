use std::sync::{Arc, RwLock};

use napi::bindgen_prelude::Status;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};

use music_stream::{
    ErrorCode, GenerationTaskSlot, LocalFileRtpPlayback, LocalFileRtpPlaybackProgress,
    RtcpNetworkQualityLevel, StreamEvent,
};

use crate::types::{StreamEventOutput, StreamStatusOutput};
use crate::{PlaybackRegistry, Result, lock_error};

pub(crate) type EventCallback =
    Arc<ThreadsafeFunction<StreamEventOutput, (), StreamEventOutput, Status, false, true, 1024>>;

pub(crate) fn internal_error_event(stream_id: &str, message: &str) -> StreamEvent {
    StreamEvent::Error {
        stream_id: stream_id.to_owned(),
        code: ErrorCode::Internal,
        message: message.to_owned(),
    }
}

pub(crate) fn event_belongs_to(event: &StreamEvent, stream_id: &str) -> bool {
    match event {
        StreamEvent::StreamStarted {
            stream_id: event_stream_id,
        }
        | StreamEvent::StreamStopped {
            stream_id: event_stream_id,
        }
        | StreamEvent::NextNeeded {
            stream_id: event_stream_id,
        }
        | StreamEvent::SourceRefreshNeeded {
            stream_id: event_stream_id,
            ..
        }
        | StreamEvent::NetworkQualityChanged {
            stream_id: event_stream_id,
            ..
        }
        | StreamEvent::Error {
            stream_id: event_stream_id,
            ..
        } => event_stream_id == stream_id,
        StreamEvent::StateChanged { status } => status.stream_id == stream_id,
    }
}

pub(crate) fn event_output_from_parts(
    event: StreamEvent,
    playbacks: &Arc<RwLock<PlaybackRegistry>>,
) -> Result<StreamEventOutput> {
    Ok(match event {
        StreamEvent::StreamStarted { stream_id } => base_event("streamStarted", stream_id),
        StreamEvent::StreamStopped { stream_id } => base_event("streamStopped", stream_id),
        StreamEvent::StateChanged { status } => {
            let stream_id = status.stream_id.clone();
            let mut status: StreamStatusOutput = status.into();
            if let Some(progress) = playback_progress_from(playbacks, &stream_id)? {
                status.apply_progress(progress);
            }
            StreamEventOutput {
                r#type: "stateChanged".to_owned(),
                stream_id: Some(stream_id),
                status: Some(status),
                ..empty_event()
            }
        }
        StreamEvent::NextNeeded { stream_id } => base_event("nextNeeded", stream_id),
        StreamEvent::SourceRefreshNeeded {
            stream_id,
            track_id,
        } => StreamEventOutput {
            r#type: "sourceRefreshNeeded".to_owned(),
            stream_id: Some(stream_id),
            track_id: Some(track_id),
            ..empty_event()
        },
        StreamEvent::NetworkQualityChanged {
            stream_id,
            quality,
            snapshot,
        } => StreamEventOutput {
            r#type: "networkQualityChanged".to_owned(),
            stream_id: Some(stream_id),
            quality: Some(network_quality_level(quality).to_owned()),
            quality_samples: Some(snapshot.samples.try_into().unwrap_or(u32::MAX)),
            latest_loss_percent: Some(snapshot.latest_loss_percent),
            average_loss_percent: Some(snapshot.average_loss_percent),
            max_loss_percent: Some(snapshot.max_loss_percent),
            average_jitter_ms: Some(micros_to_ms(snapshot.average_jitter_micros)),
            max_jitter_ms: Some(micros_to_ms(snapshot.max_jitter_micros)),
            average_round_trip_time_ms: snapshot.average_round_trip_time_micros.map(micros_to_ms),
            max_round_trip_time_ms: snapshot.max_round_trip_time_micros.map(micros_to_ms),
            ..empty_event()
        },
        StreamEvent::Error {
            stream_id,
            code,
            message,
        } => StreamEventOutput {
            r#type: "error".to_owned(),
            stream_id: Some(stream_id),
            code: Some(code.as_str().to_owned()),
            message: Some(message),
            ..empty_event()
        },
    })
}

pub(crate) fn push_events(
    events: &Arc<RwLock<Vec<StreamEvent>>>,
    playbacks: &Arc<RwLock<PlaybackRegistry>>,
    event_callback: &Arc<RwLock<Option<EventCallback>>>,
    new_events: Vec<StreamEvent>,
) {
    let _ = push_events_checked(events, playbacks, event_callback, new_events);
}

pub(crate) fn push_events_checked(
    events: &Arc<RwLock<Vec<StreamEvent>>>,
    playbacks: &Arc<RwLock<PlaybackRegistry>>,
    event_callback: &Arc<RwLock<Option<EventCallback>>>,
    new_events: Vec<StreamEvent>,
) -> Result<()> {
    if new_events.is_empty() {
        return Ok(());
    }
    let callback = event_callback.read().map_err(lock_error)?.clone();
    let outputs = if callback.is_some() {
        new_events
            .iter()
            .cloned()
            .map(|event| event_output_from_parts(event, playbacks))
            .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };

    events.write().map_err(lock_error)?.extend(new_events);

    if let Some(callback) = callback {
        for output in outputs {
            let _ = callback.call(output, ThreadsafeFunctionCallMode::NonBlocking);
        }
    }
    Ok(())
}

fn empty_event() -> StreamEventOutput {
    StreamEventOutput {
        r#type: String::new(),
        stream_id: None,
        track_id: None,
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

fn base_event(kind: &str, stream_id: String) -> StreamEventOutput {
    StreamEventOutput {
        r#type: kind.to_owned(),
        stream_id: Some(stream_id),
        ..empty_event()
    }
}

fn network_quality_level(level: RtcpNetworkQualityLevel) -> &'static str {
    match level {
        RtcpNetworkQualityLevel::Good => "good",
        RtcpNetworkQualityLevel::Degraded => "degraded",
        RtcpNetworkQualityLevel::Poor => "poor",
    }
}

fn micros_to_ms(micros: u64) -> f64 {
    micros as f64 / 1_000.0
}

fn playback_progress_from(
    playbacks: &Arc<RwLock<PlaybackRegistry>>,
    stream_id: &str,
) -> Result<Option<LocalFileRtpPlaybackProgress>> {
    Ok(playbacks
        .read()
        .map_err(lock_error)?
        .get(stream_id)
        .and_then(GenerationTaskSlot::get)
        .map(LocalFileRtpPlayback::progress))
}
