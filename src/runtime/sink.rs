use crate::domain::events::Event;
use std::sync::Arc;
use thiserror::Error;

/// Sink could not announce. Persist already succeeded; the execution stays.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SinkError {
    #[error("event sink: {0}")]
    Message(String),
}

pub trait EventSink: Send + Sync {
    /// Announce after persist `Ok`. Default swallows [`Self::try_emit`] `Err`.
    /// The scheduler calls [`Self::try_emit`]; implement that method.
    fn emit(&self, event: &Event) {
        let _ = self.try_emit(event);
    }

    /// Returning `Err` does not fail the execution or un-persist the snapshot.
    fn try_emit(&self, event: &Event) -> Result<(), SinkError>;
}

#[derive(Clone, Default)]
pub struct NoopSink;

impl EventSink for NoopSink {
    fn try_emit(&self, _event: &Event) -> Result<(), SinkError> {
        Ok(())
    }
}

pub struct FnSink<F>(pub F)
where
    F: Fn(&Event) + Send + Sync;

impl<F> EventSink for FnSink<F>
where
    F: Fn(&Event) + Send + Sync,
{
    fn try_emit(&self, event: &Event) -> Result<(), SinkError> {
        (self.0)(event);
        Ok(())
    }
}

impl EventSink for Arc<dyn EventSink> {
    fn try_emit(&self, event: &Event) -> Result<(), SinkError> {
        (**self).try_emit(event)
    }
}
