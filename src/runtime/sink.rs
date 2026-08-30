use crate::domain::events::DomainEvent;
use std::sync::{Arc, Mutex};

pub trait EventSink: Send + Sync {
    fn emit(&self, event: &DomainEvent);
}

#[derive(Clone, Default)]
pub struct NoopSink;

impl EventSink for NoopSink {
    fn emit(&self, _event: &DomainEvent) {}
}

pub struct FnSink<F>(pub F)
where
    F: Fn(&DomainEvent) + Send + Sync;

impl<F> EventSink for FnSink<F>
where
    F: Fn(&DomainEvent) + Send + Sync,
{
    fn emit(&self, event: &DomainEvent) {
        (self.0)(event);
    }
}

#[derive(Clone, Default)]
pub struct RecordingSink {
    events: Arc<Mutex<Vec<DomainEvent>>>,
}

impl RecordingSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> Vec<DomainEvent> {
        self.events.lock().expect("recording sink").clone()
    }
}

impl EventSink for RecordingSink {
    fn emit(&self, event: &DomainEvent) {
        self.events.lock().expect("recording sink").push(event.clone());
    }
}

impl EventSink for Arc<dyn EventSink> {
    fn emit(&self, event: &DomainEvent) {
        (**self).emit(event);
    }
}
