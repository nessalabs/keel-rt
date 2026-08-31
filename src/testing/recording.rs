use crate::domain::events::DomainEvent;
use crate::runtime::sink::EventSink;
use std::sync::{Arc, Mutex};

/// In-memory event log for tests. Not a production adapter.
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
