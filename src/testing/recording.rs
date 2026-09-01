use crate::domain::events::Event;
use crate::runtime::sink::EventSink;
use std::sync::{Arc, Mutex};

/// In-memory event log for tests. Not a production adapter.
#[derive(Clone, Default)]
pub struct RecordingSink {
    events: Arc<Mutex<Vec<Event>>>,
}

impl RecordingSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> Vec<Event> {
        self.events.lock().expect("recording sink").clone()
    }
}

impl EventSink for RecordingSink {
    fn try_emit(&self, event: &Event) -> Result<(), crate::runtime::sink::SinkError> {
        self.events.lock().expect("recording sink").push(event.clone());
        Ok(())
    }
}
