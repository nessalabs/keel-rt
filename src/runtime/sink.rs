use crate::domain::events::DomainEvent;
use std::sync::Arc;

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

impl EventSink for Arc<dyn EventSink> {
    fn emit(&self, event: &DomainEvent) {
        (**self).emit(event);
    }
}
