use crate::domain::ids::NodeId;
use crate::runtime::executor::{ExecutionContext, Executor};
use crate::runtime::inject::{Event, EventTx, JoinKind};
use std::collections::HashMap;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::task::AbortHandle;

/// One `tokio::spawn` per execute. Completions are [`Event::NodeFinished`].
/// The apply task never `.await`s user work.
pub(crate) struct SpawnSet {
    inflight: HashMap<NodeId, AbortHandle>,
    tx: EventTx,
}

impl SpawnSet {
    pub(crate) fn new(tx: EventTx) -> Self {
        Self {
            inflight: HashMap::new(),
            tx,
        }
    }

    pub(crate) fn spawn(&mut self, exec: Arc<dyn Executor>, ctx: ExecutionContext) {
        let node_id = ctx.node_id.clone();
        let key = node_id.clone();
        let attempt = ctx.attempt;
        let tx = self.tx.clone();
        let handle = tokio::spawn(async move {
            let result = match CatchUnwind(AssertUnwindSafe(exec.execute(ctx))).await {
                Ok(outcome) => Ok(outcome),
                Err(payload) => Err(JoinKind::Panic(panic_message(payload))),
            };
            let _ = tx.send(Event::NodeFinished {
                node_id,
                attempt,
                result,
            });
        });
        self.inflight.insert(key, handle.abort_handle());
    }

    pub(crate) fn abort_node(&mut self, id: &NodeId) {
        if let Some(h) = self.inflight.remove(id) {
            h.abort();
        }
    }

    pub(crate) fn abort_all(&mut self) {
        for (_, h) in self.inflight.drain() {
            h.abort();
        }
    }

    pub(crate) fn forget(&mut self, id: &NodeId) {
        self.inflight.remove(id);
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "executor panicked".into()
    }
}

struct CatchUnwind<F>(AssertUnwindSafe<F>);

impl<F: Future> Future for CatchUnwind<F> {
    type Output = Result<F::Output, Box<dyn std::any::Any + Send>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0 .0) };
        match catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}
