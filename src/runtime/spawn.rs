use crate::domain::ids::NodeSlot;
use crate::runtime::executor::{ExecutionContext, Executor};
use crate::runtime::inject::{Event, EventTx};
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::task::AbortHandle;

/// One `tokio::spawn` per execute. Completions are [`Event::NodeFinished`].
/// The apply task never `.await`s user work. Inflight is indexed by slot
/// (dense `0..n`); there is no `NodeId` hash on spawn, abort, or forget.
pub(crate) struct SpawnSet {
    inflight: Vec<Option<AbortHandle>>,
    tx: EventTx,
}

impl SpawnSet {
    pub(crate) fn new(tx: EventTx, n: usize) -> Self {
        Self {
            inflight: (0..n).map(|_| None).collect(),
            tx,
        }
    }

    pub(crate) fn spawn(&mut self, slot: NodeSlot, exec: Arc<dyn Executor>, ctx: ExecutionContext) {
        let node_id = ctx.node_id.clone();
        let attempt = ctx.attempt;
        let tx = self.tx.clone();
        let handle = tokio::spawn(async move {
            let result = match CatchUnwind(AssertUnwindSafe(exec.execute(ctx))).await {
                Ok(outcome) => Ok(outcome),
                Err(payload) => Err(panic_message(payload)),
            };
            let _ = tx.send(Event::NodeFinished {
                slot,
                node_id,
                attempt,
                result,
            });
        });
        self.inflight[slot.0] = Some(handle.abort_handle());
    }

    pub(crate) fn abort_node(&mut self, slot: NodeSlot) {
        if let Some(h) = self.inflight[slot.0].take() {
            h.abort();
        }
    }

    pub(crate) fn abort_all(&mut self) {
        for slot in &mut self.inflight {
            if let Some(h) = slot.take() {
                h.abort();
            }
        }
    }

    pub(crate) fn forget(&mut self, slot: NodeSlot) {
        self.inflight[slot.0] = None;
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
        // Safety: `CatchUnwind` is `!Unpin` only because of `F`. We never
        // move `F` after pinning; this is a standard pin projection to the
        // inner future.
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0 .0) };
        match catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}
