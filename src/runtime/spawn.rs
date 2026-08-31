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

    pub(crate) fn inflight_len(&self) -> usize {
        self.inflight.iter().filter(|h| h.is_some()).count()
    }
}

impl Drop for SpawnSet {
    fn drop(&mut self) {
        self.abort_all();
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

pub(crate) struct CatchUnwind<F>(pub(crate) AssertUnwindSafe<F>);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ids::{ExecutionId, NodeId, ResumeToken};
    use crate::domain::outcome::NodeOutcome;
    use crate::runtime::executor::{ExecutionContext, FunctionExecutor};
    use crate::runtime::inject;
    use crate::runtime::time::SystemClock;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drop_aborts_inflight_execute() {
        let dropped = Arc::new(AtomicBool::new(false));
        let started = Arc::new(AtomicBool::new(false));
        let (tx, _rx) = inject::channel();
        let mut set = SpawnSet::new(tx, 1);
        let exec = FunctionExecutor::new("h", {
            let dropped = dropped.clone();
            let started = started.clone();
            move |_ctx: ExecutionContext| {
                let dropped = dropped.clone();
                let started = started.clone();
                async move {
                    let _g = DropFlag(dropped);
                    started.store(true, Ordering::SeqCst);
                    std::future::pending::<NodeOutcome>().await
                }
            }
        });
        let ctx = ExecutionContext {
            execution_id: ExecutionId::new(),
            node_id: NodeId::new("h"),
            attempt: 1,
            inputs: Default::default(),
            cancel: CancellationToken::new(),
            resume_token: ResumeToken::issue(ExecutionId::new(), NodeId::new("h"), 1),
            clock: Arc::new(SystemClock),
        };
        set.spawn(NodeSlot(0), Arc::new(exec), ctx);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if started.load(Ordering::SeqCst) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("execute started");
        assert_eq!(set.inflight_len(), 1);
        drop(set);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if dropped.load(Ordering::SeqCst) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("SpawnSet Drop must abort inflight execute");
    }
}
