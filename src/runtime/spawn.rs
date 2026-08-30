use crate::domain::ids::NodeId;
use crate::runtime::executor::{ExecutionContext, Executor};
use crate::runtime::inject::{Event, EventTx, JoinKind};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::AbortHandle;

/// JoinSet analog: each `execute()` is spawned. Completions are forwarded as
/// [`Event::NodeFinished`] so the apply task never `.await`s user work.
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
        let attempt = ctx.attempt;
        let tx = self.tx.clone();
        let handle = tokio::spawn(async move {
            exec.execute(ctx).await
        });
        self.inflight.insert(node_id.clone(), handle.abort_handle());
        tokio::spawn(async move {
            let result = match handle.await {
                Ok(outcome) => Ok(outcome),
                Err(e) if e.is_cancelled() => Err(JoinKind::Cancelled),
                Err(e) if e.is_panic() => Err(JoinKind::Panic(format!("{e}"))),
                Err(e) => Err(JoinKind::Panic(format!("{e}"))),
            };
            let _ = tx.send(Event::NodeFinished {
                node_id,
                attempt,
                result,
            });
        });
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
