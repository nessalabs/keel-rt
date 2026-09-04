use crate::domain::ids::{ExecutionId, ExecutorId, NodeId, ResumeToken};
use crate::domain::outcome::NodeOutcome;
use crate::runtime::time::SharedClock;
use bytes::Bytes;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Per-invoke context. Sleep through [`ExecutionContext::sleep`] (or
/// `ctx.clock.sleep` with [`crate::Clock`] in scope) — do not reach into
/// `src/runtime`.
pub struct ExecutionContext {
    pub execution_id: ExecutionId,
    pub node_id: NodeId,
    pub attempt: u32,
    pub inputs: HashMap<NodeId, Bytes>,
    pub cancel: CancellationToken,
    pub resume_token: ResumeToken,
    pub clock: SharedClock,
}

impl ExecutionContext {
    /// Sleep on the injected [`Clock`](crate::Clock) (`SystemClock` by default).
    /// If [`Self::cancel`] fires, this parks until the execute task is aborted
    /// so a cancelled run cannot busy-loop back into user work.
    pub async fn sleep(&self, duration: Duration) {
        tokio::select! {
            _ = self.clock.sleep(duration) => {}
            _ = self.cancel.cancelled() => {
                std::future::pending::<()>().await;
            }
        }
    }
}

/// Author hook: one named adapter. Implement this, then
/// [`crate::RuntimeBuilder::register`] before [`crate::RuntimeBuilder::build`].
/// Closures go through [`crate::RuntimeBuilder::register_fn`].
/// Graph I/O is predecessor [`Bytes`] keyed by node id — not a typed schema.
pub trait Executor: Send + Sync {
    fn id(&self) -> ExecutorId;
    fn execute<'a>(
        &'a self,
        ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>>;
}

pub struct FunctionExecutor<F> {
    id: ExecutorId,
    f: F,
}

impl<F, Fut> FunctionExecutor<F>
where
    F: Fn(ExecutionContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = NodeOutcome> + Send + 'static,
{
    pub fn new(id: impl Into<ExecutorId>, f: F) -> Self {
        Self { id: id.into(), f }
    }
}

impl<F, Fut> Executor for FunctionExecutor<F>
where
    F: Fn(ExecutionContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = NodeOutcome> + Send + 'static,
{
    fn id(&self) -> ExecutorId {
        self.id.clone()
    }

    fn execute<'a>(
        &'a self,
        ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        Box::pin((self.f)(ctx))
    }
}

/// Builder-owned map. [`Clone`] copies the HashMap (Arc bumps on values);
/// it is not a shared mutex — a clone cannot live-swap another Runtime's
/// adapters. Register is [`crate::RuntimeBuilder`] only.
#[derive(Clone, Default)]
pub struct ExecutorRegistry {
    inner: HashMap<ExecutorId, Arc<dyn Executor>>,
}

impl ExecutorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, exec: Arc<dyn Executor>) {
        if exec.id().as_str().is_empty() {
            return;
        }
        self.inner.insert(exec.id(), exec);
    }

    pub fn get(&self, id: &ExecutorId) -> Option<Arc<dyn Executor>> {
        self.inner.get(id).cloned()
    }

    pub fn ids(&self) -> Vec<ExecutorId> {
        let mut ids: Vec<_> = self.inner.keys().cloned().collect();
        ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::time::SystemClock;
    use bytes::Bytes;

    fn ctx(cancel: CancellationToken) -> ExecutionContext {
        ExecutionContext {
            execution_id: ExecutionId::new(),
            node_id: NodeId::new("n"),
            attempt: 1,
            inputs: HashMap::new(),
            cancel,
            resume_token: ResumeToken::issue(ExecutionId::new(), NodeId::new("n"), 1),
            clock: Arc::new(SystemClock),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sleep_parks_when_cancel_fires_until_abort() {
        let cancel = CancellationToken::new();
        let ctx = ctx(cancel.clone());
        cancel.cancel();
        let raced = tokio::time::timeout(
            Duration::from_millis(80),
            ctx.sleep(Duration::from_secs(60)),
        )
        .await;
        assert!(
            raced.is_err(),
            "sleep must not return to user code after cancel (that busy-loops current_thread)"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sleep_zero_completes_without_cancel() {
        ctx(CancellationToken::new()).sleep(Duration::ZERO).await;
    }

    #[test]
    fn registry_skips_empty_id() {
        let mut reg = ExecutorRegistry::new();
        reg.register(Arc::new(FunctionExecutor::new("", |_ctx| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"nope"))
        })));
        assert!(
            !reg.ids().iter().any(|id| id.as_str().is_empty()),
            "empty id is not a catalog entry: {:?}",
            reg.ids()
        );
        assert!(reg.get(&ExecutorId::new("")).is_none());
    }

    #[test]
    fn registry_same_id_last_wins() {
        let mut reg = ExecutorRegistry::new();
        reg.register(Arc::new(FunctionExecutor::new("tool", |_ctx| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"first"))
        })));
        reg.register(Arc::new(FunctionExecutor::new("tool", |_ctx| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"second"))
        })));
        assert_eq!(
            reg.ids().iter().filter(|id| id.as_str() == "tool").count(),
            1
        );
        assert!(reg.get(&ExecutorId::new("tool")).is_some());
    }

    #[test]
    fn registry_clone_is_independent_map_not_live_swap() {
        let mut a = ExecutorRegistry::new();
        a.register(Arc::new(FunctionExecutor::new("keep", |_ctx| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"a"))
        })));
        let b = a.clone();
        a.register(Arc::new(FunctionExecutor::new("late", |_ctx| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"late"))
        })));
        assert!(
            a.get(&ExecutorId::new("late")).is_some(),
            "builder map still accepts a later insert"
        );
        assert!(
            b.get(&ExecutorId::new("late")).is_none(),
            "Clone is a HashMap copy, not a shared Arc<Mutex>; post-clone insert must not live-swap the other registry"
        );
        assert!(b.get(&ExecutorId::new("keep")).is_some());
    }
}
