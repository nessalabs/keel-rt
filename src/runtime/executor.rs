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
    /// Sleep on the execution clock (FakeClock in tests, system clock in apps).
    /// Returns when `duration` elapses **or** [`Self::cancel`] fires, so a
    /// cancelled execution does not wake into more user work. Abort of the
    /// execute task still drops this future if the executor ignores cancel.
    pub async fn sleep(&self, duration: Duration) {
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => {}
            _ = self.clock.sleep(duration) => {}
        }
    }
}

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

#[derive(Clone, Default)]
pub struct ExecutorRegistry {
    inner: HashMap<ExecutorId, Arc<dyn Executor>>,
}

impl ExecutorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, exec: Arc<dyn Executor>) {
        self.inner.insert(exec.id(), exec);
    }

    pub     fn get(&self, id: &ExecutorId) -> Option<Arc<dyn Executor>> {
        self.inner.get(id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::time::SystemClock;

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
    async fn sleep_returns_when_cancel_fires() {
        let cancel = CancellationToken::new();
        let ctx = ctx(cancel.clone());
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), ctx.sleep(Duration::from_secs(60)))
            .await
            .expect("ctx.sleep must return on cancel, not wait the full duration");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sleep_zero_completes_without_cancel() {
        ctx(CancellationToken::new())
            .sleep(Duration::ZERO)
            .await;
    }
}
