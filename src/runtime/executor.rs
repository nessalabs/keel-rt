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
    pub async fn sleep(&self, duration: Duration) {
        self.clock.sleep(duration).await;
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

    pub fn get(&self, id: &ExecutorId) -> Option<Arc<dyn Executor>> {
        self.inner.get(id).cloned()
    }
}
