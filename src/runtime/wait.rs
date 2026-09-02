use crate::domain::ids::ExecutorId;
use crate::domain::outcome::NodeOutcome;
use crate::runtime::executor::{ExecutionContext, Executor};
use std::future::Future;
use std::pin::Pin;

/// Builtin gate executor (`id = "wait"`). Parks the node; another task or
/// process completes it via [`crate::Runtime::complete`].
pub struct Wait;

pub const WAIT_ID: &str = "wait";

impl Wait {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Wait {
    fn default() -> Self {
        Self
    }
}

impl Executor for Wait {
    fn id(&self) -> ExecutorId {
        ExecutorId::new(WAIT_ID)
    }

    fn execute<'a>(
        &'a self,
        ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        Box::pin(async move {
            NodeOutcome::Waiting {
                token: ctx.resume_token,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ids::{ExecutionId, NodeId, ResumeToken};
    use crate::runtime::time::SystemClock;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    #[tokio::test(flavor = "current_thread")]
    async fn wait_execute_returns_ctx_token() {
        let token = ResumeToken::issue(ExecutionId::new(), NodeId::new("hold"), 1);
        let ctx = ExecutionContext {
            execution_id: ExecutionId::new(),
            node_id: NodeId::new("hold"),
            attempt: 1,
            inputs: HashMap::new(),
            cancel: CancellationToken::new(),
            resume_token: token.clone(),
            clock: Arc::new(SystemClock),
        };
        match Wait.execute(ctx).await {
            NodeOutcome::Waiting { token: got } => assert_eq!(got, token),
            other => panic!("{other:?}"),
        }
        assert_eq!(Wait::new().id().as_str(), WAIT_ID);
        assert_eq!(Wait::default().id().as_str(), WAIT_ID);
    }
}
