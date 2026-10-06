use crate::domain::ids::ExecutorId;
use crate::domain::outcome::{NodeError, NodeOutcome};
use crate::runtime::executor::{ExecutionContext, Executor};
use crate::testing::failpoint::Failpoints;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Clone, Debug)]
pub enum ScriptedAction {
    Succeed(Bytes),
    Fail(String),
    Wait,
    Panic,
    Hang { ignore_cancel: bool },
    Delay { delay: Duration, then: Box<ScriptedAction> },
    /// I/O timeout analog — [`NodeOutcome::TimedOut`], not a Failed string.
    TimedOut,
}

struct Inner {
    id: ExecutorId,
    actions: Mutex<VecDeque<ScriptedAction>>,
    attempts: Mutex<Vec<u32>>,
    last_inputs: Mutex<Option<HashMap<crate::domain::ids::NodeId, Bytes>>>,
    hang: Notify,
    hanging: AtomicBool,
    released: AtomicBool,
}

/// Named executor with a queue of outcomes / panic / hang. Records attempt numbers.
#[derive(Clone)]
pub struct ScriptedExecutor {
    inner: Arc<Inner>,
    failpoints: Arc<Failpoints>,
}

impl ScriptedExecutor {
    pub fn new(id: impl Into<ExecutorId>) -> Self {
        Self {
            inner: Arc::new(Inner {
                id: id.into(),
                actions: Mutex::new(VecDeque::new()),
                attempts: Mutex::new(Vec::new()),
                last_inputs: Mutex::new(None),
                hang: Notify::new(),
                hanging: AtomicBool::new(false),
                released: AtomicBool::new(false),
            }),
            failpoints: Arc::new(Failpoints::new()),
        }
    }

    /// Registry checked at the start of each execute.
    pub fn failpoints(&self) -> Arc<Failpoints> {
        Arc::clone(&self.failpoints)
    }

    pub fn then(self, action: ScriptedAction) -> Self {
        self.inner.actions.lock().expect("script").push_back(action);
        self
    }

    pub fn succeed(self, bytes: impl Into<Bytes>) -> Self {
        self.then(ScriptedAction::Succeed(bytes.into()))
    }

    pub fn fail(self, msg: impl Into<String>) -> Self {
        self.then(ScriptedAction::Fail(msg.into()))
    }

    pub fn wait(self) -> Self {
        self.then(ScriptedAction::Wait)
    }

    pub fn panic(self) -> Self {
        self.then(ScriptedAction::Panic)
    }

    pub fn hang(self, ignore_cancel: bool) -> Self {
        self.then(ScriptedAction::Hang { ignore_cancel })
    }

    pub fn delay_succeed(self, delay: Duration, bytes: impl Into<Bytes>) -> Self {
        self.then(ScriptedAction::Delay {
            delay,
            then: Box::new(ScriptedAction::Succeed(bytes.into())),
        })
    }

    pub fn attempts(&self) -> Vec<u32> {
        self.inner.attempts.lock().expect("script").clone()
    }

    pub fn last_inputs(&self) -> Option<HashMap<crate::domain::ids::NodeId, Bytes>> {
        self.inner.last_inputs.lock().expect("script").clone()
    }

    pub fn release(&self) {
        self.inner.released.store(true, Ordering::SeqCst);
        self.inner.hang.notify_waiters();
    }

    pub fn is_hanging(&self) -> bool {
        self.inner.hanging.load(Ordering::SeqCst)
    }

    pub async fn wait_until_hanging(&self) {
        loop {
            if self.inner.hanging.load(Ordering::SeqCst) {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    fn next_action(&self) -> ScriptedAction {
        self.inner
            .actions
            .lock()
            .expect("script")
            .pop_front()
            .unwrap_or(ScriptedAction::Succeed(Bytes::new()))
    }
}

impl Executor for ScriptedExecutor {
    fn id(&self) -> ExecutorId {
        self.inner.id.clone()
    }

    fn execute<'a>(
        &'a self,
        ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        Box::pin(self.run(ctx))
    }
}

impl ScriptedExecutor {
    async fn run(&self, ctx: ExecutionContext) -> NodeOutcome {
        if self.failpoints.take("executor.panic") {
            panic!("failpoint executor.panic");
        }
        self.inner
            .attempts
            .lock()
            .expect("script")
            .push(ctx.attempt);
        *self.inner.last_inputs.lock().expect("script") = Some(ctx.inputs.clone());
        let action = self.next_action();
        self.eval(action, &ctx).await
    }

    fn eval<'a>(
        &'a self,
        action: ScriptedAction,
        ctx: &'a ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        Box::pin(async move {
            match action {
                ScriptedAction::Succeed(b) => NodeOutcome::Succeeded(b),
                ScriptedAction::Fail(msg) => NodeOutcome::Failed(NodeError::new(msg)),
                ScriptedAction::TimedOut => NodeOutcome::TimedOut,
                ScriptedAction::Wait => NodeOutcome::Waiting {
                    token: ctx.resume_token.clone(),
                },
                ScriptedAction::Panic => panic!("ScriptedExecutor panic"),
                ScriptedAction::Hang { ignore_cancel } => {
                    self.inner.hanging.store(true, Ordering::SeqCst);
                    loop {
                        if self.inner.released.load(Ordering::SeqCst) {
                            self.inner.hanging.store(false, Ordering::SeqCst);
                            return NodeOutcome::Succeeded(Bytes::from_static(b"released"));
                        }
                        if !ignore_cancel && ctx.cancel.is_cancelled() {
                            self.inner.hanging.store(false, Ordering::SeqCst);
                            return NodeOutcome::Failed(NodeError::new("cancelled"));
                        }
                        tokio::select! {
                            _ = self.inner.hang.notified() => {}
                            _ = ctx.cancel.cancelled(), if !ignore_cancel => {}
                            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                        }
                    }
                }
                ScriptedAction::Delay { delay, then } => {
                    tokio::select! {
                        biased;
                        _ = ctx.cancel.cancelled() => {
                            NodeOutcome::Failed(NodeError::new("cancelled"))
                        }
                        _ = ctx.clock.sleep(delay) => self.eval(*then, ctx).await,
                    }
                }
            }
        })
    }
}
