use std::collections::HashMap;
use std::sync::Mutex;

/// Named hit counters owned by one test double.
///
/// `FailingStore` and `ScriptedExecutor` each hold an `Arc<Failpoints>`.
/// Two registries with the same name do not share hits. [`Failpoints::enable`]
/// arms a name; [`Failpoints::take`] consumes one remaining hit.
#[derive(Debug)]
pub struct Failpoints {
    map: Mutex<HashMap<String, u32>>,
}

impl Failpoints {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    pub fn enable(&self, name: &str, hits: u32) {
        self.lock().insert(name.to_string(), hits);
    }

    pub fn disable(&self, name: &str) {
        self.lock().remove(name);
    }

    pub fn reset(&self) {
        self.lock().clear();
    }

    /// Decrement remaining hits. Returns true when this call should inject a fault.
    pub fn take(&self, name: &str) -> bool {
        match self.lock().get_mut(name) {
            Some(n) if *n > 0 => {
                *n -= 1;
                true
            }
            _ => false,
        }
    }

    pub fn remaining(&self, name: &str) -> u32 {
        self.lock().get(name).copied().unwrap_or(0)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, u32>> {
        self.map.lock().expect("failpoints")
    }
}

impl Default for Failpoints {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::Failpoints;
    use crate::domain::definition::WorkflowDefinition;
    use crate::domain::ids::{ExecutionId, NodeId, ResumeToken};
    use crate::domain::outcome::NodeOutcome;
    use crate::domain::state::Execution;
    use crate::runtime::executor::{ExecutionContext, Executor};
    use crate::runtime::store::{StateStore, StoreError};
    use crate::runtime::time::SystemClock;
    use crate::testing::scripted::ScriptedExecutor;
    use crate::testing::store::FailingStore;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn two_registries_do_not_steal_hits() {
        let a = Failpoints::new();
        let b = Failpoints::default();
        a.enable("store.put", 1);
        b.enable("store.put", 2);

        assert!(a.take("store.put"));
        assert_eq!(a.remaining("store.put"), 0);
        assert!(!a.take("store.put"));
        assert_eq!(b.remaining("store.put"), 2);
        assert!(b.take("store.put"));
        assert_eq!(b.remaining("store.put"), 1);

        a.enable("executor.panic", 3);
        a.disable("executor.panic");
        assert_eq!(a.remaining("executor.panic"), 0);
        assert!(!a.take("executor.panic"));
        assert_eq!(b.remaining("store.put"), 1);

        b.reset();
        assert_eq!(b.remaining("store.put"), 0);
        assert!(!b.take("store.put"));
    }

    fn ctx() -> ExecutionContext {
        ExecutionContext {
            execution_id: ExecutionId::new(),
            node_id: NodeId::new("n"),
            attempt: 1,
            inputs: HashMap::new(),
            cancel: CancellationToken::new(),
            resume_token: ResumeToken::issue(ExecutionId::new(), NodeId::new("n"), 1),
            clock: Arc::new(SystemClock),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stores_and_executors_do_not_share_failpoints() {
        let a = FailingStore::fail_on_nth_put(0);
        let b = FailingStore::fail_on_nth_put(0);
        let def = WorkflowDefinition::builder("wf")
            .node("n", "n")
            .build()
            .unwrap();
        let exec = Execution::new(def);
        let snap = exec.snapshot();

        a.failpoints().enable("store.put", 1);
        let err = a.put(&snap).await.unwrap_err();
        assert!(
            matches!(err, StoreError::Message(ref m) if m.contains("failpoint store.put")),
            "{err:?}"
        );
        let err = b.put(&snap).await.unwrap_err();
        assert!(
            matches!(err, StoreError::Message(ref m) if m.contains("persist first")),
            "other store must not consume the hit: {err:?}"
        );
        assert_eq!(a.failpoints().remaining("store.put"), 0);
        assert_eq!(b.failpoints().remaining("store.put"), 0);

        a.failpoints().enable("store.put", 1);
        let err = a.persist(&exec).await.unwrap_err();
        assert!(
            matches!(err, StoreError::Message(ref m) if m.contains("failpoint store.put")),
            "{err:?}"
        );
        b.persist(&exec).await.expect("other store persist");
        a.persist(&exec).await.expect("hit already consumed on a");

        let exec_a = ScriptedExecutor::new("a");
        let exec_a_clone = exec_a.clone();
        let exec_b = ScriptedExecutor::new("b").succeed(bytes::Bytes::from_static(b"ok"));
        exec_a.failpoints().enable("executor.panic", 1);
        assert_eq!(exec_a_clone.failpoints().remaining("executor.panic"), 1);
        assert_eq!(exec_b.failpoints().remaining("executor.panic"), 0);

        let quiet = tokio::task::spawn(async move { exec_b.execute(ctx()).await }).await;
        assert!(
            matches!(quiet, Ok(NodeOutcome::Succeeded(_))),
            "unarmed executor must not panic: {quiet:?}"
        );
        let tripped = tokio::task::spawn(async move { exec_a_clone.execute(ctx()).await }).await;
        let err = tripped.expect_err("armed executor must panic");
        assert!(err.is_panic());
    }
}
