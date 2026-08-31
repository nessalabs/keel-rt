use crate::domain::definition::WorkflowDefinition;
use crate::domain::events::DomainEvent;
use crate::domain::ids::{ExecutionId, NodeId, ResumeToken};
use crate::domain::outcome::Resume;
use crate::domain::policy::Policy;
use crate::domain::snapshot::ExecutionSnapshot;
use crate::domain::state::{ApplyError, ExecutionState, NodeState};
use crate::runtime::executor::Executor;
use crate::runtime::handle::ExecutionHandle;
use crate::runtime::runtime::{Runtime, DEFAULT_CANCEL_BOUND};
use crate::runtime::sink::{NoopSink, RecordingSink};
use crate::runtime::store::{MemoryStore, StateStore};
use crate::testing::clock::FakeClock;
use crate::testing::scripted::ScriptedExecutor;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Fluent builder: `.node` / `.edge` / `.concurrency` / `.policy` / `.store` / `.run()`.
pub struct WorkflowTest {
    workflow_id: String,
    nodes: Vec<(String, Arc<dyn Executor>)>,
    scripted: HashMap<String, ScriptedExecutor>,
    edges: Vec<(String, String)>,
    concurrency: usize,
    policy: Option<Arc<dyn Policy>>,
    store: Option<Arc<dyn StateStore>>,
    clock: Arc<FakeClock>,
    cancel_bound: Duration,
    /// When false, the runtime uses [`NoopSink`] so 100k-node runs do not
    /// clone every DomainEvent into a recording buffer.
    record_events: bool,
}

impl Default for WorkflowTest {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowTest {
    pub fn new() -> Self {
        Self {
            workflow_id: "test-wf".into(),
            nodes: Vec::new(),
            scripted: HashMap::new(),
            edges: Vec::new(),
            concurrency: 8,
            policy: None,
            store: None,
            clock: Arc::new(FakeClock::new()),
            cancel_bound: DEFAULT_CANCEL_BOUND,
            record_events: true,
        }
    }

    /// Pre-size node/edge vectors for large graphs.
    pub fn with_graph_capacity(nodes: usize, edges: usize) -> Self {
        let mut t = Self::new();
        t.nodes.reserve(nodes);
        t.edges.reserve(edges);
        t
    }

    pub fn workflow_id(mut self, id: impl Into<String>) -> Self {
        self.workflow_id = id.into();
        self
    }

    pub fn node(mut self, id: impl Into<String>, executor: ScriptedExecutor) -> Self {
        let id = id.into();
        self.scripted.insert(id.clone(), executor.clone());
        self.nodes.push((id, Arc::new(executor)));
        self
    }

    pub fn executor(mut self, id: impl Into<String>, executor: impl Executor + 'static) -> Self {
        self.nodes.push((id.into(), Arc::new(executor)));
        self
    }

    /// Same `Arc<dyn Executor>` on many nodes (one registry entry when ids match).
    pub fn node_arc(mut self, id: impl Into<String>, exec: Arc<dyn Executor>) -> Self {
        self.nodes.push((id.into(), exec));
        self
    }

    /// Skip `RecordingSink` (scale benches). Scripted `last_inputs` still work.
    pub fn silent(mut self) -> Self {
        self.record_events = false;
        self
    }

    pub fn edge(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.edges.push((from.into(), to.into()));
        self
    }

    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
        self
    }

    pub fn policy(mut self, p: impl Policy + 'static) -> Self {
        self.policy = Some(Arc::new(p));
        self
    }

    pub fn store(mut self, s: impl StateStore + 'static) -> Self {
        self.store = Some(Arc::new(s));
        self
    }

    pub fn store_arc(mut self, s: Arc<dyn StateStore>) -> Self {
        self.store = Some(s);
        self
    }

    pub fn clock(mut self, clock: Arc<FakeClock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn fake_clock(&self) -> Arc<FakeClock> {
        self.clock.clone()
    }

    pub fn cancel_bound(mut self, d: Duration) -> Self {
        self.cancel_bound = d;
        self
    }

    fn build_runtime(&mut self) -> (Runtime, RecordingSink, Arc<dyn StateStore>, WorkflowDefinition) {
        let sink = RecordingSink::new();
        let store: Arc<dyn StateStore> = self
            .store
            .clone()
            .unwrap_or_else(|| Arc::new(MemoryStore::new()));
        let mut builder = Runtime::builder()
            .store_arc(store.clone())
            .concurrency(self.concurrency)
            .cancel_bound(self.cancel_bound)
            .clock(self.clock.clone());
        if self.record_events {
            builder = builder.sink(sink.clone());
        } else {
            builder = builder.sink(NoopSink);
        }
        if let Some(p) = self.policy.clone() {
            builder = builder.policy_arc(p);
        }
        for (_, exec) in &self.nodes {
            builder = builder.register_arc(exec.clone());
        }
        let mut def = WorkflowDefinition::builder(self.workflow_id.as_str());
        for (id, exec) in &self.nodes {
            def = def.node(id.as_str(), exec.id());
        }
        for (from, to) in &self.edges {
            def = def.edge(from.as_str(), to.as_str());
        }
        let definition = def.build().expect("test workflow definition");
        (builder.build(), sink, store, definition)
    }

    /// Start and wait until terminal or Waiting.
    pub async fn run(self) -> TestRun {
        let run = self.start().await;
        run.wait_stable().await;
        run
    }

    /// Start without waiting. Use for mid-run inspect / hang tests.
    pub async fn start(mut self) -> TestRun {
        let scripted = self.scripted.clone();
        let clock = self.clock.clone();
        let (runtime, sink, store, definition) = self.build_runtime();
        let handle = runtime.start(definition);
        let snap = handle.inspect().await;
        TestRun {
            execution_id: snap.execution_id.clone(),
            handle: Some(handle),
            sink,
            store,
            scripted,
            clock,
            last_snapshot: Some(snap),
        }
    }
}

pub struct TestRun {
    execution_id: ExecutionId,
    handle: Option<ExecutionHandle>,
    sink: RecordingSink,
    store: Arc<dyn StateStore>,
    scripted: HashMap<String, ScriptedExecutor>,
    clock: Arc<FakeClock>,
    last_snapshot: Option<ExecutionSnapshot>,
}

impl TestRun {
    pub fn clock(&self) -> Arc<FakeClock> {
        self.clock.clone()
    }

    pub fn scripted(&self, node: &str) -> ScriptedExecutor {
        self.scripted
            .get(node)
            .cloned()
            .unwrap_or_else(|| panic!("no scripted executor for {node}"))
    }

    pub async fn release_hang(&self, node: &str) {
        self.scripted(node).release();
    }

    pub async fn wait_stable(&self) {
        if let Some(h) = &self.handle {
            h.wait_stable().await;
        }
    }

    pub async fn wait(mut self) -> ExecutionState {
        match self.handle.take() {
            Some(h) => h.wait().await,
            None => self.execution_state().await,
        }
    }

    pub async fn snapshot(&self) -> ExecutionSnapshot {
        if let Some(h) = &self.handle {
            let snap = h.inspect().await;
            return snap;
        }
        if let Ok(Some(s)) = self.store.get(&self.execution_id).await {
            return s;
        }
        if let Some(s) = &self.last_snapshot {
            return s.clone();
        }
        panic!("no snapshot")
    }

    pub async fn state(&self, node: &str) -> NodeState {
        let snap = self.snapshot().await;
        snap.node(&NodeId::new(node))
            .map(|n| n.state.clone())
            .unwrap_or(NodeState::Pending)
    }

    pub async fn output(&self, node: &str) -> Option<Bytes> {
        self.snapshot()
            .await
            .node(&NodeId::new(node))
            .and_then(|n| n.output.clone())
    }

    pub async fn inputs(&self, node: &str) -> HashMap<NodeId, Bytes> {
        self.scripted(node).last_inputs().unwrap_or_default()
    }

    pub async fn execution_state(&self) -> ExecutionState {
        self.snapshot().await.state
    }

    pub fn events(&self) -> Vec<DomainEvent> {
        self.sink.events()
    }

    pub async fn resume(&self, token: ResumeToken, resume: Resume) -> Result<(), ApplyError> {
        let h = self.handle.as_ref().expect("handle dropped");
        h.resume(token, resume).await
    }

    pub async fn cancel(&self) {
        if let Some(h) = &self.handle {
            h.cancel().await;
        }
    }

    /// Drop the live handle (cancels; does not detach). Store remains readable.
    pub fn drop_handle(&mut self) {
        self.handle.take();
    }

    pub fn store(&self) -> Arc<dyn StateStore> {
        self.store.clone()
    }

    pub async fn stored_snapshot(&self) -> Option<ExecutionSnapshot> {
        self.store.get(&self.execution_id).await.ok().flatten()
    }

    pub async fn resume_token(&self, node: &str) -> ResumeToken {
        self.snapshot()
            .await
            .node(&NodeId::new(node))
            .and_then(|n| n.resume_token.clone())
            .unwrap_or_else(|| panic!("no resume token on {node}"))
    }
}
