//! Cross-layer load: one shared Wasm executor, native nodes, retries, snapshots,
//! events, and cancellation on a current-thread Runtime. No kernel internals.

use anyhow::Result;
use keel_rt::{
    Event, ExecutionContext, ExecutionState, FnSink, MemoryStore, NodeId, NodeOutcome, RetryPolicy,
    Runtime, StateStore, WorkflowDefinition,
};
use keel_rt_wasm::{
    wasmtime::{
        component::{Instance, Linker},
        Store,
    },
    ComponentTask, HostState, Limits, WasmExecutor,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, Mutex,
    },
    time::Duration,
};

const COMPONENT: &str = r#"(component
  (import "entered" (func $entered))
  (core func $entered (canon lower (func $entered)))
  (core module $m
    (import "host" "entered" (func $entered))
    (memory 1)
    (func (export "run") (param $input i32) (param $fail i32) (result i32)
      (local $remaining i32)
      ;; Mutate guest memory before any trap. Retry must receive fresh memory.
      i32.const 0 i32.const 0 i32.load i32.const 1 i32.add i32.store
      i32.const 2000 local.set $remaining
      (loop $work
        local.get $remaining i32.const 1 i32.sub local.tee $remaining br_if $work)
      local.get $fail if unreachable end
      local.get $input i32.const 0 i32.load i32.add)
    (func (export "spin")
      call $entered
      (loop $spin br $spin)))
  (core instance $host (export "entered" (func $entered)))
  (core instance $i (instantiate $m (with "host" (instance $host))))
  (func (export "run") (param "input" u32) (param "fail" u32) (result u32)
    (canon lift (core func $i "run")))
  (func (export "spin") (canon lift (core func $i "spin"))))"#;

#[derive(Default)]
struct Counts {
    live: AtomicUsize,
    peak: AtomicUsize,
    created: AtomicUsize,
    dropped: AtomicUsize,
    busy_entered: AtomicUsize,
}

struct Invocation {
    counts: Arc<Counts>,
    calls: usize,
}

impl Drop for Invocation {
    fn drop(&mut self) {
        self.counts.live.fetch_sub(1, SeqCst);
        self.counts.dropped.fetch_add(1, SeqCst);
    }
}

struct Work {
    counts: Arc<Counts>,
    trap_first: bool,
}

impl ComponentTask for Work {
    type State = Invocation;

    fn create_state(&self, _: &ExecutionContext) -> Result<Invocation> {
        let live = self.counts.live.fetch_add(1, SeqCst) + 1;
        self.counts.peak.fetch_max(live, SeqCst);
        self.counts.created.fetch_add(1, SeqCst);
        Ok(Invocation {
            counts: self.counts.clone(),
            calls: 0,
        })
    }

    fn link(&self, linker: &mut Linker<HostState<Invocation>>) -> Result<()> {
        linker.root().func_wrap("entered", |store, (): ()| {
            store.data().user.counts.busy_entered.fetch_add(1, SeqCst);
            Ok(())
        })?;
        Ok(())
    }

    async fn run(
        &self,
        store: &mut Store<HostState<Invocation>>,
        instance: Instance,
        ctx: ExecutionContext,
    ) -> Result<NodeOutcome> {
        store.data_mut().user.calls += 1;
        assert_eq!(
            store.data().user.calls,
            1,
            "host state leaked between invocations"
        );
        if ctx.node_id.as_str().starts_with("busy-") {
            let spin = instance.get_typed_func::<(), ()>(&mut *store, "spin")?;
            spin.call_async(store, ()).await?;
            unreachable!("busy guest must be cancelled")
        }
        assert_eq!(ctx.inputs.len(), 1);
        let input = decode(ctx.inputs.values().next().unwrap());
        let fail = u32::from(self.trap_first && input.is_multiple_of(7) && ctx.attempt == 1);
        let run = instance.get_typed_func::<(u32, u32), (u32,)>(&mut *store, "run")?;
        let (output,) = run.call_async(store, (input, fail)).await?;
        // A reused guest store would return input + 2 after its first trap.
        assert_eq!(output, input + 1, "guest memory leaked between invocations");
        Ok(NodeOutcome::succeeded(output.to_le_bytes().to_vec()))
    }
}

fn decode(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("four-byte output"))
}

fn diamonds(name: &str, count: usize) -> WorkflowDefinition {
    let mut def = WorkflowDefinition::builder(name);
    for i in 0..count {
        let source = format!("source-{i}");
        let a = format!("wasm-a-{i}");
        let b = format!("wasm-b-{i}");
        let join = format!("join-{i}");
        def = def
            .node(source.as_str(), "source")
            .node(a.as_str(), "wasm")
            .node(b.as_str(), "wasm")
            .node(join.as_str(), "join")
            .edge(source.as_str(), a.as_str())
            .edge(source.as_str(), b.as_str())
            .edge(a.as_str(), join.as_str())
            .edge(b.as_str(), join.as_str());
    }
    def.build().unwrap()
}

fn runtime(
    counts: Arc<Counts>,
    trap_first: bool,
    concurrency: usize,
    store: MemoryStore,
    events: Arc<Mutex<Vec<Event>>>,
) -> Runtime {
    let wasm = WasmExecutor::new(
        "wasm",
        COMPONENT,
        Work { counts, trap_first },
        Limits {
            fuel: if trap_first { 1_000_000 } else { u64::MAX },
            yield_interval: 500,
            ..Limits::default()
        },
    )
    .unwrap();
    Runtime::builder()
        .concurrency(concurrency)
        .store(store)
        .policy(RetryPolicy::new(2, Duration::from_millis(1)))
        .sink(FnSink(move |event: &Event| {
            events.lock().unwrap().push(event.clone())
        }))
        .register(wasm)
        .register_fn("source", |ctx| async move {
            let i: u32 = ctx
                .node_id
                .as_str()
                .strip_prefix("source-")
                .unwrap()
                .parse()
                .unwrap();
            NodeOutcome::succeeded(i.to_le_bytes().to_vec())
        })
        .register_fn("join", |ctx| async move {
            let i: u32 = ctx
                .node_id
                .as_str()
                .strip_prefix("join-")
                .unwrap()
                .parse()
                .unwrap();
            assert_eq!(ctx.inputs.len(), 2, "join ran before both Wasm branches");
            for value in ctx.inputs.values() {
                assert_eq!(decode(value), i + 1);
            }
            NodeOutcome::succeeded((2 * (i + 1)).to_le_bytes().to_vec())
        })
        .build()
}

#[tokio::test(flavor = "current_thread")]
async fn mixed_2048_nodes_trap_retry_join_and_persist_without_state_leaks() {
    const DIAMONDS: usize = 512;
    const CONCURRENCY: usize = 32;
    let counts = Arc::new(Counts::default());
    let store = MemoryStore::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let rt = runtime(
        counts.clone(),
        true,
        CONCURRENCY,
        store.clone(),
        events.clone(),
    );
    let handle = rt.start(diamonds("mixed", DIAMONDS)).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), handle.wait())
            .await
            .unwrap(),
        ExecutionState::Succeeded
    );
    let snapshot = store.get(&id).await.unwrap().unwrap();
    assert_eq!(snapshot.state, ExecutionState::Succeeded);
    assert_eq!(snapshot.nodes.len(), DIAMONDS * 4);
    for i in 0..DIAMONDS {
        let node = snapshot.node(&NodeId::new(format!("join-{i}"))).unwrap();
        assert_eq!(decode(node.output.as_ref().unwrap()), 2 * (i as u32 + 1));
        for branch in ["a", "b"] {
            let node = snapshot
                .node(&NodeId::new(format!("wasm-{branch}-{i}")))
                .unwrap();
            assert_eq!(node.attempt, if i % 7 == 0 { 2 } else { 1 });
        }
    }
    let retries = (0..DIAMONDS).filter(|i| i % 7 == 0).count() * 2;
    assert_eq!(counts.created.load(SeqCst), DIAMONDS * 2 + retries);
    assert_eq!(counts.dropped.load(SeqCst), counts.created.load(SeqCst));
    assert_eq!(counts.live.load(SeqCst), 0);
    assert!(
        counts.peak.load(SeqCst) > 1,
        "test did not overlap guest stores"
    );
    assert!(counts.peak.load(SeqCst) <= CONCURRENCY);
    let events = events.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::NodeAttemptFailed { .. }))
            .count(),
        retries
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::NodeSucceeded { .. }))
            .count(),
        DIAMONDS * 4
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::NodeStarted { .. }))
            .count(),
        DIAMONDS * 4 + retries
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::ExecutionSucceeded { .. }))
            .count(),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_busy_workflow_preserves_other_runs_and_releases_every_store() {
    const ROUNDS: usize = 8;
    const CONCURRENCY: usize = 16;
    const DIAMONDS: usize = 64;
    let counts = Arc::new(Counts::default());
    let store = MemoryStore::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let rt = runtime(
        counts.clone(),
        false,
        CONCURRENCY,
        store.clone(),
        events.clone(),
    );
    for round in 0..ROUNDS {
        let mut busy = WorkflowDefinition::builder(format!("busy-{round}"));
        for i in 0..64 {
            busy = busy.node(format!("busy-{i}"), "wasm");
        }
        let before = counts.busy_entered.load(SeqCst);
        let target = rt.start(busy.build().unwrap()).unwrap();
        let target_id = target.execution_id().clone();
        // Confirm guest entry (host import called from inside spin), not merely dispatch.
        tokio::time::timeout(Duration::from_secs(5), async {
            while counts.busy_entered.load(SeqCst) < before + CONCURRENCY {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let survivor = rt
            .start(diamonds(&format!("survivor-{round}"), DIAMONDS))
            .unwrap();
        let survivor_id = survivor.execution_id().clone();
        // The healthy workflow must finish while busy guests still occupy their permits.
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), survivor.wait())
                .await
                .unwrap(),
            ExecutionState::Succeeded
        );
        assert_eq!(target.inspect().await.state, ExecutionState::Running);
        tokio::time::timeout(Duration::from_secs(2), async {
            target.cancel().await;
            assert_eq!(target.wait().await, ExecutionState::Cancelled);
        })
        .await
        .unwrap();
        assert_eq!(
            store.get(&target_id).await.unwrap().unwrap().state,
            ExecutionState::Cancelled
        );
        let snapshot = store.get(&survivor_id).await.unwrap().unwrap();
        assert_eq!(snapshot.state, ExecutionState::Succeeded);
        for i in 0..DIAMONDS {
            assert_eq!(
                decode(
                    snapshot
                        .node(&NodeId::new(format!("join-{i}")))
                        .unwrap()
                        .output
                        .as_ref()
                        .unwrap()
                ),
                2 * (i as u32 + 1)
            );
        }
        assert_eq!(
            counts.busy_entered.load(SeqCst),
            before + CONCURRENCY,
            "pending busy nodes ran after cancel"
        );
        assert_eq!(counts.live.load(SeqCst), 0);
        assert_eq!(counts.created.load(SeqCst), counts.dropped.load(SeqCst));
        let events = events.lock().unwrap();
        let survivor_events: Vec<_> = events
            .iter()
            .filter(|e| e.execution_id() == &survivor_id)
            .collect();
        assert_eq!(
            survivor_events
                .iter()
                .filter(|e| matches!(e, Event::NodeSucceeded { .. }))
                .count(),
            DIAMONDS * 4
        );
        assert!(!survivor_events.iter().any(|e| matches!(
            e,
            Event::NodeCancelled { .. }
                | Event::NodeFailed { .. }
                | Event::NodeAttemptFailed { .. }
        )));
    }
    // Concurrency limits are per execution; the two runs must actually overlap.
    assert!(counts.peak.load(SeqCst) > CONCURRENCY);
    assert!(counts.peak.load(SeqCst) <= CONCURRENCY * 2);
    assert_eq!(
        counts.created.load(SeqCst),
        ROUNDS * (CONCURRENCY + DIAMONDS * 2)
    );
}
