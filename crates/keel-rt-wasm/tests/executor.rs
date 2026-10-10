use anyhow::{bail, Result};
use keel_rt::{
    Clock, ExecutionContext, ExecutionId, Executor, NodeId, NodeOutcome, ResumeToken, Runtime,
    Timestamp, WorkflowDefinition,
};
use keel_rt_wasm::{
    wasmtime::{
        component::{Instance, Linker},
        Store,
    },
    ComponentTask, HostState, Limits, WasmExecutor,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

struct TestClock;
#[async_trait::async_trait]
impl Clock for TestClock {
    fn now(&self) -> Timestamp {
        Timestamp::now_system()
    }
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

fn ctx() -> ExecutionContext {
    let execution_id = ExecutionId::new();
    let node_id = NodeId::new("wasm");
    ExecutionContext {
        resume_token: ResumeToken::issue(execution_id.clone(), node_id.clone(), 1),
        execution_id,
        node_id,
        attempt: 1,
        inputs: HashMap::new(),
        cancel: CancellationToken::new(),
        clock: Arc::new(TestClock),
    }
}

fn component(body: &str) -> String {
    format!(
        r#"(component
      (core module $m
        (global $counter (mut i32) (i32.const 0))
        (memory (export "memory") 1)
        (func (export "run") (result i32) {body}))
      (core instance $i (instantiate $m))
      (func (export "run") (result u32) (canon lift (core func $i "run"))))"#
    )
}

struct Scalar;
impl ComponentTask for Scalar {
    type State = ();
    fn create_state(&self, _: &ExecutionContext) -> Result<()> {
        Ok(())
    }
    async fn run(
        &self,
        store: &mut Store<HostState<()>>,
        instance: Instance,
        _: ExecutionContext,
    ) -> Result<NodeOutcome> {
        let run = instance.get_typed_func::<(), (u32,)>(&mut *store, "run")?;
        let (n,) = run.call_async(store, ()).await?;
        Ok(NodeOutcome::succeeded(n.to_le_bytes().to_vec()))
    }
}

fn failed(outcome: NodeOutcome, needle: &str) {
    match outcome {
        NodeOutcome::Failed(error) => assert!(error.message.contains(needle), "{error}"),
        other => panic!("expected failure containing {needle:?}, got {other:?}"),
    }
}

struct DropCount(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

struct SuspendedHost {
    entered: Arc<tokio::sync::Notify>,
    host_drops: Arc<std::sync::atomic::AtomicUsize>,
    state_drops: Arc<std::sync::atomic::AtomicUsize>,
}

impl ComponentTask for SuspendedHost {
    type State = DropCount;
    fn create_state(&self, _: &ExecutionContext) -> Result<Self::State> {
        Ok(DropCount(self.state_drops.clone()))
    }
    fn link(&self, linker: &mut Linker<HostState<Self::State>>) -> Result<()> {
        let entered = self.entered.clone();
        let host_drops = self.host_drops.clone();
        linker.root().func_wrap_async("answer", move |_, (): ()| {
            let entered = entered.clone();
            let host_drops = host_drops.clone();
            Box::new(async move {
                let _guard = DropCount(host_drops);
                entered.notify_one();
                std::future::pending::<keel_rt_wasm::wasmtime::Result<(u32,)>>().await
            })
        })?;
        Ok(())
    }
    async fn run(
        &self,
        store: &mut Store<HostState<Self::State>>,
        instance: Instance,
        _: ExecutionContext,
    ) -> Result<NodeOutcome> {
        let run = instance.get_typed_func::<(), (u32,)>(&mut *store, "run")?;
        let (n,) = run.call_async(store, ()).await?;
        Ok(NodeOutcome::succeeded(n.to_le_bytes().to_vec()))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_or_timing_out_a_suspended_host_import_drops_future_and_store() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let entered = Arc::new(tokio::sync::Notify::new());
    let host_drops = Arc::new(AtomicUsize::new(0));
    let state_drops = Arc::new(AtomicUsize::new(0));
    let exec = WasmExecutor::new_trusted(
        "x",
        IMPORT,
        SuspendedHost {
            entered: entered.clone(),
            host_drops: host_drops.clone(),
            state_drops: state_drops.clone(),
        },
        Limits {
            timeout: Duration::from_millis(50),
            ..Limits::default()
        },
    )
    .unwrap();
    let context = ctx();
    let cancel = context.cancel.clone();
    let (outcome, ()) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(exec.execute(context), async {
            entered.notified().await;
            cancel.cancel();
        })
    })
    .await
    .unwrap();
    failed(outcome, "cancelled");
    assert_eq!(host_drops.load(Ordering::SeqCst), 1);
    assert_eq!(state_drops.load(Ordering::SeqCst), 1);
    assert_eq!(exec.execute(ctx()).await, NodeOutcome::TimedOut);
    assert_eq!(host_drops.load(Ordering::SeqCst), 2);
    assert_eq!(state_drops.load(Ordering::SeqCst), 2);

    // A pre-cancelled context must not even construct user state or enter Wasm.
    let context = ctx();
    context.cancel.cancel();
    failed(exec.execute(context).await, "cancelled");
    assert_eq!(state_drops.load(Ordering::SeqCst), 2);
    assert_eq!(host_drops.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn resource_counts_and_table_growth_are_enforced() {
    let two_memories =
        r#"(component (core module $m (memory 1) (memory 1)) (core instance (instantiate $m)))"#;
    let two_tables = r#"(component (core module $m (table 1 funcref) (table 1 funcref)) (core instance (instantiate $m)))"#;
    let two_instances = r#"(component (core module $m) (core instance (instantiate $m)) (core instance (instantiate $m)))"#;
    for (wat, limits, error) in [
        (two_memories, Limits::default(), "memory"),
        (two_tables, Limits::default(), "table"),
        (
            two_instances,
            Limits {
                instances: 1,
                ..Limits::default()
            },
            "instance",
        ),
    ] {
        let exec = WasmExecutor::new_trusted("x", wat, Scalar, limits).unwrap();
        failed(exec.execute(ctx()).await, error);
    }
    let table_growth = r#"(component
      (core module $m
        (table 1 funcref)
        (func (export "run") (result i32) ref.null func i32.const 1 table.grow 0))
      (core instance $i (instantiate $m))
      (func (export "run") (result u32) (canon lift (core func $i "run"))))"#;
    let exec = WasmExecutor::new_trusted(
        "x",
        table_growth,
        Scalar,
        Limits {
            table_elements: 1,
            ..Limits::default()
        },
    )
    .unwrap();
    failed(exec.execute(ctx()).await, "grow");
}

#[tokio::test(flavor = "current_thread")]
async fn canonical_post_return_traps_are_not_reported_as_success() {
    let wat = r#"(component
      (core module $m
        (func (export "run") (result i32) i32.const 42)
        (func (export "cleanup") (param i32) unreachable))
      (core instance $i (instantiate $m))
      (func (export "run") (result u32)
        (canon lift (core func $i "run") (post-return (func $i "cleanup")))))"#;
    let exec = WasmExecutor::new_trusted("x", wat, Scalar, Limits::default()).unwrap();
    failed(exec.execute(ctx()).await, "unreachable");
}

#[test]
fn rejects_invalid_components_missing_imports_and_bad_configuration() {
    assert!(WasmExecutor::new_trusted("x", b"invalid", Scalar, Limits::default()).is_err());
    assert!(WasmExecutor::new_trusted("x", b"(module)", Scalar, Limits::default()).is_err());
    assert!(
        WasmExecutor::new_trusted("", component("i32.const 1"), Scalar, Limits::default()).is_err()
    );
    for limits in [
        Limits {
            fuel: 0,
            ..Limits::default()
        },
        Limits {
            yield_interval: 0,
            ..Limits::default()
        },
        Limits {
            timeout: Duration::ZERO,
            ..Limits::default()
        },
    ] {
        assert!(WasmExecutor::new_trusted("x", component("i32.const 1"), Scalar, limits).is_err());
    }
    // No ambient capabilities: an unlinked import fails at construction.
    assert!(WasmExecutor::new_trusted("x", IMPORT, Scalar, Limits::default()).is_err());
    assert!(
        WasmExecutor::from_trusted_file("x", "does-not-exist.wasm", Scalar, Limits::default())
            .is_err()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn each_invocation_has_fresh_guest_memory_and_can_run_concurrently() {
    let exec = WasmExecutor::new_trusted(
        "x",
        component(
            "global.get $counter i32.const 1 i32.add global.set $counter global.get $counter",
        ),
        Scalar,
        Limits::default(),
    )
    .unwrap();
    assert_eq!(exec.id().as_str(), "x");
    let (a, b) = tokio::join!(exec.execute(ctx()), exec.execute(ctx()));
    let expected = NodeOutcome::succeeded(1u32.to_le_bytes().to_vec());
    assert_eq!(a, expected);
    assert_eq!(b, expected);
}

#[tokio::test(flavor = "current_thread")]
async fn traps_and_export_type_errors_become_failures() {
    let exec = WasmExecutor::new_trusted("x", component("unreachable"), Scalar, Limits::default())
        .unwrap();
    failed(exec.execute(ctx()).await, "unreachable");
    let exec = WasmExecutor::new_trusted("x", b"(component)", Scalar, Limits::default()).unwrap();
    failed(exec.execute(ctx()).await, "export");
    let wrong = component("i32.const 1").replace("(result u32)", "(result s32)");
    let exec = WasmExecutor::new_trusted("x", wrong, Scalar, Limits::default()).unwrap();
    failed(exec.execute(ctx()).await, "type");
}

#[tokio::test(flavor = "current_thread")]
async fn fuel_stops_infinite_guest_and_memory_growth_is_limited() {
    let exec = WasmExecutor::new_trusted(
        "x",
        component("(loop $spin br $spin) i32.const 0"),
        Scalar,
        Limits {
            fuel: 1_000,
            yield_interval: 100,
            ..Limits::default()
        },
    )
    .unwrap();
    failed(exec.execute(ctx()).await, "fuel");
    let limits = Limits {
        memory_bytes: 65536,
        ..Limits::default()
    };
    let exec = WasmExecutor::new_trusted("x", component("i32.const 1 memory.grow"), Scalar, limits)
        .unwrap();
    failed(exec.execute(ctx()).await, "grow");
    let limits = Limits {
        memory_bytes: 1,
        ..Limits::default()
    };
    let exec = WasmExecutor::new_trusted("x", component("i32.const 1"), Scalar, limits).unwrap();
    failed(exec.execute(ctx()).await, "memory");
}

#[tokio::test(flavor = "current_thread")]
async fn busy_guest_yields_for_timeout_cancellation_and_siblings() {
    let limits = Limits {
        fuel: u64::MAX,
        yield_interval: 100,
        timeout: Duration::from_millis(20),
        ..Limits::default()
    };
    let exec = WasmExecutor::new_trusted(
        "x",
        component("(loop $spin br $spin) i32.const 0"),
        Scalar,
        limits,
    )
    .unwrap();
    assert_eq!(exec.execute(ctx()).await, NodeOutcome::TimedOut);
    let context = ctx();
    let cancel = context.cancel.clone();
    let (outcome, ()) = tokio::join!(exec.execute(context), async move {
        tokio::task::yield_now().await;
        cancel.cancel();
    });
    failed(outcome, "cancelled");

    let rt = Runtime::builder()
        .concurrency(2)
        .register(exec)
        .register_fn("ok", |_| async { NodeOutcome::succeeded(Vec::new()) })
        .build();
    let def = WorkflowDefinition::builder("busy")
        .node("busy", "x")
        .node("sibling", "ok")
        .build()
        .unwrap();
    let handle = rt.start(def).unwrap();
    assert_eq!(handle.wait_stable().await, keel_rt::ExecutionState::Failed);
    assert!(matches!(
        handle
            .inspect()
            .await
            .node(&NodeId::new("sibling"))
            .unwrap()
            .state,
        keel_rt::NodeState::Succeeded
    ));
}

const IMPORT: &str = r#"(component
  (import "answer" (func $answer (result u32)))
  (core func $answer (canon lower (func $answer)))
  (core module $m
    (import "host" "answer" (func $answer (result i32)))
    (func (export "run") (result i32) call $answer))
  (core instance $host (export "answer" (func $answer)))
  (core instance $i (instantiate $m (with "host" (instance $host))))
  (func (export "run") (result u32) (canon lift (core func $i "run"))))"#;

struct Host;
impl ComponentTask for Host {
    type State = u32;
    fn create_state(&self, ctx: &ExecutionContext) -> Result<u32> {
        Ok(ctx.attempt)
    }
    fn link(&self, linker: &mut Linker<HostState<u32>>) -> Result<()> {
        linker.root().func_wrap("answer", |mut store, (): ()| {
            store.data_mut().user += 40;
            Ok((store.data().user,))
        })?;
        Ok(())
    }
    async fn run(
        &self,
        store: &mut Store<HostState<u32>>,
        instance: Instance,
        _: ExecutionContext,
    ) -> Result<NodeOutcome> {
        let run = instance.get_typed_func::<(), (u32,)>(&mut *store, "run")?;
        let (n,) = run.call_async(store, ()).await?;
        Ok(NodeOutcome::succeeded(n.to_le_bytes().to_vec()))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn users_explicitly_link_capabilities_with_fresh_host_state() {
    let exec = WasmExecutor::new_trusted("x", IMPORT, Host, Limits::default()).unwrap();
    for _ in 0..2 {
        assert_eq!(
            exec.execute(ctx()).await,
            NodeOutcome::succeeded(41u32.to_le_bytes().to_vec())
        );
    }
}

struct OutcomeTask;
impl ComponentTask for OutcomeTask {
    type State = ();
    fn create_state(&self, _: &ExecutionContext) -> Result<()> {
        Ok(())
    }
    async fn run(
        &self,
        _: &mut Store<HostState<()>>,
        _: Instance,
        ctx: ExecutionContext,
    ) -> Result<NodeOutcome> {
        match ctx.attempt {
            1 => Ok(NodeOutcome::Waiting {
                token: ctx.resume_token,
            }),
            2 => Ok(NodeOutcome::TimedOut),
            3 => Ok(NodeOutcome::failed("application error")),
            _ => bail!("binding error"),
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn users_control_outcome_and_wait_token_mapping() {
    let exec =
        WasmExecutor::new_trusted("x", b"(component)", OutcomeTask, Limits::default()).unwrap();
    let context = ctx();
    let token = context.resume_token.clone();
    assert_eq!(exec.execute(context).await, NodeOutcome::Waiting { token });
    let mut context = ctx();
    context.attempt = 2;
    assert_eq!(exec.execute(context).await, NodeOutcome::TimedOut);
    let mut context = ctx();
    context.attempt = 3;
    failed(exec.execute(context).await, "application error");
    let mut context = ctx();
    context.attempt = 4;
    failed(exec.execute(context).await, "binding error");
}

struct Echo;
impl ComponentTask for Echo {
    type State = ();
    fn create_state(&self, _: &ExecutionContext) -> Result<()> {
        Ok(())
    }
    async fn run(
        &self,
        store: &mut Store<HostState<()>>,
        instance: Instance,
        ctx: ExecutionContext,
    ) -> Result<NodeOutcome> {
        let run = instance.get_typed_func::<(Vec<u8>,), (Vec<u8>,)>(&mut *store, "run")?;
        let input = ctx.inputs[&NodeId::new("source")].to_vec();
        let (output,) = run.call_async(store, (input,)).await?;
        Ok(NodeOutcome::succeeded(output))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn file_component_passes_binary_payload_through_a_real_dag() {
    let exec = WasmExecutor::from_trusted_file(
        "echo",
        concat!(env!("CARGO_MANIFEST_DIR"), "/examples/echo.wat"),
        Echo,
        Limits::default(),
    )
    .unwrap();
    let payload: Vec<u8> = (0..=255).cycle().take(4096).collect();
    let expected = payload.clone();
    let rt = Runtime::builder()
        .register(exec)
        .register_fn("source", move |_| {
            let bytes = payload.clone();
            async { NodeOutcome::succeeded(bytes) }
        })
        .register_fn("check", move |ctx| {
            let expected = expected.clone();
            async move {
                assert_eq!(ctx.inputs[&NodeId::new("echo")].as_ref(), expected);
                NodeOutcome::succeeded(Vec::new())
            }
        })
        .build();
    let def = WorkflowDefinition::builder("echo")
        .node("source", "source")
        .node("echo", "echo")
        .node("check", "check")
        .edge("source", "echo")
        .edge("echo", "check")
        .build()
        .unwrap();
    assert!(rt.run(def).await.unwrap().is_successful_finish());
}

#[tokio::test(flavor = "current_thread")]
async fn initialization_is_also_metered_and_cancellable() {
    let wat = r#"(component
      (core module $m (func $start (loop $spin br $spin)) (start $start))
      (core instance (instantiate $m)))"#;
    let exec = WasmExecutor::new_trusted(
        "x",
        wat,
        Scalar,
        Limits {
            fuel: 1_000,
            yield_interval: 100,
            ..Limits::default()
        },
    )
    .unwrap();
    failed(exec.execute(ctx()).await, "fuel");
    let exec = WasmExecutor::new_trusted(
        "x",
        wat,
        Scalar,
        Limits {
            fuel: u64::MAX,
            yield_interval: 100,
            timeout: Duration::from_millis(10),
            ..Limits::default()
        },
    )
    .unwrap();
    assert_eq!(exec.execute(ctx()).await, NodeOutcome::TimedOut);
    let context = ctx();
    let cancel = context.cancel.clone();
    let (outcome, ()) = tokio::join!(exec.execute(context), async move {
        tokio::task::yield_now().await;
        cancel.cancel();
    });
    failed(outcome, "cancelled");
}

struct Retry;
impl ComponentTask for Retry {
    type State = ();
    fn create_state(&self, _: &ExecutionContext) -> Result<()> {
        Ok(())
    }
    async fn run(
        &self,
        store: &mut Store<HostState<()>>,
        instance: Instance,
        ctx: ExecutionContext,
    ) -> Result<NodeOutcome> {
        let attempt = ctx.attempt;
        let outcome = Scalar.run(store, instance, ctx).await?;
        if attempt == 1 {
            Ok(NodeOutcome::failed("try again"))
        } else {
            Ok(outcome)
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn keel_policies_retry_and_snapshot_recovery_reinvokes_the_component() {
    use keel_rt::{ExecutionState, MemoryStore, Recover, RetryPolicy};
    let make_exec = || {
        WasmExecutor::new_trusted("x", component("i32.const 42"), Retry, Limits::default()).unwrap()
    };
    let def = || {
        WorkflowDefinition::builder("retry")
            .node("wasm", "x")
            .build()
            .unwrap()
    };
    let rt = Runtime::builder()
        .register(make_exec())
        .policy(RetryPolicy::new(2, Duration::ZERO))
        .build();
    let handle = rt.start(def()).unwrap();
    assert!(handle.wait_stable().await.is_successful_finish());
    assert_eq!(
        handle
            .inspect()
            .await
            .node(&NodeId::new("wasm"))
            .unwrap()
            .attempt,
        2
    );
    handle.wait().await;

    let store: Arc<dyn keel_rt::StateStore> = Arc::new(MemoryStore::new());
    let rt = Runtime::builder()
        .store(store.clone())
        .register(make_exec())
        .build();
    let handle = rt.start(def()).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(handle.wait().await, ExecutionState::Failed);
    drop(rt);
    let rt = Runtime::builder()
        .store(store)
        // The application repairs its outcome mapping before retrying the failure.
        .register(
            WasmExecutor::new_trusted("x", component("i32.const 42"), Scalar, Limits::default())
                .unwrap(),
        )
        .build();
    let handle = rt.resume_with(&id, Recover::RetryFailed).await.unwrap();
    let state = handle.wait_stable().await;
    let snapshot = handle.inspect().await;
    assert!(state.is_successful_finish(), "{snapshot:?}");
    let node = snapshot.node(&NodeId::new("wasm")).unwrap();
    // Explicit RetryFailed resets Keel's retry budget, unlike a policy retry.
    assert_eq!(node.attempt, 1);
    assert_eq!(node.output.as_ref().unwrap().as_ref(), 42u32.to_le_bytes());
    handle.wait().await;
}

#[tokio::test(flavor = "current_thread")]
async fn waiting_completes_through_keel_and_runtime_cancels_busy_wasm() {
    use keel_rt::{ExecutionState, Resume};
    let exec =
        WasmExecutor::new_trusted("x", b"(component)", OutcomeTask, Limits::default()).unwrap();
    let def = || {
        WorkflowDefinition::builder("gate")
            .node("wasm", "x")
            .build()
            .unwrap()
    };
    let rt = Runtime::builder().register(exec).build();
    let handle = rt.start(def()).unwrap();
    assert_eq!(handle.wait_stable().await, ExecutionState::Waiting);
    let snapshot = handle.inspect().await;
    let token = snapshot
        .node(&NodeId::new("wasm"))
        .unwrap()
        .resume_token
        .clone()
        .unwrap();
    handle
        .resume(token, Resume::Complete(NodeOutcome::succeeded(Vec::new())))
        .await
        .unwrap();
    assert!(handle.wait().await.is_successful_finish());

    let exec = WasmExecutor::new_trusted(
        "x",
        component("(loop $spin br $spin) i32.const 0"),
        Scalar,
        Limits {
            fuel: u64::MAX,
            yield_interval: 100,
            ..Limits::default()
        },
    )
    .unwrap();
    let rt = Runtime::builder().register(exec).build();
    let handle = rt.start(def()).unwrap();
    // Inspect is handled after launch; cancellation must interrupt an active guest.
    assert_eq!(handle.inspect().await.state, ExecutionState::Running);
    tokio::time::timeout(Duration::from_secs(1), async {
        handle.cancel().await;
        assert_eq!(handle.wait().await, ExecutionState::Cancelled);
    })
    .await
    .unwrap();
}

#[test]
fn component_bytes_and_files_are_bounded_before_compilation() {
    use keel_rt_wasm::MAX_COMPONENT_BYTES;
    let oversized = vec![b'x'; MAX_COMPONENT_BYTES + 1];
    let error = WasmExecutor::new_trusted("x", &oversized, Scalar, Limits::default())
        .err()
        .unwrap();
    assert!(error.to_string().contains("byte limit"));
    let path = std::env::temp_dir().join(format!("keel-wasm-size-{}", std::process::id()));
    let file = std::fs::File::create(&path).unwrap();
    file.set_len(MAX_COMPONENT_BYTES as u64 + 1).unwrap();
    let error = WasmExecutor::from_trusted_file("x", &path, Scalar, Limits::default())
        .err()
        .unwrap();
    std::fs::remove_file(&path).unwrap();
    assert!(error.to_string().contains("byte limit"));
    assert!(
        WasmExecutor::from_trusted_file("x", std::env::temp_dir(), Scalar, Limits::default())
            .is_err()
    );
}
