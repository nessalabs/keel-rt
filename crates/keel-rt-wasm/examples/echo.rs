use anyhow::Result;
use keel_rt::{ExecutionContext, NodeId, NodeOutcome, Runtime, WorkflowDefinition};
use keel_rt_wasm::{
    wasmtime::{component::Instance, Store},
    ComponentTask, HostState, Limits, WasmExecutor,
};

mod bindings {
    keel_rt_wasm::wasmtime::component::bindgen!({
        path: "examples/echo.wit",
        world: "echo",
        wasmtime_crate: keel_rt_wasm::wasmtime,
        exports: { default: async },
    });
}

struct Echo;

impl ComponentTask for Echo {
    type State = ();

    fn create_state(&self, _ctx: &ExecutionContext) -> Result<()> {
        Ok(())
    }

    async fn run(
        &self,
        store: &mut Store<HostState<()>>,
        instance: Instance,
        ctx: ExecutionContext,
    ) -> Result<NodeOutcome> {
        // Bindings are generated from the application's WIT, not a Keel schema.
        let guest = bindings::Echo::new(&mut *store, &instance)?;
        let input = ctx.inputs[&NodeId::new("source")].to_vec();
        let output = guest.call_run(store, &input).await?;
        Ok(NodeOutcome::succeeded(output))
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let wasm = WasmExecutor::new("echo", include_bytes!("echo.wat"), Echo, Limits::default())?;
    // To load an application-selected component:
    // let wasm = WasmExecutor::from_file("echo", "my-component.wasm", Echo, Limits::default())?;
    let rt = Runtime::builder()
        .register_fn("source", |_| async {
            NodeOutcome::succeeded(b"hello from Keel".to_vec())
        })
        .register(wasm)
        .register_fn("check", |ctx| async move {
            assert_eq!(
                ctx.inputs[&NodeId::new("echo")].as_ref(),
                b"hello from Keel"
            );
            NodeOutcome::succeeded(ctx.inputs[&NodeId::new("echo")].clone())
        })
        .build();
    let def = WorkflowDefinition::builder("wasm-echo")
        .node("source", "source")
        .node("echo", "echo")
        .node("check", "check")
        .edge("source", "echo")
        .edge("echo", "check")
        .build()?;
    let state = rt.run(def).await?;
    assert!(state.is_successful_finish());
    println!("Wasm component returned: hello from Keel");
    Ok(())
}
