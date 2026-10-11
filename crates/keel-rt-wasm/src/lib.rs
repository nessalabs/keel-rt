//! Optional Wasmtime adapter. Users own the component interface and host capabilities.
//!
//! Each invocation gets fresh guest memory and host state. Keel continues to own
//! retries, waiting, snapshots, and recovery. See the runnable `echo` example.

use anyhow::{ensure, Context, Result};
use keel_rt::{ExecutionContext, Executor, ExecutorId, NodeOutcome};
use std::{future::Future, io::Read, path::Path, pin::Pin, time::Duration};
use wasmtime::component::{Component, Instance, InstancePre, Linker};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder};

// Consumers can use exactly the version used by this adapter for their bindings.
pub use wasmtime;

/// Maximum source size accepted before parsing or compilation (binary or WAT).
pub const MAX_COMPONENT_BYTES: usize = 1024 * 1024;

/// Limits apply to each invocation, including component initialization.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Total guest instruction budget (fuel units are not CPU cycles).
    pub fuel: u64,
    /// Yield frequency so busy guests allow cancellation and other Keel work.
    pub yield_interval: u64,
    /// Per-memory byte limit. Total guest linear memory is bounded by this * memories.
    pub memory_bytes: usize,
    pub memories: usize,
    pub table_elements: usize,
    pub tables: usize,
    pub instances: usize,
    /// Uses the invocation's Keel Clock, covering initialization and user run().
    pub timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            fuel: 10_000_000,
            yield_interval: 10_000,
            memory_bytes: 64 * 1024 * 1024,
            memories: 1,
            table_elements: 10_000,
            tables: 1,
            instances: 16,
            timeout: Duration::from_secs(30),
        }
    }
}

/// Store data exposed to user-defined host imports. Resource limits stay private.
pub struct HostState<T> {
    pub user: T,
    limits: StoreLimits,
}

/// Application-owned bindings: any WIT world, imports, and NodeOutcome mapping.
///
/// Host hooks are trusted Rust code: use asynchronous I/O and do not block the
/// executor thread. Fuel and memory limits constrain Wasm, not these hooks.
pub trait ComponentTask: Send + Sync + 'static {
    type State: Send + 'static;

    /// Called once during adapter construction. No WASI or other imports are
    /// supplied automatically; explicitly link the capabilities the guest needs.
    fn link(&self, _linker: &mut Linker<HostState<Self::State>>) -> Result<()> {
        Ok(())
    }

    /// New host state for every attempt; never shared implicitly across nodes.
    fn create_state(&self, ctx: &ExecutionContext) -> Result<Self::State>;

    /// Invoke user-defined exports with typed functions or generated bindings.
    /// Wasmtime 48 performs canonical post-return cleanup automatically.
    /// Return Waiting with ctx.resume_token when the application wants a gate.
    fn run(
        &self,
        store: &mut Store<HostState<Self::State>>,
        instance: Instance,
        ctx: ExecutionContext,
    ) -> impl Future<Output = Result<NodeOutcome>> + Send;
}

/// Compiled component and resolved imports, reusable across workflow invocations.
pub struct WasmExecutor<T: ComponentTask> {
    id: ExecutorId,
    engine: Engine,
    pre: InstancePre<HostState<T::State>>,
    task: T,
    limits: Limits,
}

impl<T: ComponentTask> WasmExecutor<T> {
    /// Compile a trusted, pre-vetted component binary or WAT and resolve imports.
    ///
    /// Only use for application-owned components. Compilation is synchronous and
    /// invocation limits do not constrain compiler CPU or host memory. Never pass
    /// tenant uploads or other less-trusted input; those require compilation in a
    /// separate process with OS memory/CPU limits and a wall-clock deadline.
    /// Source bytes are capped by [`MAX_COMPONENT_BYTES`] before compilation.
    pub fn new_trusted(
        id: impl Into<ExecutorId>,
        component: impl AsRef<[u8]>,
        task: T,
        limits: Limits,
    ) -> Result<Self> {
        let component = component.as_ref();
        ensure!(
            component.len() <= MAX_COMPONENT_BYTES,
            "component exceeds byte limit"
        );
        let id = id.into();
        ensure!(!id.as_str().is_empty(), "executor id must not be empty");
        ensure!(limits.fuel > 0, "fuel must be positive");
        ensure!(limits.yield_interval > 0, "yield interval must be positive");
        ensure!(!limits.timeout.is_zero(), "timeout must be positive");
        let mut config = Config::new();
        config.wasm_component_model(true).consume_fuel(true);
        let engine = Engine::new(&config)?;
        let component =
            Component::new(&engine, component).map_err(|e| e.context("compile Wasm component"))?;
        let mut linker = Linker::new(&engine);
        task.link(&mut linker)
            .context("link component host capabilities")?;
        let pre = linker
            .instantiate_pre(&component)
            .map_err(|e| e.context("resolve component imports"))?;
        Ok(Self {
            id,
            engine,
            pre,
            task,
            limits,
        })
    }

    /// Load a trusted application-owned regular file, bounded before compilation.
    /// The same trust requirement as [`Self::new_trusted`] applies to its contents.
    /// Paths alone do not establish trust. Never deserialize native compiled code.
    pub fn from_trusted_file(
        id: impl Into<ExecutorId>,
        path: impl AsRef<Path>,
        task: T,
        limits: Limits,
    ) -> Result<Self> {
        let file = std::fs::File::open(path.as_ref())
            .with_context(|| format!("open component {}", path.as_ref().display()))?;
        ensure!(
            file.metadata()?.is_file(),
            "component must be a regular file"
        );
        ensure!(
            file.metadata()?.len() <= MAX_COMPONENT_BYTES as u64,
            "component exceeds byte limit"
        );
        // Read through a bound even if the file grows after metadata was checked.
        let mut bytes = Vec::new();
        file.take(MAX_COMPONENT_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .with_context(|| format!("read component {}", path.as_ref().display()))?;
        Self::new_trusted(id, bytes, task, limits)
    }

    async fn invoke(&self, ctx: ExecutionContext) -> Result<NodeOutcome> {
        let user = self
            .task
            .create_state(&ctx)
            .context("create component host state")?;
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.limits.memory_bytes)
            .memories(self.limits.memories)
            .table_elements(self.limits.table_elements)
            .tables(self.limits.tables)
            .instances(self.limits.instances)
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(&self.engine, HostState { user, limits });
        store.limiter(|state| &mut state.limits);
        store.set_fuel(self.limits.fuel)?;
        store.fuel_async_yield_interval(Some(self.limits.yield_interval))?;
        let instance = self
            .pre
            .instantiate_async(&mut store)
            .await
            .map_err(|e| e.context("instantiate Wasm component"))?;
        self.task.run(&mut store, instance, ctx).await
    }
}

impl<T: ComponentTask> Executor for WasmExecutor<T> {
    fn id(&self) -> ExecutorId {
        self.id.clone()
    }

    fn execute<'a>(
        &'a self,
        ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        Box::pin(async move {
            let cancel = ctx.cancel.clone();
            let clock = ctx.clock.clone();
            tokio::select! {
                biased;
                // Dropping invoke drops its Store; no background worker continues.
                // Keel owns the Cancelled state; never manufacture a guest success.
                _ = cancel.cancelled() => NodeOutcome::failed("Wasm invocation cancelled"),
                _ = clock.sleep(self.limits.timeout) => NodeOutcome::TimedOut,
                result = self.invoke(ctx) => match result {
                    Ok(outcome) => outcome,
                    Err(error) => NodeOutcome::failed(format!("Wasm executor {}: {error:#}", self.id.as_str())),
                },
            }
        })
    }
}
