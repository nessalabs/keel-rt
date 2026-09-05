# keel-rt-wasm

An optional Wasmtime Component Model executor for Keel. Applications choose the
component, WIT world, exported functions, host capabilities, and how results map
to `NodeOutcome`. There is no mandatory Keel WIT schema.

Implement `ComponentTask`, load the component, and register the resulting
`WasmExecutor` like any other executor:

```rust,ignore
let wasm = WasmExecutor::from_file("transform", "user-component.wasm", MyBindings, Limits::default())?;
let rt = Runtime::builder().register(wasm).build();
let state = rt.run(definition).await?;
```

`MyBindings` implements three hooks:

- `create_state`: construct fresh application host state for each attempt.
- `link` (optional): explicitly provide the imports this component is allowed
  to call, using `Linker<HostState<State>>`. Host functions access application
  state through `store.data().user`.
- `run`: select exports using Wasmtime typed functions or generated bindings,
  map `ExecutionContext` to the application's arguments, and return a
  `NodeOutcome`. The context includes predecessor bytes, execution/node IDs,
  attempt, cancellation, clock, and a valid waiting token.

Wasmtime is re-exported as `keel_rt_wasm::wasmtime`, so application bindings can
use the adapter's version. The `run` hook returns `anyhow::Result<NodeOutcome>`.
Implement it with `async fn`, as in the example.

When generating bindings through the re-export, set
`wasmtime_crate: keel_rt_wasm::wasmtime` inside `component::bindgen!`, as the
example does. Otherwise generated code expects a separate direct `wasmtime`
dependency in your application.

## Run the complete example

Requires Rust 1.95 or later. From the repository root:

```sh
cargo run --locked -p keel-rt-wasm --example echo
```

[`examples/echo.rs`](examples/echo.rs) runs a native source → Wasm echo → native
check workflow and verifies that predecessor bytes survive the component call.
[`examples/echo.wit`](examples/echo.wit) defines that application's interface.
[`examples/echo.wat`](examples/echo.wat) is a runnable implementation, so the
example needs no extra guest compiler. A compiled `.wasm` component implementing
the same world can replace it with `from_file`.

For your own component, compile your language's implementation into a Component
Model binary exporting your chosen WIT world. A core Wasm module alone is not a
component. Update `run` to use those exports and arguments. WIT generated Rust
host bindings are also supported through the re-exported Wasmtime APIs.

## Execution behavior

Compilation and import resolution happen once, synchronously, during adapter
construction. Missing imports and invalid component bytes fail there. Export
selection and signature checks belong to the application's `run` hook; errors
there become failed node outcomes.

Each invocation instantiates a fresh store, guest memory, and host state. This
also applies to retries and resumed executions. Component memory is not saved
in Keel snapshots. Applications persist anything needed between attempts in
explicit external storage or workflow data. Keel's snapshot recovery remains
at-least-once; external effects still need application idempotency.

Default limits per invocation:

| Resource | Default |
| --- | --- |
| Fuel | 10,000,000 units |
| Cooperative yield interval | 10,000 fuel units |
| Linear memory | 64 MiB per memory, at most 1 memory |
| Tables | 1 table, at most 10,000 elements |
| Core instances | 16 |
| Timeout | 30 seconds, using `ExecutionContext.clock` |

Set fields on `Limits` to choose application budgets. Initialization, including
guest start functions, shares the invocation's fuel, timeout, and resource
limits. Wasm execution yields periodically so a busy guest permits other Keel
work, timeouts, and cancellation to run. Timeout maps to `TimedOut`; fuel
exhaustion, guest traps, limit violations, and binding errors map to `Failed`.
Keel policies can retry those outcomes.

Cancellation drops the invocation future and its store; the adapter creates no
background worker. Keel owns the cancelled execution state. If called directly
through `Executor::execute`, cancellation returns a failed outcome. Host I/O
already issued can still have effects.

No WASI, network, filesystem, environment, or other imports are linked by
default. Applications may add imports in `link`; WASI requires the application's
own matching Wasmtime WASI integration and explicit capability configuration.
User-provided host hooks are trusted Rust code: use asynchronous I/O and do not
block the executor thread. Wasm fuel and memory limits do not constrain host
allocations, component compilation, or blocking host functions. Applications
should bound payloads and any host-side work they expose.

Return `Waiting { token: ctx.resume_token }` to park a node and use Keel's normal
completion APIs. Return `Failed`, `TimedOut`, or `Succeeded` to apply the
application's outcome semantics. The adapter does not reinterpret guest values.

## Development

All crates share the Rust 1.95+ workspace and root lockfile. Root
`cargo test --workspace` includes this adapter and its consumer regression
fixture. The kernel does not depend on Wasmtime. To check only the adapter:

```sh
just wasm
# or
cargo test --locked -p keel-rt-wasm -p keel-rt-wasm-consumer-test -- --test-threads=1
cargo clippy --locked -p keel-rt-wasm -p keel-rt-wasm-consumer-test --all-targets -- -D warnings
```

`tests/stress.rs` is included in the normal adapter test run. It checks 512
native/Wasm diamonds (2,048 nodes, 1,024 successful guest invocations, plus 148
trap retries) against persisted outputs, attempts, events, concurrency limits,
and store cleanup. A second test runs eight rounds of overlapping healthy and
infinite-loop workflows through the same executor, verifying progress,
cancellation isolation, and cleanup of every instantiated guest store.
