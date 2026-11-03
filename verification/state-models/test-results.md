# Current implementation test evidence

Source: `dbb8eceb94ff666824e6365b40171a78f71199cb`. Date: 2026-11-03.
Host: macOS arm64. Isolated official Rust 1.99.1 toolchain; repository minimum is 1.95.

- `cargo test -- --test-threads=1`: **498 passed, zero failed, one preexisting ignored**. Includes kernel unit, adversarial, consumer, graph, durable startup, lease, resume, scenario, timer, resilience, workload and stress suites, including the debug 100k-node pack. [Full log](keel-kernel-tests.log).
- `cargo test -p keel-rt-sqlite -p keel-rt-http -p keel-rt-schedule -p keel-rt-wasm -p keel-rt-wasm-consumer-test -- --test-threads=1`: **402 passed, zero failed, zero ignored** across sibling unit/integration/doc groups. Includes SQLite startup/recovery/chaos, local HTTP tests, schedule tests and Wasm/consumer stress. [Full log](keel-adapter-tests.log).

The kernel's existing `siblings_survive_timeout_same_dag` test remains marked
ignored with reason `requires failure scopes`; no test was newly skipped or
weakened by this documentation change. The explicit scenario failure-scope suite
ran as part of the default tests. Counts aggregate the test binaries; zero-test
doc groups do not add passes.

These are local tests and synthetic workloads, not a live production deployment,
real machine power-loss experiment, or benchmark comparison against the historical
release medians. Coverage instrumentation, release performance baselines and Clippy
were not rerun for these documentation/script additions. The kernel coverage floor
and empty allowlist are unchanged.
