---
id: "inventory"
title: "Find the source owner"
kind: "guide"
status: "reference"
summary: "Map kernel and sibling capabilities without turning examples into product features."
parent: "keel"
sources: ["src/lib.rs", "Cargo.toml", "docs/ARCHITECTURE.md"]
sourceRevision: "dbb8eceb94ff666824e6365b40171a78f71199cb"
diagramLinks: {}
---

# Feature and ownership inventory

| Capability | Owner | Explanation and verification disposition |
| --- | --- | --- |
| Graph validation, definitions, joins and bytes | src/domain/definition.rs, state/apply.rs | [Dependencies](graph.md); targeted join/permit model |
| Executor registry, closure executors and context | src/runtime/executor.rs, runtime.rs | [Startup](startup.md), [dependencies](graph.md); public Rust example/tests |
| Ordinary start/run and durable initialization | src/runtime/runtime.rs, store.rs | [Startup](startup.md); direct bounded model |
| Ready queue, permits and dispatch | src/runtime/scheduler.rs, spawn.rs | [Dependencies](graph.md), [leases](leases.md); selected guards only |
| Builtin wait, token complete/reinvoke | src/runtime/wait.rs, domain/state/apply.rs | [Waiting](waiting.md); direct acceptance model |
| Policy, retry time and failure propagation | src/domain/policy.rs, state/apply.rs | [Failure policy](failure-policy.md); direct retry-time model, graph transformation source review |
| Handle lifetime, cancel bound, inspection | src/runtime/handle.rs, runtime.rs | [Cancellation](cancellation.md); source-reviewed ownership |
| Snapshot resume and RetryFailed | src/domain/state/restore.rs, state/apply.rs | [Recovery](recovery.md); implementation suites, no whole-recovery proof |
| Store, sink and no-op/dirty persistence | src/runtime/store.rs, scheduler.rs, sink.rs | [Persistence](persistence.md); direct persist/emit model |
| Owner claims, epochs and stale operations | src/runtime/store.rs, scheduler.rs | [Leases](leases.md); direct acceptance model |
| SQLite transactions, durability and startup | crates/keel-rt-sqlite | [Persistence](persistence.md), [startup](startup.md); source/test adapter evidence |
| HTTP start/inspect/complete/cancel and KeelClient | crates/keel-rt-http | [HTTP](http.md); standalone adapter, not kernel HTTP types |
| Cron/timezone and tick ownership | crates/keel-rt-schedule | [Schedules](schedule.md); source-reviewed/tested sibling |
| Wasm Component executor and consumer fixture | crates/keel-rt-wasm | [Wasm](wasm.md); optional adapter and explicit synthetic consumer tests |
| Scripted faults, FakeClock and harness | src/testing, tests | [Verification](verification.md); test utilities, not production clocks |
| Stress, workload, resilience, chaos, coverage | tests, scripts, benches | [Verification](verification.md); preserve existing gates and historical baselines |

Kernel dependencies point from runtime to domain; test utilities depend on both.
SQLite, HTTP, cron and Wasm depend on the kernel, not the reverse. The repository
has no product Agent, typed workflow I/O schema, runtime-global FailSubtree switch,
YAML loader or built-in UI. Existing architecture diagrams and ADRs remain the
canonical architectural baseline; this map adds reader journeys and scoped models.
