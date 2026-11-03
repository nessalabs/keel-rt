---
id: "startup"
title: "Start and acknowledge a run"
kind: "statechart"
status: "implemented"
summary: "Durable startup commits recoverable state before returning a handle;
  ordinary startup has different acceptance semantics."
parent: "keel"
sources:
  [
    "src/runtime/runtime.rs",
    "src/runtime/store.rs",
    "crates/keel-rt-sqlite/src/startup_tests.rs",
    "tests/durable_start.rs",
    "docs/adr/0005-durable-start.md"
  ]
sourceRevision: "dbb8eceb94ff666824e6365b40171a78f71199cb"
diagramLinks: {}
model: { "spec": "tla/DurableStart.tla", "config": "tla/DurableStart.cfg" }
diagramDetails:
  [
    {
        "diagram": 0,
        "node": "Recoverable",
        "section": "Durable acceptance and uncertain acknowledgement"
      },
    {
        "diagram": 0,
        "node": "Acknowledged",
        "section": "Keep or consume the handle"
      },
    {
        "diagram": 0,
        "node": "Committed",
        "section": "Durable acceptance and uncertain acknowledgement"
      }
  ]
modelCheck:
  sourceRevision: "dbb8eceb94ff666824e6365b40171a78f71199cb"
  checker: tlc-1.7.4
  date: 2026-11-03T08:57:21.732Z
  config: tla/DurableStart.cfg
  modelDigest: f3493098367131c3a6a05248d84a395b1183cb5c141f5b75dc26c79b80b0640f
  states: 8
  held:
    - AcknowledgeAfterCommit
    - LaunchAfterCommit
  broke: []
  unchecked: []
---
# Decide what a successful start must promise

An application submitting a payment workflow may need to know that the execution
can be found after a process crash. Ordinary `Runtime::start` returns a live
handle without promising that initial state reached durable storage. Use
`start_durable(definition).await` when that initial recovery promise is required,
with a store that explicitly supports durable initialization.

```mermaid
stateDiagram-v2
    [*] --> Validating
    Validating --> Refused: Missing executor or unsupported durable initializer
    Validating --> Reserving: Reserve new execution ID locally
    Reserving --> Committed: Store atomically saves Created nodes definition and lease
    Reserving --> Refused: Initial transaction fails before commit
    Committed --> Acknowledged: Return live handle
    Committed --> Recoverable: Caller loses acknowledgement or is cancelled
    Acknowledged --> Dispatching: Reclaim and reload committed state before work
```

## Durable acceptance and uncertain acknowledgement

SQLite file-backed initialization writes the new lease, Created snapshot, all
nodes and workflow definition in one transaction. MemoryStore and NoopStore do
not provide this initializer; temporary/in-memory SQLite databases cannot
acknowledge durable initialization. Existing IDs must never be overwritten.
No executor dispatch occurs before the initializer succeeds.

Cancellation or a lost reply after commit can leave recoverable state even when
the caller did not receive a handle. Retrying startup always creates a new
ExecutionId, not an idempotent submission. Keep stable business operation IDs and
original submission inputs outside Keel when duplicate effects matter. If the
original execution ID is known, inspect/recover it rather than assuming another
start is the same operation. If it is unknown, Keel supplies no automatic
business-request lookup/deduplication procedure.

A delayed starter reclaims and reloads committed state before dispatch. Another
owner may already have advanced or cancelled that snapshot. A durable
acknowledgement is only the initial acceptance boundary: later storage errors
and external side effects still follow the normal rules in [persistence](persistence.md).

## Keep or consume the handle

`start`/`start_durable` return a handle whose drop requests cancellation. Keep it
while inspecting, waiting at a gate or completing tokens. `Runtime::run` is the
simple start-plus-terminal-wait path. `wait()` consumes a handle; `wait_stable()`
returns at Waiting or terminal while retaining it. See [cancellation](cancellation.md)
for cancellation of these operations rather than assuming all dropped futures
have identical behavior.

## Model boundary

`DurableStart` checks one new execution: acknowledgement and dispatch require a
committed initial snapshot. It independently represents commit, acknowledgement,
launch and a caller lost before acknowledgement. Controls bypass each commit
guard. Ownership generations are checked by the separate [lease model](leases.md). Filesystem durability, lost-ID discovery, adapter
atomicity and later persistence failures remain outside this model.
