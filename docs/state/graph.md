---
id: "graph"
title: "Define dependencies"
kind: "statechart"
status: "implemented"
summary: "Default joins require every predecessor to succeed; concurrency limits
  running attempts, not waiting gates."
parent: "keel"
sources:
  [
    "src/domain/definition.rs",
    "src/domain/state/apply.rs",
    "src/runtime/scheduler.rs",
    "examples/research_diamond.rs"
  ]
sourceRevision: "dbb8eceb94ff666824e6365b40171a78f71199cb"
diagramLinks: {}
model: { "spec": "tla/DependencyJoin.tla", "config": "tla/DependencyJoin.cfg" }
diagramDetails:
  [
    { "diagram": 0, "node": "Pending", "section": "Joins and input data" },
    {
        "diagram": 0,
        "node": "Waiting",
        "section": "Running and waiting use different capacity"
      },
    {
        "diagram": 0,
        "node": "Running",
        "section": "Running and waiting use different capacity"
      }
  ]
modelCheck:
  sourceRevision: "dbb8eceb94ff666824e6365b40171a78f71199cb"
  checker: tlc-1.7.4
  date: 2026-11-03T08:57:11.543Z
  config: tla/DependencyJoin.cfg
  modelDigest: 5edc649e3a4e94e81eec55186c5d305a30db5f4c1e9c1e917e1d76c665816bbd
  states: 41
  held:
    - JoinBeforeStart
    - PermitBound
  broke: []
  unchecked: []
---
# Define a graph and follow its work

For example, fetch A and B in parallel, then combine both outputs. The combine
node remains Pending until both predecessors succeed. Finishing A alone cannot
start it. The [research diamond example](../../examples/research_diamond.rs)
shows a complete public-API workflow.

Build a `WorkflowDefinition`, register each executor before building the Runtime,
and call `run` or `start`. The definition rejects empty graphs/IDs, duplicate node
IDs, unknown edge endpoints and cycles. Missing executor registrations are rejected
at start before any node runs. The builtin `wait` executor is already registered.

```mermaid
stateDiagram-v2
    [*] --> Pending
    Pending --> Ready: Join conditions satisfied
    Ready --> Running: Due now and execution has a free permit
    Running --> Succeeded: Policy accepts successful output
    Running --> Waiting: Executor yields a gate token
    Waiting --> Running: Complete supplies an outcome for this wait
    Waiting --> Ready: Reinvoke requested
    Running --> Ready: Policy retries failed attempt
    Running --> Failed: Policy accepts or rejects failure
    Running --> TimedOut: Policy accepts timeout
    Pending --> Cancelled: Failure policy or explicit cancellation
    Ready --> Cancelled: Failure policy or explicit cancellation
    Waiting --> Cancelled: Failure policy or explicit cancellation
```

## Joins and input data

The default `AllSucceeded` join needs every predecessor to succeed. `AllDone` is
an explicit per-node alternative: every predecessor must be terminal, including
Failed, TimedOut or Cancelled. Configure it on the definition, not the Runtime.
Inputs contain only successful predecessors' opaque byte outputs keyed by NodeId;
AllDone does not manufacture bytes for failed inputs. The application interprets
them and supplies any data validation.

## Running and waiting use different capacity

A FIFO ready queue dispatches while per-execution permits are available. Running
attempts hold permits; a node yielding Waiting releases its permit. Many waiting
gates can therefore coexist without exhausting execution capacity. The configured
concurrency is not a limit across every Runtime execution or cron fire. The apply
inbox is unbounded; application admission still matters under sustained producers.

Snapshot iteration follows definition order. A raw lookup is by NodeId, not an
internal graph index. `running_count` and `waiting_count` distinguish work using
permits from gates awaiting external input.

## Model boundary

`DependencyJoin` models two predecessor outcomes, one dependent and a selected
join policy. It checks the dependent cannot start before the corresponding join
condition and cannot exceed a one-permit capacity with an independent running
peer. Controls weaken join readiness and permit admission. It does not model
arbitrary graphs, FIFO fairness, queue deduplication or every cancellation route.
The chart is a node overview; retry timing, completion authority and cancellation
have their own pages.
