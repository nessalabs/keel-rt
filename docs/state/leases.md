---
id: "leases"
title: "Reject a stale owner"
kind: "statechart"
status: "implemented"
summary: "Stale epochs are refused at store write and launch-ownership checks; remote effects are not rolled back."
parent: "keel"
sources:
  [
    "src/runtime/scheduler.rs",
    "src/runtime/store.rs",
    "crates/keel-rt-sqlite/src/lib.rs",
    "tests/lease_security.rs"
  ]
sourceRevision: "dbb8eceb94ff666824e6365b40171a78f71199cb"
diagramLinks: {}
model: { "spec": "tla/LeaseFence.tla", "config": "tla/LeaseFence.cfg" }
diagramDetails:
  [
    { "diagram": 0, "node": "Superseded", "section": "Which checks matter" },
    { "diagram": 0, "node": "Launched", "section": "A successful check is a point-in-time observation" }
  ]
modelCheck:
  sourceRevision: "dbb8eceb94ff666824e6365b40171a78f71199cb"
  checker: tlc-1.7.4
  date: 2026-11-03T08:57:14.959Z
  config: tla/LeaseFence.cfg
  modelDigest: 5e2d3c2415cdb422def5ad106311384ddf61a17ca3ec7626bcb4aa7be91b5560
  states: 13
  held:
    - CurrentWriteOnly
    - CurrentLaunchCheckOnly
  broke: []
  unchecked: []
---
# Refuse work from an owner that lost its claim

Two processes can try to recover the same execution. The store lease chooses
which one may write and launch new work; it does not make old external effects
vanish. For example, an old network request may already be in flight when a newer
runtime takes over. Application idempotency is still needed.

```mermaid
stateDiagram-v2
    [*] --> Claimed
    Claimed --> Current: Store accepts owner epoch
    Current --> Rechecked: Persist then validate launch ownership
    Rechecked --> Launched: Invoke after successful ownership check
    Current --> Superseded: Another generation takes over
    Superseded --> Refused: Old write or launch check is rejected
```

## A successful check is a point-in-time observation

The scheduler awaits a heartbeat, then invokes application code in a separate
step. A long pause or lease expiry/takeover between that check and invocation is
not an atomic store-and-launch transaction. This atlas does not promise that no
old invocation can begin after such a takeover. Nor can it retract external work
already issued. Use application idempotency or external-resource fencing when
that stronger guarantee matters.

## Which checks matter

The scheduler stamps its current epoch onto the live execution for persistence.
After persistence returns, it refreshes the lease before launching executor work.
If a heartbeat fails and re-claim produces a different epoch, it releases that
new claim and stops rather than treating it as permission to continue old work.
Completion and cancellation injection also claim-or-forget their live inbox.
Forgetting an old inbox compares channel identity so it cannot unregister a new
drive under the same execution ID.

The adapter's ownership contract and real clock determine takeover eligibility.
An observed ClaimedElsewhere is a refusal to inject locally; route operations to
the live owner or wait for a legitimate ownership change. Do not force another
`start` and assume it addresses the same execution.

## Model boundary

`LeaseFence` represents two epochs and old/new presented tokens that have been
issued by that generation. It checks store write acceptance and the launch
ownership-check decision use the current epoch at that atomic check. It does not
model actual invocation after the check, or prove the check-to-invocation gap is
safe. Controls remove each epoch comparison. Takeover eligibility is an abstract
environment event; TTL, owner authorization, heartbeat scheduling and SQLite
transactions are not proved. Already issued work remains outside these checks. [Persistence](persistence.md) explains durable evidence.
