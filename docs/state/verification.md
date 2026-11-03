---
id: "verification"
title: "Check current evidence"
kind: "guide"
status: "reference"
summary: "Review exact source, current test runs, bounded model controls and unexecuted qualification separately."
parent: "keel"
sources: ["AGENTS.md", "scripts/coverage.sh", "scripts/pr_body_gate.py", "docs/FAILURE_CATALOG.md", "docs/RESUME_CATALOG.md"]
sourceRevision: "dbb8eceb94ff666824e6365b40171a78f71199cb"
diagramLinks: {}
---

# Check this atlas

Every page names the source revision actually reviewed. The models describe
selected finite design rules; TLC does not execute Rust functions, SQLite or
Wasmtime. Negative controls deliberately weaken a model guard and must expose the
expected invariant failure, rather than a parser error or an unfinished run.

## Reproduce model verification

With a built Nessa Docs checkout and its canonical pinned checker cache installed:

```sh
NESSA_DOCS_ROOT=/absolute/path/to/nessa-docs node scripts/state-docs/verify.mjs --write
```

The runner checks the catalog, unmodified baselines and all persisted controls,
saves baseline page receipts through the canonical writer, and records mutant
results separately. It neither installs a checker automatically nor edits the
authored models to run controls. See [results](../../verification/state-models/results.json),
[controls](../../verification/state-models/controls.json) and
[review findings](../../verification/state-models/review.md).

## Current execution evidence

At source `dbb8eceb94ff666824e6365b40171a78f71199cb`, on 2026-11-03:

| Check | Result |
| --- | --- |
| Kernel default Rust suite | 498 passed, zero failures, one preexisting ignored test |
| SQLite, HTTP, schedule, Wasm and consumer suites | 402 passed, zero failures or ignored tests |
| Catalog | 14 pages, zero issues |
| TLC baseline | 6 models passed all 12 configured invariants |
| Negative controls | All 12 broke the expected semantic rule; none unchecked |

[Implementation commands and logs](../../verification/state-models/test-results.md)
name the included suites and preexisting ignored test. The normal kernel run
includes debug 100k-node stress; sibling tests include SQLite chaos and Wasm
stress. These are executed tests, not claims inferred from test source. No
production implementation was changed.

The checker explored 91 states across independent finite models; the largest had 41 states.
This is not a product capacity or throughput measurement. Reader/source/model
review resolved both findings; see the linked review record. Browser smoke passed
14 pages and 12 diagrams with zero errors; see [browser receipt](../../verification/state-models/browser-results.json).

## What the models leave out

Each owner states its bounds, independently checked fact and assumed environment.
The catalog includes source-reviewed flows and contracts without direct models;
links to other owners do not establish complete feature verification. Finite
safety exploration does not prove liveness, arbitrary graph size, crash durability,
network security, native compiler isolation or exactly-once business effects.

The existing [failure catalog](../FAILURE_CATALOG.md),
[resume catalog](../RESUME_CATALOG.md), [chaos log](../CHAOS_LOG.md) and
[architecture guide](../ARCHITECTURE.md) record deeper behavior and historical
performance/qualification. Coverage remains the repository's 100% kernel-line
gate with an empty allowlist; no floor or test is weakened for these docs.
