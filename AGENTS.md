# Agent rules (keel-rt)

This crate is a **library kernel**. Do not add HTTP, YAML, Agent types, CLI, or
merge `examples/studio`. Keep DX APIs (`register_fn`, `Clock`, `Runtime::run`,
start fail-fast, `iter_nodes`, `is_successful_finish`). Coverage floor and
stress/adversarial tests are behavior locks.

The tests define the absences.

- `cargo test -- --test-threads=1` green. Do not skip or weaken adversarial,
  scenario, stress, or resilience tests.
- Coverage: `just coverage` / `./scripts/coverage.sh`. Kernel `src/` lines
  100%; `coverage/BASELINE` allowlist stays empty.
- One reason per commit. Imperative, module prefix.
- `FailSubtree` / `Join::AllDone` stay definition-only opt-in.

`just coverage` / `just stress-resume` / `just stress-100k` / `just chaos-sqlite`.
