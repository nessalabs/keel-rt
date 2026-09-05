# Keel-rt tasks. `just coverage` is the CI coverage gate.

test:
    cargo test --workspace -- --test-threads=1

# HTTP adapter (GET /inspect + POST /complete). Not kernel coverage.
http:
    cargo test -p keel-rt-http -- --test-threads=1

# Optional Wasmtime adapter and consumer regression fixture.
wasm:
    cargo test --locked -p keel-rt-wasm -p keel-rt-wasm-consumer-test -- --test-threads=1
    cargo run --locked -p keel-rt-wasm --example echo

clippy:
    cargo clippy --workspace --lib -- -D warnings

# src/ kernel line coverage + patch gate. Excludes stress_100k (separate job).
coverage:
    ./scripts/coverage.sh

# PR description: mermaid + When a caller + base main (unless [stack]).
pr-body:
    python3 scripts/pr_body_gate.py --self-test

# Sqlite resume stress (256-wide, sequential crash loop). Not coverage.
stress-resume:
    cargo test -p keel-rt-sqlite --test resume_stress -- --test-threads=1 --nocapture

# 100k scale pack — no coverage instrumentation.
stress-100k:
    cargo test --test stress_100k -- --test-threads=1

# Standing sqlite chaos / load (thousands of jobs, wide AND-join, HITL, two Runtimes)
# plus the seeded crash-inject pack. Not coverage.
chaos-sqlite:
    cargo test -p keel-rt-sqlite --test chaos --test crash_inject -- --test-threads=1 --nocapture
