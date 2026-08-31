# Keel-rt tasks. `just coverage` is the CI coverage gate.

test:
    cargo test --workspace -- --test-threads=1

clippy:
    cargo clippy --workspace --lib -- -D warnings

# src/ kernel line coverage + patch gate. Excludes stress_100k (separate job).
coverage:
    ./scripts/coverage.sh

# Sqlite resume stress (256-wide, sequential crash loop). Not coverage.
stress-resume:
    cargo test -p keel-rt-sqlite --test resume_stress -- --test-threads=1 --nocapture

# 100k scale pack — no coverage instrumentation.
stress-100k:
    cargo test --test stress_100k -- --test-threads=1

# Standing sqlite chaos / load (thousands of jobs, wide AND-join, HITL, two Runtimes).
# Not coverage.
chaos-sqlite:
    cargo test -p keel-rt-sqlite --test chaos -- --test-threads=1 --nocapture
