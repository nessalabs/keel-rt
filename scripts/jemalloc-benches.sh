#!/usr/bin/env bash
# Compare hot-path benches: system allocator vs jemalloc.
# jemalloc lives only in benches/jemalloc_compare (not a keel-rt feature).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

echo "=== system allocator (examples/kernel_benches) ==="
cargo run --release --example kernel_benches -- "$@"

echo "=== jemalloc (benches/jemalloc_compare; not published) ==="
cargo run --release --manifest-path benches/jemalloc_compare/Cargo.toml -- "$@"
