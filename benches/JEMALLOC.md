# jemalloc vs system allocator

`keel-rt` has **no** `jemalloc` Cargo feature and never sets
`#[global_allocator]`. Allocator choice belongs to a consumer binary.

This file is a **one-machine experiment**: the same hot-path benches under
the system allocator vs jemalloc (`tikv-jemallocator` in an unpublished
binary only). It is not CI.

## Reproduce

System allocator: `examples/kernel_benches.rs` (no global allocator).

Jemalloc: `benches/jemalloc_compare/` — a separate unpublished package
(not a workspace member, not a published feature). It is the only place
in this repo that installs **jemalloc** as `#[global_allocator]`. If jemalloc
cannot compile on a platform, skip this directory; default `cargo test`
never builds it.

```bash
./scripts/jemalloc-benches.sh
./scripts/jemalloc-benches.sh --multi-thread
# equivalent:
cargo run --release --example kernel_benches
cargo run --release --manifest-path benches/jemalloc_compare/Cargo.toml
```

Tokio remains **current_thread** (Phase 1 FIFO apply loop). `--multi-thread`
is a labelled experiment and does not change `Runtime::start`.

## current_thread (Phase 1 default)

*Re-measured after removing the crate feature. Table filled from this machine.*

| scenario | sys | jemalloc | delta |
|---|---:|---:|---:|
| wide_256 | — | — | — |
| chain_128 | — | — | — |
| diamond_10k | — | — | — |
| apply_only | — | — | — |
| wide_100k | — | — | — |

## multi_thread Tokio (experiment)

Not the Phase 1 scheduler contract.

| scenario | sys | jemalloc | delta |
|---|---:|---:|---:|
| wide_256 | — | — | — |
| chain_128 | — | — | — |

## Alloc count (system allocator, not a CI gate)

`examples/alloc_count.rs` wrapping `System` on current_thread, one 256-wide
join (258 nodes):

| metric | value |
|---|---:|
| allocs | 3732 |
| deallocs | 3193 |
| bytes requested | 685_100 |
| elapsed | 0.605 ms |

Not a fail-under. Re-measure if a hot-path change explodes this (e.g. 10×).
