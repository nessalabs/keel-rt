# jemalloc vs system allocator

`keel-rt` has **no** `jemalloc` Cargo feature and never sets
`#[global_allocator]`. Allocator choice belongs to a consumer binary.

This file is a **one-machine experiment**: the same hot-path benches under
the system allocator vs jemalloc (`tikv-jemallocator` in an unpublished
binary only). It is not CI.

**Verdict (Phase 1 `current_thread`):** jemalloc is **not a stable win**.
Deltas are single-digit and **change sign** across back-to-back runs.
Do **not** recommend jemalloc for production binaries from these numbers.

## Reproduce

System allocator: `examples/kernel_benches.rs` (no global allocator).

Jemalloc: `benches/jemalloc_compare/` — a separate unpublished package
(not a workspace member, not a published feature). It is the only place
in this repo that installs **jemalloc** as `#[global_allocator]`. If
jemalloc cannot compile on a platform, skip this directory; default
`cargo test` never builds it.

```bash
./scripts/jemalloc-benches.sh
./scripts/jemalloc-benches.sh --multi-thread
# equivalent:
cargo run --release --example kernel_benches
cargo run --release --manifest-path benches/jemalloc_compare/Cargo.toml
```

Tokio remains **current_thread** (Phase 1 FIFO apply loop). `--multi-thread`
is a labelled experiment and does not change `Runtime::start`.

Machine: Cloud Agent VM (x86_64, 4× Intel Xeon). Release. Median of 7
except `wide_100k` (n=1). Instant-succeed executors.

## current_thread (Phase 1 default)

Warm pair (second back-to-back run):

| scenario | sys | jemalloc | delta |
|---|---:|---:|---:|
| wide_256 | 0.479 ms | 0.453 ms | −5.4% |
| chain_128 | 0.225 ms | 0.220 ms | −2.2% |
| diamond_10k | 18.323 ms | 17.708 ms | −3.4% |
| apply_only | 6.404 ms | 6.025 ms | −5.9% |
| wide_100k | 228.133 ms | 217.332 ms | −4.7% |

The **first** pair on the same machine, same binaries, minutes earlier:

| scenario | sys | jemalloc | delta |
|---|---:|---:|---:|
| wide_256 | 0.480 ms | 0.486 ms | +1.3% |
| chain_128 | 0.225 ms | 0.224 ms | −0.4% |
| diamond_10k | 18.506 ms | 19.460 ms | +5.2% |
| apply_only | 6.397 ms | 6.893 ms | +7.8% |
| wide_100k | 231.501 ms | 228.738 ms | −1.2% |

Same code, opposite story. Treat it as noise, not a reason to set
`tikv-jemallocator` in a production binary. The hot path is the apply
loop and slot vectors, not cross-thread allocator contention.

## multi_thread Tokio (experiment)

`Builder::new_multi_thread().worker_threads(4)`. One apply task per
execution still; execute tasks may run on other workers. **Not** the
Phase 1 scheduler contract. Also noisy on this VM:

| run | scenario | sys | jemalloc | delta |
|---|---|---:|---:|---:|
| 1 | wide_256 | 1.755 ms | 1.299 ms | −26.0% |
| 1 | chain_128 | 0.456 ms | 0.618 ms | +35.5% |
| 2 | wide_256 | 1.522 ms | 1.062 ms | −30.2% |
| 2 | chain_128 | 0.764 ms | 0.611 ms | −20.0% |

Wide fan-out sometimes looks better under jemalloc; chain does not
consistently. This does **not** justify changing `current_thread` or
FIFO, and it is **not** a recommendation to ship jemalloc.

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
