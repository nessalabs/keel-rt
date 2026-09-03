# keel-rt-http

Thin adapter. Another binary inspects a live wait, reads the token, then
`POST`s that token and payload; this process calls `Runtime::inspect` /
`Runtime::complete`. Not the kernel. No forms, no identity, no start HTTP.

A **shared secret is required** on both routes. Send
`Authorization: Bearer <secret>` or `X-Keel-Complete: <secret>`. Missing
or wrong secret is **401**. Query-string secrets are ignored.

Default bind is **`127.0.0.1`** (`DEFAULT_BIND` / `serve` / `serve_ephemeral`).
`0.0.0.0` only via explicit `serve_on`. Body larger than **1 MiB**
(`MAX_COMPLETE_BODY`) is **413** and does not call `complete`.

[`KeelClient::inspect`] returns [`InspectView`] (id, state, nodes + **wait**
token once, in [`InspectNodeState::Waiting`]) — not an `ExecutionHandle`.
Running-node tokens are omitted from the DTO type (not a cloned kernel
`NodeState`). `InspectView::resume_token` reads `InspectNodeState::Waiting { token }`.
Unknown execution is **404**. Terminal
and Cancelled are **200** with state; `complete` of a cancelled token is
still **409**. Two Runtimes: inspect is read-only; complete on a
non-owner is **423 Locked** [`KeelClientError::ClaimedElsewhere`]
(`{"error":"claimed_elsewhere"}`), not 400 and not 409. The client sends both secret headers, does not follow
redirects, and fails [`KeelClientError::Hung`] if the server is silent
past [`HANG_BOUND`] (5s, tokio time). In-process complete stays
`Runtime::complete`. The client does not open sqlite or take a store lease.

```
GET /inspect/{execution_id}
Authorization: Bearer <secret>
```

```
POST /complete
Authorization: Bearer <secret>
Content-Type: application/json

{ "token": { ... }, "resume": { "Complete": { "Succeeded": "<bytes>" } } }
```

```rust
let secret = keel_rt_http::CompleteSecret::new(std::env::var("KEEL_COMPLETE_SECRET")?)?;
keel_rt_http::serve(runtime, secret).await?;
```

```rust
let client = keel_rt_http::KeelClient::new("http://127.0.0.1:port", secret)?;
let view = client.inspect(&execution_id).await?;
let token = view.resume_token(&NodeId::new("hold")).cloned().unwrap();
client.complete(token, Resume::Complete(NodeOutcome::Succeeded(bytes))).await?;
// or Decision::Complete(bytes) / Fail / Reinvoke — maps onto Resume
```

Serve `keel_rt_http::router(Arc<Runtime>, secret)` or `serve` / `serve_on` /
`serve_ephemeral`. Delete this crate without editing `scheduler.rs`.
