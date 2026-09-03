# keel-rt-http

Thin adapter. Another binary starts a run (`POST /start` is kernel
`WorkflowDefinition::durable_bytes` — `id`, `on_failure`, nodes with
`join`, edges), inspects a live wait, reads the token, then `POST`s
that token and payload; this process calls `Runtime::start` /
`Runtime::inspect` / `Runtime::complete`. Not the kernel.
No forms, no identity, no schedule HTTP.

A **shared secret is required** on every route. Send
`Authorization: Bearer <secret>` or `X-Keel-Complete: <secret>`. Missing
or wrong secret is **401**. Query-string secrets are ignored.

Default bind is **`127.0.0.1`** (`DEFAULT_BIND` / `serve` / `serve_ephemeral`).
`0.0.0.0` only via explicit `serve_on`. Body larger than **1 MiB**
(`MAX_BODY`) is **413** and does not call start or complete.

The **engine process** registers executors (`register_fn`) before it
serves. [`KeelClient::start`] sends only the definition
(`WorkflowDefinition::durable_bytes`). It cannot register a function.
A node whose `executor_id` is not on that Runtime is **400**
`{"error":"unregistered","executors":["…"]}`
(`client_start_unregistered_is_400_nothing_runs`).
[`KeelClient::executors`] (`GET /executors`) lists the ids this Runtime
has registered, plus builtin `wait`. Builtin `wait` is already on
`Runtime::builder`. Runnable loop:
`cargo run -p keel-rt-http --example sdk_loop`.

[`KeelClient::inspect`] returns [`InspectView`] (id, state, nodes + **wait**
token once, in [`InspectNodeState::Waiting`]) — not an `ExecutionHandle`.
Running-node tokens are omitted from the DTO type (not a cloned kernel
`NodeState`). Result bytes live only on
[`InspectNodeState::Succeeded { output }`].
`InspectView::resume_token` reads `InspectNodeState::Waiting { token }`.
Unknown execution is **404**. Terminal
and Cancelled are **200** with state; `complete` of a cancelled token is
still **409**. Two Runtimes: inspect is read-only; complete on a
non-owner is **423 Locked** [`KeelClientError::ClaimedElsewhere`]
(`{"error":"claimed_elsewhere"}`), not 400 and not 409. The client sends both secret headers, does not follow
redirects, and fails [`KeelClientError::Hung`] if the server is silent
past [`HANG_BOUND`] (5s, tokio time). In-process complete stays
`Runtime::complete`. The client does not open sqlite or take a store lease.

```
POST /start
Authorization: Bearer <secret>
Content-Type: application/json

{ "id": "wf", "on_failure": "FailExecution", "nodes": [{"id":"hold","executor_id":"wait","join":"AllSucceeded"}], "edges": [] }
```

```
GET /inspect/{execution_id}
Authorization: Bearer <secret>
```

```
GET /executors
Authorization: Bearer <secret>
```

```
POST /complete
Authorization: Bearer <secret>
Content-Type: application/json

{ "token": { ... }, "resume": { "Complete": { "Succeeded": "<bytes>" } } }
```

```
POST /approve
Authorization: Bearer <secret>
Content-Type: application/json

{ "token": { ... }, "output": "<bytes>" }
```

```
POST /reject
Authorization: Bearer <secret>
Content-Type: application/json

{ "token": { ... } }
```

```rust
let secret = keel_rt_http::CompleteSecret::new(std::env::var("KEEL_COMPLETE_SECRET")?)?;
keel_rt_http::serve(runtime, secret).await?;
```

```rust
let client = keel_rt_http::KeelClient::new("http://127.0.0.1:port", secret)?;
let ids = client.executors().await?;
let id = client.start(definition).await?;
let view = client.inspect(&id).await?;
let token = view.resume_token(&NodeId::new("hold")).cloned().unwrap();
client.approve(token, bytes).await?; // POST /approve
// or reject(token) → POST /reject → Decision::Fail
// or complete(token, Resume / Decision) → POST /complete
client.cancel(&id).await?; // one execution; later approve is 409
```

Serve `keel_rt_http::router(Arc<Runtime>, secret)` or `serve` / `serve_on` /
`serve_ephemeral`. Delete this crate without editing `scheduler.rs`.
