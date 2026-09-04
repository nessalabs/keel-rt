//! Thin HTTP adapter. Another process starts a run, inspects for a wait
//! token, then POSTs that token + [`Resume`], or cancels by id. This
//! process calls [`Runtime::start`] / [`Runtime::inspect`] /
//! [`Runtime::complete`] / [`Runtime::cancel`]. No forms, no identity.
//!
//! A shared secret is required. Default bind is `127.0.0.1` only.
//! The engine process registers executors; [`KeelClient::start`] sends
//! only the definition. [`GET /executors`](KeelClient::executors) lists
//! the ids this Runtime has registered (plus builtin `wait`).
//! [`KeelClient::start`] + [`KeelClient::inspect`] +
//! [`KeelClient::complete`] / [`KeelClient::approve`] /
//! [`KeelClient::reject`] / [`KeelClient::cancel`] is the out-of-process
//! wait round-trip (`examples/sdk_loop.rs`). Kernel `keel-rt` does not
//! depend on this crate.

mod client;

pub use client::{Decision, KeelClient, KeelClientError, HANG_BOUND};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Json;
use axum::Router;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use keel_rt::{
    CancelError, CompleteError, ExecutionHandle, ExecutionId, ExecutionSnapshot, ExecutionState,
    ExecutorId, Join, NodeId, NodeState, OnFailure, Resume, ResumeToken, Runtime, StartError,
    WorkflowDefinition, WorkflowId,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// Default listen address: loopback, ephemeral port. Never `0.0.0.0`.
pub const DEFAULT_BIND: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

/// JSON larger than this is not accepted on the request, and inspect JSON
/// larger than this is **413**. Compact encoding is not a cap: a huge
/// Succeeded still 413s. Oversized request body does not call start or
/// complete. Cancel is id-only: that route rejects any body (`413`).
pub const MAX_BODY: usize = 1024 * 1024;

/// `POST /complete` when another Runtime holds the execution
/// (`CompleteError::ClaimedElsewhere`). **423 Locked**. Not 400 (malformed
/// body) and not 409 (Cancelled). Body is `{"error":"claimed_elsewhere"}`.
pub const CLAIMED_ELSEWHERE: u16 = 423;

/// Shared-secret header (alternative to `Authorization: Bearer …`).
/// Same header on start, inspect, and complete. Wire name is historical.
pub const SECRET_HEADER: &str = "x-keel-complete";

/// Shared secret for **every** adapter route (start, inspect, complete).
///
/// The type is still `CompleteSecret` because the wire header is historical
/// [`SECRET_HEADER`] (`x-keel-complete`). Renaming the type would fork it
/// from that header. It is not a complete-only credential. Empty is rejected.
#[derive(Clone)]
pub struct CompleteSecret(Arc<str>);

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SecretError {
    #[error("secret must not be empty")]
    Empty,
}

impl CompleteSecret {
    pub fn new(secret: impl Into<String>) -> Result<Self, SecretError> {
        let s = secret.into();
        if s.is_empty() {
            return Err(SecretError::Empty);
        }
        Ok(Self(Arc::from(s)))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CompleteSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CompleteSecret(..)")
    }
}

/// HTTP wire for [`Resume`]. Kernel serde stays `[u8]` arrays in-process;
/// this adapter maps Succeeded bytes through [`wire_bytes`] (one base64 field).
#[derive(Clone, Debug, Serialize, Deserialize)]
enum WireResume {
    Complete(WireOutcome),
    Reinvoke,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum WireOutcome {
    Succeeded(#[serde(with = "wire_bytes")] Bytes),
    Failed(keel_rt::NodeError),
    Waiting { token: ResumeToken },
    TimedOut,
}

impl From<&Resume> for WireResume {
    fn from(resume: &Resume) -> Self {
        match resume {
            Resume::Complete(o) => Self::Complete(match o {
                keel_rt::NodeOutcome::Succeeded(b) => WireOutcome::Succeeded(b.clone()),
                keel_rt::NodeOutcome::Failed(e) => WireOutcome::Failed(e.clone()),
                keel_rt::NodeOutcome::Waiting { token } => WireOutcome::Waiting {
                    token: token.clone(),
                },
                keel_rt::NodeOutcome::TimedOut => WireOutcome::TimedOut,
            }),
            Resume::Reinvoke => Self::Reinvoke,
        }
    }
}

impl From<WireResume> for Resume {
    fn from(wire: WireResume) -> Self {
        match wire {
            WireResume::Complete(o) => Resume::Complete(match o {
                WireOutcome::Succeeded(b) => keel_rt::NodeOutcome::Succeeded(b),
                WireOutcome::Failed(e) => keel_rt::NodeOutcome::Failed(e),
                WireOutcome::Waiting { token } => keel_rt::NodeOutcome::Waiting { token },
                WireOutcome::TimedOut => keel_rt::NodeOutcome::TimedOut,
            }),
            WireResume::Reinvoke => Resume::Reinvoke,
        }
    }
}

mod wire_resume {
    use super::{Resume, WireResume};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(resume: &Resume, serializer: S) -> Result<S::Ok, S::Error> {
        WireResume::from(resume).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Resume, D::Error> {
        WireResume::deserialize(deserializer).map(Resume::from)
    }
}

/// `POST /complete` body. [`Resume`] on the value; Succeeded bytes on the
/// wire are the same base64 field as inspect / approve.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompleteBody {
    pub token: ResumeToken,
    #[serde(with = "wire_resume")]
    pub resume: Resume,
}

/// Compact inspect / approve payload. A JSON `[u8]` array of 1 MiB is a
/// multi-MiB bomb; this is one base64 field.
mod wire_bytes {
    use super::{Bytes, Engine, STANDARD};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(bytes.as_ref()))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Bytes, D::Error> {
        let s = String::deserialize(deserializer)?;
        STANDARD
            .decode(s.as_bytes())
            .map(Bytes::from)
            .map_err(serde::de::Error::custom)
    }
}

mod opt_wire_bytes {
    use super::{Bytes, Engine, STANDARD};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        value: &Option<Bytes>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(b) => super::wire_bytes::serialize(b, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Bytes>, D::Error> {
        match Option::<String>::deserialize(deserializer)? {
            None => Ok(None),
            Some(s) => STANDARD
                .decode(s.as_bytes())
                .map(|v| Some(Bytes::from(v)))
                .map_err(serde::de::Error::custom),
        }
    }
}

/// `POST /approve` body: token + optional output bytes (base64).
/// Maps onto [`Decision::Complete`] → the existing complete path.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutputBody {
    pub token: ResumeToken,
    #[serde(default, with = "opt_wire_bytes")]
    pub output: Option<Bytes>,
}

/// `POST /reject` body: token only.
/// Maps onto [`Decision::Fail`] → the existing complete path.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenBody {
    pub token: ResumeToken,
}

/// `GET /executors` body. Ids from [`Runtime::executor_ids`] — including
/// builtin [`keel_rt::WAIT_ID`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutorsView {
    pub executors: Vec<ExecutorId>,
}

/// `POST /start` 400 when the definition names ids this Runtime does not
/// have. `error` is `"unregistered"`; `executors` is the missing ids.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnregisteredBody {
    pub error: String,
    pub executors: Vec<ExecutorId>,
}

/// `POST /start` body. This **is** kernel durable JSON
/// ([`WorkflowDefinition::durable_bytes`] / [`WorkflowDefinition::from_durable_bytes`]),
/// not a second graph language and not a snapshot dump.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartBody {
    pub id: WorkflowId,
    pub on_failure: OnFailure,
    pub nodes: Vec<StartNode>,
    pub edges: Vec<StartEdge>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartNode {
    pub id: NodeId,
    pub executor_id: ExecutorId,
    pub join: Join,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartEdge {
    pub from: NodeId,
    pub to: NodeId,
}

/// `POST /start` response. [`Runtime::start`] returns a handle; HTTP returns
/// the new [`ExecutionId`] (the handle stays on the server so Drop does not
/// cancel).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartView {
    pub execution_id: ExecutionId,
}

impl From<&WorkflowDefinition> for StartBody {
    fn from(def: &WorkflowDefinition) -> Self {
        serde_json::from_slice(&def.durable_bytes()).expect("durable_bytes is StartBody JSON")
    }
}

impl From<WorkflowDefinition> for StartBody {
    fn from(def: WorkflowDefinition) -> Self {
        Self::from(&def)
    }
}

impl StartBody {
    fn into_definition(self) -> Result<WorkflowDefinition, keel_rt::DefinitionError> {
        WorkflowDefinition::from_durable_bytes(
            &serde_json::to_vec(&self).expect("StartBody is durable JSON"),
        )
    }
}

/// Read-only view of an execution for `GET /inspect/:id`.
/// Not an [`keel_rt::ExecutionHandle`]. Not a cloned [`ExecutionSnapshot`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectView {
    pub execution_id: ExecutionId,
    pub state: ExecutionState,
    pub nodes: Vec<InspectNode>,
}

/// One node on the inspect wire: id + DTO state.
/// Wait token exists only as [`InspectNodeState::Waiting { token }`].
/// Output bytes exist only as [`InspectNodeState::Succeeded { output }`].
/// Running-node snapshot tokens, last_error, and resume_token are not on this type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectNode {
    pub id: NodeId,
    pub state: InspectNodeState,
}

/// HTTP inspect state. Mapped from kernel [`NodeState`]; not kernel serde.
/// `Waiting` is the only variant that carries a token.
/// `Succeeded` is the only variant that carries output bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InspectNodeState {
    Pending,
    Ready,
    Running {
        attempt: u32,
    },
    Waiting {
        token: ResumeToken,
        attempt: u32,
    },
    Succeeded {
        #[serde(with = "wire_bytes")]
        output: Bytes,
    },
    Failed,
    Cancelled,
    TimedOut,
}

impl InspectNodeState {
    fn from_kernel(n: &keel_rt::NodeSnapshot) -> Self {
        match &n.state {
            NodeState::Pending => Self::Pending,
            NodeState::Ready { .. } => Self::Ready,
            NodeState::Running { attempt } => Self::Running { attempt: *attempt },
            NodeState::Waiting { token, attempt } => Self::Waiting {
                token: token.clone(),
                attempt: *attempt,
            },
            NodeState::Succeeded => Self::Succeeded {
                output: n.output.clone().unwrap_or_default(),
            },
            NodeState::Failed => Self::Failed,
            NodeState::Cancelled => Self::Cancelled,
            NodeState::TimedOut => Self::TimedOut,
        }
    }
}

impl InspectView {
    pub fn from_snapshot(snap: &ExecutionSnapshot) -> Self {
        Self {
            execution_id: snap.execution_id.clone(),
            state: snap.state,
            nodes: snap
                .iter_nodes()
                .map(|(id, n)| InspectNode {
                    id: id.clone(),
                    state: InspectNodeState::from_kernel(n),
                })
                .collect(),
        }
    }

    pub fn node(&self, id: &NodeId) -> Option<&InspectNode> {
        self.nodes.iter().find(|n| n.id == *id)
    }

    /// Wait token for `id` if that node is [`InspectNodeState::Waiting`].
    pub fn resume_token(&self, id: &NodeId) -> Option<&ResumeToken> {
        match &self.node(id)?.state {
            InspectNodeState::Waiting { token, .. } => Some(token),
            _ => None,
        }
    }
}

#[derive(Clone)]
struct App {
    runtime: Arc<Runtime>,
    secret: CompleteSecret,
    /// [`Runtime::start`] returns a handle; Drop cancels. Hold them here.
    started: Arc<Mutex<Vec<ExecutionHandle>>>,
}

/// Router a caller can nest or serve. Paths: `POST /start`, `GET /inspect/:id`,
/// `GET /executors`, `POST /complete`, `POST /approve`, `POST /reject`,
/// `POST /cancel/:id`.
pub fn router(runtime: Arc<Runtime>, secret: CompleteSecret) -> Router {
    Router::new()
        .route("/start", post(start_handler))
        .route("/complete", post(complete_handler))
        .route("/approve", post(approve_handler))
        .route("/reject", post(reject_handler))
        .route("/executors", get(executors_handler))
        .route("/inspect/:id", get(inspect_handler))
        .route(
            "/cancel/:id",
            post(cancel_handler).layer(DefaultBodyLimit::max(0)),
        )
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .with_state(App {
            runtime,
            secret,
            started: Arc::new(Mutex::new(Vec::new())),
        })
}

fn provided_secret(headers: &HeaderMap) -> Option<&[u8]> {
    if let Some(v) = headers.get(SECRET_HEADER) {
        return Some(v.as_bytes());
    }
    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.as_bytes().strip_prefix(b"Bearer "))
}

fn secrets_equal(expected: &str, got: &[u8]) -> bool {
    let exp = expected.as_bytes();
    if exp.len() != got.len() {
        return false;
    }
    exp.iter()
        .zip(got.iter())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

/// Hold iff the snapshot is not terminal. Dropping a terminal handle
/// sends Cancel, which is a no-op on an already-finished run. Do not
/// park on the handle: consuming wait marks the handle used (server-drop
/// would not cancel a live park); the stable-wait helper also returns
/// on Waiting (reaping a park drops the cancel token). Kernel snapshot
/// `state.is_terminal()` is the only signal.
fn hold_if_live(started: &Mutex<Vec<ExecutionHandle>>, handle: ExecutionHandle, terminal: bool) {
    if terminal {
        return;
    }
    started
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push(handle);
}

fn drop_held_if_terminal(started: &Mutex<Vec<ExecutionHandle>>, id: &ExecutionId, terminal: bool) {
    if !terminal {
        return;
    }
    started
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .retain(|h| h.execution_id() != id);
}

/// Snapshot-only: drop held handles whose execution is already terminal.
/// Called on the next start so N finished runs do not sit until process drop.
async fn reap_held_terminals(runtime: &Runtime, started: &Mutex<Vec<ExecutionHandle>>) {
    let ids: Vec<ExecutionId> = {
        started
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|h| h.execution_id().clone())
            .collect()
    };
    for id in ids {
        let terminal = runtime
            .inspect(&id)
            .await
            .is_some_and(|s| s.state.is_terminal());
        drop_held_if_terminal(started, &id, terminal);
    }
}

fn authorize(app: &App, headers: &HeaderMap) -> Result<(), StatusCode> {
    match provided_secret(headers) {
        Some(got) if secrets_equal(app.secret.as_str(), got) => Ok(()),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

async fn start_handler(
    State(app): State<App>,
    headers: HeaderMap,
    Json(body): Json<StartBody>,
) -> axum::response::Response {
    if authorize(&app, &headers).is_err() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let def = match body.into_definition() {
        Ok(def) => def,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    match app.runtime.start(def) {
        Ok(handle) => {
            let execution_id = handle.execution_id().clone();
            reap_held_terminals(&app.runtime, &app.started).await;
            let terminal = handle.inspect().await.state.is_terminal();
            hold_if_live(&app.started, handle, terminal);
            Json(StartView { execution_id }).into_response()
        }
        Err(StartError::UnregisteredExecutors(missing)) => (
            StatusCode::BAD_REQUEST,
            Json(UnregisteredBody {
                error: "unregistered".into(),
                executors: missing.0,
            }),
        )
            .into_response(),
    }
}

async fn executors_handler(
    State(app): State<App>,
    headers: HeaderMap,
) -> Result<Json<ExecutorsView>, StatusCode> {
    authorize(&app, &headers)?;
    Ok(Json(ExecutorsView {
        executors: app.runtime.executor_ids(),
    }))
}

async fn inspect_handler(
    State(app): State<App>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> axum::response::Response {
    if authorize(&app, &headers).is_err() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let id = match ExecutionId::parse(&id) {
        Ok(id) => id,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    match app.runtime.inspect(&id).await {
        Some(snap) => {
            drop_held_if_terminal(&app.started, &id, snap.state.is_terminal());
            let json = match serde_json::to_vec(&InspectView::from_snapshot(&snap)) {
                Ok(j) => j,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };
            if json.len() > MAX_BODY {
                return StatusCode::PAYLOAD_TOO_LARGE.into_response();
            }
            ([(CONTENT_TYPE, "application/json")], json).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
}

async fn complete_handler(
    State(app): State<App>,
    headers: HeaderMap,
    Json(body): Json<CompleteBody>,
) -> axum::response::Response {
    if authorize(&app, &headers).is_err() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let id = body.token.execution_id().clone();
    match app.runtime.complete(body.token, body.resume).await {
        Ok(()) => {
            let terminal = app
                .runtime
                .inspect(&id)
                .await
                .is_some_and(|s| s.state.is_terminal());
            drop_held_if_terminal(&app.started, &id, terminal);
            StatusCode::OK.into_response()
        }
        Err(CompleteError::UnknownToken) => StatusCode::NOT_FOUND.into_response(),
        Err(CompleteError::Cancelled) => StatusCode::CONFLICT.into_response(),
        Err(CompleteError::ClaimedElsewhere) => (
            StatusCode::LOCKED,
            Json(ErrorBody {
                error: "claimed_elsewhere",
            }),
        )
            .into_response(),
        Err(_) => StatusCode::BAD_REQUEST.into_response(),
    }
}

async fn approve_handler(
    State(app): State<App>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    if authorize(&app, &headers).is_err() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let parsed: OutputBody = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    complete_handler(
        State(app),
        headers,
        Json(CompleteBody {
            token: parsed.token,
            resume: Decision::Complete(parsed.output.unwrap_or_default()).into(),
        }),
    )
    .await
}

async fn reject_handler(
    State(app): State<App>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    if authorize(&app, &headers).is_err() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let parsed: TokenBody = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    complete_handler(
        State(app),
        headers,
        Json(CompleteBody {
            token: parsed.token,
            resume: Decision::Fail.into(),
        }),
    )
    .await
}

async fn cancel_handler(
    State(app): State<App>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Bytes,
) -> axum::response::Response {
    if authorize(&app, &headers).is_err() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if !body.is_empty() {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }
    let id = match ExecutionId::parse(&id) {
        Ok(id) => id,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    match app.runtime.cancel(&id).await {
        Ok(()) => {
            let terminal = app
                .runtime
                .inspect(&id)
                .await
                .is_some_and(|s| s.state.is_terminal());
            drop_held_if_terminal(&app.started, &id, terminal);
            StatusCode::OK.into_response()
        }
        Err(CancelError::UnknownExecution) => StatusCode::NOT_FOUND.into_response(),
        Err(CancelError::ClaimedElsewhere) => (
            StatusCode::LOCKED,
            Json(ErrorBody {
                error: "claimed_elsewhere",
            }),
        )
            .into_response(),
        Err(_) => StatusCode::BAD_REQUEST.into_response(),
    }
}

/// Bind [`DEFAULT_BIND`] (`127.0.0.1`) and serve. Never `0.0.0.0`.
pub async fn serve(runtime: Arc<Runtime>, secret: CompleteSecret) -> std::io::Result<()> {
    serve_on(runtime, secret, DEFAULT_BIND).await
}

/// Explicit address. `0.0.0.0` only if the caller passes it.
pub async fn serve_on(
    runtime: Arc<Runtime>,
    secret: CompleteSecret,
    addr: SocketAddr,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(runtime, secret)).await
}

/// Bind `127.0.0.1:0` and return the local address plus the server task.
pub async fn serve_ephemeral(
    runtime: Arc<Runtime>,
    secret: CompleteSecret,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<std::io::Result<()>>)> {
    let listener = tokio::net::TcpListener::bind(DEFAULT_BIND).await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move { axum::serve(listener, router(runtime, secret)).await });
    Ok((addr, handle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_body_is_durable_bytes_not_snapshot() {
        let def = WorkflowDefinition::builder("wf")
            .on_failure(OnFailure::FailSubtree)
            .node("hold", "wait")
            .node("next", "next")
            .join("next", Join::AllDone)
            .edge("hold", "next")
            .build()
            .unwrap();
        let body = StartBody::from(&def);
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["id"], "wf");
        assert_eq!(json["on_failure"], "FailSubtree");
        assert_eq!(json["nodes"][1]["join"], "AllDone");
        assert!(json.get("nodes").unwrap().as_array().unwrap()[0]
            .get("state")
            .is_none());
        assert!(json.get("resume_token").is_none());
        let durable: serde_json::Value = serde_json::from_slice(&def.durable_bytes()).unwrap();
        assert_eq!(json, durable, "StartBody must be kernel durable JSON");
        let back = body.into_definition().unwrap();
        assert_eq!(back.content_hash(), def.content_hash());
        assert_eq!(back.on_failure(), OnFailure::FailSubtree);
        assert_eq!(
            back.join_of(&keel_rt::NodeId::new("next")),
            Some(Join::AllDone)
        );
    }

    #[test]
    fn body_round_trips() {
        let token = ResumeToken::issue(
            keel_rt::ExecutionId::parse("exec-1").unwrap(),
            keel_rt::NodeId::new("hold"),
            1,
        );
        let body = CompleteBody {
            token,
            resume: Resume::Reinvoke,
        };
        let v = serde_json::to_value(&body).unwrap();
        let back: CompleteBody = serde_json::from_value(v).unwrap();
        assert!(matches!(back.resume, Resume::Reinvoke));
    }

    #[test]
    fn complete_body_json_succeeded_is_base64_not_array() {
        let token = ResumeToken::issue(
            keel_rt::ExecutionId::parse("exec-1").unwrap(),
            keel_rt::NodeId::new("hold"),
            1,
        );
        let body = CompleteBody {
            token,
            resume: Resume::Complete(keel_rt::NodeOutcome::Succeeded(bytes::Bytes::from_static(
                b"g",
            ))),
        };
        let v = serde_json::to_value(&body).unwrap();
        let succeeded = &v["resume"]["Complete"]["Succeeded"];
        assert!(
            succeeded.is_string(),
            "complete Succeeded must be the same base64 field as inspect/approve: {v}"
        );
        assert_eq!(succeeded, "Zw==");
        assert!(
            !succeeded.is_array(),
            "kernel Resume [u8] array must not be the HTTP complete wire: {v}"
        );
        let kernel = serde_json::to_value(&body.resume).unwrap();
        assert!(
            kernel["Complete"]["Succeeded"].is_array(),
            "in-process Resume serde stays a [u8] array: {kernel}"
        );
        let back: CompleteBody = serde_json::from_value(v).unwrap();
        assert!(matches!(
            back.resume,
            Resume::Complete(keel_rt::NodeOutcome::Succeeded(ref b)) if b.as_ref() == b"g"
        ));
    }

    #[test]
    fn default_bind_is_loopback_not_unspecified() {
        assert!(DEFAULT_BIND.ip().is_loopback());
        assert!(!DEFAULT_BIND.ip().is_unspecified());
        assert_eq!(DEFAULT_BIND.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(DEFAULT_BIND.port(), 0);
    }

    #[test]
    fn empty_secret_is_rejected() {
        assert_eq!(CompleteSecret::new("").unwrap_err(), SecretError::Empty);
        assert!(CompleteSecret::new("s").is_ok());
        assert_eq!(
            format!("{:?}", CompleteSecret::new("s").unwrap()),
            "CompleteSecret(..)"
        );
    }

    #[test]
    fn secrets_equal_rejects_wrong_and_empty() {
        assert!(secrets_equal("abc", b"abc"));
        assert!(!secrets_equal("abc", b"ab"));
        assert!(!secrets_equal("abc", b"abd"));
        assert!(!secrets_equal("abc", b""));
    }

    #[test]
    fn provided_secret_reads_bearer_or_header() {
        let mut h = HeaderMap::new();
        assert!(provided_secret(&h).is_none());
        h.insert(AUTHORIZATION, "Basic nope".parse().unwrap());
        assert!(provided_secret(&h).is_none());
        h.insert(AUTHORIZATION, "Bearer tok".parse().unwrap());
        assert_eq!(provided_secret(&h), Some(&b"tok"[..]));
        h.insert(SECRET_HEADER, "hdr".parse().unwrap());
        assert_eq!(provided_secret(&h), Some(&b"hdr"[..]));
    }

    #[test]
    fn serve_source_does_not_default_to_unspecified() {
        let src = include_str!("lib.rs");
        let serve_fn = src.split("pub async fn serve(").nth(1).unwrap();
        let serve_body = serve_fn.split("pub async fn serve_on").next().unwrap();
        assert!(
            serve_body.contains("DEFAULT_BIND") && serve_body.contains("serve_on"),
            "serve must use DEFAULT_BIND via serve_on, not an implicit unspecified bind"
        );
    }

    #[test]
    fn inspect_view_reads_wait_token_from_snapshot() {
        let id = keel_rt::ExecutionId::parse("exec-1").unwrap();
        let hold = keel_rt::NodeId::new("hold");
        let token = ResumeToken::issue(id.clone(), hold.clone(), 1);
        let mut snap = ExecutionSnapshot {
            schema_version: keel_rt::SCHEMA_VERSION,
            revision: 1,
            execution_id: id.clone(),
            workflow_id: keel_rt::WorkflowId::new("wf"),
            state: ExecutionState::Waiting,
            nodes: Default::default(),
            node_order: vec![hold.clone()],
            definition_hash: Default::default(),
        };
        snap.nodes.insert(
            hold.clone(),
            keel_rt::NodeSnapshot {
                state: NodeState::Waiting {
                    token: token.clone(),
                    attempt: 1,
                },
                output: None,
                attempt: 1,
                resume_token: Some(token.clone()),
                last_error: None,
            },
        );
        let view = InspectView::from_snapshot(&snap);
        assert_eq!(view.execution_id, id);
        assert_eq!(view.state, ExecutionState::Waiting);
        assert_eq!(view.resume_token(&hold), Some(&token));
        assert!(view.node(&keel_rt::NodeId::new("missing")).is_none());
        let running_tok = ResumeToken::issue(id.clone(), keel_rt::NodeId::new("slow"), 1);
        let mut running = snap.clone();
        running.nodes.insert(
            keel_rt::NodeId::new("slow"),
            keel_rt::NodeSnapshot {
                state: NodeState::Running { attempt: 1 },
                output: None,
                attempt: 1,
                resume_token: Some(running_tok.clone()),
                last_error: None,
            },
        );
        running.node_order.push(keel_rt::NodeId::new("slow"));
        let running_view = InspectView::from_snapshot(&running);
        assert!(
            running_view
                .resume_token(&keel_rt::NodeId::new("slow"))
                .is_none(),
            "InspectView must not expose a Running-node token as a wait token"
        );
        let json = serde_json::to_string(&running_view).unwrap();
        let wait_json = serde_json::to_string(&token).unwrap();
        let run_json = serde_json::to_string(&running_tok).unwrap();
        assert_eq!(
            json.matches(wait_json.as_str()).count(),
            1,
            "wait token once: {json}"
        );
        assert!(
            !json.contains(&run_json),
            "Running-node resume token must not appear on inspect JSON: {json}"
        );
        assert!(
            json.contains("\"kind\":\"waiting\"") && json.contains("\"kind\":\"running\""),
            "InspectView DTO is tagged InspectNodeState, not a kernel NodeState dump: {json}"
        );
        assert!(
            !json.contains("resume_token"),
            "InspectView DTO has no resume_token field: {json}"
        );
        assert_eq!(CLAIMED_ELSEWHERE, StatusCode::LOCKED.as_u16());
    }

    #[test]
    fn inspect_view_json_has_exactly_one_wait_token() {
        let id = keel_rt::ExecutionId::parse("exec-1").unwrap();
        let hold = keel_rt::NodeId::new("hold");
        let token = ResumeToken::issue(id.clone(), hold.clone(), 1);
        let mut snap = ExecutionSnapshot {
            schema_version: keel_rt::SCHEMA_VERSION,
            revision: 1,
            execution_id: id,
            workflow_id: keel_rt::WorkflowId::new("wf"),
            state: ExecutionState::Waiting,
            nodes: Default::default(),
            node_order: vec![hold.clone()],
            definition_hash: Default::default(),
        };
        snap.nodes.insert(
            hold.clone(),
            keel_rt::NodeSnapshot {
                state: NodeState::Waiting {
                    token: token.clone(),
                    attempt: 1,
                },
                output: None,
                attempt: 1,
                resume_token: Some(token.clone()),
                last_error: None,
            },
        );
        let view = InspectView::from_snapshot(&snap);
        assert_eq!(view.resume_token(&hold), Some(&token));
        assert!(matches!(
            view.node(&hold).unwrap().state,
            InspectNodeState::Waiting { .. }
        ));
        let json = serde_json::to_string(&view).unwrap();
        let token_json = serde_json::to_string(&token).unwrap();
        assert_eq!(json.matches(token_json.as_str()).count(), 1, "{json}");
        assert!(!json.contains("resume_token"), "{json}");
        let nonce = format!("{:032x}", token.nonce());
        assert_eq!(json.matches(nonce.as_str()).count(), 1, "{json}");
        let back: InspectView = serde_json::from_str(&json).unwrap();
        assert_eq!(back.resume_token(&hold), Some(&token));
    }

    #[test]
    fn inspect_view_json_running_pred_does_not_contain_running_token() {
        let id = keel_rt::ExecutionId::parse("exec-mix").unwrap();
        let slow = keel_rt::NodeId::new("slow");
        let hold = keel_rt::NodeId::new("hold");
        let wait_tok = ResumeToken::issue(id.clone(), hold.clone(), 1);
        let run_tok = ResumeToken::issue(id.clone(), slow.clone(), 1);
        let mut snap = ExecutionSnapshot {
            schema_version: keel_rt::SCHEMA_VERSION,
            revision: 1,
            execution_id: id.clone(),
            workflow_id: keel_rt::WorkflowId::new("wf"),
            state: ExecutionState::Running,
            nodes: Default::default(),
            node_order: vec![slow.clone(), hold.clone()],
            definition_hash: Default::default(),
        };
        snap.nodes.insert(
            slow.clone(),
            keel_rt::NodeSnapshot {
                state: NodeState::Running { attempt: 1 },
                output: None,
                attempt: 1,
                resume_token: Some(run_tok.clone()),
                last_error: None,
            },
        );
        snap.nodes.insert(
            hold.clone(),
            keel_rt::NodeSnapshot {
                state: NodeState::Waiting {
                    token: wait_tok.clone(),
                    attempt: 1,
                },
                output: None,
                attempt: 1,
                resume_token: Some(wait_tok.clone()),
                last_error: None,
            },
        );
        let view = InspectView::from_snapshot(&snap);
        assert_eq!(view.resume_token(&hold), Some(&wait_tok));
        assert!(view.resume_token(&slow).is_none());
        let json = serde_json::to_string(&view).unwrap();
        assert_eq!(
            json.matches(serde_json::to_string(&wait_tok).unwrap().as_str())
                .count(),
            1,
            "{json}"
        );
        assert!(
            !json.contains(&serde_json::to_string(&run_tok).unwrap()),
            "Running resume token leaked: {json}"
        );
        assert!(
            json.contains("\"kind\":\"waiting\"") && json.contains("\"kind\":\"running\""),
            "DTO wire, not cloned kernel enum: {json}"
        );
        assert!(matches!(
            view.node(&slow).unwrap().state,
            InspectNodeState::Running { .. }
        ));
        assert!(
            !json.contains("\"output\""),
            "Running/Waiting inspect JSON must not carry an output field: {json}"
        );
    }

    #[test]
    fn inspect_view_json_succeeded_owns_output() {
        let id = keel_rt::ExecutionId::parse("exec-out").unwrap();
        let write = keel_rt::NodeId::new("write");
        let slow = keel_rt::NodeId::new("slow");
        let run_tok = ResumeToken::issue(id.clone(), slow.clone(), 1);
        let mut snap = ExecutionSnapshot {
            schema_version: keel_rt::SCHEMA_VERSION,
            revision: 1,
            execution_id: id,
            workflow_id: keel_rt::WorkflowId::new("wf"),
            state: ExecutionState::Running,
            nodes: Default::default(),
            node_order: vec![write.clone(), slow.clone()],
            definition_hash: Default::default(),
        };
        snap.nodes.insert(
            write.clone(),
            keel_rt::NodeSnapshot {
                state: NodeState::Succeeded,
                output: Some(bytes::Bytes::from_static(b"human-ok")),
                attempt: 1,
                resume_token: None,
                last_error: None,
            },
        );
        snap.nodes.insert(
            slow.clone(),
            keel_rt::NodeSnapshot {
                state: NodeState::Running { attempt: 1 },
                output: Some(bytes::Bytes::from_static(b"ghost")),
                attempt: 1,
                resume_token: Some(run_tok),
                last_error: None,
            },
        );
        let view = InspectView::from_snapshot(&snap);
        assert_eq!(
            view.node(&write).map(|n| &n.state),
            Some(&InspectNodeState::Succeeded {
                output: bytes::Bytes::from_static(b"human-ok"),
            })
        );
        assert!(matches!(
            view.node(&slow).unwrap().state,
            InspectNodeState::Running { .. }
        ));
        let json = serde_json::to_value(&view).unwrap();
        let write_json = json["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["id"] == "write")
            .cloned()
            .unwrap();
        let slow_json = json["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["id"] == "slow")
            .cloned()
            .unwrap();
        assert_eq!(write_json["state"]["kind"], "succeeded");
        assert!(
            write_json["state"]["output"].is_string(),
            "Succeeded output is one base64 field, not a u8 array: {write_json}"
        );
        assert!(
            !write_json["state"]["output"].is_array(),
            "u8-array output is a JSON bomb: {write_json}"
        );
        let back: InspectView = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(
            back.node(&write).map(|n| &n.state),
            Some(&InspectNodeState::Succeeded {
                output: bytes::Bytes::from_static(b"human-ok"),
            })
        );
        assert!(write_json["state"].get("token").is_none());
        assert!(
            slow_json["state"].get("output").is_none(),
            "Running must not own output: {slow_json}"
        );
        assert!(slow_json["state"].get("token").is_none());
        assert!(slow_json.get("last_error").is_none());
        assert!(slow_json.get("resume_token").is_none());
        assert!(slow_json["state"].get("last_error").is_none());
        assert!(slow_json["state"].get("resume_token").is_none());
    }

    #[test]
    fn inspect_view_json_non_succeeded_has_no_output_or_error() {
        let id = keel_rt::ExecutionId::parse("exec-leak").unwrap();
        let tok = ResumeToken::issue(id.clone(), keel_rt::NodeId::new("n"), 1);
        let fat = bytes::Bytes::from(vec![9u8; 256]);
        let err = keel_rt::NodeError::new("boom");
        let states = [
            NodeState::Pending,
            NodeState::Ready { runnable_at: None },
            NodeState::Running { attempt: 1 },
            NodeState::Waiting {
                token: tok.clone(),
                attempt: 1,
            },
            NodeState::Failed,
            NodeState::Cancelled,
            NodeState::TimedOut,
        ];
        for (i, state) in states.into_iter().enumerate() {
            let nid = keel_rt::NodeId::new(format!("n{i}"));
            let mut snap = ExecutionSnapshot {
                schema_version: keel_rt::SCHEMA_VERSION,
                revision: 1,
                execution_id: id.clone(),
                workflow_id: keel_rt::WorkflowId::new("wf"),
                state: ExecutionState::Running,
                nodes: Default::default(),
                node_order: vec![nid.clone()],
                definition_hash: Default::default(),
            };
            snap.nodes.insert(
                nid.clone(),
                keel_rt::NodeSnapshot {
                    state,
                    output: Some(fat.clone()),
                    attempt: 1,
                    resume_token: Some(tok.clone()),
                    last_error: Some(err.clone()),
                },
            );
            let view = InspectView::from_snapshot(&snap);
            let node_json = serde_json::to_value(&view.node(&nid).unwrap().state).unwrap();
            if matches!(
                view.node(&nid).unwrap().state,
                InspectNodeState::Waiting { .. }
            ) {
                assert!(node_json.get("token").is_some(), "{node_json}");
            } else {
                assert!(node_json.get("token").is_none(), "{node_json}");
            }
            assert!(
                node_json.get("output").is_none(),
                "non-Succeeded must not own output: {node_json}"
            );
            assert!(
                node_json.get("last_error").is_none() && node_json.get("resume_token").is_none(),
                "inspect must not dump last_error/resume_token: {node_json}"
            );
            let wire = serde_json::to_string(&view).unwrap();
            assert!(!wire.contains("boom"), "Failed reason leaked: {wire}");
            assert!(
                !wire.contains("[9,") && !wire.contains("[9,"),
                "fat snapshot output leaked as array: {wire}"
            );
        }
    }

    #[test]
    fn inspect_view_json_1mib_succeeded_is_compact_base64() {
        let id = keel_rt::ExecutionId::parse("exec-fat").unwrap();
        let write = keel_rt::NodeId::new("write");
        let fat = bytes::Bytes::from(vec![0x5au8; 1024 * 1024]);
        let mut snap = ExecutionSnapshot {
            schema_version: keel_rt::SCHEMA_VERSION,
            revision: 1,
            execution_id: id,
            workflow_id: keel_rt::WorkflowId::new("wf"),
            state: ExecutionState::Succeeded,
            nodes: Default::default(),
            node_order: vec![write.clone()],
            definition_hash: Default::default(),
        };
        snap.nodes.insert(
            write.clone(),
            keel_rt::NodeSnapshot {
                state: NodeState::Succeeded,
                output: Some(fat.clone()),
                attempt: 1,
                resume_token: None,
                last_error: None,
            },
        );
        let view = InspectView::from_snapshot(&snap);
        let json = serde_json::to_vec(&view).expect("json");
        assert!(
            json.len() < 1024 * 1024 * 4 / 3 + 4096,
            "1MiB Succeeded must not explode as a u8 JSON array: {} B",
            json.len()
        );
        let text = String::from_utf8_lossy(&json);
        assert!(
            !text.contains("[90,") && !text.contains("[90,"),
            "Succeeded output must not be a number array"
        );
        let back: InspectView = serde_json::from_slice(&json).unwrap();
        match &back.node(&write).unwrap().state {
            InspectNodeState::Succeeded { output } => assert_eq!(output, &fat),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn claimed_elsewhere_status_is_423_locked_not_409() {
        assert_eq!(CLAIMED_ELSEWHERE, 423);
        assert_eq!(CLAIMED_ELSEWHERE, StatusCode::LOCKED.as_u16());
        assert_ne!(CLAIMED_ELSEWHERE, StatusCode::BAD_REQUEST.as_u16());
        assert_ne!(CLAIMED_ELSEWHERE, StatusCode::CONFLICT.as_u16());
        let src = include_str!("lib.rs");
        assert!(src.contains("CompleteError::ClaimedElsewhere"));
        assert!(src.contains("StatusCode::LOCKED"));
        assert!(src.contains("claimed_elsewhere"));
    }

    #[test]
    fn resume_token_helper_reads_waiting_state() {
        let id = keel_rt::ExecutionId::parse("exec-1").unwrap();
        let hold = keel_rt::NodeId::new("hold");
        let token = ResumeToken::issue(id.clone(), hold.clone(), 1);
        let view = InspectView {
            execution_id: id,
            state: ExecutionState::Waiting,
            nodes: vec![InspectNode {
                id: hold.clone(),
                state: InspectNodeState::Waiting {
                    token: token.clone(),
                    attempt: 1,
                },
            }],
        };
        assert_eq!(view.resume_token(&hold), Some(&token));
        let json = serde_json::to_value(&view).unwrap();
        assert!(json["nodes"][0].get("resume_token").is_none());
        let back: InspectView = serde_json::from_value(json).unwrap();
        assert_eq!(back.resume_token(&hold), Some(&token));
        assert!(back.resume_token(&keel_rt::NodeId::new("slow")).is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drop_held_if_terminal_keeps_waiting() {
        let rt = Runtime::builder().build();
        let started = Mutex::new(Vec::new());
        let handle = rt
            .start(
                WorkflowDefinition::builder("wf")
                    .node("hold", "wait")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let id = handle.execution_id().clone();
        assert_eq!(handle.wait_stable().await, ExecutionState::Waiting);
        let snap = handle.inspect().await;
        assert!(!snap.state.is_terminal());
        let token = snap
            .node(&keel_rt::NodeId::new("hold"))
            .and_then(|n| n.resume_token.clone())
            .unwrap();
        hold_if_live(&started, handle, snap.state.is_terminal());
        drop_held_if_terminal(&started, &id, snap.state.is_terminal());
        assert_eq!(started.lock().unwrap().len(), 1, "Waiting stays held");
        rt.complete(
            token,
            Resume::Complete(keel_rt::NodeOutcome::Succeeded(bytes::Bytes::from_static(
                b"ok",
            ))),
        )
        .await
        .unwrap();
        let done = rt.inspect(&id).await.unwrap();
        drop_held_if_terminal(&started, &id, done.state.is_terminal());
        assert_eq!(
            started.lock().unwrap().len(),
            0,
            "terminal snapshot drops the handle; Running/Waiting must not"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn hold_if_live_skips_terminal_snapshot() {
        let rt = Runtime::builder()
            .register_fn("next", |_ctx: keel_rt::ExecutionContext| async {
                keel_rt::NodeOutcome::Succeeded(bytes::Bytes::from_static(b"ok"))
            })
            .build();
        let started = Mutex::new(Vec::new());
        let handle = rt
            .start(
                WorkflowDefinition::builder("wf")
                    .node("next", "next")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let id = handle.execution_id().clone();
        let mut snap = handle.inspect().await;
        for _ in 0..64 {
            if snap.state.is_terminal() {
                break;
            }
            tokio::task::yield_now().await;
            snap = handle.inspect().await;
        }
        assert!(snap.state.is_terminal(), "instant node must finish");
        hold_if_live(&started, handle, snap.state.is_terminal());
        assert_eq!(started.lock().unwrap().len(), 0);
        assert_eq!(
            rt.inspect(&id).await.unwrap().state,
            ExecutionState::Succeeded
        );
    }

    /// N finished handles forced into the vec (start raced before terminal)
    /// plus one park. Snapshot-only reap leaves only the park.
    #[tokio::test(flavor = "current_thread")]
    async fn reap_held_terminals_clears_n_finished_keeps_park() {
        const N: usize = 8;
        let rt = Runtime::builder()
            .register_fn("next", |_ctx: keel_rt::ExecutionContext| async {
                keel_rt::NodeOutcome::Succeeded(bytes::Bytes::from_static(b"ok"))
            })
            .build();
        let started = Mutex::new(Vec::new());
        let mut finished = Vec::new();
        for _ in 0..N {
            let handle = rt
                .start(
                    WorkflowDefinition::builder("wf")
                        .node("next", "next")
                        .build()
                        .unwrap(),
                )
                .unwrap();
            finished.push(handle.execution_id().clone());
            started
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(handle);
        }
        for id in &finished {
            let mut snap = rt.inspect(id).await.unwrap();
            for _ in 0..256 {
                if snap.state.is_terminal() {
                    break;
                }
                tokio::task::yield_now().await;
                snap = rt.inspect(id).await.unwrap();
            }
            assert!(snap.state.is_terminal(), "instant node must finish");
        }
        let park = rt
            .start(
                WorkflowDefinition::builder("park")
                    .node("hold", "wait")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let park_id = park.execution_id().clone();
        assert_eq!(park.wait_stable().await, ExecutionState::Waiting);
        started.lock().unwrap_or_else(|p| p.into_inner()).push(park);
        reap_held_terminals(&rt, &started).await;
        {
            let held = started.lock().unwrap();
            assert_eq!(held.len(), 1, "N terminals reaped; live park stays");
            assert_eq!(held[0].execution_id(), &park_id);
        }
        for id in &finished {
            assert_eq!(
                rt.inspect(id).await.unwrap().state,
                ExecutionState::Succeeded,
                "reap Drop of a terminal handle must not Cancel"
            );
        }
        assert_eq!(
            rt.inspect(&park_id).await.unwrap().state,
            ExecutionState::Waiting
        );
    }
}
