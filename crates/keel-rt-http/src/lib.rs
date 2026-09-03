//! Thin HTTP adapter. Another process starts a run, inspects for a wait
//! token, then POSTs that token + [`Resume`]. This process calls
//! [`Runtime::start`] / [`Runtime::inspect`] / [`Runtime::complete`].
//! No forms, no identity.
//!
//! A shared secret is required. Default bind is `127.0.0.1` only.
//! [`KeelClient::start`] + [`KeelClient::inspect`] + [`KeelClient::complete`]
//! is the out-of-process wait round-trip. Kernel `keel-rt` does not depend
//! on this crate.

mod client;

pub use client::{Decision, KeelClient, KeelClientError, HANG_BOUND};

use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Json;
use axum::Router;
use keel_rt::{
    CompleteError, ExecutionHandle, ExecutionId, ExecutionSnapshot, ExecutionState, ExecutorId,
    NodeId, NodeState, Resume, ResumeToken, Runtime, StartError, WorkflowDefinition, WorkflowId,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// Default listen address: loopback, ephemeral port. Never `0.0.0.0`.
pub const DEFAULT_BIND: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

/// JSON larger than this is not accepted. Oversized body does not call start
/// or complete.
pub const MAX_BODY: usize = 1024 * 1024;

/// `POST /complete` when another Runtime holds the execution
/// (`CompleteError::ClaimedElsewhere`). **423 Locked**. Not 400 (malformed
/// body) and not 409 (Cancelled). Body is `{"error":"claimed_elsewhere"}`.
pub const CLAIMED_ELSEWHERE: u16 = 423;

/// Shared-secret header (alternative to `Authorization: Bearer …`).
/// Same header on start, inspect, and complete. Wire name is historical.
pub const SECRET_HEADER: &str = "x-keel-complete";

/// Shared secret for the adapter. Not identity. Empty is rejected.
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

/// `POST /complete` body.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompleteBody {
    pub token: ResumeToken,
    pub resume: Resume,
}

/// Narrow start request. Not a [`WorkflowDefinition`] dump (no join /
/// on_failure / hash). Server builds the kernel definition with defaults.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartBody {
    pub workflow_id: WorkflowId,
    pub nodes: Vec<StartNode>,
    pub edges: Vec<StartEdge>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartNode {
    pub id: NodeId,
    pub executor_id: ExecutorId,
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
        Self {
            workflow_id: def.id().clone(),
            nodes: def
                .nodes()
                .iter()
                .map(|n| StartNode {
                    id: n.id.clone(),
                    executor_id: n.executor_id.clone(),
                })
                .collect(),
            edges: def
                .edges()
                .iter()
                .map(|e| StartEdge {
                    from: e.from.clone(),
                    to: e.to.clone(),
                })
                .collect(),
        }
    }
}

impl From<WorkflowDefinition> for StartBody {
    fn from(def: WorkflowDefinition) -> Self {
        Self::from(&def)
    }
}

impl StartBody {
    fn into_definition(self) -> Result<WorkflowDefinition, keel_rt::DefinitionError> {
        let mut b = WorkflowDefinition::builder(self.workflow_id);
        for n in self.nodes {
            b = b.node(n.id, n.executor_id);
        }
        for e in self.edges {
            b = b.edge(e.from, e.to);
        }
        b.build()
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
/// Running-node snapshot tokens, outputs, and last_error are not on this type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectNode {
    pub id: NodeId,
    pub state: InspectNodeState,
}

/// HTTP inspect state. Mapped from kernel [`NodeState`]; not kernel serde.
/// `Waiting` is the only variant that carries a token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InspectNodeState {
    Pending,
    Ready,
    Running { attempt: u32 },
    Waiting { token: ResumeToken, attempt: u32 },
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl InspectNodeState {
    fn from_kernel(state: &NodeState) -> Self {
        match state {
            NodeState::Pending => Self::Pending,
            NodeState::Ready { .. } => Self::Ready,
            NodeState::Running { attempt } => Self::Running { attempt: *attempt },
            NodeState::Waiting { token, attempt } => Self::Waiting {
                token: token.clone(),
                attempt: *attempt,
            },
            NodeState::Succeeded => Self::Succeeded,
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
                    state: InspectNodeState::from_kernel(&n.state),
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
/// `POST /complete`.
pub fn router(runtime: Arc<Runtime>, secret: CompleteSecret) -> Router {
    Router::new()
        .route("/start", post(start_handler))
        .route("/complete", post(complete_handler))
        .route("/inspect/:id", get(inspect_handler))
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
            app.started
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(handle);
            Json(StartView { execution_id }).into_response()
        }
        Err(StartError::UnregisteredExecutors(_)) => StatusCode::BAD_REQUEST.into_response(),
    }
}

async fn inspect_handler(
    State(app): State<App>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<InspectView>, StatusCode> {
    authorize(&app, &headers)?;
    let id = ExecutionId::parse(&id).map_err(|_| StatusCode::NOT_FOUND)?;
    match app.runtime.inspect(&id).await {
        Some(snap) => Ok(Json(InspectView::from_snapshot(&snap))),
        None => Err(StatusCode::NOT_FOUND),
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
    match app.runtime.complete(body.token, body.resume).await {
        Ok(()) => StatusCode::OK.into_response(),
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
    fn start_body_is_narrow_not_snapshot() {
        let def = WorkflowDefinition::builder("wf")
            .node("hold", "wait")
            .node("next", "next")
            .edge("hold", "next")
            .build()
            .unwrap();
        let body = StartBody::from(&def);
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["workflow_id"], "wf");
        assert!(json.get("on_failure").is_none());
        assert!(json["nodes"][0].get("join").is_none());
        assert!(json.get("nodes").unwrap().as_array().unwrap()[0]
            .get("state")
            .is_none());
        assert!(json.get("resume_token").is_none());
        let back = body.into_definition().unwrap();
        assert_eq!(back.id(), def.id());
        assert_eq!(back.nodes().len(), 2);
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
}
