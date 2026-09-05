//! Out-of-process start, inspect, complete, and cancel. POSTs the same JSON
//! [`CompleteBody`] the server already accepts. Not a second token type.

use crate::{
    CompleteBody, CompleteSecret, ExecutorsView, InspectView, OutputBody, StartBody, StartView,
    TokenBody, UnregisteredBody, CLAIMED_ELSEWHERE, SECRET_HEADER,
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{Method, Request, Uri};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use keel_rt::{ExecutionId, ExecutorId, NodeOutcome, Resume, ResumeToken};
use std::fmt;
use std::time::Duration;
use thiserror::Error;

/// Bound for one start, inspect, complete, or cancel request. A hung server is
/// [`KeelClientError::Hung`], not a forever wait. Tokio time.
pub const HANG_BOUND: Duration = Duration::from_secs(5);

/// Thin mapping onto [`Resume`]. Not a second state machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    Complete(Bytes),
    Fail,
    Reinvoke,
}

impl From<Decision> for Resume {
    fn from(decision: Decision) -> Self {
        match decision {
            Decision::Complete(bytes) => Resume::Complete(NodeOutcome::Succeeded(bytes)),
            Decision::Fail => Resume::Complete(NodeOutcome::failed("failed")),
            Decision::Reinvoke => Resume::Reinvoke,
        }
    }
}

/// Errors from [`KeelClient::start`], [`KeelClient::inspect`],
/// [`KeelClient::complete`], and [`KeelClient::cancel`]. Status map matches
/// the server. Display is verb-neutral for shared codes.
#[derive(Debug, Error)]
pub enum KeelClientError {
    #[error("request rejected: missing or wrong secret")]
    Unauthorized,
    #[error("unknown resume token")]
    UnknownToken,
    #[error("unknown execution")]
    UnknownExecution,
    #[error("token belongs to a cancelled execution")]
    Cancelled,
    #[error("request body exceeds server limit")]
    PayloadTooLarge,
    #[error("request rejected")]
    BadRequest,
    #[error(
        "unregistered executors: {}",
        .executors.iter().map(|e| e.as_str()).collect::<Vec<_>>().join(", ")
    )]
    Unregistered { executors: Vec<ExecutorId> },
    #[error("execution claimed elsewhere")]
    ClaimedElsewhere,
    #[error("unexpected status {0}")]
    Unexpected(u16),
    #[error("request hung: no response within hang-bound")]
    Hung,
    #[error("transport: {0}")]
    Transport(String),
}

/// Out-of-process SDK client. This crate implements [`Self::start`],
/// [`Self::inspect`], [`Self::complete`], and [`Self::cancel`]. No schedule HTTP.
///
/// Sends [`SECRET_HEADER`] and `Authorization: Bearer` when constructed
/// with a secret. Duplicate complete is Ok (server 200 noop).
/// Cancel of an already-terminal run is Ok (kernel noop). Does not revive
/// Cancelled. Does not follow redirects.
/// Encoded request bodies over [`crate::MAX_BODY`] are rejected before sending.
#[derive(Clone)]
pub struct KeelClient {
    http: Client<HttpConnector, Full<Bytes>>,
    base: String,
    secret: Option<CompleteSecret>,
    hang_bound: Duration,
}

impl KeelClient {
    /// `base_url` is the server root (e.g. `http://127.0.0.1:port`), not a route.
    pub fn new(base_url: impl AsRef<str>, secret: CompleteSecret) -> Result<Self, KeelClientError> {
        Self::build(base_url.as_ref(), Some(secret))
    }

    /// Omits the secret headers. A protected server answers 401.
    pub fn without_secret(base_url: impl AsRef<str>) -> Result<Self, KeelClientError> {
        Self::build(base_url.as_ref(), None)
    }

    fn build(base_url: &str, secret: Option<CompleteSecret>) -> Result<Self, KeelClientError> {
        Ok(Self {
            http: Client::builder(TokioExecutor::new()).build_http(),
            base: base_url.trim_end_matches('/').to_string(),
            secret,
            hang_bound: HANG_BOUND,
        })
    }

    /// One URL builder for start / inspect / complete / cancel. No verb-only field.
    fn uri(&self, path: &str) -> Result<Uri, KeelClientError> {
        path_url(&self.base, path)
            .parse::<Uri>()
            .map_err(|e| KeelClientError::Transport(e.to_string()))
    }

    /// Override [`HANG_BOUND`] for this client.
    pub fn hang_bound(mut self, bound: Duration) -> Self {
        self.hang_bound = bound;
        self
    }

    fn with_secret(
        &self,
        mut builder: hyper::http::request::Builder,
    ) -> hyper::http::request::Builder {
        if let Some(secret) = &self.secret {
            builder = builder
                .header(SECRET_HEADER, secret.as_str())
                .header(AUTHORIZATION, format!("Bearer {}", secret.as_str()));
        }
        builder
    }

    /// Headers + hang-bound. Callers map status.
    async fn send(
        &self,
        builder: hyper::http::request::Builder,
        body: Bytes,
    ) -> Result<hyper::Response<hyper::body::Incoming>, KeelClientError> {
        // The server can close while rejecting an unread oversized upload,
        // racing its 413 response with a transport error. Check wire bytes here.
        if body.len() > crate::MAX_BODY {
            return Err(KeelClientError::PayloadTooLarge);
        }
        let req = self
            .with_secret(builder)
            .body(Full::new(body))
            .map_err(|e| KeelClientError::Transport(e.to_string()))?;
        match tokio::time::timeout(self.hang_bound, self.http.request(req)).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) => Err(KeelClientError::Transport(e.to_string())),
            Err(_) => Err(KeelClientError::Hung),
        }
    }

    /// `POST /start` — same secret and hang-bound as inspect / complete.
    /// Returns the new [`ExecutionId`]. Each call is a new run.
    pub async fn start(
        &self,
        definition: impl Into<StartBody>,
    ) -> Result<ExecutionId, KeelClientError> {
        let json = serde_json::to_vec(&definition.into())
            .map_err(|e| KeelClientError::Transport(e.to_string()))?;
        let resp = self
            .send(
                Request::builder()
                    .method(Method::POST)
                    .uri(self.uri("start")?)
                    .header(CONTENT_TYPE, "application/json")
                    .header(CONTENT_LENGTH, json.len()),
                Bytes::from(json),
            )
            .await?;
        match resp.status().as_u16() {
            200 => {
                let bytes = resp
                    .into_body()
                    .collect()
                    .await
                    .map_err(|e| KeelClientError::Transport(e.to_string()))?
                    .to_bytes();
                let view: StartView = serde_json::from_slice(&bytes)
                    .map_err(|e| KeelClientError::Transport(e.to_string()))?;
                Ok(view.execution_id)
            }
            401 => Err(KeelClientError::Unauthorized),
            413 => Err(KeelClientError::PayloadTooLarge),
            400 => {
                let bytes = resp
                    .into_body()
                    .collect()
                    .await
                    .map_err(|e| KeelClientError::Transport(e.to_string()))?
                    .to_bytes();
                if let Ok(body) = serde_json::from_slice::<UnregisteredBody>(&bytes) {
                    if body.error == "unregistered" && !body.executors.is_empty() {
                        return Err(KeelClientError::Unregistered {
                            executors: body.executors,
                        });
                    }
                }
                Err(KeelClientError::BadRequest)
            }
            other => Err(KeelClientError::Unexpected(other)),
        }
    }

    /// `GET /executors` — ids this Runtime has registered (plus builtin `wait`).
    pub async fn executors(&self) -> Result<Vec<ExecutorId>, KeelClientError> {
        let resp = self
            .send(
                Request::builder()
                    .method(Method::GET)
                    .uri(self.uri("executors")?),
                Bytes::new(),
            )
            .await?;
        match resp.status().as_u16() {
            200 => {
                let bytes = resp
                    .into_body()
                    .collect()
                    .await
                    .map_err(|e| KeelClientError::Transport(e.to_string()))?
                    .to_bytes();
                let view: ExecutorsView = serde_json::from_slice(&bytes)
                    .map_err(|e| KeelClientError::Transport(e.to_string()))?;
                Ok(view.executors)
            }
            401 => Err(KeelClientError::Unauthorized),
            other => Err(KeelClientError::Unexpected(other)),
        }
    }

    /// `GET /inspect/:id` — same secret as complete. Hang-bound applies.
    /// Inspect JSON over [`crate::MAX_BODY`] is [`KeelClientError::PayloadTooLarge`].
    pub async fn inspect(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<InspectView, KeelClientError> {
        let resp = self
            .send(
                Request::builder()
                    .method(Method::GET)
                    .uri(self.uri(&format!("inspect/{}", execution_id.as_str()))?),
                Bytes::new(),
            )
            .await?;
        match resp.status().as_u16() {
            200 => {
                let bytes = resp
                    .into_body()
                    .collect()
                    .await
                    .map_err(|e| KeelClientError::Transport(e.to_string()))?
                    .to_bytes();
                serde_json::from_slice(&bytes)
                    .map_err(|e| KeelClientError::Transport(e.to_string()))
            }
            401 => Err(KeelClientError::Unauthorized),
            404 => Err(KeelClientError::UnknownExecution),
            413 => Err(KeelClientError::PayloadTooLarge),
            other => Err(KeelClientError::Unexpected(other)),
        }
    }

    /// POST the same [`CompleteBody`] the server already accepts.
    ///
    /// `resume` is [`Resume`] or [`Decision`].
    pub async fn complete(
        &self,
        token: ResumeToken,
        resume: impl Into<Resume>,
    ) -> Result<(), KeelClientError> {
        let json = serde_json::to_vec(&CompleteBody {
            token,
            resume: resume.into(),
        })
        .map_err(|e| KeelClientError::Transport(e.to_string()))?;
        self.post_apply("complete", json).await
    }

    /// `POST /approve` — `Decision::Complete(bytes)` through the same
    /// complete apply path as [`Self::complete`].
    pub async fn approve(
        &self,
        token: ResumeToken,
        output: impl Into<Bytes>,
    ) -> Result<(), KeelClientError> {
        let json = serde_json::to_vec(&OutputBody {
            token,
            output: Some(output.into()),
        })
        .map_err(|e| KeelClientError::Transport(e.to_string()))?;
        self.post_apply("approve", json).await
    }

    /// `POST /reject` — `Decision::Fail` through the same complete apply
    /// path (`NodeOutcome::failed("failed")`).
    pub async fn reject(&self, token: ResumeToken) -> Result<(), KeelClientError> {
        let json = serde_json::to_vec(&TokenBody { token })
            .map_err(|e| KeelClientError::Transport(e.to_string()))?;
        self.post_apply("reject", json).await
    }

    /// One POST + status map for complete / approve / reject. Same secret,
    /// hang-bound, and 409/423 codes — not a second state machine.
    async fn post_apply(&self, path: &str, json: Vec<u8>) -> Result<(), KeelClientError> {
        let resp = self
            .send(
                Request::builder()
                    .method(Method::POST)
                    .uri(self.uri(path)?)
                    .header(CONTENT_TYPE, "application/json")
                    .header(CONTENT_LENGTH, json.len()),
                Bytes::from(json),
            )
            .await?;
        match resp.status().as_u16() {
            200 => Ok(()),
            401 => Err(KeelClientError::Unauthorized),
            404 => Err(KeelClientError::UnknownToken),
            409 => Err(KeelClientError::Cancelled),
            413 => Err(KeelClientError::PayloadTooLarge),
            400 => Err(KeelClientError::BadRequest),
            CLAIMED_ELSEWHERE => Err(KeelClientError::ClaimedElsewhere),
            other => Err(KeelClientError::Unexpected(other)),
        }
    }

    /// `POST /cancel/:id` — same secret and hang-bound as start / inspect /
    /// complete. Cancels **one** execution via [`keel_rt::Runtime::cancel`].
    /// Already-terminal is Ok (kernel Cancel is a noop). Unknown id is 404.
    /// Another Runtime owning the run is [`KeelClientError::ClaimedElsewhere`].
    pub async fn cancel(&self, execution_id: &ExecutionId) -> Result<(), KeelClientError> {
        let resp = self
            .send(
                Request::builder()
                    .method(Method::POST)
                    .uri(self.uri(&format!("cancel/{}", execution_id.as_str()))?),
                Bytes::new(),
            )
            .await?;
        match resp.status().as_u16() {
            200 => Ok(()),
            401 => Err(KeelClientError::Unauthorized),
            404 => Err(KeelClientError::UnknownExecution),
            CLAIMED_ELSEWHERE => Err(KeelClientError::ClaimedElsewhere),
            other => Err(KeelClientError::Unexpected(other)),
        }
    }
}

impl fmt::Debug for KeelClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeelClient")
            .field("base", &self.base)
            .field("secret", &self.secret)
            .field("hang_bound", &self.hang_bound)
            .finish()
    }
}

fn path_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn encoded_body_limit_does_not_depend_on_an_http_response() {
        // No server can reject the body here. Oversize must be rejected locally;
        // bodies at or below the protocol limit must still attempt transport.
        let client = KeelClient::without_secret("http://127.0.0.1:0").unwrap();
        for size in [crate::MAX_BODY + 1, crate::MAX_BODY, crate::MAX_BODY - 1] {
            let result = client
                .send(
                    Request::builder()
                        .method(Method::POST)
                        .uri("http://127.0.0.1:0/complete"),
                    Bytes::from(vec![b' '; size]),
                )
                .await;
            if size > crate::MAX_BODY {
                assert!(
                    matches!(result, Err(KeelClientError::PayloadTooLarge)),
                    "{result:?}"
                );
            } else {
                assert!(
                    matches!(result, Err(KeelClientError::Transport(_))),
                    "{result:?}"
                );
            }
        }
    }

    #[test]
    fn decision_maps_onto_resume() {
        let bytes = Bytes::from_static(b"ok");
        assert_eq!(
            Resume::from(Decision::Complete(bytes.clone())),
            Resume::Complete(NodeOutcome::Succeeded(bytes))
        );
        assert_eq!(
            Resume::from(Decision::Fail),
            Resume::Complete(NodeOutcome::failed("failed"))
        );
        assert_eq!(Resume::from(Decision::Reinvoke), Resume::Reinvoke);
        assert_eq!(
            Resume::from(Decision::Fail),
            Resume::Complete(NodeOutcome::failed("failed")),
            "reject is Decision::Fail; no second fail contract"
        );
    }

    #[test]
    fn path_url_trims_trailing_slash() {
        assert_eq!(
            path_url("http://127.0.0.1:9", "complete"),
            "http://127.0.0.1:9/complete"
        );
        assert_eq!(
            path_url("http://127.0.0.1:9/", "/start"),
            "http://127.0.0.1:9/start"
        );
        assert_eq!(
            path_url("http://127.0.0.1:9/", "inspect/exec-1"),
            "http://127.0.0.1:9/inspect/exec-1"
        );
        assert_eq!(
            path_url("http://127.0.0.1:9/", "cancel/exec-1"),
            "http://127.0.0.1:9/cancel/exec-1"
        );
        assert_eq!(
            path_url("http://127.0.0.1:9/", "executors"),
            "http://127.0.0.1:9/executors"
        );
        assert_eq!(
            path_url("http://127.0.0.1:9/", "approve"),
            "http://127.0.0.1:9/approve"
        );
        assert_eq!(
            path_url("http://127.0.0.1:9/", "reject"),
            "http://127.0.0.1:9/reject"
        );
    }

    #[test]
    fn debug_redacts_secret() {
        let secret = CompleteSecret::new("s3cret").unwrap();
        let c = KeelClient::new("http://127.0.0.1:1", secret).unwrap();
        let dbg = format!("{c:?}");
        assert!(dbg.contains("CompleteSecret(..)"), "{dbg}");
        assert!(!dbg.contains("s3cret"), "{dbg}");
    }

    #[test]
    fn hang_bound_default_is_five_seconds() {
        assert_eq!(HANG_BOUND, Duration::from_secs(5));
        assert_eq!(crate::CLAIMED_ELSEWHERE, 423);
    }

    #[test]
    fn unregistered_display_names_ids() {
        let err = KeelClientError::Unregistered {
            executors: vec![keel_rt::ExecutorId::new("not-on-this-engine")],
        };
        let text = err.to_string();
        assert!(text.contains("not-on-this-engine"), "{text}");
        assert!(text.contains("unregistered"), "{text}");
    }

    #[test]
    fn inspect_shared_errors_display_does_not_say_complete() {
        for err in [
            KeelClientError::Unauthorized,
            KeelClientError::UnknownExecution,
            KeelClientError::Hung,
        ] {
            let text = err.to_string();
            assert!(
                !text.to_ascii_lowercase().contains("complete"),
                "inspect-shared error must be verb-neutral: {text}"
            );
        }
    }
}
