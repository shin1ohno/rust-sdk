//! Tower service implementation for the legacy MCP SSE transport.
//!
//! Implements the `event-stream` GET endpoint paired with a separate
//! POST endpoint per [MCP 2024-11-05 Transports — SSE](https://modelcontextprotocol.io/specification/2024-11-05/basic/transports/sse).
//! New deployments should prefer the Streamable HTTP transport
//! (`StreamableHttpService`); this module exists for compatibility with
//! clients that have not migrated.

use std::{
    collections::HashMap, convert::Infallible, fmt::Display, panic::AssertUnwindSafe, sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use futures::{FutureExt, StreamExt, future::BoxFuture, stream};
use http::{Method, Request, Response, StatusCode, header};
use http_body::Body;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use sse_stream::{KeepAlive, Sse, SseBody};
use tokio::sync::{RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::{CancellationToken, PollSender};
use tracing::Instrument;

use crate::{
    RoleServer,
    transport::{
        common::server_side_http::{
            BoxResponse, DEFAULT_AUTO_PING_INTERVAL, SessionId, TokioTimer, accepted_response,
            expect_json, internal_error_response, session_id,
        },
        sink_stream::SinkStreamTransport,
        streamable_http_server::tower::{host_is_allowed, normalize_authority},
    },
};

/// Default capacity of the server→SSE queue. Chosen to match the
/// streamable HTTP server's per-session bookkeeping; raise via
/// [`SseServerConfig::with_sse_outbound_buffer`] if your service emits
/// bursty server-initiated notifications.
const DEFAULT_SSE_OUTBOUND_BUFFER: usize = 32;
/// Default capacity of the POST→service queue.
const DEFAULT_POST_INBOUND_BUFFER: usize = 32;

/// Configuration for [`SseServer`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SseServerConfig {
    /// Path served by the SSE GET endpoint.
    ///
    /// Defaults to `/sse`. The path is matched as a literal — no path
    /// parameters or globs are interpreted.
    pub sse_path: String,
    /// Base path of the POST message endpoint.
    ///
    /// Defaults to `/message`. Each session-specific URL is built by
    /// appending `?session_id=<id>` to this path. The same string is sent
    /// back to the client in the initial `endpoint` event so the client
    /// knows where to POST follow-ups.
    pub post_path: String,
    /// Optional URL prefix prepended to the `endpoint` event payload.
    ///
    /// Useful when the SSE server runs behind a reverse proxy that
    /// strips or rewrites the path component. When `None`, the endpoint
    /// event uses [`Self::post_path`] as-is.
    pub endpoint_prefix: Option<String>,
    /// `Host` header values that the server is willing to accept (DNS
    /// rebinding protection — see RFC 9110 §7.2). Empty disables the
    /// check.
    pub allowed_hosts: Vec<String>,
    /// Keep-alive interval for SSE streams. The server emits a
    /// zero-byte SSE comment at this interval so intermediaries do not
    /// idle out the stream. `None` disables the keep-alive.
    pub keep_alive: Option<Duration>,
    /// Capacity of the per-session server→SSE queue.
    ///
    /// Defaults to 32. Raise for services that emit bursty
    /// server-initiated notifications faster than clients drain them.
    pub sse_outbound_buffer: usize,
    /// Capacity of the per-session POST→service queue.
    ///
    /// Defaults to 32. Raise if your POST handler may receive a burst
    /// of client requests faster than the service's protocol loop
    /// processes them.
    pub post_inbound_buffer: usize,
    /// Token used to terminate every active SSE stream cooperatively.
    pub cancellation_token: CancellationToken,
}

impl Default for SseServerConfig {
    fn default() -> Self {
        Self {
            sse_path: "/sse".into(),
            post_path: "/message".into(),
            endpoint_prefix: None,
            allowed_hosts: vec!["localhost".into(), "127.0.0.1".into(), "::1".into()],
            keep_alive: Some(DEFAULT_AUTO_PING_INTERVAL),
            sse_outbound_buffer: DEFAULT_SSE_OUTBOUND_BUFFER,
            post_inbound_buffer: DEFAULT_POST_INBOUND_BUFFER,
            cancellation_token: CancellationToken::new(),
        }
    }
}

impl SseServerConfig {
    /// Override the SSE GET path.
    pub fn with_sse_path(mut self, path: impl Into<String>) -> Self {
        self.sse_path = path.into();
        self
    }

    /// Override the POST message path.
    pub fn with_post_path(mut self, path: impl Into<String>) -> Self {
        self.post_path = path.into();
        self
    }

    /// Set a path prefix prepended to the `endpoint` event payload.
    pub fn with_endpoint_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.endpoint_prefix = Some(prefix.into());
        self
    }

    /// Replace the allow-list of acceptable `Host` header values.
    pub fn with_allowed_hosts(
        mut self,
        allowed_hosts: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.allowed_hosts = allowed_hosts.into_iter().map(Into::into).collect();
        self
    }

    /// Disable `Host` header validation entirely.
    ///
    /// **Warning** — every `Host` header is accepted, including
    /// arbitrary attacker-controlled values. Only call this from tests
    /// or when another layer (a reverse proxy allow-list, a Tailscale
    /// ACL, etc.) terminates the trust boundary.
    pub fn disable_allowed_hosts(mut self) -> Self {
        self.allowed_hosts.clear();
        self
    }

    /// Override the SSE keep-alive interval.
    pub fn with_keep_alive(mut self, interval: Option<Duration>) -> Self {
        self.keep_alive = interval;
        self
    }

    /// Override the per-session server→SSE queue capacity.
    pub fn with_sse_outbound_buffer(mut self, capacity: usize) -> Self {
        self.sse_outbound_buffer = capacity;
        self
    }

    /// Override the per-session POST→service queue capacity.
    pub fn with_post_inbound_buffer(mut self, capacity: usize) -> Self {
        self.post_inbound_buffer = capacity;
        self
    }

    /// Override the cancellation token. The token is forked per
    /// session, so cancelling it tears down every active SSE stream
    /// cooperatively.
    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = token;
        self
    }
}

/// Per-session entry tracked by [`SseServer`]'s session map.
struct SseSessionEntry {
    /// Channel that the POST handler uses to deliver client messages
    /// to the spawned service.
    inbound: mpsc::Sender<crate::model::ClientJsonRpcMessage>,
    /// Per-session cancellation token. Triggered when the SSE stream
    /// ends, the POST handler cannot deliver, or the service exits.
    cancel: CancellationToken,
}

type SessionMap = Arc<RwLock<HashMap<SessionId, SseSessionEntry>>>;

/// Tower service implementing the legacy MCP SSE transport.
///
/// `Debug` is intentionally not derived: `S` and `F` are unconstrained
/// generic parameters and rarely implement `Debug` themselves; the
/// service's interesting state is the session map, which is logged via
/// `tracing` instead.
#[non_exhaustive]
pub struct SseServer<S, F> {
    config: SseServerConfig,
    service_factory: Arc<F>,
    sessions: SessionMap,
    _phantom: std::marker::PhantomData<fn() -> S>,
}

impl<S, F> Clone for SseServer<S, F> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            service_factory: self.service_factory.clone(),
            sessions: self.sessions.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<S, F> SseServer<S, F>
where
    S: crate::Service<RoleServer> + Send + 'static,
    F: Fn() -> Result<S, std::io::Error> + Send + Sync + 'static,
{
    /// Build a new `SseServer`.
    pub fn new(service_factory: F, config: SseServerConfig) -> Self {
        Self {
            config,
            service_factory: Arc::new(service_factory),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            _phantom: std::marker::PhantomData,
        }
    }

    fn host_is_allowed(&self, headers: &http::HeaderMap) -> bool {
        let Some(authority) = headers
            .get(http::header::HOST)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| http::uri::Authority::try_from(h).ok())
            .map(|a| normalize_authority(a.host(), a.port_u16()))
        else {
            // No or invalid Host header — only allow when validation is
            // explicitly disabled.
            return self.config.allowed_hosts.is_empty();
        };
        host_is_allowed(&authority, &self.config.allowed_hosts)
    }

    async fn handle_get(self) -> Response<BoxBody<Bytes, Infallible>> {
        let sid: SessionId = session_id();
        let session_cancel = self.config.cancellation_token.child_token();

        let (in_tx, in_rx) =
            mpsc::channel::<crate::model::ClientJsonRpcMessage>(self.config.post_inbound_buffer);
        let (out_tx, out_rx) =
            mpsc::channel::<crate::model::ServerJsonRpcMessage>(self.config.sse_outbound_buffer);

        let service = match (self.service_factory)() {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "service factory failed");
                return internal_error_response("create service")(e);
            }
        };

        // Spawn a worker that drives the user-supplied service against
        // the in-memory transport. The cleanup guard ensures the
        // session is removed and downstream POSTs unblock no matter
        // how the spawned future exits — clean return, error, panic,
        // or cancellation.
        let stream = ReceiverStream::new(in_rx);
        use futures::SinkExt;
        let sink = PollSender::new(out_tx).sink_map_err(|_: tokio_util::sync::PollSendError<_>| {
            SseSendError("SSE outbound channel closed")
        });
        let transport = SinkStreamTransport::new(sink, stream);

        let span = tracing::info_span!("sse_session", session_id = %sid);
        let cleanup = SessionCleanup {
            sessions: self.sessions.clone(),
            session_id: sid.clone(),
            cancel: session_cancel.clone(),
        };
        tokio::spawn(
            async move {
                let _cleanup = cleanup; // hold across the await
                let serve = AssertUnwindSafe(async {
                    use crate::ServiceExt;
                    match service.serve(transport).await {
                        Ok(running) => match running.waiting().await {
                            Ok(_quit_reason) => {}
                            Err(e) => {
                                tracing::debug!(error = %e, "service waiting ended");
                            }
                        },
                        Err(e) => {
                            tracing::warn!(error = %e, "service::serve failed");
                        }
                    }
                });
                if let Err(panic) = serve.catch_unwind().await {
                    tracing::error!(?panic, "spawned SSE session task panicked");
                }
            }
            .instrument(span),
        );

        // Compose the SSE body: the `endpoint` event first (legacy SSE
        // spec uses a custom event name and a URL string, not JSON), then
        // forward each `ServerJsonRpcMessage` as `event: message`.
        let endpoint_sse = Sse::default()
            .event("endpoint")
            .data(endpoint_payload(&self.config, &sid));

        let outbound = ReceiverStream::new(out_rx).map(|msg| {
            let json = serde_json::to_string(&msg).unwrap_or_else(|_| "{}".into());
            Sse::default().event("message").data(json)
        });

        let cancel = session_cancel.clone();
        let frames = stream::once(async move { endpoint_sse })
            .chain(outbound)
            .map(Result::<Sse, Infallible>::Ok)
            .take_until(async move { cancel.cancelled().await });

        let body = SseBody::new(frames);
        let body = match self.config.keep_alive {
            Some(interval) => body
                .with_keep_alive::<TokioTimer>(KeepAlive::new().interval(interval))
                .boxed(),
            None => body.boxed(),
        };

        // Register the session AFTER spawning so the worker is guaranteed
        // to be live before any POST handler can find this entry.
        self.sessions.write().await.insert(
            sid,
            SseSessionEntry {
                inbound: in_tx,
                cancel: session_cancel,
            },
        );

        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream; charset=utf-8")
            .header(header::CACHE_CONTROL, "no-store")
            // Tell intermediaries (notably nginx) NOT to buffer the
            // streaming response. Without this, reverse proxies can
            // hold the initial `endpoint` event back until enough bytes
            // accumulate, causing the client to time out before
            // learning where to POST.
            .header("x-accel-buffering", "no")
            .body(body)
            .expect("valid response")
    }

    async fn handle_post<B>(self, sid: SessionId, body: B) -> Response<BoxBody<Bytes, Infallible>>
    where
        B: Body + Send + 'static,
        B::Error: Display,
    {
        let entry = match self.sessions.read().await.get(&sid) {
            Some(s) => SseSessionEntry {
                inbound: s.inbound.clone(),
                cancel: s.cancel.clone(),
            },
            None => {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::new(Bytes::from(format!("Session {sid} not found"))).boxed())
                    .expect("valid response");
            }
        };

        let message = match expect_json(body).await {
            Ok(m) => m,
            Err(resp) => return resp,
        };

        if entry.inbound.send(message).await.is_err() {
            // Receiver gone — service has exited. Tear down on this
            // path too so subsequent POSTs see a clean 410 Gone.
            entry.cancel.cancel();
            self.sessions.write().await.remove(&sid);
            return Response::builder()
                .status(StatusCode::GONE)
                .body(Full::new(Bytes::from("Session terminated")).boxed())
                .expect("valid response");
        }

        accepted_response()
    }
}

/// Build the `data:` payload of the initial `endpoint` event. Free
/// function so unit tests do not need to instantiate `SseServer`.
pub(crate) fn endpoint_payload(config: &SseServerConfig, sid: &SessionId) -> String {
    let prefix = config.endpoint_prefix.as_deref().unwrap_or("");
    format!("{prefix}{}?session_id={sid}", config.post_path)
}

/// Extract the `session_id` query parameter from a raw query string.
pub(crate) fn extract_session_id(query: Option<&str>) -> Option<SessionId> {
    let q = query?;
    for pair in q.split('&') {
        let mut split = pair.splitn(2, '=');
        let key = split.next()?;
        let val = split.next()?;
        if key == "session_id" {
            return Some(val.to_owned().into());
        }
    }
    None
}

/// Cleanup guard for a spawned session worker.
///
/// On drop — whether the worker returned cleanly, errored, panicked, or
/// was cancelled — cancels the per-session token (waking the SSE
/// stream's `take_until`) and removes the session entry from the
/// shared map (so subsequent POSTs see 404 Not Found rather than
/// hanging on a dead receiver).
struct SessionCleanup {
    sessions: SessionMap,
    session_id: SessionId,
    cancel: CancellationToken,
}

impl Drop for SessionCleanup {
    fn drop(&mut self) {
        self.cancel.cancel();
        let sessions = self.sessions.clone();
        let sid = self.session_id.clone();
        // Removal needs the async write lock; spawn a one-shot task.
        // Synchronous drop should not block on tokio internals.
        tokio::spawn(async move {
            sessions.write().await.remove(&sid);
        });
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct SseSendError(&'static str);

impl<RequestBody, S, F> tower_service::Service<Request<RequestBody>> for SseServer<S, F>
where
    RequestBody: Body + Send + 'static,
    RequestBody::Error: Display,
    RequestBody::Data: Send + 'static,
    S: crate::Service<RoleServer> + Send + 'static,
    F: Fn() -> Result<S, std::io::Error> + Send + Sync + 'static,
{
    type Response = BoxResponse;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn call(&mut self, req: Request<RequestBody>) -> Self::Future {
        let this = self.clone();
        Box::pin(async move {
            // DNS rebinding protection.
            if !this.host_is_allowed(req.headers()) {
                return Ok(Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .body(Full::new(Bytes::from("Forbidden Host")).boxed())
                    .expect("valid response"));
            }
            let method = req.method().clone();
            let path = req.uri().path().to_owned();
            let query = req.uri().query().map(str::to_owned);
            if method == Method::GET && path == this.config.sse_path {
                return Ok(this.handle_get().await);
            }
            if method == Method::POST && path == this.config.post_path {
                let Some(sid) = extract_session_id(query.as_deref()) else {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Full::new(Bytes::from("missing session_id")).boxed())
                        .expect("valid response"));
                };
                return Ok(this.handle_post(sid, req.into_body()).await);
            }
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from("Not Found")).boxed())
                .expect("valid response"))
        })
    }

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_sid() -> SessionId {
        "abc".to_string().into()
    }

    #[test]
    fn endpoint_payload_includes_session_id() {
        let cfg = SseServerConfig::default();
        assert_eq!(
            endpoint_payload(&cfg, &sample_sid()),
            "/message?session_id=abc"
        );
    }

    #[test]
    fn endpoint_payload_uses_prefix() {
        let cfg = SseServerConfig::default().with_endpoint_prefix("/roon");
        assert_eq!(
            endpoint_payload(&cfg, &sample_sid()),
            "/roon/message?session_id=abc"
        );
    }

    #[test]
    fn endpoint_payload_uses_custom_post_path() {
        let cfg = SseServerConfig::default().with_post_path("/messages");
        assert_eq!(
            endpoint_payload(&cfg, &sample_sid()),
            "/messages?session_id=abc"
        );
    }

    #[test]
    fn extract_session_id_parses_query() {
        assert_eq!(
            extract_session_id(Some("foo=1&session_id=abc&bar=2")).as_deref(),
            Some("abc")
        );
        assert!(extract_session_id(Some("foo=1")).is_none());
        assert!(extract_session_id(None).is_none());
    }

    #[test]
    fn extract_session_id_handles_first_pair() {
        assert_eq!(
            extract_session_id(Some("session_id=first")).as_deref(),
            Some("first")
        );
    }

    #[test]
    fn config_setters_round_trip() {
        let token = CancellationToken::new();
        let cfg = SseServerConfig::default()
            .with_sse_path("/x")
            .with_post_path("/y/")
            .with_endpoint_prefix("/svc")
            .with_allowed_hosts(["example.com"])
            .with_keep_alive(Some(Duration::from_secs(5)))
            .with_sse_outbound_buffer(64)
            .with_post_inbound_buffer(8);
        assert_eq!(cfg.sse_path, "/x");
        assert_eq!(cfg.post_path, "/y/");
        assert_eq!(cfg.endpoint_prefix.as_deref(), Some("/svc"));
        assert_eq!(cfg.allowed_hosts, vec!["example.com".to_string()]);
        assert_eq!(cfg.keep_alive, Some(Duration::from_secs(5)));
        assert_eq!(cfg.sse_outbound_buffer, 64);
        assert_eq!(cfg.post_inbound_buffer, 8);
        // Token field still present (used by Service); silence unused.
        let _ = token;
    }

    #[test]
    fn disable_allowed_hosts_clears_list() {
        let cfg = SseServerConfig::default()
            .with_allowed_hosts(["example.com", "example.org"])
            .disable_allowed_hosts();
        assert!(cfg.allowed_hosts.is_empty());
    }

    #[test]
    fn with_keep_alive_none_disables() {
        let cfg = SseServerConfig::default().with_keep_alive(None);
        assert!(cfg.keep_alive.is_none());
    }
}
