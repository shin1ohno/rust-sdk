//! Legacy MCP SSE transport server (MCP spec [2024-11-05]).
//!
//! This module re-implements the historical SSE transport that pairs
//! an `event-stream` GET endpoint with a separate POST `/message`
//! endpoint. It is provided for compatibility with clients that have
//! not migrated to the Streamable HTTP transport (notably the
//! Anthropic Claude.ai MCP connector at the time of writing). New
//! deployments should prefer [`StreamableHttpService`].
//!
//! # Protocol summary
//!
//! 1. The client opens an `EventSource` against the SSE GET endpoint
//!    (default `/sse`).
//! 2. The server immediately emits an `endpoint` event whose data is
//!    the POST URL for the new session, e.g.
//!    `/message?session_id=<uuid>`.
//! 3. JSON-RPC requests from the client arrive as `POST <endpoint>`
//!    with a JSON body. The server responds `202 Accepted` and
//!    forwards the response back over the SSE channel as `event:
//!    message`.
//! 4. The server may push unsolicited notifications (`event: message`,
//!    server-initiated requests, etc.) on the same SSE stream.
//!
//! # Usage
//!
//! [`SseServer`] implements [`tower_service::Service`] for
//! `http::Request<B>` so it can be plugged into any hyper-based HTTP
//! server:
//!
//! ```ignore
//! use std::sync::Arc;
//! use rmcp::transport::sse_server::{SseServer, SseServerConfig};
//!
//! let svc = SseServer::new(
//!     || Ok(MyMcpServer::default()),
//!     SseServerConfig::default(),
//! );
//!
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
//! loop {
//!     let (stream, _) = listener.accept().await?;
//!     let app = svc.clone();
//!     tokio::spawn(async move {
//!         let app = hyper::service::service_fn(move |req| {
//!             let mut svc = app.clone();
//!             async move { tower_service::Service::call(&mut svc, req).await }
//!         });
//!         let _ = hyper_util::server::conn::auto::Builder::new(
//!             hyper_util::rt::TokioExecutor::new(),
//!         )
//!         .serve_connection(hyper_util::rt::TokioIo::new(stream), app)
//!         .await;
//!     });
//! }
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! ```
//!
//! [2024-11-05]: https://modelcontextprotocol.io/specification/2024-11-05/basic/transports/sse
//! [`StreamableHttpService`]: super::streamable_http_server::StreamableHttpService

pub mod tower;
pub use self::tower::{SseServer, SseServerConfig};
