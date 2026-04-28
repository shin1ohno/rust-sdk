//! Integration tests for the legacy MCP SSE transport server
//! (`crates/rmcp/src/transport/sse_server`).
//!
//! Exercises the SSE handshake against a real
//! `tokio::net::TcpListener`:
//!
//! 1. `GET /sse` returns an `event: endpoint` frame whose data is the
//!    `/message?session_id=…` POST URL for this session.
//! 2. POSTing an `initialize` JSON-RPC to that URL drives the response
//!    out as an `event: message` frame on the SSE stream.
//! 3. Two concurrent SSE streams produce distinct `session_id` values.

#![cfg(not(feature = "local"))]
#![cfg(feature = "__reqwest")]

use std::time::Duration;

use futures::StreamExt;
use rmcp::{
    ServerHandler,
    model::{Implementation, ServerCapabilities, ServerInfo},
    transport::sse_server::{SseServer, SseServerConfig},
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default)]
struct TestServer;

impl ServerHandler for TestServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().build())
            .with_server_info(Implementation::new("sse-test-server", "0.1.0"))
    }
}

/// Spawn a fresh SSE server on `127.0.0.1:0`. Returns the bound base
/// URL (e.g. `http://127.0.0.1:54321`).
async fn spawn_test_server(ct: CancellationToken) -> String {
    let svc = SseServer::new(
        || Ok(TestServer),
        SseServerConfig::default()
            .with_cancellation_token(ct.child_token())
            // Random :PORT host header — disable host validation for tests.
            .disable_allowed_hosts(),
    );

    let router = axum::Router::new().fallback_service(svc);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().unwrap();
    let base = format!("http://127.0.0.1:{}", addr.port());

    let ct_serve = ct.clone();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move { ct_serve.cancelled().await })
            .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    base
}

/// Read SSE frames from `stream` (already opened) until one matches
/// `wanted_event` and `data_predicate`. Returns the matching `data:`
/// payload, or `None` on timeout / stream end.
async fn read_until<S>(
    stream: &mut S,
    buf: &mut String,
    wanted_event: &str,
    data_predicate: impl Fn(&str) -> bool,
    timeout: Duration,
) -> Option<String>
where
    S: futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin,
{
    tokio::time::timeout(timeout, async {
        loop {
            let chunk = match stream.next().await {
                Some(Ok(c)) => c,
                _ => return None,
            };
            buf.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(end) = buf
                .find("\n\n")
                .map(|i| i + 2)
                .or_else(|| buf.find("\r\n\r\n").map(|i| i + 4))
            {
                let frame: String = buf.drain(..end).collect();
                let mut event = "message";
                let mut data = String::new();
                for line in frame.lines() {
                    if let Some(v) = line.strip_prefix("event:") {
                        event = v.trim_start();
                    } else if let Some(v) = line.strip_prefix("data:") {
                        if !data.is_empty() {
                            data.push('\n');
                        }
                        data.push_str(v.trim_start());
                    }
                }
                if event == wanted_event && data_predicate(&data) {
                    return Some(data);
                }
            }
        }
    })
    .await
    .ok()
    .flatten()
}

/// Open `GET <base>/sse` and pull the first `endpoint` event.
/// Returns the response (still streaming subsequent frames) plus the
/// announced absolute messages URL.
async fn open_session(
    client: &reqwest::Client,
    base: &str,
) -> (
    impl futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin,
    String,
    String,
) {
    let resp = client
        .get(format!("{base}/sse"))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("GET /sse");
    assert!(
        resp.status().is_success(),
        "GET /sse returned {}",
        resp.status()
    );
    let mut stream = resp.bytes_stream();
    let mut buf = String::new();
    let endpoint = read_until(
        &mut stream,
        &mut buf,
        "endpoint",
        |_| true,
        Duration::from_secs(2),
    )
    .await
    .expect("first endpoint event");
    let absolute = if endpoint.starts_with("http") {
        endpoint.clone()
    } else {
        format!("{base}{endpoint}")
    };
    (stream, buf, absolute)
}

#[tokio::test]
async fn endpoint_event_announces_session_url() {
    let ct = CancellationToken::new();
    let base = spawn_test_server(ct.clone()).await;
    let client = reqwest::Client::new();

    let (_stream, _buf, endpoint) = open_session(&client, &base).await;
    assert!(
        endpoint.contains("/message"),
        "endpoint event {endpoint} should reference /message"
    );
    assert!(
        endpoint.contains("session_id="),
        "endpoint event {endpoint} should carry session_id"
    );
    ct.cancel();
}

#[tokio::test]
async fn initialize_round_trip_arrives_on_sse_stream() {
    let ct = CancellationToken::new();
    let base = spawn_test_server(ct.clone()).await;
    let client = reqwest::Client::new();

    let (mut stream, mut buf, post_url) = open_session(&client, &base).await;

    let resp = client
        .post(&post_url)
        .header("Content-Type", "application/json")
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "0.0" }
            }
        }))
        .send()
        .await
        .expect("POST initialize");
    assert_eq!(resp.status().as_u16(), 202, "POST should return 202");

    let payload = read_until(
        &mut stream,
        &mut buf,
        "message",
        |data| data.contains("\"id\":1") && data.contains("\"result\""),
        Duration::from_secs(2),
    )
    .await;
    assert!(
        payload.is_some(),
        "initialize response did not arrive on SSE stream"
    );
    ct.cancel();
}

#[tokio::test]
async fn concurrent_sessions_are_isolated() {
    let ct = CancellationToken::new();
    let base = spawn_test_server(ct.clone()).await;
    let client = reqwest::Client::new();

    let (_a, _ba, ep_a) = open_session(&client, &base).await;
    let (_b, _bb, ep_b) = open_session(&client, &base).await;

    let extract_sid = |url: &str| {
        url.split("session_id=")
            .nth(1)
            .map(|s| s.split('&').next().unwrap_or("").to_string())
            .unwrap_or_default()
    };
    let sid_a = extract_sid(&ep_a);
    let sid_b = extract_sid(&ep_b);
    assert!(!sid_a.is_empty());
    assert!(!sid_b.is_empty());
    assert_ne!(sid_a, sid_b, "concurrent sessions must have distinct ids");
    ct.cancel();
}
