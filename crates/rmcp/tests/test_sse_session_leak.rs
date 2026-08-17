//! Regression tests for SSE session teardown on client disconnect
//! (`crates/rmcp/src/transport/sse_server`).
//!
//! A monitoring prober that opens `GET /sse`, reads the `endpoint`
//! event and hangs up is a perfectly ordinary client. Every one of
//! those must release its session: the spawned worker task has to
//! finish, the user-supplied service instance has to drop, and the
//! entry has to leave the session map. Otherwise a once-a-minute probe
//! accumulates ~1440 live sessions a day and the process grows without
//! bound.
//!
//! These tests assert teardown by observable effects only — the
//! service's `Drop` impl firing, and a later POST to the dead session
//! returning 404 — so they stay valid regardless of how the transport
//! tracks sessions internally.

#![cfg(not(feature = "local"))]
#![cfg(feature = "__reqwest")]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::StreamExt;
use rmcp::{
    ServerHandler,
    model::{Implementation, ServerCapabilities, ServerInfo},
    transport::sse_server::{SseServer, SseServerConfig},
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

/// Service whose `Drop` marks that the per-session worker task really
/// finished. The transport owns the only instance, so the counter going
/// up is proof the task (and everything it held) was released.
struct DropCountingServer {
    dropped: Arc<AtomicUsize>,
}

impl Drop for DropCountingServer {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl ServerHandler for DropCountingServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().build())
            .with_server_info(Implementation::new("sse-leak-test-server", "0.1.0"))
    }
}

/// Spawn an SSE server on an ephemeral port. Returns its base URL.
async fn spawn_test_server(ct: CancellationToken, dropped: Arc<AtomicUsize>) -> String {
    let svc = SseServer::new(
        move || {
            Ok(DropCountingServer {
                dropped: dropped.clone(),
            })
        },
        SseServerConfig::default()
            .with_cancellation_token(ct.child_token())
            // Random :PORT host header — disable host validation for tests.
            .disable_allowed_hosts()
            // Short keep-alive so a half-open stream is noticed quickly.
            .with_keep_alive(Some(Duration::from_millis(100))),
    );

    let router = axum::Router::new().fallback_service(svc);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().unwrap().port();

    let ct_serve = ct.clone();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move { ct_serve.cancelled().await })
            .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    format!("http://127.0.0.1:{port}")
}

/// Open `GET /sse`, read the `endpoint` event, then drop the response —
/// exactly what a liveness prober does. Returns the session's POST path.
async fn open_sse_then_hang_up(client: &reqwest::Client, base: &str) -> String {
    let resp = client
        .get(format!("{base}/sse"))
        .header("accept", "text/event-stream")
        .send()
        .await
        .expect("GET /sse");
    assert_eq!(resp.status(), 200, "SSE stream should open");

    let mut stream = resp.bytes_stream();
    let mut buf = String::new();
    let endpoint = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let chunk = match stream.next().await {
                Some(Ok(c)) => c,
                _ => return None,
            };
            buf.push_str(&String::from_utf8_lossy(&chunk));
            if let Some(line) = buf.lines().find(|l| l.starts_with("data: ")) {
                return Some(line.trim_start_matches("data: ").to_string());
            }
        }
    })
    .await
    .expect("timed out waiting for endpoint event")
    .expect("SSE stream ended before endpoint event");

    // Hang up: dropping the body closes the connection, which is all a
    // prober does after it has confirmed the handshake.
    drop(stream);
    endpoint
}

/// A client that opens an SSE stream and disconnects must release the
/// session: the worker task ends and the service instance drops.
#[tokio::test]
async fn sse_session_is_released_when_client_disconnects() {
    let ct = CancellationToken::new();
    let dropped = Arc::new(AtomicUsize::new(0));
    let base = spawn_test_server(ct.clone(), dropped.clone()).await;
    let client = reqwest::Client::new();

    let endpoint = open_sse_then_hang_up(&client, &base).await;

    // Give the server a bounded window to notice the hang-up.
    let released = wait_for(Duration::from_secs(10), || {
        dropped.load(Ordering::SeqCst) == 1
    })
    .await;
    assert!(
        released,
        "service instance was never dropped after the client disconnected — \
         the session worker task is still alive and leaking (dropped={})",
        dropped.load(Ordering::SeqCst)
    );

    // The session must also be gone from the routing table, so a late
    // POST is rejected rather than delivered to a dead worker.
    let status = client
        .post(format!("{base}{endpoint}"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"ping"}))
        .send()
        .await
        .expect("POST to dead session")
        .status();
    assert_eq!(
        status, 404,
        "POST to a disconnected session should be 404, got {status}"
    );

    ct.cancel();
}

/// The realistic failure shape: a prober reconnecting on a schedule.
/// Every cycle must be reclaimed, otherwise sessions accumulate one per
/// probe forever.
#[tokio::test]
async fn repeated_probe_cycles_do_not_accumulate_sessions() {
    const CYCLES: usize = 10;

    let ct = CancellationToken::new();
    let dropped = Arc::new(AtomicUsize::new(0));
    let base = spawn_test_server(ct.clone(), dropped.clone()).await;
    let client = reqwest::Client::new();

    for _ in 0..CYCLES {
        open_sse_then_hang_up(&client, &base).await;
    }

    let released = wait_for(Duration::from_secs(15), || {
        dropped.load(Ordering::SeqCst) == CYCLES
    })
    .await;
    assert!(
        released,
        "expected all {CYCLES} probe sessions to be reclaimed, but only {} were dropped",
        dropped.load(Ordering::SeqCst)
    );

    ct.cancel();
}

/// Poll `cond` until it holds or `limit` elapses.
async fn wait_for(limit: Duration, cond: impl Fn() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        if cond() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
