use axum::{
    extract::{
        ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade},
        ConnectInfo, State,
    },
    http::StatusCode,
    response::IntoResponse,
};
use std::net::{IpAddr, SocketAddr};
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::{interval, timeout};
use tracing::{debug, info};

use crate::config::StreamConfig;
use crate::middleware::ConnectionLimiter;
use crate::models::PreSerializedMessage;

static HEARTBEAT_JSON: &str = r#"{"message_type":"heartbeat"}"#;

/// Disconnect policy for persistently-lagging clients. Lives in a module so
/// the const is reachable from tests AND so a future tweak goes through a
/// single named symbol instead of a scattered literal.
pub mod lag_policy {
    /// Number of consecutive `broadcast::Receiver::Lagged` events tolerated
    /// before the WebSocket connection is forcibly closed. Pre-1.5.0 this
    /// was effectively infinite — slow clients held FDs and skewed metrics
    /// indefinitely.
    pub const MAX_CONSECUTIVE_LAGS: u32 = 5;
}

pub struct AppState {
    pub tx: broadcast::Sender<Arc<PreSerializedMessage>>,
    pub connections: ConnectionCounter,
    pub limiter: Arc<ConnectionLimiter>,
    pub streams: Arc<StreamConfig>,
    pub stats: Arc<crate::api::ServerStats>,
}

#[derive(Default)]
pub struct ConnectionCounter {
    full: AtomicU64,
    lite: AtomicU64,
    domains: AtomicU64,
}

impl ConnectionCounter {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    fn increment(&self, stream_type: StreamType) {
        match stream_type {
            StreamType::Full => self.full.fetch_add(1, Ordering::Relaxed),
            StreamType::Lite => self.lite.fetch_add(1, Ordering::Relaxed),
            StreamType::DomainsOnly => self.domains.fetch_add(1, Ordering::Relaxed),
        };
        self.update_metrics();
    }

    #[inline]
    fn decrement(&self, stream_type: StreamType) {
        match stream_type {
            StreamType::Full => self.full.fetch_sub(1, Ordering::Relaxed),
            StreamType::Lite => self.lite.fetch_sub(1, Ordering::Relaxed),
            StreamType::DomainsOnly => self.domains.fetch_sub(1, Ordering::Relaxed),
        };
        self.update_metrics();
    }

    #[inline]
    fn update_metrics(&self) {
        let total = self.full.load(Ordering::Relaxed)
            + self.lite.load(Ordering::Relaxed)
            + self.domains.load(Ordering::Relaxed);
        metrics::gauge!("certstream_ws_connections_total").set(total as f64);
        metrics::gauge!("certstream_ws_connections_full").set(self.full.load(Ordering::Relaxed) as f64);
        metrics::gauge!("certstream_ws_connections_lite").set(self.lite.load(Ordering::Relaxed) as f64);
        metrics::gauge!("certstream_ws_connections_domains").set(self.domains.load(Ordering::Relaxed) as f64);
    }

    pub fn total(&self) -> u64 {
        self.full.load(Ordering::Relaxed)
            + self.lite.load(Ordering::Relaxed)
            + self.domains.load(Ordering::Relaxed)
    }
}

pub async fn handle_full_stream(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    let ip = addr.ip();
    if !state.limiter.try_acquire(ip) {
        return (StatusCode::TOO_MANY_REQUESTS, "Connection limit exceeded").into_response();
    }
    let rx = state.tx.subscribe();
    ws.on_upgrade(move |socket| handle_socket(socket, rx, StreamType::Full, state, ip))
        .into_response()
}

pub async fn handle_lite_stream(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    let ip = addr.ip();
    if !state.limiter.try_acquire(ip) {
        return (StatusCode::TOO_MANY_REQUESTS, "Connection limit exceeded").into_response();
    }
    let rx = state.tx.subscribe();
    ws.on_upgrade(move |socket| handle_socket(socket, rx, StreamType::Lite, state, ip))
        .into_response()
}

pub async fn handle_domains_only(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    let ip = addr.ip();
    if !state.limiter.try_acquire(ip) {
        return (StatusCode::TOO_MANY_REQUESTS, "Connection limit exceeded").into_response();
    }
    let rx = state.tx.subscribe();
    ws.on_upgrade(move |socket| handle_socket(socket, rx, StreamType::DomainsOnly, state, ip))
        .into_response()
}

#[derive(Clone, Copy)]
enum StreamType {
    Full,
    Lite,
    DomainsOnly,
}

async fn handle_socket(
    socket: WebSocket,
    mut rx: broadcast::Receiver<Arc<PreSerializedMessage>>,
    stream_type: StreamType,
    state: Arc<AppState>,
    client_ip: IpAddr,
) {
    let (mut sender, mut receiver) = socket.split();

    state.connections.increment(stream_type);
    let stream_name = match stream_type {
        StreamType::Full => "full",
        StreamType::Lite => "lite",
        StreamType::DomainsOnly => "domains",
    };

    info!(
        stream = stream_name,
        total = state.connections.total(),
        ip = %client_ip,
        "WS client connected"
    );

    // Outbound bytes are accumulated per connection and pushed to the shared
    // counter in batches. Doing it per frame would turn one broadcast into one
    // atomic write per subscriber, which is the one thing the pre-serialized
    // fan-out path is built to avoid.
    let mut pending_bytes: u64 = 0;
    let mut pending_frames: u32 = 0;
    const BYTES_FLUSH_FRAMES: u32 = 256;

    let mut heartbeat_interval = interval(Duration::from_secs(30));
    let mut ping_interval = interval(Duration::from_secs(15));
    let mut last_pong = std::time::Instant::now();
    let pong_timeout = Duration::from_secs(45);

    // After this many *consecutive* Lagged events the client is hopeless —
    // it's burning channel capacity without ever catching up, so we cut it.
    // Reset on every successful send / receive. Constant lives in
    // `lag_policy` so the regression test can lock it down — accidentally
    // bumping it to u32::MAX would silently disable the disconnect path.
    const MAX_CONSECUTIVE_LAGS: u32 = lag_policy::MAX_CONSECUTIVE_LAGS;
    // Outbound write deadline. A client whose TCP send buffer is full will
    // back-pressure axum's Sink and `sender.send().await` blocks indefinitely.
    // While blocked, this task can't drain `rx` or service pongs — the
    // broadcast Receiver keeps growing (other clients lag), the FD stays
    // open, and the connection counter is poisoned. 10 s is a generous
    // upper bound: any healthy client drains tens of KB in <1 s.
    const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
    let mut consecutive_lags: u32 = 0;

    // Helper: send with timeout. Returns false on send error OR timeout,
    // which the caller uses to break the loop.
    async fn send_with_deadline(
        sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
        msg: Message,
        deadline: Duration,
    ) -> bool {
        match timeout(deadline, sender.send(msg)).await {
            Ok(Ok(())) => true,
            Ok(Err(_)) => false,
            Err(_) => {
                // Write didn't complete within deadline → slow/dead client.
                metrics::counter!("certstream_ws_disconnect_write_timeout").increment(1);
                false
            }
        }
    }

    loop {
        tokio::select! {
            biased;

            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Ping(data))) => {
                        if !send_with_deadline(&mut sender, Message::Pong(data), WRITE_TIMEOUT).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        last_pong = std::time::Instant::now();
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }
                    _ => {}
                }
            }

            _ = ping_interval.tick() => {
                if last_pong.elapsed() > pong_timeout {
                    debug!(ip = %client_ip, "client pong timeout, disconnecting");
                    break;
                }
                if !send_with_deadline(&mut sender, Message::Ping(bytes::Bytes::new()), WRITE_TIMEOUT).await {
                    break;
                }
            }

            _ = heartbeat_interval.tick() => {
                // Text frame per certstream wire convention — JSON over WebSocket
                // is Text, not Binary. (Pre-1.5 sent this as Binary, which broke
                // strict clients that demuxed by frame type.)
                let hb = Message::Text(Utf8Bytes::from_static(HEARTBEAT_JSON));
                if !send_with_deadline(&mut sender, hb, WRITE_TIMEOUT).await {
                    break;
                }
            }

            result = rx.recv() => {
                match result {
                    Ok(msg) => {
                        // Payloads are pre-validated Utf8Bytes — cloning is a
                        // refcount bump on the shared Bytes, no per-client
                        // UTF-8 scan and no allocation.
                        let text = match stream_type {
                            StreamType::Full => msg.full.clone(),
                            StreamType::Lite => msg.lite.clone(),
                            StreamType::DomainsOnly => msg.domains_only.clone(),
                        };
                        let frame_len = text.len() as u64;
                        if !send_with_deadline(&mut sender, Message::Text(text), WRITE_TIMEOUT).await {
                            break;
                        }
                        pending_bytes += frame_len;
                        pending_frames += 1;
                        if pending_frames >= BYTES_FLUSH_FRAMES {
                            flush_bytes_sent(&state, &mut pending_bytes, &mut pending_frames);
                        }
                        consecutive_lags = 0;
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        debug!(lagged = n, ip = %client_ip, "client lagged, skipping messages");
                        metrics::counter!("certstream_ws_messages_lagged").increment(n);
                        consecutive_lags = consecutive_lags.saturating_add(1);
                        if consecutive_lags >= MAX_CONSECUTIVE_LAGS {
                            debug!(
                                ip = %client_ip,
                                lags = consecutive_lags,
                                "client persistently lagged, disconnecting"
                            );
                            metrics::counter!("certstream_ws_disconnect_lag").increment(1);
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        }
    }

    flush_bytes_sent(&state, &mut pending_bytes, &mut pending_frames);
    state.limiter.release(client_ip);
    state.connections.decrement(stream_type);
    info!(
        stream = stream_name,
        total = state.connections.total(),
        ip = %client_ip,
        "WS client disconnected"
    );
}

/// Push a connection's accumulated outbound bytes to the shared counter.
fn flush_bytes_sent(state: &AppState, pending_bytes: &mut u64, pending_frames: &mut u32) {
    if *pending_bytes == 0 {
        return;
    }
    state
        .stats
        .bytes_sent
        .fetch_add(*pending_bytes, Ordering::Relaxed);
    metrics::counter!("certstream_bytes_sent_total", "protocol" => "websocket")
        .increment(*pending_bytes);
    *pending_bytes = 0;
    *pending_frames = 0;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::PreSerializedMessage;
    use std::sync::Arc;
    use tokio::sync::broadcast;

    /// Lock down MAX_CONSECUTIVE_LAGS: an accidental bump to a very large
    /// value would silently disable the lag-disconnect path (#10).
    /// `const { assert!(..) }` instead of a runtime assertion so any future
    /// change has to be deliberate at the const-eval boundary.
    #[test]
    fn lag_policy_disconnect_threshold_is_sane() {
        const _: () = assert!(
            lag_policy::MAX_CONSECUTIVE_LAGS >= 1 && lag_policy::MAX_CONSECUTIVE_LAGS <= 16,
            "MAX_CONSECUTIVE_LAGS outside sane window [1, 16]"
        );
        // Runtime touch so the test still produces a result in the output.
        assert_eq!(lag_policy::MAX_CONSECUTIVE_LAGS, 5);
    }

    /// Reproduces the wire-level Lagged event the handle_socket loop relies
    /// on: a sender that overruns the channel capacity while a receiver
    /// doesn't drain must surface `RecvError::Lagged(n)` (not silently drop
    /// messages, not `Closed`). This is the upstream behaviour the
    /// disconnect logic counts on; if tokio ever changed it, the disconnect
    /// path would never fire.
    #[tokio::test]
    async fn broadcast_emits_lagged_when_receiver_falls_behind() {
        let (tx, mut rx) = broadcast::channel::<Arc<PreSerializedMessage>>(4);

        let dummy = || {
            Arc::new(PreSerializedMessage {
                full: Utf8Bytes::from_static("f"),
                lite: Utf8Bytes::from_static("l"),
                domains_only: Utf8Bytes::from_static("d"),
            })
        };

        // Push 16 messages into a 4-cap channel. The receiver never drains;
        // its next `recv().await` must therefore be Lagged.
        for _ in 0..16 {
            let _ = tx.send(dummy());
        }

        let err = rx
            .recv()
            .await
            .expect_err("receiver should report lag after overrun");
        match err {
            broadcast::error::RecvError::Lagged(n) => {
                assert!(n > 0, "Lagged(n) must report n>0; got {n}");
            }
            other => panic!("expected Lagged, got {other:?}"),
        }
    }

    /// Simulate the handle_socket lag-counter logic in isolation: feed
    /// successive Lagged errors and assert the disconnect threshold fires
    /// at exactly MAX_CONSECUTIVE_LAGS. Mirrors the conditional at
    /// lines ~272-285 without spinning up an actual WebSocket.
    #[test]
    fn lag_counter_disconnects_at_threshold() {
        let mut consecutive_lags: u32 = 0;
        let mut disconnected = false;
        for _ in 0..(lag_policy::MAX_CONSECUTIVE_LAGS as usize + 5) {
            // Mirror the Err arm of `rx.recv()` in handle_socket.
            consecutive_lags = consecutive_lags.saturating_add(1);
            if consecutive_lags >= lag_policy::MAX_CONSECUTIVE_LAGS {
                disconnected = true;
                break;
            }
        }
        assert!(disconnected, "loop must break by MAX_CONSECUTIVE_LAGS");
        assert_eq!(consecutive_lags, lag_policy::MAX_CONSECUTIVE_LAGS);
    }

    // handle_full_stream/handle_lite_stream/handle_domains_only all take a
    // WebSocketUpgrade extractor, which (unlike Query/State/ConnectInfo) has
    // no public constructor -- it's tied to a real HTTP upgrade handshake.
    // So this spins up a real axum server bound to an ephemeral port, in
    // this same test process (so it's still covered by cargo-llvm-cov,
    // unlike tests/server_e2e.rs's separate release-binary subprocess), and
    // drives it with a real WebSocket client.
    mod handle_socket_tests {
        use super::*;
        use crate::config::ConnectionLimitConfig;
        use axum::routing::get;
        use axum::Router;
        use tokio_tungstenite::tungstenite::Message as ClientMessage;

        fn test_app_state(limit: ConnectionLimitConfig) -> Arc<AppState> {
            let (tx, _rx) = broadcast::channel(16);
            Arc::new(AppState {
                tx,
                connections: ConnectionCounter::new(),
                limiter: ConnectionLimiter::new(limit, None),
                streams: Arc::new(StreamConfig::default()),
                stats: Arc::new(crate::api::ServerStats::new()),
            })
        }

        /// Starts a real server with the given route wired to `state` and
        /// returns its `ws://…` base URL. The listener task is detached
        /// (not joined) — it runs for the process's lifetime, which is fine
        /// for a short-lived test binary.
        async fn spawn_server(path: &'static str, handler: axum::routing::MethodRouter<Arc<AppState>>, state: Arc<AppState>) -> String {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind ephemeral port");
            let addr = listener.local_addr().unwrap();
            let app = Router::new().route(path, handler).with_state(state);
            tokio::spawn(async move {
                axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
                    .await
                    .ok();
            });
            format!("ws://127.0.0.1:{}{}", addr.port(), path)
        }

        /// Reads frames until it finds a Text frame with the given content,
        /// skipping anything else. `ping_interval`/`heartbeat_interval`
        /// both fire on their *first* tick immediately (tokio::interval's
        /// documented behavior), so a fresh connection sees a Ping and a
        /// heartbeat Text frame before whatever this test actually
        /// published -- skipping other Text frames too is what makes this
        /// robust to that, rather than asserting on frame position.
        async fn recv_text(
            ws: &mut (impl futures_util::Stream<
                Item = Result<ClientMessage, tokio_tungstenite::tungstenite::Error>,
            > + Unpin),
            expected: &str,
        ) {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                assert!(remaining > Duration::ZERO, "never saw {expected:?} within 5s");
                let frame = tokio::time::timeout(remaining, ws.next())
                    .await
                    .expect("no message within deadline")
                    .expect("stream ended")
                    .expect("frame error");
                if let ClientMessage::Text(text) = &frame
                    && text.as_str() == expected
                {
                    return;
                }
            }
        }

        fn dummy_message() -> Arc<PreSerializedMessage> {
            Arc::new(PreSerializedMessage {
                full: Utf8Bytes::from_static(r#"{"stream":"full"}"#),
                lite: Utf8Bytes::from_static(r#"{"stream":"lite"}"#),
                domains_only: Utf8Bytes::from_static(r#"{"stream":"domains"}"#),
            })
        }

        #[tokio::test]
        async fn lite_stream_delivers_a_broadcast_message() {
            let state = test_app_state(ConnectionLimitConfig::default());
            let url = spawn_server("/", get(handle_lite_stream), state.clone()).await;

            let (mut ws, response) = tokio_tungstenite::connect_async(&url)
                .await
                .expect("handshake");
            assert_eq!(response.status(), 101);

            // Give the server task a moment to reach `state.tx.subscribe()`
            // before publishing, or the message would have nowhere to go.
            tokio::time::sleep(Duration::from_millis(50)).await;
            state.tx.send(dummy_message()).ok();

            recv_text(&mut ws, r#"{"stream":"lite"}"#).await;

            ws.close(None).await.ok();
        }

        #[tokio::test]
        async fn full_stream_delivers_the_full_payload() {
            let state = test_app_state(ConnectionLimitConfig::default());
            let url = spawn_server("/", get(handle_full_stream), state.clone()).await;

            let (mut ws, _resp) = tokio_tungstenite::connect_async(&url).await.expect("handshake");
            tokio::time::sleep(Duration::from_millis(50)).await;
            state.tx.send(dummy_message()).ok();

            recv_text(&mut ws, r#"{"stream":"full"}"#).await;
            ws.close(None).await.ok();
        }

        #[tokio::test]
        async fn domains_only_stream_delivers_the_domains_payload() {
            let state = test_app_state(ConnectionLimitConfig::default());
            let url = spawn_server("/", get(handle_domains_only), state.clone()).await;

            let (mut ws, _resp) = tokio_tungstenite::connect_async(&url).await.expect("handshake");
            tokio::time::sleep(Duration::from_millis(50)).await;
            state.tx.send(dummy_message()).ok();

            recv_text(&mut ws, r#"{"stream":"domains"}"#).await;
            ws.close(None).await.ok();
        }

        #[tokio::test]
        async fn connection_limit_rejects_the_upgrade() {
            let state = test_app_state(ConnectionLimitConfig {
                enabled: true,
                max_connections: 0,
                per_ip_limit: None,
            });
            let url = spawn_server("/", get(handle_lite_stream), state).await;

            let err = tokio_tungstenite::connect_async(&url)
                .await
                .expect_err("handshake should be rejected");
            match err {
                tokio_tungstenite::tungstenite::Error::Http(resp) => {
                    assert_eq!(resp.status(), 429);
                }
                other => panic!("expected an HTTP error response, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn client_close_disconnects_and_releases_the_slot() {
            let state = test_app_state(ConnectionLimitConfig {
                enabled: true,
                max_connections: 10,
                per_ip_limit: None,
            });
            let url = spawn_server("/", get(handle_lite_stream), state.clone()).await;

            let (mut ws, _resp) = tokio_tungstenite::connect_async(&url).await.expect("handshake");
            // Let the server-side task register the connection.
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(state.connections.total(), 1);

            ws.send(ClientMessage::Close(None)).await.ok();
            drop(ws);

            // The server-side disconnect (limiter release, counter
            // decrement) happens asynchronously after the close frame is
            // processed.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while state.connections.total() != 0 {
                assert!(tokio::time::Instant::now() < deadline, "connection never cleaned up");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(state.limiter.current_connections(), 0);
        }
    }
}
