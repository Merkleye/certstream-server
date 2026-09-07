use axum::{
    extract::{ConnectInfo, Query, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
};
use futures_util::StreamExt;
use serde::Deserialize;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::BroadcastStream;
use tracing::info;

use crate::middleware::ConnectionLimiter;
use crate::models::PreSerializedMessage;

static SSE_CONNECTION_COUNT: AtomicU64 = AtomicU64::new(0);

/// Issue #13: Copy enum — no heap allocation, no clone per message.
/// Previously `stream_type: String` was cloned into the closure on every received message.
#[derive(Clone, Copy)]
enum SseStreamType {
    Full,
    Lite,
    DomainsOnly,
}

impl SseStreamType {
    fn from_str(s: &str) -> Self {
        match s {
            "full" => Self::Full,
            "domains" | "domains-only" => Self::DomainsOnly,
            _ => Self::Lite,
        }
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct SseQueryParams {
    #[serde(default)]
    pub stream: Option<String>,
}

pub async fn handle_sse_stream(
    Query(params): Query<SseQueryParams>,
    State(state): State<Arc<crate::websocket::AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    let ip = addr.ip();

    if !state.limiter.try_acquire(ip) {
        return (StatusCode::TOO_MANY_REQUESTS, "Connection limit exceeded").into_response();
    }

    let stream_type = SseStreamType::from_str(params.stream.as_deref().unwrap_or("lite"));

    let stream_enabled = match stream_type {
        SseStreamType::Full => state.streams.full,
        SseStreamType::Lite => state.streams.lite,
        SseStreamType::DomainsOnly => state.streams.domains_only,
    };
    if !stream_enabled {
        state.limiter.release(ip);
        return (StatusCode::NOT_FOUND, "Stream type not available").into_response();
    }

    let rx = state.tx.subscribe();

    SSE_CONNECTION_COUNT.fetch_add(1, Ordering::Relaxed);
    update_sse_metrics();

    info!(
        stream = params.stream.as_deref().unwrap_or("lite"),
        total = SSE_CONNECTION_COUNT.load(Ordering::Relaxed),
        ip = %ip,
        "SSE client connected"
    );

    // Outbound bytes for this connection, accumulated here and pushed to the
    // shared counter in batches by the wrapper. Per-message writes to the
    // global counter would put every subscriber on the same cache line.
    let pending_bytes = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&pending_bytes);

    let stream = BroadcastStream::new(rx).filter_map(move |result| {
        // stream_type is Copy — captured by value, zero allocation per message.
        std::future::ready(match result {
            Ok(msg) => process_message(msg, stream_type, &counter),
            Err(_) => None,
        })
    });

    let stream = SseStreamWrapper {
        inner: Box::pin(stream),
        limiter: state.limiter.clone(),
        client_ip: ip,
        stats: state.stats.clone(),
        pending_bytes,
    };

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("heartbeat"),
    ).into_response()
}

fn process_message(
    msg: Arc<PreSerializedMessage>,
    stream_type: SseStreamType,
    pending_bytes: &AtomicU64,
) -> Option<Result<Event, std::convert::Infallible>> {
    let text = match stream_type {
        SseStreamType::Full => &msg.full,
        SseStreamType::DomainsOnly => &msg.domains_only,
        SseStreamType::Lite => &msg.lite,
    };
    pending_bytes.fetch_add(text.len() as u64, Ordering::Relaxed);

    // Payloads are pre-validated Utf8Bytes — no per-client UTF-8 scan here.
    // (Event::data still copies into the event's own buffer; that copy is
    // inherent to axum's SSE Event API.)
    Some(Ok(Event::default().data(text.as_str())))
}

struct SseStreamWrapper<S> {
    inner: std::pin::Pin<Box<S>>,
    limiter: Arc<ConnectionLimiter>,
    client_ip: IpAddr,
    stats: Arc<crate::api::ServerStats>,
    pending_bytes: Arc<AtomicU64>,
}

impl<S> SseStreamWrapper<S> {
    /// Move this connection's accumulated bytes into the shared counter.
    fn flush_bytes(&self) {
        let n = self.pending_bytes.swap(0, Ordering::Relaxed);
        if n > 0 {
            self.stats.bytes_sent.fetch_add(n, Ordering::Relaxed);
            metrics::counter!("certstream_bytes_sent_total", "protocol" => "sse").increment(n);
        }
    }
}

impl<S> Drop for SseStreamWrapper<S> {
    fn drop(&mut self) {
        self.flush_bytes();
        self.limiter.release(self.client_ip);
        SSE_CONNECTION_COUNT.fetch_sub(1, Ordering::Relaxed);
        update_sse_metrics();
        info!(
            total = SSE_CONNECTION_COUNT.load(Ordering::Relaxed),
            ip = %self.client_ip,
            "SSE client disconnected"
        );
    }
}

impl<S> futures_util::Stream for SseStreamWrapper<S>
where
    S: futures_util::Stream<Item = Result<Event, std::convert::Infallible>>,
{
    type Item = Result<Event, std::convert::Infallible>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let polled = self.inner.as_mut().poll_next(cx);
        // Long-lived connections would otherwise only report their bytes on
        // disconnect, which for SSE can be hours.
        const FLUSH_THRESHOLD_BYTES: u64 = 256 * 1024;
        if self.pending_bytes.load(Ordering::Relaxed) >= FLUSH_THRESHOLD_BYTES {
            self.flush_bytes();
        }
        polled
    }
}

fn update_sse_metrics() {
    metrics::gauge!("certstream_sse_connections")
        .set(SSE_CONNECTION_COUNT.load(Ordering::Relaxed) as f64);
}

pub fn sse_connection_count() -> u64 {
    SSE_CONNECTION_COUNT.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ServerStats;
    use crate::config::{ConnectionLimitConfig, StreamConfig};
    use crate::models::PreSerializedMessage;
    use crate::websocket::{AppState, ConnectionCounter};
    use axum::extract::ws::Utf8Bytes;
    use axum::extract::{ConnectInfo, Query, State};
    use futures_util::stream;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use tokio::sync::broadcast;

    fn addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345)
    }

    fn app_state(streams: StreamConfig) -> Arc<AppState> {
        let (tx, _rx) = broadcast::channel(16);
        Arc::new(AppState {
            tx,
            connections: ConnectionCounter::new(),
            limiter: ConnectionLimiter::new(ConnectionLimitConfig::default(), None),
            streams: Arc::new(streams),
            stats: Arc::new(ServerStats::new()),
        })
    }

    fn message() -> Arc<PreSerializedMessage> {
        Arc::new(PreSerializedMessage {
            full: Utf8Bytes::from_static(r#"{"stream":"full"}"#),
            lite: Utf8Bytes::from_static(r#"{"stream":"lite"}"#),
            domains_only: Utf8Bytes::from_static(r#"{"stream":"domains"}"#),
        })
    }

    #[test]
    fn stream_type_from_str() {
        assert!(matches!(SseStreamType::from_str("full"), SseStreamType::Full));
        assert!(matches!(SseStreamType::from_str("domains"), SseStreamType::DomainsOnly));
        assert!(matches!(
            SseStreamType::from_str("domains-only"),
            SseStreamType::DomainsOnly
        ));
        assert!(matches!(SseStreamType::from_str("lite"), SseStreamType::Lite));
        assert!(matches!(SseStreamType::from_str("anything-else"), SseStreamType::Lite));
        assert!(matches!(SseStreamType::from_str(""), SseStreamType::Lite));
    }

    #[test]
    fn process_message_selects_the_requested_stream_and_counts_bytes() {
        let msg = message();
        let counter = AtomicU64::new(0);

        let full = process_message(msg.clone(), SseStreamType::Full, &counter).unwrap();
        assert!(matches!(full, Ok(_)));
        assert_eq!(counter.load(Ordering::Relaxed), msg.full.len() as u64);

        let before = counter.load(Ordering::Relaxed);
        let _ = process_message(msg.clone(), SseStreamType::Lite, &counter).unwrap();
        assert_eq!(
            counter.load(Ordering::Relaxed),
            before + msg.lite.len() as u64
        );

        let before = counter.load(Ordering::Relaxed);
        let expected_len = msg.domains_only.len() as u64;
        let _ = process_message(msg, SseStreamType::DomainsOnly, &counter).unwrap();
        assert_eq!(counter.load(Ordering::Relaxed), before + expected_len);
    }

    #[tokio::test]
    async fn handle_sse_stream_rejects_over_the_connection_limit() {
        let (tx, _rx) = broadcast::channel(16);
        let state = Arc::new(AppState {
            tx,
            connections: ConnectionCounter::new(),
            // The limiter no-ops (try_acquire always true) unless enabled,
            // so a default config could never actually reject anything.
            limiter: ConnectionLimiter::new(
                ConnectionLimitConfig {
                    enabled: true,
                    max_connections: 1,
                    per_ip_limit: None,
                },
                None,
            ),
            streams: Arc::new(StreamConfig::default()),
            stats: Arc::new(ServerStats::new()),
        });
        // Exhaust the single connection slot before the handler ever gets a
        // chance.
        let ip = addr().ip();
        assert!(state.limiter.try_acquire(ip));
        assert!(!state.limiter.try_acquire(ip));

        let response = handle_sse_stream(
            Query(SseQueryParams { stream: None }),
            State(state),
            ConnectInfo(addr()),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn handle_sse_stream_404s_for_a_disabled_stream() {
        let state = app_state(StreamConfig {
            full: false,
            lite: true,
            domains_only: true,
        });

        let response = handle_sse_stream(
            Query(SseQueryParams {
                stream: Some("full".to_string()),
            }),
            State(state),
            ConnectInfo(addr()),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn handle_sse_stream_succeeds_for_an_enabled_stream() {
        let state = app_state(StreamConfig::default());

        let response = handle_sse_stream(
            Query(SseQueryParams {
                stream: Some("lite".to_string()),
            }),
            State(state),
            ConnectInfo(addr()),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn wrapper_flushes_pending_bytes_and_releases_its_slot_on_drop() {
        // Enabled, or try_acquire/current_connections are both no-ops (see
        // ConnectionLimiter::try_acquire's `if !config.enabled` short
        // circuit) and this test would prove nothing.
        let limiter = ConnectionLimiter::new(
            ConnectionLimitConfig {
                enabled: true,
                max_connections: 10,
                per_ip_limit: None,
            },
            None,
        );
        let ip = addr().ip();
        assert!(limiter.try_acquire(ip));
        assert_eq!(limiter.current_connections(), 1);

        let stats = Arc::new(ServerStats::new());
        let pending_bytes = Arc::new(AtomicU64::new(0));
        let before_count = sse_connection_count();
        SSE_CONNECTION_COUNT.fetch_add(1, Ordering::Relaxed);

        {
            let wrapper = SseStreamWrapper {
                inner: Box::pin(stream::empty::<Result<Event, std::convert::Infallible>>()),
                limiter: limiter.clone(),
                client_ip: ip,
                stats: stats.clone(),
                pending_bytes: pending_bytes.clone(),
            };
            pending_bytes.store(123, Ordering::Relaxed);
            drop(wrapper);
        }

        assert_eq!(stats.bytes_sent.load(Ordering::Relaxed), 123);
        assert_eq!(limiter.current_connections(), 0);
        assert_eq!(sse_connection_count(), before_count);
    }

    #[tokio::test]
    async fn wrapper_flushes_mid_stream_once_the_threshold_is_crossed() {
        let limiter = ConnectionLimiter::new(ConnectionLimitConfig::default(), None);
        let ip = addr().ip();
        assert!(limiter.try_acquire(ip));
        let stats = Arc::new(ServerStats::new());
        let pending_bytes = Arc::new(AtomicU64::new(0));

        let mut wrapper = SseStreamWrapper {
            inner: Box::pin(stream::once(async {
                Ok(Event::default().data("x"))
            })),
            limiter,
            client_ip: ip,
            stats: stats.clone(),
            pending_bytes: pending_bytes.clone(),
        };
        // Simulate accumulated bytes crossing FLUSH_THRESHOLD_BYTES from a
        // previous message, then poll once to trigger the mid-stream flush.
        pending_bytes.store(256 * 1024, Ordering::Relaxed);
        let item = futures_util::StreamExt::next(&mut wrapper).await;
        assert!(item.is_some());
        assert_eq!(stats.bytes_sent.load(Ordering::Relaxed), 256 * 1024);
        assert_eq!(pending_bytes.load(Ordering::Relaxed), 0);
    }
}
