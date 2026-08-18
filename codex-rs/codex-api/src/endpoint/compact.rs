use crate::auth::SharedAuthProvider;
use crate::common::CompactionInput;
use crate::endpoint::session::EndpointSession;
use crate::error::ApiError;
use crate::provider::Provider;
use codex_client::HttpTransport;
use codex_client::RequestTelemetry;
use codex_client::RetryNotifier;
use codex_protocol::models::ResponseItem;
use http::HeaderMap;
use http::Method;
use serde::Deserialize;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

const X_CODEX_TURN_STATE_HEADER: &str = "x-codex-turn-state";

pub struct CompactClient<T: HttpTransport> {
    session: EndpointSession<T>,
}

impl<T: HttpTransport> CompactClient<T> {
    pub fn new(transport: T, provider: Provider, auth: SharedAuthProvider) -> Self {
        Self {
            session: EndpointSession::new(transport, provider, auth),
        }
    }

    pub fn with_telemetry(self, request: Option<Arc<dyn RequestTelemetry>>) -> Self {
        Self {
            session: self.session.with_request_telemetry(request),
        }
    }

    pub fn with_retry_notifier(self, retry_notifier: Option<RetryNotifier>) -> Self {
        Self {
            session: self.session.with_retry_notifier(retry_notifier),
        }
    }

    fn path() -> &'static str {
        "responses/compact"
    }

    pub async fn compact(
        &self,
        body: serde_json::Value,
        extra_headers: HeaderMap,
        request_timeout: Duration,
        turn_state: Option<&OnceLock<String>>,
    ) -> Result<Vec<ResponseItem>, ApiError> {
        let resp = self
            .session
            .execute_with(
                Method::POST,
                Self::path(),
                extra_headers,
                Some(body),
                |req| {
                    req.timeout = Some(request_timeout);
                },
            )
            .await?;
        if let Some(turn_state) = turn_state
            && let Some(header_value) = resp
                .headers
                .get(X_CODEX_TURN_STATE_HEADER)
                .and_then(|value| value.to_str().ok())
        {
            let _ = turn_state.set(header_value.to_string());
        }
        let parsed: CompactHistoryResponse =
            serde_json::from_slice(&resp.body).map_err(|e| ApiError::Retryable {
                message: format!("failed to decode compaction response: {e}"),
                delay: None,
            })?;
        Ok(parsed.output)
    }

    pub async fn compact_input(
        &self,
        input: &CompactionInput<'_>,
        extra_headers: HeaderMap,
        request_timeout: Duration,
        turn_state: Option<&OnceLock<String>>,
    ) -> Result<Vec<ResponseItem>, ApiError> {
        let body = serde_json::to_value(input).map_err(|e| ApiError::InvalidRequest {
            message: format!("failed to encode compaction input: {e}"),
        })?;
        self.compact(body, extra_headers, request_timeout, turn_state)
            .await
    }
}

#[derive(Debug, Deserialize)]
struct CompactHistoryResponse {
    output: Vec<ResponseItem>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthProvider;
    use codex_client::Request;
    use codex_client::Response;
    use codex_client::RetryDisposition;
    use codex_client::RetryStatus;
    use codex_client::StreamResponse;
    use codex_client::TransportError;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    #[derive(Clone, Default)]
    struct DummyTransport;

    impl HttpTransport for DummyTransport {
        async fn execute(&self, _req: Request) -> Result<Response, TransportError> {
            Err(TransportError::Build("execute should not run".to_string()))
        }

        async fn stream(&self, _req: Request) -> Result<StreamResponse, TransportError> {
            Err(TransportError::Build("stream should not run".to_string()))
        }
    }

    #[derive(Clone)]
    struct SequenceTransport {
        responses: Arc<Mutex<VecDeque<Result<Response, TransportError>>>>,
    }

    impl SequenceTransport {
        fn new(responses: Vec<Result<Response, TransportError>>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
            }
        }
    }

    impl HttpTransport for SequenceTransport {
        async fn execute(&self, _req: Request) -> Result<Response, TransportError> {
            self.responses
                .lock()
                .expect("response queue lock")
                .pop_front()
                .expect("response queue should contain an attempt")
        }

        async fn stream(&self, _req: Request) -> Result<StreamResponse, TransportError> {
            Err(TransportError::Build("stream should not run".to_string()))
        }
    }

    #[derive(Clone, Default)]
    struct DummyAuth;

    impl AuthProvider for DummyAuth {
        fn add_auth_headers(&self, _headers: &mut HeaderMap) {}
    }

    fn provider() -> Provider {
        Provider {
            name: "test".to_string(),
            base_url: "https://example.com/v1".to_string(),
            query_params: None,
            headers: HeaderMap::new(),
            retry: crate::provider::RetryConfig {
                max_attempts: 0,
                base_delay: Duration::ZERO,
                retry_429: true,
                retry_5xx: true,
                retry_transport: true,
            },
            stream_idle_timeout: Duration::from_secs(1),
        }
    }

    fn capacity_error() -> TransportError {
        TransportError::Http {
            status: http::StatusCode::SERVICE_UNAVAILABLE,
            url: None,
            headers: None,
            body: Some("Selected model is at capacity. Please try a different model.".to_string()),
        }
    }

    #[test]
    fn path_is_responses_compact() {
        assert_eq!(CompactClient::<DummyTransport>::path(), "responses/compact");
    }

    #[tokio::test]
    async fn legacy_remote_compaction_retries_capacity_and_preserves_success() {
        let transport = SequenceTransport::new(vec![
            Err(capacity_error()),
            Ok(Response {
                status: http::StatusCode::OK,
                headers: HeaderMap::new(),
                body: br#"{"output":[]}"#.to_vec().into(),
            }),
        ]);
        let statuses = Arc::new(Mutex::new(Vec::<RetryStatus>::new()));
        let status_sink = Arc::clone(&statuses);
        let client = CompactClient::new(transport, provider(), Arc::new(DummyAuth))
            .with_retry_notifier(Some(Arc::new(move |status| {
                status_sink.lock().expect("status lock").push(status);
                Box::pin(async {})
            })));

        let output = client
            .compact(
                serde_json::json!({"input": []}),
                HeaderMap::new(),
                Duration::from_secs(1),
                None,
            )
            .await
            .expect("capacity retry should recover");

        assert!(output.is_empty(), "successful response must be preserved");
        let statuses = statuses.lock().expect("status lock");
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].disposition, RetryDisposition::Capacity);
        assert_eq!(statuses[0].attempt, 1);
    }
}
