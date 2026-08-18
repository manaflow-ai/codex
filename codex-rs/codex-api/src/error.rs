use crate::rate_limits::RateLimitError;
use codex_client::RetryDisposition;
use codex_client::RetryOn;
use codex_client::TransportError;
use codex_client::classify_http_response;
use codex_client::classify_io_error;
use codex_client::classify_provider_error_text;
use codex_client::classify_transport_error;
use codex_client::is_capacity_error_body;
use codex_client::is_capacity_error_text;
use codex_client::is_permanent_error_text;
use codex_client::is_transient_error_text;
use http::StatusCode;
use std::time::Duration;
use thiserror::Error;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::error::UrlError;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("api error {status}: {message}")]
    Api { status: StatusCode, message: String },
    #[error("stream error: {0}")]
    Stream(String),
    #[error("context window exceeded")]
    ContextWindowExceeded,
    #[error("quota exceeded")]
    QuotaExceeded,
    #[error("usage not included")]
    UsageNotIncluded,
    #[error("retryable error: {message}")]
    Retryable {
        message: String,
        delay: Option<Duration>,
    },
    #[error("rate limit: {0}")]
    RateLimit(String),
    #[error("invalid request: {message}")]
    InvalidRequest { message: String },
    #[error("cyber policy: {message}")]
    CyberPolicy { message: String },
    #[error("server overloaded")]
    ServerOverloaded,
    #[error("request cancelled")]
    Cancelled,
}

impl ApiError {
    /// Classifies this API failure for a caller-owned retry loop.
    ///
    /// The result is semantic, not a retry budget. Callers must still choose a finite budget for
    /// ordinary transient failures and a separate persistent budget for provider capacity.
    pub fn retry_disposition(&self) -> RetryDisposition {
        match self {
            Self::ServerOverloaded => RetryDisposition::Capacity,
            Self::Retryable { message, .. } => {
                if is_capacity_error_body(message) {
                    return RetryDisposition::Capacity;
                }
                if is_permanent_error_text(message) {
                    return RetryDisposition::DoNotRetry;
                }
                match classify_provider_error_text(message) {
                    RetryDisposition::Capacity => RetryDisposition::Capacity,
                    RetryDisposition::Transient | RetryDisposition::DoNotRetry => {
                        RetryDisposition::Transient
                    }
                }
            }
            Self::RateLimit(message) => {
                if is_capacity_error_body(message) {
                    RetryDisposition::Capacity
                } else {
                    classify_provider_error_text(message)
                }
            }
            Self::Transport(error) => classify_transport_error(error),
            Self::Api { status, message } => classify_http_response(*status, Some(message)),
            Self::Stream(message) => classify_provider_error_text(message),
            Self::ContextWindowExceeded
            | Self::QuotaExceeded
            | Self::UsageNotIncluded
            | Self::InvalidRequest { .. }
            | Self::CyberPolicy { .. }
            | Self::Cancelled => RetryDisposition::DoNotRetry,
        }
    }

    pub(crate) fn is_retryable_for_attempt(
        &self,
        retry_on: &RetryOn,
        attempt: u64,
        max_attempts: u64,
    ) -> bool {
        if attempt >= max_attempts {
            return false;
        }
        match self {
            Self::Retryable { message, .. } => {
                is_capacity_error_body(message) || !is_permanent_error_text(message)
            }
            Self::ServerOverloaded => true,
            Self::RateLimit(message) => !matches!(
                classify_provider_error_text(message),
                RetryDisposition::DoNotRetry
            ),
            Self::Stream(message) => match classify_provider_error_text(message) {
                RetryDisposition::Capacity => true,
                RetryDisposition::Transient => retry_on.retry_transport,
                RetryDisposition::DoNotRetry => false,
            },
            Self::Transport(error) => retry_on.should_retry(error, attempt, max_attempts),
            Self::Api { status, message } => {
                let transport_error = TransportError::Http {
                    status: *status,
                    url: None,
                    headers: None,
                    body: Some(message.clone()),
                };
                retry_on.should_retry(&transport_error, attempt, max_attempts)
            }
            Self::ContextWindowExceeded
            | Self::QuotaExceeded
            | Self::UsageNotIncluded
            | Self::InvalidRequest { .. }
            | Self::CyberPolicy { .. }
            | Self::Cancelled => false,
        }
    }
}

pub(crate) fn is_permanent_error_message(message: &str) -> bool {
    is_permanent_error_text(message)
}

pub(crate) fn is_permanent_error_fields(
    error_type: Option<&str>,
    code: Option<&str>,
    message: Option<&str>,
) -> bool {
    [error_type, code, message]
        .into_iter()
        .flatten()
        .any(is_permanent_error_message)
}

/// Returns true when a provider error identifies temporary model or server capacity.
///
/// Providers use several wire formats for the same condition. Keep this decision in one place
/// so SSE and HTTP responses do not disagree about whether a capacity response is retryable.
pub(crate) fn is_server_overloaded_error(
    error_type: Option<&str>,
    code: Option<&str>,
    message: Option<&str>,
) -> bool {
    [error_type, code, message]
        .into_iter()
        .flatten()
        .any(is_capacity_error_text)
}

/// Classifies each WebSocket library error branch. Unknown TLS details fail closed unless their
/// text identifies a known transient transport failure.
pub(crate) fn classify_websocket_error(error: &WsError) -> RetryDisposition {
    match error {
        WsError::Http(response) => {
            let body = response
                .body()
                .as_ref()
                .and_then(|bytes| std::str::from_utf8(bytes).ok());
            classify_http_response(response.status(), body)
        }
        WsError::ConnectionClosed | WsError::AlreadyClosed => RetryDisposition::Transient,
        WsError::Io(error) => classify_io_error(error),
        WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => {
            RetryDisposition::Transient
        }
        WsError::Url(UrlError::UnableToConnect(_) | UrlError::ProxyConnect(_)) => {
            RetryDisposition::Transient
        }
        WsError::Tls(error) => classify_provider_error_text(&error.to_string()),
        WsError::Capacity(_)
        | WsError::Protocol(_)
        | WsError::WriteBufferFull(_)
        | WsError::Utf8(_)
        | WsError::AttackAttempt
        | WsError::Url(_)
        | WsError::HttpFormat(_) => RetryDisposition::DoNotRetry,
    }
}

pub(crate) fn map_websocket_operation_error(error: WsError, context: &str) -> ApiError {
    let disposition = classify_websocket_error(&error);
    let message = format!("{context}: {error}");
    match disposition {
        RetryDisposition::Capacity => ApiError::ServerOverloaded,
        RetryDisposition::Transient => ApiError::Retryable {
            message,
            delay: None,
        },
        RetryDisposition::DoNotRetry => ApiError::Stream(message),
    }
}

pub(crate) fn classify_websocket_close(code: CloseCode, reason: &str) -> RetryDisposition {
    let capacity = is_capacity_error_body(reason);
    let permanent_close_code = matches!(
        code,
        CloseCode::Protocol
            | CloseCode::Unsupported
            | CloseCode::Invalid
            | CloseCode::Policy
            | CloseCode::Size
            | CloseCode::Extension
            | CloseCode::Tls
            | CloseCode::Reserved(_)
            | CloseCode::Iana(_)
            | CloseCode::Library(_)
            | CloseCode::Bad(_)
    );
    // A protocol-level policy or format close is terminal even when a gateway copied a capacity
    // phrase into its reason. For retry-oriented close codes, keep a genuine capacity wrapper
    // retryable while still honoring a plain permanent provider message.
    if permanent_close_code || (is_permanent_error_text(reason) && !capacity) {
        return RetryDisposition::DoNotRetry;
    }
    if capacity || code == CloseCode::Again {
        return RetryDisposition::Capacity;
    }
    if is_transient_error_text(reason)
        || matches!(
            code,
            CloseCode::Normal
                | CloseCode::Away
                | CloseCode::Status
                | CloseCode::Abnormal
                | CloseCode::Error
                | CloseCode::Restart
        )
    {
        return RetryDisposition::Transient;
    }
    RetryDisposition::DoNotRetry
}

pub(crate) fn map_websocket_close_error(code: CloseCode, reason: &str, context: &str) -> ApiError {
    let message = if reason.is_empty() {
        format!("{context}: close code {code}")
    } else {
        format!("{context}: close code {code}: {reason}")
    };
    match classify_websocket_close(code, reason) {
        RetryDisposition::Capacity => ApiError::ServerOverloaded,
        RetryDisposition::Transient => ApiError::Retryable {
            message,
            delay: None,
        },
        RetryDisposition::DoNotRetry => ApiError::Stream(message),
    }
}

impl From<RateLimitError> for ApiError {
    fn from(err: RateLimitError) -> Self {
        Self::RateLimit(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::error::CapacityError;

    #[test]
    fn websocket_classifier_retries_only_transient_error_branches() {
        let transient = [
            WsError::ConnectionClosed,
            WsError::Io(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
            WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake),
            WsError::Url(UrlError::UnableToConnect("host".to_string())),
        ];
        for error in transient {
            assert_eq!(
                classify_websocket_error(&error),
                RetryDisposition::Transient,
                "expected {error} to retry"
            );
        }

        let permanent = [
            WsError::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
            WsError::Capacity(CapacityError::MessageTooLong {
                size: 2,
                max_size: 1,
            }),
            WsError::Url(UrlError::NoHostName),
            WsError::Utf8("invalid frame".to_string()),
        ];
        for error in permanent {
            assert_eq!(
                classify_websocket_error(&error),
                RetryDisposition::DoNotRetry,
                "did not expect {error} to retry"
            );
        }
    }

    #[test]
    fn websocket_close_classifier_separates_restart_capacity_and_policy() {
        assert_eq!(
            classify_websocket_close(CloseCode::Restart, "server restart"),
            RetryDisposition::Transient
        );
        assert_eq!(
            classify_websocket_close(CloseCode::Again, "server overloaded"),
            RetryDisposition::Capacity
        );
        assert_eq!(
            classify_websocket_close(CloseCode::Policy, "try again later"),
            RetryDisposition::DoNotRetry
        );
        assert_eq!(
            classify_websocket_close(
                CloseCode::Policy,
                "Selected model is at capacity. Please try a different model."
            ),
            RetryDisposition::DoNotRetry
        );
    }

    #[test]
    fn websocket_close_classifier_reads_capacity_from_a_wrapped_error_body() {
        let reason = r#"{"type":"invalid_request_error","error":{"message":"Selected model is at capacity. Please try a different model."}}"#;
        assert_eq!(
            classify_websocket_close(CloseCode::Error, reason),
            RetryDisposition::Capacity
        );
    }

    #[test]
    fn explicit_retryable_errors_cannot_override_permanent_provider_semantics() {
        let error = ApiError::Retryable {
            message: "invalid request: account suspended".to_string(),
            delay: None,
        };
        assert_eq!(error.retry_disposition(), RetryDisposition::DoNotRetry);
        assert!(!error.is_retryable_for_attempt(
            &RetryOn {
                retry_429: true,
                retry_5xx: true,
                retry_transport: true,
            },
            0,
            1,
        ));
    }
}
