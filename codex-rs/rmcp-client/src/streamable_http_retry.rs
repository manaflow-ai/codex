use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;
use anyhow::anyhow;
use codex_client::RetryDisposition;
use codex_client::backoff;
use codex_client::classify_http_response;
use codex_client::classify_provider_error_text;
use codex_client::is_capacity_error_body;
use codex_client::is_permanent_error_text;
use codex_exec_server::ExecServerError;
use http::StatusCode;
use rmcp::service::RoleClient;
use rmcp::service::RunningService;
use rmcp::transport::streamable_http_client::StreamableHttpError;
use tokio::time;
use tracing::warn;

use crate::elicitation_client_service::ElicitationClientService;
use crate::http_client_adapter::StreamableHttpClientAdapterError;
use crate::oauth::OAuthPersistor;

use super::McpRetryStatus;
use super::PendingTransport;
use super::RmcpClient;

const JSON_RPC_INTERNAL_ERROR_CODE: i64 = -32603;
const MCP_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
// Tool and catalog operations can be replayed after a broken transport, but side effects make an
// unbounded transient loop unsafe. Capacity remains persistent because it is detected before the
// remote operation is accepted.
pub(super) const MCP_TRANSIENT_MAX_RETRIES: u64 = 2;
pub(super) const MCP_CAPACITY_MAX_RETRIES: u64 = codex_client::UNLIMITED_RETRIES;

pub(super) fn mcp_retry_delay(attempt: u64) -> Duration {
    backoff(MCP_RETRY_BASE_DELAY, attempt).min(Duration::from_secs(60))
}

impl RmcpClient {
    pub(super) async fn connect_pending_transport_with_initialize_retries(
        &self,
        initial_transport: PendingTransport,
        client_service: ElicitationClientService,
        timeout: Option<Duration>,
    ) -> Result<(
        Arc<RunningService<RoleClient, ElicitationClientService>>,
        Option<OAuthPersistor>,
    )> {
        let should_retry = match &initial_transport {
            PendingTransport::InProcess { .. } | PendingTransport::Stdio { .. } => false,
            PendingTransport::StreamableHttp { .. }
            | PendingTransport::StreamableHttpWithOAuth { .. } => true,
        };
        let mut retry_deadline = timeout.map(|duration| Instant::now() + duration);
        let mut pending_transport = Some(initial_transport);
        let mut transient_retries = 0;
        let mut capacity_retries = 0;

        loop {
            let transport = match pending_transport.take() {
                Some(transport) => transport,
                None => {
                    let remaining = remaining_initialize_timeout(timeout, retry_deadline)?;
                    match remaining {
                        Some(remaining) => time::timeout(
                            remaining,
                            Self::create_pending_transport(&self.transport_recipe),
                        )
                        .await
                        .map_err(|_| initialize_timeout_error(timeout, remaining))??,
                        None => Self::create_pending_transport(&self.transport_recipe).await?,
                    }
                }
            };
            if let PendingTransport::StreamableHttpWithOAuth {
                oauth_persistor, ..
            } = &transport
            {
                // OAuth refresh has its own lock and provider request bounds. Exclude it from the
                // MCP handshake budget, and finish persistence before attempting initialize.
                oauth_persistor.set_retry_notifier(self.retry_notifier.clone());
                let refresh_started_at = Instant::now();
                oauth_persistor.refresh_if_needed().await?;
                if let Some(deadline) = retry_deadline.as_mut() {
                    *deadline += refresh_started_at.elapsed();
                }
            }
            let attempt_timeout = remaining_initialize_timeout(timeout, retry_deadline)?;

            match self
                .connect_pending_transport(transport, client_service.clone(), attempt_timeout)
                .await
            {
                Ok(result) => return Ok(result),
                Err(error) if should_retry => {
                    let disposition = Self::classify_initialize_error(&error);
                    let (retry_count, max_retries) = match disposition {
                        RetryDisposition::Capacity
                            if capacity_retries < MCP_CAPACITY_MAX_RETRIES =>
                        {
                            capacity_retries += 1;
                            (capacity_retries, MCP_CAPACITY_MAX_RETRIES)
                        }
                        RetryDisposition::Transient
                            if transient_retries < MCP_TRANSIENT_MAX_RETRIES =>
                        {
                            transient_retries += 1;
                            (transient_retries, MCP_TRANSIENT_MAX_RETRIES)
                        }
                        RetryDisposition::DoNotRetry
                        | RetryDisposition::Transient
                        | RetryDisposition::Capacity => return Err(error),
                    };
                    let delay = mcp_retry_delay(retry_count);
                    warn!(
                        attempt = retry_count,
                        max_retries,
                        delay_ms = delay.as_millis(),
                        error = %error,
                        retry_disposition = ?disposition,
                        "streamable HTTP MCP initialize failed with a retryable error; retrying"
                    );
                    self.emit_retry_status(McpRetryStatus {
                        operation: "initialize".to_string(),
                        disposition,
                        attempt: retry_count,
                        max_retries,
                        delay,
                        error: error.to_string(),
                    })
                    .await;
                    if !sleep_with_retry_deadline(delay, retry_deadline).await {
                        let duration = timeout.unwrap_or(delay);
                        return Err(anyhow!(
                            "timed out handshaking with MCP server after {duration:?}"
                        ));
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn classify_initialize_error(error: &anyhow::Error) -> RetryDisposition {
        let mut disposition = RetryDisposition::DoNotRetry;
        for source in error.chain() {
            let current = source
                .downcast_ref::<HandshakeError>()
                .map(|error| Self::classify_client_initialize_error(&error.source))
                .or_else(|| {
                    source
                        .downcast_ref::<rmcp::service::ClientInitializeError>()
                        .map(Self::classify_client_initialize_error)
                })
                .unwrap_or(RetryDisposition::DoNotRetry);
            match current {
                RetryDisposition::Capacity => return RetryDisposition::Capacity,
                RetryDisposition::Transient => disposition = RetryDisposition::Transient,
                RetryDisposition::DoNotRetry => {}
            }
        }
        disposition
    }

    fn classify_client_initialize_error(
        error: &rmcp::service::ClientInitializeError,
    ) -> RetryDisposition {
        match error {
            rmcp::service::ClientInitializeError::TransportError { error, context }
                if matches!(
                    context.as_ref(),
                    "send initialize request" | "send discover request"
                ) =>
            {
                error
                    .error
                    .downcast_ref::<StreamableHttpError<StreamableHttpClientAdapterError>>()
                    .map(Self::classify_streamable_http_error)
                    .unwrap_or(RetryDisposition::DoNotRetry)
            }
            rmcp::service::ClientInitializeError::TransportError { error, context }
                if context.as_ref() == "send initialized notification" =>
            {
                error
                    .error
                    .downcast_ref::<StreamableHttpError<StreamableHttpClientAdapterError>>()
                    .map(Self::classify_streamable_http_error)
                    .unwrap_or(RetryDisposition::DoNotRetry)
            }
            _ => RetryDisposition::DoNotRetry,
        }
    }

    #[cfg(test)]
    fn is_retryable_client_initialize_error(error: &rmcp::service::ClientInitializeError) -> bool {
        Self::classify_client_initialize_error(error) != RetryDisposition::DoNotRetry
    }

    #[cfg(test)]
    pub(super) fn is_retryable_streamable_http_error(
        error: &StreamableHttpError<StreamableHttpClientAdapterError>,
    ) -> bool {
        Self::classify_streamable_http_error(error) != RetryDisposition::DoNotRetry
    }

    pub(super) fn classify_streamable_http_error(
        error: &StreamableHttpError<StreamableHttpClientAdapterError>,
    ) -> RetryDisposition {
        match error {
            StreamableHttpError::Client(StreamableHttpClientAdapterError::HttpRequest(error)) => {
                classify_exec_server_error(error)
            }
            StreamableHttpError::UnexpectedServerResponse(message) => {
                classify_unexpected_server_response(message.as_ref())
            }
            StreamableHttpError::Io(error) => codex_client::classify_io_error(error),
            StreamableHttpError::Sse(sse_stream::Error::Body(error)) => {
                classify_typed_transient_error(&error.to_string())
            }
            StreamableHttpError::UnexpectedEndOfStream
            | StreamableHttpError::TransportChannelClosed => RetryDisposition::Transient,
            StreamableHttpError::AuthRequired(_)
            | StreamableHttpError::InsufficientScope(_)
            | StreamableHttpError::SessionExpired
            | StreamableHttpError::UnexpectedContentType(_)
            | StreamableHttpError::ServerDoesNotSupportSse
            | StreamableHttpError::ServerDoesNotSupportDeleteSession
            | StreamableHttpError::TokioJoinError(_)
            | StreamableHttpError::Deserialize(_)
            | StreamableHttpError::Auth(_)
            | StreamableHttpError::MissingSessionIdInResponse
            | StreamableHttpError::Client(StreamableHttpClientAdapterError::SessionExpired404)
            | StreamableHttpError::Client(StreamableHttpClientAdapterError::Header(_))
            | StreamableHttpError::Client(StreamableHttpClientAdapterError::ResponseTooLarge {
                ..
            })
            | StreamableHttpError::ReservedHeaderConflict(_)
            | StreamableHttpError::Sse(_) => RetryDisposition::DoNotRetry,
            _ => RetryDisposition::DoNotRetry,
        }
    }
}

fn classify_unexpected_server_response(message: &str) -> RetryDisposition {
    let Some(message) = message.strip_prefix("HTTP ") else {
        return classify_provider_error_text(message);
    };
    let status_code = message
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    let Ok(status) = status_code.parse::<u16>() else {
        return RetryDisposition::DoNotRetry;
    };
    let Ok(status) = StatusCode::from_u16(status) else {
        return RetryDisposition::DoNotRetry;
    };
    let body = message
        .split_once(':')
        .map(|(_, body)| body.trim())
        .unwrap_or_default();
    classify_http_response(status, Some(body))
}

fn classify_exec_server_error(error: &ExecServerError) -> RetryDisposition {
    match error {
        ExecServerError::Spawn(_)
        | ExecServerError::WebSocketConfiguration(_)
        | ExecServerError::Json(_)
        | ExecServerError::ProvisioningModeConflict { .. }
        | ExecServerError::EnvironmentRegistryConfig(_)
        | ExecServerError::EnvironmentRegistryAuth(_) => RetryDisposition::DoNotRetry,
        ExecServerError::EnvironmentRegistryHttp {
            status, message, ..
        } => classify_http_response(*status, Some(message)),
        ExecServerError::Server { code, message } => {
            let disposition = if is_capacity_error_body(message) {
                RetryDisposition::Capacity
            } else {
                classify_provider_error_text(message)
            };
            if disposition != RetryDisposition::DoNotRetry {
                disposition
            } else if *code == JSON_RPC_INTERNAL_ERROR_CODE && !is_permanent_error_text(message) {
                RetryDisposition::Transient
            } else {
                RetryDisposition::DoNotRetry
            }
        }
        ExecServerError::HttpRequest(message)
        | ExecServerError::Disconnected(message)
        | ExecServerError::Protocol(message) => classify_typed_transient_error(message),
        ExecServerError::WebSocketConnectTimeout { .. }
        | ExecServerError::InitializeTimedOut { .. }
        | ExecServerError::Closed => RetryDisposition::Transient,
        ExecServerError::WebSocketConnect { source, .. } => {
            classify_typed_transient_error(&source.to_string())
        }
        ExecServerError::ConnectionAttempt(source) => classify_exec_server_error(source.as_ref()),
        ExecServerError::EnvironmentRegistryRequest(error) => {
            let message = error.to_string();
            if is_capacity_error_body(&message) {
                RetryDisposition::Capacity
            } else if is_permanent_error_text(&message) {
                RetryDisposition::DoNotRetry
            } else if error.is_timeout() || error.is_connect() || error.is_body() {
                RetryDisposition::Transient
            } else if let Some(status) = error.status() {
                classify_http_response(status, Some(&message))
            } else {
                RetryDisposition::DoNotRetry
            }
        }
    }
}

fn classify_typed_transient_error(message: &str) -> RetryDisposition {
    // Typed exec-server failures still carry provider or transport text. Use the shared
    // fail-closed classifier instead of treating every unknown string as replay-safe. This
    // keeps configuration and protocol failures terminal while preserving known connection,
    // timeout, overload, and capacity markers.
    classify_provider_error_text(message)
}

pub(super) fn classify_mcp_error(error: &rmcp::model::ErrorData) -> RetryDisposition {
    let message = error.message.as_ref();
    let data = error
        .data
        .as_ref()
        .map(serde_json::Value::to_string)
        .unwrap_or_default();
    // MCP servers often put a provider error object in JSON-RPC `data` while using the generic
    // INVALID_REQUEST code. Inspect the nested semantics before rejecting the wrapper as
    // permanent, so the latest model-capacity response remains retryable.
    if is_capacity_error_body(message) || is_capacity_error_body(&data) {
        return RetryDisposition::Capacity;
    }
    if is_permanent_error_text(message) || is_permanent_error_text(&data) {
        return RetryDisposition::DoNotRetry;
    }
    for text in [message, data.as_str()] {
        match classify_provider_error_text(text) {
            RetryDisposition::Capacity => return RetryDisposition::Capacity,
            RetryDisposition::Transient => return RetryDisposition::Transient,
            RetryDisposition::DoNotRetry => {}
        }
    }
    if i64::from(error.code.0) == JSON_RPC_INTERNAL_ERROR_CODE {
        RetryDisposition::Transient
    } else {
        RetryDisposition::DoNotRetry
    }
}

#[cfg(test)]
fn is_retryable_http_status(status: StatusCode) -> bool {
    codex_client::is_transient_http_status(status)
}

fn remaining_initialize_timeout(
    timeout: Option<Duration>,
    deadline: Option<Instant>,
) -> Result<Option<Duration>> {
    let Some(deadline) = deadline else {
        return Ok(None);
    };
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(initialize_timeout_error(timeout, remaining))
    } else {
        Ok(Some(remaining))
    }
}

fn initialize_timeout_error(timeout: Option<Duration>, fallback: Duration) -> anyhow::Error {
    let duration = timeout.unwrap_or(fallback);
    anyhow!("timed out handshaking with MCP server after {duration:?}")
}

pub(super) async fn sleep_with_retry_deadline(delay: Duration, deadline: Option<Instant>) -> bool {
    if let Some(deadline) = deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        time::timeout(remaining, time::sleep(delay)).await.is_ok()
    } else {
        time::sleep(delay).await;
        true
    }
}

#[derive(Debug, thiserror::Error)]
#[error("handshaking with MCP server failed: {source}")]
pub(super) struct HandshakeError {
    #[source]
    pub(super) source: rmcp::service::ClientInitializeError,
}

#[cfg(test)]
#[path = "streamable_http_retry_tests.rs"]
mod tests;
