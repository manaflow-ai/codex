//! Shared retry and transport fallback decisions for Responses requests.

use std::time::Duration;

use crate::client::ModelClientSession;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use crate::util::backoff;
use codex_client::RetryOperation;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::WarningEvent;
use http::StatusCode;
use tracing::warn;

const INITIAL_CONNECTION_RETRY_DELAY: Duration = Duration::from_secs(5);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);
const MAX_CAPACITY_RETRIES: u64 = 100;

#[derive(Debug, Clone, Copy)]
pub(crate) enum ResponsesStreamRequest {
    Sampling,
    RemoteCompactionV1,
    RemoteCompactionV2,
}

pub(crate) struct ResponsesStreamRetryState {
    retries: u64,
    connection_retries: u64,
    connection_retry_delay: Duration,
    capacity_retries: u64,
}

impl Default for ResponsesStreamRetryState {
    fn default() -> Self {
        Self {
            retries: 0,
            connection_retries: 0,
            connection_retry_delay: INITIAL_CONNECTION_RETRY_DELAY,
            capacity_retries: 0,
        }
    }
}

pub(crate) fn should_retry_response_stream_error(
    request: ResponsesStreamRequest,
    err: &CodexErr,
) -> bool {
    match err.details() {
        CodexErrorDetails::ServerOverloaded
        | CodexErrorDetails::Timeout
        | CodexErrorDetails::RequestTimeout
        | CodexErrorDetails::InternalServerError
        | CodexErrorDetails::InternalAgentDied
        | CodexErrorDetails::Io(_)
        | CodexErrorDetails::TokioJoin(_) => true,
        CodexErrorDetails::Stream(_) | CodexErrorDetails::Json(_) => {
            !matches!(request, ResponsesStreamRequest::RemoteCompactionV1)
        }
        CodexErrorDetails::UnexpectedStatus(error) => is_transient_http_status(error.status),
        CodexErrorDetails::RetryLimit(error) => is_transient_http_status(error.status),
        CodexErrorDetails::ResponseStreamFailed(error) => {
            error.source.status().is_none_or(is_transient_http_status)
        }
        CodexErrorDetails::ConnectionFailed(error) => {
            error.source.status().is_none_or(is_transient_http_status)
        }
        CodexErrorDetails::TurnAborted
        | CodexErrorDetails::SessionBudgetExceeded
        | CodexErrorDetails::ContextWindowExceeded
        | CodexErrorDetails::ThreadNotFound(_)
        | CodexErrorDetails::AgentLimitReached { .. }
        | CodexErrorDetails::SessionConfiguredNotFirstEvent
        | CodexErrorDetails::Spawn
        | CodexErrorDetails::Interrupted
        | CodexErrorDetails::InvalidRequest(_)
        | CodexErrorDetails::ToolCollision(_)
        | CodexErrorDetails::InvalidImageRequest()
        | CodexErrorDetails::UsageLimitReached(_)
        | CodexErrorDetails::CyberPolicy { .. }
        | CodexErrorDetails::QuotaExceeded
        | CodexErrorDetails::UsageNotIncluded
        | CodexErrorDetails::Sandbox(_)
        | CodexErrorDetails::LandlockSandboxExecutableNotProvided
        | CodexErrorDetails::UnsupportedOperation(_)
        | CodexErrorDetails::RefreshTokenFailed(_)
        | CodexErrorDetails::Fatal(_)
        | CodexErrorDetails::EnvVar(_) => false,
        #[cfg(target_os = "linux")]
        CodexErrorDetails::LandlockRuleset(_) | CodexErrorDetails::LandlockPathFd(_) => false,
    }
}

fn is_transient_http_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_EARLY | StatusCode::TOO_MANY_REQUESTS
    ) || status.is_server_error()
}

/// Handles a retryable stream error and returns `Ok(())` when the caller should
/// retry the request loop.
pub(crate) async fn handle_retryable_response_stream_error(
    retry_state: &mut ResponsesStreamRetryState,
    max_retries: u64,
    err: CodexErr,
    client_session: &mut ModelClientSession,
    sess: &Session,
    turn_context: &TurnContext,
    request: ResponsesStreamRequest,
) -> Result<(), CodexErr> {
    handle_retryable_response_error_inner(
        retry_state,
        max_retries,
        err,
        Some(client_session),
        sess,
        turn_context,
        request,
    )
    .await
}

/// Handles a retryable unary Responses error and returns `Ok(())` when the
/// caller should retry the request loop.
pub(crate) async fn handle_retryable_response_error(
    retry_state: &mut ResponsesStreamRetryState,
    max_retries: u64,
    err: CodexErr,
    sess: &Session,
    turn_context: &TurnContext,
    request: ResponsesStreamRequest,
) -> Result<(), CodexErr> {
    handle_retryable_response_error_inner(
        retry_state,
        max_retries,
        err,
        None,
        sess,
        turn_context,
        request,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn handle_retryable_response_error_inner(
    retry_state: &mut ResponsesStreamRetryState,
    max_retries: u64,
    err: CodexErr,
    mut client_session: Option<&mut ModelClientSession>,
    sess: &Session,
    turn_context: &TurnContext,
    request: ResponsesStreamRequest,
) -> Result<(), CodexErr> {
    let operation = match request {
        ResponsesStreamRequest::Sampling => RetryOperation::Sampling,
        ResponsesStreamRequest::RemoteCompactionV1 => RetryOperation::RemoteCompactionV1,
        ResponsesStreamRequest::RemoteCompactionV2 => RetryOperation::RemoteCompactionV2,
    };
    let can_fallback_from_websocket = client_session.is_some()
        && sess.services.model_client.responses_websocket_enabled()
        && is_websocket_endpoint_unavailable(&err);
    if !should_retry_response_stream_error(request, &err) && !can_fallback_from_websocket {
        return Err(err);
    }

    if matches!(err.details(), CodexErrorDetails::ServerOverloaded) {
        if retry_state.capacity_retries >= MAX_CAPACITY_RETRIES {
            return Err(err);
        }
        retry_state.capacity_retries = retry_state.capacity_retries.saturating_add(1);
        let retry_count = retry_state.capacity_retries;
        let delay = err
            .retry_delay()
            .unwrap_or_else(|| capacity_retry_delay(retry_count))
            .min(MAX_RETRY_DELAY);
        warn!(
            turn_id = %turn_context.sub_id,
            retries = retry_count,
            max_retries = MAX_CAPACITY_RETRIES,
            ?delay,
            "model at capacity; waiting to retry Responses request"
        );
        sess.notify_stream_error(
            turn_context,
            format!(
                "Model at capacity. Retrying in {} (attempt {retry_count})",
                format_retry_delay(delay)
            ),
            err,
        )
        .await;
        codex_client::record_retry!(retry_count, delay, operation);
        tokio::time::sleep(delay).await;
        return Ok(());
    }

    if matches!(err.details(), CodexErrorDetails::ConnectionFailed(_))
        && !turn_context.session_source.is_internal()
        && !turn_context.provider.info().is_amazon_bedrock()
    {
        if retry_state.connection_retries >= max_retries {
            if client_session.as_deref_mut().is_some_and(|client_session| {
                client_session.try_switch_fallback_transport(
                    &turn_context.session_telemetry,
                    &turn_context.model_info,
                )
            }) {
                notify_transport_fallback(sess, turn_context, &err).await;
                retry_state.connection_retries = 0;
                retry_state.connection_retry_delay = INITIAL_CONNECTION_RETRY_DELAY;
                return Ok(());
            }
            return Err(err);
        }

        retry_state.connection_retries = retry_state.connection_retries.saturating_add(1);
        let retry_count = retry_state.connection_retries;
        let retry_delay = retry_state.connection_retry_delay;
        warn!(
            turn_id = %turn_context.sub_id,
            error = %err,
            ?retry_delay,
            "stream connection failed; waiting to retry"
        );
        sess.notify_stream_error(
            turn_context,
            format!(
                "Network error. Retrying in {} (attempt {retry_count}/{max_retries})",
                format_retry_delay(retry_delay)
            ),
            err,
        )
        .await;
        codex_client::record_retry!(retry_count, retry_delay, operation);
        tokio::time::sleep(retry_delay).await;
        retry_state.connection_retry_delay = retry_delay.saturating_mul(2).min(MAX_RETRY_DELAY);
        return Ok(());
    }

    if retry_state.retries >= max_retries
        && client_session.is_some_and(|client_session| {
            client_session.try_switch_fallback_transport(
                &turn_context.session_telemetry,
                &turn_context.model_info,
            )
        })
    {
        notify_transport_fallback(sess, turn_context, &err).await;
        retry_state.retries = 0;
        return Ok(());
    }

    if retry_state.retries < max_retries {
        retry_state.retries += 1;
        let retry_count = retry_state.retries;
        let delay = err
            .retry_delay()
            .unwrap_or_else(|| backoff(retry_count))
            .min(MAX_RETRY_DELAY);
        log_retry(request, turn_context, &err, retry_count, max_retries, delay);

        sess.notify_stream_error(
            turn_context,
            format!(
                "Request failed. Retrying in {} (attempt {retry_count}/{max_retries})",
                format_retry_delay(delay)
            ),
            err,
        )
        .await;
        codex_client::record_retry!(retry_count, delay, operation);
        tokio::time::sleep(delay).await;
        return Ok(());
    }

    Err(err)
}

async fn notify_transport_fallback(sess: &Session, turn_context: &TurnContext, err: &CodexErr) {
    sess.send_event(
        turn_context,
        EventMsg::Warning(WarningEvent {
            message: format!("Falling back from WebSockets to HTTPS transport. {err:#}"),
        }),
    )
    .await;
}

fn capacity_retry_delay(attempt: u64) -> Duration {
    backoff(attempt.min(32)).min(MAX_RETRY_DELAY)
}

fn is_websocket_endpoint_unavailable(err: &CodexErr) -> bool {
    matches!(
        err.details(),
        CodexErrorDetails::UnexpectedStatus(error)
            if matches!(error.status, StatusCode::NOT_FOUND | StatusCode::UPGRADE_REQUIRED)
    )
}

fn format_retry_delay(delay: Duration) -> String {
    if delay < Duration::from_secs(1) {
        format!("{}ms", delay.as_millis().max(1))
    } else {
        format!("{:.1}s", delay.as_secs_f64())
    }
}

fn log_retry(
    request: ResponsesStreamRequest,
    turn_context: &TurnContext,
    err: &CodexErr,
    retries: u64,
    max_retries: u64,
    delay: Duration,
) {
    match request {
        ResponsesStreamRequest::Sampling => {
            warn!(
                turn_id = %turn_context.sub_id,
                retries,
                max_retries,
                sampling_error = %err,
                "stream disconnected - retrying sampling request ({retries}/{max_retries} in {delay:?})...",
            );
        }
        ResponsesStreamRequest::RemoteCompactionV1 => {
            warn!(
                turn_id = %turn_context.sub_id,
                retries,
                max_retries,
                compact_error = %err,
                "remote compaction stream failed; retrying unary request after delay"
            );
        }
        ResponsesStreamRequest::RemoteCompactionV2 => {
            warn!(
                turn_id = %turn_context.sub_id,
                retries,
                max_retries,
                compact_error = %err,
                "remote compaction v2 stream failed; retrying request after delay"
            );
        }
    }
}

#[cfg(test)]
#[path = "responses_retry_tests.rs"]
mod tests;
