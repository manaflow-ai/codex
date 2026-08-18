//! Shared retry and transport fallback decisions for Responses requests.

use std::time::Duration;

use crate::client::ModelClientSession;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use crate::util::backoff;
use codex_client::RetryDisposition;
use codex_client::RetryNotifier;
use codex_client::RetryOperation;
use codex_client::RetryStatus;
use codex_client::classify_connection_error;
use codex_client::classify_http_response;
use codex_client::format_retry_budget;
use codex_client::is_capacity_error_body;
use codex_client::is_permanent_error_text;
use codex_client::is_transient_http_status;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::WarningEvent;
use http::StatusCode;
use tokio_util::sync::CancellationToken;
use tracing::warn;

const INITIAL_CONNECTION_RETRY_DELAY: Duration = Duration::from_secs(5);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);
const MAX_CAPACITY_RETRIES: u64 = codex_client::PERSISTENT_CAPACITY_MAX_RETRIES;

pub(crate) fn retry_status_notifier(
    sess: std::sync::Arc<Session>,
    turn_context: std::sync::Arc<TurnContext>,
) -> RetryNotifier {
    std::sync::Arc::new(move |status: RetryStatus| {
        let sess = std::sync::Arc::clone(&sess);
        let turn_context = std::sync::Arc::clone(&turn_context);
        Box::pin(async move {
            let message = match status.disposition {
                RetryDisposition::Capacity => format!(
                    "Model at capacity. Retrying in {} (HTTP attempt {}/{})",
                    format_retry_delay(status.delay),
                    status.attempt,
                    format_retry_budget(status.max_retries),
                ),
                RetryDisposition::Transient => format!(
                    "Request failed. Retrying in {} (HTTP attempt {}/{})",
                    format_retry_delay(status.delay),
                    status.attempt,
                    format_retry_budget(status.max_retries),
                ),
                RetryDisposition::DoNotRetry => return,
            };
            let error = match status.disposition {
                RetryDisposition::Capacity => CodexErr::ServerOverloaded,
                RetryDisposition::Transient => {
                    CodexErr::Stream(status.error).with_explicit_retryable()
                }
                RetryDisposition::DoNotRetry => return,
            };
            sess.notify_stream_error(&turn_context, message, error)
                .await;
        })
    })
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ResponsesStreamRequest {
    Sampling,
    LocalCompaction,
    RemoteCompactionV1,
    RemoteCompactionV2,
}

pub(crate) struct ResponsesStreamRetryState {
    retries: u64,
    connection_retries: u64,
    connection_retry_delay: Duration,
    capacity_retries: u64,
    rate_limit_retries: u64,
}

impl Default for ResponsesStreamRetryState {
    fn default() -> Self {
        Self {
            retries: 0,
            connection_retries: 0,
            connection_retry_delay: INITIAL_CONNECTION_RETRY_DELAY,
            capacity_retries: 0,
            rate_limit_retries: 0,
        }
    }
}

pub(crate) fn should_retry_response_stream_error(
    _request: ResponsesStreamRequest,
    err: &CodexErr,
) -> bool {
    response_error_retry_disposition(err) != RetryDisposition::DoNotRetry
}

/// Classify an error after the API bridge has converted it to a protocol error.
///
/// Capacity has a persistent outer budget. Keep this classification separate from the finite
/// transient budget so a capacity body hidden inside `UnexpectedStatus` or `Stream` does not
/// accidentally consume the ordinary retry limit.
fn response_error_retry_disposition(err: &CodexErr) -> RetryDisposition {
    match err.details() {
        CodexErrorDetails::ServerOverloaded => RetryDisposition::Capacity,
        CodexErrorDetails::Timeout
        | CodexErrorDetails::RequestTimeout
        | CodexErrorDetails::InternalServerError => RetryDisposition::Transient,
        // These variants can be produced by local task, parser, or auth plumbing. They are
        // retryable only when the producer has explicitly classified them as transient.
        CodexErrorDetails::InternalAgentDied
        | CodexErrorDetails::Io(_)
        | CodexErrorDetails::Json(_) => {
            if err.is_explicitly_retryable() {
                RetryDisposition::Transient
            } else {
                RetryDisposition::DoNotRetry
            }
        }
        CodexErrorDetails::Stream(message) => {
            if !err.is_explicitly_retryable() {
                RetryDisposition::DoNotRetry
            } else if is_capacity_error_body(message) {
                RetryDisposition::Capacity
            } else if !is_permanent_error_text(message) {
                RetryDisposition::Transient
            } else {
                RetryDisposition::DoNotRetry
            }
        }
        // A join error means the producer task was cancelled or panicked. Retrying can hide a
        // shutdown signal or repeatedly restart a broken task, so both cases are terminal here.
        CodexErrorDetails::TokioJoin(_) => RetryDisposition::DoNotRetry,
        CodexErrorDetails::UnexpectedStatus(error) => {
            classify_http_response(error.status, Some(&error.body))
        }
        CodexErrorDetails::RetryLimit(error) => {
            if is_transient_http_status(error.status) {
                RetryDisposition::Transient
            } else {
                RetryDisposition::DoNotRetry
            }
        }
        CodexErrorDetails::ResponseStreamFailed(error) => match error.source.status() {
            Some(status) if is_transient_http_status(status) => RetryDisposition::Transient,
            Some(_) => RetryDisposition::DoNotRetry,
            None => classify_connection_error(&error.source),
        },
        CodexErrorDetails::ConnectionFailed(error) => classify_connection_error(&error.source),
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
        | CodexErrorDetails::EnvVar(_) => RetryDisposition::DoNotRetry,
        #[cfg(target_os = "linux")]
        CodexErrorDetails::LandlockRuleset(_) | CodexErrorDetails::LandlockPathFd(_) => {
            RetryDisposition::DoNotRetry
        }
    }
}

/// Cancellation-aware variant used by an active turn. A cancelled turn must stop during a
/// backoff window instead of starting another provider request.
pub(crate) async fn handle_retryable_response_stream_error_with_cancellation(
    retry_state: &mut ResponsesStreamRetryState,
    max_retries: u64,
    err: CodexErr,
    client_session: &mut ModelClientSession,
    sess: &Session,
    turn_context: &TurnContext,
    request: ResponsesStreamRequest,
    cancellation_token: &CancellationToken,
) -> Result<(), CodexErr> {
    handle_retryable_response_error_inner(
        retry_state,
        max_retries,
        err,
        Some(client_session),
        sess,
        turn_context,
        request,
        Some(cancellation_token),
    )
    .await
}

/// Cancellation-aware variant for unary compaction requests. It prevents a cancelled manual or
/// automatic compaction from sleeping through a retry delay and issuing another request.
pub(crate) async fn handle_retryable_response_error_with_cancellation(
    retry_state: &mut ResponsesStreamRetryState,
    max_retries: u64,
    err: CodexErr,
    sess: &Session,
    turn_context: &TurnContext,
    request: ResponsesStreamRequest,
    cancellation_token: &CancellationToken,
) -> Result<(), CodexErr> {
    handle_retryable_response_error_inner(
        retry_state,
        max_retries,
        err,
        None,
        sess,
        turn_context,
        request,
        Some(cancellation_token),
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
    cancellation_token: Option<&CancellationToken>,
) -> Result<(), CodexErr> {
    if cancellation_token.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
        return Err(CodexErr::TurnAborted);
    }

    let operation = match request {
        ResponsesStreamRequest::Sampling => RetryOperation::Sampling,
        ResponsesStreamRequest::LocalCompaction => RetryOperation::LocalCompaction,
        ResponsesStreamRequest::RemoteCompactionV1 => RetryOperation::RemoteCompactionV1,
        ResponsesStreamRequest::RemoteCompactionV2 => RetryOperation::RemoteCompactionV2,
    };
    let can_fallback_from_websocket = client_session.is_some()
        && sess.services.model_client.responses_websocket_enabled()
        && is_websocket_endpoint_unavailable(&err);
    if !should_retry_response_stream_error(request, &err) && !can_fallback_from_websocket {
        return Err(err);
    }

    if response_error_retry_disposition(&err) == RetryDisposition::Capacity {
        if retry_state.capacity_retries == MAX_CAPACITY_RETRIES {
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
        wait_for_retry(delay, cancellation_token).await?;
        return Ok(());
    }

    // A low-level request can still return a 429 after its configured transport budget. Give
    // that provider rate limit the same bounded outer budget as the stream request instead of
    // surfacing it immediately, but do not turn it into an unbounded loop.
    if matches!(
        err.details(),
        CodexErrorDetails::RetryLimit(error) if error.status == StatusCode::TOO_MANY_REQUESTS
    ) {
        if retry_state.rate_limit_retries >= max_retries {
            return Err(err);
        }
        retry_state.rate_limit_retries = retry_state.rate_limit_retries.saturating_add(1);
        let retry_count = retry_state.rate_limit_retries;
        let delay = backoff(retry_count).min(MAX_RETRY_DELAY);
        warn!(
            turn_id = %turn_context.sub_id,
            retries = retry_count,
            max_retries = format_retry_budget(max_retries),
            ?delay,
            "rate limit exhausted a transport retry budget; waiting to retry Responses request"
        );
        sess.notify_stream_error(
            turn_context,
            format!(
                "Rate limited. Retrying in {} (attempt {}/{})",
                format_retry_delay(delay),
                retry_count,
                format_retry_budget(max_retries),
            ),
            err,
        )
        .await;
        codex_client::record_retry!(retry_count, delay, operation);
        wait_for_retry(delay, cancellation_token).await?;
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
                format_retry_delay(retry_delay),
                max_retries = format_retry_budget(max_retries)
            ),
            err,
        )
        .await;
        codex_client::record_retry!(retry_count, retry_delay, operation);
        wait_for_retry(retry_delay, cancellation_token).await?;
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
                format_retry_delay(delay),
                max_retries = format_retry_budget(max_retries)
            ),
            err,
        )
        .await;
        codex_client::record_retry!(retry_count, delay, operation);
        wait_for_retry(delay, cancellation_token).await?;
        return Ok(());
    }

    Err(err)
}

async fn wait_for_retry(
    delay: Duration,
    cancellation_token: Option<&CancellationToken>,
) -> Result<(), CodexErr> {
    if let Some(cancellation_token) = cancellation_token {
        tokio::select! {
            _ = tokio::time::sleep(delay) => Ok(()),
            _ = cancellation_token.cancelled() => Err(CodexErr::TurnAborted),
        }
    } else {
        tokio::time::sleep(delay).await;
        Ok(())
    }
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
                "stream disconnected - retrying sampling request ({retries}/{} in {delay:?})...",
                format_retry_budget(max_retries),
            );
        }
        ResponsesStreamRequest::LocalCompaction => {
            warn!(
                turn_id = %turn_context.sub_id,
                retries,
                max_retries,
                compact_error = %err,
                "local compaction stream failed; retrying request after delay"
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
