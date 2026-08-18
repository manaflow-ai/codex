use super::ResponsesStreamRequest;
use super::capacity_retry_delay;
use super::handle_retryable_response_stream_error_with_cancellation;
use super::log_retry;
use super::retry_status_notifier;
use super::should_retry_response_stream_error;
use crate::session::tests::make_session_and_context;
use crate::session::tests::make_session_and_context_with_rx;
use codex_client::RetryDisposition;
use codex_client::RetryStatus;
use codex_protocol::error::CodexErr;
use codex_protocol::error::RetryLimitReachedError;
use codex_protocol::error::UnexpectedResponseError;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use http::StatusCode;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing_test::internal::MockWriter;

#[test]
fn overloads_are_retryable_for_sampling_and_remote_compaction() {
    let err = CodexErr::ServerOverloaded;

    assert!(!err.is_retryable());
    assert!(should_retry_response_stream_error(
        ResponsesStreamRequest::Sampling,
        &err
    ));
    assert!(should_retry_response_stream_error(
        ResponsesStreamRequest::RemoteCompactionV2,
        &err
    ));
}

#[tokio::test]
async fn subagent_capacity_retry_emits_status_without_becoming_terminal() {
    let (session, mut turn_context, events) = make_session_and_context_with_rx().await;
    Arc::get_mut(&mut turn_context)
        .expect("test should hold the only turn context reference")
        .session_source = SessionSource::SubAgent(SubAgentSource::Review);
    let mut client_session = session.services.model_client.new_session();
    let mut retry_state = super::ResponsesStreamRetryState::default();

    let result = handle_retryable_response_stream_error_with_cancellation(
        &mut retry_state,
        /*max_retries*/ 0,
        CodexErr::ServerOverloaded,
        &mut client_session,
        &session,
        &turn_context,
        ResponsesStreamRequest::Sampling,
        &CancellationToken::new(),
    )
    .await;

    assert!(result.is_ok());
    assert_eq!(retry_state.capacity_retries, 1);
    let event = events
        .recv()
        .await
        .expect("capacity retry should emit status");
    assert!(matches!(
        event.msg,
        EventMsg::StreamError(ref retry)
            if retry.message.starts_with("Model at capacity. Retrying in ")
    ));
}

#[tokio::test]
async fn wrapped_capacity_http_errors_use_the_persistent_outer_budget() {
    let (session, turn_context, events) = make_session_and_context_with_rx().await;
    let mut client_session = session.services.model_client.new_session();
    let mut retry_state = super::ResponsesStreamRetryState::default();
    let error = CodexErr::UnexpectedStatus(UnexpectedResponseError {
        status: StatusCode::BAD_REQUEST,
        body: r#"{"error":{"type":"invalid_request_error","message":"Selected model is at capacity. Please try a different model."}}"#
            .to_string(),
        user_message: None,
        url: None,
        cf_ray: None,
        request_id: None,
        identity_authorization_error: None,
        identity_error_code: None,
    });

    let result = handle_retryable_response_stream_error_with_cancellation(
        &mut retry_state,
        /*max_retries*/ 0,
        error,
        &mut client_session,
        &session,
        &turn_context,
        ResponsesStreamRequest::Sampling,
        &CancellationToken::new(),
    )
    .await;

    assert!(
        result.is_ok(),
        "capacity should use the separate outer budget"
    );
    assert_eq!(retry_state.capacity_retries, 1);
    let event = events
        .recv()
        .await
        .expect("capacity retry should emit status");
    assert!(matches!(
        event.msg,
        EventMsg::StreamError(ref retry)
            if retry.message.starts_with("Model at capacity. Retrying in ")
    ));
}

#[tokio::test]
async fn low_level_http_retry_status_uses_the_ui_event_channel() {
    let (session, turn_context, events) = make_session_and_context_with_rx().await;
    let notifier = retry_status_notifier(Arc::clone(&session), Arc::clone(&turn_context));

    notifier(RetryStatus {
        operation: "http/request".to_string(),
        disposition: RetryDisposition::Transient,
        attempt: 1,
        max_retries: 4,
        delay: Duration::from_secs(1),
        error: "connection reset".to_string(),
    })
    .await;

    let event = events.recv().await.expect("retry status should be sent");
    assert!(matches!(
        event.msg,
        EventMsg::StreamError(ref retry)
            if retry.message
                == "Request failed. Retrying in 1.0s (HTTP attempt 1/4)"
    ));
}

#[test]
fn exhausted_transient_http_retries_remain_retryable_at_the_request_layer() {
    let err = CodexErr::RetryLimit(RetryLimitReachedError {
        status: StatusCode::TOO_MANY_REQUESTS,
        request_id: Some("request-1".to_string()),
    });

    assert!(should_retry_response_stream_error(
        ResponsesStreamRequest::Sampling,
        &err
    ));
    assert!(should_retry_response_stream_error(
        ResponsesStreamRequest::RemoteCompactionV2,
        &err
    ));
}

#[test]
fn permanent_http_status_and_user_cancellation_are_not_retried() {
    let bad_request = CodexErr::UnexpectedStatus(UnexpectedResponseError {
        status: StatusCode::BAD_REQUEST,
        body: "bad request".to_string(),
        user_message: None,
        url: None,
        cf_ray: None,
        request_id: None,
        identity_authorization_error: None,
        identity_error_code: None,
    });

    assert!(!should_retry_response_stream_error(
        ResponsesStreamRequest::Sampling,
        &bad_request
    ));

    let permanent_503 = CodexErr::UnexpectedStatus(UnexpectedResponseError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        body: "account_suspended".to_string(),
        user_message: None,
        url: None,
        cf_ray: None,
        request_id: None,
        identity_authorization_error: None,
        identity_error_code: None,
    });
    assert!(!should_retry_response_stream_error(
        ResponsesStreamRequest::Sampling,
        &permanent_503
    ));
    assert!(!should_retry_response_stream_error(
        ResponsesStreamRequest::Sampling,
        &CodexErr::TurnAborted
    ));
}

#[test]
fn unmarked_stream_errors_are_not_retried_for_legacy_compaction() {
    let stream = CodexErr::Stream("malformed compaction response".to_string());
    assert!(!should_retry_response_stream_error(
        ResponsesStreamRequest::RemoteCompactionV1,
        &stream
    ));

    let provider_retryable = stream.with_explicit_retryable();
    assert!(should_retry_response_stream_error(
        ResponsesStreamRequest::RemoteCompactionV1,
        &provider_retryable
    ));
}

#[test]
fn wrapped_capacity_stream_errors_remain_retryable_after_http_mapping() {
    let error = CodexErr::Stream(
        r#"{"error":{"type":"invalid_request_error","message":"Selected model is at capacity. Please try a different model."}}"#
            .to_string(),
    )
    .with_explicit_retryable();

    assert!(should_retry_response_stream_error(
        ResponsesStreamRequest::Sampling,
        &error
    ));
}

#[test]
fn unclassified_local_errors_are_not_retried_for_sampling() {
    let stream = CodexErr::Stream("malformed provider frame".to_string());
    let json = CodexErr::Json(serde_json::from_str::<serde_json::Value>("not-json").unwrap_err());
    let io = CodexErr::Io(std::io::Error::other("local pipe failed"));

    for error in [stream, json, io, CodexErr::InternalAgentDied] {
        assert!(
            !should_retry_response_stream_error(ResponsesStreamRequest::Sampling, &error),
            "unclassified local error should be terminal: {error:?}"
        );
    }

    assert!(should_retry_response_stream_error(
        ResponsesStreamRequest::Sampling,
        &CodexErr::Stream("provider closed the stream".to_string()).with_explicit_retryable()
    ));
}

#[tokio::test]
async fn task_join_failures_are_not_retried() {
    let panic_error = tokio::spawn(async { panic!("test task panic") })
        .await
        .expect_err("task should panic");
    let panic_error = CodexErr::from(panic_error);
    assert!(!should_retry_response_stream_error(
        ResponsesStreamRequest::Sampling,
        &panic_error
    ));

    let cancelled_task = tokio::spawn(async { std::future::pending::<()>().await });
    cancelled_task.abort();
    let cancelled_error =
        CodexErr::from(cancelled_task.await.expect_err("task should be cancelled"));
    assert!(!should_retry_response_stream_error(
        ResponsesStreamRequest::Sampling,
        &cancelled_error
    ));
}

#[tokio::test]
async fn cancelled_turn_stops_before_retry_backoff() {
    let (session, turn_context) = make_session_and_context().await;
    let mut client_session = session.services.model_client.new_session();
    let mut retry_state = super::ResponsesStreamRetryState::default();
    let cancellation_token = CancellationToken::new();
    cancellation_token.cancel();

    let result = handle_retryable_response_stream_error_with_cancellation(
        &mut retry_state,
        5,
        CodexErr::ServerOverloaded,
        &mut client_session,
        &session,
        &turn_context,
        ResponsesStreamRequest::Sampling,
        &cancellation_token,
    )
    .await;

    assert!(
        matches!(result, Err(error) if matches!(error.details(), codex_protocol::error::CodexErrorDetails::TurnAborted))
    );
    assert_eq!(retry_state.retries, 0);
    assert_eq!(retry_state.capacity_retries, 0);
}

#[tokio::test]
async fn exhausted_outer_rate_limit_budget_is_terminal() {
    let (session, turn_context) = make_session_and_context().await;
    let mut client_session = session.services.model_client.new_session();
    let mut retry_state = super::ResponsesStreamRetryState::default();
    let error = CodexErr::RetryLimit(RetryLimitReachedError {
        status: StatusCode::TOO_MANY_REQUESTS,
        request_id: None,
    });

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        handle_retryable_response_stream_error_with_cancellation(
            &mut retry_state,
            /*max_retries*/ 0,
            error,
            &mut client_session,
            &session,
            &turn_context,
            ResponsesStreamRequest::Sampling,
            &CancellationToken::new(),
        ),
    )
    .await
    .expect("an exhausted rate-limit budget must not loop forever");

    assert!(matches!(
        result,
        Err(error) if matches!(error.details(), codex_protocol::error::CodexErrorDetails::RetryLimit(_))
    ));
}

#[tokio::test]
async fn capacity_budget_guard_is_terminal_when_state_is_already_over_limit() {
    let (session, turn_context) = make_session_and_context().await;
    let mut client_session = session.services.model_client.new_session();
    let mut retry_state = super::ResponsesStreamRetryState::default();
    retry_state.capacity_retries = super::MAX_CAPACITY_RETRIES + 1;

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        handle_retryable_response_stream_error_with_cancellation(
            &mut retry_state,
            /*max_retries*/ 0,
            CodexErr::ServerOverloaded,
            &mut client_session,
            &session,
            &turn_context,
            ResponsesStreamRequest::Sampling,
            &CancellationToken::new(),
        ),
    )
    .await
    .expect("an over-budget capacity state must not sleep or retry");

    assert!(matches!(
        result,
        Err(error) if matches!(error.details(), codex_protocol::error::CodexErrorDetails::ServerOverloaded)
    ));
    assert_eq!(retry_state.capacity_retries, super::MAX_CAPACITY_RETRIES + 1);
}

#[test]
fn capacity_retry_delay_uses_exponential_backoff_with_a_sixty_second_cap() {
    let first = capacity_retry_delay(1);
    let second = capacity_retry_delay(2);

    assert!((Duration::from_millis(180)..Duration::from_millis(220)).contains(&first));
    assert!((Duration::from_millis(360)..Duration::from_millis(440)).contains(&second));
    // Jitter may shorten the capped delay, but it must never exceed the one-minute bound.
    let capped = capacity_retry_delay(u64::MAX);
    assert!(
        (Duration::from_secs(53)..=Duration::from_secs(60)).contains(&capped),
        "capacity retry delay escaped its bounded jitter window: {capped:?}"
    );
}

#[tokio::test]
async fn sampling_retry_logs_stream_error_context() {
    let (_session, turn_context) = make_session_and_context().await;
    let buffer: &'static std::sync::Mutex<Vec<u8>> =
        Box::leak(Box::new(std::sync::Mutex::new(Vec::new())));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(MockWriter::new(buffer))
        .finish();
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    log_retry(
        ResponsesStreamRequest::Sampling,
        &turn_context,
        &CodexErr::Stream("websocket closed by server before response.completed".to_string()),
        /*retries*/ 2,
        /*max_retries*/ 5,
        Duration::from_secs(1),
    );

    let logs = String::from_utf8(
        buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )
    .expect("retry log should be valid utf-8");
    assert!(logs.contains("stream disconnected - retrying sampling request"));
    assert!(logs.contains(&format!("turn_id={}", turn_context.sub_id)));
    assert!(logs.contains("retries=2"));
    assert!(logs.contains("max_retries=5"));
    assert!(logs.contains(
        "sampling_error=stream disconnected before completion: websocket closed by server before response.completed"
    ));
}
