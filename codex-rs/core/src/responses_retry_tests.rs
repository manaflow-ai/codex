use super::ResponsesStreamRequest;
use super::capacity_retry_delay;
use super::log_retry;
use super::should_retry_response_stream_error;
use crate::session::tests::make_session_and_context;
use codex_protocol::error::CodexErr;
use codex_protocol::error::RetryLimitReachedError;
use codex_protocol::error::UnexpectedResponseError;
use http::StatusCode;
use std::time::Duration;
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
    assert!(!should_retry_response_stream_error(
        ResponsesStreamRequest::Sampling,
        &CodexErr::TurnAborted
    ));
}

#[test]
fn capacity_retry_delay_uses_exponential_backoff_with_a_sixty_second_cap() {
    let first = capacity_retry_delay(1);
    let second = capacity_retry_delay(2);

    assert!((Duration::from_millis(180)..Duration::from_millis(220)).contains(&first));
    assert!((Duration::from_millis(360)..Duration::from_millis(440)).contains(&second));
    assert_eq!(capacity_retry_delay(u64::MAX), Duration::from_secs(60));
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
