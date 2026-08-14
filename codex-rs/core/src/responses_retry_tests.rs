use super::ResponsesStreamRequest;
use super::capacity_retry_delay;
use super::log_retry;
use super::should_retry_response_stream_error;
use crate::session::tests::make_session_and_context;
use codex_protocol::error::CodexErr;
use std::time::Duration;
use tracing_test::internal::MockWriter;

#[test]
fn sampling_overloads_are_persistent_without_changing_compaction_retryability() {
    let err = CodexErr::ServerOverloaded;

    assert!(!err.is_retryable());
    assert!(should_retry_response_stream_error(
        ResponsesStreamRequest::Sampling,
        &err
    ));
    assert!(!should_retry_response_stream_error(
        ResponsesStreamRequest::RemoteCompactionV2,
        &err
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
