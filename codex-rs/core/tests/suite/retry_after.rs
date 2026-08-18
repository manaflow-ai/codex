use anyhow::Result;
use codex_features::Feature;
use codex_login::CodexAuth;
use codex_protocol::protocol::CodexErrorInfo;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::StreamErrorEvent;
use codex_protocol::turn_input::TurnInputRequest;
use codex_protocol::user_input::UserInput;
use core_test_support::responses;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event_with_timeout;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::net::TcpListener;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::Event;
use tracing::Subscriber;
use tracing::dispatcher::DefaultGuard;
use tracing::field::Field;
use tracing::field::Visit;
use tracing::span::Attributes;
use tracing::span::Id;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use wiremock::MockServer;
use wiremock::ResponseTemplate;

const FIRST_RETRY_MIN_DELAY: Duration = Duration::from_millis(180);
const FIRST_RETRY_MAX_DELAY: Duration = Duration::from_millis(220);
const SECOND_RETRY_MIN_DELAY: Duration = Duration::from_millis(360);
const SECOND_RETRY_MAX_DELAY: Duration = Duration::from_millis(440);

#[derive(Debug, PartialEq, Eq)]
struct RetryTelemetryEvent {
    attempt: u64,
    delay: Duration,
    layer: String,
    operation: String,
}

#[derive(Default)]
struct RetryTelemetryVisitor {
    name: Option<String>,
    attempt: Option<u64>,
    delay_ms: Option<u64>,
    layer: Option<String>,
    operation: Option<String>,
}

impl Visit for RetryTelemetryVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "retry.attempt" => self.attempt = Some(value),
            "retry.delay_ms" => self.delay_ms = Some(value),
            _ => {}
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if let Ok(value) = u64::try_from(value) {
            self.record_u64(field, value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "event.name" => self.name = Some(value.to_string()),
            "retry.layer" => self.layer = Some(value.to_string()),
            "retry.operation" => self.operation = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let value = format!("{value:?}");
        self.record_str(field, value.trim_matches('"'));
    }
}

struct RetryTelemetryLayer {
    events: mpsc::UnboundedSender<RetryTelemetryEvent>,
    resumptions: mpsc::UnboundedSender<Duration>,
    pending_retry: Mutex<Option<Instant>>,
}

impl RetryTelemetryLayer {
    fn record_request_after_retry(&self) {
        let started = self
            .pending_retry
            .lock()
            .expect("pending retry should not be poisoned")
            .take();
        if let Some(started) = started {
            let _ = self.resumptions.send(started.elapsed());
        }
    }
}

impl<S> Layer<S> for RetryTelemetryLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        if event.metadata().target() == "codex_http_client::transport" {
            self.record_request_after_retry();
            return;
        }

        if event.metadata().target() != "codex_otel.trace_safe" {
            return;
        }

        let mut visitor = RetryTelemetryVisitor::default();
        event.record(&mut visitor);
        if visitor.name.as_deref() != Some("codex.retry") {
            return;
        }

        let retry = RetryTelemetryEvent {
            attempt: visitor
                .attempt
                .expect("retry event should include an attempt"),
            delay: Duration::from_millis(
                visitor
                    .delay_ms
                    .expect("retry event should include its selected delay"),
            ),
            layer: visitor
                .layer
                .expect("retry event should identify its layer"),
            operation: visitor
                .operation
                .expect("retry event should identify its operation"),
        };
        let started = Instant::now();
        *self
            .pending_retry
            .lock()
            .expect("pending retry should not be poisoned") = Some(started);
        let _ = self.events.send(retry);
    }

    fn on_new_span(&self, attributes: &Attributes<'_>, _id: &Id, _context: Context<'_, S>) {
        if attributes.metadata().name() == "responses_websocket.connect" {
            self.record_request_after_retry();
        }
    }
}

struct RetryTelemetryCapture {
    events: mpsc::UnboundedReceiver<RetryTelemetryEvent>,
    resumptions: mpsc::UnboundedReceiver<Duration>,
    _subscriber: DefaultGuard,
}

impl RetryTelemetryCapture {
    fn install() -> Self {
        let (sender, events) = mpsc::unbounded_channel();
        let (resumptions_sender, resumptions) = mpsc::unbounded_channel();
        let subscriber = tracing_subscriber::registry()
            .with(RetryTelemetryLayer {
                events: sender,
                resumptions: resumptions_sender,
                pending_retry: Mutex::new(None),
            })
            .set_default();

        Self {
            events,
            resumptions,
            _subscriber: subscriber,
        }
    }

    async fn next_retry(&mut self) -> RetryTelemetryEvent {
        let retry = self
            .events
            .recv()
            .await
            .expect("retry telemetry subscriber should remain installed");
        // Parallel tests may first register request callsites without our thread-local subscriber.
        tracing::callsite::rebuild_interest_cache();
        retry
    }
}

async fn wait_for_retry(
    telemetry: &mut RetryTelemetryCapture,
    retry: &RetryTelemetryEvent,
) -> Duration {
    let elapsed = telemetry
        .resumptions
        .recv()
        .await
        .expect("retry should start another request after its sleep");
    assert!(
        elapsed >= retry.delay,
        "{} {} retry waited {elapsed:?}, less than its selected {:?} delay",
        retry.layer,
        retry.operation,
        retry.delay
    );
    elapsed
}

async fn submit_user_input(test: &TestCodex, text: &str) -> Result<()> {
    test.codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: text.to_string(),
            text_elements: Vec::new(),
        }]))
        .await?;
    Ok(())
}

async fn wait_for_turn_completion(test: &TestCodex) {
    let EventMsg::TurnComplete(completed) = wait_for_event_with_timeout(
        &test.codex,
        |event| matches!(event, EventMsg::TurnComplete(_)),
        Duration::from_secs(30),
    )
    .await
    else {
        unreachable!("predicate guarantees a turn complete event");
    };
    assert_eq!(completed.error, None, "turn should complete successfully");
}

async fn wait_for_stream_retry_success(
    test: &TestCodex,
    expected_retries: usize,
) -> Vec<StreamErrorEvent> {
    let mut retry_events = Vec::new();
    loop {
        match wait_for_event_with_timeout(&test.codex, |_| true, Duration::from_secs(30)).await {
            EventMsg::StreamError(event) => retry_events.push(event),
            EventMsg::Error(error) => panic!("capacity retry became terminal: {error:?}"),
            EventMsg::TurnComplete(completed) => {
                assert_eq!(
                    completed.error, None,
                    "turn should recover from capacity errors"
                );
                break;
            }
            _ => {}
        }
    }

    assert_eq!(retry_events.len(), expected_retries);
    retry_events
}

async fn wait_for_capacity_retry_success(test: &TestCodex, expected_retries: usize) {
    let retry_events = wait_for_stream_retry_success(test, expected_retries).await;
    for (index, event) in retry_events.iter().enumerate() {
        assert!(
            event.message.starts_with("Model at capacity. Retrying in "),
            "retry status should explain the capacity wait: {}",
            event.message
        );
        assert!(
            event.message.ends_with(&format!("(attempt {})", index + 1))
                || event
                    .message
                    .contains(&format!("(HTTP attempt {}/100)", index + 1)),
            "retry status should expose its attempt and budget: {}",
            event.message
        );
    }
}

// TODO(anp) respect Retry-After
/// HTTP overloads currently retry with local backoff instead of the upstream header delay.
#[tokio::test(flavor = "current_thread")]
async fn responses_http_uses_local_backoff_despite_retry_after() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![
            ResponseTemplate::new(503)
                .insert_header("Retry-After", "1")
                .set_body_json(json!({ "error": { "code": "server_is_overloaded" } })),
            responses::sse_response(responses::sse(vec![
                responses::ev_response_created("recovered"),
                responses::ev_completed("recovered"),
            ])),
        ],
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(1);
            config.model_provider.stream_max_retries = Some(0);
        })
        .build_with_auto_env(&server)
        .await?;

    submit_user_input(&test, "retry the upstream overload").await?;
    let retry = telemetry.next_retry().await;
    assert!((FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY).contains(&retry.delay));
    assert_eq!(
        retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: retry.delay,
            layer: "http".into(),
            operation: "request".into(),
        }
    );
    wait_for_retry(&mut telemetry, &retry).await;
    wait_for_turn_completion(&test).await;

    assert_eq!(response_mock.requests().len(), 2);
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );
    Ok(())
}

/// Headerless HTTP overloads keep retrying after the configured retry budget is exhausted.
#[tokio::test(flavor = "current_thread")]
async fn responses_http_overload_without_retry_after_retries_until_success() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![
            ResponseTemplate::new(503)
                .set_body_json(json!({ "error": { "code": "server_is_overloaded" } })),
            ResponseTemplate::new(503)
                .set_body_json(json!({ "error": { "code": "server_is_overloaded" } })),
            responses::sse_response(responses::sse(vec![
                responses::ev_response_created("recovered"),
                responses::ev_completed("recovered"),
            ])),
        ],
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(0);
        })
        .build_with_auto_env(&server)
        .await?;

    submit_user_input(&test, "wait for the model to have capacity").await?;
    for attempt in 1..=2 {
        let retry = telemetry.next_retry().await;
        let expected_range = if attempt == 1 {
            FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY
        } else {
            SECOND_RETRY_MIN_DELAY..SECOND_RETRY_MAX_DELAY
        };
        assert!(expected_range.contains(&retry.delay));
        assert_eq!(
            retry,
            RetryTelemetryEvent {
                attempt,
                delay: retry.delay,
                layer: "http".into(),
                operation: "request".into(),
            }
        );
        wait_for_retry(&mut telemetry, &retry).await;
    }
    wait_for_capacity_retry_success(&test, /*expected_retries*/ 2).await;

    assert_eq!(response_mock.requests().len(), 3);
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );

    Ok(())
}

// TODO(anp) respect Retry-After
/// Remote compaction v2 currently retries with local backoff instead of the upstream header delay.
#[tokio::test(flavor = "current_thread")]
async fn compact_v2_uses_local_backoff_despite_retry_after() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![
            responses::sse_response(responses::sse(vec![
                responses::ev_response_created("seed"),
                responses::ev_completed("seed"),
            ])),
            ResponseTemplate::new(503)
                .insert_header("Retry-After", "1")
                .set_body_json(json!({ "error": { "code": "server_is_overloaded" } })),
            responses::sse_response(responses::sse(vec![
                json!({
                    "type": "response.output_item.done",
                    "item": {
                        "type": "compaction",
                        "encrypted_content": "RETRIED_COMPACTION_SUMMARY",
                    }
                }),
                responses::ev_completed("compacted"),
            ])),
        ],
    )
    .await;
    let test = test_codex()
        .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(1);
            config.model_provider.stream_max_retries = Some(0);
        })
        .build_with_auto_env(&server)
        .await?;
    test.submit_turn("seed history for compaction").await?;

    test.codex.submit(Op::Compact).await?;
    let retry = telemetry.next_retry().await;
    assert!((FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY).contains(&retry.delay));
    assert_eq!(
        retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: retry.delay,
            layer: "http".into(),
            operation: "request".into(),
        }
    );
    wait_for_retry(&mut telemetry, &retry).await;
    wait_for_turn_completion(&test).await;

    let requests = response_mock.requests();
    assert_eq!(requests.len(), 3);
    for request in &requests[1..] {
        assert_eq!(request.path(), "/v1/responses");
        assert!(
            !request.inputs_of_type("compaction_trigger").is_empty(),
            "expected a remote compaction v2 request"
        );
    }
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );
    Ok(())
}

// TODO(anp) respect Retry-After
/// Remote compaction v2 stream failures retry without using the enclosing response header.
#[tokio::test(flavor = "current_thread")]
async fn compact_v2_stream_failure_uses_local_backoff_despite_retry_after() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![
            responses::sse_response(responses::sse(vec![
                responses::ev_response_created("seed"),
                responses::ev_completed("seed"),
            ])),
            responses::sse_response(responses::sse_failed(
                "rate-limited",
                "rate_limit_exceeded",
                "Rate limit exceeded.",
            ))
            .insert_header("Retry-After", "1"),
            responses::sse_response(responses::sse(vec![
                json!({
                    "type": "response.output_item.done",
                    "item": {
                        "type": "compaction",
                        "encrypted_content": "RETRIED_COMPACTION_SUMMARY",
                    }
                }),
                responses::ev_completed("compacted"),
            ])),
        ],
    )
    .await;
    let test = test_codex()
        .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(1);
        })
        .build_with_auto_env(&server)
        .await?;
    test.submit_turn("seed history for compaction").await?;

    test.codex.submit(Op::Compact).await?;
    let retry = telemetry.next_retry().await;
    assert!((FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY).contains(&retry.delay));
    assert_eq!(
        retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: retry.delay,
            layer: "stream".into(),
            operation: "remote_compaction_v2".into(),
        }
    );
    wait_for_retry(&mut telemetry, &retry).await;
    wait_for_turn_completion(&test).await;

    let requests = response_mock.requests();
    assert_eq!(requests.len(), 3);
    for request in &requests[1..] {
        assert_eq!(request.path(), "/v1/responses");
        assert!(
            !request.inputs_of_type("compaction_trigger").is_empty(),
            "expected a remote compaction v2 request"
        );
    }
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );

    Ok(())
}

/// Headerless remote compaction stream rate limits exhaust retries before one terminal error.
#[tokio::test(flavor = "current_thread")]
async fn compact_v2_stream_failure_without_retry_after_exhausts_stream_retries() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![
            responses::sse_response(responses::sse(vec![
                responses::ev_response_created("seed"),
                responses::ev_completed("seed"),
            ])),
            responses::sse_response(responses::sse_failed(
                "rate-limited",
                "rate_limit_exceeded",
                "Rate limit exceeded.",
            )),
            responses::sse_response(responses::sse_failed(
                "still-rate-limited",
                "rate_limit_exceeded",
                "Rate limit exceeded.",
            )),
        ],
    )
    .await;
    let test = test_codex()
        .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(1);
        })
        .build_with_auto_env(&server)
        .await?;
    test.submit_turn("seed history for compaction").await?;

    test.codex.submit(Op::Compact).await?;
    let retry = telemetry.next_retry().await;
    assert!((FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY).contains(&retry.delay));
    assert_eq!(
        retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: retry.delay,
            layer: "stream".into(),
            operation: "remote_compaction_v2".into(),
        }
    );
    wait_for_retry(&mut telemetry, &retry).await;

    let mut error_events = 0;
    let mut stream_error_events = 0;
    loop {
        match wait_for_event_with_timeout(&test.codex, |_| true, Duration::from_secs(30)).await {
            EventMsg::Error(error) => {
                error_events += 1;
                assert_eq!(error.codex_error_info, Some(CodexErrorInfo::Other));
                assert!(error.message.contains("Rate limit exceeded."));
            }
            EventMsg::StreamError(_) => stream_error_events += 1,
            EventMsg::TurnComplete(event) => {
                assert_eq!(
                    event.error.and_then(|error| error.codex_error_info),
                    Some(CodexErrorInfo::Other)
                );
                break;
            }
            _ => {}
        }
    }

    assert_eq!(error_events, 1);
    assert_eq!(stream_error_events, 1);
    let requests = response_mock.requests();
    assert_eq!(requests.len(), 3);
    for request in &requests[1..] {
        assert_eq!(request.path(), "/v1/responses");
        assert!(
            !request.inputs_of_type("compaction_trigger").is_empty(),
            "expected a remote compaction v2 request"
        );
    }
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );

    Ok(())
}

// TODO(anp) respect Retry-After
/// Remote compaction v2 already honors exact retry advice embedded in rate-limit messages.
#[tokio::test(flavor = "current_thread")]
async fn compact_v2_rate_limit_message_uses_server_advised_retry_delay() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![
            responses::sse_response(responses::sse(vec![
                responses::ev_response_created("seed"),
                responses::ev_completed("seed"),
            ])),
            responses::sse_response(responses::sse_failed(
                "rate-limited",
                "rate_limit_exceeded",
                "Rate limit exceeded. Please try again in 1s.",
            ))
            .insert_header("Retry-After", "2"),
            responses::sse_response(responses::sse(vec![
                json!({
                    "type": "response.output_item.done",
                    "item": {
                        "type": "compaction",
                        "encrypted_content": "RETRIED_COMPACTION_SUMMARY",
                    }
                }),
                responses::ev_completed("compacted"),
            ])),
        ],
    )
    .await;
    let test = test_codex()
        .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(1);
        })
        .build_with_auto_env(&server)
        .await?;
    test.submit_turn("seed history for compaction").await?;

    test.codex.submit(Op::Compact).await?;
    let retry = telemetry.next_retry().await;
    assert_eq!(
        retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: Duration::from_secs(1),
            layer: "stream".into(),
            operation: "remote_compaction_v2".into(),
        }
    );
    assert!(wait_for_retry(&mut telemetry, &retry).await >= Duration::from_secs(1));
    wait_for_turn_completion(&test).await;

    let requests = response_mock.requests();
    assert_eq!(requests.len(), 3);
    for request in &requests[1..] {
        assert_eq!(request.path(), "/v1/responses");
        assert!(
            !request.inputs_of_type("compaction_trigger").is_empty(),
            "expected a remote compaction v2 request"
        );
    }
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );

    Ok(())
}

/// Remote compaction rate-limit messages provide exact retry advice without an HTTP header.
#[tokio::test(flavor = "current_thread")]
async fn compact_v2_rate_limit_message_without_retry_after_uses_server_advised_delay() -> Result<()>
{
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![
            responses::sse_response(responses::sse(vec![
                responses::ev_response_created("seed"),
                responses::ev_completed("seed"),
            ])),
            responses::sse_response(responses::sse_failed(
                "rate-limited",
                "rate_limit_exceeded",
                "Rate limit exceeded. Please try again in 1s.",
            )),
            responses::sse_response(responses::sse(vec![
                json!({
                    "type": "response.output_item.done",
                    "item": {
                        "type": "compaction",
                        "encrypted_content": "RETRIED_COMPACTION_SUMMARY",
                    }
                }),
                responses::ev_completed("compacted"),
            ])),
        ],
    )
    .await;
    let test = test_codex()
        .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(1);
        })
        .build_with_auto_env(&server)
        .await?;
    test.submit_turn("seed history for compaction").await?;

    test.codex.submit(Op::Compact).await?;
    let retry = telemetry.next_retry().await;
    assert_eq!(
        retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: Duration::from_secs(1),
            layer: "stream".into(),
            operation: "remote_compaction_v2".into(),
        }
    );
    assert!(wait_for_retry(&mut telemetry, &retry).await >= Duration::from_secs(1));
    wait_for_turn_completion(&test).await;

    let requests = response_mock.requests();
    assert_eq!(requests.len(), 3);
    for request in &requests[1..] {
        assert_eq!(request.path(), "/v1/responses");
        assert!(
            !request.inputs_of_type("compaction_trigger").is_empty(),
            "expected a remote compaction v2 request"
        );
    }
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );

    Ok(())
}

/// Remote compaction v2 retries a capacity failure after the HTTP retry budget is exhausted.
#[tokio::test(flavor = "current_thread")]
async fn compact_v2_capacity_failure_retries_until_success() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![
            responses::sse_response(responses::sse(vec![
                responses::ev_response_created("seed"),
                responses::ev_completed("seed"),
            ])),
            ResponseTemplate::new(503)
                .set_body_json(json!({ "error": { "code": "server_is_overloaded" } })),
            ResponseTemplate::new(503)
                .set_body_json(json!({ "error": { "code": "server_is_overloaded" } })),
            ResponseTemplate::new(503)
                .set_body_json(json!({ "error": { "code": "server_is_overloaded" } })),
            responses::sse_response(responses::sse(vec![
                json!({
                    "type": "response.output_item.done",
                    "item": {
                        "type": "compaction",
                        "encrypted_content": "RECOVERED_AFTER_CAPACITY",
                    }
                }),
                responses::ev_completed("compacted"),
            ])),
        ],
    )
    .await;
    let test = test_codex()
        .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(2);
            config.model_provider.stream_max_retries = Some(2);
        })
        .build_with_auto_env(&server)
        .await?;
    test.submit_turn("seed history for compaction").await?;

    test.codex.submit(Op::Compact).await?;
    for attempt in 1..=3 {
        let retry = telemetry.next_retry().await;
        let expected_range = match attempt {
            1 => FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY,
            2 => SECOND_RETRY_MIN_DELAY..SECOND_RETRY_MAX_DELAY,
            _ => SECOND_RETRY_MIN_DELAY..Duration::from_millis(900),
        };
        assert!(expected_range.contains(&retry.delay));
        assert_eq!(
            retry,
            RetryTelemetryEvent {
                attempt,
                delay: retry.delay,
                layer: "http".into(),
                operation: "request".into(),
            }
        );
        wait_for_retry(&mut telemetry, &retry).await;
    }
    let retry_events = wait_for_stream_retry_success(&test, /*expected_retries*/ 3).await;
    assert!(
        retry_events
            .iter()
            .all(|event| event.message.starts_with("Model at capacity. Retrying in "))
    );
    let requests = response_mock.requests();
    assert_eq!(
        requests.len(),
        5,
        "expected a seed request, three HTTP attempts, and one capacity retry"
    );
    for request in &requests[1..] {
        assert_eq!(request.path(), "/v1/responses");
        assert!(
            !request.inputs_of_type("compaction_trigger").is_empty(),
            "expected a remote compaction v2 request"
        );
    }
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );

    Ok(())
}

/// Legacy remote compaction retries a capacity response instead of ending the compact turn.
#[tokio::test(flavor = "current_thread")]
async fn compact_v1_capacity_failure_retries_until_success() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = responses::start_mock_server().await;
    let seed_mock = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("seed"),
            responses::ev_completed("seed"),
        ]),
    )
    .await;
    let compact_mock = responses::mount_compact_response_sequence(
        &server,
        vec![
            ResponseTemplate::new(503)
                .set_body_json(json!({ "error": { "code": "server_is_overloaded" } })),
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_json(json!({
                    "output": [{
                        "type": "compaction",
                        "encrypted_content": "RECOVERED_LEGACY_COMPACTION",
                    }]
                })),
        ],
    )
    .await;
    let test = test_codex()
        .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        .with_config(|config| {
            let _ = config.features.disable(Feature::RemoteCompactionV2);
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(1);
        })
        .build_with_auto_env(&server)
        .await?;
    test.submit_turn("seed history for legacy compaction")
        .await?;

    test.codex.submit(Op::Compact).await?;
    wait_for_capacity_retry_success(&test, /*expected_retries*/ 1).await;

    assert_eq!(seed_mock.requests().len(), 1);
    assert_eq!(compact_mock.requests().len(), 2);
    Ok(())
}

// TODO(anp) respect Retry-After
/// SSE failures currently retry with local backoff instead of the enclosing response header.
#[tokio::test(flavor = "current_thread")]
async fn sse_failure_uses_local_backoff_despite_retry_after() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![
            responses::sse_response(responses::sse(vec![
                json!({
                    "type": "error",
                    "error": {
                        "type": "tokens",
                        "code": "rate_limit_exceeded",
                        "message": "Rate limit exceeded."
                    }
                }),
                json!({
                    "type": "response.failed",
                    "response": {
                        "id": "rate-limited",
                        "status": "failed",
                        "error": {
                            "code": "rate_limit_exceeded",
                            "message": "Rate limit exceeded."
                        }
                    }
                }),
            ]))
            .insert_header("Retry-After", "1"),
            responses::sse_response(responses::sse(vec![
                responses::ev_response_created("recovered"),
                responses::ev_completed("recovered"),
            ])),
        ],
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(1);
        })
        .build_with_auto_env(&server)
        .await?;

    submit_user_input(&test, "retry the rate-limited stream").await?;
    let retry = telemetry.next_retry().await;
    assert!((FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY).contains(&retry.delay));
    assert_eq!(
        retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: retry.delay,
            layer: "stream".into(),
            operation: "sampling".into(),
        }
    );
    wait_for_retry(&mut telemetry, &retry).await;
    wait_for_turn_completion(&test).await;

    assert_eq!(response_mock.requests().len(), 2);
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );
    Ok(())
}

/// Headerless sampled stream rate limits exhaust retries before one terminal error.
#[tokio::test(flavor = "current_thread")]
async fn sse_failure_without_retry_after_exhausts_stream_retries() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![
            responses::sse_response(responses::sse_failed(
                "rate-limited",
                "rate_limit_exceeded",
                "Rate limit exceeded.",
            )),
            responses::sse_response(responses::sse_failed(
                "still-rate-limited",
                "rate_limit_exceeded",
                "Rate limit exceeded.",
            )),
        ],
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(1);
        })
        .build_with_auto_env(&server)
        .await?;

    submit_user_input(&test, "exhaust the headerless rate-limited stream").await?;
    let retry = telemetry.next_retry().await;
    assert!((FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY).contains(&retry.delay));
    assert_eq!(
        retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: retry.delay,
            layer: "stream".into(),
            operation: "sampling".into(),
        }
    );
    wait_for_retry(&mut telemetry, &retry).await;

    let mut error_events = 0;
    let mut stream_error_events = 0;
    loop {
        match wait_for_event_with_timeout(&test.codex, |_| true, Duration::from_secs(30)).await {
            EventMsg::Error(error) => {
                error_events += 1;
                assert_eq!(error.codex_error_info, Some(CodexErrorInfo::Other));
                assert!(error.message.contains("Rate limit exceeded."));
            }
            EventMsg::StreamError(_) => stream_error_events += 1,
            EventMsg::TurnComplete(event) => {
                assert_eq!(
                    event.error.and_then(|error| error.codex_error_info),
                    Some(CodexErrorInfo::Other)
                );
                break;
            }
            _ => {}
        }
    }

    assert_eq!(error_events, 1);
    assert_eq!(stream_error_events, 1);
    assert_eq!(response_mock.requests().len(), 2);
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );

    Ok(())
}

/// Rate-limit messages already provide an exact retry delay without an HTTP header.
#[tokio::test(flavor = "current_thread")]
async fn sse_rate_limit_message_uses_server_advised_retry_delay() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![
            responses::sse_response(responses::sse_failed(
                "rate-limited",
                "rate_limit_exceeded",
                "Rate limit exceeded. Please try again in 1s.",
            )),
            responses::sse_response(responses::sse(vec![
                responses::ev_response_created("recovered"),
                responses::ev_completed("recovered"),
            ])),
        ],
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(1);
        })
        .build_with_auto_env(&server)
        .await?;

    submit_user_input(&test, "retry after the rate-limit message delay").await?;
    let retry = telemetry.next_retry().await;
    assert_eq!(
        retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: Duration::from_secs(1),
            layer: "stream".into(),
            operation: "sampling".into(),
        }
    );
    assert!(wait_for_retry(&mut telemetry, &retry).await >= Duration::from_secs(1));
    wait_for_turn_completion(&test).await;

    assert_eq!(response_mock.requests().len(), 2);
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );

    Ok(())
}

// TODO(anp) respect Retry-After
/// Rate-limit messages currently override an enclosing response's different retry delay.
#[tokio::test(flavor = "current_thread")]
async fn sse_rate_limit_message_with_retry_after_uses_server_advised_retry_delay() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![
            responses::sse_response(responses::sse_failed(
                "rate-limited",
                "rate_limit_exceeded",
                "Rate limit exceeded. Please try again in 1s.",
            ))
            .insert_header("Retry-After", "2"),
            responses::sse_response(responses::sse(vec![
                responses::ev_response_created("recovered"),
                responses::ev_completed("recovered"),
            ])),
        ],
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(1);
        })
        .build_with_auto_env(&server)
        .await?;

    submit_user_input(&test, "retry after both rate-limit delay signals").await?;
    let retry = telemetry.next_retry().await;
    assert_eq!(
        retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: Duration::from_secs(1),
            layer: "stream".into(),
            operation: "sampling".into(),
        }
    );
    assert!(wait_for_retry(&mut telemetry, &retry).await >= Duration::from_secs(1));
    wait_for_turn_completion(&test).await;

    assert_eq!(response_mock.requests().len(), 2);
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );

    Ok(())
}

// TODO(anp) respect Retry-After
/// A streamed backend overload retries persistently despite the configured retry budget.
#[tokio::test(flavor = "current_thread")]
async fn sse_overload_with_retry_after_retries_until_success() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![
            responses::sse_response(responses::sse_failed(
                "at-capacity",
                "server_is_overloaded",
                "This model is at capacity.",
            ))
            .insert_header("Retry-After", "1"),
            responses::sse_response(responses::sse(vec![
                responses::ev_response_created("recovered"),
                responses::ev_completed("recovered"),
            ])),
        ],
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(0);
        })
        .build_with_auto_env(&server)
        .await?;

    submit_user_input(&test, "retry the streamed overload").await?;
    let retry = telemetry.next_retry().await;
    assert!((FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY).contains(&retry.delay));
    assert_eq!(
        retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: retry.delay,
            layer: "stream".into(),
            operation: "sampling".into(),
        }
    );
    wait_for_retry(&mut telemetry, &retry).await;
    wait_for_capacity_retry_success(&test, /*expected_retries*/ 1).await;

    assert_eq!(response_mock.requests().len(), 2);
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );

    Ok(())
}

/// A streamed backend overload without retry advice retries until the model recovers.
#[tokio::test(flavor = "current_thread")]
async fn sse_overload_without_retry_after_retries_until_success() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![
            responses::sse_response(responses::sse_failed(
                "at-capacity",
                "server_is_overloaded",
                "This model is at capacity.",
            )),
            responses::sse_response(responses::sse(vec![
                responses::ev_response_created("recovered"),
                responses::ev_completed("recovered"),
            ])),
        ],
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(0);
        })
        .build_with_auto_env(&server)
        .await?;

    submit_user_input(&test, "retry the headerless streamed overload").await?;
    let retry = telemetry.next_retry().await;
    assert!((FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY).contains(&retry.delay));
    wait_for_retry(&mut telemetry, &retry).await;
    wait_for_capacity_retry_success(&test, /*expected_retries*/ 1).await;

    assert_eq!(response_mock.requests().len(), 2);
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );

    Ok(())
}

/// A completed tool result remains in the prompt when the follow-up model request hits capacity.
#[tokio::test(flavor = "current_thread")]
async fn capacity_after_tool_call_preserves_the_successful_tool_result() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = responses::start_mock_server().await;
    let call_id = "capacity-tool-call";
    let response_mock = responses::mount_sse_sequence(
        &server,
        vec![
            responses::sse(vec![
                responses::ev_response_created("tool-response"),
                responses::ev_function_call(call_id, "test_sync_tool", "{}"),
                responses::ev_completed("tool-response"),
            ]),
            responses::sse_failed(
                "at-capacity",
                "server_is_overloaded",
                "Selected model is at capacity. Please try a different model.",
            ),
            responses::sse(vec![
                responses::ev_assistant_message("recovered-message", "done"),
                responses::ev_completed("recovered"),
            ]),
        ],
    )
    .await;
    let test = test_codex()
        .with_model("test-gpt-5.1-codex")
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(0);
        })
        .build_with_auto_env(&server)
        .await?;

    submit_user_input(&test, "run the test tool, then recover from capacity").await?;
    wait_for_capacity_retry_success(&test, /*expected_retries*/ 1).await;

    let requests = response_mock.requests();
    assert_eq!(requests.len(), 3);
    let first_follow_up_output = requests[1].function_call_output(call_id);
    let retried_follow_up_output = requests[2].function_call_output(call_id);
    assert_eq!(retried_follow_up_output, first_follow_up_output);
    let retried_body = requests[2].body_json().to_string();
    assert!(
        !retried_body.contains("Model at capacity") && !retried_body.contains("Retrying in"),
        "user-visible retry status must not enter model input: {retried_body}"
    );
    Ok(())
}

/// A server-side response cancellation is transient and retries the model request.
#[tokio::test(flavor = "current_thread")]
async fn server_cancelled_response_retries_until_success() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_sse_sequence(
        &server,
        vec![
            responses::sse(vec![json!({
                "type": "response.cancelled",
                "response": {
                    "id": "cancelled-by-server",
                    "status": "cancelled"
                }
            })]),
            responses::sse(vec![
                responses::ev_response_created("recovered"),
                responses::ev_completed("recovered"),
            ]),
        ],
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(1);
        })
        .build_with_auto_env(&server)
        .await?;

    submit_user_input(&test, "retry a server-cancelled response").await?;
    let retry = telemetry.next_retry().await;
    assert!((FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY).contains(&retry.delay));
    wait_for_retry(&mut telemetry, &retry).await;
    let retry_events = wait_for_stream_retry_success(&test, /*expected_retries*/ 1).await;
    assert!(retry_events[0].message.contains("Retrying"));
    assert!(
        retry_events[0]
            .additional_details
            .as_deref()
            .is_some_and(|details| details.contains("Response cancelled by server")),
        "server cancellation should remain visible in retry details"
    );

    assert_eq!(response_mock.requests().len(), 2);
    Ok(())
}

/// Invalid prompts are permanent and do not consume a retry attempt.
#[tokio::test(flavor = "current_thread")]
async fn invalid_prompt_is_not_retried() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_sse_once(
        &server,
        responses::sse_failed("invalid-prompt", "invalid_prompt", "The prompt is invalid."),
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(5);
        })
        .build_with_auto_env(&server)
        .await?;

    submit_user_input(&test, "do not retry a permanent request error").await?;
    let mut stream_errors = 0;
    loop {
        match wait_for_event_with_timeout(&test.codex, |_| true, Duration::from_secs(30)).await {
            EventMsg::StreamError(_) => stream_errors += 1,
            EventMsg::TurnComplete(event) => {
                assert!(
                    event.error.is_some(),
                    "invalid prompt should remain terminal"
                );
                break;
            }
            _ => {}
        }
    }

    assert_eq!(stream_errors, 0);
    assert_eq!(response_mock.requests().len(), 1);
    Ok(())
}

/// Network reconnects keep their own attempt count without consuming stream retry budget.
#[tokio::test(flavor = "current_thread")]
async fn connection_failures_increment_retry_telemetry_without_consuming_retry_budget() -> Result<()>
{
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let bootstrap_server = responses::start_mock_server().await;
    let unavailable_listener = TcpListener::bind("127.0.0.1:0")?;
    let unavailable_address = unavailable_listener.local_addr()?;
    drop(unavailable_listener);

    let test = test_codex()
        .with_config(move |config| {
            config.model_provider.base_url = Some(format!("http://{unavailable_address}/v1"));
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(2);
            config.model_provider.supports_websockets = false;
        })
        .build_with_auto_env(&bootstrap_server)
        .await?;

    submit_user_input(&test, "recover after repeated network failures").await?;

    let first_retry = telemetry.next_retry().await;
    assert_eq!(
        first_retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: Duration::from_secs(5),
            layer: "stream".into(),
            operation: "sampling".into(),
        }
    );
    wait_for_retry(&mut telemetry, &first_retry).await;

    let second_retry = telemetry.next_retry().await;
    assert_eq!(
        second_retry,
        RetryTelemetryEvent {
            attempt: 2,
            delay: Duration::from_secs(10),
            layer: "stream".into(),
            operation: "sampling".into(),
        }
    );

    let recovered_server = MockServer::builder()
        .listener(TcpListener::bind(unavailable_address)?)
        .start()
        .await;
    let response_mock = responses::mount_sse_once(
        &recovered_server,
        responses::sse(vec![
            responses::ev_response_created("recovered"),
            responses::ev_completed("recovered"),
        ]),
    )
    .await;

    wait_for_retry(&mut telemetry, &second_retry).await;
    wait_for_turn_completion(&test).await;

    assert_eq!(response_mock.requests().len(), 1);
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );

    Ok(())
}

/// Network reconnects stop after the configured stream retry limit.
#[tokio::test(flavor = "current_thread")]
async fn connection_failures_stop_after_the_retry_limit() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let bootstrap_server = responses::start_mock_server().await;
    let unavailable_listener = TcpListener::bind("127.0.0.1:0")?;
    let unavailable_address = unavailable_listener.local_addr()?;
    drop(unavailable_listener);

    let test = test_codex()
        .with_config(move |config| {
            config.model_provider.base_url = Some(format!("http://{unavailable_address}/v1"));
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(1);
            config.model_provider.supports_websockets = false;
        })
        .build_with_auto_env(&bootstrap_server)
        .await?;

    submit_user_input(&test, "stop after one network retry").await?;
    let retry = telemetry.next_retry().await;
    assert_eq!(retry.attempt, 1);
    wait_for_retry(&mut telemetry, &retry).await;

    let stream_errors = tokio::time::timeout(Duration::from_secs(2), async {
        let mut stream_errors = 0;
        loop {
            match wait_for_event_with_timeout(&test.codex, |_| true, Duration::from_secs(30)).await
            {
                EventMsg::StreamError(_) => stream_errors += 1,
                EventMsg::TurnComplete(event) => {
                    assert!(
                        event.error.is_some(),
                        "network exhaustion should be terminal"
                    );
                    break stream_errors;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("network retry limit should end the turn promptly");

    assert_eq!(stream_errors, 1);
    Ok(())
}

/// Retryable websocket errors reconnect after the delay reported by retry telemetry.
#[tokio::test(flavor = "current_thread")]
async fn websocket_connection_limit_retries_with_local_backoff() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_websocket_server(vec![
        vec![
            vec![
                responses::ev_response_created("prewarm"),
                responses::ev_completed("prewarm"),
            ],
            vec![json!({
                "type": "error",
                "status": 400,
                "error": {
                    "type": "invalid_request_error",
                    "code": "websocket_connection_limit_reached",
                    "message": "Responses websocket connection limit reached (60 minutes). Create a new websocket connection to continue."
                }
            })],
        ],
        vec![vec![
            responses::ev_response_created("recovered"),
            responses::ev_completed("recovered"),
        ]],
    ])
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(1);
        })
        .build_with_websocket_server(&server)
        .await?;

    submit_user_input(&test, "retry after reaching the websocket connection limit").await?;
    let retry = telemetry.next_retry().await;
    assert!((FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY).contains(&retry.delay));
    assert_eq!(
        retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: retry.delay,
            layer: "stream".into(),
            operation: "sampling".into(),
        }
    );
    wait_for_retry(&mut telemetry, &retry).await;
    wait_for_turn_completion(&test).await;

    let connections = server.connections();
    assert_eq!(connections.len(), 2);
    let request_count: usize = connections.iter().map(Vec::len).sum();
    assert_eq!(request_count, 3);
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );
    server.shutdown().await;

    Ok(())
}

// TODO(anp) respect Retry-After
/// A websocket rate limit is retried after the transport retry budget is exhausted.
#[tokio::test(flavor = "current_thread")]
async fn websocket_rate_limit_with_nested_retry_after_recovers() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_websocket_server(vec![
        vec![
            vec![
                responses::ev_response_created("prewarm"),
                responses::ev_completed("prewarm"),
            ],
            vec![json!({
                "type": "error",
                "status": 429,
                "error": {
                    "type": "rate_limit_error",
                    "code": "rate_limit_exceeded",
                    "message": "Rate limit exceeded.",
                    "headers": { "Retry-After": "1" }
                }
            })],
        ],
        vec![vec![
            responses::ev_response_created("recovered"),
            responses::ev_completed("recovered"),
        ]],
    ])
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(1);
        })
        .build_with_websocket_server(&server)
        .await?;

    submit_user_input(&test, "retry the websocket rate limit").await?;
    let retry = telemetry.next_retry().await;
    assert!((FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY).contains(&retry.delay));
    assert_eq!(
        retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: retry.delay,
            layer: "stream".into(),
            operation: "sampling".into(),
        }
    );
    wait_for_retry(&mut telemetry, &retry).await;
    let retry_events = wait_for_stream_retry_success(&test, /*expected_retries*/ 1).await;
    assert!(retry_events[0].message.contains("Retrying"));

    let request_count: usize = server.connections().iter().map(Vec::len).sum();
    assert_eq!(
        request_count, 3,
        "expected prewarm, rate limit, and recovery"
    );
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );
    server.shutdown().await;

    Ok(())
}

/// A headerless websocket rate limit uses bounded local backoff and recovers.
#[tokio::test(flavor = "current_thread")]
async fn websocket_rate_limit_without_retry_after_recovers() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_websocket_server(vec![
        vec![
            vec![
                responses::ev_response_created("prewarm"),
                responses::ev_completed("prewarm"),
            ],
            vec![json!({
                "type": "error",
                "status": 429,
                "error": {
                    "type": "rate_limit_error",
                    "code": "rate_limit_exceeded",
                    "message": "Rate limit exceeded."
                }
            })],
        ],
        vec![vec![
            responses::ev_response_created("recovered"),
            responses::ev_completed("recovered"),
        ]],
    ])
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(1);
        })
        .build_with_websocket_server(&server)
        .await?;

    submit_user_input(&test, "retry the headerless websocket rate limit").await?;
    let retry = telemetry.next_retry().await;
    assert!((FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY).contains(&retry.delay));
    wait_for_retry(&mut telemetry, &retry).await;
    let retry_events = wait_for_stream_retry_success(&test, /*expected_retries*/ 1).await;
    assert!(retry_events[0].message.contains("Retrying"));

    let request_count: usize = server.connections().iter().map(Vec::len).sum();
    assert_eq!(
        request_count, 3,
        "expected prewarm, rate limit, and recovery"
    );
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );
    server.shutdown().await;

    Ok(())
}

// TODO(anp) respect Retry-After
/// Websocket overloads reconnect and retry despite the configured retry budget.
#[tokio::test(flavor = "current_thread")]
async fn websocket_overload_with_nested_retry_after_retries_until_success() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_websocket_server(vec![
        vec![
            vec![
                responses::ev_response_created("prewarm"),
                responses::ev_completed("prewarm"),
            ],
            vec![json!({
                "type": "error",
                "status": 503,
                "error": {
                    "code": "server_is_overloaded",
                    "message": "This model is disabled.",
                    "headers": { "Retry-After": "1" }
                }
            })],
        ],
        vec![vec![
            responses::ev_response_created("recovered"),
            responses::ev_completed("recovered"),
        ]],
    ])
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(0);
        })
        .build_with_websocket_server(&server)
        .await?;

    submit_user_input(&test, "retry the websocket overload").await?;
    let retry = telemetry.next_retry().await;
    assert!((FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY).contains(&retry.delay));
    assert_eq!(
        retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: retry.delay,
            layer: "stream".into(),
            operation: "sampling".into(),
        }
    );
    wait_for_retry(&mut telemetry, &retry).await;
    wait_for_capacity_retry_success(&test, /*expected_retries*/ 1).await;

    let request_count: usize = server.connections().iter().map(Vec::len).sum();
    assert_eq!(request_count, 3, "expected prewarm, overload, and recovery");
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );
    server.shutdown().await;

    Ok(())
}

/// Headerless websocket overloads reconnect and retry until recovery.
#[tokio::test(flavor = "current_thread")]
async fn websocket_overload_without_retry_after_retries_until_success() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let mut telemetry = RetryTelemetryCapture::install();
    let server = responses::start_websocket_server(vec![
        vec![
            vec![
                responses::ev_response_created("prewarm"),
                responses::ev_completed("prewarm"),
            ],
            vec![json!({
                "type": "error",
                "status": 503,
                "error": {
                    "code": "server_is_overloaded",
                    "message": "This model is disabled."
                }
            })],
        ],
        vec![vec![
            responses::ev_response_created("recovered"),
            responses::ev_completed("recovered"),
        ]],
    ])
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(0);
        })
        .build_with_websocket_server(&server)
        .await?;

    submit_user_input(&test, "retry the headerless websocket overload").await?;
    let retry = telemetry.next_retry().await;
    assert!((FIRST_RETRY_MIN_DELAY..FIRST_RETRY_MAX_DELAY).contains(&retry.delay));
    assert_eq!(
        retry,
        RetryTelemetryEvent {
            attempt: 1,
            delay: retry.delay,
            layer: "stream".into(),
            operation: "sampling".into(),
        }
    );
    wait_for_retry(&mut telemetry, &retry).await;
    wait_for_capacity_retry_success(&test, /*expected_retries*/ 1).await;

    let request_count: usize = server.connections().iter().map(Vec::len).sum();
    assert_eq!(request_count, 3, "expected prewarm, overload, and recovery");
    assert_eq!(
        telemetry.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );
    server.shutdown().await;

    Ok(())
}
