use codex_client::Request;
use codex_client::RequestCompression;
use codex_client::RetryDisposition;
use codex_client::RetryOn;
use codex_client::RetryPolicy;
use codex_client::RetryStatus;
use codex_client::TransportError;
use codex_client::classify_http_response;
use codex_client::classify_io_error;
use codex_client::classify_provider_error_text;
use codex_client::run_with_retry;
use http::HeaderMap;
use http::Method;
use http::StatusCode;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

fn http_error(status: StatusCode) -> TransportError {
    TransportError::Http {
        status,
        url: None,
        headers: None,
        body: None,
    }
}

#[test]
fn retries_all_transient_http_statuses() {
    let retry_on = RetryOn {
        retry_429: true,
        retry_5xx: true,
        retry_transport: true,
    };

    for status in [
        StatusCode::MISDIRECTED_REQUEST,
        StatusCode::REQUEST_TIMEOUT,
        StatusCode::TOO_EARLY,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::BAD_GATEWAY,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::GATEWAY_TIMEOUT,
    ] {
        assert!(
            retry_on.should_retry(&http_error(status), 0, 1),
            "expected {status} to be retryable"
        );
    }
}

#[test]
fn backoff_handles_large_attempt_numbers_without_overflow() {
    let delay = codex_client::backoff(Duration::from_millis(200), u64::from(u32::MAX) + 1);
    assert!(delay <= Duration::from_secs(60));
}

#[test]
fn backoff_caps_even_an_explicitly_large_base_delay() {
    let delay = codex_client::backoff(Duration::from_secs(90), 0);
    assert!(
        (Duration::from_secs(53)..=Duration::from_secs(60)).contains(&delay),
        "large base delay escaped its bounded jitter window: {delay:?}"
    );
}

#[test]
fn backoff_handles_maximum_duration_without_narrowing_overflow() {
    let delay = codex_client::backoff(Duration::MAX, 31);
    assert!(
        (Duration::from_secs(53)..=Duration::from_secs(60)).contains(&delay),
        "maximum base delay escaped its bounded jitter window: {delay:?}"
    );
}

#[test]
fn retries_transient_standard_and_provider_server_error_statuses() {
    let retry_on = RetryOn {
        retry_429: true,
        retry_5xx: true,
        retry_transport: true,
    };

    for status in [
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::BAD_GATEWAY,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::GATEWAY_TIMEOUT,
        StatusCode::INSUFFICIENT_STORAGE,
        StatusCode::from_u16(529).expect("529 should be a valid provider status"),
    ] {
        assert!(
            retry_on.should_retry(&http_error(status), 0, 1),
            "expected {status} to be retryable"
        );
    }
}

#[test]
fn does_not_retry_permanent_standard_server_error_statuses() {
    let retry_on = RetryOn {
        retry_429: true,
        retry_5xx: true,
        retry_transport: true,
    };

    for status in [
        StatusCode::NOT_IMPLEMENTED,
        StatusCode::HTTP_VERSION_NOT_SUPPORTED,
        StatusCode::VARIANT_ALSO_NEGOTIATES,
        StatusCode::LOOP_DETECTED,
        StatusCode::NOT_EXTENDED,
        StatusCode::NETWORK_AUTHENTICATION_REQUIRED,
    ] {
        assert!(
            !retry_on.should_retry(&http_error(status), 0, 1),
            "did not expect {status} to be retryable"
        );
    }
}

#[test]
fn shared_classifier_prioritizes_semantics_and_authentication() {
    assert_eq!(
        classify_http_response(
            StatusCode::BAD_REQUEST,
            Some("Selected model is at capacity. Please try a different model."),
        ),
        RetryDisposition::Capacity,
    );
    assert_eq!(
        classify_http_response(
            StatusCode::BAD_REQUEST,
            Some(
                r#"{"error":{"type":"invalid_request_error","message":"Selected model is at capacity. Please try a different model."}}"#,
            ),
        ),
        RetryDisposition::Capacity,
    );
    assert_eq!(
        classify_http_response(
            StatusCode::SERVICE_UNAVAILABLE,
            Some(r#"{"error":{"code":"account_suspended"}}"#),
        ),
        RetryDisposition::DoNotRetry,
    );
    assert_eq!(
        classify_http_response(
            StatusCode::UNAUTHORIZED,
            Some("Selected model is at capacity. Please try a different model."),
        ),
        RetryDisposition::DoNotRetry,
    );
    assert_eq!(
        classify_provider_error_text("connection reset by peer"),
        RetryDisposition::Transient,
    );
    assert_eq!(
        classify_provider_error_text("invalid request"),
        RetryDisposition::DoNotRetry,
    );
    assert_eq!(
        classify_provider_error_text(
            r#"{"error":{"type":"invalid_request_error","message":"Selected model is at capacity. Please try a different model."}}"#,
        ),
        RetryDisposition::Capacity,
    );
    assert_eq!(
        classify_provider_error_text("slow_down"),
        RetryDisposition::Capacity,
    );
}

#[test]
fn classifier_does_not_turn_generic_or_permanent_text_into_persistent_capacity() {
    assert_eq!(
        classify_provider_error_text("service unavailable"),
        RetryDisposition::Transient
    );
    assert_eq!(
        classify_provider_error_text("try again later"),
        RetryDisposition::Transient
    );
    assert_eq!(
        classify_provider_error_text("You've hit your usage limit. Please try again later."),
        RetryDisposition::DoNotRetry
    );
    assert_eq!(
        classify_http_response(StatusCode::NOT_FOUND, Some("try again later")),
        RetryDisposition::DoNotRetry
    );
}

#[test]
fn network_failures_retry_unclassified_transport_text_but_not_permanent_text() {
    let retry_on = RetryOn {
        retry_429: true,
        retry_5xx: true,
        retry_transport: true,
    };

    assert!(retry_on.should_retry(
        &TransportError::Network("malformed local frame".to_string()),
        0,
        1
    ));
    assert!(!retry_on.should_retry(
        &TransportError::Network("certificate verification failed".to_string()),
        0,
        1
    ));
    assert!(retry_on.should_retry(
        &TransportError::Network("DNS lookup: host not found".to_string()),
        0,
        1
    ));
}

#[test]
fn io_classifier_retries_connection_failures_but_not_permanent_local_errors() {
    assert_eq!(
        classify_io_error(&std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
        RetryDisposition::Transient
    );
    assert_eq!(
        classify_io_error(&std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        RetryDisposition::DoNotRetry
    );
    assert_eq!(
        classify_io_error(&std::io::Error::other("malformed local frame")),
        RetryDisposition::DoNotRetry
    );
    assert_eq!(
        classify_io_error(&std::io::Error::other("connection closed by peer")),
        RetryDisposition::Transient
    );
    assert_eq!(
        classify_io_error(&std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "operation canceled by caller",
        )),
        RetryDisposition::DoNotRetry
    );
}

#[test]
fn does_not_retry_permanent_http_statuses_or_build_failures() {
    let retry_on = RetryOn {
        retry_429: true,
        retry_5xx: true,
        retry_transport: true,
    };

    for status in [
        StatusCode::BAD_REQUEST,
        StatusCode::UNAUTHORIZED,
        StatusCode::FORBIDDEN,
        StatusCode::NOT_FOUND,
        StatusCode::UNPROCESSABLE_ENTITY,
    ] {
        assert!(
            !retry_on.should_retry(&http_error(status), 0, 1),
            "did not expect {status} to be retryable"
        );
    }

    assert!(!retry_on.should_retry(&TransportError::Build("invalid request".to_string()), 0, 1));
}

#[test]
fn retries_timeout_and_network_errors() {
    let retry_on = RetryOn {
        retry_429: false,
        retry_5xx: false,
        retry_transport: true,
    };
    assert!(retry_on.should_retry(&TransportError::Timeout, 0, 1));
    assert!(retry_on.should_retry(
        &TransportError::Network("connection reset".to_string()),
        0,
        1
    ));
    assert!(retry_on.should_retry(
        &TransportError::Network("DNS lookup: host not found".to_string()),
        0,
        1
    ));
}

#[test]
fn does_not_retry_usage_limit_429_responses() {
    let retry_on = RetryOn {
        retry_429: true,
        retry_5xx: true,
        retry_transport: true,
    };
    let error = TransportError::Http {
        status: StatusCode::TOO_MANY_REQUESTS,
        url: None,
        headers: None,
        body: Some(r#"{"error":{"type":"usage_limit_reached","message":"limit"}}"#.to_string()),
    };

    assert!(!retry_on.should_retry(&error, 0, 1));

    for body in [
        r#"{"code":"insufficient_quota"}"#,
        r#"{"error":{"code":"billing_hard_limit_reached"}}"#,
        r#"{"error":{"type":"account_deactivated"}}"#,
    ] {
        let error = TransportError::Http {
            status: StatusCode::TOO_MANY_REQUESTS,
            url: None,
            headers: None,
            body: Some(body.to_string()),
        };
        assert!(
            !retry_on.should_retry(&error, 0, 1),
            "did not expect {body} to retry"
        );
    }
}

#[test]
fn does_not_retry_permanent_error_bodies_with_transient_http_statuses() {
    let retry_on = RetryOn {
        retry_429: true,
        retry_5xx: true,
        retry_transport: true,
    };

    for (status, body) in [
        (
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"error":{"type":"authentication_error"}}"#,
        ),
        (
            StatusCode::BAD_GATEWAY,
            r#"{"code":"context_length_exceeded"}"#,
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"error":{"CODE":"usage_not_included"}}"#,
        ),
    ] {
        let error = TransportError::Http {
            status,
            url: None,
            headers: None,
            body: Some(body.to_string()),
        };
        assert!(
            !retry_on.should_retry(&error, 0, 1),
            "did not expect permanent body to retry for {status}: {body}"
        );
    }
}

#[test]
fn does_not_retry_additional_permanent_provider_codes() {
    let retry_on = RetryOn {
        retry_429: true,
        retry_5xx: true,
        retry_transport: true,
    };

    for code in [
        "account_is_not_active",
        "account_suspended",
        "access_denied",
        "authorization_failed",
        "content_policy_violation",
        "forbidden",
        "invalid_model",
        "invalid_prompt",
        "max_usage_reached",
        "not_eligible",
        "payment_required",
        "plan_limit_reached",
        "request_too_large",
        "subscription_required",
        "unsupported_model",
        "usage_not_included",
    ] {
        let body = format!(r#"{{"error":{{"code":"{code}"}}}}"#);
        let error = TransportError::Http {
            status: StatusCode::SERVICE_UNAVAILABLE,
            url: None,
            headers: None,
            body: Some(body),
        };
        assert!(
            !retry_on.should_retry(&error, 0, 1),
            "did not expect permanent provider code {code} to retry"
        );
    }
}

#[test]
fn local_cancellation_and_permanent_model_or_tls_errors_are_terminal() {
    let retry_on = RetryOn {
        retry_429: true,
        retry_5xx: true,
        retry_transport: true,
    };
    for message in ["operation canceled", "operation cancelled"] {
        assert!(!retry_on.should_retry(&TransportError::Network(message.to_string()), 0, 1,));
        assert_eq!(
            classify_provider_error_text(message),
            RetryDisposition::DoNotRetry,
            "caller cancellation must stay terminal in provider-shaped text: {message}",
        );
    }
    for message in [
        "operation canceled by caller",
        "request cancelled by user",
        "model does not exist; try a different model",
        "no such model: gpt-missing",
        "this model does not support tool calls; try a different model",
        "exceeded your current quota",
        "invalid certificate presented by proxy",
        "certificate verify failed",
    ] {
        assert_eq!(
            classify_provider_error_text(message),
            RetryDisposition::DoNotRetry,
            "did not expect permanent message to retry: {message}",
        );
    }
    assert_eq!(
        classify_provider_error_text("response cancelled by server"),
        RetryDisposition::Transient,
    );
}

#[tokio::test]
async fn successful_http_retry_emits_status_without_changing_the_response() {
    let statuses = Arc::new(Mutex::new(Vec::<RetryStatus>::new()));
    let status_sink = Arc::clone(&statuses);
    let policy = RetryPolicy {
        max_attempts: 1,
        capacity_max_attempts: 1,
        base_delay: Duration::ZERO,
        retry_on: RetryOn {
            retry_429: true,
            retry_5xx: true,
            retry_transport: true,
        },
        retry_notifier: Some(Arc::new(move |status| {
            status_sink
                .lock()
                .expect("status lock should not be poisoned")
                .push(status);
            Box::pin(async {})
        })),
    };

    let result = run_with_retry(
        policy,
        || Request {
            method: Method::GET,
            url: "https://example.com".to_string(),
            headers: HeaderMap::new(),
            body: None,
            compression: RequestCompression::None,
            timeout: None,
        },
        |_request, attempt| async move {
            if attempt == 0 {
                Err(TransportError::Http {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    url: None,
                    headers: None,
                    body: Some("Selected model is at capacity".to_string()),
                })
            } else {
                Ok("preserved response")
            }
        },
    )
    .await;

    assert_eq!(result.expect("retry should recover"), "preserved response");
    let statuses = statuses.lock().expect("status lock should not be poisoned");
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].disposition, RetryDisposition::Capacity);
    assert_eq!(statuses[0].attempt, 1);
}

#[tokio::test]
async fn capacity_uses_its_separate_persistent_retry_budget() {
    let policy = RetryPolicy {
        max_attempts: 0,
        capacity_max_attempts: 2,
        base_delay: Duration::ZERO,
        retry_on: RetryOn {
            retry_429: true,
            retry_5xx: true,
            retry_transport: true,
        },
        retry_notifier: None,
    };

    let result = run_with_retry(
        policy,
        || Request {
            method: Method::GET,
            url: "https://example.com".to_string(),
            headers: HeaderMap::new(),
            body: None,
            compression: RequestCompression::None,
            timeout: None,
        },
        |_request, attempt| async move {
            if attempt < 2 {
                Err(TransportError::Http {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    url: None,
                    headers: None,
                    body: Some("Selected model is at capacity".to_string()),
                })
            } else {
                Ok("recovered")
            }
        },
    )
    .await;

    assert_eq!(
        result.expect("capacity retries should recover"),
        "recovered"
    );
}

#[tokio::test]
async fn persistent_capacity_budget_returns_the_last_error_after_one_hundred_retries() {
    let attempts = Arc::new(AtomicU64::new(0));
    let attempts_for_request = Arc::clone(&attempts);
    let policy = RetryPolicy {
        max_attempts: 0,
        capacity_max_attempts: codex_client::PERSISTENT_CAPACITY_MAX_RETRIES,
        base_delay: Duration::ZERO,
        retry_on: RetryOn {
            retry_429: true,
            retry_5xx: true,
            retry_transport: true,
        },
        retry_notifier: None,
    };

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        run_with_retry(
            policy,
            || Request {
                method: Method::GET,
                url: "https://example.com".to_string(),
                headers: HeaderMap::new(),
                body: None,
                compression: RequestCompression::None,
                timeout: None,
            },
            move |_request, _attempt| {
                let attempts = Arc::clone(&attempts_for_request);
                async move {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    Err::<(), _>(TransportError::Http {
                        status: StatusCode::SERVICE_UNAVAILABLE,
                        url: None,
                        headers: None,
                        body: Some(
                            "Selected model is at capacity. Please try a different model."
                                .to_string(),
                        ),
                    })
                }
            },
        ),
    )
    .await
    .expect("capacity retries must have a bounded attempt count")
    .expect_err("the final capacity error should be returned");

    assert!(matches!(result, TransportError::Http { .. }));
    assert_eq!(
        attempts.load(Ordering::Relaxed),
        101,
        "one initial request plus one hundred bounded retries is expected"
    );
}
