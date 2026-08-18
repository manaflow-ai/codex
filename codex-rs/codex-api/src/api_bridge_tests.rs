use super::*;
use base64::Engine;
use codex_protocol::protocol::RateLimitReachedType;
use pretty_assertions::assert_eq;

#[test]
fn map_api_error_maps_server_overloaded() {
    let err = map_api_error(ApiError::ServerOverloaded);
    assert!(matches!(err.details(), CodexErrorDetails::ServerOverloaded));
}

#[test]
fn map_api_error_preserves_retry_delay() {
    let retry_delay = std::time::Duration::from_secs(17);
    let err = map_api_error(ApiError::Retryable {
        message: "retry later".to_string(),
        delay: Some(retry_delay),
    });

    assert!(matches!(
        err.details(),
        CodexErrorDetails::Stream(message) if message == "retry later"
    ));
    assert_eq!(err.retry_delay(), Some(retry_delay));
}

#[test]
fn map_api_error_promotes_wrapped_capacity_retryable_errors_to_overload() {
    let err = map_api_error(ApiError::Retryable {
        message: r#"{"error":{"type":"invalid_request_error","message":"Selected model is at capacity. Please try a different model."}}"#
            .to_string(),
        delay: None,
    });

    assert!(matches!(err.details(), CodexErrorDetails::ServerOverloaded));
}

#[test]
fn map_api_error_promotes_capacity_rate_limit_errors_to_overload() {
    let err = map_api_error(ApiError::RateLimit(
        r#"{"error":{"type":"invalid_request_error","message":"Selected model is at capacity. Please try a different model."}}"#
            .to_string(),
    ));

    assert!(matches!(err.details(), CodexErrorDetails::ServerOverloaded));
}

#[test]
fn map_api_error_keeps_request_build_failures_permanent() {
    let err = map_api_error(ApiError::Transport(TransportError::Build(
        "failed to serialize request".to_string(),
    )));

    assert!(matches!(
        err.details(),
        CodexErrorDetails::InvalidRequest(message) if message == "failed to serialize request"
    ));
}

#[test]
fn map_api_error_does_not_mark_permanent_network_failures_retryable() {
    let err = map_api_error(ApiError::Transport(TransportError::Network(
        "certificate verification failed".to_string(),
    )));

    assert!(matches!(
        err.details(),
        CodexErrorDetails::Stream(message) if message == "certificate verification failed"
    ));
    assert!(!err.is_explicitly_retryable());
}

#[test]
fn map_api_error_marks_unclassified_network_failures_retryable() {
    let err = map_api_error(ApiError::Transport(TransportError::Network(
        "malformed local frame".to_string(),
    )));

    assert!(err.is_explicitly_retryable());
}

#[test]
fn sideband_does_not_retry_unclassified_stream_errors() {
    let retry_on = codex_client::RetryOn {
        retry_429: true,
        retry_5xx: true,
        retry_transport: true,
    };
    assert!(
        !ApiError::Stream("malformed request".to_string())
            .is_retryable_for_attempt(&retry_on, 0, 1,)
    );
    assert!(
        ApiError::Retryable {
            message: "connection closed".to_string(),
            delay: None,
        }
        .is_retryable_for_attempt(&retry_on, 0, 1)
    );
}

#[test]
fn sideband_retries_capacity_but_not_permanent_rate_limit_messages() {
    let retry_on = codex_client::RetryOn {
        retry_429: true,
        retry_5xx: true,
        retry_transport: true,
    };
    assert!(
        ApiError::Api {
            status: http::StatusCode::BAD_REQUEST,
            message: "Selected model is at capacity. Please try a different model.".to_string(),
        }
        .is_retryable_for_attempt(&retry_on, 0, 1)
    );
    assert!(
        !ApiError::RateLimit("account_suspended".to_string())
            .is_retryable_for_attempt(&retry_on, 0, 1)
    );
    assert!(
        !ApiError::Api {
            status: http::StatusCode::UNAUTHORIZED,
            message: "Selected model is at capacity. Please try a different model.".to_string(),
        }
        .is_retryable_for_attempt(&retry_on, 0, 1)
    );
}

#[test]
fn api_error_message_semantics_override_transient_http_status() {
    let capacity = map_api_error(ApiError::Api {
        status: http::StatusCode::BAD_REQUEST,
        message: "Selected model is at capacity. Please try a different model.".to_string(),
    });
    assert!(matches!(
        capacity.details(),
        CodexErrorDetails::ServerOverloaded
    ));

    let permanent = map_api_error(ApiError::Api {
        status: http::StatusCode::SERVICE_UNAVAILABLE,
        message: "account_suspended".to_string(),
    });
    assert!(matches!(
        permanent.details(),
        CodexErrorDetails::InvalidRequest(_)
    ));

    let auth = map_api_error(ApiError::Api {
        status: http::StatusCode::UNAUTHORIZED,
        message: "Selected model is at capacity. Please try a different model.".to_string(),
    });
    assert!(matches!(
        auth.details(),
        CodexErrorDetails::UnexpectedStatus(error)
            if error.status == http::StatusCode::UNAUTHORIZED
    ));
}

#[test]
fn map_api_error_maps_server_overloaded_from_503_body() {
    let body = serde_json::json!({
        "error": {
            "code": "server_is_overloaded"
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::SERVICE_UNAVAILABLE,
        url: Some("http://example.com/v1/responses".to_string()),
        headers: None,
        body: Some(body),
    }));

    assert!(matches!(err.details(), CodexErrorDetails::ServerOverloaded));
}

#[test]
fn map_api_error_maps_rate_limit_overload_bodies_to_server_overloaded() {
    let body = serde_json::json!({
        "error": {
            "type": "server_overloaded",
            "message": "Selected model is at capacity. Please try a different model."
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::TOO_MANY_REQUESTS,
        url: None,
        headers: None,
        body: Some(body),
    }));

    assert!(matches!(err.details(), CodexErrorDetails::ServerOverloaded));
}

#[test]
fn map_api_error_keeps_latest_capacity_message_retryable_when_gateway_wraps_it() {
    let body = serde_json::json!({
        "error": {
            "type": "invalid_request_error",
            "message": "Selected model is at capacity. Please try a different model."
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::BAD_REQUEST,
        url: None,
        headers: None,
        body: Some(body),
    }));

    assert!(matches!(err.details(), CodexErrorDetails::ServerOverloaded));
}

#[test]
fn map_api_error_maps_top_level_capacity_fields() {
    for body in [
        serde_json::json!({
            "code": "MODEL_AT_CAPACITY",
            "message": "The selected model is temporarily unavailable"
        }),
        serde_json::json!({
            "type": "server_overloaded",
            "message": "high demand"
        }),
    ] {
        let err = map_api_error(ApiError::Transport(TransportError::Http {
            status: http::StatusCode::BAD_GATEWAY,
            url: None,
            headers: None,
            body: Some(body.to_string()),
        }));
        assert!(
            matches!(err.details(), CodexErrorDetails::ServerOverloaded),
            "expected capacity body to map to overload: {body}"
        );
    }
}

#[test]
fn map_api_error_maps_plain_text_capacity_failures_for_any_http_status() {
    let body = "Selected model is at capacity. Please try a different model.".to_string();
    for status in [
        http::StatusCode::BAD_REQUEST,
        http::StatusCode::TOO_MANY_REQUESTS,
        http::StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let err = map_api_error(ApiError::Transport(TransportError::Http {
            status,
            url: None,
            headers: None,
            body: Some(body.clone()),
        }));
        assert!(
            matches!(err.details(), CodexErrorDetails::ServerOverloaded),
            "expected plain capacity response to retry for {status}: {err:?}"
        );
    }
}

#[test]
fn map_api_error_maps_nested_capacity_failures() {
    let body = serde_json::json!({
        "gateway": {
            "details": {
                "error": {
                    "code": "model_at_capacity",
                    "message": "Selected model is at capacity. Please try a different model."
                }
            }
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::BAD_GATEWAY,
        url: None,
        headers: None,
        body: Some(body),
    }));

    assert!(matches!(err.details(), CodexErrorDetails::ServerOverloaded));
}

#[test]
fn map_api_error_does_not_retry_permanent_semantics_hidden_in_5xx() {
    for (body, expected) in [
        (
            serde_json::json!({ "error": { "code": "insufficient_quota" } }),
            "quota",
        ),
        (
            serde_json::json!({ "code": "context_length_exceeded" }),
            "context",
        ),
        (
            serde_json::json!({ "error": { "type": "invalid_request_error", "message": "bad" } }),
            "invalid",
        ),
    ] {
        let err = map_api_error(ApiError::Transport(TransportError::Http {
            status: http::StatusCode::SERVICE_UNAVAILABLE,
            url: None,
            headers: None,
            body: Some(body.to_string()),
        }));
        match expected {
            "quota" => assert!(matches!(err.details(), CodexErrorDetails::QuotaExceeded)),
            "context" => assert!(matches!(
                err.details(),
                CodexErrorDetails::ContextWindowExceeded
            )),
            "invalid" => assert!(matches!(
                err.details(),
                CodexErrorDetails::InvalidRequest(message) if message == "bad"
            )),
            _ => unreachable!(),
        }
    }
}

#[test]
fn map_api_error_keeps_plain_and_array_wrapped_permanent_5xx_terminal() {
    let plain = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::SERVICE_UNAVAILABLE,
        url: None,
        headers: None,
        body: Some("account_suspended".to_string()),
    }));
    assert!(matches!(
        plain.details(),
        CodexErrorDetails::InvalidRequest(_)
    ));

    let array_wrapped = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::SERVICE_UNAVAILABLE,
        url: None,
        headers: None,
        body: Some(
            serde_json::json!({
                "errors": [{ "code": "insufficient_quota" }]
            })
            .to_string(),
        ),
    }));
    assert!(matches!(
        array_wrapped.details(),
        CodexErrorDetails::QuotaExceeded
    ));
}

#[test]
fn map_api_error_keeps_misdirected_request_transient() {
    let api_error = ApiError::Transport(TransportError::Http {
        status: http::StatusCode::MISDIRECTED_REQUEST,
        url: None,
        headers: None,
        body: None,
    });
    let retry_on = codex_client::RetryOn {
        retry_429: true,
        retry_5xx: true,
        retry_transport: true,
    };
    assert!(api_error.is_retryable_for_attempt(&retry_on, 0, 1));
    let err = map_api_error(api_error);
    assert!(matches!(
        err.details(),
        CodexErrorDetails::UnexpectedStatus(error)
            if error.status == http::StatusCode::MISDIRECTED_REQUEST
    ));
}

#[test]
fn map_api_error_maps_cloudflare_blocked_response_to_user_message() {
    let mut headers = HeaderMap::new();
    headers.insert(CF_RAY_HEADER, http::HeaderValue::from_static("ray-id"));
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::FORBIDDEN,
        url: Some("http://example.com/blocked".to_string()),
        headers: Some(headers),
        body: Some(
            "<html><body>Cloudflare error: Sorry, you have been blocked</body></html>".to_string(),
        ),
    }));

    let CodexErrorDetails::UnexpectedStatus(err) = err.details() else {
        panic!("expected CodexErrorDetails::UnexpectedStatus, got {err:?}");
    };
    assert_eq!(
        err.user_message.as_deref(),
        Some(
            "Access blocked by Cloudflare. This usually happens when connecting from a restricted region (status 403 Forbidden)"
        )
    );
    assert_eq!(
        err.to_string(),
        "Access blocked by Cloudflare. This usually happens when connecting from a restricted region (status 403 Forbidden), url: http://example.com/blocked, cf-ray: ray-id"
    );
}

#[test]
fn map_api_error_maps_cyber_policy_from_400_body() {
    let body = serde_json::json!({
        "error": {
            "message": "This request has been flagged for potentially high-risk cyber activity.",
            "type": "invalid_request",
            "param": null,
            "code": "cyber_policy"
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::BAD_REQUEST,
        url: Some("http://example.com/v1/responses".to_string()),
        headers: None,
        body: Some(body),
    }));

    let CodexErrorDetails::CyberPolicy { message } = err.details() else {
        panic!("expected CodexErrorDetails::CyberPolicy, got {err:?}");
    };
    assert_eq!(
        message,
        "This request has been flagged for potentially high-risk cyber activity."
    );
}

#[test]
fn map_api_error_maps_wrapped_websocket_cyber_policy_from_400_body() {
    let body = serde_json::json!({
        "type": "error",
        "status": 400,
        "error": {
            "message": "This websocket request was flagged.",
            "type": "invalid_request",
            "code": "cyber_policy"
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::BAD_REQUEST,
        url: Some("ws://example.com/v1/responses".to_string()),
        headers: None,
        body: Some(body),
    }));

    let CodexErrorDetails::CyberPolicy { message } = err.details() else {
        panic!("expected CodexErrorDetails::CyberPolicy, got {err:?}");
    };
    assert_eq!(message, "This websocket request was flagged.");
}

#[test]
fn map_api_error_uses_cyber_policy_fallback_for_missing_message() {
    let body = serde_json::json!({
        "error": {
            "code": "cyber_policy"
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::BAD_REQUEST,
        url: Some("http://example.com/v1/responses".to_string()),
        headers: None,
        body: Some(body),
    }));

    let CodexErrorDetails::CyberPolicy { message } = err.details() else {
        panic!("expected CodexErrorDetails::CyberPolicy, got {err:?}");
    };
    assert_eq!(
        message,
        "This request has been flagged for possible cybersecurity risk."
    );
}

#[test]
fn map_api_error_keeps_unknown_400_errors_generic() {
    let body = serde_json::json!({
        "error": {
            "message": "Some other bad request.",
            "code": "some_other_policy"
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::BAD_REQUEST,
        url: Some("http://example.com/v1/responses".to_string()),
        headers: None,
        body: Some(body.clone()),
    }));

    let CodexErrorDetails::InvalidRequest(message) = err.details() else {
        panic!("expected CodexErrorDetails::InvalidRequest, got {err:?}");
    };
    assert_eq!(message, &body);
}

#[test]
fn map_api_error_maps_usage_limit_limit_name_header() {
    let mut headers = HeaderMap::new();
    headers.insert(
        ACTIVE_LIMIT_HEADER,
        http::HeaderValue::from_static("codex_other"),
    );
    headers.insert(
        "x-codex-other-limit-name",
        http::HeaderValue::from_static("codex_other"),
    );
    let body = serde_json::json!({
        "error": {
            "type": "usage_limit_reached",
            "plan_type": "pro",
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::TOO_MANY_REQUESTS,
        url: Some("http://example.com/v1/responses".to_string()),
        headers: Some(headers),
        body: Some(body),
    }));

    let CodexErrorDetails::UsageLimitReached(usage_limit) = err.details() else {
        panic!("expected CodexErrorDetails::UsageLimitReached, got {err:?}");
    };
    assert_eq!(
        usage_limit
            .rate_limits
            .as_ref()
            .and_then(|snapshot| snapshot.limit_name.as_deref()),
        Some("codex_other")
    );
}

#[test]
fn map_api_error_does_not_fallback_limit_name_to_limit_id() {
    let mut headers = HeaderMap::new();
    headers.insert(
        ACTIVE_LIMIT_HEADER,
        http::HeaderValue::from_static("codex_other"),
    );
    let body = serde_json::json!({
        "error": {
            "type": "usage_limit_reached",
            "plan_type": "pro",
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::TOO_MANY_REQUESTS,
        url: Some("http://example.com/v1/responses".to_string()),
        headers: Some(headers),
        body: Some(body),
    }));

    let CodexErrorDetails::UsageLimitReached(usage_limit) = err.details() else {
        panic!("expected CodexErrorDetails::UsageLimitReached, got {err:?}");
    };
    assert_eq!(
        usage_limit
            .rate_limits
            .as_ref()
            .and_then(|snapshot| snapshot.limit_name.as_deref()),
        None
    );
}

#[test]
fn map_api_error_copies_rate_limit_reached_type_to_usage_limit_snapshot() {
    for (active_limit, expected_limit_id) in [(None, "codex"), (Some("codex_other"), "codex_other")]
    {
        let mut headers = HeaderMap::new();
        if let Some(active_limit) = active_limit {
            headers.insert(
                ACTIVE_LIMIT_HEADER,
                http::HeaderValue::from_static(active_limit),
            );
        }
        for (name, value) in [
            ("x-codex-credits-has-credits", "true"),
            ("x-codex-credits-unlimited", "false"),
            ("x-codex-credits-balance", ""),
            (
                "x-codex-rate-limit-reached-type",
                "workspace_member_usage_limit_reached",
            ),
        ] {
            headers.insert(name, http::HeaderValue::from_static(value));
        }
        let body = serde_json::json!({
            "error": {
                "type": "usage_limit_reached",
                "plan_type": "pro",
            }
        })
        .to_string();

        let err = map_api_error(ApiError::Transport(TransportError::Http {
            status: http::StatusCode::TOO_MANY_REQUESTS,
            url: Some("http://example.com/v1/responses".to_string()),
            headers: Some(headers),
            body: Some(body),
        }));

        let CodexErrorDetails::UsageLimitReached(usage_limit) = err.details() else {
            panic!("expected CodexErrorDetails::UsageLimitReached, got {err:?}");
        };
        assert_eq!(
            usage_limit.rate_limit_reached_type,
            Some(RateLimitReachedType::WorkspaceMemberUsageLimitReached)
        );
        let snapshot = usage_limit
            .rate_limits
            .as_ref()
            .expect("usage limit snapshot");
        assert_eq!(snapshot.limit_id.as_deref(), Some(expected_limit_id));
        assert_eq!(
            snapshot.rate_limit_reached_type,
            Some(RateLimitReachedType::WorkspaceMemberUsageLimitReached)
        );
        assert_eq!(
            snapshot.credits.as_ref().map(|credits| (
                credits.has_credits,
                credits.unlimited,
                credits.balance.as_deref()
            )),
            Some((true, false, None))
        );
    }
}

#[test]
fn map_api_error_ignores_unparseable_rate_limit_reached_type_headers() {
    let values = [
        http::HeaderValue::from_static("future_rate_limit_reached_type"),
        http::HeaderValue::from_bytes(&[0xff]).expect("valid opaque header value"),
    ];

    for value in values {
        let mut headers = HeaderMap::new();
        headers.insert("x-codex-rate-limit-reached-type", value);
        let body = serde_json::json!({
            "error": {
                "type": "usage_limit_reached",
                "plan_type": "pro",
            }
        })
        .to_string();
        let err = map_api_error(ApiError::Transport(TransportError::Http {
            status: http::StatusCode::TOO_MANY_REQUESTS,
            url: Some("http://example.com/v1/responses".to_string()),
            headers: Some(headers),
            body: Some(body),
        }));

        let CodexErrorDetails::UsageLimitReached(usage_limit) = err.details() else {
            panic!("expected CodexErrorDetails::UsageLimitReached, got {err:?}");
        };
        assert_eq!(usage_limit.rate_limit_reached_type, None);
    }
}

#[test]
fn map_api_error_extracts_identity_auth_details_from_headers() {
    let mut headers = HeaderMap::new();
    headers.insert(REQUEST_ID_HEADER, http::HeaderValue::from_static("req-401"));
    headers.insert(CF_RAY_HEADER, http::HeaderValue::from_static("ray-401"));
    headers.insert(
        X_OPENAI_AUTHORIZATION_ERROR_HEADER,
        http::HeaderValue::from_static("missing_authorization_header"),
    );
    let x_error_json =
        base64::engine::general_purpose::STANDARD.encode(r#"{"error":{"code":"token_expired"}}"#);
    headers.insert(
        X_ERROR_JSON_HEADER,
        http::HeaderValue::from_str(&x_error_json).expect("valid x-error-json header"),
    );

    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::UNAUTHORIZED,
        url: Some("https://chatgpt.com/backend-api/codex/models".to_string()),
        headers: Some(headers),
        body: Some(r#"{"detail":"Unauthorized"}"#.to_string()),
    }));

    let CodexErrorDetails::UnexpectedStatus(err) = err.details() else {
        panic!("expected CodexErrorDetails::UnexpectedStatus, got {err:?}");
    };
    assert_eq!(err.request_id.as_deref(), Some("req-401"));
    assert_eq!(err.cf_ray.as_deref(), Some("ray-401"));
    assert_eq!(
        err.identity_authorization_error.as_deref(),
        Some("missing_authorization_header")
    );
    assert_eq!(err.identity_error_code.as_deref(), Some("token_expired"));
}
