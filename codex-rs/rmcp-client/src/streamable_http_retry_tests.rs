use std::any::TypeId;
use std::sync::Arc;
use std::time::Duration;

use codex_exec_server::ExecServerError;
use pretty_assertions::assert_eq;
use rmcp::transport::DynamicTransportError;
use rmcp::transport::streamable_http_client::AuthRequiredError;
use rmcp::transport::streamable_http_client::StreamableHttpError;

use crate::http_client_adapter::StreamableHttpClientAdapterError;
use crate::rmcp_client::ClientOperationError;

use super::*;

#[test]
fn retryable_initialize_error_includes_discovery_and_initialized_notification_context() {
    let contexts = [
        "send discover request",
        "send initialize request",
        "send initialized notification",
        "receive initialize response",
    ];

    assert_eq!(
        contexts.map(|context| {
            RmcpClient::is_retryable_client_initialize_error(&retryable_initialize_error(context))
        }),
        [true, true, true, false],
    );
}

#[test]
fn retryable_streamable_http_error_includes_remote_body_stream_failure() {
    let errors = [
        StreamableHttpError::Client(StreamableHttpClientAdapterError::HttpRequest(
            ExecServerError::HttpRequest("error sending request for url".to_string()),
        )),
        StreamableHttpError::Client(StreamableHttpClientAdapterError::HttpRequest(
            ExecServerError::Server {
                code: JSON_RPC_INTERNAL_ERROR_CODE,
                message: "http/request failed: error sending request for url".to_string(),
            },
        )),
        StreamableHttpError::Client(StreamableHttpClientAdapterError::HttpRequest(
            ExecServerError::Protocol(
                "http response stream `http-1` failed: exec-server transport disconnected"
                    .to_string(),
            ),
        )),
        StreamableHttpError::Client(StreamableHttpClientAdapterError::HttpRequest(
            ExecServerError::Protocol(
                "http response stream `http-1` received seq 2, expected 1".to_string(),
            ),
        )),
        StreamableHttpError::UnexpectedServerResponse("HTTP 502: upstream failure".into()),
        StreamableHttpError::UnexpectedServerResponse("HTTP 400: bad request".into()),
    ];

    assert_eq!(
        errors.map(|error| RmcpClient::is_retryable_streamable_http_error(&error)),
        [true, true, true, false, true, false],
    );
}

#[test]
fn closed_mcp_transport_is_retryable_after_initialize() {
    assert!(RmcpClient::is_retryable_streamable_http_error(
        &StreamableHttpError::TransportChannelClosed
    ));
}

#[test]
fn mcp_stream_transport_errors_retry_but_protocol_errors_do_not() {
    let retryable = [
        StreamableHttpError::Io(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
        StreamableHttpError::Sse(sse_stream::Error::Body(Box::new(std::io::Error::from(
            std::io::ErrorKind::BrokenPipe,
        )))),
        StreamableHttpError::UnexpectedEndOfStream,
        StreamableHttpError::UnexpectedServerResponse(
            "timed out waiting for MCP event stream response headers".into(),
        ),
    ];
    for error in retryable {
        assert!(
            RmcpClient::is_retryable_streamable_http_error(&error),
            "expected {error} to retry"
        );
    }

    let permanent = [
        StreamableHttpError::Io(std::io::Error::from(std::io::ErrorKind::InvalidData)),
        StreamableHttpError::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        StreamableHttpError::Sse(sse_stream::Error::InvalidLine),
        StreamableHttpError::MissingSessionIdInResponse,
    ];
    for error in permanent {
        assert!(
            !RmcpClient::is_retryable_streamable_http_error(&error),
            "did not expect {error} to retry"
        );
    }
}

#[test]
fn permanent_mcp_error_body_is_not_retryable_even_with_server_status() {
    assert!(!RmcpClient::is_retryable_streamable_http_error(
        &StreamableHttpError::UnexpectedServerResponse(
            "HTTP 503: {\"error\":{\"code\":\"account_suspended\"}}".into(),
        )
    ));
}

#[test]
fn capacity_mcp_error_keeps_its_retry_class() {
    let error = StreamableHttpError::UnexpectedServerResponse(
        "HTTP 400: Selected model is at capacity. Please try a different model.".into(),
    );

    assert_eq!(
        RmcpClient::classify_streamable_http_error(&error),
        RetryDisposition::Capacity,
    );
}

#[test]
fn wrapped_mcp_capacity_error_overrides_generic_json_rpc_request_code() {
    let error = rmcp::model::ErrorData::new(
        rmcp::model::ErrorCode::INVALID_REQUEST,
        "Selected model is at capacity. Please try a different model.",
        Some(serde_json::json!({
            "type": "invalid_request_error",
            "message": "Selected model is at capacity. Please try a different model."
        })),
    );

    assert_eq!(classify_mcp_error(&error), RetryDisposition::Capacity);
}

#[test]
fn nested_mcp_capacity_message_overrides_generic_json_rpc_request_code() {
    let error = rmcp::model::ErrorData::new(
        rmcp::model::ErrorCode::INVALID_REQUEST,
        r#"{"error":{"type":"invalid_request_error","message":"Selected model is at capacity. Please try a different model."}}"#,
        None,
    );

    assert_eq!(classify_mcp_error(&error), RetryDisposition::Capacity);
}

#[test]
fn unclassified_exec_server_failures_and_nested_permanent_failures_do_not_retry() {
    for error in [
        ExecServerError::HttpRequest("invalid URL".to_string()),
        ExecServerError::Disconnected("configuration rejected".to_string()),
        ExecServerError::ConnectionAttempt(Arc::new(ExecServerError::Spawn(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "missing exec server",
        )))),
    ] {
        assert_eq!(
            classify_exec_server_error(&error),
            RetryDisposition::DoNotRetry,
            "did not expect {error} to retry",
        );
    }
}

#[test]
fn too_early_http_responses_are_retryable() {
    assert!(is_retryable_http_status(StatusCode::TOO_EARLY));
}

#[test]
fn transient_server_error_statuses_are_retryable() {
    for status in [
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::BAD_GATEWAY,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::GATEWAY_TIMEOUT,
        StatusCode::INSUFFICIENT_STORAGE,
        StatusCode::from_u16(529).expect("529 should be a valid provider status"),
    ] {
        assert!(
            is_retryable_http_status(status),
            "expected {status} to be retryable"
        );
    }
}

#[test]
fn permanent_server_error_statuses_are_not_retryable() {
    for status in [
        StatusCode::NOT_IMPLEMENTED,
        StatusCode::HTTP_VERSION_NOT_SUPPORTED,
        StatusCode::VARIANT_ALSO_NEGOTIATES,
        StatusCode::LOOP_DETECTED,
        StatusCode::NOT_EXTENDED,
        StatusCode::NETWORK_AUTHENTICATION_REQUIRED,
    ] {
        assert!(
            !is_retryable_http_status(status),
            "did not expect {status} to be retryable"
        );
    }
}

#[test]
fn mcp_retry_delay_is_exponential_and_capped_at_one_minute() {
    let first = mcp_retry_delay(1);
    let second = mcp_retry_delay(2);

    assert!((Duration::from_millis(225)..Duration::from_millis(275)).contains(&first));
    assert!((Duration::from_millis(450)..Duration::from_millis(550)).contains(&second));
    assert_eq!(mcp_retry_delay(u64::MAX), Duration::from_secs(60));
}

#[test]
fn mcp_transient_and_capacity_budgets_are_bounded() {
    assert_eq!(MCP_TRANSIENT_MAX_RETRIES, 2);
    assert_eq!(
        MCP_CAPACITY_MAX_RETRIES,
        codex_client::PERSISTENT_CAPACITY_MAX_RETRIES
    );
    assert_eq!(MCP_CAPACITY_MAX_RETRIES, 100);
}

#[test]
fn startup_http_authentication_challenges_require_reauthorization() {
    let transport_error = || {
        DynamicTransportError::from_parts(
            "streamable_http",
            TypeId::of::<()>(),
            Box::new(
                StreamableHttpError::<StreamableHttpClientAdapterError>::AuthRequired(
                    AuthRequiredError::new("Bearer error=\"invalid_token\"".to_string()),
                ),
            ),
        )
    };
    let errors = [
        anyhow::Error::new(rmcp::service::ClientInitializeError::TransportError {
            error: transport_error(),
            context: "send initialize request".into(),
        }),
        anyhow::Error::new(ClientOperationError::from(
            rmcp::service::ServiceError::TransportSend(transport_error()),
        )),
    ];

    for error in errors {
        assert!(crate::startup_error::is_authentication_required_error(
            &error
        ));
    }
}

fn retryable_initialize_error(context: &'static str) -> rmcp::service::ClientInitializeError {
    rmcp::service::ClientInitializeError::TransportError {
        error: DynamicTransportError::from_parts(
            "streamable_http",
            TypeId::of::<()>(),
            Box::new(StreamableHttpError::Client(
                StreamableHttpClientAdapterError::HttpRequest(ExecServerError::HttpRequest(
                    "error sending request for url".to_string(),
                )),
            )),
        ),
        context: context.into(),
    }
}
