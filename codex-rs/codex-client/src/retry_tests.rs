use super::*;
use http::StatusCode;

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

    assert!(!retry_on.should_retry(
        &TransportError::Build("invalid request".to_string()),
        0,
        1
    ));
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
}
