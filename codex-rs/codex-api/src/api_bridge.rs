use crate::TransportError;
use crate::error::ApiError;
use crate::error::is_permanent_error_fields;
use crate::error::is_permanent_error_message;
use crate::error::is_server_overloaded_error;
use crate::rate_limits::parse_promo_message;
use crate::rate_limits::parse_rate_limit_for_limit;
use crate::rate_limits::parse_rate_limit_reached_type;
use base64::Engine;
use chrono::DateTime;
use chrono::Utc;
use codex_client::is_capacity_error_body;
use codex_protocol::auth::PlanType;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::ConnectionFailedError;
use codex_protocol::error::RetryLimitReachedError;
use codex_protocol::error::UnexpectedResponseError;
use codex_protocol::error::UsageLimitReachedError;
use http::HeaderMap;
use serde_json::Value;

pub fn map_api_error(err: ApiError) -> CodexErr {
    match err {
        ApiError::ContextWindowExceeded => CodexErr::ContextWindowExceeded,
        ApiError::QuotaExceeded => CodexErr::QuotaExceeded,
        ApiError::UsageNotIncluded => CodexErr::UsageNotIncluded,
        ApiError::Retryable { message, delay } => {
            if is_capacity_error_body(&message) {
                return CodexErr::ServerOverloaded;
            }
            let error = CodexErr::Stream(message).with_explicit_retryable();
            match delay {
                Some(delay) => error.with_retry_delay(delay),
                None => error,
            }
        }
        ApiError::Stream(msg) => CodexErr::Stream(msg),
        ApiError::ServerOverloaded => CodexErr::ServerOverloaded,
        ApiError::Cancelled => CodexErr::TurnAborted,
        ApiError::Api { status, message } => {
            let is_auth_error = matches!(
                status,
                http::StatusCode::UNAUTHORIZED | http::StatusCode::FORBIDDEN
            );
            if !is_auth_error && is_capacity_error_body(&message) {
                return CodexErr::ServerOverloaded;
            }
            if !is_auth_error && is_permanent_error_message(&message) {
                return CodexErr::InvalidRequest(message);
            }
            if !is_auth_error && is_server_overloaded_error(None, None, Some(&message)) {
                return CodexErr::ServerOverloaded;
            }
            let user_message = api_error_user_message(status, &message);
            CodexErr::UnexpectedStatus(UnexpectedResponseError {
                status,
                body: message,
                user_message,
                url: None,
                cf_ray: None,
                request_id: None,
                identity_authorization_error: None,
                identity_error_code: None,
            })
        }
        ApiError::InvalidRequest { message } => CodexErr::InvalidRequest(message),
        ApiError::CyberPolicy { message } => {
            CodexErr::new(CodexErrorDetails::CyberPolicy { message })
        }
        ApiError::Transport(transport) => match transport {
            TransportError::Http {
                status,
                url,
                headers,
                body,
            } => {
                let body_text = body.unwrap_or_default();
                let provider_errors = parse_provider_error_bodies(&body_text);
                let provider_error = provider_errors.first();

                // Capacity is a provider availability state. Resolve it before generic wrapper
                // fields such as `invalid_request_error`, which some gateways attach to the same
                // capacity message.
                if status != http::StatusCode::UNAUTHORIZED
                    && status != http::StatusCode::FORBIDDEN
                    && is_server_overloaded_body(&body_text)
                {
                    return CodexErr::ServerOverloaded;
                }

                // Provider gateways sometimes put a permanent semantic error behind a 5xx or
                // 429 status. Resolve the semantic error before the HTTP retry classifier sees
                // the status, otherwise a permanent failure can be replayed indefinitely.
                if status != http::StatusCode::UNAUTHORIZED
                    && status != http::StatusCode::FORBIDDEN
                    && let Some(mapped) = provider_errors
                        .iter()
                        .find_map(|error| map_known_provider_error(error, headers.as_ref()))
                {
                    return mapped;
                }

                // Some gateways return permanent provider errors as plain text. Keep the
                // semantic result when the outer sampling or compaction layer sees this error,
                // instead of letting the transient HTTP status make it retry again.
                if status != http::StatusCode::UNAUTHORIZED
                    && status != http::StatusCode::FORBIDDEN
                    && is_permanent_error_message(&body_text)
                {
                    return CodexErr::InvalidRequest(body_text);
                }

                if status == http::StatusCode::BAD_REQUEST {
                    if provider_error.is_some_and(|error| {
                        error
                            .code
                            .as_deref()
                            .is_some_and(|code| code.eq_ignore_ascii_case(CYBER_POLICY_ERROR_CODE))
                    }) {
                        let message = provider_error
                            .and_then(|error| error.message.clone())
                            .filter(|message| !message.trim().is_empty())
                            .unwrap_or_else(|| CYBER_POLICY_FALLBACK_MESSAGE.to_string());
                        CodexErr::new(CodexErrorDetails::CyberPolicy { message })
                    } else if body_text
                        .contains("The image data you provided does not represent a valid image")
                    {
                        CodexErr::InvalidImageRequest()
                    } else {
                        CodexErr::InvalidRequest(body_text)
                    }
                } else if status == http::StatusCode::TOO_MANY_REQUESTS {
                    CodexErr::RetryLimit(RetryLimitReachedError {
                        status,
                        request_id: extract_request_tracking_id(headers.as_ref()),
                    })
                } else if status == http::StatusCode::INTERNAL_SERVER_ERROR {
                    CodexErr::InternalServerError
                } else {
                    CodexErr::UnexpectedStatus(UnexpectedResponseError {
                        status,
                        user_message: api_error_user_message(status, &body_text),
                        body: body_text,
                        url,
                        cf_ray: extract_header(headers.as_ref(), CF_RAY_HEADER),
                        request_id: extract_request_id(headers.as_ref()),
                        identity_authorization_error: extract_header(
                            headers.as_ref(),
                            X_OPENAI_AUTHORIZATION_ERROR_HEADER,
                        ),
                        identity_error_code: extract_x_error_json_code(headers.as_ref()),
                    })
                }
            }
            TransportError::RetryLimit => CodexErr::RetryLimit(RetryLimitReachedError {
                status: http::StatusCode::INTERNAL_SERVER_ERROR,
                request_id: None,
            }),
            TransportError::Timeout => CodexErr::RequestTimeout,
            TransportError::Connection(source) => {
                CodexErr::ConnectionFailed(ConnectionFailedError { source })
            }
            TransportError::Network(msg) => {
                match codex_client::classify_provider_error_text(&msg) {
                    codex_client::RetryDisposition::Capacity => CodexErr::ServerOverloaded,
                    codex_client::RetryDisposition::Transient => {
                        CodexErr::Stream(msg).with_explicit_retryable()
                    }
                    codex_client::RetryDisposition::DoNotRetry
                        if codex_client::is_permanent_error_text(&msg) =>
                    {
                        CodexErr::Stream(msg)
                    }
                    // Reqwest uses Network for protocol and body-read failures with unstable text.
                    // Preserve the transport class as an explicit transient when no permanent
                    // provider semantic is present.
                    codex_client::RetryDisposition::DoNotRetry => {
                        CodexErr::Stream(msg).with_explicit_retryable()
                    }
                }
            }
            TransportError::Build(msg) => CodexErr::InvalidRequest(msg),
        },
        ApiError::RateLimit(msg) => {
            if is_capacity_error_body(&msg) {
                CodexErr::ServerOverloaded
            } else if is_permanent_error_message(&msg) {
                CodexErr::InvalidRequest(msg)
            } else {
                CodexErr::Stream(msg).with_explicit_retryable()
            }
        }
    }
}

const ACTIVE_LIMIT_HEADER: &str = "x-codex-active-limit";
const REQUEST_ID_HEADER: &str = "x-request-id";
const OAI_REQUEST_ID_HEADER: &str = "x-oai-request-id";
const CF_RAY_HEADER: &str = "cf-ray";
const X_OPENAI_AUTHORIZATION_ERROR_HEADER: &str = "x-openai-authorization-error";
const X_ERROR_JSON_HEADER: &str = "x-error-json";
const CYBER_POLICY_ERROR_CODE: &str = "cyber_policy";
const CYBER_POLICY_FALLBACK_MESSAGE: &str =
    "This request has been flagged for possible cybersecurity risk.";
const CLOUDFLARE_BLOCKED_MESSAGE: &str =
    "Access blocked by Cloudflare. This usually happens when connecting from a restricted region";

#[cfg(test)]
#[path = "api_bridge_tests.rs"]
mod tests;

fn extract_request_tracking_id(headers: Option<&HeaderMap>) -> Option<String> {
    extract_request_id(headers).or_else(|| extract_header(headers, CF_RAY_HEADER))
}

fn api_error_user_message(status: http::StatusCode, body: &str) -> Option<String> {
    if status == http::StatusCode::FORBIDDEN
        && body.contains("Cloudflare")
        && body.contains("blocked")
    {
        Some(format!("{CLOUDFLARE_BLOCKED_MESSAGE} (status {status})"))
    } else {
        None
    }
}

fn extract_request_id(headers: Option<&HeaderMap>) -> Option<String> {
    extract_header(headers, REQUEST_ID_HEADER)
        .or_else(|| extract_header(headers, OAI_REQUEST_ID_HEADER))
}

fn extract_header(headers: Option<&HeaderMap>, name: &str) -> Option<String> {
    headers.and_then(|map| {
        map.get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    })
}

fn extract_x_error_json_code(headers: Option<&HeaderMap>) -> Option<String> {
    let encoded = extract_header(headers, X_ERROR_JSON_HEADER)?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()?;
    let parsed = serde_json::from_slice::<Value>(&decoded).ok()?;
    parsed
        .get("error")
        .and_then(|error| error.get("code"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

#[derive(Debug, Default)]
struct ProviderErrorBody {
    error_type: Option<String>,
    code: Option<String>,
    message: Option<String>,
    plan_type: Option<PlanType>,
    resets_at: Option<i64>,
}

fn parse_provider_error_bodies(body: &str) -> Vec<ProviderErrorBody> {
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return Vec::new();
    };

    let mut errors = Vec::new();
    collect_provider_error_bodies(&value, &mut errors, 0);
    errors
}

fn collect_provider_error_bodies(value: &Value, errors: &mut Vec<ProviderErrorBody>, depth: usize) {
    if depth > 8 {
        return;
    }

    if let Some(array) = value.as_array() {
        for child in array {
            collect_provider_error_bodies(child, errors, depth + 1);
        }
        return;
    }

    let Some(object) = value.as_object() else {
        return;
    };

    let error = ProviderErrorBody {
        error_type: object
            .get("type")
            .and_then(Value::as_str)
            .map(str::to_string),
        code: object
            .get("code")
            .and_then(Value::as_str)
            .map(str::to_string),
        message: object
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_string),
        plan_type: object
            .get("plan_type")
            .and_then(|value| serde_json::from_value::<PlanType>(value.clone()).ok()),
        resets_at: object.get("resets_at").and_then(Value::as_i64),
    };
    if error.error_type.is_some()
        || error.code.is_some()
        || error.message.is_some()
        || error.plan_type.is_some()
        || error.resets_at.is_some()
    {
        errors.push(error);
    }

    // Gateways can wrap provider errors in several envelopes. Walk all object values, rather
    // than only a field named `error`, while keeping a depth bound for hostile payloads.
    for child in object.values() {
        collect_provider_error_bodies(child, errors, depth + 1);
    }
}

fn map_known_provider_error(
    error: &ProviderErrorBody,
    headers: Option<&HeaderMap>,
) -> Option<CodexErr> {
    let matches_kind = |candidate: &str| {
        let candidate = candidate.replace(['_', '-'], " ");
        [error.code.as_deref(), error.error_type.as_deref()]
            .into_iter()
            .flatten()
            .any(|value| value.to_ascii_lowercase().replace(['_', '-'], " ") == candidate)
    };
    if error.code.is_none() && error.error_type.is_none() && error.message.is_none() {
        return None;
    }
    if matches_kind("context_length_exceeded") {
        return Some(CodexErr::ContextWindowExceeded);
    }
    if matches_kind("insufficient_quota") || matches_kind("quota_exceeded") {
        return Some(CodexErr::QuotaExceeded);
    }
    if matches_kind("usage_not_included") {
        return Some(CodexErr::UsageNotIncluded);
    }
    if matches_kind("usage_limit_reached") {
        let limit_id = extract_header(headers, ACTIVE_LIMIT_HEADER);
        let promo_message = headers.and_then(parse_promo_message);
        let rate_limit_reached_type = headers.and_then(parse_rate_limit_reached_type);
        let rate_limits = headers
            .and_then(|map| parse_rate_limit_for_limit(map, limit_id.as_deref()))
            .map(|mut snapshot| {
                snapshot.rate_limit_reached_type = rate_limit_reached_type;
                snapshot
            });
        let resets_at = error
            .resets_at
            .and_then(|seconds| DateTime::<Utc>::from_timestamp(seconds, 0));
        return Some(CodexErr::UsageLimitReached(UsageLimitReachedError {
            plan_type: error.plan_type.clone(),
            resets_at,
            rate_limits: rate_limits.map(Box::new),
            promo_message,
            rate_limit_reached_type,
        }));
    }
    if matches_kind(CYBER_POLICY_ERROR_CODE) {
        return Some(CodexErr::new(CodexErrorDetails::CyberPolicy {
            message: error
                .message
                .clone()
                .filter(|message| !message.trim().is_empty())
                .unwrap_or_else(|| CYBER_POLICY_FALLBACK_MESSAGE.to_string()),
        }));
    }
    if is_permanent_error_fields(error.error_type.as_deref(), error.code.as_deref(), None) {
        if is_server_overloaded_error(
            error.error_type.as_deref(),
            error.code.as_deref(),
            error.message.as_deref(),
        ) {
            return None;
        }
        return Some(CodexErr::InvalidRequest(
            error
                .message
                .clone()
                .unwrap_or_else(|| "Invalid request.".to_string()),
        ));
    }
    None
}

fn is_server_overloaded_body(body: &str) -> bool {
    if is_server_overloaded_error(None, None, Some(body)) {
        return true;
    }
    parse_provider_error_bodies(body).iter().any(|error| {
        is_server_overloaded_error(
            error.error_type.as_deref(),
            error.code.as_deref(),
            error.message.as_deref(),
        )
    })
}
