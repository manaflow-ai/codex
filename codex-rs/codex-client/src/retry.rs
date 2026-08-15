use codex_http_client::Request;
use codex_http_client::TransportError;
use http::StatusCode;
use rand::Rng;
use std::future::Future;
use std::time::Duration;

const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u64,
    pub base_delay: Duration,
    pub retry_on: RetryOn,
}

#[derive(Debug, Clone)]
pub struct RetryOn {
    pub retry_429: bool,
    pub retry_5xx: bool,
    pub retry_transport: bool,
}

impl RetryOn {
    pub fn should_retry(&self, err: &TransportError, attempt: u64, max_attempts: u64) -> bool {
        if attempt >= max_attempts {
            return false;
        }
        match err {
            TransportError::Http { status, body, .. } => match *status {
                StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_EARLY => self.retry_transport,
                StatusCode::TOO_MANY_REQUESTS => {
                    self.retry_429 && !is_permanent_rate_limit_body(body.as_deref())
                }
                _ if status.is_server_error() => self.retry_5xx,
                _ => false,
            },
            TransportError::Timeout
            | TransportError::Connection(_)
            | TransportError::Network(_) => self.retry_transport,
            _ => false,
        }
    }
}

fn is_permanent_rate_limit_body(body: Option<&str>) -> bool {
    let Some(body) = body else {
        return false;
    };
    let compact = body
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect::<String>();
    [
        "\"code\":\"insufficient_quota\"",
        "\"type\":\"insufficient_quota\"",
        "\"code\":\"usage_limit_reached\"",
        "\"type\":\"usage_limit_reached\"",
        "\"code\":\"usage_not_included\"",
        "\"type\":\"usage_not_included\"",
        "\"code\":\"billing_hard_limit_reached\"",
        "\"type\":\"billing_hard_limit_reached\"",
        "\"code\":\"account_deactivated\"",
        "\"type\":\"account_deactivated\"",
    ]
    .iter()
    .any(|marker| compact.contains(marker))
}

pub fn backoff(base: Duration, attempt: u64) -> Duration {
    if attempt == 0 {
        return base;
    }
    let exp = 2u64.saturating_pow(attempt as u32 - 1);
    let millis = base.as_millis() as u64;
    let raw = millis.saturating_mul(exp);
    let jitter: f64 = rand::rng().random_range(0.9..1.1);
    Duration::from_millis((raw as f64 * jitter) as u64).min(MAX_RETRY_DELAY)
}

/// Identifies a retry path and its associated trace-event layer.
#[derive(Debug, Clone, Copy)]
pub enum RetryOperation {
    HttpRequest,
    Sampling,
    LocalCompaction,
    RemoteCompactionV1,
    RemoteCompactionV2,
}

/// Emits retry telemetry at the caller's source location without adding it to normal OTEL logs.
#[macro_export]
macro_rules! record_retry {
    ($attempt:expr, $delay:expr, $operation:expr $(,)?) => {{
        let (layer, operation) = match $operation {
            $crate::RetryOperation::HttpRequest => ("http", "request"),
            $crate::RetryOperation::Sampling => ("stream", "sampling"),
            $crate::RetryOperation::LocalCompaction => ("stream", "local_compaction"),
            $crate::RetryOperation::RemoteCompactionV1 => ("request", "remote_compaction_v1"),
            $crate::RetryOperation::RemoteCompactionV2 => ("stream", "remote_compaction_v2"),
        };

        ::tracing::event!(
            target: "codex_otel.trace_safe",
            ::tracing::Level::TRACE,
            event.name = "codex.retry",
            retry.attempt = $attempt,
            retry.delay_ms = ($delay).as_millis() as u64,
            retry.layer = layer,
            retry.operation = operation,
        );
    }};
}

pub async fn run_with_retry<T, F, Fut>(
    policy: RetryPolicy,
    mut make_req: impl FnMut() -> Request,
    op: F,
) -> Result<T, TransportError>
where
    F: Fn(Request, u64) -> Fut,
    Fut: Future<Output = Result<T, TransportError>>,
{
    for attempt in 0..=policy.max_attempts {
        let req = make_req();
        match op(req, attempt).await {
            Ok(resp) => return Ok(resp),
            Err(err)
                if policy
                    .retry_on
                    .should_retry(&err, attempt, policy.max_attempts) =>
            {
                let retry_attempt = attempt + 1;
                let delay = backoff(policy.base_delay, retry_attempt);
                tracing::warn!(
                    target: "codex_client::retry",
                    attempt = retry_attempt,
                    max_attempts = policy.max_attempts,
                    delay_ms = delay.as_millis() as u64,
                    error = %retry_error_kind(&err),
                    "transient HTTP request failed; retrying"
                );
                crate::record_retry!(retry_attempt, delay, RetryOperation::HttpRequest);
                tokio::time::sleep(delay).await;
            }
            Err(err) => return Err(err),
        }
    }
    Err(TransportError::RetryLimit)
}

fn retry_error_kind(error: &TransportError) -> String {
    match error {
        TransportError::Http { status, .. } => format!("http {status}"),
        TransportError::RetryLimit => "retry limit".to_string(),
        TransportError::Timeout => "timeout".to_string(),
        TransportError::Connection(_) => "connection".to_string(),
        TransportError::Network(_) => "network".to_string(),
        TransportError::Build(_) => "build".to_string(),
    }
}
