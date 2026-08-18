use codex_http_client::Request;
use codex_http_client::TransportError;
use futures::future::BoxFuture;
use http::StatusCode;
use rand::Rng;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);
/// Sentinel for callers that explicitly opt into an unbounded retry budget.
pub const UNLIMITED_RETRIES: u64 = u64::MAX;
/// Persistent capacity retries remain available for a long provider outage, but stop after a
/// bounded number of attempts so a dead provider cannot keep a turn alive forever.
pub const PERSISTENT_CAPACITY_MAX_RETRIES: u64 = 100;

pub fn format_retry_budget(max_retries: u64) -> String {
    if max_retries == UNLIMITED_RETRIES {
        "unlimited".to_string()
    } else {
        max_retries.to_string()
    }
}

#[derive(Clone)]
pub struct RetryPolicy {
    pub max_attempts: u64,
    pub capacity_max_attempts: u64,
    pub base_delay: Duration,
    pub retry_on: RetryOn,
    pub retry_notifier: Option<RetryNotifier>,
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
            TransportError::Http { status, body, .. } => {
                match classify_http_response(*status, body.as_deref()) {
                    // Capacity is a provider semantic, not an HTTP transport preference. A
                    // caller that uses this fork must not leak it as terminal only because a
                    // provider placed it behind an unusual status code.
                    RetryDisposition::Capacity => true,
                    RetryDisposition::Transient if *status == StatusCode::TOO_MANY_REQUESTS => {
                        self.retry_429
                    }
                    RetryDisposition::Transient if status.is_server_error() => self.retry_5xx,
                    RetryDisposition::Transient => self.retry_transport,
                    RetryDisposition::DoNotRetry => false,
                }
            }
            TransportError::Timeout => self.retry_transport,
            TransportError::Connection(source) => {
                self.retry_transport
                    && classify_connection_error(source) != RetryDisposition::DoNotRetry
            }
            TransportError::Network(message) => {
                self.retry_transport
                    && classify_network_error(message) != RetryDisposition::DoNotRetry
            }
            TransportError::Build(_) | TransportError::RetryLimit => false,
        }
    }
}

/// The shared semantic result of classifying a failed remote request.
///
/// Unknown errors fail closed as [`RetryDisposition::DoNotRetry`]. Callers keep ownership of
/// operation-specific retry limits because replaying a model request and replaying a tool call
/// have different safety costs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDisposition {
    DoNotRetry,
    Transient,
    Capacity,
}

/// One bounded retry that a caller can expose through its UI event channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryStatus {
    pub operation: String,
    pub disposition: RetryDisposition,
    pub attempt: u64,
    pub max_retries: u64,
    pub delay: Duration,
    pub error: String,
}

/// An asynchronous retry-status sink. It must never add status text to request content.
pub type RetryNotifier = Arc<dyn Fn(RetryStatus) -> BoxFuture<'static, ()> + Send + Sync>;

const PERMANENT_ERROR_MARKERS: &[&str] = &[
    "account deactivated",
    "account disabled",
    "account is not active",
    "account suspended",
    "access denied",
    "auth error",
    "authentication error",
    "authentication failed",
    "authorization failed",
    "bad request",
    "billing error",
    "billing hard limit",
    "content filter",
    "content policy error",
    "content policy violation",
    "context length exceeded",
    "context window exceeded",
    "credentials rejected",
    "cyber policy",
    "does not have access",
    "does not support",
    "forbidden",
    "insufficient quota",
    "invalid api key",
    "invalid argument",
    "invalid credentials",
    "invalid input",
    "invalid model",
    "invalid prompt",
    "invalid request",
    "invalid token",
    "malformed request",
    "max usage reached",
    "model not found",
    "not eligible",
    "not supported",
    "payment required",
    "permission denied",
    "permission error",
    "plan does not allow",
    "plan limit reached",
    "policy error",
    "proxy authentication required",
    "quota error",
    "quota exceeded",
    "request too large",
    "request rejected",
    "required field",
    "resource not found",
    "route not found",
    "endpoint not found",
    "self signed certificate",
    "safety violation",
    "subscription required",
    "unrecognized request argument",
    "unsupported model",
    "unsupported parameter",
    "unauthorized",
    "unknown parameter",
    "unprocessable entity",
    "usage limit reached",
    "usage limit",
    "usage not included",
    "certificate expired",
    "certificate has expired",
    "certificate verify failed",
    "certificate verification failed",
    "certificate validation failed",
    "hostname mismatch",
    "invalid certificate",
    "model does not exist",
    "no such model",
    "unknown issuer",
    "exceeded your current quota",
    "canceled by caller",
    "cancelled by caller",
    "canceled by user",
    "cancelled by user",
    "user aborted",
];

/// Returns true when provider text identifies a permanent request, account, or policy failure.
pub fn is_permanent_error_text(text: &str) -> bool {
    let normalized = text.to_ascii_lowercase().replace(['_', '-'], " ");
    PERMANENT_ERROR_MARKERS
        .iter()
        .any(|marker| normalized.contains(marker))
}

/// Returns true when provider text identifies temporary model or server capacity.
pub fn is_capacity_error_text(text: &str) -> bool {
    if is_permanent_error_text(text) {
        return false;
    }
    let normalized = normalize_error_text(text);
    const CAPACITY_MARKERS: &[&str] = &[
        "at capacity",
        "capacity exceeded",
        "connection limit reached",
        "high demand",
        "model is temporarily unavailable",
        "model overloaded",
        "over capacity",
        "overloaded",
        "server busy",
        "server is overloaded",
        "server overloaded",
        "temporarily overloaded",
        "too many connections",
        "websocket connection limit",
        "slow down",
    ];
    CAPACITY_MARKERS
        .iter()
        .any(|marker| normalized.contains(marker))
}

/// Returns true when unstructured provider or transport text identifies a transient failure.
pub fn is_transient_error_text(text: &str) -> bool {
    let normalized = normalize_error_text(text);
    if is_capacity_error_text(&normalized) {
        return true;
    }
    const TRANSIENT_MARKERS: &[&str] = &[
        "broken pipe",
        "canceled",
        "cancelled",
        "connection closed",
        "connection dropped",
        "connection error",
        "connection refused",
        "connection reset",
        "connection terminated",
        "connection timed out",
        "deadline exceeded",
        "dns error",
        "dns lookup failed",
        "disconnected",
        "error decoding response body",
        "error sending request",
        "gateway timeout",
        "host not found",
        "internal error",
        "name or service not known",
        "network error",
        "network unreachable",
        "rate limit",
        "recovering",
        "remote host closed",
        "request canceled",
        "request cancelled",
        "server error",
        "server restarting",
        "service restarting",
        "socket closed",
        "stream closed",
        "stream ended",
        "temporary failure",
        "temporary name resolution failure",
        "timed out",
        "timeout",
        "too many requests",
        "transport closed",
        "try again",
        "try again later",
        "unexpected eof",
        "upstream failure",
        "host unreachable",
        "goaway",
        "refused stream",
        "service unavailable",
        "slow down",
        // Startup/reconnect layers use this phrase for a failed connection attempt. It is
        // narrower than a bare "failed" marker, which would replay permanent configuration
        // errors that happen to contain that word.
        "startup failed",
        "startup failure",
        "temporarily unavailable",
    ];
    TRANSIENT_MARKERS
        .iter()
        .any(|marker| normalized.contains(marker))
}

/// Classifies provider text without assuming that an unknown message is safe to replay.
pub fn classify_provider_error_text(text: &str) -> RetryDisposition {
    // A gateway can serialize a temporary provider error inside a generic
    // `invalid_request_error` envelope. Inspect nested capacity semantics before
    // the envelope's permanent-looking text wins.
    if is_capacity_error_body(text) {
        RetryDisposition::Capacity
    } else if is_local_cancellation_text(text) {
        // Tokio and reqwest can surface caller cancellation as ordinary provider-looking text.
        // Keep it terminal so an aborted turn cannot be resurrected by a retry loop.
        RetryDisposition::DoNotRetry
    } else if is_permanent_error_text(text) {
        RetryDisposition::DoNotRetry
    } else if is_capacity_error_text(text) {
        RetryDisposition::Capacity
    } else if is_transient_error_text(text) {
        RetryDisposition::Transient
    } else {
        RetryDisposition::DoNotRetry
    }
}

/// Classifies operating-system I/O failures without treating unknown local failures as safe to
/// replay. The listed kinds describe a connection or temporary readiness failure.
pub fn classify_io_error(error: &std::io::Error) -> RetryDisposition {
    use std::io::ErrorKind;

    if is_local_cancellation_text(&error.to_string()) || is_permanent_error_text(&error.to_string())
    {
        return RetryDisposition::DoNotRetry;
    }

    match error.kind() {
        ErrorKind::ConnectionRefused
        | ErrorKind::ConnectionReset
        | ErrorKind::HostUnreachable
        | ErrorKind::NetworkUnreachable
        | ErrorKind::ConnectionAborted
        | ErrorKind::NotConnected
        | ErrorKind::NetworkDown
        | ErrorKind::BrokenPipe
        | ErrorKind::WouldBlock
        | ErrorKind::TimedOut
        | ErrorKind::WriteZero
        | ErrorKind::Interrupted
        | ErrorKind::UnexpectedEof => RetryDisposition::Transient,
        ErrorKind::Other => classify_provider_error_text(&error.to_string()),
        _ => RetryDisposition::DoNotRetry,
    }
}

/// Classifies a request connection failure. Certificate, credential, and other permanent setup
/// failures remain terminal even though reqwest reports them through its connect category.
pub fn classify_connection_error(error: &codex_http_client::HttpError) -> RetryDisposition {
    if let Some(status) = error.status() {
        let message = error.to_string();
        if is_local_cancellation_text(&message) || is_permanent_error_text(&message) {
            return RetryDisposition::DoNotRetry;
        }
        if is_capacity_error_body(&message) {
            return RetryDisposition::Capacity;
        }
        return classify_http_response(status, None);
    }
    let message = error.to_string();
    if is_local_cancellation_text(&message) {
        RetryDisposition::DoNotRetry
    } else if is_capacity_error_body(&message) {
        RetryDisposition::Capacity
    } else if is_permanent_error_text(&message) {
        RetryDisposition::DoNotRetry
    } else if error.is_connect() || error.is_timeout() {
        RetryDisposition::Transient
    } else {
        classify_provider_error_text(&message)
    }
}

pub fn classify_transport_error(error: &TransportError) -> RetryDisposition {
    match error {
        TransportError::Http { status, body, .. } => {
            classify_http_response(*status, body.as_deref())
        }
        TransportError::Timeout => RetryDisposition::Transient,
        TransportError::Connection(error) => classify_connection_error(error),
        TransportError::Network(message) => classify_network_error(message),
        TransportError::Build(_) | TransportError::RetryLimit => RetryDisposition::DoNotRetry,
    }
}

/// Classifies an HTTP response after provider semantics have overridden its transport status.
pub fn classify_http_response(status: StatusCode, body: Option<&str>) -> RetryDisposition {
    // Authentication responses are terminal even when a gateway puts capacity words in the
    // response body. Replaying them cannot repair the caller's credentials.
    if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
        return RetryDisposition::DoNotRetry;
    }

    // Capacity is a provider availability state. Check it before generic wrapper fields such as
    // `invalid_request_error`, which some gateways attach to the same capacity message.
    if body.is_some_and(is_capacity_error_body) {
        return RetryDisposition::Capacity;
    }
    if body.is_some_and(is_permanent_error_body) {
        return RetryDisposition::DoNotRetry;
    }
    if is_transient_http_status(status) {
        return RetryDisposition::Transient;
    }
    if body.is_some_and(is_transient_error_text)
        && (status.is_server_error()
            || matches!(
                status,
                StatusCode::MISDIRECTED_REQUEST
                    | StatusCode::REQUEST_TIMEOUT
                    | StatusCode::TOO_EARLY
                    | StatusCode::TOO_MANY_REQUESTS
            ))
    {
        return RetryDisposition::Transient;
    }
    RetryDisposition::DoNotRetry
}

/// Returns true when a plain or nested provider response body identifies model or server
/// capacity. Structured wrappers are inspected before generic permanent-error markers, because a
/// gateway may label the envelope `invalid_request_error` while the nested provider message is
/// temporary capacity.
pub fn is_capacity_error_body(body: &str) -> bool {
    if is_capacity_error_text(body) {
        return true;
    }

    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    let mut fields = Vec::new();
    collect_capacity_error_fields(&value, &mut fields, 0);
    fields.into_iter().any(is_capacity_error_text)
}

fn collect_capacity_error_fields<'a>(
    value: &'a serde_json::Value,
    fields: &mut Vec<&'a str>,
    depth: usize,
) {
    if depth > 8 {
        return;
    }
    if let Some(array) = value.as_array() {
        for child in array {
            collect_capacity_error_fields(child, fields, depth + 1);
        }
        return;
    }
    let Some(object) = value.as_object() else {
        return;
    };
    for (key, child) in object {
        if matches!(
            key.as_str(),
            "code" | "type" | "error_code" | "error_type" | "message" | "reason" | "detail"
        ) && let Some(text) = child.as_str()
        {
            fields.push(text);
        }
        collect_capacity_error_fields(child, fields, depth + 1);
    }
}

/// A transport layer has already identified this failure as a network operation error. Unknown
/// network text remains transient because reqwest uses this branch for response-body and protocol
/// failures that have no stable error string. Known permanent provider text still fails closed.
fn classify_network_error(text: &str) -> RetryDisposition {
    if is_local_cancellation_text(text) {
        RetryDisposition::DoNotRetry
    } else if is_capacity_error_body(text) {
        RetryDisposition::Capacity
    } else if is_permanent_error_text(text) {
        RetryDisposition::DoNotRetry
    } else {
        match classify_provider_error_text(text) {
            RetryDisposition::Capacity => RetryDisposition::Capacity,
            RetryDisposition::Transient | RetryDisposition::DoNotRetry => {
                RetryDisposition::Transient
            }
        }
    }
}

fn is_local_cancellation_text(text: &str) -> bool {
    let normalized = normalize_error_text(text);
    [
        "operation canceled",
        "operation cancelled",
        // Hyper uses these forms when a caller cancellation drops an in-flight request.
        "operation was canceled",
        "operation was cancelled",
        // Reqwest and several upstream runtimes use the shorter request/context forms.
        "request canceled",
        "request cancelled",
        "context canceled",
        "context cancelled",
        "request was canceled",
        "request was cancelled",
        "task canceled by caller",
        "task cancelled by caller",
        "request aborted by caller",
        "request aborted by user",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
}

/// Returns true for HTTP statuses that are safe to retry without response-body semantics.
pub fn is_transient_http_status(status: StatusCode) -> bool {
    if matches!(
        status,
        StatusCode::MISDIRECTED_REQUEST
            | StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_EARLY
            | StatusCode::TOO_MANY_REQUESTS
    ) {
        return true;
    }
    if !status.is_server_error() {
        return false;
    }

    // These standard 5xx statuses describe unsupported protocols, configuration loops, or
    // missing authentication. A later attempt with the same request cannot repair them. Unknown
    // and provider-specific 5xx statuses remain transient so codes such as 529 keep working.
    !matches!(
        status,
        StatusCode::NOT_IMPLEMENTED
            | StatusCode::HTTP_VERSION_NOT_SUPPORTED
            | StatusCode::VARIANT_ALSO_NEGOTIATES
            | StatusCode::LOOP_DETECTED
            | StatusCode::NOT_EXTENDED
            | StatusCode::NETWORK_AUTHENTICATION_REQUIRED
    )
}

fn normalize_error_text(text: &str) -> String {
    text.to_ascii_lowercase().replace(['_', '-'], " ")
}

fn is_permanent_error_body(body: &str) -> bool {
    if is_permanent_error_text(body) {
        return true;
    }

    // Some providers use a top-level object, while others nest the fields under `error`. Parse
    // both shapes and inspect a few common wrapper fields. The textual fallback above still
    // covers non-JSON gateway responses.
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    let mut objects = Vec::new();
    collect_error_objects(&value, &mut objects, 0);
    objects.into_iter().any(|object| {
        [
            "code",
            "type",
            "error_code",
            "error_type",
            "message",
            "reason",
            "detail",
        ]
        .iter()
        .any(|field| {
            object
                .get(*field)
                .and_then(serde_json::Value::as_str)
                .is_some_and(is_permanent_error_text)
        })
    })
}

fn collect_error_objects<'a>(
    value: &'a serde_json::Value,
    objects: &mut Vec<&'a serde_json::Value>,
    depth: usize,
) {
    if depth > 8 {
        return;
    }
    if let Some(array) = value.as_array() {
        for value in array {
            collect_error_objects(value, objects, depth + 1);
        }
        return;
    }
    let Some(object) = value.as_object() else {
        return;
    };
    objects.push(value);
    for value in object.values() {
        collect_error_objects(value, objects, depth + 1);
    }
}

pub fn backoff(base: Duration, attempt: u64) -> Duration {
    if attempt == 0 {
        return base.min(MAX_RETRY_DELAY);
    }
    // Cap the exponent before narrowing to `u32`. Persistent capacity retries can run for a
    // long time, and a wrapped exponent would otherwise panic or reset the delay unexpectedly.
    let exponent = attempt.saturating_sub(1).min(31) as u32;
    let exp = 2u64.saturating_pow(exponent);
    let millis = base.min(MAX_RETRY_DELAY).as_millis().min(u64::MAX as u128) as u64;
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
    let mut attempt = 0u64;
    let mut transient_retries = 0u64;
    let mut capacity_retries = 0u64;
    loop {
        let req = make_req();
        match op(req, attempt).await {
            Ok(resp) => return Ok(resp),
            Err(err) => {
                let disposition = classify_transport_error(&err);
                let (retry_attempt, max_retries) = match disposition {
                    RetryDisposition::Capacity
                        if policy.retry_on.should_retry(
                            &err,
                            capacity_retries,
                            policy.capacity_max_attempts,
                        ) =>
                    {
                        capacity_retries = capacity_retries.saturating_add(1);
                        (capacity_retries, policy.capacity_max_attempts)
                    }
                    RetryDisposition::Transient
                        if policy.retry_on.should_retry(
                            &err,
                            transient_retries,
                            policy.max_attempts,
                        ) =>
                    {
                        transient_retries = transient_retries.saturating_add(1);
                        (transient_retries, policy.max_attempts)
                    }
                    RetryDisposition::DoNotRetry
                    | RetryDisposition::Transient
                    | RetryDisposition::Capacity => return Err(err),
                };
                let delay = backoff(policy.base_delay, retry_attempt);
                tracing::warn!(
                    target: "codex_client::retry",
                    attempt = retry_attempt,
                    max_attempts = max_retries,
                    delay_ms = delay.as_millis() as u64,
                    error = %retry_error_kind(&err),
                    retry_disposition = ?disposition,
                    "transient HTTP request failed; retrying"
                );
                if let Some(retry_notifier) = policy.retry_notifier.as_ref() {
                    retry_notifier(RetryStatus {
                        operation: "http/request".to_string(),
                        disposition,
                        attempt: retry_attempt,
                        max_retries,
                        delay,
                        error: err.to_string(),
                    })
                    .await;
                }
                crate::record_retry!(retry_attempt, delay, RetryOperation::HttpRequest);
                tokio::time::sleep(delay).await;
                attempt = attempt.saturating_add(1);
            }
        }
    }
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
