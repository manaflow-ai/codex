use codex_api::ImageEditRequest;
use codex_api::ImageGenerationRequest;
use codex_api::ImageResponse;
use codex_api::ImagesClient;
use codex_api::ReqwestTransport;
use codex_api::map_api_error;
use codex_client::RetryDisposition;
use codex_client::RetryNotifier;
use codex_client::RetryStatus;
use codex_client::format_retry_budget;
use codex_login::default_client::add_originator_header;
use codex_login::default_client::create_client;
use codex_model_provider::SharedModelProvider;
use codex_protocol::error::CodexErr;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::StreamErrorEvent;
use http::HeaderMap;
use http::HeaderValue;
use std::sync::Arc;
use std::time::Duration;

const X_CODEX_IMAGE_TURN_ID_HEADER: &str = "x-codex-image-turn-id";

pub(crate) struct ImageBackendError {
    message: String,
    codex_error: CodexErr,
}

impl ImageBackendError {
    fn from_api(error: codex_api::ApiError) -> Self {
        let message = error.to_string();
        Self {
            message,
            codex_error: map_api_error(error),
        }
    }

    fn from_message(message: String) -> Self {
        Self {
            codex_error: CodexErr::Stream(message.clone()),
            message,
        }
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn codex_error(&self) -> &CodexErr {
        &self.codex_error
    }
}

#[derive(Clone)]
pub(crate) struct CodexImagesBackend {
    provider: SharedModelProvider,
    originator: Option<String>,
}

impl CodexImagesBackend {
    /// Creates a backend that sends image requests through the active model provider.
    pub(crate) fn new(provider: SharedModelProvider, originator: Option<String>) -> Self {
        Self {
            provider,
            originator,
        }
    }

    /// Resolves the provider and auth required for the current image API request.
    async fn client(&self) -> Result<ImagesClient<ReqwestTransport>, ImageBackendError> {
        let provider = self
            .provider
            .api_provider()
            .await
            .map_err(|err| ImageBackendError::from_message(err.to_string()))?;
        let auth = self
            .provider
            .api_auth()
            .await
            .map_err(|err| ImageBackendError::from_message(err.to_string()))?;
        Ok(ImagesClient::new(
            ReqwestTransport::from_http_client(create_client()),
            provider,
            auth,
        ))
    }

    /// Sends a standalone image generation request through the configured Images client.
    pub(crate) async fn generate(
        &self,
        request: ImageGenerationRequest,
        turn_id: &str,
        retry_notifier: Option<RetryNotifier>,
    ) -> Result<ImageResponse, ImageBackendError> {
        self.client()
            .await?
            .with_retry_notifier(retry_notifier)
            .generate(
                &request,
                image_request_headers(self.originator.as_deref(), turn_id),
            )
            .await
            .map_err(ImageBackendError::from_api)
    }

    /// Sends a standalone image edit request through the configured Images client.
    pub(crate) async fn edit(
        &self,
        request: ImageEditRequest,
        turn_id: &str,
        retry_notifier: Option<RetryNotifier>,
    ) -> Result<ImageResponse, ImageBackendError> {
        self.client()
            .await?
            .with_retry_notifier(retry_notifier)
            .edit(
                &request,
                image_request_headers(self.originator.as_deref(), turn_id),
            )
            .await
            .map_err(ImageBackendError::from_api)
    }
}

pub(crate) fn image_retry_notifier(
    emitter: Arc<dyn codex_tools::TurnItemEmitter>,
) -> RetryNotifier {
    Arc::new(move |status: RetryStatus| {
        let emitter = Arc::clone(&emitter);
        Box::pin(async move {
            let retry_kind = match status.disposition {
                RetryDisposition::Capacity => "Image service is at capacity",
                RetryDisposition::Transient => "Image request failed",
                RetryDisposition::DoNotRetry => return,
            };
            let delay = if status.delay < Duration::from_secs(1) {
                format!("{}ms", status.delay.as_millis().max(1))
            } else {
                format!("{:.1}s", status.delay.as_secs_f64())
            };
            emitter
                .emit_event(EventMsg::StreamError(StreamErrorEvent {
                    message: format!(
                        "{retry_kind}. Retrying in {delay} (attempt {}/{})",
                        status.attempt,
                        format_retry_budget(status.max_retries),
                    ),
                    codex_error_info: None,
                    additional_details: Some(status.error),
                }))
                .await;
        })
    })
}

fn image_request_headers(originator: Option<&str>, turn_id: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Ok(turn_id) = HeaderValue::from_str(turn_id) {
        headers.insert(X_CODEX_IMAGE_TURN_ID_HEADER, turn_id);
    }
    if let Some(originator) = originator {
        add_originator_header(&mut headers, originator);
    }
    headers
}
