use std::sync::Arc;
use std::sync::OnceLock;

use super::trim_function_call_history_to_fit_context_window;
use crate::Prompt;
use crate::client::CompactConversationRequestSettings;
use crate::compact::CompactionAnalyticsDetails;
use crate::responses_metadata::CodexResponsesRequestKind;
use crate::responses_metadata::CompactionTurnMetadata;
use crate::responses_retry::ResponsesStreamRequest;
use crate::responses_retry::ResponsesStreamRetryState;
use crate::responses_retry::handle_retryable_response_error_with_cancellation;
use crate::responses_retry::retry_status_notifier;
use crate::session::session::Session;
use crate::session::step_context::StepContext;
use codex_async_utils::OrCancelExt;
use codex_protocol::auth::AuthMode;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::models::ResponseItem;
use codex_rollout_trace::CompactionTraceContext;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub(super) struct RemoteCompactAttempt {
    pub(super) new_history: Vec<ResponseItem>,
    pub(super) trace_input_history: Option<Vec<ResponseItem>>,
}

pub(super) async fn run_remote_compact_attempt(
    sess: &Arc<Session>,
    step_context: &Arc<StepContext>,
    turn_state: Option<Arc<OnceLock<String>>>,
    compaction_trace: &CompactionTraceContext,
    compaction_metadata: CompactionTurnMetadata,
    analytics_details: &mut CompactionAnalyticsDetails,
    cancellation_token: &CancellationToken,
) -> CodexResult<RemoteCompactAttempt> {
    if cancellation_token.is_cancelled() {
        return Err(codex_protocol::error::CodexErr::TurnAborted);
    }
    let turn_context = &step_context.turn;
    let mut history = sess.clone_history().await;
    let base_instructions = sess.get_base_instructions().await;
    let (rewritten_outputs, estimated_deleted_tokens) =
        trim_function_call_history_to_fit_context_window(
            &mut history,
            turn_context.as_ref(),
            &base_instructions,
        );
    if rewritten_outputs > 0 {
        info!(
            turn_id = %turn_context.sub_id,
            rewritten_outputs,
            "rewrote history outputs before remote compaction"
        );
    }
    if estimated_deleted_tokens > 0 {
        let max_local_deleted_tokens = sess
            .estimated_tokens_after_last_model_generated_item()
            .await;
        analytics_details.active_context_tokens_before = analytics_details
            .active_context_tokens_before
            .map(|active_context_tokens_before| {
                active_context_tokens_before
                    .saturating_sub(estimated_deleted_tokens.min(max_local_deleted_tokens))
            });
    }
    let trace_input_history = compaction_trace
        .is_enabled()
        .then(|| history.raw_items().cloned().collect());
    let prompt_input = history.for_prompt(&turn_context.model_info.input_modalities);
    let tool_router = &step_context.tool_router;
    let prompt = Prompt {
        input: prompt_input,
        tools: tool_router.model_visible_specs(),
        parallel_tool_calls: true,
        base_instructions,
        output_schema: None,
        output_schema_strict: true,
    };
    let window_id = sess.current_window_id().await;
    let responses_metadata = turn_context.turn_metadata_state.to_responses_metadata(
        sess.installation_id.clone(),
        window_id,
        CodexResponsesRequestKind::Compaction(compaction_metadata),
    );
    let max_retries = turn_context.provider.info().stream_max_retries();
    let mut retry_state = ResponsesStreamRetryState::default();
    let new_history = loop {
        if cancellation_token.is_cancelled() {
            return Err(codex_protocol::error::CodexErr::TurnAborted);
        }
        let result = sess
            .services
            .model_client
            .compact_conversation_history(
                &prompt,
                &turn_context.model_info,
                turn_state.clone(),
                CompactConversationRequestSettings {
                    effort: turn_context.reasoning_effort.clone(),
                    summary: turn_context.reasoning_summary,
                    service_tier: if sess.services.auth_manager.auth_mode()
                        == Some(AuthMode::ApiKey)
                    {
                        None
                    } else {
                        turn_context.config.service_tier.clone()
                    },
                },
                &turn_context.session_telemetry,
                Some(retry_status_notifier(
                    Arc::clone(sess),
                    Arc::clone(turn_context),
                )),
                compaction_trace,
                &responses_metadata,
            )
            .or_cancel(cancellation_token)
            .await
            .map_err(|_| codex_protocol::error::CodexErr::TurnAborted)?;
        match result {
            Ok(new_history) => break new_history,
            Err(err) => {
                handle_retryable_response_error_with_cancellation(
                    &mut retry_state,
                    max_retries,
                    err,
                    sess,
                    turn_context,
                    ResponsesStreamRequest::RemoteCompactionV1,
                    cancellation_token,
                )
                .await?;
            }
        }
    };
    Ok(RemoteCompactAttempt {
        new_history,
        trace_input_history,
    })
}
