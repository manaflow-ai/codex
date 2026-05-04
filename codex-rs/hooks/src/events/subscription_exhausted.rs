use std::path::PathBuf;

use codex_protocol::ThreadId;
use codex_protocol::protocol::HookCompletedEvent;
use codex_protocol::protocol::HookEventName;
use codex_protocol::protocol::HookOutputEntry;
use codex_protocol::protocol::HookOutputEntryKind;
use codex_protocol::protocol::HookRunStatus;
use codex_protocol::protocol::HookRunSummary;
use codex_utils_absolute_path::AbsolutePathBuf;

use super::common;
use crate::engine::CommandShell;
use crate::engine::ConfiguredHandler;
use crate::engine::command_runner::CommandRunResult;
use crate::engine::dispatcher;
use crate::engine::output_parser;
use crate::schema::NullableString;
use crate::schema::SubscriptionExhaustedCommandInput;

#[derive(Debug, Clone)]
pub struct SubscriptionExhaustedRequest {
    pub session_id: ThreadId,
    pub turn_id: String,
    pub cwd: AbsolutePathBuf,
    pub codex_home: PathBuf,
    pub transcript_path: Option<PathBuf>,
    pub model: String,
    pub permission_mode: String,
    pub error_kind: String,
    pub plan_type: Option<String>,
    pub resets_at: Option<i64>,
    pub account_id: Option<String>,
}

#[derive(Debug)]
pub struct SubscriptionExhaustedOutcome {
    pub hook_events: Vec<HookCompletedEvent>,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SubscriptionExhaustedHandlerData;

pub(crate) fn preview(
    handlers: &[ConfiguredHandler],
    _request: &SubscriptionExhaustedRequest,
) -> Vec<HookRunSummary> {
    dispatcher::select_handlers(
        handlers,
        HookEventName::SubscriptionExhausted,
        /*matcher_input*/ None,
    )
    .into_iter()
    .map(|handler| dispatcher::running_summary(&handler))
    .collect()
}

pub(crate) async fn run(
    handlers: &[ConfiguredHandler],
    shell: &CommandShell,
    request: SubscriptionExhaustedRequest,
) -> SubscriptionExhaustedOutcome {
    let matched = dispatcher::select_handlers(
        handlers,
        HookEventName::SubscriptionExhausted,
        /*matcher_input*/ None,
    );
    if matched.is_empty() {
        return SubscriptionExhaustedOutcome {
            hook_events: Vec::new(),
        };
    }

    let input_json = match serde_json::to_string(&SubscriptionExhaustedCommandInput {
        session_id: request.session_id.to_string(),
        turn_id: request.turn_id.clone(),
        transcript_path: NullableString::from_path(request.transcript_path.clone()),
        cwd: request.cwd.display().to_string(),
        codex_home: request.codex_home.display().to_string(),
        hook_event_name: "SubscriptionExhausted".to_string(),
        model: request.model.clone(),
        permission_mode: request.permission_mode.clone(),
        error_kind: request.error_kind.clone(),
        plan_type: NullableString::from_string(request.plan_type.clone()),
        resets_at: request.resets_at,
        account_id: NullableString::from_string(request.account_id.clone()),
    }) {
        Ok(input_json) => input_json,
        Err(error) => {
            return SubscriptionExhaustedOutcome {
                hook_events: common::serialization_failure_hook_events(
                    matched,
                    Some(request.turn_id),
                    format!("failed to serialize subscription exhausted hook input: {error}"),
                ),
            };
        }
    };

    let results = dispatcher::execute_handlers(
        shell,
        matched,
        input_json,
        request.cwd.as_path(),
        Some(request.turn_id),
        parse_completed,
    )
    .await;

    SubscriptionExhaustedOutcome {
        hook_events: results.into_iter().map(|result| result.completed).collect(),
    }
}

fn parse_completed(
    handler: &ConfiguredHandler,
    run_result: CommandRunResult,
    turn_id: Option<String>,
) -> dispatcher::ParsedHandler<SubscriptionExhaustedHandlerData> {
    let mut entries = Vec::new();
    let mut status = HookRunStatus::Completed;

    match run_result.error.as_deref() {
        Some(error) => {
            status = HookRunStatus::Failed;
            entries.push(HookOutputEntry {
                kind: HookOutputEntryKind::Error,
                text: error.to_string(),
            });
        }
        None => match run_result.exit_code {
            Some(0) => {
                let trimmed_stdout = run_result.stdout.trim();
                if trimmed_stdout.is_empty() {
                } else if let Some(parsed) =
                    output_parser::parse_subscription_exhausted(&run_result.stdout)
                {
                    if !parsed.universal.suppress_output
                        && let Some(system_message) = parsed.universal.system_message
                    {
                        entries.push(HookOutputEntry {
                            kind: HookOutputEntryKind::Warning,
                            text: system_message,
                        });
                    }
                    if !parsed.universal.continue_processing {
                        status = HookRunStatus::Stopped;
                        if let Some(stop_reason) = parsed.universal.stop_reason {
                            entries.push(HookOutputEntry {
                                kind: HookOutputEntryKind::Stop,
                                text: stop_reason,
                            });
                        }
                    }
                } else if trimmed_stdout.starts_with('{') || trimmed_stdout.starts_with('[') {
                    status = HookRunStatus::Failed;
                    entries.push(HookOutputEntry {
                        kind: HookOutputEntryKind::Error,
                        text: "hook returned invalid subscription exhausted JSON output"
                            .to_string(),
                    });
                } else {
                    entries.push(HookOutputEntry {
                        kind: HookOutputEntryKind::Warning,
                        text: trimmed_stdout.to_string(),
                    });
                }
            }
            Some(exit_code) => {
                status = HookRunStatus::Failed;
                entries.push(HookOutputEntry {
                    kind: HookOutputEntryKind::Error,
                    text: format!("hook exited with code {exit_code}"),
                });
            }
            None => {
                status = HookRunStatus::Failed;
                entries.push(HookOutputEntry {
                    kind: HookOutputEntryKind::Error,
                    text: "hook exited without a status code".to_string(),
                });
            }
        },
    }

    let completed = HookCompletedEvent {
        turn_id,
        run: dispatcher::completed_summary(handler, &run_result, status, entries),
    };

    dispatcher::ParsedHandler {
        completed,
        data: SubscriptionExhaustedHandlerData,
    }
}
