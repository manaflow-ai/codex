use std::sync::Arc;

use codex_hooks::SubscriptionExhaustedRequest;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_protocol::auth::PlanType;
use codex_protocol::error::CodexErr;

use crate::hook_runtime::run_subscription_exhausted_hooks;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SubscriptionExhaustionRecovery {
    pub auth_changed: bool,
}

#[derive(PartialEq, Eq)]
struct AuthSnapshot {
    mode: codex_app_server_protocol::AuthMode,
    account_id: Option<String>,
    token: Option<String>,
}

pub(crate) fn should_run_hook_for_error(err: &CodexErr) -> bool {
    matches!(
        err,
        CodexErr::UsageLimitReached(_) | CodexErr::QuotaExceeded
    )
}

pub(crate) async fn recover_with_hook_if_available(
    sess: &Arc<Session>,
    turn_context: &Arc<TurnContext>,
    err: &CodexErr,
) -> Option<SubscriptionExhaustionRecovery> {
    let auth_manager = sess.services.model_client.auth_manager()?;
    let auth_before = auth_manager.auth_cached()?;

    if !can_refresh_managed_chatgpt_auth(&auth_before) {
        return None;
    }

    let before = auth_snapshot(&auth_before);
    let request =
        build_hook_request(sess, turn_context, auth_manager.as_ref(), &auth_before, err).await;

    if !run_subscription_exhausted_hooks(sess, turn_context, request).await {
        return None;
    }

    auth_manager.reload().await;

    let auth_changed = auth_manager
        .auth_cached()
        .is_some_and(|auth_after| auth_snapshot(&auth_after) != before);

    Some(SubscriptionExhaustionRecovery { auth_changed })
}

fn can_refresh_managed_chatgpt_auth(auth: &CodexAuth) -> bool {
    auth.is_chatgpt_auth() && !auth.is_external_chatgpt_tokens()
}

async fn build_hook_request(
    sess: &Arc<Session>,
    turn_context: &Arc<TurnContext>,
    auth_manager: &AuthManager,
    auth: &CodexAuth,
    err: &CodexErr,
) -> SubscriptionExhaustedRequest {
    let (error_kind, plan_type, resets_at) = subscription_error_fields(err);

    SubscriptionExhaustedRequest {
        session_id: sess.conversation_id,
        turn_id: turn_context.sub_id.clone(),
        cwd: turn_context.cwd.clone(),
        codex_home: auth_manager.codex_home().to_path_buf(),
        transcript_path: sess.hook_transcript_path().await,
        model: turn_context.model_info.slug.clone(),
        permission_mode: crate::hook_runtime::hook_permission_mode(turn_context),
        error_kind,
        plan_type,
        resets_at,
        account_id: auth.get_account_id(),
    }
}

fn subscription_error_fields(err: &CodexErr) -> (String, Option<String>, Option<i64>) {
    match err {
        CodexErr::UsageLimitReached(err) => (
            "usage_limit_reached".to_string(),
            err.plan_type.clone().map(plan_type_wire_value),
            err.resets_at.map(|resets_at| resets_at.timestamp()),
        ),
        CodexErr::QuotaExceeded => ("quota_exceeded".to_string(), None, None),
        _ => unreachable!("subscription hook request only builds for subscription errors"),
    }
}

fn plan_type_wire_value(plan_type: PlanType) -> String {
    match plan_type {
        PlanType::Known(plan) => plan.raw_value().to_string(),
        PlanType::Unknown(raw) => raw,
    }
}

fn auth_snapshot(auth: &CodexAuth) -> AuthSnapshot {
    AuthSnapshot {
        mode: auth.api_auth_mode(),
        account_id: auth.get_account_id(),
        token: auth.get_token().ok(),
    }
}
