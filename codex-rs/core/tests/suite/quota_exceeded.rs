use anyhow::Result;
#[cfg(unix)]
use codex_features::Feature;
#[cfg(unix)]
use codex_login::CodexAuth;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::user_input::UserInput;
#[cfg(unix)]
use core_test_support::responses::ev_assistant_message;
#[cfg(unix)]
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
#[cfg(unix)]
use core_test_support::responses::mount_response_sequence;
use core_test_support::responses::mount_sse_once;
use core_test_support::responses::sse;
#[cfg(unix)]
use core_test_support::responses::sse_failed;
#[cfg(unix)]
use core_test_support::responses::sse_response;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::json;
#[cfg(unix)]
use wiremock::ResponseTemplate;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quota_exceeded_emits_single_error_event() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mut builder = test_codex();

    mount_sse_once(
        &server,
        sse(vec![
            ev_response_created("resp-1"),
            json!({
                "type": "response.failed",
                "response": {
                    "id": "resp-1",
                    "error": {
                        "code": "insufficient_quota",
                        "message": "You exceeded your current quota, please check your plan and billing details."
                    }
                }
            }),
        ]),
    )
    .await;

    let test = builder.build(&server).await?;

    test.codex
        .submit(Op::UserInput {
            environments: None,
            items: vec![UserInput::Text {
                text: "quota?".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
        })
        .await
        .unwrap();

    let mut error_events = 0;

    loop {
        let event = wait_for_event(&test.codex, |_| true).await;

        match event {
            EventMsg::Error(err) => {
                error_events += 1;
                assert_eq!(
                    err.message,
                    "Quota exceeded. Check your plan and billing details."
                );
            }
            EventMsg::TurnComplete(_) => break,
            _ => {}
        }
    }

    assert_eq!(error_events, 1, "expected exactly one Codex:Error event");

    Ok(())
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quota_exceeded_runs_successful_subscription_exhausted_hook_and_retries_with_fresh_route()
-> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let tempdir = tempfile::tempdir()?;
    let same_auth_path = tempdir.path().join("same-auth.json");
    std::fs::write(
        &same_auth_path,
        auth_json_for_chatgpt("Access Token", "account_id"),
    )?;

    let command_path = tempdir.path().join("successful_hook.py");
    std::fs::write(
        &command_path,
        format!(
            r#"import json
import shutil
import sys
from pathlib import Path

payload = json.load(sys.stdin)
codex_home = Path(payload["codex_home"])
codex_home.mkdir(parents=True, exist_ok=True)
with (codex_home / "subscription_exhausted_hook_log.jsonl").open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(payload) + "\n")
shutil.copyfile(r"{same_auth_path}", codex_home / "auth.json")
"#,
            same_auth_path = same_auth_path.display()
        ),
    )?;

    let quota_exceeded = sse_response(sse_failed("resp-1", "insufficient_quota", "quota exceeded"))
        .insert_header("x-codex-turn-state", "stale-route");
    let success = ResponseTemplate::new(200)
        .insert_header("content-type", "text/event-stream")
        .set_body_string(sse(vec![
            ev_response_created("resp-2"),
            ev_assistant_message("msg-2", "ok"),
            ev_completed("resp-2"),
        ]));
    let request_log = mount_response_sequence(&server, vec![quota_exceeded, success]).await;

    let command = format!("python3 {}", command_path.display());
    let mut builder = test_codex()
        .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        .with_pre_build_hook(move |home| {
            let hooks = json!({
                "hooks": {
                    "SubscriptionExhausted": [{
                        "hooks": [{
                            "type": "command",
                            "command": command,
                            "statusMessage": "selecting a fresh subscription route",
                            "timeout": 5
                        }]
                    }]
                }
            });
            std::fs::write(home.join("hooks.json"), hooks.to_string()).expect("write hooks.json");
        })
        .with_config(|config| {
            config
                .features
                .enable(Feature::CodexHooks)
                .expect("test config should allow hooks feature");
        });
    let test = builder.build(&server).await?;

    test.submit_turn("hello").await?;

    let requests = request_log.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].header("x-codex-turn-state"), None);
    assert_eq!(requests[1].header("x-codex-turn-state"), None);
    assert_eq!(
        requests[0].header("authorization").as_deref(),
        Some("Bearer Access Token")
    );
    assert_eq!(
        requests[1].header("authorization").as_deref(),
        Some("Bearer Access Token")
    );

    let hook_log_path = test
        .codex_home_path()
        .join("subscription_exhausted_hook_log.jsonl");
    let hook_log = std::fs::read_to_string(hook_log_path)?;
    let hook_payload: serde_json::Value = serde_json::from_str(
        hook_log
            .lines()
            .next()
            .expect("subscription hook should log one invocation"),
    )?;
    assert_eq!(hook_payload["hook_event_name"], "SubscriptionExhausted");
    assert_eq!(hook_payload["error_kind"], "quota_exceeded");

    Ok(())
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn usage_limit_reached_runs_subscription_exhausted_hook_and_retries_with_rotated_auth()
-> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let tempdir = tempfile::tempdir()?;
    let rotated_auth_path = tempdir.path().join("rotated-auth.json");
    std::fs::write(
        &rotated_auth_path,
        auth_json_for_chatgpt("rotated-access-token", "rotated-account"),
    )?;

    let command_path = tempdir.path().join("rotate_auth.py");
    std::fs::write(
        &command_path,
        format!(
            r#"import json
import shutil
import sys
from pathlib import Path

payload = json.load(sys.stdin)
codex_home = Path(payload["codex_home"])
codex_home.mkdir(parents=True, exist_ok=True)
with (codex_home / "subscription_exhausted_hook_log.jsonl").open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(payload) + "\n")
shutil.copyfile(r"{rotated_auth_path}", codex_home / "auth.json")
"#,
            rotated_auth_path = rotated_auth_path.display()
        ),
    )?;

    let usage_limit = ResponseTemplate::new(429).set_body_json(json!({
        "error": {
            "type": "usage_limit_reached",
            "message": "limit reached",
            "resets_at": 1704067242,
            "plan_type": "pro"
        }
    }));
    let success = ResponseTemplate::new(200)
        .insert_header("content-type", "text/event-stream")
        .set_body_string(sse(vec![
            ev_response_created("resp-2"),
            ev_assistant_message("msg-2", "ok"),
            ev_completed("resp-2"),
        ]));
    let request_log = mount_response_sequence(&server, vec![usage_limit, success]).await;

    let command = format!("python3 {}", command_path.display());
    let mut builder = test_codex()
        .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        .with_pre_build_hook(move |home| {
            let hooks = json!({
                "hooks": {
                    "SubscriptionExhausted": [{
                        "hooks": [{
                            "type": "command",
                            "command": command,
                            "statusMessage": "rotating exhausted subscription auth",
                            "timeout": 5
                        }]
                    }]
                }
            });
            std::fs::write(home.join("hooks.json"), hooks.to_string()).expect("write hooks.json");
        })
        .with_config(|config| {
            config
                .features
                .enable(Feature::CodexHooks)
                .expect("test config should allow hooks feature");
        });
    let test = builder.build(&server).await?;
    let listed_hooks = codex_hooks::list_hooks(codex_hooks::HooksConfig {
        feature_enabled: true,
        config_layer_stack: Some(test.config.config_layer_stack.clone()),
        ..codex_hooks::HooksConfig::default()
    });
    assert_eq!(listed_hooks.hooks.len(), 1);

    test.submit_turn("hello").await?;

    let hook_log_path = test
        .codex_home_path()
        .join("subscription_exhausted_hook_log.jsonl");
    assert!(
        hook_log_path.exists(),
        "subscription exhausted hook should have run"
    );
    let requests = request_log.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].header("authorization").as_deref(),
        Some("Bearer Access Token")
    );
    assert_eq!(
        requests[1].header("authorization").as_deref(),
        Some("Bearer rotated-access-token")
    );
    assert_eq!(
        requests[1].header("chatgpt-account-id").as_deref(),
        Some("rotated-account")
    );
    let hook_log = std::fs::read_to_string(hook_log_path)?;
    let hook_payload: serde_json::Value = serde_json::from_str(
        hook_log
            .lines()
            .next()
            .expect("subscription hook should log one invocation"),
    )?;
    assert_eq!(hook_payload["hook_event_name"], "SubscriptionExhausted");
    assert_eq!(hook_payload["error_kind"], "usage_limit_reached");
    assert_eq!(hook_payload["plan_type"], "pro");
    assert_eq!(hook_payload["resets_at"], 1704067242);
    assert_eq!(hook_payload["account_id"], "account_id");
    assert_eq!(
        hook_payload["codex_home"],
        test.codex_home_path().display().to_string()
    );

    Ok(())
}

#[cfg(unix)]
fn auth_json_for_chatgpt(access_token: &str, account_id: &str) -> String {
    use base64::Engine as _;

    let header = json!({ "alg": "none", "typ": "JWT" });
    let payload = json!({
        "email": "rotated@example.com",
        "https://api.openai.com/auth": {
            "chatgpt_plan_type": "plus",
            "chatgpt_account_id": account_id
        }
    });
    let encode = |value: &serde_json::Value| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.to_string())
    };
    let id_token = format!("{}.{}.sig", encode(&header), encode(&payload));

    json!({
        "auth_mode": "chatgpt",
        "OPENAI_API_KEY": null,
        "tokens": {
            "id_token": id_token,
            "access_token": access_token,
            "refresh_token": "rotated-refresh-token",
            "account_id": account_id
        },
        "last_refresh": chrono::Utc::now()
    })
    .to_string()
}
