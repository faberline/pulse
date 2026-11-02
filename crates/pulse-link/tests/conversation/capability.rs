use super::*;

#[tokio::test]
async fn legacy_chat_clarifies_implicit_followups_but_keeps_explicit_controls() {
    let (_root, hub, cred, epoch) = setup().await;
    say(&hub, &cred, epoch, "a", "任務 A：讀 README.md").await;
    let legacy = hub.begin(&cred.device, 0, pulse_link::now()).await.unwrap();
    // No declaration: an old client can query local memory without a boundary.
    say(&hub, &cred, legacy, "implicit", "再詳細一點").await;
    assert!(reply(&hub, "implicit").await.contains("哪個任務"));
    say(&hub, &cred, legacy, "query", "A").await;
    say(&hub, &cred, legacy, "implicit-again", "用表格整理").await;
    assert!(reply(&hub, "implicit-again").await.contains("哪個任務"));
    assert_eq!(hub.core.task("task-a").await.unwrap().revision, 1);
    say(&hub, &cred, legacy, "explicit", "補充 A：保留來源").await;
    assert_eq!(hub.core.task("task-a").await.unwrap().revision, 2);
    assert_eq!(hub.core.list_tasks(100).await.unwrap().len(), 1);
    let focus: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM settings WHERE key='chat_conversation_context_v1'",
    )
    .fetch_one(&hub.core.application_pool())
    .await
    .unwrap();
    assert_eq!(focus, 0);
    assert!(remote::step(&hub, pulse_link::now()).await.unwrap());
    let blocked = hub.core.task("task-a").await.unwrap();
    assert_eq!(blocked.state, State::Blocked);
    assert_eq!(blocked.reason.as_deref(), Some("provider_not_configured"));
    let calls: i64 = sqlx::query_scalar("SELECT count(*) FROM budget")
        .fetch_one(&hub.core.application_pool())
        .await
        .unwrap();
    assert_eq!(calls, 0);
}

#[tokio::test]
async fn unready_interlude_cannot_restore_old_pending_text_after_upgrade() {
    let (_root, hub, cred, epoch) = setup().await;
    say(&hub, &cred, epoch, "a", "任務 A：讀 README.md").await;
    say(&hub, &cred, epoch, "b", "任務 B：讀 Cargo.toml").await;
    say(&hub, &cred, epoch, "both", "查詢 A、B").await;
    say(&hub, &cred, epoch, "pending", "用表格整理").await;
    assert!(reply(&hub, "pending").await.contains("哪個任務"));
    let legacy = hub.begin(&cred.device, 0, pulse_link::now()).await.unwrap();
    // The legacy client sends no Human frame. Its local topic change is invisible.
    hub.end(&cred.device, legacy).await.unwrap();
    let current = hub.begin(&cred.device, 0, pulse_link::now()).await.unwrap();
    assert!(
        hub.conversation_ready(&cred.device, current, 1, pulse_link::now() + LEASE_MS + 1)
            .await
            .is_err()
    );
    hub.conversation_ready(&cred.device, current, 1, pulse_link::now())
        .await
        .unwrap();
    say(&hub, &cred, current, "select", "B").await;
    let b = hub.core.task("task-b").await.unwrap();
    assert_eq!(b.revision, 1);
    assert!(!b.goal.contains("用表格整理"));
    say(&hub, &cred, current, "fresh", "再簡短一點").await;
    assert_eq!(hub.core.task("task-b").await.unwrap().revision, 2);
    assert_eq!(hub.core.task("task-a").await.unwrap().revision, 1);
}

#[tokio::test]
async fn conversation_declaration_requires_exact_version_current_chat_lease_and_valid_metadata() {
    let (_root, hub, cred, epoch) = setup().await;
    say(&hub, &cred, epoch, "a", "任務 A：讀 README.md").await;
    let current = hub.begin(&cred.device, 0, pulse_link::now()).await.unwrap();
    assert!(
        hub.conversation_ready(&cred.device, epoch, 1, pulse_link::now())
            .await
            .is_err()
    );
    assert!(
        hub.conversation_ready(&cred.device, current, 2, pulse_link::now())
            .await
            .is_err()
    );
    say(&hub, &cred, current, "unready", "再詳細一點").await;
    assert_eq!(hub.core.task("task-a").await.unwrap().revision, 1);
    hub.conversation_ready(&cred.device, current, 1, pulse_link::now())
        .await
        .unwrap();
    say(&hub, &cred, current, "focus", "A").await;
    say(&hub, &cred, current, "ready", "再詳細一點").await;
    assert_eq!(hub.core.task("task-a").await.unwrap().revision, 2);
    for (index, value) in [
        "not-json",
        r#"{"version":2,"epoch":2}"#,
        r#"{"version":1,"epoch":999}"#,
    ]
    .iter()
    .enumerate()
    {
        say(&hub, &cred, current, &format!("focus-{index}"), "A").await;
        sqlx::query("UPDATE settings SET value=? WHERE key=?")
            .bind(value)
            .bind(format!("chat-context:{}", cred.device))
            .execute(&hub.core.application_pool())
            .await
            .unwrap();
        say(&hub, &cred, current, &format!("bad-{index}"), "用條列整理").await;
        assert_eq!(hub.core.task("task-a").await.unwrap().revision, 2);
        hub.conversation_ready(&cred.device, current, 1, pulse_link::now())
            .await
            .unwrap();
    }
    say(&hub, &cred, current, "before-end", "A").await;
    hub.end(&cred.device, current).await.unwrap();
    assert!(
        hub.conversation_ready(&cred.device, current, 1, pulse_link::now())
            .await
            .is_err()
    );
    say(&hub, &cred, current, "ended", "再詳細一點").await;
    assert_eq!(hub.core.task("task-a").await.unwrap().revision, 2);
    let current = hub.begin(&cred.device, 0, pulse_link::now()).await.unwrap();
    hub.conversation_ready(&cred.device, current, 1, pulse_link::now())
        .await
        .unwrap();
    say(&hub, &cred, current, "before-recovery", "A").await;
    hub.recover().await.unwrap();
    let current = hub.begin(&cred.device, 0, pulse_link::now()).await.unwrap();
    hub.conversation_ready(&cred.device, current, 1, pulse_link::now())
        .await
        .unwrap();
    say(&hub, &cred, current, "recovered", "再詳細一點").await;
    assert!(reply(&hub, "recovered").await.contains("哪個任務"));
    assert_eq!(hub.core.task("task-a").await.unwrap().revision, 2);
    let code = hub
        .pairing(ClientRole::LocalExecutor, pulse_link::now())
        .await
        .unwrap();
    let local = hub
        .redeem(
            PairRequest {
                code,
                role: ClientRole::LocalExecutor,
                device: uuid::Uuid::new_v4().to_string(),
            },
            pulse_link::now(),
        )
        .await
        .unwrap();
    let local_epoch = hub
        .begin(&local.device, 0, pulse_link::now())
        .await
        .unwrap();
    assert!(
        hub.conversation_ready(&local.device, local_epoch, 1, pulse_link::now())
            .await
            .is_err()
    );
    hub.revoke(&cred.device).await.unwrap();
    assert!(
        hub.conversation_ready(&cred.device, current, 1, pulse_link::now())
            .await
            .is_err()
    );
    assert!(
        serde_json::from_value::<ClientWire>(
            serde_json::json!({"kind":"conversation_ready","version":1,"grant":true})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<HumanAction>(
            serde_json::json!({"kind":"conversation_ready","version":1})
        )
        .is_err()
    );
}
