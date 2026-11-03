use super::*;

async fn unprojected(f: &Fixture) -> OperationRequest {
    f.hub
        .model_proposal(&f.run.token, &reply(f.action(), "press"), crate::now())
        .await
        .unwrap();
    let data: String = sqlx::query_scalar("SELECT data FROM operation_requests")
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    serde_json::from_str(&data).unwrap()
}
async fn count_card(f: &Fixture, id: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM link_chat WHERE id=?")
        .bind(id)
        .fetch_one(&f.hub.db)
        .await
        .unwrap()
}
#[tokio::test]
async fn projection_publication_uses_original_revision_and_reconciles_a_keeper_only_withdrawal() {
    for revised in [false, true] {
        let f = Fixture::setup_with_memory("action-good", true).await;
        let memory = f
            .hub
            .core
            .list_memories(crate::now())
            .await
            .unwrap()
            .remove(0);
        let request = unprojected(&f).await;
        if revised {
            let event=f.hub.core.ingest(Origin::Human,"fixture","revise",json!({"commands":[{"kind":"revise","id":"task","expected_revision":"1","goal":"New independent goal"}]}),crate::now()).await.unwrap();
            f.hub.core.route(&event.event_id).await.unwrap();
            assert!(
                f.hub
                    .core
                    .lease_task_memory("task", crate::now())
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        sqlx::query("CREATE TRIGGER block_card_redaction BEFORE DELETE ON memories BEGIN SELECT RAISE(ABORT,'controlled source failure'); END")
            .execute(&f.hub.db).await.unwrap();
        let now = crate::now();
        assert!(
            f.hub
                .core
                .change_memory(
                    Origin::Human,
                    &pulse_core::MemoryRequest {
                        source_id: uuid::Uuid::new_v4().to_string(),
                        source_at: now,
                        command: pulse_core::MemoryCommand::Forget {
                            id: memory.id,
                            revision: memory.revision
                        },
                    },
                    now
                )
                .await
                .is_err()
        );
        assert!(f.hub.project_action_approvals(crate::now()).await.is_err());
        assert_eq!(count_card(&f, &request.proposal.id).await, 0);
        sqlx::query("DROP TRIGGER block_card_redaction")
            .execute(&f.hub.db)
            .await
            .unwrap();
        f.hub.project_action_approvals(crate::now()).await.unwrap();
        assert_eq!(count_card(&f, &request.proposal.id).await, 0);
        assert_eq!(f.effects(), 0);
        if revised {
            assert_eq!(
                f.hub.core.task("task").await.unwrap().goal,
                "New independent goal"
            );
        }
    }
}
#[tokio::test]
async fn busy_card_publication_defers_and_message_lineage_commit_is_atomic() {
    let f = Fixture::setup_with_memory("action-good", true).await;
    let request = unprojected(&f).await;
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(f._root.path().join("server.memory-lock"))
        .unwrap();
    lock.lock().unwrap();
    f.hub.project_action_approvals(crate::now()).await.unwrap();
    assert_eq!(count_card(&f, &request.proposal.id).await, 0);
    lock.unlock().unwrap();
    sqlx::query("CREATE TRIGGER block_card_lineage BEFORE INSERT ON memory_derived_messages BEGIN SELECT RAISE(ABORT,'controlled provenance failure'); END")
        .execute(&f.hub.db).await.unwrap();
    assert!(f.hub.project_action_approvals(crate::now()).await.is_err());
    assert_eq!(count_card(&f, &request.proposal.id).await, 0);
    sqlx::query("DROP TRIGGER block_card_lineage")
        .execute(&f.hub.db)
        .await
        .unwrap();
    f.hub.project_action_approvals(crate::now()).await.unwrap();
    f.hub.project_action_approvals(crate::now()).await.unwrap();
    assert_eq!(count_card(&f, &request.proposal.id).await, 1);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM memory_derived_messages WHERE id=?")
        .bind(&request.proposal.id)
        .fetch_one(&f.hub.db)
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(f.effects(), 0);
}

#[tokio::test]
async fn legacy_card_without_lineage_is_still_source_gated_in_snapshots() {
    let f = Fixture::setup_with_memory("action-good", true).await;
    let request = f.propose().await;
    sqlx::query("DELETE FROM memory_derived_messages WHERE id=?")
        .bind(&request.proposal.id)
        .execute(&f.hub.db)
        .await
        .unwrap();
    f.hub
        .projection_erasure_ready(&f.chat.device, f.chat_epoch, crate::now())
        .await
        .unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(f._root.path().join("server.memory-lock"))
        .unwrap();
    lock.lock().unwrap();
    f.hub
        .snapshot(&f.chat.device, f.chat_epoch, crate::now())
        .await
        .unwrap();
    let data = serde_json::to_string(&f.hub.outgoing(&f.chat.device).await.unwrap()).unwrap();
    assert!(!data.contains(&request.proposal.id));
    lock.unlock().unwrap();
    f.hub
        .snapshot(&f.chat.device, f.chat_epoch, crate::now())
        .await
        .unwrap();
    let frames = f.hub.outgoing(&f.chat.device).await.unwrap();
    assert!(frames.iter().any(|f|matches!(&f.payload,ServerPayload::DerivedSnapshot{messages,sources,..}
        if !sources.is_empty() && messages.iter().any(|m|m.id==request.proposal.id && m.approval.is_some()))));
    assert_eq!(f.effects(), 0);
}
