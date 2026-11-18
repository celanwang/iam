use super::*;

fn account(tenant_id: u64, user_id: u64) -> Identity {
    Identity {
        tenant_id,
        user_id,
        device_id: generate_id128(),
    }
}

fn allow(_: bool) -> Result<()> {
    Ok(())
}

async fn befriend(store: &TidbStore, from: Identity, to: Identity) -> u64 {
    store.request_friend(from, to.user_id, allow).await.unwrap();
    store.accept_friend(to, from.user_id, allow).await.unwrap()
}

async fn fixture() -> (url::Url, TidbStore, [Identity; 3]) {
    let url = support::isolated_database().await;
    let store = TidbStore::open(url.as_str(), generate_id128(), Some(&MemoryStore::new(0)))
        .await
        .unwrap();
    let users = [account(1, 100), account(1, 200), account(1, u64::MAX)];
    for &peer in &users[1..] {
        befriend(&store, users[0], peer).await;
    }
    (url, store, users)
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_group_create_replays_the_member_set_and_discoverable_kind() {
    let (url, store, users @ [creator, bob, carol]) = fixture().await;
    let same_creator = account(creator.tenant_id, creator.user_id);
    let request_id = generate_id128();
    let peers = [bob.user_id, carol.user_id];
    let duplicate = [carol.user_id, bob.user_id, bob.user_id];
    let (first, second) = tokio::join!(
        store.create_group(creator, request_id, &peers, allow),
        store.create_group(same_creator, request_id, &duplicate, allow),
    );
    let group = first.unwrap();
    assert_eq!(second.unwrap(), group);

    let store = TidbStore::open(url.as_str(), store.id, None).await.unwrap();
    for (user, previous) in users.into_iter().zip([2, 1, 1]) {
        let stream = store.stream(user, Scope::Account).await.unwrap();
        assert_eq!(stream.upper, previous + 1);
        let mut tx = store.begin_read().await.unwrap();
        let changes = TidbStore::changes(&mut tx, user, Scope::Account, previous, stream.upper, 1)
            .await
            .unwrap();
        let Some(change::Payload::ConversationUpsert(state)) = &changes[0].payload else {
            panic!("group missing from account changes");
        };
        assert_eq!(state.conversation_id, Some(group));
        assert_eq!(state.kind(), ConversationKind::Group);
        assert_eq!(state.access_version, Some(1));
        let page = TidbStore::account_snapshot(&mut tx, user, 0, 100)
            .await
            .unwrap();
        assert_eq!(
            page.iter().find(|c| c.conversation_id == Some(group)),
            Some(state)
        );
        assert!(page.iter().any(|c| c.kind() == ConversationKind::Direct));
    }

    // A known same-tenant user with only a trusted Direct, and a cross-tenant friend, both fail.
    store.create_direct(1, 7, 100, 50).await.unwrap();
    befriend(&store, account(2, 100), account(2, 60)).await;
    for peer in [50, 60] {
        assert!(matches!(
            store
                .create_group(creator, generate_id128(), &[bob.user_id, peer], allow)
                .await,
            Err(Error::PermissionDenied)
        ));
    }
    for invalid in [
        &[bob.user_id, bob.user_id][..],
        &[0, bob.user_id],
        &[creator.user_id, bob.user_id],
        &[bob.user_id; 65],
    ] {
        assert!(matches!(
            store
                .create_group(creator, generate_id128(), invalid, allow)
                .await,
            Err(Error::InvalidRequest(_))
        ));
    }
    assert!(matches!(
        store
            .create_group(creator, request_id, &[bob.user_id, 60], allow)
            .await,
        Err(Error::GroupKeyConflict)
    ));
    assert_eq!(
        store
            .create_group(creator, request_id, &peers, |write| {
                assert!(!write);
                Ok(())
            })
            .await
            .unwrap(),
        group
    );
    let count: i64 =
        sqlx::query_scalar("SELECT CAST(COUNT(*) AS SIGNED) FROM conversations WHERE kind = 2")
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_group_revoke_uses_existing_message_and_membership_permissions() {
    let (_, store, [creator, bob, carol]) = fixture().await;
    let request_id = generate_id128();
    let peers = [bob.user_id, carol.user_id];
    let group = store
        .create_group(creator, request_id, &peers, allow)
        .await
        .unwrap();
    let send_key = |owner| SendKey {
        owner,
        conversation: group,
        client_id: generate_id128(),
    };
    let content = text_content("group message");
    let first = store
        .decide(&send_key(bob), Some(&content), true)
        .await
        .unwrap();
    assert_eq!(first.delivery.unwrap().members.len(), 3);

    store
        .set_member_access(1, group, bob.user_id, false)
        .await
        .unwrap();
    assert!(matches!(
        store.set_member_access(1, group, 300, true).await,
        Err(Error::InvalidRequest(_))
    ));
    for user in [bob, account(1, 300), account(2, carol.user_id)] {
        assert!(matches!(
            store.stream(user, Scope::Conversation(group)).await,
            Err(Error::PermissionDenied)
        ));
        let denied = store
            .decide(&send_key(user), Some(&content), true)
            .await
            .unwrap();
        assert!(matches!(
            denied.outcome.value,
            Some(send_outcome::Value::RejectedFinal(error)) if error.code() == ErrorCode::PermissionDenied
        ));
    }
    let sent = store
        .decide(&send_key(creator), Some(&content), true)
        .await
        .unwrap();
    let mut recipients: Vec<_> = sent
        .delivery
        .unwrap()
        .members
        .into_iter()
        .map(|(user, _)| user)
        .collect();
    recipients.sort_unstable();
    assert_eq!(recipients, [creator.user_id, carol.user_id]);

    // Revoking an initial member leaves the creation receipt and earlier group history intact.
    store
        .set_member_access(1, group, creator.user_id, false)
        .await
        .unwrap();
    assert_eq!(
        store
            .create_group(creator, request_id, &peers, allow)
            .await
            .unwrap(),
        group
    );
    store
        .set_member_access(1, group, bob.user_id, true)
        .await
        .unwrap();
    let restored = store.stream(bob, Scope::Conversation(group)).await.unwrap();
    assert_eq!(restored.upper, 2);
    assert_ne!(restored.access_version, 1);
    let mut tx = store.begin_read().await.unwrap();
    let page = TidbStore::account_snapshot(&mut tx, bob, 0, 100)
        .await
        .unwrap();
    let state = page
        .iter()
        .find(|c| c.conversation_id == Some(group))
        .unwrap();
    assert_eq!(state.kind(), ConversationKind::Group);
    assert_eq!(state.access_version, Some(restored.access_version));
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_group_upgrade_preserves_direct_receipts_cursors_and_friends() {
    let (url, store, [creator, bob, carol]) = fixture().await;
    let store_id = store.id;
    let mut tx = store.begin_read().await.unwrap();
    let before = TidbStore::account_snapshot(&mut tx, creator, 0, 100)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let direct = before[0].conversation_id.unwrap();
    let key = SendKey {
        owner: creator,
        conversation: direct,
        client_id: generate_id128(),
    };
    let receipt = store
        .decide(&key, Some(&text_content("before migration")), true)
        .await
        .unwrap()
        .outcome;
    let cursor = iam_core::SyncCursor {
        tenant_id: creator.tenant_id,
        user_id: creator.user_id,
        device_id: creator.device_id,
        conversation_id: 0,
        epoch: store_id,
        access_version: 0,
        covered_through: 2,
        issued_at_ms: store.now().await.unwrap(),
        is_account: true,
    };
    let token = store.tokens.sign_cursor(&cursor);

    for sql in [
        "DROP TABLE group_creations",
        "ALTER TABLE conversations DROP COLUMN kind",
        "UPDATE store_control SET friends_ready = 2 WHERE singleton = 1",
    ] {
        sqlx::query(sql).execute(&store.pool).await.unwrap();
    }
    drop(store);
    assert!(TidbStore::open(url.as_str(), store_id, None).await.is_err());
    TidbStore::upgrade(url.as_str(), store_id).await.unwrap();
    TidbStore::upgrade(url.as_str(), store_id).await.unwrap();
    let store = TidbStore::open(url.as_str(), store_id, None).await.unwrap();
    assert_eq!(
        store.decide(&key, None, true).await.unwrap().outcome,
        receipt
    );
    assert_eq!(
        store
            .tokens
            .verify_cursor(&token, store.now().await.unwrap())
            .unwrap(),
        cursor
    );
    let mut tx = store.begin_read().await.unwrap();
    assert_eq!(
        TidbStore::account_snapshot(&mut tx, creator, 0, 100)
            .await
            .unwrap(),
        before
    );
    tx.commit().await.unwrap();
    store
        .create_group(
            creator,
            generate_id128(),
            &[bob.user_id, carol.user_id],
            allow,
        )
        .await
        .unwrap();
}
