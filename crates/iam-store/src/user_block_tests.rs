use super::*;
use iam_protocol::pb::ReadPositionRequest;

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

fn admit(_: bool, _: Option<u64>) -> Result<()> {
    Ok(())
}

async fn fresh(seeds: &MemoryStore) -> (url::Url, TidbStore) {
    let url = support::isolated_database().await;
    let store = TidbStore::open(url.as_str(), generate_id128(), Some(seeds))
        .await
        .unwrap();
    (url, store)
}

async fn friends() -> (url::Url, TidbStore, [Identity; 3]) {
    let mut seeds = MemoryStore::new(0);
    seeds.seed_direct(1, 4, 100, 200);
    seeds.seed_direct(1, 9, 100, 200);
    let (url, store) = fresh(&seeds).await;
    let users = [account(1, 100), account(1, 200), account(1, 300)];
    for &peer in &users[1..] {
        store
            .request_friend(users[0], peer.user_id, allow)
            .await
            .unwrap();
        store
            .accept_friend(peer, users[0].user_id, allow)
            .await
            .unwrap();
    }
    (url, store, users)
}

async fn change(store: &TidbStore, owner: Identity, peer: u64, blocked: bool) {
    assert_eq!(
        store
            .set_user_block(owner, peer, generate_id128(), blocked, admit)
            .await
            .unwrap(),
        blocked
    );
}

fn send_key(owner: Identity, conversation: u64) -> SendKey {
    SendKey {
        owner,
        conversation,
        client_id: generate_id128(),
    }
}

async fn send(store: &TidbStore, owner: Identity, conversation: u64, allowed: bool) {
    let result = store
        .decide(
            &send_key(owner, conversation),
            Some(&text_content("message")),
            true,
        )
        .await
        .unwrap();
    if allowed {
        assert!(matches!(
            result.outcome.value,
            Some(send_outcome::Value::Committed(_))
        ));
    } else {
        assert!(matches!(
            result.outcome.value,
            Some(send_outcome::Value::RejectedFinal(error)) if error.code() == ErrorCode::PermissionDenied
        ));
    }
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_user_blocks_before_direct_creation_and_replays_without_reversing_changes() {
    let (url, store) = fresh(&MemoryStore::new(0)).await;
    let alice = account(1, 100);
    let bob = account(1, u64::MAX);
    let same_alice = account(1, alice.user_id);
    let other_tenant = account(2, alice.user_id);
    let request = generate_id128();

    assert!(store
        .set_user_block(alice, bob.user_id, request, true, admit)
        .await
        .unwrap());
    assert!(!store.user_block_status(bob, alice.user_id).await.unwrap());
    assert!(!store
        .user_block_status(other_tenant, bob.user_id)
        .await
        .unwrap());
    assert!(store
        .set_user_block(other_tenant, bob.user_id, request, true, admit)
        .await
        .unwrap());
    store
        .create_direct(1, 4, alice.user_id, bob.user_id)
        .await
        .unwrap();
    send(&store, alice, 4, false).await;
    send(&store, bob, 4, false).await;
    for (target, state) in [(bob.user_id, false), (300, true)] {
        assert!(matches!(
            store
                .set_user_block(same_alice, target, request, state, admit)
                .await,
            Err(Error::UserBlockKeyConflict)
        ));
    }

    assert!(store
        .set_user_block(bob, alice.user_id, request, true, admit)
        .await
        .unwrap());
    change(&store, alice, bob.user_id, false).await;
    let store = TidbStore::open(url.as_str(), store.id, None).await.unwrap();
    assert!(store
        .set_user_block(same_alice, bob.user_id, request, true, |write, target| {
            assert!(!write && target.is_none());
            Ok(())
        })
        .await
        .unwrap());
    assert!(!store.user_block_status(alice, bob.user_id).await.unwrap());
    assert!(store.user_block_status(bob, alice.user_id).await.unwrap());
    send(&store, alice, 4, false).await;

    store
        .set_member_access(1, 4, bob.user_id, false)
        .await
        .unwrap();
    change(&store, bob, alice.user_id, false).await;
    send(&store, alice, 4, true).await;
    send(&store, bob, 4, false).await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_user_blocks_compose_with_friend_removal_and_keep_existing_results() {
    let (_, store, [alice, bob, carol]) = friends().await;
    let generation = store
        .friend_status(alice, bob.user_id)
        .await
        .unwrap()
        .unwrap()
        .0;
    let key = send_key(alice, 4);
    let text = text_content("old message");
    let receipt = store.decide(&key, Some(&text), true).await.unwrap().outcome;
    let group_key = generate_id128();
    let peers = [bob.user_id, carol.user_id];
    let group = store
        .create_group(alice, group_key, &peers, allow)
        .await
        .unwrap();

    change(&store, bob, alice.user_id, true).await;
    assert_eq!(
        store.decide(&key, None, true).await.unwrap().outcome,
        receipt
    );
    assert_eq!(
        store
            .accept_friend(bob, alice.user_id, allow)
            .await
            .unwrap(),
        4
    );
    assert!(matches!(
        store
            .apply_friend(alice, bob.user_id, Some(generate_id128()), allow)
            .await,
        Err(Error::PermissionDenied)
    ));
    assert!(matches!(
        store
            .create_group(alice, generate_id128(), &peers, allow)
            .await,
        Err(Error::PermissionDenied)
    ));
    assert_eq!(
        store
            .create_group(alice, group_key, &peers, allow)
            .await
            .unwrap(),
        group
    );
    assert_eq!(
        store.page(bob, 4, None, 10).await.unwrap().messages.len(),
        1
    );
    send(&store, bob, group, true).await;
    for direct in [4, 9] {
        send(&store, alice, direct, false).await;
        send(&store, bob, direct, false).await;
    }

    store
        .remove_friend(alice, bob.user_id, generation, allow)
        .await
        .unwrap();
    change(&store, bob, alice.user_id, false).await;
    send(&store, alice, 4, false).await;
    send(&store, bob, 9, false).await;
    let request = generate_id128();
    assert_eq!(
        store
            .apply_friend(alice, bob.user_id, Some(request), allow)
            .await
            .unwrap(),
        FriendAttempt::Pending
    );
    assert_eq!(
        store
            .decide_friend(bob, alice.user_id, Some(request), true, allow)
            .await
            .unwrap(),
        FriendAttempt::Accepted(4)
    );
    send(&store, alice, 4, true).await;
    send(&store, bob, 9, true).await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_user_blocks_hold_a_pending_request_until_an_explicit_decision() {
    let (_, store) = fresh(&MemoryStore::new(0)).await;
    let alice = account(1, 100);
    let bob = account(1, 200);
    let request = generate_id128();
    store
        .apply_friend(alice, bob.user_id, Some(request), allow)
        .await
        .unwrap();
    change(&store, alice, bob.user_id, true).await;
    let inbox = store.friend_inbox(bob, 0, false).await.unwrap();
    assert_eq!(
        inbox.requests[0].request_id.as_deref(),
        Some(request.as_slice())
    );
    assert!(matches!(
        store
            .decide_friend(bob, alice.user_id, Some(request), true, allow)
            .await,
        Err(Error::PermissionDenied)
    ));
    assert!(matches!(
        store.request_friend(bob, alice.user_id, allow).await,
        Err(Error::PermissionDenied)
    ));
    assert_eq!(store.friend_status(bob, alice.user_id).await.unwrap(), None);
    change(&store, alice, bob.user_id, false).await;
    assert_eq!(store.friend_inbox(bob, 0, false).await.unwrap(), inbox);
    assert!(matches!(
        store
            .decide_friend(bob, alice.user_id, Some(request), true, allow)
            .await
            .unwrap(),
        FriendAttempt::Accepted(_)
    ));
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_user_blocks_upgrade_preserves_old_pauses_generations_and_read_positions() {
    let (url, store, [alice, bob, carol]) = friends().await;
    let id = store.id;
    let removed = store
        .friend_status(alice, bob.user_id)
        .await
        .unwrap()
        .unwrap();
    let active = store.friend_status(alice, carol.user_id).await.unwrap();
    send(&store, alice, 4, true).await;
    let stream = store.stream(bob, Scope::Conversation(4)).await.unwrap();
    let mut position = ReadPositionRequest {
        conversation_id: Some(4),
        history_id: Some(stream.epoch.to_vec().into()),
        access_version: Some(stream.access_version),
        read_through: Some(1),
    };
    assert_eq!(
        store
            .read_position(bob, &position, || Ok(()))
            .await
            .unwrap(),
        Some(1)
    );
    store
        .remove_friend(alice, bob.user_id, removed.0, allow)
        .await
        .unwrap();
    for sql in [
        "DROP TABLE user_blocks",
        "DROP TABLE user_block_changes",
        "UPDATE store_control SET friends_ready = 4 WHERE singleton = 1",
    ] {
        sqlx::query(sql).execute(&store.pool).await.unwrap();
    }
    drop(store);
    assert!(TidbStore::open(url.as_str(), id, None).await.is_err());
    TidbStore::upgrade(url.as_str(), id).await.unwrap();
    TidbStore::upgrade(url.as_str(), id).await.unwrap();
    let store = TidbStore::open(url.as_str(), id, None).await.unwrap();
    assert_eq!(
        store.friend_status(alice, carol.user_id).await.unwrap(),
        active
    );
    assert_eq!(
        store
            .remove_friend(bob, alice.user_id, removed.0, allow)
            .await
            .unwrap(),
        4
    );
    assert!(!store.user_block_status(alice, bob.user_id).await.unwrap());
    position.read_through = None;
    assert_eq!(
        store
            .read_position(bob, &position, || Ok(()))
            .await
            .unwrap(),
        Some(1)
    );
    send(&store, bob, 9, false).await;
}
