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

fn readback(write: bool) -> Result<()> {
    assert!(!write, "replay must not require write admission");
    Ok(())
}

async fn fixture() -> (url::Url, TidbStore, [Identity; 3]) {
    let url = support::isolated_database().await;
    let mut seeds = MemoryStore::new(0);
    seeds.seed_direct(1, 4, 100, 200);
    seeds.seed_direct(1, 9, 100, 200);
    let store = TidbStore::open(url.as_str(), generate_id128(), Some(&seeds))
        .await
        .unwrap();
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

fn send(owner: Identity, conversation: u64) -> SendKey {
    SendKey {
        owner,
        conversation,
        client_id: generate_id128(),
    }
}

async fn assert_denied(store: &TidbStore, key: &SendKey, content: &MessageContent) {
    let result = store.decide(key, Some(content), true).await.unwrap();
    assert!(matches!(
        result.outcome.value,
        Some(send_outcome::Value::RejectedFinal(error)) if error.code() == ErrorCode::PermissionDenied
    ));
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_friend_removal_blocks_old_directs_preserves_reads_and_reopens_with_a_new_generation()
{
    let (url, store, [alice, bob, carol]) = fixture().await;
    let (old_generation, direct) = store
        .friend_status(alice, bob.user_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(direct, 4);
    assert_eq!(
        store.friend_status(bob, alice.user_id).await.unwrap(),
        Some((old_generation, direct))
    );
    let group_key = generate_id128();
    let peers = [bob.user_id, carol.user_id];
    let group = store
        .create_group(alice, group_key, &peers, allow)
        .await
        .unwrap();
    let old_application = generate_id128();
    assert_eq!(
        store
            .apply_friend(alice, bob.user_id, Some(old_application), allow)
            .await
            .unwrap(),
        FriendAttempt::Accepted(direct)
    );
    let content = text_content("message before removal");
    let old_key = send(alice, direct);
    let receipt = store
        .decide(&old_key, Some(&content), true)
        .await
        .unwrap()
        .outcome;
    assert!(matches!(
        receipt.value,
        Some(send_outcome::Value::Committed(_))
    ));
    let before = store.stream(bob, Scope::Account).await.unwrap().upper;

    for other in [carol, account(2, alice.user_id)] {
        assert!(matches!(
            store
                .remove_friend(other, bob.user_id, old_generation, allow)
                .await,
            Err(Error::FriendNotFound)
        ));
    }
    let (first, second) = tokio::join!(
        store.remove_friend(alice, bob.user_id, old_generation, allow),
        store.remove_friend(bob, alice.user_id, old_generation, allow),
    );
    assert_eq!(first.unwrap(), direct);
    assert_eq!(second.unwrap(), direct);
    let store = TidbStore::open(url.as_str(), store.id, None).await.unwrap();
    assert_eq!(store.friend_status(bob, alice.user_id).await.unwrap(), None);
    assert_eq!(
        store.stream(bob, Scope::Account).await.unwrap().upper,
        before
    );
    assert_eq!(
        store
            .page(bob, direct, None, 10)
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
    assert_eq!(
        store.decide(&old_key, None, true).await.unwrap().outcome,
        receipt
    );
    assert_eq!(
        store
            .decide(&old_key, Some(&content), true)
            .await
            .unwrap()
            .outcome,
        receipt
    );
    assert_eq!(
        store
            .accept_friend(bob, alice.user_id, readback)
            .await
            .unwrap(),
        direct
    );
    assert_eq!(
        store
            .apply_friend(alice, bob.user_id, Some(old_application), readback)
            .await
            .unwrap(),
        FriendAttempt::Accepted(direct)
    );
    assert!(matches!(
        store.request_friend(bob, alice.user_id, allow).await,
        Err(Error::FriendChanged)
    ));
    assert_eq!(store.friend_status(alice, bob.user_id).await.unwrap(), None);

    let rejected = send(alice, direct);
    assert_denied(&store, &rejected, &content).await;
    assert_denied(&store, &send(bob, 9), &content).await;
    assert!(store
        .decide(&send(bob, group), Some(&content), true)
        .await
        .unwrap()
        .delivery
        .is_some());
    assert_eq!(
        store
            .create_group(alice, group_key, &peers, readback)
            .await
            .unwrap(),
        group
    );
    assert!(matches!(
        store
            .create_group(alice, generate_id128(), &peers, allow)
            .await,
        Err(Error::PermissionDenied)
    ));

    store
        .set_member_access(1, direct, bob.user_id, false)
        .await
        .unwrap();
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
        FriendAttempt::Accepted(direct)
    );
    let (new_generation, reopened) = store
        .friend_status(alice, bob.user_id)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(old_generation, new_generation);
    assert_eq!(reopened, direct);
    assert_eq!(
        store
            .remove_friend(bob, alice.user_id, old_generation, readback)
            .await
            .unwrap(),
        direct
    );
    assert_eq!(
        store.friend_status(bob, alice.user_id).await.unwrap(),
        Some((new_generation, direct))
    );
    assert!(matches!(
        store.stream(bob, Scope::Conversation(direct)).await,
        Err(Error::PermissionDenied)
    ));
    assert_denied(&store, &send(bob, direct), &content).await;
    assert_denied(&store, &rejected, &content).await;
    for key in [send(alice, direct), send(bob, 9)] {
        assert!(store
            .decide(&key, Some(&content), true)
            .await
            .unwrap()
            .delivery
            .is_some());
    }
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_friend_removal_upgrade_backfills_a_stable_generation_without_touching_old_state() {
    let (url, store, [alice, bob, _]) = fixture().await;
    let id = store.id;
    let before = store.stream(alice, Scope::Account).await.unwrap().upper;
    let epoch = store
        .stream(alice, Scope::Conversation(4))
        .await
        .unwrap()
        .epoch;
    let key = send(alice, 4);
    let receipt = store
        .decide(&key, Some(&text_content("original")), true)
        .await
        .unwrap()
        .outcome;
    for sql in [
        "DROP TABLE friend_removals",
        "DROP TABLE direct_send_blocks",
        "ALTER TABLE direct_pairs DROP COLUMN generation",
        "UPDATE store_control SET friends_ready = 3 WHERE singleton = 1",
    ] {
        sqlx::query(sql).execute(&store.pool).await.unwrap();
    }
    drop(store);
    assert!(TidbStore::open(url.as_str(), id, None).await.is_err());
    TidbStore::upgrade(url.as_str(), id).await.unwrap();
    let store = TidbStore::open(url.as_str(), id, None).await.unwrap();
    let status = store
        .friend_status(alice, bob.user_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.1, 4);
    TidbStore::upgrade(url.as_str(), id).await.unwrap();
    assert_eq!(
        store.friend_status(bob, alice.user_id).await.unwrap(),
        Some(status)
    );
    assert_eq!(
        store.stream(alice, Scope::Account).await.unwrap().upper,
        before
    );
    assert_eq!(
        store
            .stream(alice, Scope::Conversation(4))
            .await
            .unwrap()
            .epoch,
        epoch
    );
    assert_eq!(
        store
            .accept_friend(bob, alice.user_id, readback)
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        store.decide(&key, None, true).await.unwrap().outcome,
        receipt
    );
}
