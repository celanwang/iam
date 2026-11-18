use super::*;
use FriendAttempt::{Accepted, Pending, Rejected};

fn owner(tenant_id: u64, user_id: u64) -> Identity {
    Identity {
        tenant_id,
        user_id,
        device_id: generate_id128(),
    }
}

fn allow(_: bool) -> Result<()> {
    Ok(())
}

async fn fresh() -> (url::Url, TidbStore) {
    let url = support::isolated_database().await;
    let store = TidbStore::open(url.as_str(), generate_id128(), Some(&MemoryStore::new(0)))
        .await
        .unwrap();
    (url, store)
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_friend_decisions_old_keys_cannot_open_or_decide_a_new_attempt() {
    let (url, store) = fresh().await;
    let alice = owner(1, 10);
    let bob = owner(1, 20);
    let first = generate_id128();
    let conflict = generate_id128();
    let second = generate_id128();
    assert_eq!(
        store
            .apply_friend(alice, 20, Some(first), allow)
            .await
            .unwrap(),
        Pending
    );
    assert!(matches!(
        store.apply_friend(alice, 30, Some(first), allow).await,
        Err(Error::FriendKeyConflict)
    ));
    assert!(matches!(
        store.apply_friend(alice, 20, Some(conflict), allow).await,
        Err(Error::FriendInProgress)
    ));
    assert!(matches!(
        store.accept_friend(bob, 10, allow).await,
        Err(Error::FriendNotFound)
    ));
    assert_eq!(
        store
            .decide_friend(bob, 10, Some(first), false, allow)
            .await
            .unwrap(),
        Rejected
    );
    assert!(matches!(
        store.apply_friend(alice, 20, Some(conflict), allow).await,
        Err(Error::FriendInProgress)
    ));
    assert_eq!(
        store
            .apply_friend(alice, 20, Some(second), allow)
            .await
            .unwrap(),
        Pending
    );

    let store = TidbStore::open(url.as_str(), store.id, None).await.unwrap();
    assert_eq!(
        store
            .apply_friend(alice, 20, Some(first), allow)
            .await
            .unwrap(),
        Rejected
    );
    assert!(matches!(
        store.apply_friend(alice, 20, Some(conflict), allow).await,
        Err(Error::FriendInProgress)
    ));
    assert_eq!(
        store
            .decide_friend(bob, 10, Some(first), true, allow)
            .await
            .unwrap(),
        Rejected
    );
    for other in [owner(1, 30), owner(2, 20)] {
        assert!(matches!(
            store
                .decide_friend(other, 10, Some(second), true, allow)
                .await,
            Err(Error::FriendNotFound)
        ));
    }
    assert_eq!(store.stream(bob, Scope::Account).await.unwrap().upper, 0);
    let page = store.friend_inbox(bob, 0, false).await.unwrap();
    let [request] = page.requests.as_slice() else {
        panic!("expected one pending request");
    };
    assert_eq!(request.from_user_id, Some(alice.user_id));
    assert_eq!(request.request_id.as_deref(), Some(second.as_slice()));

    // Two devices race opposing decisions; both read back the winner.
    let other_device = owner(bob.tenant_id, bob.user_id);
    let (accepted, rejected) = tokio::join!(
        store.decide_friend(bob, 10, Some(second), true, allow),
        store.decide_friend(other_device, 10, Some(second), false, allow),
    );
    let winner = accepted.unwrap();
    assert_eq!(winner, rejected.unwrap());
    assert!(matches!(winner, Accepted(_) | Rejected));
    assert!(store
        .friend_inbox(bob, 0, false)
        .await
        .unwrap()
        .requests
        .is_empty());
    assert_eq!(
        store.stream(bob, Scope::Account).await.unwrap().upper,
        u64::from(matches!(winner, Accepted(_)))
    );
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_friend_decisions_crossed_attempts_converge_without_rewriting_a_rejection() {
    let (_, store) = fresh().await;
    let alice = owner(1, 10);
    let bob = owner(1, 20);
    let first = generate_id128();
    let ab = generate_id128();
    let ba = generate_id128();
    store
        .apply_friend(alice, 20, Some(first), allow)
        .await
        .unwrap();
    store
        .decide_friend(bob, 10, Some(first), false, allow)
        .await
        .unwrap();
    store
        .apply_friend(alice, 20, Some(ab), allow)
        .await
        .unwrap();
    store.apply_friend(bob, 10, Some(ba), allow).await.unwrap();
    let (left, right) = tokio::join!(
        store.decide_friend(alice, 20, Some(ba), true, allow),
        store.decide_friend(bob, 10, Some(ab), true, allow),
    );
    let Accepted(conversation) = left.unwrap() else {
        panic!("expected a direct conversation");
    };
    assert_eq!(right.unwrap(), Accepted(conversation));
    assert_eq!(
        store
            .decide_friend(bob, 10, Some(first), true, allow)
            .await
            .unwrap(),
        Rejected
    );
    let new_key = generate_id128();
    assert_eq!(
        store
            .apply_friend(alice, 20, Some(new_key), allow)
            .await
            .unwrap(),
        Accepted(conversation)
    );
    assert!(matches!(
        store.apply_friend(alice, 30, Some(new_key), allow).await,
        Err(Error::FriendKeyConflict)
    ));
    for user in [alice, bob] {
        assert_eq!(store.stream(user, Scope::Account).await.unwrap().upper, 1);
    }
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_friend_decisions_upgrade_legacy_pending_and_accepted() {
    let (url, store) = fresh().await;
    let id = store.id;
    let alice = owner(1, 10);
    let bob = owner(1, 20);
    store.create_direct(1, 55, 10, 20).await.unwrap();
    sqlx::query("UPDATE direct_pairs SET friends = TRUE WHERE conversation_id = 55")
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO friend_requests VALUES (1, 20, 10, FALSE), (1, 30, 10, TRUE)")
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("DROP TABLE friend_attempts")
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE store_control SET friends_ready = 1 WHERE singleton = 1")
        .execute(&store.pool)
        .await
        .unwrap();
    drop(store);

    assert!(TidbStore::open(url.as_str(), id, None).await.is_err());
    TidbStore::upgrade(url.as_str(), id).await.unwrap();
    let store = TidbStore::open(url.as_str(), id, None).await.unwrap();
    assert_eq!(
        store.request_friend(alice, 20, allow).await.unwrap(),
        Some(55)
    );
    assert_eq!(store.accept_friend(bob, 10, allow).await.unwrap(), 55);
    assert_eq!(store.stream(bob, Scope::Account).await.unwrap().upper, 1);

    let carol = owner(1, 30);
    assert_eq!(
        store
            .incoming_friends(carol, 0)
            .await
            .unwrap()
            .from_user_ids,
        [10]
    );
    let page = store.friend_inbox(carol, 0, false).await.unwrap();
    let legacy_id = validate_id128(page.requests[0].request_id.as_deref().unwrap()).unwrap();
    TidbStore::upgrade(url.as_str(), id).await.unwrap();
    assert_eq!(store.friend_inbox(carol, 0, false).await.unwrap(), page);
    assert_eq!(
        store
            .decide_friend(carol, 10, Some(legacy_id), false, allow)
            .await
            .unwrap(),
        Rejected
    );
    let next = generate_id128();
    assert_eq!(
        store
            .apply_friend(alice, 30, Some(next), allow)
            .await
            .unwrap(),
        Pending
    );
    assert!(matches!(
        store.accept_friend(carol, 10, allow).await,
        Err(Error::FriendChanged)
    ));
    assert!(matches!(
        store.request_friend(alice, 30, allow).await,
        Err(Error::FriendChanged)
    ));
    assert!(store
        .incoming_friends(carol, 0)
        .await
        .unwrap()
        .from_user_ids
        .is_empty());
    assert!(matches!(
        store
            .decide_friend(carol, 10, Some(next), true, allow)
            .await
            .unwrap(),
        Accepted(_)
    ));
    assert_eq!(
        store
            .decide_friend(carol, 10, Some(legacy_id), true, allow)
            .await
            .unwrap(),
        Rejected
    );
}
