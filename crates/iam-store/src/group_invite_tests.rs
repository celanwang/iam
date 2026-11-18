use super::*;
use iam_protocol::pb::ReadPositionRequest;
use GroupInviteOutcome::{Accepted, Pending, Rejected};

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
    assert!(!write);
    Ok(())
}

async fn fresh() -> (url::Url, TidbStore) {
    let url = support::isolated_database().await;
    let store = TidbStore::open(url.as_str(), generate_id128(), Some(&MemoryStore::new(0)))
        .await
        .unwrap();
    (url, store)
}

async fn friends(store: &TidbStore, owner: Identity, peers: impl IntoIterator<Item = u64>) {
    for peer in peers {
        store.request_friend(owner, peer, allow).await.unwrap();
        store
            .accept_friend(account(owner.tenant_id, peer), owner.user_id, allow)
            .await
            .unwrap();
    }
}

async fn small_group(store: &TidbStore, creator: Identity) -> u64 {
    friends(store, creator, [2, 3, 4]).await;
    store
        .create_group(creator, generate_id128(), &[2, 3], allow)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_group_invite_accept_replay_does_not_restore_a_revoked_membership() {
    let (url, store) = fresh().await;
    let creator = account(1, 1);
    let recipient = account(1, 4);
    let same_recipient = account(1, 4);
    let group = small_group(&store, creator).await;
    let key = SendKey {
        owner: creator,
        conversation: group,
        client_id: generate_id128(),
    };
    store
        .decide(&key, Some(&text_content("before invitation")), true)
        .await
        .unwrap();
    let invite = generate_id128();
    assert_eq!(
        store
            .invite_group_member(creator, group, 4, invite, allow)
            .await
            .unwrap(),
        Pending
    );
    let before = store.stream(recipient, Scope::Account).await.unwrap().upper;
    let (first, replay) = tokio::join!(
        store.decide_group_invite(recipient, group, invite, true, allow),
        store.decide_group_invite(same_recipient, group, invite, true, allow),
    );
    assert_eq!(
        (first.unwrap(), replay.unwrap()),
        (Accepted(group), Accepted(group))
    );
    assert_eq!(
        store.stream(recipient, Scope::Account).await.unwrap().upper,
        before + 1
    );
    let first = store
        .stream(recipient, Scope::Conversation(group))
        .await
        .unwrap();
    assert_ne!(first.access_version, 1);
    assert_eq!(
        store
            .page(recipient, group, None, 10)
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
    let mut position = ReadPositionRequest {
        conversation_id: Some(group),
        history_id: Some(first.epoch.to_vec().into()),
        access_version: Some(first.access_version),
        read_through: Some(1),
    };
    assert_eq!(
        store
            .read_position(recipient, &position, || Ok(()))
            .await
            .unwrap(),
        Some(1)
    );

    store.set_member_access(1, group, 4, false).await.unwrap();
    let store = TidbStore::open(url.as_str(), store.id, None).await.unwrap();
    assert_eq!(
        store
            .decide_group_invite(recipient, group, invite, true, readback)
            .await
            .unwrap(),
        Accepted(group)
    );
    assert!(matches!(
        store.stream(recipient, Scope::Conversation(group)).await,
        Err(Error::PermissionDenied)
    ));

    let stale = generate_id128();
    assert_eq!(
        store
            .invite_group_member(creator, group, 4, stale, allow)
            .await
            .unwrap(),
        Pending
    );
    store.set_member_access(1, group, 4, true).await.unwrap();
    store.set_member_access(1, group, 4, false).await.unwrap();
    assert!(matches!(
        store
            .decide_group_invite(recipient, group, stale, true, readback)
            .await,
        Err(Error::GroupInviteExpired)
    ));
    assert!(store
        .group_invite_inbox(recipient, 0)
        .await
        .unwrap()
        .invitations
        .is_empty());

    let new_invite = generate_id128();
    store
        .invite_group_member(creator, group, 4, new_invite, allow)
        .await
        .unwrap();
    assert_eq!(
        store
            .decide_group_invite(recipient, group, new_invite, true, allow)
            .await
            .unwrap(),
        Accepted(group)
    );
    let second = store
        .stream(recipient, Scope::Conversation(group))
        .await
        .unwrap();
    assert_eq!(first.epoch, second.epoch);
    assert!(first.access_version < second.access_version);
    position.read_through = None;
    assert!(matches!(
        store.read_position(recipient, &position, || Ok(())).await,
        Err(Error::ReadPositionContextChanged)
    ));
    position.access_version = Some(second.access_version);
    assert_eq!(
        store
            .read_position(recipient, &position, || Ok(()))
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_group_invite_checks_current_scope_and_persists_the_winning_decision() {
    let (_, store) = fresh().await;
    let creator = account(1, 1);
    let recipient = account(1, 4);
    let group = small_group(&store, creator).await;
    let invite = generate_id128();
    let busy = generate_id128();
    assert_eq!(
        store
            .invite_group_member(creator, group, 4, invite, allow)
            .await
            .unwrap(),
        Pending
    );
    let different_group = group.wrapping_add(1).max(1);
    for (conversation, target) in [(group, 2), (different_group, 4)] {
        assert!(matches!(
            store
                .invite_group_member(creator, conversation, target, invite, readback)
                .await,
            Err(Error::GroupInviteKeyConflict)
        ));
    }
    assert!(matches!(
        store
            .invite_group_member(creator, group, 4, busy, allow)
            .await,
        Err(Error::GroupInviteInProgress)
    ));
    for outsider in [account(1, 2), account(2, 1)] {
        assert!(matches!(
            store
                .invite_group_member(outsider, group, 4, generate_id128(), allow)
                .await,
            Err(Error::PermissionDenied)
        ));
    }
    for outsider in [account(1, 2), account(2, 4)] {
        assert!(matches!(
            store
                .decide_group_invite(outsider, group, invite, true, allow)
                .await,
            Err(Error::GroupInviteNotFound)
        ));
    }
    let inbox = store.group_invite_inbox(recipient, 0).await.unwrap();
    assert_eq!(inbox.invitations[0].inviter_user_id, Some(1));
    assert_eq!(
        inbox.invitations[0].request_id.as_deref(),
        Some(invite.as_slice())
    );
    store
        .set_user_block(recipient, 1, generate_id128(), true, |_, _| Ok(()))
        .await
        .unwrap();
    assert!(matches!(
        store
            .decide_group_invite(recipient, group, invite, true, allow)
            .await,
        Err(Error::PermissionDenied)
    ));
    store
        .set_user_block(recipient, 1, generate_id128(), false, |_, _| Ok(()))
        .await
        .unwrap();
    store
        .set_member_access(1, group, creator.user_id, false)
        .await
        .unwrap();
    assert!(matches!(
        store
            .decide_group_invite(recipient, group, invite, true, allow)
            .await,
        Err(Error::PermissionDenied)
    ));
    assert_eq!(store.group_invite_inbox(recipient, 0).await.unwrap(), inbox);
    store
        .set_member_access(1, group, creator.user_id, true)
        .await
        .unwrap();
    let (one, two) = tokio::join!(
        store.decide_group_invite(recipient, group, invite, false, allow),
        store.decide_group_invite(recipient, group, invite, true, allow),
    );
    let winner = one.unwrap();
    assert_eq!(winner, two.unwrap());
    assert!(matches!(winner, Accepted(_) | Rejected));
    assert_eq!(
        store
            .invite_group_member(creator, group, 4, invite, readback)
            .await
            .unwrap(),
        winner
    );
    assert!(matches!(
        store
            .invite_group_member(creator, group, 4, busy, readback)
            .await,
        Err(Error::GroupInviteInProgress)
    ));
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_group_invite_capacity_serializes_with_trusted_regrant() {
    let (_, store) = fresh().await;
    let creator = account(1, 1);
    let recipient = account(1, 33);
    friends(&store, creator, 2..=33).await;
    let initial: Vec<u64> = (2..=32).collect();
    let group = store
        .create_group(creator, generate_id128(), &initial, allow)
        .await
        .unwrap();
    let invite = generate_id128();
    assert_eq!(
        store
            .invite_group_member(creator, group, 33, invite, allow)
            .await
            .unwrap(),
        Pending
    );
    assert!(matches!(
        store
            .decide_group_invite(recipient, group, invite, true, allow)
            .await,
        Err(Error::GroupFull)
    ));
    assert_eq!(
        store
            .group_invite_inbox(recipient, 0)
            .await
            .unwrap()
            .invitations
            .len(),
        1
    );

    store.set_member_access(1, group, 32, false).await.unwrap();
    let (decision, regrant) = tokio::join!(
        store.decide_group_invite(recipient, group, invite, true, allow),
        store.set_member_access(1, group, 32, true),
    );
    store.set_member_access(1, group, 31, false).await.unwrap();
    match (decision, regrant) {
        (Ok(Accepted(id)), Err(Error::GroupFull)) => {
            assert_eq!(id, group);
            store.set_member_access(1, group, 32, true).await.unwrap();
        }
        (Err(Error::GroupFull), Ok(())) => {
            assert_eq!(
                store
                    .decide_group_invite(recipient, group, invite, true, allow)
                    .await
                    .unwrap(),
                Accepted(group)
            );
        }
        other => panic!("one operation must win the final spot, got {other:?}"),
    }
    let count: u64 = sqlx::query_scalar("SELECT CAST(COUNT(*) AS UNSIGNED) FROM members WHERE tenant_id = 1 AND conversation_id = ?")
        .bind(group).fetch_one(&store.pool).await.unwrap();
    assert_eq!(count, 32);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_group_invite_upgrade_adds_an_inbox_with_exclusive_paging() {
    let (url, store) = fresh().await;
    let creator = account(1, 1);
    let target = account(1, 4);
    let group = small_group(&store, creator).await;
    let before = store
        .stream(creator, Scope::Conversation(group))
        .await
        .unwrap();
    sqlx::query("DROP TABLE group_invites")
        .execute(&store.pool)
        .await
        .unwrap();
    let id = store.id;
    drop(store);
    assert!(TidbStore::open(url.as_str(), id, None).await.is_err());
    TidbStore::upgrade(url.as_str(), id).await.unwrap();
    TidbStore::upgrade(url.as_str(), id).await.unwrap();
    let store = TidbStore::open(url.as_str(), id, None).await.unwrap();
    let after = store
        .stream(creator, Scope::Conversation(group))
        .await
        .unwrap();
    assert_eq!(
        (before.epoch, before.access_version),
        (after.epoch, after.access_version)
    );

    let mut insert = sqlx::QueryBuilder::<MySql>::new(
        "INSERT INTO group_invites (tenant_id, inviter_id, request_id, conversation_id, recipient_id, outcome) ",
    );
    insert.push_values(1_u64..=102, |mut row, id| {
        row.push_bind(1_u64)
            .push_bind(1_u64)
            .push_bind(generate_id128().to_vec())
            .push_bind(id)
            .push_bind(target.user_id)
            .push_bind(1_u8);
    });
    insert.build().execute(&store.pool).await.unwrap();
    let first = store.group_invite_inbox(target, 0).await.unwrap();
    assert_eq!(first.invitations.len(), 100);
    assert_eq!(first.next_after_conversation_id, Some(100));
    let last = store.group_invite_inbox(target, 100).await.unwrap();
    let ids: Vec<_> = last
        .invitations
        .iter()
        .map(|invite| invite.conversation_id.unwrap())
        .collect();
    assert_eq!(ids, [101, 102]);
    assert_eq!(last.next_after_conversation_id, None);
}
