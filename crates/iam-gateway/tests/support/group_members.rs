use super::*;
use iam_protocol::pb::{
    history_response, ConversationState, GroupMember, GroupMemberRemoveRequest, GroupMembersAck,
    HistoryRequest, PageLimit, StreamContext,
};

fn member(roster: &GroupMembersAck, user: u64) -> &GroupMember {
    roster
        .members
        .iter()
        .find(|member| member.user_id == Some(user))
        .unwrap()
}

fn client_error<T: std::fmt::Debug>(result: iam_client::Result<T>, code: ErrorCode) {
    assert!(
        matches!(&result, Err(iam_client::Error::Server(error)) if error.code() == code),
        "{result:?}"
    );
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_group_members_leave_and_reauthorize_without_replaying_into_the_new_period() {
    let fixture = GroupFixture::new().await;
    let outsider = credential(&fixture.base, 1, 400, b"group-outsider");
    let foreign_owner = credential(&fixture.base, 2, 100, b"group-foreign-owner");
    let gateway = fixture
        .start(OpenMode::Initialize, &[outsider, foreign_owner])
        .await;
    fixture.establish_friendships(&gateway).await;
    let disk = tempfile::tempdir().unwrap();
    let [alice_device, bob_device] = fixture.base.credentials();
    let alice = open_client(&disk.path().join("alice.db"), alice_device).await;
    let group = alice
        .create_group(&mut gateway.client(), ALICE, generate_id128(), &MEMBERS)
        .await
        .unwrap();

    let sent = message_key(group);
    let mut socket = login(&gateway, bob_device.device_id, BOB).await;
    let DecodedBody::SendAck(ack) = request(
        &mut socket,
        2,
        MsgType::Send,
        &send_request(sent.clone(), "before leaving"),
    )
    .await
    else {
        panic!("expected SEND_ACK");
    };
    let original = ack.outcome.unwrap();
    assert!(matches!(
        original.value,
        Some(send_outcome::Value::Committed(_))
    ));
    drop(socket);

    let path = disk.path().join("bob.db");
    let mut bob = open_client(&path, bob_device).await;
    let mut observer = observe(&path).await;
    let mut syncing = ObservedTls::new(&gateway);
    sync_through(&mut bob, &mut syncing, &mut observer, BOB, group, 1).await;
    drop(syncing);
    let page = bob.list_conversations(0, 100).await.unwrap();
    let state = &page
        .items
        .iter()
        .find(|item| item.state.conversation_id == Some(group))
        .unwrap()
        .state;
    let direct = page
        .items
        .iter()
        .find(|item| item.state.kind() == ConversationKind::Direct)
        .unwrap();
    assert_eq!(bob.messages(group).await.unwrap().len(), 1);
    let roster = bob
        .group_members(&mut gateway.client(), BOB, group)
        .await
        .unwrap();
    assert_eq!(roster.owner_user_id, Some(100));
    assert_eq!(roster.history_id, state.history_id);
    assert_eq!(member(&roster, 200).access_version, state.access_version);
    assert_eq!(
        roster
            .members
            .iter()
            .map(|member| member.user_id.unwrap())
            .collect::<Vec<_>>(),
        [100, 200, 300]
    );

    client_error(
        bob.group_members(
            &mut gateway.client(),
            BOB,
            direct.state.conversation_id.unwrap(),
        )
        .await,
        ErrorCode::PermissionDenied,
    );
    client_error(
        bob.leave_group(&mut gateway.client(), BOB, &direct.state)
            .await,
        ErrorCode::PermissionDenied,
    );
    let owner_state = ConversationState {
        access_version: member(&roster, 100).access_version,
        ..state.clone()
    };
    client_error(
        alice
            .leave_group(&mut gateway.client(), ALICE, &owner_state)
            .await,
        ErrorCode::InvalidArgument,
    );
    client_error(
        bob.remove_group_member(&mut gateway.client(), BOB, &roster, member(&roster, 300))
            .await,
        ErrorCode::PermissionDenied,
    );
    for (credential, token) in [
        (outsider, &b"group-outsider"[..]),
        (foreign_owner, &b"group-foreign-owner"[..]),
    ] {
        let client = open_client(
            &disk.path().join(format!(
                "{}-{}.db",
                credential.tenant_id, credential.user_id
            )),
            credential,
        )
        .await;
        client_error(
            client
                .group_members(&mut gateway.client(), token, group)
                .await,
            ErrorCode::PermissionDenied,
        );
        client_error(
            client
                .remove_group_member(&mut gateway.client(), token, &roster, member(&roster, 200))
                .await,
            ErrorCode::PermissionDenied,
        );
    }
    client_error(
        alice
            .remove_group_member(
                &mut gateway.client(),
                ALICE,
                &roster,
                &GroupMember {
                    user_id: Some(999),
                    access_version: Some(1),
                },
            )
            .await,
        ErrorCode::NotFound,
    );
    client_error(
        alice
            .remove_group_member(
                &mut gateway.client(),
                ALICE,
                &roster,
                &GroupMember {
                    access_version: Some(u64::MAX),
                    ..*member(&roster, 200)
                },
            )
            .await,
        ErrorCode::ContextChanged,
    );
    let wrong_history = ConversationState {
        history_id: Some(generate_id128().to_vec().into()),
        ..state.clone()
    };
    client_error(
        bob.leave_group(&mut gateway.client(), BOB, &wrong_history)
            .await,
        ErrorCode::ContextChanged,
    );

    bob.leave_group(&mut gateway.client(), BOB, state)
        .await
        .unwrap();
    bob.leave_group(&mut gateway.client(), BOB, state)
        .await
        .unwrap();
    client_error(
        bob.group_members(&mut gateway.client(), BOB, group).await,
        ErrorCode::PermissionDenied,
    );
    let remaining = alice
        .group_members(&mut gateway.client(), ALICE, group)
        .await
        .unwrap();
    assert_eq!(
        remaining.members,
        [*member(&roster, 100), *member(&roster, 300)]
    );

    let mut revoked = login(&gateway, bob_device.device_id, BOB).await;
    let status = request(
        &mut revoked,
        2,
        MsgType::SendStatus,
        &SendStatusRequest {
            key: Some(sent.clone()),
        },
    )
    .await;
    assert!(matches!(status, DecodedBody::SendStatusAck(ack) if ack.outcome == Some(original)));
    assert_nonmember(&mut revoked, group).await;
    let history = request(
        &mut revoked,
        10,
        MsgType::History,
        &HistoryRequest {
            context: Some(StreamContext {
                target: Some(SyncTarget {
                    value: Some(sync_target::Value::Conversation(group)),
                }),
                epoch: state.history_id.clone(),
                access_version: state.access_version,
            }),
            limit: Some(PageLimit {
                max_items: Some(10),
                max_bytes: Some(256 * 1024),
            }),
            ..HistoryRequest::default()
        },
    )
    .await;
    assert!(matches!(history, DecodedBody::HistoryAck(ack)
        if matches!(&ack.value, Some(history_response::Value::Error(error))
            if error.code() == ErrorCode::PermissionDenied)));
    let mut online = login(&gateway, fixture.charlie.device_id, CHARLIE).await;
    let mut sender = login(&gateway, alice_device.device_id, ALICE).await;
    send(&mut sender, 2, message_key(group), "after Bob left").await;
    assert!(matches!(receive(&mut online).await, DecodedBody::Push(_)));
    assert!(timeout(Duration::from_millis(150), revoked.receive())
        .await
        .is_err());
    drop(revoked);

    let mut syncing = ObservedTls::new(&gateway);
    sync_through(&mut bob, &mut syncing, &mut observer, BOB, 0, 3).await;
    drop(syncing);
    assert!(bob.messages(group).await.unwrap().is_empty());
    assert!(bob
        .list_conversations(0, 100)
        .await
        .unwrap()
        .items
        .iter()
        .all(|item| item.state.conversation_id != Some(group)));

    gateway
        .service
        .set_member_access(1, group, 200, true)
        .await
        .unwrap();
    bob.leave_group(&mut gateway.client(), BOB, state)
        .await
        .unwrap();
    alice
        .remove_group_member(&mut gateway.client(), ALICE, &roster, member(&roster, 200))
        .await
        .unwrap();
    let renewed = bob
        .group_members(&mut gateway.client(), BOB, group)
        .await
        .unwrap();
    assert_ne!(member(&renewed, 200).access_version, state.access_version);

    let db = MySqlPool::connect(fixture.base.database.as_str())
        .await
        .unwrap();
    let texts: Vec<Vec<u8>> = sqlx::query_scalar("SELECT `text` FROM messages WHERE tenant_id = 1 AND conversation_id = ? ORDER BY `sequence`")
        .bind(group).fetch_all(&db).await.unwrap();
    assert_eq!(texts, [b"before leaving".as_slice(), b"after Bob left"]);
    let revoked_periods: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_events WHERE tenant_id = 1 AND conversation_id = ? AND user_id = 200 AND access_version IS NULL")
        .bind(group).fetch_one(&db).await.unwrap();
    assert_eq!(revoked_periods, 1);
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_group_members_retry_owner_removal_after_lost_commit_response() {
    let mut fixture = GroupFixture::new().await;
    let db = MySqlPool::connect(fixture.base.database.as_str())
        .await
        .unwrap();
    let proxy = CommitProxy::start(&fixture.base.database).await;
    fixture.base.database = proxy.url.clone();
    let gateway = fixture.start(OpenMode::Initialize, &[]).await;
    fixture.establish_friendships(&gateway).await;
    let disk = tempfile::tempdir().unwrap();
    let alice = open_client(&disk.path().join("alice.db"), fixture.base.credentials()[0]).await;
    let group = alice
        .create_group(&mut gateway.client(), ALICE, generate_id128(), &MEMBERS)
        .await
        .unwrap();
    let roster = alice
        .group_members(&mut gateway.client(), ALICE, group)
        .await
        .unwrap();
    let mut sender = login(&gateway, fixture.base.alice, ALICE).await;
    send(&mut sender, 2, message_key(group), "before removal").await;
    drop(sender);
    let charlie_path = disk.path().join("charlie.db");
    let mut charlie = open_client(&charlie_path, fixture.charlie).await;
    let mut observer = observe(&charlie_path).await;
    let mut syncing = ObservedTls::new(&gateway);
    sync_through(&mut charlie, &mut syncing, &mut observer, CHARLIE, group, 1).await;
    assert_eq!(charlie.messages(group).await.unwrap().len(), 1);
    drop(syncing);
    let target = member(&roster, 300);
    let change = GroupMemberRemoveRequest {
        conversation_id: roster.conversation_id,
        history_id: roster.history_id.clone(),
        user_id: target.user_id,
        access_version: target.access_version,
    };
    let mut owner = login(&gateway, fixture.base.alice, ALICE).await;
    let (checkpoint, disconnect) = proxy.arm(CommitPhase::After);
    let removing =
        tokio::spawn(
            async move { request(&mut owner, 2, MsgType::GroupMemberRemove, &change).await },
        );
    timeout(WAIT, checkpoint).await.unwrap().unwrap().unwrap();
    assert!(!removing.is_finished());
    let committed: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM members WHERE tenant_id = 1 AND conversation_id = ? AND user_id = 300),
                (SELECT COUNT(*) FROM account_events WHERE tenant_id = 1 AND conversation_id = ? AND user_id = 300 AND access_version IS NULL)",
    ).bind(group).bind(group).fetch_one(&db).await.unwrap();
    assert_eq!(committed, (0, 1));
    disconnect.send(()).unwrap();
    let failure = removing.await.unwrap();
    assert!(
        matches!(&failure, DecodedBody::Error(error)
        if error.error.as_ref().is_some_and(|info| info.code() == ErrorCode::ResultUnavailable
            && info.action() == iam_protocol::pb::RetryAction::RetrySameRequest)),
        "{failure:?}"
    );

    client_error(
        charlie
            .group_members(&mut gateway.client(), CHARLIE, group)
            .await,
        ErrorCode::PermissionDenied,
    );
    let mut syncing = ObservedTls::new(&gateway);
    sync_through(&mut charlie, &mut syncing, &mut observer, CHARLIE, 0, 3).await;
    assert!(charlie.messages(group).await.unwrap().is_empty());
    assert!(charlie
        .list_conversations(0, 100)
        .await
        .unwrap()
        .items
        .iter()
        .all(|item| item.state.conversation_id != Some(group)));
    drop(syncing);
    alice
        .remove_group_member(&mut gateway.client(), ALICE, &roster, target)
        .await
        .unwrap();
    gateway
        .service
        .set_member_access(1, group, 300, true)
        .await
        .unwrap();
    gateway
        .service
        .set_member_access(1, group, 100, false)
        .await
        .unwrap();
    client_error(
        alice
            .remove_group_member(&mut gateway.client(), ALICE, &roster, target)
            .await,
        ErrorCode::PermissionDenied,
    );
    let renewed = charlie
        .group_members(&mut gateway.client(), CHARLIE, group)
        .await
        .unwrap();
    assert_eq!(renewed.owner_user_id, Some(100));
    assert_ne!(member(&renewed, 300).access_version, target.access_version);
    assert!(renewed
        .members
        .iter()
        .all(|member| member.user_id != renewed.owner_user_id));
    gateway
        .service
        .set_member_access(1, group, 100, true)
        .await
        .unwrap();
    alice
        .remove_group_member(&mut gateway.client(), ALICE, &roster, target)
        .await
        .unwrap();
    let still_active = charlie
        .group_members(&mut gateway.client(), CHARLIE, group)
        .await
        .unwrap();
    assert_eq!(member(&still_active, 300), member(&renewed, 300));
    gateway.stop().await;
}
