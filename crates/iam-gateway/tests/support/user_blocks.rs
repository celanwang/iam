use super::removal::{
    assert_only_message, committed, denied, establish, generation, removed, send_before_change,
    upgrade_duplicate_directs, Peer,
};
use super::*;
use iam_protocol::pb::UserBlockRequest;

impl Peer {
    async fn block_change(&mut self, peer: u64, key: Id128, blocked: bool) -> DecodedBody {
        self.call(
            MsgType::UserBlock,
            &UserBlockRequest {
                peer_user_id: Some(peer),
                request_id: Some(key.to_vec().into()),
                blocked: Some(blocked),
            },
        )
        .await
    }

    async fn block_status(&mut self, peer: u64) -> bool {
        let reply = self
            .call(
                MsgType::UserBlock,
                &UserBlockRequest {
                    peer_user_id: Some(peer),
                    ..Default::default()
                },
            )
            .await;
        let DecodedBody::UserBlockAck(ack) = reply else {
            panic!("expected block status, got {reply:?}");
        };
        ack.blocked.unwrap()
    }

    async fn set_block(&mut self, peer: u64, blocked: bool) {
        let reply = self.block_change(peer, generate_id128(), blocked).await;
        changed(reply, blocked);
    }
}

fn changed(reply: DecodedBody, blocked: bool) {
    let DecodedBody::UserBlockAck(ack) = reply else {
        panic!("expected block change receipt, got {reply:?}");
    };
    assert_eq!(ack.blocked, Some(blocked));
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_blocks_both_directions_preserve_send_history_group_and_friend_receipts() {
    let fixture = empty_fixture().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let [a, b] = fixture.credentials();
    let c = groups::credential(&fixture, 1, 300, b"charlie");
    let gateway = fixture
        .start_with_credentials(
            OpenMode::Initialize,
            Limits::default(),
            "127.0.0.1:0",
            [a, b, c],
        )
        .await;
    let mut alice = Peer::open(&gateway, a.device_id, ALICE).await;
    let mut bob = Peer::open(&gateway, b.device_id, BOB).await;
    let mut charlie = Peer::open(&gateway, c.device_id, b"charlie").await;
    let (accepted_attempt, direct, friendship) = establish(&mut alice, &mut bob).await;
    let ac = generate_id128();
    alice.apply(300, ac).await;
    charlie.accept(100, ac).await;
    let group_key = generate_id128();
    let group = groups::group_id(alice.group(group_key).await);
    let key = message_key(direct);
    let receipt = alice.send(&key, "before the block").await;
    committed(&receipt, 1);
    let context = bob.push_context(direct).await;

    bob.set_block(100, true).await;
    assert!(bob.block_status(100).await);
    assert!(!alice.block_status(200).await);
    assert_eq!(alice.send_status(&key).await, receipt);
    assert_eq!(alice.send(&key, "before the block").await, receipt);
    assert_only_message(&bob.history(&context).await, &receipt);
    assert_eq!(bob.accept(100, accepted_attempt).await, direct);
    let replay = alice.apply_reply(200, accepted_attempt).await;
    assert!(
        matches!(&replay, DecodedBody::FriendApplyAck(ack) if matches!(ack.value,
        Some(iam_protocol::pb::friend_attempt_ack::Value::ConversationId(id)) if id == direct)),
        "{replay:?}"
    );
    expect_error(
        alice.apply_reply(200, generate_id128()).await,
        ErrorCode::PermissionDenied,
    );
    let rejected_key = message_key(direct);
    let rejected = alice.send(&rejected_key, "while blocked").await;
    denied(&rejected);
    denied(&bob.send(&message_key(direct), "blocker's own send").await);
    expect_error(
        alice.group(generate_id128()).await,
        ErrorCode::PermissionDenied,
    );
    assert_eq!(groups::group_id(alice.group(group_key).await), group);

    // Read and write the already-created group across the same two blocked accounts.
    let group_receipt = bob.send(&message_key(group), "existing group").await;
    committed(&group_receipt, 1);
    let group_context = alice.push_context(group).await;
    assert_only_message(&alice.history(&group_context).await, &group_receipt);
    alice.set_block(200, true).await;
    bob.set_block(100, false).await;
    assert!(!bob.block_status(100).await);
    assert!(alice.block_status(200).await);
    denied(
        &bob.send(&message_key(direct), "one direction remains")
            .await,
    );
    expect_error(
        alice.group(generate_id128()).await,
        ErrorCode::PermissionDenied,
    );
    groups::assert_one_three_person_group(&db, group).await;

    alice.set_block(200, false).await;
    assert_eq!(alice.status(200).await.as_ref(), Some(&friendship));
    assert_eq!(alice.send_status(&rejected_key).await, rejected);
    assert_eq!(alice.send(&rejected_key, "while blocked").await, rejected);
    committed(&bob.send(&message_key(direct), "both unblocked").await, 2);
    // The first invitation is authorized, but the second invitee blocks the creator.
    charlie.set_block(100, true).await;
    expect_error(
        alice.group(generate_id128()).await,
        ErrorCode::PermissionDenied,
    );
    groups::assert_one_three_person_group(&db, group).await;
    charlie.set_block(100, false).await;
    // Only the creator issues invitations. Bob blocking fellow invitee Charlie is independent.
    bob.set_block(300, true).await;
    assert_ne!(groups::group_id(alice.group(generate_id128()).await), group);
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_blocks_leave_pending_keyed_and_legacy_friend_requests_decidable() {
    let fixture = empty_fixture().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let [a, b] = fixture.credentials();
    let c = groups::credential(&fixture, 1, 300, b"charlie");
    let gateway = fixture
        .start_with_credentials(
            OpenMode::Initialize,
            Limits::default(),
            "127.0.0.1:0",
            [a, b, c],
        )
        .await;
    let mut alice = Peer::open(&gateway, a.device_id, ALICE).await;
    let mut bob = Peer::open(&gateway, b.device_id, BOB).await;
    let mut charlie = login(&gateway, c.device_id, b"charlie").await;
    let attempt = generate_id128();
    alice.apply(200, attempt).await;
    let reverse = generate_id128();
    bob.apply(100, reverse).await;
    pending(apply(&mut charlie, 2, 200).await);
    bob.set_block(100, true).await;
    bob.set_block(300, true).await;
    expect_error(
        bob.decide_reply(100, attempt, true).await,
        ErrorCode::PermissionDenied,
    );
    let accept_legacy = FriendAcceptRequest {
        from_user_id: Some(300),
    };
    expect_error(
        bob.call(MsgType::FriendAccept, &accept_legacy).await,
        ErrorCode::PermissionDenied,
    );
    expect_error(
        bob.apply_reply(100, generate_id128()).await,
        ErrorCode::PermissionDenied,
    );
    expect_error(
        alice.apply_reply(200, generate_id128()).await,
        ErrorCode::PermissionDenied,
    );
    expect_error(
        bob.call(
            MsgType::FriendRequest,
            &FriendRequest {
                target_user_id: Some(300),
            },
        )
        .await,
        ErrorCode::PermissionDenied,
    );
    let directs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM conversations")
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(directs, 0);
    bob.set_block(100, false).await;
    bob.set_block(300, false).await;
    assert!(alice.status(200).await.is_none());
    assert!(bob.status(300).await.is_none());
    let decision = alice.decide_reply(200, reverse, false).await;
    assert!(
        matches!(&decision, DecodedBody::FriendDecideAck(ack) if matches!(
        ack.value, Some(iam_protocol::pb::friend_attempt_ack::Value::Rejected(_)))),
        "{decision:?}"
    );
    let ab = bob.accept(100, attempt).await;
    let bc = accepted(bob.call(MsgType::FriendAccept, &accept_legacy).await);
    assert_ne!(ab, bc);
    assert_eq!(bob.accept(100, attempt).await, ab);
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_blocks_recover_lost_commit_and_parallel_receipts_after_newer_unblock() {
    let mut fixture = empty_fixture().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let proxy = CommitProxy::start(&fixture.database).await;
    fixture.database = proxy.url.clone();
    let [a, b] = fixture.credentials();
    let second = groups::credential(&fixture, 1, 100, b"alice-other");
    let c = groups::credential(&fixture, 1, 300, b"charlie");
    let start = |mode| {
        fixture.start_with_credentials(mode, Limits::default(), "127.0.0.1:0", [a, b, second, c])
    };
    let gateway = start(OpenMode::Initialize).await;
    let mut alice = Peer::open(&gateway, a.device_id, ALICE).await;
    let mut bob = Peer::open(&gateway, b.device_id, BOB).await;
    let (_, direct, _) = establish(&mut alice, &mut bob).await;
    let operation = generate_id128();
    let (checkpoint, disconnect) = proxy.arm(CommitPhase::After);
    let setting = tokio::spawn(async move { alice.block_change(200, operation, true).await });
    timeout(WAIT, checkpoint).await.unwrap().unwrap().unwrap();
    let send_key = message_key(direct);
    let refused = bob.send(&send_key, "commit already visible").await;
    denied(&refused);
    assert!(!bob.block_status(100).await);
    disconnect.send(()).unwrap();
    let reply = setting.await.unwrap();
    let DecodedBody::Error(error) = reply else {
        panic!("expected unknown result, got {reply:?}");
    };
    let error = error.error.unwrap();
    assert_eq!(error.code(), ErrorCode::ResultUnavailable);
    assert_eq!(
        error.action(),
        iam_protocol::pb::RetryAction::RetrySameRequest
    );
    gateway.stop().await;

    let resumed = start(OpenMode::Resume).await;
    let disk = tempfile::tempdir().unwrap();
    let laptop = groups::open_client(&disk.path().join("other-device.db"), second).await;
    let mut socket = resumed.client();
    let token = b"alice-other";
    assert!(laptop
        .user_block_status(&mut socket, token, 200)
        .await
        .unwrap());
    assert!(laptop
        .set_user_block(&mut socket, token, 200, operation, true)
        .await
        .unwrap());
    let unblock = generate_id128();
    assert!(!laptop
        .set_user_block(&mut socket, token, 200, unblock, false)
        .await
        .unwrap());
    drop(socket);
    let mut alice = Peer::open(&resumed, a.device_id, ALICE).await;
    let mut other = Peer::open(&resumed, second.device_id, token).await;
    let (one, two) = competing_requests(
        &db,
        alice.block_change(200, operation, true),
        other.block_change(200, operation, true),
    )
    .await;
    changed(one, true);
    changed(two, true);
    assert!(!alice.block_status(200).await);
    expect_error(
        alice.block_change(300, operation, true).await,
        ErrorCode::IdempotencyConflict,
    );
    expect_error(
        alice.block_change(200, operation, false).await,
        ErrorCode::IdempotencyConflict,
    );
    assert!(!alice.block_status(300).await);
    // A first call whose target state already holds also binds its UUID permanently.
    let noop = generate_id128();
    changed(other.block_change(200, noop, false).await, false);
    let (fresh, old) = competing_requests(
        &db,
        alice.block_change(200, generate_id128(), true),
        other.block_change(200, unblock, false),
    )
    .await;
    changed(fresh, true);
    changed(old, false);
    changed(other.block_change(200, noop, false).await, false);
    assert!(alice.block_status(200).await);
    expect_error(
        other.block_change(200, noop, true).await,
        ErrorCode::IdempotencyConflict,
    );
    alice.set_block(200, false).await;
    let mut bob = Peer::open(&resumed, b.device_id, BOB).await;
    assert_eq!(bob.send_status(&send_key).await, refused);
    assert_eq!(bob.send(&send_key, "commit already visible").await, refused);
    committed(&bob.send(&message_key(direct), "after replay").await, 1);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM user_block_changes")
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(count, 5);
    resumed.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_blocks_scope_direction_and_operation_id_to_tenant_and_sender() {
    let fixture = empty_fixture().await;
    let [a, b] = fixture.credentials();
    let c = groups::credential(&fixture, 1, 300, b"charlie");
    let a2 = groups::credential(&fixture, 2, 100, b"other-alice");
    let b2 = groups::credential(&fixture, 2, 200, b"other-bob");
    let gateway = fixture
        .start_with_credentials(
            OpenMode::Initialize,
            Limits::default(),
            "127.0.0.1:0",
            [a, b, c, a2, b2],
        )
        .await;
    let mut alice = Peer::open(&gateway, a.device_id, ALICE).await;
    let mut bob = Peer::open(&gateway, b.device_id, BOB).await;
    let mut charlie = Peer::open(&gateway, c.device_id, b"charlie").await;
    let mut foreign = Peer::open(&gateway, a2.device_id, b"other-alice").await;
    let mut foreign_bob = Peer::open(&gateway, b2.device_id, b"other-bob").await;
    let (_, direct, _) = establish(&mut alice, &mut bob).await;
    gateway
        .service
        .create_direct(2, direct, 100, 200)
        .await
        .unwrap();
    let (_, same_direct, _) = establish(&mut foreign, &mut foreign_bob).await;
    assert_eq!(same_direct, direct);
    let key = generate_id128();
    changed(alice.block_change(200, key, true).await, true);
    assert!(!bob.block_status(100).await);
    assert!(!charlie.block_status(200).await);
    assert!(!foreign.block_status(200).await);
    changed(foreign.block_change(200, key, false).await, false);
    changed(charlie.block_change(200, key, false).await, false);
    changed(bob.block_change(100, key, false).await, false);
    assert!(alice.block_status(200).await);
    denied(&bob.send(&message_key(direct), "tenant one").await);
    committed(
        &foreign_bob.send(&message_key(direct), "tenant two").await,
        1,
    );
    foreign.set_block(200, true).await;
    alice.set_block(200, false).await;
    assert!(foreign.block_status(200).await);
    denied(
        &foreign_bob
            .send(&message_key(direct), "now blocked here")
            .await,
    );
    committed(
        &bob.send(&message_key(direct), "independently unblocked")
            .await,
        1,
    );
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_blocks_legacy_directs_preserve_history_removal_and_independent_revocations() {
    let fixture = empty_fixture().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    upgrade_duplicate_directs(&fixture, &db).await;
    let [a, b] = fixture.credentials();
    let old_device = groups::credential(&fixture, 1, 200, b"bob-old");
    let gateway = fixture
        .start_with_credentials(
            OpenMode::Resume,
            Limits::default(),
            "127.0.0.1:0",
            [a, b, old_device],
        )
        .await;
    let mut alice = Peer::open(&gateway, a.device_id, ALICE).await;
    let mut bob = Peer::open(&gateway, b.device_id, BOB).await;
    let gen = generation(&alice.status(200).await.unwrap());
    let mut saved = Vec::new();
    for id in [42, 43] {
        let key = message_key(id);
        let receipt = alice.send(&key, "before blocking").await;
        committed(&receipt, 1);
        saved.push((key, receipt, bob.push_context(id).await));
    }
    // Bob is the sole active member of 43 at the instant Alice blocks him.
    gateway
        .service
        .set_member_access(1, 43, 100, false)
        .await
        .unwrap();
    alice.set_block(200, true).await;
    for (key, receipt, context) in &saved {
        let id = key.conversation_id.unwrap();
        assert_eq!(&alice.send_status(key).await, receipt);
        assert_only_message(&bob.history(context).await, receipt);
        denied(&bob.send(&message_key(id), "blocked original pair").await);
    }
    let mut old_socket = gateway.client();
    old_socket.reconnect().await.unwrap();
    let auth_response::Value::Session(old_session) =
        auth(&mut old_socket, 1, old_device.device_id, b"bob-old").await
    else {
        panic!("expected initial session");
    };
    gateway.gateway.revoke_device(1, 200, old_device.device_id);
    expect_close(&mut old_socket, ErrorCode::Unauthenticated).await;
    alice.set_block(200, false).await;
    old_socket.reconnect().await.unwrap();
    let reply = request(
        &mut old_socket,
        1,
        MsgType::Reconnect,
        &resume(&old_session, old_device.device_id),
    )
    .await;
    assert!(
        matches!(&reply, DecodedBody::ReconnectAck(ack) if matches!(&ack.value,
        Some(reconnect_response::Value::Error(info)) if info.code() == ErrorCode::Unauthenticated)),
        "{reply:?}"
    );
    assert!(
        matches!(auth(&mut old_socket, 2, old_device.device_id, b"bob-old").await,
        auth_response::Value::Error(info) if info.code() == ErrorCode::Unauthenticated)
    );
    committed(
        &bob.send(&message_key(43), "authorized legacy sender").await,
        2,
    );
    denied(
        &alice
            .send(&message_key(43), "member access remains revoked")
            .await,
    );
    committed(&alice.send(&message_key(42), "canonical").await, 2);

    bob.set_block(100, true).await;
    removed(alice.remove(200, gen).await, 42);
    bob.set_block(100, false).await;
    assert!(alice.status(200).await.is_none());
    for id in [42, 43] {
        denied(&bob.send(&message_key(id), "friend removal remains").await);
    }
    let (_, canonical, renewed) = establish(&mut alice, &mut bob).await;
    assert_eq!(canonical, 42);
    assert_ne!(generation(&renewed), gen);
    committed(&bob.send(&message_key(42), "new generation").await, 3);
    denied(&alice.send(&message_key(43), "still revoked").await);
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_blocks_serialize_with_inflight_send_and_protect_later_trusted_direct() {
    let fixture = empty_fixture().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let [a, b] = fixture.credentials();
    let c = groups::credential(&fixture, 1, 300, b"charlie");
    let gateway = fixture
        .start_with_credentials(
            OpenMode::Initialize,
            Limits::default(),
            "127.0.0.1:0",
            [a, b, c],
        )
        .await;
    let mut alice = Peer::open(&gateway, a.device_id, ALICE).await;
    let mut bob = Peer::open(&gateway, b.device_id, BOB).await;
    let (_, direct, _) = establish(&mut alice, &mut bob).await;
    let prior = message_key(direct);
    let (receipt, ()) = send_before_change(
        &db,
        direct,
        alice.send(&prior, "before the block commit"),
        bob.set_block(100, true),
    )
    .await;
    committed(&receipt, 1);
    assert_eq!(alice.send_status(&prior).await, receipt);
    denied(&alice.send(&message_key(direct), "after the commit").await);

    // Trusted provisioning may create a Direct after the directional block already exists.
    alice.set_block(300, true).await;
    let later = if direct == 77 { 78 } else { 77 };
    gateway
        .service
        .create_direct(1, later, 100, 300)
        .await
        .unwrap();
    let mut charlie = Peer::open(&gateway, c.device_id, b"charlie").await;
    denied(&alice.send(&message_key(later), "new trusted direct").await);
    denied(
        &charlie
            .send(&message_key(later), "other original participant")
            .await,
    );
    alice.set_block(300, false).await;
    committed(&charlie.send(&message_key(later), "now writable").await, 1);
    denied(
        &alice
            .send(&message_key(direct), "separate pair still blocked")
            .await,
    );
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_blocks_uncommitted_unblock_can_be_retried_after_peer_leaves_directory() {
    let mut fixture = empty_fixture().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let proxy = CommitProxy::start(&fixture.database).await;
    fixture.database = proxy.url.clone();
    let [a, _] = fixture.credentials();
    let gateway = fixture.start(OpenMode::Initialize, Limits::default()).await;
    let mut alice = Peer::open(&gateway, a.device_id, ALICE).await;
    let block = generate_id128();
    changed(alice.block_change(200, block, true).await, true);
    let unblock = generate_id128();
    let (checkpoint, disconnect) = proxy.arm(CommitPhase::Before);
    let clearing = tokio::spawn(async move { alice.block_change(200, unblock, false).await });
    timeout(WAIT, checkpoint).await.unwrap().unwrap().unwrap();
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM user_blocks), (SELECT COUNT(*) FROM user_block_changes)",
    )
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(counts, (1, 1));
    disconnect.send(()).unwrap();
    expect_error(clearing.await.unwrap(), ErrorCode::ResultUnavailable);
    gateway.stop().await;

    let resumed = fixture
        .start_with_credentials(OpenMode::Resume, Limits::default(), "127.0.0.1:0", [a])
        .await;
    let mut alice = Peer::open(&resumed, a.device_id, ALICE).await;
    assert!(alice.block_status(200).await);
    changed(alice.block_change(200, unblock, false).await, false);
    changed(alice.block_change(200, block, true).await, true);
    assert!(!alice.block_status(200).await);
    changed(alice.block_change(200, unblock, false).await, false);
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM user_blocks), (SELECT COUNT(*) FROM user_block_changes)",
    )
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(counts, (0, 2));
    resumed.stop().await;
}
