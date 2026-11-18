use super::*;
use iam_protocol::pb::{
    friend_attempt_ack, friend_status_ack, history_response, FriendApplyRequest,
    FriendDecideRequest, FriendRelationship, FriendRemoveRequest, FriendStatusRequest,
    GroupCreateRequest, HistoryRequest, PageLimit, StreamContext,
};

// One authenticated TLS connection per device, including for fault-injected requests.
pub(super) struct Peer {
    tls: TlsTransport,
    next_id: u64,
}

impl Peer {
    pub(super) async fn open(gateway: &RunningGateway, device: Id128, token: &[u8]) -> Self {
        Self {
            tls: login(gateway, device, token).await,
            next_id: 2,
        }
    }

    pub(super) async fn call(&mut self, kind: MsgType, body: &impl Message) -> DecodedBody {
        let id = self.next_id;
        self.next_id += 1;
        request(&mut self.tls, id, kind, body).await
    }

    pub(super) async fn apply_reply(&mut self, target: u64, key: Id128) -> DecodedBody {
        self.call(
            MsgType::FriendApply,
            &FriendApplyRequest {
                target_user_id: Some(target),
                request_id: Some(key.to_vec().into()),
            },
        )
        .await
    }

    pub(super) async fn apply(&mut self, target: u64, key: Id128) {
        let reply = self.apply_reply(target, key).await;
        assert!(
            matches!(&reply, DecodedBody::FriendApplyAck(ack) if matches!(&ack.value,
            Some(friend_attempt_ack::Value::Pending(_)))),
            "{reply:?}"
        );
    }

    pub(super) async fn decide_reply(
        &mut self,
        from: u64,
        key: Id128,
        accept: bool,
    ) -> DecodedBody {
        self.call(
            MsgType::FriendDecide,
            &FriendDecideRequest {
                from_user_id: Some(from),
                request_id: Some(key.to_vec().into()),
                accept: Some(accept),
            },
        )
        .await
    }

    pub(super) async fn accept(&mut self, from: u64, key: Id128) -> u64 {
        let reply = self.decide_reply(from, key, true).await;
        let DecodedBody::FriendDecideAck(ack) = reply else {
            panic!("expected accepted, got {reply:?}");
        };
        let Some(friend_attempt_ack::Value::ConversationId(id)) = ack.value else {
            panic!("unexpected decision {ack:?}");
        };
        id
    }

    pub(super) async fn status(&mut self, peer: u64) -> Option<FriendRelationship> {
        let reply = self
            .call(
                MsgType::FriendStatus,
                &FriendStatusRequest {
                    peer_user_id: Some(peer),
                },
            )
            .await;
        let DecodedBody::FriendStatusAck(ack) = reply else {
            panic!("expected status, got {reply:?}");
        };
        match ack.value.unwrap() {
            friend_status_ack::Value::NotFriends(_) => None,
            friend_status_ack::Value::Relationship(relationship) => Some(relationship),
        }
    }

    pub(super) async fn remove(&mut self, peer: u64, generation: Id128) -> DecodedBody {
        self.call(
            MsgType::FriendRemove,
            &FriendRemoveRequest {
                peer_user_id: Some(peer),
                generation: Some(generation.to_vec().into()),
            },
        )
        .await
    }

    pub(super) async fn group(&mut self, key: Id128) -> DecodedBody {
        self.call(
            MsgType::GroupCreate,
            &GroupCreateRequest {
                request_id: Some(key.to_vec().into()),
                member_user_ids: vec![200, 300],
            },
        )
        .await
    }

    pub(super) async fn send(&mut self, key: &MessageKey, text: &str) -> send_outcome::Value {
        let reply = self
            .call(MsgType::Send, &send_request(key.clone(), text))
            .await;
        let DecodedBody::SendAck(ack) = reply else {
            panic!("expected SEND_ACK: {reply:?}");
        };
        assert_eq!(ack.key.as_ref(), Some(key));
        ack.outcome.unwrap().value.unwrap()
    }

    pub(super) async fn send_status(&mut self, key: &MessageKey) -> send_outcome::Value {
        let reply = self
            .call(
                MsgType::SendStatus,
                &SendStatusRequest {
                    key: Some(key.clone()),
                },
            )
            .await;
        let DecodedBody::SendStatusAck(ack) = reply else {
            panic!("expected SEND_STATUS_ACK: {reply:?}");
        };
        assert_eq!(ack.key.as_ref(), Some(key));
        ack.outcome.unwrap().value.unwrap()
    }

    pub(super) async fn push_context(&mut self, id: u64) -> StreamContext {
        let reply = receive(&mut self.tls).await;
        let DecodedBody::Push(push) = reply else {
            panic!("expected PUSH: {reply:?}");
        };
        let context = push.context.unwrap();
        assert_eq!(
            context.target.as_ref().unwrap().value,
            Some(sync_target::Value::Conversation(id))
        );
        context
    }

    pub(super) async fn history(
        &mut self,
        context: &StreamContext,
    ) -> Vec<iam_protocol::pb::MessageView> {
        let reply = self
            .call(
                MsgType::History,
                &HistoryRequest {
                    context: Some(context.clone()),
                    before_create_seq: None,
                    page_token: None,
                    limit: Some(PageLimit {
                        max_items: Some(10),
                        max_bytes: Some(256 * 1024),
                    }),
                },
            )
            .await;
        let DecodedBody::HistoryAck(ack) = reply else {
            panic!("expected HISTORY_ACK: {reply:?}");
        };
        let Some(history_response::Value::Page(page)) = ack.value else {
            panic!("history not accessible: {ack:?}");
        };
        assert_eq!(page.context.as_ref(), Some(context));
        assert!(page.next_page_token.is_none());
        page.messages
    }
}

pub(super) fn generation(status: &FriendRelationship) -> Id128 {
    iam_protocol::parse_id128_bytes(status.generation.as_deref().unwrap()).unwrap()
}

pub(super) fn removed(reply: DecodedBody, expected: u64) {
    assert!(
        matches!(&reply, DecodedBody::FriendRemoveAck(ack) if ack.conversation_id == Some(expected)),
        "{reply:?}"
    );
}

pub(super) fn committed(outcome: &send_outcome::Value, sequence: u64) {
    assert!(
        matches!(outcome, send_outcome::Value::Committed(receipt) if receipt.position.as_ref().unwrap().create_seq == Some(sequence)),
        "{outcome:?}"
    );
}

pub(super) fn denied(outcome: &send_outcome::Value) {
    assert!(
        matches!(outcome, send_outcome::Value::RejectedFinal(info) if info.code() == ErrorCode::PermissionDenied),
        "{outcome:?}"
    );
}

pub(super) fn assert_only_message(
    history: &[iam_protocol::pb::MessageView],
    outcome: &send_outcome::Value,
) {
    let [view] = history else {
        panic!("expected one historical message, found {}", history.len());
    };
    let send_outcome::Value::Committed(receipt) = outcome else {
        panic!("expected committed send: {outcome:?}");
    };
    assert_eq!(view.r#ref, receipt.r#ref);
    assert_eq!(view.position, receipt.position);
}

pub(super) async fn establish(
    alice: &mut Peer,
    bob: &mut Peer,
) -> (Id128, u64, FriendRelationship) {
    let attempt = generate_id128();
    alice.apply(200, attempt).await;
    let direct = bob.accept(100, attempt).await;
    let status = bob.status(100).await.unwrap();
    assert_eq!(status.conversation_id, Some(direct));
    assert_eq!(alice.status(200).await.as_ref(), Some(&status));
    (attempt, direct, status)
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_removal_recovers_lost_commit_reply_and_preserves_generation_and_send_receipts() {
    let mut fixture = empty_fixture().await;
    let pool = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
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
    let (attempt, direct, initial) = establish(&mut alice, &mut bob).await;
    let old_generation = generation(&initial);
    let other_attempt = generate_id128();
    alice.apply(300, other_attempt).await;
    let mut charlie = Peer::open(&gateway, c.device_id, b"charlie").await;
    charlie.accept(100, other_attempt).await;
    let original_group_key = generate_id128();
    let DecodedBody::GroupCreateAck(ack) = alice.group(original_group_key).await else {
        panic!("group creation failed");
    };
    let group = ack.conversation_id.unwrap();
    let old_key = message_key(direct);
    let success = alice.send(&old_key, "before removal").await;
    committed(&success, 1);
    let context = bob.push_context(direct).await;

    let (checkpoint, disconnect) = proxy.arm(CommitPhase::After);
    let removing = tokio::spawn(async move { alice.remove(200, old_generation).await });
    timeout(WAIT, checkpoint).await.unwrap().unwrap().unwrap();
    // Another participant sees the committed removal while the original caller still awaits its ACK.
    assert!(bob.status(100).await.is_none());
    let failed_key = message_key(direct);
    let rejected = bob.send(&failed_key, "after removal").await;
    denied(&rejected);
    disconnect.send(()).unwrap();
    let reply = removing.await.unwrap();
    let DecodedBody::Error(error) = reply else {
        panic!("expected uncertain removal: {reply:?}");
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
    let laptop = groups::open_client(&disk.path().join("alice-other.db"), second).await;
    let mut transport = resumed.client();
    assert!(laptop
        .friend_status(&mut transport, b"alice-other", 200)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        laptop
            .remove_friend(&mut transport, b"alice-other", 200, old_generation)
            .await
            .unwrap(),
        direct
    );
    let mut alice = Peer::open(&resumed, a.device_id, ALICE).await;
    let mut bob = Peer::open(&resumed, b.device_id, BOB).await;
    removed(bob.remove(100, old_generation).await, direct);
    assert_eq!(alice.send_status(&old_key).await, success);
    assert_eq!(alice.send(&old_key, "before removal").await, success);
    assert_eq!(bob.send_status(&failed_key).await, rejected);
    assert_eq!(bob.accept(100, attempt).await, direct);
    assert!(alice.status(200).await.is_none());
    denied(&alice.send(&message_key(direct), "late accept").await);
    expect_error(
        alice.group(generate_id128()).await,
        ErrorCode::PermissionDenied,
    );
    let replay = alice.group(original_group_key).await;
    assert!(
        matches!(replay, DecodedBody::GroupCreateAck(ack) if ack.conversation_id == Some(group))
    );
    assert_only_message(&bob.history(&context).await, &success);
    committed(&bob.send(&message_key(group), "existing group").await, 1);
    let group_context = alice.push_context(group).await;
    assert_eq!(alice.history(&group_context).await.len(), 1);

    let renewal = generate_id128();
    bob.apply(100, renewal).await;
    assert_eq!(bob.accept(100, attempt).await, direct);
    assert!(alice.status(200).await.is_none());
    denied(&bob.send(&message_key(direct), "only an application").await);
    assert_eq!(alice.accept(200, renewal).await, direct);
    let renewed = alice.status(200).await.unwrap();
    assert_eq!(renewed.conversation_id, Some(direct));
    assert_ne!(generation(&renewed), old_generation);
    assert_eq!(
        laptop
            .friend_status(&mut transport, b"alice-other", 200)
            .await
            .unwrap()
            .as_ref(),
        Some(&renewed)
    );
    removed(alice.remove(200, old_generation).await, direct);
    assert_eq!(bob.accept(100, attempt).await, direct);
    assert_eq!(bob.status(100).await.as_ref(), Some(&renewed));
    assert_eq!(bob.send(&failed_key, "after removal").await, rejected);
    assert_eq!(bob.send_status(&failed_key).await, rejected);
    committed(&bob.send(&message_key(direct), "new generation").await, 2);
    removed(bob.remove(100, generation(&renewed)).await, direct);
    assert!(alice.status(200).await.is_none());
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM conversations), (SELECT COUNT(*) FROM members), (SELECT COUNT(*) FROM account_events)",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(
        counts,
        (3, 7, 7),
        "removing and restoring friendship preserves read membership"
    );
    let receipts: Vec<(Vec<u8>, u64)> = sqlx::query_as(
        "SELECT generation, conversation_id FROM friend_removals WHERE tenant_id = 1 AND user_low = 100 AND user_high = 200"
    ).fetch_all(&pool).await.unwrap();
    assert_eq!(receipts.len(), 2);
    for expected in [old_generation, generation(&renewed)] {
        assert!(receipts.contains(&(expected.to_vec(), direct)));
    }
    resumed.stop().await;
}

async fn wait_for_statement(db: &MySqlPool, prefix: &str) {
    timeout(WAIT, async {
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM information_schema.processlist
                 WHERE DB = DATABASE() AND ID <> CONNECTION_ID() AND INFO LIKE ?",
            )
            .bind(prefix)
            .fetch_one(db)
            .await
            .unwrap();
            if waiting > 0 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no transaction reached {prefix}"));
}

// Start the competing change only after SEND owns store_control and has checked authorization.
pub(super) async fn send_before_change<T>(
    db: &MySqlPool,
    direct: u64,
    send: impl std::future::Future<Output = send_outcome::Value>,
    change: impl std::future::Future<Output = T>,
) -> (send_outcome::Value, T) {
    let mut blocker = db.acquire().await.unwrap();
    sqlx::query("SET SESSION tidb_txn_mode = 'pessimistic'")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let mut lock = blocker.begin().await.unwrap();
    sqlx::query("SELECT conversation_id FROM conversations WHERE tenant_id = 1 AND conversation_id = ? FOR UPDATE")
        .bind(direct).fetch_one(&mut *lock).await.unwrap();
    let change_second = async {
        wait_for_statement(db, "UPDATE conversations SET next_sequence%").await;
        let unlock = async {
            wait_for_statement(db, "SELECT store_id, token_key,%").await;
            lock.rollback().await.unwrap();
        };
        let (result, ()) = tokio::join!(change, unlock);
        result
    };
    tokio::join!(send, change_second)
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_removal_waits_for_an_earlier_send_to_commit() {
    let fixture = empty_fixture().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let gateway = fixture.start(OpenMode::Initialize, Limits::default()).await;
    let mut alice = Peer::open(&gateway, fixture.alice, ALICE).await;
    let mut bob = Peer::open(&gateway, fixture.bob, BOB).await;
    let (_, direct, state) = establish(&mut alice, &mut bob).await;
    let gen = generation(&state);
    let before = message_key(direct);
    let (receipt, result) = send_before_change(
        &db,
        direct,
        alice.send(&before, "owns the control row"),
        bob.remove(100, gen),
    )
    .await;
    committed(&receipt, 1);
    removed(result, direct);
    let after = message_key(direct);
    let rejection = alice.send(&after, "after the removal commit").await;
    denied(&rejection);
    assert_eq!(alice.send_status(&before).await, receipt);
    assert_eq!(alice.send_status(&after).await, rejection);
    let saved: (i64, i64) =
        sqlx::query_as("SELECT (SELECT COUNT(*) FROM messages), (SELECT COUNT(*) FROM rejections)")
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(saved, (1, 1));
    gateway.stop().await;
}

pub(super) async fn upgrade_duplicate_directs(fixture: &Fixture, db: &MySqlPool) {
    decisions::old_friend_store(fixture, db).await;
    let epoch = generate_id128();
    let mut tx = db.begin().await.unwrap();
    sqlx::query("INSERT INTO conversations VALUES (1, 43, ?, 1)")
        .bind(epoch.as_slice())
        .execute(&mut *tx)
        .await
        .unwrap();
    for user in [100u64, 200] {
        sqlx::query("INSERT INTO members VALUES (1, ?, 43, 2)")
            .bind(user)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_events VALUES (1, ?, 2, 43, ?, 1)")
            .bind(user)
            .bind(epoch.as_slice())
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    TidbService::upgrade_local(fixture.database.as_str(), fixture.store)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_removal_upgrade_blocks_all_legacy_directs_and_keeps_their_history() {
    let fixture = empty_fixture().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    upgrade_duplicate_directs(&fixture, &db).await;
    let gateway = fixture.start(OpenMode::Resume, Limits::default()).await;
    let mut alice = Peer::open(&gateway, fixture.alice, ALICE).await;
    let mut bob = Peer::open(&gateway, fixture.bob, BOB).await;
    let current = alice.status(200).await.unwrap();
    assert_eq!(current.conversation_id, Some(42));
    let mut previous = Vec::new();
    for id in [42, 43] {
        let key = message_key(id);
        let outcome = alice.send(&key, "before removal").await;
        committed(&outcome, 1);
        previous.push((key, outcome, bob.push_context(id).await));
    }
    // 43 has one active member at removal time, but both accounts occur in its original history.
    gateway
        .service
        .set_member_access(1, 43, 100, false)
        .await
        .unwrap();
    let pre_rejected = message_key(43);
    let refused = alice.send(&pre_rejected, "revoked before removal").await;
    denied(&refused);
    removed(alice.remove(200, generation(&current)).await, 42);
    gateway.stop().await;
    TidbService::upgrade_local(fixture.database.as_str(), fixture.store)
        .await
        .unwrap();
    let gateway = fixture.start(OpenMode::Resume, Limits::default()).await;
    let mut alice = Peer::open(&gateway, fixture.alice, ALICE).await;
    let mut bob = Peer::open(&gateway, fixture.bob, BOB).await;
    let legacy = bob
        .call(
            MsgType::FriendAccept,
            &FriendAcceptRequest {
                from_user_id: Some(100),
            },
        )
        .await;
    assert_eq!(accepted(legacy), 42);
    assert!(bob.status(100).await.is_none());
    gateway
        .service
        .set_member_access(1, 43, 100, true)
        .await
        .unwrap();
    assert_eq!(alice.send_status(&pre_rejected).await, refused);
    for (key, receipt, context) in &previous {
        let id = key.conversation_id.unwrap();
        assert_eq!(&alice.send_status(key).await, receipt);
        assert_only_message(&bob.history(context).await, receipt);
        denied(&alice.send(&message_key(id), "removed sender").await);
        denied(&bob.send(&message_key(id), "removed peer").await);
    }

    gateway
        .service
        .set_member_access(1, 43, 100, false)
        .await
        .unwrap();
    let (_, id, renewed) = establish(&mut alice, &mut bob).await;
    assert_eq!(id, 42);
    assert_ne!(generation(&renewed), generation(&current));
    committed(&alice.send(&message_key(42), "canonical").await, 2);
    denied(&alice.send(&message_key(43), "access still revoked").await);
    committed(
        &bob.send(&message_key(43), "authorized legacy member").await,
        2,
    );
    gateway
        .service
        .set_member_access(1, 43, 100, true)
        .await
        .unwrap();
    assert_eq!(
        alice.send(&pre_rejected, "revoked before removal").await,
        refused
    );
    committed(&alice.send(&message_key(43), "trusted regrant").await, 3);
    let ids: Vec<u64> =
        sqlx::query_scalar("SELECT conversation_id FROM conversations ORDER BY conversation_id")
            .fetch_all(&db)
            .await
            .unwrap();
    assert_eq!(ids, [42, 43]);
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_removal_scopes_relationship_and_generation_to_the_authenticated_pair() {
    let fixture = empty_fixture().await;
    let [a, b] = fixture.credentials();
    let c = groups::credential(&fixture, 1, 400, b"outsider");
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
    let mut third = Peer::open(&gateway, c.device_id, b"outsider").await;
    let mut other_alice = Peer::open(&gateway, a2.device_id, b"other-alice").await;
    let mut other_bob = Peer::open(&gateway, b2.device_id, b"other-bob").await;
    let (_, direct, local) = establish(&mut alice, &mut bob).await;
    // Deliberately reuse both account IDs and the Direct ID in tenant two.
    gateway
        .service
        .create_direct(2, direct, 100, 200)
        .await
        .unwrap();
    let (_, other_direct, foreign) = establish(&mut other_alice, &mut other_bob).await;
    assert_eq!(other_direct, direct);
    assert_ne!(generation(&local), generation(&foreign));
    let gen = generation(&local);
    assert!(third.status(100).await.is_none());
    assert!(third.status(200).await.is_none());
    expect_error(third.remove(200, gen).await, ErrorCode::NotFound);
    expect_error(other_bob.remove(100, gen).await, ErrorCode::NotFound);
    expect_error(
        alice.remove(200, generation(&foreign)).await,
        ErrorCode::NotFound,
    );
    assert_eq!(alice.status(200).await.as_ref(), Some(&local));
    removed(bob.remove(100, gen).await, direct);
    // Replaying a completed removal checks the same identity scope as its first execution.
    expect_error(third.remove(200, gen).await, ErrorCode::NotFound);
    expect_error(other_alice.remove(200, gen).await, ErrorCode::NotFound);
    assert_eq!(other_bob.status(100).await.as_ref(), Some(&foreign));
    committed(
        &other_bob
            .send(&message_key(other_direct), "other tenant still active")
            .await,
        1,
    );
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_removal_before_commit_preserves_friendship_until_explicit_retry() {
    let mut fixture = empty_fixture().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let proxy = CommitProxy::start(&fixture.database).await;
    fixture.database = proxy.url.clone();
    let gateway = fixture.start(OpenMode::Initialize, Limits::default()).await;
    let mut alice = Peer::open(&gateway, fixture.alice, ALICE).await;
    let mut bob = Peer::open(&gateway, fixture.bob, BOB).await;
    let (_, direct, initial) = establish(&mut alice, &mut bob).await;
    let key = generation(&initial);
    let (before, disconnect) = proxy.arm(CommitPhase::Before);
    let attempt = tokio::spawn(async move { bob.remove(100, key).await });
    timeout(WAIT, before).await.unwrap().unwrap().unwrap();
    let saved: (i64, i64, i64) = sqlx::query_as(
        "SELECT
        (SELECT COUNT(*) FROM direct_pairs WHERE friends = TRUE),
        (SELECT COUNT(*) FROM friend_removals),
        (SELECT COUNT(*) FROM direct_send_blocks)",
    )
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(saved, (1, 0, 0));
    disconnect.send(()).unwrap();
    expect_error(attempt.await.unwrap(), ErrorCode::ResultUnavailable);
    gateway.stop().await;
    let resumed = fixture.start(OpenMode::Resume, Limits::default()).await;
    let mut alice = Peer::open(&resumed, fixture.alice, ALICE).await;
    assert_eq!(alice.status(200).await.as_ref(), Some(&initial));
    committed(
        &alice
            .send(&message_key(direct), "rollback preserved friendship")
            .await,
        1,
    );
    removed(alice.remove(200, key).await, direct);
    denied(&alice.send(&message_key(direct), "after retry").await);
    resumed.stop().await;
}
