use super::*;
use iam_protocol::pb::{
    friend_attempt_ack::Value as Outcome, Empty, FriendApplicant, FriendDecideRequest,
};
use std::path::Path;

const PENDING: Outcome = Outcome::Pending(Empty {});
const REJECTED: Outcome = Outcome::Rejected(Empty {});

struct FriendClient {
    client: Client,
    socket: TlsTransport,
    token: &'static [u8],
}

impl FriendClient {
    async fn open(
        directory: &Path,
        gateway: &RunningGateway,
        credential: DeviceCredential,
        token: &'static [u8],
    ) -> Self {
        let path = directory.join(hex::encode(credential.device_id));
        let client = Client::open(
            path,
            Identity {
                tenant_id: credential.tenant_id,
                user_id: credential.user_id,
                device_id: credential.device_id,
            },
        )
        .await
        .unwrap();
        Self {
            client,
            socket: gateway.client(),
            token,
        }
    }

    async fn apply(&mut self, target: u64, attempt: Id128) -> Outcome {
        self.client
            .apply_friend(&mut self.socket, self.token, target, attempt)
            .await
            .unwrap()
            .value
            .unwrap()
    }

    async fn apply_error(&mut self, target: u64, attempt: Id128) -> iam_protocol::pb::ErrorInfo {
        let result = self
            .client
            .apply_friend(&mut self.socket, self.token, target, attempt)
            .await;
        let Err(iam_client::Error::Server(error)) = result else {
            panic!("expected a server error: {result:?}");
        };
        error
    }

    async fn decide(
        &mut self,
        from: u64,
        attempt: Id128,
        accept: bool,
    ) -> iam_client::Result<Outcome> {
        let reply = self
            .client
            .decide_friend(&mut self.socket, self.token, from, attempt, accept)
            .await?;
        Ok(reply.value.unwrap())
    }

    async fn inbox(&mut self) -> Vec<FriendApplicant> {
        let page = self
            .client
            .friend_inbox(&mut self.socket, self.token, 0)
            .await
            .unwrap();
        assert!(page.next_after_user_id.is_none());
        page.requests
    }
}

fn applicant(from: u64, attempt: Id128) -> FriendApplicant {
    FriendApplicant {
        from_user_id: Some(from),
        request_id: Some(attempt.to_vec().into()),
    }
}

async fn assert_no_direct(db: &MySqlPool) {
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM conversations), (SELECT COUNT(*) FROM members), (SELECT COUNT(*) FROM account_events)",
    ).fetch_one(db).await.unwrap();
    assert_eq!(counts, (0, 0, 0));
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_decisions_recover_lost_rejection_and_isolate_next_attempt() {
    let mut fixture = empty_fixture().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let proxy = CommitProxy::start(&fixture.database).await;
    fixture.database = proxy.url.clone();
    let gateway = fixture.start(OpenMode::Initialize, Limits::default()).await;
    let disk = tempfile::tempdir().unwrap();
    let [a, b] = fixture.credentials();
    let mut alice = FriendClient::open(disk.path(), &gateway, a, ALICE).await;
    let mut bob = FriendClient::open(disk.path(), &gateway, b, BOB).await;
    let first = generate_id128();
    assert_eq!(alice.apply(200, first).await, PENDING);
    assert_eq!(bob.inbox().await, [applicant(100, first)]);

    // Authenticate before arming the proxy so it intercepts the decision transaction.
    let mut tls = login(&gateway, fixture.bob, BOB).await;
    let (committed, disconnect) = proxy.arm(CommitPhase::After);
    let rejecting = tokio::spawn(async move {
        request(
            &mut tls,
            2,
            MsgType::FriendDecide,
            &FriendDecideRequest {
                from_user_id: Some(100),
                request_id: Some(first.to_vec().into()),
                accept: Some(false),
            },
        )
        .await
    });
    timeout(WAIT, committed).await.unwrap().unwrap().unwrap();
    disconnect.send(()).unwrap();
    let reply = rejecting.await.unwrap();
    let DecodedBody::Error(error) = reply else {
        panic!("expected uncertain rejection, got {reply:?}");
    };
    let info = error.error.unwrap();
    assert_eq!(info.code(), ErrorCode::ResultUnavailable);
    assert_eq!(
        info.action(),
        iam_protocol::pb::RetryAction::RetrySameRequest
    );
    gateway.stop().await;
    drop(alice);
    drop(bob);

    let second_alice = DeviceCredential {
        device_id: generate_id128(),
        token_sha256: Sha256::digest(b"alice-second").into(),
        ..a
    };
    let resumed = fixture
        .start_with_credentials(
            OpenMode::Resume,
            Limits::default(),
            "127.0.0.1:0",
            [a, b, second_alice],
        )
        .await;
    let mut alice = FriendClient::open(disk.path(), &resumed, second_alice, b"alice-second").await;
    let mut bob = FriendClient::open(disk.path(), &resumed, b, BOB).await;
    assert!(bob.inbox().await.is_empty());
    assert_eq!(alice.apply(200, first).await, REJECTED);
    assert_eq!(bob.decide(100, first, false).await.unwrap(), REJECTED);

    let next = generate_id128();
    assert_eq!(alice.apply(200, next).await, PENDING);
    let mut legacy = login(&resumed, fixture.bob, BOB).await;
    expect_error(accept(&mut legacy, 2, 100).await, ErrorCode::NotFound);
    assert_eq!(bob.decide(100, first, true).await.unwrap(), REJECTED);
    assert_eq!(bob.decide(100, first, false).await.unwrap(), REJECTED);
    assert_eq!(alice.apply(200, first).await, REJECTED);
    assert_eq!(bob.inbox().await, [applicant(100, next)]);
    assert_no_direct(&db).await;

    let Outcome::ConversationId(conversation) = bob.decide(100, next, true).await.unwrap() else {
        panic!("second attempt should be accepted");
    };
    assert_eq!(
        bob.decide(100, next, false).await.unwrap(),
        Outcome::ConversationId(conversation)
    );
    assert_eq!(
        alice.apply(200, first).await,
        REJECTED,
        "a later friendship must not overwrite the first decision"
    );
    assert_eq!(
        alice.apply(200, next).await,
        Outcome::ConversationId(conversation)
    );
    assert!(bob.inbox().await.is_empty());
    assert_single_direct(&db, conversation).await;
    resumed.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_decisions_concurrent_accept_and_reject_return_one_durable_outcome() {
    let fixture = empty_fixture().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let (gateway, second_device) = start_with_bob_devices(&fixture, Limits::default()).await;
    let disk = tempfile::tempdir().unwrap();
    let [a, b] = fixture.credentials();
    let mut alice = FriendClient::open(disk.path(), &gateway, a, ALICE).await;
    let attempt = generate_id128();
    assert_eq!(alice.apply(200, attempt).await, PENDING);
    // Both devices are authenticated before the race, with independent gateway connections.
    let mut phone = login(&gateway, fixture.bob, BOB).await;
    let mut desktop = login(&gateway, second_device, b"bob-second").await;
    let decide = |accept| FriendDecideRequest {
        from_user_id: Some(100),
        request_id: Some(attempt.to_vec().into()),
        accept: Some(accept),
    };
    let yes = decide(true);
    let no = decide(false);
    let mut blocker = db.acquire().await.unwrap();
    sqlx::query("SET SESSION tidb_txn_mode = 'pessimistic'")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let mut lock = blocker.begin().await.unwrap();
    sqlx::query("SELECT singleton FROM store_control WHERE singleton = 1 FOR UPDATE")
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let release = async {
        // Verify overlap at TiDB before allowing either decision to commit.
        timeout(WAIT, async {
            loop {
                let waiting: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM information_schema.processlist
                     WHERE DB = DATABASE() AND ID <> CONNECTION_ID() AND INFO LIKE 'SELECT store_id, token_key,%'",
                ).fetch_one(&db).await.unwrap();
                if waiting == 2 {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("both decisions must reach the authority lock");
        lock.rollback().await.unwrap();
    };
    let (yes, no, ()) = tokio::join!(
        request(&mut phone, 2, MsgType::FriendDecide, &yes),
        request(&mut desktop, 2, MsgType::FriendDecide, &no),
        release,
    );
    let (DecodedBody::FriendDecideAck(yes), DecodedBody::FriendDecideAck(no)) = (yes, no) else {
        panic!("both calls must return the durable terminal result");
    };
    let outcome = yes.value.unwrap();
    assert_eq!(Some(&outcome), no.value.as_ref());
    gateway.stop().await;
    drop(alice);

    let resumed = fixture.start(OpenMode::Resume, Limits::default()).await;
    let mut alice = FriendClient::open(disk.path(), &resumed, a, ALICE).await;
    let mut bob = FriendClient::open(disk.path(), &resumed, b, BOB).await;
    assert_eq!(alice.apply(200, attempt).await, outcome);
    assert_eq!(bob.decide(100, attempt, false).await.unwrap(), outcome);
    assert_eq!(bob.decide(100, attempt, true).await.unwrap(), outcome);
    assert!(bob.inbox().await.is_empty());
    match outcome {
        Outcome::ConversationId(conversation) => assert_single_direct(&db, conversation).await,
        Outcome::Rejected(_) => assert_no_direct(&db).await,
        Outcome::Pending(_) => panic!("a decision must settle the attempt"),
    }
    resumed.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_decisions_only_the_recipient_in_the_originating_tenant_can_decide() {
    let fixture = empty_fixture().await;
    let [a, b] = fixture.credentials();
    let credential = |tenant_id, user_id, token: &[u8]| DeviceCredential {
        tenant_id,
        user_id,
        device_id: generate_id128(),
        token_sha256: Sha256::digest(token).into(),
        expires_at_ms: fixture.expires_at_ms,
    };
    let third = credential(1, 300, b"charlie");
    let other_alice = credential(2, 100, b"other-alice");
    let other_bob = credential(2, 200, b"other-bob");
    let gateway = fixture
        .start_with_credentials(
            OpenMode::Initialize,
            Limits::default(),
            "127.0.0.1:0",
            [a, b, third, other_alice, other_bob],
        )
        .await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let disk = tempfile::tempdir().unwrap();
    let mut alice = FriendClient::open(disk.path(), &gateway, a, ALICE).await;
    let mut bob = FriendClient::open(disk.path(), &gateway, b, BOB).await;
    let mut charlie = FriendClient::open(disk.path(), &gateway, third, b"charlie").await;
    let mut foreign = FriendClient::open(disk.path(), &gateway, other_bob, b"other-bob").await;
    let attempt = generate_id128();
    assert_eq!(alice.apply(200, attempt).await, PENDING);
    assert_eq!(
        alice.apply_error(300, attempt).await.code(),
        ErrorCode::IdempotencyConflict
    );
    for outsider in [&mut charlie, &mut foreign] {
        assert!(outsider.inbox().await.is_empty());
        for accept in [true, false] {
            let result = outsider.decide(100, attempt, accept).await;
            assert!(
                matches!(&result, Err(iam_client::Error::Server(info)) if info.code() == ErrorCode::NotFound),
                "{result:?}"
            );
        }
    }
    assert_eq!(bob.inbox().await, [applicant(100, attempt)]);
    assert_eq!(bob.decide(100, attempt, false).await.unwrap(), REJECTED);
    assert_no_direct(&db).await;
    gateway.stop().await;
}

// A stopped PR #18 store with existing friends (100, 200) and crossed pending requests (200, 300).
// A frozen schema keeps this fixture independent of the current migration code.
async fn old_friend_store(fixture: &Fixture, db: &MySqlPool) {
    for ddl in include_str!("friends-v1.sql")
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        sqlx::query(ddl).execute(db).await.unwrap();
    }
    let history = generate_id128();
    let mut tx = db.begin().await.unwrap();
    sqlx::query("INSERT INTO store_control VALUES (1, ?, ?, 0, -1, 1)")
        .bind(fixture.store.as_slice())
        .bind([7u8; 32].as_slice())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO conversations VALUES (1, 42, ?, 1)")
        .bind(history.as_slice())
        .execute(&mut *tx)
        .await
        .unwrap();
    for user in [100u64, 200] {
        sqlx::query("INSERT INTO members VALUES (1, ?, 42, 1)")
            .bind(user)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_events VALUES (1, ?, 1, 42, ?, 1)")
            .bind(user)
            .bind(history.as_slice())
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO direct_pairs VALUES (1, 100, 200, 42, TRUE)")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO friend_requests VALUES (1, 200, 100, FALSE), (1, 200, 300, TRUE), (1, 300, 200, TRUE)")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_decisions_upgrade_v1_and_keep_legacy_accept_bound_to_its_attempt() {
    let fixture = empty_fixture().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    old_friend_store(&fixture, &db).await;
    TidbService::upgrade_local(fixture.database.as_str(), fixture.store)
        .await
        .unwrap();
    let [a, b] = fixture.credentials();
    let c = DeviceCredential {
        user_id: 300,
        device_id: generate_id128(),
        token_sha256: Sha256::digest(b"charlie").into(),
        ..a
    };
    let start = || {
        fixture.start_with_credentials(
            OpenMode::Resume,
            Limits::default(),
            "127.0.0.1:0",
            [a, b, c],
        )
    };
    let gateway = start().await;
    let disk = tempfile::tempdir().unwrap();
    let mut alice = login(&gateway, fixture.alice, ALICE).await;
    let mut legacy_bob = login(&gateway, fixture.bob, BOB).await;
    assert_eq!(inbox(&mut legacy_bob, 2).await, [300]);
    assert_eq!(accepted(accept(&mut legacy_bob, 3, 100).await), 42);
    send(&mut alice, 2, message_key(42), "after upgrade").await;
    let mut bob = FriendClient::open(disk.path(), &gateway, b, BOB).await;
    let entries = bob.inbox().await;
    let [entry] = entries.as_slice() else {
        panic!("expected one upgraded pending request: {entries:?}");
    };
    assert_eq!(entry.from_user_id, Some(300));
    let first = iam_protocol::parse_id128_bytes(entry.request_id.as_deref().unwrap()).unwrap();
    assert_eq!(bob.decide(300, first, false).await.unwrap(), REJECTED);
    let mut charlie = FriendClient::open(disk.path(), &gateway, c, b"charlie").await;
    let next = generate_id128();
    assert_eq!(charlie.apply(200, next).await, PENDING);
    drop(bob);
    drop(charlie);
    gateway.stop().await;

    // Rerunning the upgrade must not resurrect a rejected row from the old schema.
    TidbService::upgrade_local(fixture.database.as_str(), fixture.store)
        .await
        .unwrap();
    let gateway = start().await;
    let mut bob = FriendClient::open(disk.path(), &gateway, b, BOB).await;
    let mut charlie = FriendClient::open(disk.path(), &gateway, c, b"charlie").await;
    assert_eq!(bob.inbox().await, [applicant(300, next)]);
    assert_eq!(bob.decide(300, first, true).await.unwrap(), REJECTED);
    let mut legacy_bob = login(&gateway, fixture.bob, BOB).await;
    expect_error(
        accept(&mut legacy_bob, 2, 300).await,
        ErrorCode::ContextChanged,
    );
    assert!(inbox(&mut legacy_bob, 3).await.is_empty());
    let mut legacy_sender = login(&gateway, c.device_id, b"charlie").await;
    expect_error(
        apply(&mut legacy_sender, 2, 200).await,
        ErrorCode::ContextChanged,
    );
    assert_eq!(bob.inbox().await, [applicant(300, next)]);
    assert_eq!(bob.decide(300, next, false).await.unwrap(), REJECTED);
    assert_eq!(charlie.apply(200, next).await, REJECTED);
    assert_single_direct(&db, 42).await;

    // A legacy client can still accept the opposite pending row originally created by PR #18.
    let mut legacy_sender = login(&gateway, c.device_id, b"charlie").await;
    assert_eq!(inbox(&mut legacy_sender, 2).await, [200]);
    let conversation = accepted(accept(&mut legacy_sender, 3, 200).await);
    assert_ne!(conversation, 42);
    assert_eq!(
        accepted(accept(&mut legacy_sender, 4, 200).await),
        conversation
    );
    assert_eq!(charlie.apply(200, next).await, REJECTED);
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_decisions_crossed_accept_preserves_prior_rejection_and_busy_key() {
    let fixture = empty_fixture().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let gateway = fixture.start(OpenMode::Initialize, Limits::default()).await;
    let disk = tempfile::tempdir().unwrap();
    let [a, b] = fixture.credentials();
    let mut alice = FriendClient::open(disk.path(), &gateway, a, ALICE).await;
    let mut bob = FriendClient::open(disk.path(), &gateway, b, BOB).await;
    let first = generate_id128();
    let busy = generate_id128();
    assert_eq!(alice.apply(200, first).await, PENDING);
    let conflict = alice.apply_error(200, busy).await;
    assert_eq!(conflict.code(), ErrorCode::InProgress);
    assert_eq!(conflict.action(), iam_protocol::pb::RetryAction::None);
    let reverse = generate_id128();
    assert_eq!(bob.apply(100, reverse).await, PENDING);
    assert_eq!(bob.decide(100, first, false).await.unwrap(), REJECTED);
    assert_eq!(
        alice.apply_error(200, busy).await.code(),
        ErrorCode::InProgress
    );
    assert!(bob.inbox().await.is_empty());
    assert_eq!(alice.inbox().await, [applicant(200, reverse)]);

    let next = generate_id128();
    assert_eq!(alice.apply(200, next).await, PENDING);
    let result = alice.decide(200, reverse, true).await.unwrap();
    let Outcome::ConversationId(conversation) = result else {
        panic!("expected accepted, got {result:?}");
    };
    assert_eq!(bob.decide(100, next, false).await.unwrap(), result);
    assert_eq!(alice.apply(200, next).await, result);
    assert_eq!(alice.apply(200, first).await, REJECTED);
    assert_eq!(
        alice.apply_error(200, busy).await.code(),
        ErrorCode::InProgress
    );
    assert!(alice.inbox().await.is_empty());
    assert!(bob.inbox().await.is_empty());
    assert_single_direct(&db, conversation).await;
    gateway.stop().await;
}
