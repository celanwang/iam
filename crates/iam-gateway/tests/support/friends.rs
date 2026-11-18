#[path = "friend_decisions.rs"]
mod decisions;
#[path = "small_groups.rs"]
mod groups;
#[path = "friend_removal.rs"]
mod removal;

use super::*;
use iam_protocol::pb::{
    friend_request_ack, sync_response, sync_target, FriendAcceptRequest, FriendListRequest,
    FriendRequest, SyncRequest, SyncTarget,
};
use sqlx::MySqlPool;
use std::io;

async fn empty_fixture() -> Fixture {
    Fixture {
        seed_direct: false,
        ..Fixture::new().await
    }
}

// Bob can manage requests on one device while the other is offline.
async fn start_with_bob_devices(fixture: &Fixture, limits: Limits) -> (RunningGateway, Id128) {
    let [alice, bob] = fixture.credentials();
    let second = DeviceCredential {
        device_id: generate_id128(),
        token_sha256: Sha256::digest(b"bob-second").into(),
        ..bob
    };
    let gateway = fixture
        .start_with_credentials(
            OpenMode::Initialize,
            limits,
            "127.0.0.1:0",
            [alice, bob, second],
        )
        .await;
    (gateway, second.device_id)
}

async fn login(gateway: &RunningGateway, device: Id128, token: &[u8]) -> TlsTransport {
    let mut socket = gateway.client();
    socket.reconnect().await.unwrap();
    assert!(matches!(
        auth(&mut socket, 1, device, token).await,
        auth_response::Value::Session(_)
    ));
    socket
}

// Observe the real client's outgoing SYNCs while keeping TLS framing and replies intact.
struct ObservedTls {
    tls: TlsTransport,
    syncs: Vec<SyncRequest>,
}

impl Transport for ObservedTls {
    async fn reconnect(&mut self) -> io::Result<()> {
        self.tls.reconnect().await
    }

    async fn exchange(&mut self, bytes: Vec<u8>) -> io::Result<Vec<u8>> {
        let request = decode(&bytes);
        let reply = self.tls.exchange(bytes).await?;
        if let DecodedBody::Sync(req) = request {
            if req.cursor.is_some() {
                let body = decode(&reply);
                assert!(
                    matches!(&body, DecodedBody::SyncAck(ack)
                    if matches!(ack.value, Some(sync_response::Value::Page(_)))),
                    "expected an incremental page for the saved cursor, got {body:?}"
                );
            }
            self.syncs.push(req);
        }
        Ok(reply)
    }

    async fn receive(&mut self) -> io::Result<Vec<u8>> {
        self.tls.receive().await
    }
}

impl ObservedTls {
    fn new(gateway: &RunningGateway) -> Self {
        Self {
            tls: gateway.client(),
            syncs: Vec::new(),
        }
    }

    fn first_cursor(&self, target: u64) -> Option<&[u8]> {
        let target = if target == 0 {
            sync_target::Value::Account(Default::default())
        } else {
            sync_target::Value::Conversation(target)
        };
        let req = self
            .syncs
            .iter()
            .find(|req| req.target.as_ref().and_then(|scope| scope.value.as_ref()) == Some(&target))
            .expect("client did not discover the stream");
        assert!(req.page_token.is_none());
        req.cursor.as_deref()
    }
}

async fn competing_requests(
    db: &MySqlPool,
    first: impl std::future::Future<Output = DecodedBody>,
    second: impl std::future::Future<Output = DecodedBody>,
) -> (DecodedBody, DecodedBody) {
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
        // Verify overlap at TiDB before allowing either request to commit.
        timeout(WAIT, async {
            loop {
                let waiting: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM information_schema.processlist
                     WHERE DB = DATABASE() AND ID <> CONNECTION_ID() AND INFO LIKE 'SELECT store_id, token_key,%'",
                ).fetch_one(db).await.unwrap();
                if waiting == 2 {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("both requests must reach the authority lock");
        lock.rollback().await.unwrap();
    };
    let (first, second, ()) = tokio::join!(first, second, release);
    (first, second)
}

async fn saved_cursor(db: &mut SqliteConnection, target: u64, sequence: u64) -> Option<Vec<u8>> {
    sqlx::query_scalar("SELECT cursor FROM streams WHERE target = ? AND covered = ?")
        .bind(target.to_be_bytes().to_vec())
        .bind(sequence.to_be_bytes().to_vec())
        .fetch_optional(db)
        .await
        .unwrap()
}

async fn sync_through(
    client: &mut Client,
    socket: &mut ObservedTls,
    observer: &mut SqliteConnection,
    token: &[u8],
    target: u64,
    sequence: u64,
) -> Vec<u8> {
    let (stop, mut submissions) = mpsc::channel(1);
    let watch = async {
        loop {
            if let Some(cursor) = saved_cursor(observer, target, sequence).await {
                drop(stop);
                return Ok(cursor);
            }
            sleep(Duration::from_millis(10)).await;
        }
    };
    let options = Options {
        page_items: 1,
        ..Options::default()
    };
    let ((), cursor) = timeout(WAIT, async {
        tokio::try_join!(client.run(socket, token, options, &mut submissions), watch)
    })
    .await
    .expect("timed out waiting for durable client progress")
    .unwrap();
    cursor
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_friends_discovered_from_saved_account_cursor_and_recover_missed_messages() {
    let fixture = empty_fixture().await;
    let (gateway, second_device) = start_with_bob_devices(
        &fixture,
        Limits {
            push_bytes: 1,
            ..Limits::default()
        },
    )
    .await;
    let local = tempfile::tempdir().unwrap();
    let path = local.path().join("bob.db");
    let bob_id = Identity {
        tenant_id: 1,
        user_id: 200,
        device_id: fixture.bob,
    };
    let mut bob = Client::open(&path, bob_id).await.unwrap();
    let mut observer = SqliteConnection::connect_with(
        &SqliteConnectOptions::new().filename(&path).read_only(true),
    )
    .await
    .unwrap();
    let mut socket = ObservedTls::new(&gateway);
    let empty_cursor = sync_through(&mut bob, &mut socket, &mut observer, BOB, 0, 0).await;
    assert_eq!(socket.syncs.len(), 1);
    drop(socket);
    drop(bob);

    let bob_inbox = Client::open(
        local.path().join("bob-inbox.db"),
        Identity {
            device_id: second_device,
            ..bob_id
        },
    )
    .await
    .unwrap();
    let alice = Client::open(
        local.path().join("alice.db"),
        Identity {
            tenant_id: 1,
            user_id: 100,
            device_id: fixture.alice,
        },
    )
    .await
    .unwrap();
    let mut alice_socket = gateway.client();
    let mut bob_socket = gateway.client();
    assert_eq!(
        alice
            .request_friend(&mut alice_socket, ALICE, 200)
            .await
            .unwrap(),
        None
    );
    let inbox = bob_inbox
        .incoming_friend_requests(&mut bob_socket, b"bob-second", 0)
        .await
        .unwrap();
    assert_eq!(inbox.from_user_ids, [100]);
    assert!(inbox.next_after_user_id.is_none());
    let conversation = bob_inbox
        .accept_friend(&mut bob_socket, b"bob-second", 100)
        .await
        .unwrap();
    assert_ne!(conversation, 0);
    assert_eq!(
        alice
            .request_friend(&mut alice_socket, ALICE, 200)
            .await
            .unwrap(),
        Some(conversation)
    );
    drop(bob_socket);

    let mut sender = login(&gateway, fixture.alice, ALICE).await;
    send(
        &mut sender,
        2,
        message_key(conversation),
        "first while offline",
    )
    .await;
    let mut bob = Client::open(&path, bob_id).await.unwrap();
    let mut socket = ObservedTls::new(&gateway);
    let message_cursor =
        sync_through(&mut bob, &mut socket, &mut observer, BOB, conversation, 1).await;
    assert_eq!(socket.first_cursor(0), Some(empty_cursor.as_slice()));
    assert_eq!(socket.first_cursor(conversation), None);
    let account_cursor = saved_cursor(&mut observer, 0, 1).await.unwrap();
    let first = bob.messages(conversation).await.unwrap();
    assert_eq!(first.len(), 1);

    // The reader is still connected, but the gateway's one-byte push budget drops this PUSH.
    send(
        &mut sender,
        3,
        message_key(conversation),
        "last dropped push",
    )
    .await;
    assert!(timeout(Duration::from_millis(150), socket.receive())
        .await
        .is_err());
    drop(socket);
    drop(bob);
    drop(sender);
    gateway.stop().await;

    let resumed = fixture.start(OpenMode::Resume, Limits::default()).await;
    let mut socket = ObservedTls::new(&resumed);
    let mut bob = Client::open(&path, bob_id).await.unwrap();
    sync_through(&mut bob, &mut socket, &mut observer, BOB, conversation, 2).await;
    assert_eq!(socket.first_cursor(0), Some(account_cursor.as_slice()));
    assert_eq!(
        socket.first_cursor(conversation),
        Some(message_cursor.as_slice())
    );
    let messages = bob.messages(conversation).await.unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0], first[0]);
    for (i, (view, text)) in messages
        .iter()
        .zip(["first while offline", "last dropped push"])
        .enumerate()
    {
        assert_eq!(view.sender_id, Some(100));
        assert_eq!(
            view.r#ref.as_ref().unwrap().conversation_id,
            Some(conversation)
        );
        assert_eq!(
            view.position.as_ref().unwrap().create_seq,
            Some(i as u64 + 1)
        );
        assert!(matches!(view.content.as_ref().unwrap().value.as_ref(),
            Some(message_content::Value::Text(value)) if value.text.as_deref() == Some(text)));
    }
    resumed.stop().await;
}

async fn apply(socket: &mut TlsTransport, id: u64, target: u64) -> DecodedBody {
    request(
        socket,
        id,
        MsgType::FriendRequest,
        &FriendRequest {
            target_user_id: Some(target),
        },
    )
    .await
}

async fn accept(socket: &mut TlsTransport, id: u64, from: u64) -> DecodedBody {
    request(
        socket,
        id,
        MsgType::FriendAccept,
        &FriendAcceptRequest {
            from_user_id: Some(from),
        },
    )
    .await
}

fn pending(response: DecodedBody) {
    assert!(
        matches!(&response, DecodedBody::FriendRequestAck(ack)
        if matches!(ack.value, Some(friend_request_ack::Value::Pending(_)))),
        "{response:?}"
    );
}

fn accepted(response: DecodedBody) -> u64 {
    let DecodedBody::FriendAcceptAck(ack) = response else {
        panic!("expected FRIEND_ACCEPT_ACK, got {response:?}");
    };
    ack.conversation_id.filter(|&id| id != 0).unwrap()
}

fn expect_error(response: DecodedBody, code: ErrorCode) {
    assert!(
        matches!(&response, DecodedBody::Error(err)
        if err.error.as_ref().unwrap().code() == code),
        "{response:?}"
    );
}

async fn inbox(socket: &mut TlsTransport, id: u64) -> Vec<u64> {
    let reply = request(
        socket,
        id,
        MsgType::FriendList,
        &FriendListRequest {
            after_user_id: Some(0),
        },
    )
    .await;
    let DecodedBody::FriendListAck(page) = reply else {
        panic!("expected FRIEND_LIST_ACK, got {reply:?}");
    };
    assert!(page.next_after_user_id.is_none());
    page.from_user_ids
}

// Read the whole isolated catalog to catch extra IDs, members or events for any tenant.
async fn assert_single_direct(db: &MySqlPool, conversation: u64) {
    let conversations: Vec<(u64, u64)> =
        sqlx::query_as("SELECT tenant_id, conversation_id FROM conversations")
            .fetch_all(db)
            .await
            .unwrap();
    assert_eq!(conversations, [(1, conversation)]);
    for table in ["members", "account_events"] {
        let rows: Vec<(u64, u64, u64, u64)> = sqlx::query_as(&format!(
            "SELECT tenant_id, user_id, conversation_id, account_sequence FROM {table} ORDER BY user_id, account_sequence"
        )).fetch_all(db).await.unwrap();
        assert_eq!(
            rows,
            [(1, 100, conversation, 1), (1, 200, conversation, 1)],
            "{table}"
        );
    }
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_friends_crossed_requests_and_concurrent_accepts_create_one_direct() {
    let fixture = empty_fixture().await;
    let (gateway, second_device) = start_with_bob_devices(&fixture, Limits::default()).await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let mut alice = login(&gateway, fixture.alice, ALICE).await;
    let mut bob = login(&gateway, fixture.bob, BOB).await;
    let (a, b) = tokio::join!(apply(&mut alice, 2, 200), apply(&mut bob, 2, 100));
    pending(a);
    pending(b);
    pending(apply(&mut alice, 3, 200).await);
    assert_eq!(inbox(&mut alice, 4).await, [200]);
    assert_eq!(inbox(&mut bob, 3).await, [100]);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM conversations")
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(count, 0, "crossed requests require explicit acceptance");

    let mut other_device = login(&gateway, second_device, b"bob-second").await;
    let (a, b, duplicate) = tokio::join!(
        accept(&mut alice, 5, 200),
        accept(&mut bob, 4, 100),
        accept(&mut other_device, 2, 100),
    );
    let conversation = accepted(a);
    assert_eq!(accepted(b), conversation);
    assert_eq!(accepted(duplicate), conversation);
    assert_eq!(accepted(accept(&mut alice, 6, 200).await), conversation);
    assert_eq!(accepted(accept(&mut bob, 5, 100).await), conversation);
    assert!(inbox(&mut alice, 7).await.is_empty());
    assert!(inbox(&mut bob, 6).await.is_empty());
    assert_single_direct(&db, conversation).await;
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_friends_retry_accept_after_tidb_commits_and_loses_reply() {
    check_accept_fault(CommitPhase::After).await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_friends_retry_accept_after_disconnect_before_commit() {
    check_accept_fault(CommitPhase::Before).await;
}

async fn check_accept_fault(phase: CommitPhase) {
    let mut fixture = empty_fixture().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let proxy = CommitProxy::start(&fixture.database).await;
    fixture.database = proxy.url.clone();
    let gateway = fixture.start(OpenMode::Initialize, Limits::default()).await;
    let mut alice = login(&gateway, fixture.alice, ALICE).await;
    let mut bob = login(&gateway, fixture.bob, BOB).await;
    pending(apply(&mut alice, 2, 200).await);
    let (checkpoint, disconnect) = proxy.arm(phase);
    let accepting = tokio::spawn(async move { accept(&mut bob, 2, 100).await });
    timeout(WAIT, checkpoint).await.unwrap().unwrap().unwrap();
    let original: Option<u64> = sqlx::query_scalar("SELECT conversation_id FROM conversations")
        .fetch_optional(&db)
        .await
        .unwrap();
    match phase {
        CommitPhase::Before => {
            assert_eq!(original, None);
            let counts: (i64, i64) = sqlx::query_as(
                "SELECT (SELECT COUNT(*) FROM members), (SELECT COUNT(*) FROM account_events)",
            )
            .fetch_one(&db)
            .await
            .unwrap();
            assert_eq!(counts, (0, 0));
        }
        CommitPhase::After => assert_single_direct(&db, original.unwrap()).await,
    }
    disconnect.send(()).unwrap();
    let result = accepting.await.unwrap();
    let DecodedBody::Error(reply) = result else {
        panic!("expected uncertain ACCEPT, got {result:?}");
    };
    let info = reply.error.unwrap();
    assert_eq!(info.code(), ErrorCode::ResultUnavailable);
    assert_eq!(
        info.action(),
        iam_protocol::pb::RetryAction::RetrySameRequest
    );
    gateway.stop().await;

    let resumed = fixture.start(OpenMode::Resume, Limits::default()).await;
    let mut bob = login(&resumed, fixture.bob, BOB).await;
    let pending = inbox(&mut bob, 2).await;
    if original.is_some() {
        assert!(pending.is_empty());
    } else {
        assert_eq!(pending, [100]);
    }
    let conversation = accepted(accept(&mut bob, 3, 100).await);
    if let Some(original) = original {
        assert_eq!(conversation, original);
    }
    assert!(inbox(&mut bob, 4).await.is_empty());
    assert_single_direct(&db, conversation).await;
    resumed.stop().await;
}

async fn assert_nonmember(socket: &mut TlsTransport, conversation: u64) {
    let result = request(
        socket,
        8,
        MsgType::Sync,
        &SyncRequest {
            target: Some(SyncTarget {
                value: Some(sync_target::Value::Conversation(conversation)),
            }),
            cursor: None,
            page_token: None,
            limit: Some(iam_protocol::pb::PageLimit {
                max_items: Some(10),
                max_bytes: Some(256 * 1024),
            }),
        },
    )
    .await;
    assert!(
        matches!(&result, DecodedBody::SyncAck(ack)
        if matches!(&ack.value, Some(sync_response::Value::Error(error))
            if error.code() == ErrorCode::PermissionDenied)),
        "{result:?}"
    );

    let result = request(
        socket,
        9,
        MsgType::Send,
        &send_request(message_key(conversation), "unrelated account"),
    )
    .await;
    assert!(
        matches!(&result, DecodedBody::SendAck(ack)
        if matches!(ack.outcome.as_ref().and_then(|outcome| outcome.value.as_ref()),
            Some(send_outcome::Value::RejectedFinal(error))
                if error.code() == ErrorCode::PermissionDenied)),
        "{result:?}"
    );
    assert!(timeout(Duration::from_millis(150), socket.receive())
        .await
        .is_err());
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_friends_pending_and_messages_are_scoped_to_participants_and_tenant() {
    let fixture = empty_fixture().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let mut credentials = fixture.credentials().to_vec();
    let extra = |tenant_id, user_id, token: &[u8]| DeviceCredential {
        tenant_id,
        user_id,
        device_id: generate_id128(),
        token_sha256: Sha256::digest(token).into(),
        expires_at_ms: fixture.expires_at_ms,
    };
    let charlie = extra(1, 300, b"charlie");
    let other_bob = extra(2, 200, b"other-bob");
    let other_alice = extra(2, 100, b"other-alice");
    credentials.extend([charlie, other_bob, other_alice]);
    let gateway = fixture
        .start_with_credentials(
            OpenMode::Initialize,
            Limits::default(),
            "127.0.0.1:0",
            credentials,
        )
        .await;
    let mut alice = login(&gateway, fixture.alice, ALICE).await;
    let mut bob = login(&gateway, fixture.bob, BOB).await;
    let mut third = login(&gateway, charlie.device_id, b"charlie").await;
    let mut foreign = login(&gateway, other_bob.device_id, b"other-bob").await;

    expect_error(apply(&mut alice, 2, 100).await, ErrorCode::InvalidArgument);
    expect_error(accept(&mut alice, 3, 100).await, ErrorCode::InvalidArgument);
    pending(apply(&mut alice, 4, 200).await);
    expect_error(accept(&mut alice, 5, 200).await, ErrorCode::NotFound);
    assert!(inbox(&mut third, 2).await.is_empty());
    assert!(inbox(&mut foreign, 2).await.is_empty());
    expect_error(accept(&mut third, 3, 100).await, ErrorCode::NotFound);
    expect_error(accept(&mut foreign, 3, 100).await, ErrorCode::NotFound);
    expect_error(apply(&mut foreign, 4, 300).await, ErrorCode::NotFound);
    assert_eq!(inbox(&mut bob, 2).await, [100]);
    let conversation = accepted(accept(&mut bob, 3, 100).await);
    expect_error(accept(&mut alice, 6, 200).await, ErrorCode::NotFound);
    expect_error(accept(&mut third, 5, 100).await, ErrorCode::NotFound);
    expect_error(accept(&mut foreign, 5, 100).await, ErrorCode::NotFound);

    send(&mut alice, 7, message_key(conversation), "members only").await;
    assert!(matches!(receive(&mut bob).await, DecodedBody::Push(_)));
    tokio::join!(
        assert_nonmember(&mut third, conversation),
        assert_nonmember(&mut foreign, conversation)
    );
    assert_single_direct(&db, conversation).await;
    let messages: Vec<(u64, u64, u64)> =
        sqlx::query_as("SELECT tenant_id, conversation_id, user_id FROM messages")
            .fetch_all(&db)
            .await
            .unwrap();
    assert_eq!(messages, [(1, conversation, 100)]);
    gateway.stop().await;
}
