mod support;

#[path = "support/commit_proxy.rs"]
mod commit_proxy;

use commit_proxy::{CommitPhase, CommitProxy};
use iam_core::Engine;
use iam_protocol::pb::{
    auth_response, change, message_content, send_outcome, snapshot_item, snapshot_response,
    sync_response, sync_target, AuthRequest, Empty, ErrorCode, Head, MessageContent, MessageKey,
    PageLimit, SendReceipt, SendRequest, SendStatusRequest, SnapshotPage, SnapshotRequest,
    SyncPage, SyncRequest, SyncTarget, TextContent,
};
use iam_protocol::{
    decode_frame, encode_frame, generate_id128, id128_timestamp_ms, DecodedBody, DecodedFrame,
    Frame, FrameDecoder, Id128, MsgType,
};
use iam_store::{OpenMode, TidbService};
use prost::Message;

type Pushes = Vec<(Id128, Vec<u8>)>;

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_rename_preserves_receipts_rejections_and_cursors() {
    let database = support::isolated_database().await;
    let archive = support::isolated_database().await;
    let store_id = generate_id128();
    let mut service =
        TidbService::connect_local(database.as_str(), store_id, seeded(), OpenMode::Initialize)
            .await
            .unwrap();
    let mut alice = Peer::new();
    let mut bob = Peer::new();
    alice.auth(&mut service, b"alice").await;
    bob.auth(&mut service, b"bob").await;
    let conversation = sync_target::Value::Conversation(1);
    let cursor = bob
        .snapshot(&mut service, conversation)
        .await
        .resume_cursor
        .unwrap();
    let key = message_key(1, generate_id128());
    let (value, _) = alice.send(&mut service, &key, "before rename").await;
    let original = committed(value);
    let bad_key = message_key(1, generate_id128());
    let (rejection, _) = alice.send(&mut service, &bad_key, "").await;
    assert!(matches!(rejection, send_outcome::Value::RejectedFinal(_)));
    drop(service);

    let pool = sqlx::MySqlPool::connect(database.as_str()).await.unwrap();
    sqlx::query("RENAME TABLE store_control TO protocol_store, conversations TO protocol_conversations, messages TO protocol_messages, rejections TO protocol_rejections, members TO protocol_members")
        .execute(&pool).await.unwrap();
    for mode in [OpenMode::Initialize, OpenMode::Resume] {
        assert!(
            TidbService::connect_local(database.as_str(), store_id, identities(), mode)
                .await
                .is_err()
        );
    }

    // Simulate both old schemas having lived in the same database.
    sqlx::query("CREATE TABLE conversations LIKE protocol_conversations")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE messages LIKE protocol_messages")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO conversations SELECT * FROM protocol_conversations")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO messages SELECT * FROM protocol_messages")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE messages SET `text` = 'legacy text' WHERE tenant_id = 1 AND conversation_id = 1",
    )
    .execute(&pool)
    .await
    .unwrap();
    let migration = include_str!("../migrations/rename_protocol_tables.sql");
    assert!(sqlx::query(migration).execute(&pool).await.is_err());
    let existing: u64 = sqlx::query_scalar("SELECT CAST(COUNT(*) AS UNSIGNED) FROM protocol_store")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(existing, 1);
    let archive_name = archive.path().trim_start_matches('/');
    sqlx::query(&format!(
        "RENAME TABLE conversations TO `{archive_name}`.conversations, messages TO `{archive_name}`.messages"
    )).execute(&pool).await.unwrap();
    sqlx::query(migration).execute(&pool).await.unwrap();
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY table_name",
    ).fetch_all(&pool).await.unwrap();
    assert_eq!(
        tables,
        [
            "conversations",
            "members",
            "messages",
            "rejections",
            "store_control"
        ]
    );
    let archived: u64 = sqlx::query_scalar(&format!(
        "SELECT CAST(COUNT(*) AS UNSIGNED) FROM `{archive_name}`.conversations"
    ))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(archived, 2);
    let text: Vec<u8> = sqlx::query_scalar(&format!(
        "SELECT `text` FROM `{archive_name}`.messages WHERE tenant_id = 1 AND conversation_id = 1"
    ))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(text, b"legacy text");

    let mut resumed =
        TidbService::connect_local(database.as_str(), store_id, identities(), OpenMode::Resume)
            .await
            .unwrap();
    alice.auth(&mut resumed, b"alice").await;
    bob.auth(&mut resumed, b"bob").await;
    assert_eq!(committed(alice.status(&mut resumed, &key).await), original);
    assert_eq!(alice.status(&mut resumed, &bad_key).await, rejection);
    let page = sync_page(
        bob.sync(
            &mut resumed,
            conversation,
            Some(cursor.to_vec()),
            None,
            1,
            256 * 1024,
        )
        .await,
    );
    assert_change(&page, &original);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_protocol_restarts_and_syncs_discarded_pushes() {
    let database = support::isolated_database().await;
    let store_id = generate_id128();
    let mut service =
        TidbService::connect_local(database.as_str(), store_id, seeded(), OpenMode::Initialize)
            .await
            .unwrap();
    let mut alice = Peer::new();
    let mut bob = Peer::new();
    alice.auth(&mut service, b"alice").await;
    bob.auth(&mut service, b"bob").await;
    let account = sync_target::Value::Account(Empty {});
    let conversation = sync_target::Value::Conversation(1);
    let account_page = bob.snapshot(&mut service, account).await;
    assert!(matches!(
        account_page.items.as_slice(),
        [item] if matches!(&item.value, Some(snapshot_item::Value::Conversation(c)) if c.conversation_id == Some(1))
    ));
    let account_cursor = account_page.resume_cursor.unwrap().to_vec();
    let baseline = bob.snapshot(&mut service, conversation).await;
    assert!(baseline.items.is_empty());
    let cursor = baseline.resume_cursor.unwrap().to_vec();

    let first_key = message_key(1, generate_id128());
    let (first, pushes) = alice.send(&mut service, &first_key, "first").await;
    let first = committed(first);
    assert_push(&pushes, &bob, &first);
    drop(pushes);
    let second_key = message_key(1, generate_id128());
    let (second, pushes) = alice.send(&mut service, &second_key, "second").await;
    let second = committed(second);
    assert_push(&pushes, &bob, &second);
    drop(pushes);

    let too_small = bob
        .sync(&mut service, conversation, Some(cursor.clone()), None, 1, 1)
        .await;
    assert!(matches!(too_small, sync_response::Value::Error(error)
        if error.code == Some(ErrorCode::PageBudgetTooSmall as i32)));
    let page = sync_page(
        bob.sync(
            &mut service,
            conversation,
            Some(cursor),
            None,
            1,
            256 * 1024,
        )
        .await,
    );
    assert_change(&page, &first);
    assert_eq!(page.upper_bound, Some(2));
    let next_page = page.next_page_token.unwrap().to_vec();
    drop(service);

    // Recreate the engine with identity seeds only. Conversation data and tokens come from TiDB.
    let mut service =
        TidbService::connect_local(database.as_str(), store_id, identities(), OpenMode::Resume)
            .await
            .unwrap();
    alice.auth(&mut service, b"alice").await;
    bob.auth(&mut service, b"bob").await;
    assert_eq!(
        committed(alice.status(&mut service, &first_key).await),
        first
    );
    let (retry, pushes) = alice.send(&mut service, &first_key, "first").await;
    assert_eq!(committed(retry), first);
    assert!(pushes.is_empty());

    let (third, pushes) = alice
        .send(&mut service, &message_key(1, generate_id128()), "third")
        .await;
    let third = committed(third);
    assert_push(&pushes, &bob, &third);
    drop(pushes);
    let page = sync_page(
        bob.sync(
            &mut service,
            conversation,
            None,
            Some(next_page),
            1,
            256 * 1024,
        )
        .await,
    );
    assert_change(&page, &second);
    assert_eq!(page.upper_bound, Some(2));
    assert!(page.next_page_token.is_none());
    let page = sync_page(
        bob.sync(
            &mut service,
            conversation,
            Some(page.next_cursor.unwrap().to_vec()),
            None,
            100,
            256 * 1024,
        )
        .await,
    );
    assert_change(&page, &third);
    assert_eq!(page.upper_bound, Some(3));
    let page = sync_page(
        bob.sync(
            &mut service,
            account,
            Some(account_cursor),
            None,
            100,
            256 * 1024,
        )
        .await,
    );
    assert!(page.changes.is_empty());
    assert_eq!(page.covered_through, Some(1));
    let snapshot = bob.snapshot(&mut service, conversation).await;
    assert_eq!(snapshot.upper_bound, Some(3));
    assert_eq!(snapshot.items.len(), 3);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_protocol_resolves_a_lost_commit_response() {
    check_commit_disconnect(CommitPhase::After).await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_protocol_retries_a_rolled_back_send() {
    check_commit_disconnect(CommitPhase::Before).await;
}

async fn check_commit_disconnect(phase: CommitPhase) {
    let database = support::isolated_database().await;
    let store_id = generate_id128();
    let proxy = CommitProxy::start(&database).await;
    let mut service =
        TidbService::connect_local(proxy.url.as_str(), store_id, seeded(), OpenMode::Initialize)
            .await
            .unwrap();
    let mut alice = Peer::new();
    alice.auth(&mut service, b"alice").await;
    let key = message_key(1, generate_id128());
    let (reached, disconnect) = proxy.arm(phase);
    let (outcome, pushes) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let mut send = std::pin::pin!(alice.send(&mut service, &key, "unknown commit"));
        tokio::select! {
            result = reached => result.unwrap().unwrap(),
            _ = &mut send => panic!("send completed before dropping COMMIT response"),
        }
        disconnect.send(()).unwrap();
        send.await
    })
    .await
    .unwrap();
    assert_unresolved(outcome, ErrorCode::ResultUnavailable, false);
    assert!(pushes.is_empty());
    drop(service);

    let mut service =
        TidbService::connect_local(database.as_str(), store_id, identities(), OpenMode::Resume)
            .await
            .unwrap();
    alice.auth(&mut service, b"alice").await;
    let status = alice.status(&mut service, &key).await;
    let original = match phase {
        CommitPhase::After => Some(committed(status)),
        CommitPhase::Before => {
            assert_unresolved(status, ErrorCode::NotFound, false);
            None
        }
    };
    let (retried, _) = alice.send(&mut service, &key, "unknown commit").await;
    let receipt = committed(retried);
    if let Some(original) = original {
        assert_eq!(receipt, original);
    }
    assert_eq!(receipt.position.unwrap().create_seq, Some(1));
    let snapshot = alice
        .snapshot(&mut service, sync_target::Value::Conversation(1))
        .await;
    assert_eq!(snapshot.upper_bound, Some(1));
    assert_eq!(snapshot.items.len(), 1);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_protocol_replays_rejections_and_enforces_scopes_and_windows() {
    let database = support::isolated_database().await;
    let store_id = generate_id128();
    let mut service =
        TidbService::connect_local(database.as_str(), store_id, seeded(), OpenMode::Initialize)
            .await
            .unwrap();
    let mut alice = Peer::new();
    let mut other = Peer::new();
    alice.auth(&mut service, b"alice").await;
    other.auth(&mut service, b"other").await;
    let key = message_key(1, generate_id128());
    let (outcome, _) = alice.send(&mut service, &key, "").await;
    assert!(matches!(outcome, send_outcome::Value::RejectedFinal(error)
        if error.code == Some(ErrorCode::InvalidArgument as i32)));
    // Even an identical user/device/key in another tenant has independent state.
    other.device = alice.device;
    other.auth(&mut service, b"other").await;
    assert_unresolved(
        other.status(&mut service, &key).await,
        ErrorCode::NotFound,
        false,
    );
    let (outcome, _) = other.send(&mut service, &key, "different tenant").await;
    let other_receipt = committed(outcome);
    let snapshot = alice
        .snapshot(&mut service, sync_target::Value::Conversation(1))
        .await;
    assert!(snapshot.items.is_empty());
    drop(service);

    assert!(TidbService::connect_local(
        database.as_str(),
        generate_id128(),
        identities(),
        OpenMode::Resume,
    )
    .await
    .is_err());
    let mut service =
        TidbService::connect_local(database.as_str(), store_id, identities(), OpenMode::Resume)
            .await
            .unwrap();
    alice.auth(&mut service, b"alice").await;
    other.auth(&mut service, b"other").await;
    assert!(
        matches!(alice.status(&mut service, &key).await, send_outcome::Value::RejectedFinal(error)
        if error.code == Some(ErrorCode::InvalidArgument as i32))
    );
    let (outcome, _) = alice.send(&mut service, &key, "corrected text").await;
    assert_unresolved(outcome, ErrorCode::IdempotencyConflict, true);
    assert_eq!(
        committed(other.status(&mut service, &key).await),
        other_receipt
    );
    let cursor = alice
        .snapshot(&mut service, sync_target::Value::Conversation(1))
        .await
        .resume_cursor
        .unwrap();
    let result = other
        .sync(
            &mut service,
            sync_target::Value::Conversation(1),
            Some(cursor.to_vec()),
            None,
            1,
            256 * 1024,
        )
        .await;
    assert!(matches!(result, sync_response::Value::Error(error)
        if error.code == Some(ErrorCode::InvalidCursor as i32)));

    let day = 86_400_000;
    let old = message_key(1, client_id_days_ago(2 * day));
    assert!(
        matches!(alice.status(&mut service, &old).await, send_outcome::Value::RejectedFinal(error)
        if error.code == Some(ErrorCode::RequestExpired as i32))
    );
    let expired = message_key(1, client_id_days_ago(9 * day));
    assert_unresolved(
        alice.status(&mut service, &expired).await,
        ErrorCode::ResultExpired,
        true,
    );
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_protocol_serializes_same_key_across_instances() {
    let database = support::isolated_database().await;
    let store_id = generate_id128();
    let mut first =
        TidbService::connect_local(database.as_str(), store_id, seeded(), OpenMode::Initialize)
            .await
            .unwrap();
    let mut second =
        TidbService::connect_local(database.as_str(), store_id, identities(), OpenMode::Resume)
            .await
            .unwrap();
    let mut peer = Peer::new();
    let mut retry = Peer::new();
    retry.device = peer.device;
    peer.auth(&mut first, b"alice").await;
    retry.auth(&mut second, b"alice").await;
    let key = message_key(1, generate_id128());
    let ((first_outcome, _), (second_outcome, _)) = tokio::join!(
        peer.send(&mut first, &key, "concurrent"),
        retry.send(&mut second, &key, "concurrent"),
    );
    let receipt = committed(first_outcome);
    assert_eq!(committed(second_outcome), receipt);
    assert_eq!(receipt.position.unwrap().create_seq, Some(1));
    let snapshot = peer
        .snapshot(&mut first, sync_target::Value::Conversation(1))
        .await;
    assert_eq!(snapshot.upper_bound, Some(1));
    assert_eq!(snapshot.items.len(), 1);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_protocol_preserves_the_close_watermark_during_clock_rollback() {
    let database = support::isolated_database().await;
    let store_id = generate_id128();
    let mut service =
        TidbService::connect_local(database.as_str(), store_id, seeded(), OpenMode::Initialize)
            .await
            .unwrap();
    let mut alice = Peer::new();
    alice.auth(&mut service, b"alice").await;
    let key = message_key(1, generate_id128());
    let (original, _) = alice.send(&mut service, &key, "committed").await;
    let original = committed(original);
    drop(service);

    let pool = sqlx::MySqlPool::connect(database.as_str()).await.unwrap();
    let day = 86_400_000;
    let observed = id128_timestamp_ms(&generate_id128()) as i64 + 60_000;
    let watermark = observed - day;
    sqlx::query("UPDATE store_control SET max_observed_now_ms = ?, closed_through_client_ms = ? WHERE singleton = 1")
        .bind(observed)
        .bind(watermark)
        .execute(&pool)
        .await
        .unwrap();

    let mut service =
        TidbService::connect_local(database.as_str(), store_id, identities(), OpenMode::Resume)
            .await
            .unwrap();
    alice.auth(&mut service, b"alice").await;
    assert_eq!(committed(alice.status(&mut service, &key).await), original);
    let expired = message_key(1, client_id_days_ago(2 * day as u64));
    assert_unresolved(
        alice.status(&mut service, &expired).await,
        ErrorCode::ResultUnavailable,
        true,
    );
    let new_key = message_key(1, generate_id128());
    let (outcome, _) = alice.send(&mut service, &new_key, "blocked").await;
    assert_unresolved(outcome, ErrorCode::ResultUnavailable, false);
    let persisted: (i64, i64) = sqlx::query_as(
        "SELECT max_observed_now_ms, closed_through_client_ms FROM store_control WHERE singleton = 1",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(persisted, (observed, watermark));
    let snapshot = alice
        .snapshot(&mut service, sync_target::Value::Conversation(1))
        .await;
    assert_eq!(snapshot.upper_bound, Some(1));
}

struct Peer {
    cid: Option<Id128>,
    device: Id128,
    request: u64,
}

impl Peer {
    fn new() -> Self {
        Self {
            cid: None,
            device: generate_id128(),
            request: 0,
        }
    }

    async fn auth(&mut self, service: &mut TidbService, token: &[u8]) {
        service.disconnect(self.cid.take());
        self.request = 0;
        let (body, _) = self
            .call(
                service,
                MsgType::Auth,
                &AuthRequest {
                    access_token: Some(token.to_vec().into()),
                    device_id: Some(self.device.to_vec().into()),
                    requested_capabilities: vec![],
                },
            )
            .await;
        assert!(matches!(body, DecodedBody::AuthAck(response)
            if matches!(response.value, Some(auth_response::Value::Session(_)))));
        assert!(self.cid.is_some());
    }

    async fn send(
        &mut self,
        service: &mut TidbService,
        key: &MessageKey,
        text: &str,
    ) -> (send_outcome::Value, Pushes) {
        let (body, pushes) = self
            .call(
                service,
                MsgType::Send,
                &SendRequest {
                    key: Some(key.clone()),
                    content: Some(MessageContent {
                        value: Some(message_content::Value::Text(TextContent {
                            text: Some(text.to_owned()),
                        })),
                    }),
                },
            )
            .await;
        let DecodedBody::SendAck(response) = body else {
            panic!("expected SEND_ACK: {body:?}")
        };
        assert_eq!(response.key.as_ref(), Some(key));
        (response.outcome.unwrap().value.unwrap(), pushes)
    }

    async fn status(&mut self, service: &mut TidbService, key: &MessageKey) -> send_outcome::Value {
        let (body, _) = self
            .call(
                service,
                MsgType::SendStatus,
                &SendStatusRequest {
                    key: Some(key.clone()),
                },
            )
            .await;
        let DecodedBody::SendStatusAck(response) = body else {
            panic!("expected SEND_STATUS_ACK: {body:?}")
        };
        assert_eq!(response.key.as_ref(), Some(key));
        response.outcome.unwrap().value.unwrap()
    }

    async fn sync(
        &mut self,
        service: &mut TidbService,
        target: sync_target::Value,
        cursor: Option<Vec<u8>>,
        page: Option<Vec<u8>>,
        items: u32,
        bytes: u32,
    ) -> sync_response::Value {
        let (body, _) = self
            .call(
                service,
                MsgType::Sync,
                &SyncRequest {
                    target: Some(SyncTarget {
                        value: Some(target),
                    }),
                    cursor: cursor.map(Into::into),
                    page_token: page.map(Into::into),
                    limit: Some(PageLimit {
                        max_items: Some(items),
                        max_bytes: Some(bytes),
                    }),
                },
            )
            .await;
        let DecodedBody::SyncAck(response) = body else {
            panic!("expected SYNC_ACK: {body:?}")
        };
        response.value.unwrap()
    }

    async fn snapshot(
        &mut self,
        service: &mut TidbService,
        target: sync_target::Value,
    ) -> SnapshotPage {
        let reset = self
            .sync(service, target, None, None, 100, 256 * 1024)
            .await;
        let sync_response::Value::Reset(reset) = reset else {
            panic!("expected cold-start reset: {reset:?}")
        };
        let (body, _) = self
            .call(
                service,
                MsgType::Snapshot,
                &SnapshotRequest {
                    page_token: reset.snapshot_token,
                    limit: Some(PageLimit {
                        max_items: Some(100),
                        max_bytes: Some(256 * 1024),
                    }),
                },
            )
            .await;
        let DecodedBody::SnapshotAck(response) = body else {
            panic!("expected SNAPSHOT_ACK: {body:?}")
        };
        let Some(snapshot_response::Value::Page(page)) = response.value else {
            panic!("expected snapshot page: {response:?}")
        };
        page
    }

    async fn call(
        &mut self,
        service: &mut TidbService,
        kind: MsgType,
        body: &impl Message,
    ) -> (DecodedBody, Pushes) {
        self.request += 1;
        let head = Head {
            req_id: Some(self.request),
        };
        let bytes = encode_frame(kind, &head, body).unwrap();
        let (cid, output) = service
            .handle_frame(self.cid, &frame(&bytes))
            .await
            .unwrap();
        self.cid = cid;
        let [response] = output.responses.as_slice() else {
            panic!("expected exactly one response")
        };
        let decoded = decoded(response);
        assert_eq!(decoded.head, head);
        (decoded.body, output.pushes)
    }
}

fn frame(bytes: &[u8]) -> Frame {
    let mut decoder = FrameDecoder::new();
    decoder.push(bytes);
    let frame = decoder.next_frame().unwrap().unwrap();
    decoder.finish().unwrap();
    frame
}

fn decoded(bytes: &[u8]) -> DecodedFrame {
    decode_frame(&frame(bytes)).unwrap()
}

fn identities() -> Engine {
    let mut engine = Engine::new(0);
    engine.store.seed_user(1, 100, b"alice".to_vec());
    engine.store.seed_user(1, 200, b"bob".to_vec());
    engine.store.seed_user(2, 100, b"other".to_vec());
    engine
}

fn seeded() -> Engine {
    let mut engine = identities();
    engine.store.seed_direct(1, 1, 100, 200);
    engine.store.seed_direct(2, 1, 100, 200);
    engine
}

fn message_key(conversation: u64, id: Id128) -> MessageKey {
    MessageKey {
        conversation_id: Some(conversation),
        client_msg_id: Some(id.to_vec().into()),
    }
}

fn committed(value: send_outcome::Value) -> SendReceipt {
    let send_outcome::Value::Committed(receipt) = value else {
        panic!("expected committed: {value:?}")
    };
    receipt
}

fn assert_unresolved(value: send_outcome::Value, code: ErrorCode, closed: bool) {
    let send_outcome::Value::Unresolved(result) = value else {
        panic!("expected unresolved: {value:?}")
    };
    assert_eq!(result.error.unwrap().code, Some(code as i32));
    assert_eq!(result.acceptance_closed, Some(closed));
}

fn sync_page(value: sync_response::Value) -> SyncPage {
    let sync_response::Value::Page(page) = value else {
        panic!("expected sync page: {value:?}")
    };
    page
}

fn assert_change(page: &SyncPage, receipt: &SendReceipt) {
    let [event] = page.changes.as_slice() else {
        panic!("expected one change: {page:?}")
    };
    let Some(change::Payload::MessageUpsert(view)) = &event.payload else {
        panic!("expected message upsert")
    };
    assert_eq!(view.r#ref, receipt.r#ref);
    assert_eq!(view.position, receipt.position);
    assert_eq!(view.accepted_at_ms, receipt.accepted_at_ms);
    assert_eq!(page.covered_through, event.event_seq);
}

fn assert_push(pushes: &Pushes, peer: &Peer, receipt: &SendReceipt) {
    let [(cid, bytes)] = pushes.as_slice() else {
        panic!("expected one push")
    };
    assert_eq!(Some(*cid), peer.cid);
    let DecodedBody::Push(push) = decoded(bytes).body else {
        panic!("expected push frame")
    };
    let [event] = push.changes.as_slice() else {
        panic!("expected one push change")
    };
    let Some(change::Payload::MessageUpsert(view)) = &event.payload else {
        panic!("expected message upsert")
    };
    assert_eq!(view.r#ref, receipt.r#ref);
}

fn client_id_days_ago(milliseconds: u64) -> Id128 {
    let mut id = generate_id128();
    let time = id128_timestamp_ms(&id) - milliseconds;
    id[..6].copy_from_slice(&time.to_be_bytes()[2..]);
    id
}
