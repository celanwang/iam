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
async fn tidb_protocol_restarts_and_syncs_discarded_pushes() {
    let database = support::isolated_database().await;
    let store_id = generate_id128();
    let service =
        TidbService::connect_local(database.as_str(), store_id, seeded(), OpenMode::Initialize)
            .await
            .unwrap();
    let mut alice = Peer::new();
    let mut bob = Peer::new();
    alice.auth(&service, b"alice").await;
    bob.auth(&service, b"bob").await;
    let account = sync_target::Value::Account(Empty {});
    let conversation = sync_target::Value::Conversation(1);
    let account_page = bob.snapshot(&service, account).await;
    assert!(matches!(
        account_page.items.as_slice(),
        [item] if matches!(&item.value, Some(snapshot_item::Value::Conversation(c)) if c.conversation_id == Some(1))
    ));
    let account_cursor = account_page.resume_cursor.unwrap().to_vec();
    let baseline = bob.snapshot(&service, conversation).await;
    assert!(baseline.items.is_empty());
    let cursor = baseline.resume_cursor.unwrap().to_vec();

    let first_key = message_key(1, generate_id128());
    let (first, pushes) = alice.send(&service, &first_key, "first").await;
    let first = committed(first);
    assert_push(&pushes, &bob, &first);
    drop(pushes);
    let second_key = message_key(1, generate_id128());
    let (second, pushes) = alice.send(&service, &second_key, "second").await;
    let second = committed(second);
    assert_push(&pushes, &bob, &second);
    drop(pushes);

    let too_small = bob
        .sync(&service, conversation, Some(cursor.clone()), None, 1, 1)
        .await;
    assert!(matches!(too_small, sync_response::Value::Error(error)
        if error.code == Some(ErrorCode::PageBudgetTooSmall as i32)));
    let page = sync_page(
        bob.sync(&service, conversation, Some(cursor), None, 1, 256 * 1024)
            .await,
    );
    assert_change(&page, &first);
    assert_eq!(page.upper_bound, Some(2));
    let next_page = page.next_page_token.unwrap().to_vec();
    drop(service);

    // Recreate the engine with identity seeds only. Conversation data and tokens come from TiDB.
    let service =
        TidbService::connect_local(database.as_str(), store_id, identities(), OpenMode::Resume)
            .await
            .unwrap();
    alice.auth(&service, b"alice").await;
    bob.auth(&service, b"bob").await;
    assert_eq!(committed(alice.status(&service, &first_key).await), first);
    let (retry, pushes) = alice.send(&service, &first_key, "first").await;
    assert_eq!(committed(retry), first);
    assert!(pushes.is_empty());

    let (third, pushes) = alice
        .send(&service, &message_key(1, generate_id128()), "third")
        .await;
    let third = committed(third);
    assert_push(&pushes, &bob, &third);
    drop(pushes);
    let page = sync_page(
        bob.sync(&service, conversation, None, Some(next_page), 1, 256 * 1024)
            .await,
    );
    assert_change(&page, &second);
    assert_eq!(page.upper_bound, Some(2));
    assert!(page.next_page_token.is_none());
    let page = sync_page(
        bob.sync(
            &service,
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
            &service,
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
    let snapshot = bob.snapshot(&service, conversation).await;
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
    let service =
        TidbService::connect_local(proxy.url.as_str(), store_id, seeded(), OpenMode::Initialize)
            .await
            .unwrap();
    let mut alice = Peer::new();
    alice.auth(&service, b"alice").await;
    let key = message_key(1, generate_id128());
    let (reached, disconnect) = proxy.arm(phase);
    let (outcome, pushes) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let mut send = std::pin::pin!(alice.send(&service, &key, "unknown commit"));
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

    let service =
        TidbService::connect_local(database.as_str(), store_id, identities(), OpenMode::Resume)
            .await
            .unwrap();
    alice.auth(&service, b"alice").await;
    let status = alice.status(&service, &key).await;
    let original = match phase {
        CommitPhase::After => Some(committed(status)),
        CommitPhase::Before => {
            assert_unresolved(status, ErrorCode::NotFound, false);
            None
        }
    };
    let (retried, _) = alice.send(&service, &key, "unknown commit").await;
    let receipt = committed(retried);
    if let Some(original) = original {
        assert_eq!(receipt, original);
    }
    assert_eq!(receipt.position.unwrap().create_seq, Some(1));
    let snapshot = alice
        .snapshot(&service, sync_target::Value::Conversation(1))
        .await;
    assert_eq!(snapshot.upper_bound, Some(1));
    assert_eq!(snapshot.items.len(), 1);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_protocol_replays_rejections_and_enforces_scopes_and_windows() {
    let database = support::isolated_database().await;
    let store_id = generate_id128();
    let service =
        TidbService::connect_local(database.as_str(), store_id, seeded(), OpenMode::Initialize)
            .await
            .unwrap();
    let mut alice = Peer::new();
    let mut other = Peer::new();
    alice.auth(&service, b"alice").await;
    other.auth(&service, b"other").await;
    let key = message_key(1, generate_id128());
    let (outcome, _) = alice.send(&service, &key, "").await;
    assert!(matches!(outcome, send_outcome::Value::RejectedFinal(error)
        if error.code == Some(ErrorCode::InvalidArgument as i32)));
    // Even an identical user/device/key in another tenant has independent state.
    other.device = alice.device;
    other.auth(&service, b"other").await;
    assert_unresolved(
        other.status(&service, &key).await,
        ErrorCode::NotFound,
        false,
    );
    let (outcome, _) = other.send(&service, &key, "different tenant").await;
    let other_receipt = committed(outcome);
    let snapshot = alice
        .snapshot(&service, sync_target::Value::Conversation(1))
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
    let service =
        TidbService::connect_local(database.as_str(), store_id, identities(), OpenMode::Resume)
            .await
            .unwrap();
    alice.auth(&service, b"alice").await;
    other.auth(&service, b"other").await;
    assert!(
        matches!(alice.status(&service, &key).await, send_outcome::Value::RejectedFinal(error)
        if error.code == Some(ErrorCode::InvalidArgument as i32))
    );
    let (outcome, _) = alice.send(&service, &key, "corrected text").await;
    assert_unresolved(outcome, ErrorCode::IdempotencyConflict, true);
    assert_eq!(committed(other.status(&service, &key).await), other_receipt);
    let cursor = alice
        .snapshot(&service, sync_target::Value::Conversation(1))
        .await
        .resume_cursor
        .unwrap();
    let result = other
        .sync(
            &service,
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
        matches!(alice.status(&service, &old).await, send_outcome::Value::RejectedFinal(error)
        if error.code == Some(ErrorCode::RequestExpired as i32))
    );
    let expired = message_key(1, client_id_days_ago(9 * day));
    assert_unresolved(
        alice.status(&service, &expired).await,
        ErrorCode::ResultExpired,
        true,
    );
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_protocol_serializes_same_key_across_instances() {
    let database = support::isolated_database().await;
    let store_id = generate_id128();
    let first =
        TidbService::connect_local(database.as_str(), store_id, seeded(), OpenMode::Initialize)
            .await
            .unwrap();
    let second =
        TidbService::connect_local(database.as_str(), store_id, identities(), OpenMode::Resume)
            .await
            .unwrap();
    let mut peer = Peer::new();
    let mut retry = Peer::new();
    retry.device = peer.device;
    peer.auth(&first, b"alice").await;
    retry.auth(&second, b"alice").await;
    let key = message_key(1, generate_id128());
    let ((first_outcome, _), (second_outcome, _)) = tokio::join!(
        peer.send(&first, &key, "concurrent"),
        retry.send(&second, &key, "concurrent"),
    );
    let receipt = committed(first_outcome);
    assert_eq!(committed(second_outcome), receipt);
    assert_eq!(receipt.position.unwrap().create_seq, Some(1));
    let snapshot = peer
        .snapshot(&first, sync_target::Value::Conversation(1))
        .await;
    assert_eq!(snapshot.upper_bound, Some(1));
    assert_eq!(snapshot.items.len(), 1);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_protocol_preserves_the_close_watermark_during_clock_rollback() {
    let database = support::isolated_database().await;
    let store_id = generate_id128();
    let service =
        TidbService::connect_local(database.as_str(), store_id, seeded(), OpenMode::Initialize)
            .await
            .unwrap();
    let mut alice = Peer::new();
    alice.auth(&service, b"alice").await;
    let key = message_key(1, generate_id128());
    let (original, _) = alice.send(&service, &key, "committed").await;
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

    let service =
        TidbService::connect_local(database.as_str(), store_id, identities(), OpenMode::Resume)
            .await
            .unwrap();
    alice.auth(&service, b"alice").await;
    assert_eq!(committed(alice.status(&service, &key).await), original);
    let expired = message_key(1, client_id_days_ago(2 * day as u64));
    assert_unresolved(
        alice.status(&service, &expired).await,
        ErrorCode::ResultUnavailable,
        true,
    );
    let new_key = message_key(1, generate_id128());
    let (outcome, _) = alice.send(&service, &new_key, "blocked").await;
    assert_unresolved(outcome, ErrorCode::ResultUnavailable, false);
    let persisted: (i64, i64) = sqlx::query_as(
        "SELECT max_observed_now_ms, closed_through_client_ms FROM store_control WHERE singleton = 1",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(persisted, (observed, watermark));
    let snapshot = alice
        .snapshot(&service, sync_target::Value::Conversation(1))
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

    async fn auth(&mut self, service: &TidbService, token: &[u8]) {
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
        service: &TidbService,
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

    async fn status(&mut self, service: &TidbService, key: &MessageKey) -> send_outcome::Value {
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
        service: &TidbService,
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
        service: &TidbService,
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
        service: &TidbService,
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

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_clock_batch_rejects_expired_tickets_during_clock_rollback() {
    use iam_protocol::pb::{reconnect_response, ReconnectRequest, ResetReason};

    let database = support::isolated_database().await;
    let service = TidbService::connect_local(
        database.as_str(),
        generate_id128(),
        seeded(),
        OpenMode::Initialize,
    )
    .await
    .unwrap();
    let mut alice = Peer::new();
    let mut bob = Peer::new();
    let auth = |device: Id128, token: &'static [u8]| AuthRequest {
        access_token: Some(token.into()),
        device_id: Some(device.to_vec().into()),
        requested_capabilities: vec![],
    };
    let (reply, _) = alice
        .call(&service, MsgType::Auth, &auth(alice.device, b"alice"))
        .await;
    let DecodedBody::AuthAck(ack) = reply else {
        panic!("AUTH_ACK");
    };
    let Some(auth_response::Value::Session(alice_session)) = ack.value else {
        panic!("session");
    };
    bob.auth(&service, b"bob").await;

    let target = sync_target::Value::Conversation(1);
    let old_cursor = bob
        .snapshot(&service, target)
        .await
        .resume_cursor
        .unwrap()
        .to_vec();
    let key = message_key(1, generate_id128());
    let (sent, _) = alice.send(&service, &key, "before rollback").await;
    let sent = committed(sent);
    let normal = sync_page(
        bob.sync(
            &service,
            target,
            Some(old_cursor.clone()),
            None,
            100,
            256 * 1024,
        )
        .await,
    );
    assert_change(&normal, &sent);

    let db = sqlx::MySqlPool::connect(database.as_str()).await.unwrap();
    let (observed,): (i64,) = sqlx::query_as("SELECT max_observed_now_ms FROM store_control")
        .fetch_one(&db)
        .await
        .unwrap();
    let (token_key,): (Vec<u8>,) = sqlx::query_as("SELECT token_key FROM store_control")
        .fetch_one(&db)
        .await
        .unwrap();
    let signer = iam_core::TokenSigner::new(token_key.try_into().unwrap());
    let cursor = signer.verify_cursor(&old_cursor, observed).unwrap();
    let day = 86_400_000;
    let deadline = cursor.issued_at_ms + 30 * day;
    sqlx::query("UPDATE store_control SET max_observed_now_ms = ?, closed_through_client_ms = ? WHERE singleton = 1")
        .bind(deadline).bind(deadline - day).execute(&db).await.unwrap();

    let mut reconnecting = Peer::new();
    reconnecting.device = alice.device;
    let reconnect = ReconnectRequest {
        device_id: Some(alice.device.to_vec().into()),
        resume_token: alice_session.resume_token,
        previous_connection_id: alice_session.connection_id,
        requested_capabilities: vec![],
    };
    let mut authenticating = Peer::new();
    let auth_request = auth(authenticating.device, b"bob");
    let (sync, (resume, _), (auth, _)) = tokio::join!(
        bob.sync(&service, target, Some(old_cursor), None, 100, 256 * 1024),
        reconnecting.call(&service, MsgType::Reconnect, &reconnect),
        authenticating.call(&service, MsgType::Auth, &auth_request),
    );
    assert!(
        matches!(sync, sync_response::Value::Reset(reset) if reset.reason() == ResetReason::CursorExpired)
    );
    assert!(matches!(resume, DecodedBody::ReconnectAck(ack)
        if matches!(&ack.value, Some(reconnect_response::Value::Error(error))
            if error.code() == ErrorCode::Unauthenticated)));
    let DecodedBody::AuthAck(ack) = auth else {
        panic!("expected AUTH_ACK");
    };
    let Some(auth_response::Value::Session(fresh)) = ack.value else {
        panic!("expected a session from the new clock batch");
    };
    assert_eq!(fresh.server_time_ms, Some(deadline));
    let mut fresh_peer = Peer::new();
    fresh_peer.device = authenticating.device;
    let (reply, _) = fresh_peer
        .call(
            &service,
            MsgType::Reconnect,
            &ReconnectRequest {
                device_id: fresh.device_id,
                resume_token: fresh.resume_token,
                previous_connection_id: fresh.connection_id,
                requested_capabilities: vec![],
            },
        )
        .await;
    assert!(matches!(reply, DecodedBody::ReconnectAck(ack)
        if matches!(&ack.value, Some(reconnect_response::Value::Session(session))
            if session.server_time_ms == Some(deadline))));
    let actual: (i64, i64) =
        sqlx::query_as("SELECT max_observed_now_ms, closed_through_client_ms FROM store_control")
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(actual, (deadline, deadline - day));
    let (outcome, pushes) = alice
        .send(&service, &message_key(1, generate_id128()), "blocked")
        .await;
    assert_unresolved(outcome, ErrorCode::ResultExpired, true);
    assert!(pushes.is_empty());
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_clock_batch_retries_when_its_commit_response_is_lost() {
    use iam_protocol::pb::RetryAction;
    use std::time::Duration;

    let database = support::isolated_database().await;
    let proxy = CommitProxy::start(&database).await;
    let service = TidbService::connect_local(
        proxy.url.as_str(),
        generate_id128(),
        seeded(),
        OpenMode::Initialize,
    )
    .await
    .unwrap();
    let mut bob = Peer::new();
    bob.auth(&service, b"bob").await;
    let target = sync_target::Value::Conversation(1);
    let cursor = bob.snapshot(&service, target).await.resume_cursor.unwrap();
    let sync = SyncRequest {
        target: Some(SyncTarget {
            value: Some(target),
        }),
        cursor: Some(cursor),
        page_token: None,
        limit: Some(PageLimit {
            max_items: Some(100),
            max_bytes: Some(256 * 1024),
        }),
    };
    let mut alice = Peer::new();
    let auth = AuthRequest {
        device_id: Some(alice.device.to_vec().into()),
        access_token: Some(b"alice".as_slice().into()),
        requested_capabilities: vec![],
    };
    let (reached, drop_reply) = proxy.arm(CommitPhase::After);
    let ((auth_error, _), (sync_error, _)) = tokio::time::timeout(Duration::from_secs(10), async {
        let (auth, sync, ()) = tokio::join!(
            alice.call(&service, MsgType::Auth, &auth),
            bob.call(&service, MsgType::Sync, &sync),
            async {
                reached.await.unwrap().unwrap();
                drop_reply.send(()).unwrap();
            },
        );
        (auth, sync)
    })
    .await
    .unwrap();
    for response in [auth_error, sync_error] {
        let DecodedBody::Error(error) = response else {
            panic!("expected a retryable error");
        };
        let info = error.error.unwrap();
        assert_eq!(info.code(), ErrorCode::InternalError);
        assert_eq!(info.action(), RetryAction::RetrySameRequest);
        assert_eq!(error.close_connection, Some(false));
    }
    assert!(alice.cid.is_none());
    alice.auth(&service, b"alice").await;
    let page = sync_page(
        bob.sync(
            &service,
            target,
            sync.cursor.map(|c| c.to_vec()),
            None,
            100,
            256 * 1024,
        )
        .await,
    );
    assert!(page.changes.is_empty());
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_clock_batch_and_status_wait_for_an_unsettled_send_at_close() {
    use std::time::Duration;

    let db = support::isolated_database().await;
    let store_id = generate_id128();
    let proxy = CommitProxy::start(&db).await;
    let writer =
        TidbService::connect_local(proxy.url.as_str(), store_id, seeded(), OpenMode::Initialize)
            .await
            .unwrap();
    let reader = TidbService::connect_local(db.as_str(), store_id, identities(), OpenMode::Resume)
        .await
        .unwrap();
    let mut alice = Peer::new();
    let mut status_peer = Peer::new();
    status_peer.device = alice.device;
    alice.auth(&writer, b"alice").await;
    status_peer.auth(&reader, b"alice").await;

    let pool = sqlx::MySqlPool::connect(db.as_str()).await.unwrap();
    let db_now = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED)",
        )
        .fetch_one(&pool)
        .await
        .unwrap()
    };
    let day = 86_400_000_i64;
    let closes = db_now().await + 1_500;
    let stamp = closes - day;
    let mut id = generate_id128();
    id[..6].copy_from_slice(&(stamp as u64).to_be_bytes()[2..]);
    let key = message_key(1, id);

    let (reached, rollback) = proxy.arm(CommitPhase::Before);
    let mut send = Box::pin(alice.send(&writer, &key, "unsettled at close"));
    tokio::select! {
        result = reached => result.unwrap().unwrap(),
        result = &mut send => panic!("send bypassed COMMIT pause: {result:?}"),
    }
    assert!(db_now().await < closes);
    let mut status = Box::pin(status_peer.status(&reader, &key));
    let (mut one, mut two) = (Peer::new(), Peer::new());
    let mut auth = Box::pin(async {
        tokio::join!(one.auth(&reader, b"bob"), two.auth(&reader, b"bob"));
    });
    tokio::select! {
        _ = &mut auth => panic!("clock batch returned while SEND held the control row"),
        _ = &mut status => panic!("SEND_STATUS resolved before the original transaction settled"),
        _ = tokio::time::timeout(Duration::from_secs(5), async {
            while db_now().await < closes {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }) => {}
    }
    assert!(db_now().await >= closes);
    let watermark: i64 = sqlx::query_scalar("SELECT closed_through_client_ms FROM store_control")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        watermark < stamp,
        "watermark advanced while an older SEND could still commit"
    );
    rollback.send(()).unwrap();
    let ((outcome, pushes), status, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(send, status, auth)
    })
    .await
    .unwrap();
    assert_unresolved(outcome, ErrorCode::ResultUnavailable, false);
    assert!(pushes.is_empty());
    assert!(matches!(status, send_outcome::Value::RejectedFinal(error)
        if error.code() == ErrorCode::RequestExpired));
    let watermark: i64 = sqlx::query_scalar("SELECT closed_through_client_ms FROM store_control")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(watermark >= stamp);
    let (retry, pushes) = alice.send(&writer, &key, "unsettled at close").await;
    assert!(matches!(retry, send_outcome::Value::RejectedFinal(error)
        if error.code() == ErrorCode::RequestExpired));
    assert!(pushes.is_empty());
}
