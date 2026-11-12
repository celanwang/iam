//! Ignored tests run only on a real local TiDB. They never silently skip or substitute MySQL.
use super::*;
use crate::service::{OpenMode, TidbTextService};
use crate::store::{local_options, Append, TidbStore};
use iam_core::Engine;
use iam_protocol::pb::{AuthRequest, Head};
use iam_protocol::{generate_id128, DecodedBody, DecodedFrame, MsgType};

#[test]
fn configuration_rejects_nonlocal_and_unspecified_databases() {
    assert!(local_options("mysql://root@127.0.0.1:4000/iam_local").is_ok());
    assert!(local_options("mysql://root@db.example:4000/iam_local").is_err());
    assert!(local_options("mysql://root@127.0.0.1:4000").is_err());
    assert!(local_options("mysql://root@localhost/iam_local?socket=/tmp/mysql.sock").is_err());
}

async fn empty_database() -> (String, TidbStore) {
    assert_eq!(
        std::env::var("IAM_TIDB_ALLOW_TEST_WRITES").as_deref(),
        Ok("1"),
        "set IAM_TIDB_ALLOW_TEST_WRITES=1 for an isolated local test database"
    );
    let url = std::env::var("IAM_TIDB_TEST_URL").expect("IAM_TIDB_TEST_URL is required");
    let options = local_options(&url).expect("local test endpoint required");
    assert!(
        options.get_database().unwrap().starts_with("iam_test"),
        "test database name must start with iam_test"
    );
    let parent = TidbStore::connect(&url)
        .await
        .expect("real TiDB required (tidb_version must succeed)");
    // Every case gets a new catalog; never DROP/TRUNCATE or reuse someone else's rows.
    let database = format!("iam_test_{}", hex::encode(generate_id128()));
    sqlx::query(&format!("CREATE DATABASE `{database}`"))
        .execute(&parent.pool)
        .await
        .unwrap();
    let (base, query) = url.split_once('?').unwrap_or((&url, ""));
    let prefix = base.rsplit_once('/').unwrap().0;
    let isolated = if query.is_empty() {
        format!("{prefix}/{database}")
    } else {
        format!("{prefix}/{database}?{query}")
    };
    let store = TidbStore::connect(&isolated).await.unwrap();
    (isolated, store)
}

async fn fixture() -> (String, TidbStore) {
    let (url, store) = empty_database().await;
    store.install_schema().await.unwrap();
    (url, store)
}

fn identity(tenant_id: u64) -> Identity {
    Identity {
        tenant_id,
        user_id: 100,
        device_id: generate_id128(),
    }
}

fn client_at(now: i64) -> Id128 {
    let mut id = generate_id128();
    id[..6].copy_from_slice(&(now as u64).to_be_bytes()[2..]);
    id
}

async fn append(
    store: &TidbStore,
    identity: Identity,
    client: Id128,
    text: &str,
) -> Result<crate::store::Appended> {
    let mut observed_now = 0;
    store
        .append(Append {
            identity,
            conversation_id: 1,
            client_msg_id: client,
            text,
            message_id: generate_id128(),
            max_observed_now: &mut observed_now,
        })
        .await
}

fn engine() -> Engine {
    let mut engine = Engine::new(0);
    for (tenant, token) in [(1, b"one".to_vec()), (2, b"two".to_vec())] {
        engine.store.seed_user(tenant, 100, token);
        engine.store.seed_direct(tenant, 1, 100, 200);
    }
    engine.store.seed_user(1, 200, b"other".to_vec());
    engine
}

async fn auth(service: &mut TidbTextService, token: &[u8], device: Id128) -> Id128 {
    let frame = DecodedFrame {
        msg_type: MsgType::Auth,
        head: Head { req_id: Some(1) },
        body: DecodedBody::Auth(AuthRequest {
            access_token: Some(token.to_vec().into()),
            device_id: Some(device.to_vec().into()),
            requested_capabilities: vec![],
        }),
    };
    service
        .handle_session(None, &frame)
        .await
        .unwrap()
        .0
        .unwrap()
}

#[tokio::test]
#[ignore = "requires real local TiDB; see README validation instructions"]
async fn tidb_initialization_is_idempotent_and_schema_has_only_two_tables() {
    let (_, store) = fixture().await;
    let scope = Identity {
        user_id: u64::MAX,
        ..identity(u64::MAX)
    };
    let first = store.initialize(scope, u64::MAX).await.unwrap();
    assert!(first.created);
    let again = store.initialize(scope, u64::MAX).await.unwrap();
    assert!(!again.created);
    assert_eq!(first.history_id, again.history_id);
    assert_eq!(again.next_sequence, 1);
    let key = client_at(store.now().await.unwrap());
    let mut observed_now = 0;
    let saved = store
        .append(Append {
            identity: scope,
            conversation_id: u64::MAX,
            client_msg_id: key,
            text: "unsigned identities",
            message_id: generate_id128(),
            max_observed_now: &mut observed_now,
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .lookup(scope, u64::MAX, key)
            .await
            .unwrap()
            .unwrap()
            .user_id,
        u64::MAX
    );
    assert_eq!(
        saved.message.receipt.r#ref.unwrap().conversation_id,
        Some(u64::MAX)
    );
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY table_name",
    ).fetch_all(&store.pool).await.unwrap();
    assert_eq!(tables, vec!["conversations", "messages"]);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_insert_failure_rolls_back_sequence_and_message_together() {
    let (_, store) = fixture().await;
    let scope = identity(1);
    let initial = store.initialize(scope, 1).await.unwrap();
    let now = store.now().await.unwrap();
    let first = append(&store, scope, client_at(now), "first")
        .await
        .unwrap();
    let duplicate_id = validate_id128(
        first
            .message
            .receipt
            .r#ref
            .as_ref()
            .unwrap()
            .message_id
            .as_ref()
            .unwrap(),
    )
    .unwrap();
    let failed_key = client_at(now);
    let mut observed_now = now;
    let failed = store
        .append(Append {
            identity: scope,
            conversation_id: 1,
            client_msg_id: failed_key,
            text: "must rollback",
            message_id: duplicate_id,
            max_observed_now: &mut observed_now,
        })
        .await;
    assert!(matches!(failed, Err(Error::Database(_))));
    assert!(store.lookup(scope, 1, failed_key).await.unwrap().is_none());
    let unchanged = store.initialize(scope, 1).await.unwrap();
    assert_eq!(unchanged.history_id, initial.history_id);
    assert_eq!(unchanged.next_sequence, 2);
    let next = append(&store, scope, failed_key, "next").await.unwrap();
    assert_eq!(next.message.receipt.position.unwrap().create_seq, Some(2));
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_concurrent_sends_have_unique_order_and_concurrent_retries_share_receipt() {
    let (_, store) = fixture().await;
    let scope = identity(1);
    store.initialize(scope, 1).await.unwrap();
    let now = store.now().await.unwrap();
    let mut jobs = Vec::new();
    for _ in 0..24 {
        let store = store.clone();
        let key = client_at(now);
        jobs.push(tokio::spawn(async move {
            append(&store, scope, key, "concurrent").await.unwrap()
        }));
    }
    let mut positions = Vec::new();
    for job in jobs {
        positions.push(
            job.await
                .unwrap()
                .message
                .receipt
                .position
                .unwrap()
                .create_seq
                .unwrap(),
        );
    }
    positions.sort_unstable();
    assert_eq!(positions, (1..=24).collect::<Vec<_>>());
    let key = client_at(now);
    let mut jobs = Vec::new();
    for _ in 0..12 {
        let store = store.clone();
        jobs.push(tokio::spawn(async move {
            append(&store, scope, key, "same").await.unwrap()
        }));
    }
    let mut receipts = Vec::new();
    let mut inserted = 0;
    for job in jobs {
        let result = job.await.unwrap();
        inserted += usize::from(!result.replayed);
        receipts.push(result.message.receipt);
    }
    assert_eq!(inserted, 1);
    assert!(receipts.iter().all(|receipt| receipt == &receipts[0]));
    assert_eq!(store.initialize(scope, 1).await.unwrap().next_sequence, 26);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_content_comparison_preserves_case_unicode_and_full_text_limit() {
    let (_, store) = fixture().await;
    let scope = identity(1);
    store.initialize(scope, 1).await.unwrap();
    let now = store.now().await.unwrap();
    let key = client_at(now);
    let original = append(&store, scope, key, "Text é").await.unwrap();
    assert_eq!(
        append(&store, scope, key, "Text é")
            .await
            .unwrap()
            .message
            .receipt,
        original.message.receipt
    );
    for changed in ["text é", "Text e\u{301}", "", "Text é "] {
        assert!(matches!(
            append(&store, scope, key, changed).await,
            Err(Error::ContentConflict)
        ));
    }
    let maximum = "x".repeat(65_536);
    let boundary_key = client_at(now);
    append(&store, scope, boundary_key, &maximum).await.unwrap();
    assert_eq!(
        store
            .lookup(scope, 1, boundary_key)
            .await
            .unwrap()
            .unwrap()
            .text,
        maximum
    );
    assert!(matches!(
        append(&store, scope, client_at(now), &"x".repeat(65_537)).await,
        Err(Error::InvalidRequest(_))
    ));
    assert!(matches!(
        append(&store, scope, client_at(now - 86_400_000), "late").await,
        Err(Error::AcceptanceExpired)
    ));
    assert!(matches!(
        append(&store, scope, client_at(now + 180_000), "future").await,
        Err(Error::ClockSkew)
    ));
    assert_eq!(store.initialize(scope, 1).await.unwrap().next_sequence, 3);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_authenticated_path_isolates_tenants_users_and_devices_and_pages_in_order() {
    let (url, store) = fixture().await;
    let mut service = TidbTextService::connect_local(&url, engine(), OpenMode::FreshDatabase)
        .await
        .unwrap();
    let device = generate_id128();
    let a = auth(&mut service, b"one", device).await;
    let b = auth(&mut service, b"two", device).await;
    let other = auth(&mut service, b"other", generate_id128()).await;
    let other_device = auth(&mut service, b"one", generate_id128()).await;
    let wire_send = DecodedFrame {
        msg_type: MsgType::Send,
        head: Head { req_id: Some(2) },
        body: DecodedBody::Send(iam_protocol::pb::SendRequest::default()),
    };
    assert!(matches!(
        service.handle_session(Some(a), &wire_send).await,
        Err(Error::InvalidRequest(_))
    ));
    let one = service.initialize_conversation(a, 1).await.unwrap();
    let two = service.initialize_conversation(b, 1).await.unwrap();
    assert_ne!(one.history_id, two.history_id);
    let key = client_at(store.now().await.unwrap());
    let saved = service.send_text(a, 1, key, "tenant one").await.unwrap();
    let saved_two = service.send_text(b, 1, key, "tenant two").await.unwrap();
    assert_eq!(saved.receipt.position.as_ref().unwrap().create_seq, Some(1));
    assert_eq!(
        saved_two.receipt.position.as_ref().unwrap().create_seq,
        Some(1)
    );
    assert!(matches!(
        service.query_send(other, 1, key).await.unwrap(),
        SendLookup::Unknown
    ));
    assert!(matches!(
        service.query_send(other_device, 1, key).await.unwrap(),
        SendLookup::Unknown
    ));
    for text in ["second", "third", "fourth"] {
        service
            .send_text(a, 1, client_at(store.now().await.unwrap()), text)
            .await
            .unwrap();
    }
    let first = service.read_messages(other, 1, None, 2).await.unwrap();
    assert_eq!(
        first
            .messages
            .iter()
            .map(|m| m.position.as_ref().unwrap().create_seq.unwrap())
            .collect::<Vec<_>>(),
        vec![4, 3]
    );
    service
        .send_text(a, 1, client_at(store.now().await.unwrap()), "later")
        .await
        .unwrap();
    let second = service
        .read_messages(other, 1, first.next_before, 2)
        .await
        .unwrap();
    assert_eq!(
        second
            .messages
            .iter()
            .map(|m| m.position.as_ref().unwrap().create_seq.unwrap())
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert!(second.next_before.is_none());
    let tenant_two = service.read_messages(b, 1, None, 100).await.unwrap();
    assert_eq!(tenant_two.messages.len(), 1);
    assert_eq!(tenant_two.history_id, two.history_id);
    let user_send = service
        .send_text(other, 1, key, "other user")
        .await
        .unwrap();
    let device_send = service
        .send_text(other_device, 1, key, "other device")
        .await
        .unwrap();
    assert_ne!(user_send.receipt, saved.receipt);
    assert_ne!(device_send.receipt, saved.receipt);
    assert_ne!(user_send.receipt, device_send.receipt);
    for (connection, expected) in [
        (a, &saved.receipt),
        (other, &user_send.receipt),
        (other_device, &device_send.receipt),
    ] {
        match service.query_send(connection, 1, key).await.unwrap() {
            SendLookup::Found(receipt) => assert_eq!(&receipt, expected),
            SendLookup::Unknown => panic!("successful result disappeared"),
        }
    }
    assert!(matches!(
        service.read_messages(a, 999, None, 1).await,
        Err(Error::PermissionDenied)
    ));
    assert!(matches!(
        service.read_messages(a, 1, None, 0).await,
        Err(Error::InvalidRequest(_))
    ));
    service.disconnect(Some(a));
    assert!(matches!(
        service.query_send(a, 1, key).await,
        Err(Error::Unauthenticated)
    ));
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_restart_replays_success_but_never_reopens_unknown_sends() {
    let (url, store) = fixture().await;
    let device = generate_id128();
    let key = client_at(store.now().await.unwrap());
    let mut first = TidbTextService::connect_local(&url, engine(), OpenMode::FreshDatabase)
        .await
        .unwrap();
    let connection = auth(&mut first, b"one", device).await;
    let initial = first.initialize_conversation(connection, 1).await.unwrap();
    let saved = first
        .send_text(connection, 1, key, "retained")
        .await
        .unwrap();
    first.stop_accepting();
    assert!(
        first
            .send_text(connection, 1, key, "retained")
            .await
            .unwrap()
            .replayed
    );
    assert!(matches!(
        first
            .send_text(
                connection,
                1,
                client_at(store.now().await.unwrap()),
                "blocked"
            )
            .await,
        Err(Error::ResultUnavailable)
    ));
    drop(first);
    assert!(
        TidbTextService::connect_local(&url, engine(), OpenMode::FreshDatabase)
            .await
            .is_err()
    );
    let mut restarted = TidbTextService::connect_local(&url, engine(), OpenMode::ReadOnly)
        .await
        .unwrap();
    let connection = auth(&mut restarted, b"one", device).await;
    let unchanged = restarted
        .initialize_conversation(connection, 1)
        .await
        .unwrap();
    assert!(!unchanged.created);
    assert_eq!(initial.history_id, unchanged.history_id);
    assert_eq!(unchanged.next_sequence, 2);
    let replayed = restarted
        .send_text(connection, 1, key, "retained")
        .await
        .unwrap();
    assert_eq!(saved.receipt, replayed.receipt);
    match restarted.query_send(connection, 1, key).await.unwrap() {
        SendLookup::Found(receipt) => assert_eq!(receipt, saved.receipt),
        SendLookup::Unknown => panic!("persisted success disappeared"),
    }
    let unknown = client_at(store.now().await.unwrap());
    assert!(matches!(
        restarted.query_send(connection, 1, unknown).await.unwrap(),
        SendLookup::Unknown
    ));
    assert!(matches!(
        restarted
            .send_text(connection, 1, unknown, "do not accept")
            .await,
        Err(Error::ResultUnavailable)
    ));
    assert!(matches!(
        restarted
            .query_send(
                connection,
                1,
                client_at(store.now().await.unwrap() - 691_200_000)
            )
            .await,
        Err(Error::QueryExpired)
    ));
    assert_eq!(
        restarted
            .read_messages(connection, 1, None, 10)
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
    // A later permission loss cannot turn an original send receipt into a new rejection.
    let mut without_membership = engine();
    without_membership
        .store
        .conversations
        .get_mut(&(1, 1))
        .unwrap()
        .members
        .remove(&100);
    let mut restricted =
        TidbTextService::connect_local(&url, without_membership, OpenMode::ReadOnly)
            .await
            .unwrap();
    let connection = auth(&mut restricted, b"one", device).await;
    assert!(
        restricted
            .send_text(connection, 1, key, "retained")
            .await
            .unwrap()
            .replayed
    );
    assert!(matches!(
        restricted.read_messages(connection, 1, None, 10).await,
        Err(Error::PermissionDenied)
    ));
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_read_only_open_does_not_initialize_or_accept_missing_state() {
    let (url, store) = fixture().await;
    let mut service = TidbTextService::connect_local(&url, engine(), OpenMode::ReadOnly)
        .await
        .unwrap();
    let connection = auth(&mut service, b"one", generate_id128()).await;
    assert!(matches!(
        service.initialize_conversation(connection, 1).await,
        Err(Error::MissingConversation)
    ));
    let key = client_at(store.now().await.unwrap());
    assert!(matches!(
        service.send_text(connection, 1, key, "do not accept").await,
        Err(Error::ResultUnavailable)
    ));
    assert!(store.is_empty().await.unwrap());
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_window_is_rechecked_after_lock_wait_and_sequence_does_not_wrap() {
    let (_, store) = fixture().await;
    let scope = identity(1);
    store.initialize(scope, 1).await.unwrap();
    let mut lock = store.pool.begin().await.unwrap();
    sqlx::query("SELECT next_sequence FROM conversations WHERE tenant_id = ? AND conversation_id = ? FOR UPDATE")
        .bind(scope.tenant_id).bind(1u64).fetch_one(&mut *lock).await.unwrap();
    let now = store.now().await.unwrap();
    let key = client_at(now - 86_400_000 + 200);
    let waiting_store = store.clone();
    let job =
        tokio::spawn(async move { append(&waiting_store, scope, key, "queued too late").await });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    lock.rollback().await.unwrap();
    assert!(matches!(job.await.unwrap(), Err(Error::AcceptanceExpired)));
    assert_eq!(store.conversation(scope, 1).await.unwrap().next_sequence, 1);
    sqlx::query(
        "UPDATE conversations SET next_sequence = ? WHERE tenant_id = ? AND conversation_id = ?",
    )
    .bind(u64::MAX)
    .bind(scope.tenant_id)
    .bind(1u64)
    .execute(&store.pool)
    .await
    .unwrap();
    let result = append(
        &store,
        scope,
        client_at(store.now().await.unwrap()),
        "overflow",
    )
    .await;
    assert!(matches!(result, Err(Error::SequenceExhausted)));
    assert_eq!(
        store.conversation(scope, 1).await.unwrap().next_sequence,
        u64::MAX
    );
    assert!(store
        .page(scope, 1, None, 100)
        .await
        .unwrap()
        .messages
        .is_empty());
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_read_only_startup_never_creates_missing_tables() {
    let (url, store) = empty_database().await;
    assert!(
        TidbTextService::connect_local(&url, engine(), OpenMode::ReadOnly)
            .await
            .is_err()
    );
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()",
    )
    .fetch_all(&store.pool)
    .await
    .unwrap();
    assert!(tables.is_empty());
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_startup_rejects_a_missing_deduplication_constraint_without_repairing_it() {
    let (url, store) = empty_database().await;
    let incomplete =
        include_str!("../schema.sql").replace("UNIQUE KEY successful_send", "KEY successful_send");
    for statement in incomplete
        .split(';')
        .filter(|statement| !statement.trim().is_empty())
    {
        sqlx::query(statement).execute(&store.pool).await.unwrap();
    }
    assert!(matches!(
        TidbTextService::connect_local(&url, engine(), OpenMode::ReadOnly).await,
        Err(Error::Integrity(_))
    ));
    assert!(store.is_empty().await.unwrap());
    let unique: Vec<String> = sqlx::query_scalar(
        "SELECT index_name FROM information_schema.statistics WHERE table_schema = DATABASE() \
         AND table_name = 'messages' AND index_name = 'successful_send' AND non_unique = 0",
    )
    .fetch_all(&store.pool)
    .await
    .unwrap();
    assert!(unique.is_empty());
}

async fn wait_for_blocked_append(store: &TidbStore) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT CAST(COUNT(*) AS SIGNED) FROM information_schema.processlist \
                 WHERE `DB` = DATABASE() AND `ID` <> CONNECTION_ID() AND `COMMAND` <> 'Sleep' \
                 AND `INFO` LIKE ?",
            )
            .bind("SELECT history_id, next_sequence FROM conversations%")
            .fetch_one(&store.pool)
            .await
            .unwrap();
            if waiting > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the real writer must reach its SQL lock wait before cancellation");
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_cancelled_real_writer_blocks_new_acceptance_but_keeps_reads_available() {
    let (url, store) = fixture().await;
    let mut seeded = engine();
    seeded.store.seed_direct(1, 2, 100, 200);
    let mut service = TidbTextService::connect_local(&url, seeded, OpenMode::FreshDatabase)
        .await
        .unwrap();
    let connection = auth(&mut service, b"one", generate_id128()).await;
    service
        .initialize_conversation(connection, 1)
        .await
        .unwrap();
    let mut lock = store.pool.begin().await.unwrap();
    sqlx::query("SELECT next_sequence FROM conversations WHERE tenant_id = 1 AND conversation_id = 1 FOR UPDATE")
        .fetch_one(&mut *lock).await.unwrap();
    let key = client_at(store.now().await.unwrap());
    let mut operation = Box::pin(service.send_text(connection, 1, key, "cancelled"));
    tokio::select! {
        result = &mut operation => panic!("writer unexpectedly finished before cancellation: {result:?}"),
        () = wait_for_blocked_append(&store) => {}
    }
    drop(operation);
    lock.rollback().await.unwrap();
    assert!(matches!(
        service.query_send(connection, 1, key).await.unwrap(),
        SendLookup::Unknown
    ));
    assert!(matches!(
        service
            .send_text(
                connection,
                1,
                client_at(store.now().await.unwrap()),
                "blocked"
            )
            .await,
        Err(Error::ResultUnavailable)
    ));
    assert!(matches!(
        service.initialize_conversation(connection, 2).await,
        Err(Error::MissingConversation)
    ));
    assert!(service
        .read_messages(connection, 1, None, 10)
        .await
        .unwrap()
        .messages
        .is_empty());
    assert_eq!(
        store
            .conversation(identity(1), 1)
            .await
            .unwrap()
            .next_sequence,
        1
    );
}
