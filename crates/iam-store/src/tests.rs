use super::*;
use std::time::Duration;

#[test]
fn configuration_requires_a_local_tcp_database() {
    assert!(local_options("mysql://root@127.0.0.1:4000/iam_local").is_ok());
    assert!(local_options("mysql://root@db.example:4000/iam_local").is_err());
    assert!(local_options("mysql://root@127.0.0.1:4000").is_err());
    assert!(local_options("mysql://root@localhost/iam_local?socket=/tmp/mysql.sock").is_err());
}

async fn fixture(tenant: u64, user: u64) -> (String, TidbStore, SendKey) {
    let url = support::isolated_database().await.to_string();
    let mut seeds = MemoryStore::new(0);
    seeds.seed_direct(tenant, 1, user, 200);
    let store = TidbStore::open(&url, generate_id128(), Some(&seeds))
        .await
        .unwrap();
    let key = SendKey {
        owner: Identity {
            tenant_id: tenant,
            user_id: user,
            device_id: generate_id128(),
        },
        conversation: 1,
        client_id: generate_id128(),
    };
    (url, store, key)
}

fn committed(decision: Decision) -> iam_protocol::pb::SendReceipt {
    let Some(send_outcome::Value::Committed(receipt)) = decision.outcome.value else {
        panic!("expected commit: {:?}", decision.outcome);
    };
    receipt
}

fn error_code(decision: Decision) -> ErrorCode {
    match decision.outcome.value.unwrap() {
        send_outcome::Value::RejectedFinal(error) => error.code(),
        send_outcome::Value::Unresolved(result) => result.error.unwrap().code(),
        send_outcome::Value::Committed(receipt) => panic!("unexpected commit: {receipt:?}"),
    }
}

async fn tables(pool: &MySqlPool) -> Vec<String> {
    sqlx::query_scalar("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY table_name")
        .fetch_all(pool)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_single_schema_supports_unsigned_ids_and_ordered_pages() {
    let (_, store, mut key) = fixture(u64::MAX, u64::MAX).await;
    assert_eq!(
        tables(&store.pool).await,
        [
            "account_events",
            "conversation_retention",
            "conversations",
            "direct_pairs",
            "friend_requests",
            "members",
            "messages",
            "rejections",
            "store_control"
        ]
    );

    let first_text = format!("é{}", "a".repeat(64 * 1024 - 2));
    let first = committed(
        store
            .decide(&key, Some(&text_content(&first_text)), true)
            .await
            .unwrap(),
    );
    let replay = committed(
        store
            .decide(&key, Some(&text_content(&first_text)), true)
            .await
            .unwrap(),
    );
    assert_eq!(first, replay);
    let conflict = store
        .decide(&key, Some(&text_content(&first_text.to_uppercase())), true)
        .await
        .unwrap();
    assert_eq!(error_code(conflict), ErrorCode::IdempotencyConflict);

    key.client_id = generate_id128();
    let oversized = store
        .decide(&key, Some(&text_content(&format!("{first_text}a"))), true)
        .await
        .unwrap();
    assert_eq!(error_code(oversized), ErrorCode::ResourceLimit);
    key.client_id = generate_id128();
    let second = committed(
        store
            .decide(&key, Some(&text_content("second")), true)
            .await
            .unwrap(),
    );
    let latest = store.page(key.owner, 1, None, 1).await.unwrap();
    assert_eq!(latest.messages[0].position, second.position);
    assert_eq!(latest.next_before, Some(2));
    let older = store
        .page(key.owner, 1, latest.next_before, 1)
        .await
        .unwrap();
    assert_eq!(older.messages[0].position, first.position);
    assert_eq!(older.messages[0].sender_id, Some(u64::MAX));
    assert_eq!(older.messages[0].content, Some(text_content(&first_text)));
    assert!(older.next_before.is_none());
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_insert_failure_rolls_back_sequence_and_message() {
    let (_, store, mut key) = fixture(1, 100).await;
    committed(
        store
            .decide(&key, Some(&text_content("first")), true)
            .await
            .unwrap(),
    );
    // Force the message INSERT to fail after the conversation sequence UPDATE.
    sqlx::query(
        "ALTER TABLE messages ADD UNIQUE KEY test_one_message (tenant_id, conversation_id)",
    )
    .execute(&store.pool)
    .await
    .unwrap();
    key.client_id = generate_id128();
    let result = store
        .decide(&key, Some(&text_content("second")), true)
        .await;
    assert!(matches!(result, Err(Error::Database(_))));
    let stream = store
        .stream(key.owner, Scope::Conversation(1))
        .await
        .unwrap();
    assert_eq!(stream.upper, 1);
    assert_eq!(
        error_code(store.decide(&key, None, true).await.unwrap()),
        ErrorCode::NotFound
    );
    assert_eq!(
        store
            .page(key.owner, 1, None, 10)
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_resume_checks_schema_without_repairing_it() {
    let url = support::isolated_database().await;
    let pool = connect_pool(url.as_str()).await.unwrap();
    let id = generate_id128();
    assert!(TidbStore::open(url.as_str(), id, None).await.is_err());
    assert!(tables(&pool).await.is_empty());

    TidbStore::open(url.as_str(), id, Some(&MemoryStore::new(0)))
        .await
        .unwrap();
    sqlx::query("UPDATE store_control SET friends_ready = 0 WHERE singleton = 1")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        TidbStore::open(url.as_str(), id, None).await,
        Err(Error::Configuration(_))
    ));
    TidbStore::upgrade(url.as_str(), id).await.unwrap();
    sqlx::query("ALTER TABLE messages DROP INDEX successful_send")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        TidbStore::open(url.as_str(), id, None).await,
        Err(Error::Integrity(_))
    ));
    let index: Option<String> = sqlx::query_scalar(
        "SELECT index_name FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'messages' AND index_name = 'successful_send'",
    ).fetch_optional(&pool).await.unwrap();
    assert!(index.is_none());
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_initialize_leaves_legacy_rows_untouched() {
    let url = support::isolated_database().await;
    let pool = connect_pool(url.as_str()).await.unwrap();
    let schema = include_str!("../schema.sql");
    for statement in schema.split(';').map(str::trim).filter(|statement| {
        statement.starts_with("CREATE TABLE IF NOT EXISTS conversations ")
            || statement.starts_with("CREATE TABLE IF NOT EXISTS messages ")
    }) {
        sqlx::query(statement).execute(&pool).await.unwrap();
    }
    sqlx::query("INSERT INTO conversations (tenant_id, conversation_id, history_id, next_sequence) VALUES (1, 1, ?, 2)")
        .bind(generate_id128().as_slice()).execute(&pool).await.unwrap();

    let result = TidbStore::open(url.as_str(), generate_id128(), Some(&MemoryStore::new(0))).await;
    assert!(matches!(result, Err(Error::Configuration(_))));
    assert_eq!(tables(&pool).await, ["conversations", "messages"]);
    let sequence: u64 = sqlx::query_scalar(
        "SELECT next_sequence FROM conversations WHERE tenant_id = 1 AND conversation_id = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(sequence, 2);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_friend_crossed_requests_commit_one_direct_and_two_account_events() {
    let url = support::isolated_database().await;
    let id = generate_id128();
    let store = TidbStore::open(url.as_str(), id, Some(&MemoryStore::new(0)))
        .await
        .unwrap();
    let alice = friend_identity(1, u64::MAX);
    let bob = friend_identity(1, 2);
    let other_tenant = friend_identity(2, bob.user_id);

    for (from, to) in [(alice, bob), (bob, alice)] {
        assert_eq!(
            store
                .request_friend(from, to.user_id, |_| Ok(()))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .request_friend(from, to.user_id, |write| {
                    assert!(!write);
                    Ok(())
                })
                .await
                .unwrap(),
            None,
        );
        assert_eq!(
            store.incoming_friends(to, 0).await.unwrap().from_user_ids,
            [from.user_id],
        );
        assert_eq!(store.stream(to, Scope::Account).await.unwrap().upper, 0);
    }
    assert!(matches!(
        store
            .accept_friend(other_tenant, alice.user_id, |_| Ok(()))
            .await,
        Err(Error::FriendNotFound)
    ));

    let (ab, ba) = tokio::join!(
        store.accept_friend(alice, bob.user_id, |_| Ok(())),
        store.accept_friend(bob, alice.user_id, |_| Ok(())),
    );
    let conversation = ab.unwrap();
    assert_eq!(ba.unwrap(), conversation);

    let resumed = TidbStore::open(url.as_str(), id, None).await.unwrap();
    for (recipient, sender) in [(alice, bob), (bob, alice)] {
        let no_write = |write: bool| {
            assert!(!write);
            Ok(())
        };
        assert_eq!(
            resumed
                .accept_friend(recipient, sender.user_id, no_write)
                .await
                .unwrap(),
            conversation
        );
        assert_eq!(
            resumed
                .request_friend(recipient, sender.user_id, no_write)
                .await
                .unwrap(),
            Some(conversation)
        );
        assert!(resumed
            .incoming_friends(recipient, 0)
            .await
            .unwrap()
            .from_user_ids
            .is_empty());
        assert_eq!(
            resumed
                .stream(recipient, Scope::Account)
                .await
                .unwrap()
                .upper,
            1
        );
        assert_eq!(
            resumed
                .stream(recipient, Scope::Conversation(conversation))
                .await
                .unwrap()
                .access_version,
            1
        );
    }
    let count: i64 = sqlx::query_scalar("SELECT CAST(COUNT(*) AS SIGNED) FROM conversations")
        .fetch_one(&resumed.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_friend_upgrade_preserves_legacy_ids_access_and_cursors() {
    let url = support::isolated_database().await;
    let id = generate_id128();
    let mut seeds = MemoryStore::new(0);
    let canonical = seeds.seed_direct(1, 4, 10, 20);
    let duplicate = seeds.seed_direct(1, 7, 10, 20);
    let store = TidbStore::open(url.as_str(), id, Some(&seeds))
        .await
        .unwrap();
    let alice = friend_identity(1, 10);
    let bob = friend_identity(1, 20);
    store
        .set_member_access(1, 4, bob.user_id, false)
        .await
        .unwrap();
    let upper = store.stream(bob, Scope::Account).await.unwrap().upper;
    let cursor = iam_core::SyncCursor {
        tenant_id: bob.tenant_id,
        user_id: bob.user_id,
        device_id: bob.device_id,
        conversation_id: 0,
        epoch: id,
        access_version: 0,
        covered_through: upper,
        issued_at_ms: store.now().await.unwrap(),
        is_account: true,
    };
    let token = store.tokens.sign_cursor(&cursor);

    // Recreate the pre-friends schema, keeping existing memberships and account events.
    let pool = store.pool.clone();
    drop(store);
    drop_friend_schema(&pool).await;
    assert!(TidbStore::open(url.as_str(), id, None).await.is_err());
    assert!(TidbStore::upgrade(url.as_str(), generate_id128())
        .await
        .is_err());
    assert!(!tables(&pool).await.contains(&"direct_pairs".to_owned()));

    TidbStore::upgrade(url.as_str(), id).await.unwrap();
    TidbStore::upgrade(url.as_str(), id).await.unwrap();
    let store = TidbStore::open(url.as_str(), id, None).await.unwrap();
    assert_eq!(
        store
            .tokens
            .verify_cursor(&token, store.now().await.unwrap())
            .unwrap(),
        cursor
    );
    assert_eq!(
        store.stream(bob, Scope::Account).await.unwrap().upper,
        upper
    );
    assert_eq!(
        store
            .stream(bob, Scope::Conversation(7))
            .await
            .unwrap()
            .epoch,
        duplicate
    );
    assert!(matches!(
        store.stream(bob, Scope::Conversation(4)).await,
        Err(Error::PermissionDenied)
    ));
    assert!(store.create_direct(1, 9, 10, 20).await.is_err());
    assert_eq!(
        store
            .request_friend(alice, bob.user_id, |_| Ok(()))
            .await
            .unwrap(),
        None
    );
    assert!(matches!(
        store.accept_friend(bob, alice.user_id, |_| Ok(())).await,
        Err(Error::PermissionDenied)
    ));
    assert_eq!(
        store.incoming_friends(bob, 0).await.unwrap().from_user_ids,
        [alice.user_id]
    );

    store
        .set_member_access(1, 4, bob.user_id, true)
        .await
        .unwrap();
    assert_eq!(
        store
            .accept_friend(bob, alice.user_id, |_| Ok(()))
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        store
            .stream(bob, Scope::Conversation(4))
            .await
            .unwrap()
            .epoch,
        canonical
    );
    assert_eq!(
        store.stream(bob, Scope::Account).await.unwrap().upper,
        upper + 1
    );
    assert_eq!(store.stream(alice, Scope::Account).await.unwrap().upper, 2);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_friend_lists_only_pending_requests_with_exclusive_paging() {
    let url = support::isolated_database().await;
    let store = TidbStore::open(url.as_str(), generate_id128(), Some(&MemoryStore::new(0)))
        .await
        .unwrap();
    let bob = friend_identity(1, u64::MAX);
    let mut insert = sqlx::QueryBuilder::<MySql>::new("INSERT INTO friend_requests ");
    insert.push_values(1_u64..=102, |mut row, sender| {
        row.push_bind(1_u64)
            .push_bind(bob.user_id)
            .push_bind(sender)
            .push_bind(true);
    });
    insert.build().execute(&store.pool).await.unwrap();
    store.accept_friend(bob, 1, |_| Ok(())).await.unwrap();
    store
        .request_friend(friend_identity(2, 3), bob.user_id, |_| Ok(()))
        .await
        .unwrap();

    let first = store.incoming_friends(bob, 0).await.unwrap();
    assert_eq!(first.from_user_ids, (2..=101).collect::<Vec<_>>());
    assert_eq!(first.next_after_user_id, Some(101));
    let last = store
        .incoming_friends(bob, first.next_after_user_id.unwrap())
        .await
        .unwrap();
    assert_eq!(last.from_user_ids, [102]);
    assert_eq!(last.next_after_user_id, None);
}

fn friend_identity(tenant_id: u64, user_id: u64) -> Identity {
    Identity {
        tenant_id,
        user_id,
        device_id: generate_id128(),
    }
}

async fn drop_friend_schema(pool: &MySqlPool) {
    for ddl in [
        "DROP TABLE friend_requests",
        "DROP TABLE direct_pairs",
        "ALTER TABLE store_control DROP COLUMN friends_ready",
    ] {
        sqlx::query(ddl).execute(pool).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_checks_acceptance_window_after_lock_wait_and_sequence_overflow() {
    let (_, store, mut key) = fixture(1, 100).await;
    let lock = hold_control(&store).await;
    let deadline = db_now(&store).await + 2_000;
    let client_ms = deadline - ACCEPT_WINDOW_MS;
    key.client_id[..6].copy_from_slice(&(client_ms as u64).to_be_bytes()[2..]);
    let content = text_content("too late");
    let mut send = Box::pin(store.decide(&key, Some(&content), true));
    tokio::select! {
        _ = &mut send => panic!("writer completed before acquiring the control lock"),
        () = wait_for_writer(&store) => {}
    }
    assert!(db_now(&store).await < deadline);
    tokio::time::timeout(Duration::from_secs(5), async {
        while db_now(&store).await < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    lock.rollback().await.unwrap();
    assert_eq!(error_code(send.await.unwrap()), ErrorCode::RequestExpired);
    assert_eq!(
        store
            .stream(key.owner, Scope::Conversation(1))
            .await
            .unwrap()
            .upper,
        0
    );

    sqlx::query(
        "UPDATE conversations SET next_sequence = ? WHERE tenant_id = 1 AND conversation_id = 1",
    )
    .bind(u64::MAX)
    .execute(&store.pool)
    .await
    .unwrap();
    key.client_id = generate_id128();
    assert!(matches!(
        store.decide(&key, Some(&content), true).await,
        Err(Error::SequenceExhausted)
    ));
    assert_eq!(
        store
            .stream(key.owner, Scope::Conversation(1))
            .await
            .unwrap()
            .upper,
        u64::MAX - 1
    );
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_cancelled_waiting_send_can_be_safely_retried() {
    let (_, store, key) = fixture(1, 100).await;
    let lock = hold_control(&store).await;
    let content = text_content("cancelled");
    let mut send = Box::pin(store.decide(&key, Some(&content), true));
    tokio::select! {
        _ = &mut send => panic!("writer completed before acquiring the control lock"),
        () = wait_for_writer(&store) => {}
    }
    drop(send);
    lock.rollback().await.unwrap();
    assert_eq!(
        error_code(store.decide(&key, None, true).await.unwrap()),
        ErrorCode::NotFound
    );
    let receipt = committed(store.decide(&key, Some(&content), true).await.unwrap());
    assert_eq!(receipt.position.unwrap().create_seq, Some(1));
}

async fn db_now(store: &TidbStore) -> i64 {
    sqlx::query_scalar(CLOCK_SQL)
        .fetch_one(&store.pool)
        .await
        .unwrap()
}

async fn hold_control(store: &TidbStore) -> Transaction<'_, MySql> {
    let mut lock = store.pool.begin().await.unwrap();
    sqlx::query(CONTROL).fetch_one(&mut *lock).await.unwrap();
    lock
}

async fn wait_for_writer(store: &TidbStore) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let count: i64 = sqlx::query_scalar(
                "SELECT CAST(COUNT(*) AS SIGNED) FROM information_schema.processlist WHERE DB = DATABASE() AND ID <> CONNECTION_ID() AND INFO LIKE 'SELECT store_id, token_key,%'",
            ).fetch_one(&store.pool).await.unwrap();
            if count != 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("writer reached the control row lock");
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_upgrade_preserves_receipts_and_account_cursors() {
    use iam_protocol::pb::{
        snapshot_response, sync_response, sync_target, Empty, PageLimit, SnapshotRequest,
        SyncRequest, SyncTarget,
    };

    let (url, store, key) = fixture(1, 100).await;
    let receipt = committed(
        store
            .decide(&key, Some(&text_content("before upgrade")), true)
            .await
            .unwrap(),
    );
    let limit = Some(PageLimit {
        max_items: Some(10),
        max_bytes: Some(256 * 1024),
    });
    let mut request = SyncRequest {
        target: Some(SyncTarget {
            value: Some(sync_target::Value::Account(Empty {})),
        }),
        limit,
        ..SyncRequest::default()
    };
    let sync_response::Value::Reset(reset) = store
        .sync_at(key.owner, &request, store.now().await.unwrap())
        .await
    else {
        panic!("expected initial account reset");
    };
    let snapshot_response::Value::Page(page) = store
        .snapshot(
            key.owner,
            &SnapshotRequest {
                page_token: reset.snapshot_token,
                limit,
            },
        )
        .await
    else {
        panic!("expected account snapshot");
    };
    request.cursor = page.resume_cursor;
    assert!(request.cursor.is_some());

    drop_friend_schema(&store.pool).await;
    for statement in [
        "DROP TABLE account_events",
        "DROP TABLE conversation_retention",
    ] {
        sqlx::query(statement).execute(&store.pool).await.unwrap();
    }
    let id = store.id;
    assert!(TidbStore::open(url.as_str(), id, None).await.is_err());
    assert!(TidbStore::upgrade(url.as_str(), generate_id128())
        .await
        .is_err());
    assert!(!tables(&store.pool)
        .await
        .iter()
        .any(|table| table == "account_events"));
    TidbStore::upgrade(url.as_str(), id).await.unwrap();
    TidbStore::upgrade(url.as_str(), id).await.unwrap();

    let resumed = TidbStore::open(url.as_str(), id, None).await.unwrap();
    assert_eq!(
        committed(resumed.decide(&key, None, true).await.unwrap()),
        receipt
    );
    let sync_response::Value::Page(page) = resumed
        .sync_at(key.owner, &request, resumed.now().await.unwrap())
        .await
    else {
        panic!("old account cursor must remain valid");
    };
    assert_eq!(page.covered_through, Some(1));
    assert!(page.changes.is_empty());
}
