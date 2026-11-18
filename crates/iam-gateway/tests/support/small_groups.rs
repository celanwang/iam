use super::*;
use iam_protocol::pb::{ConversationKind, GroupCreateRequest};
use std::path::Path;

#[path = "group_members.rs"]
mod member_management;

const CHARLIE: &[u8] = b"group-charlie";
const ALICE_LAPTOP: &[u8] = b"group-alice-laptop";
const MEMBERS: [u64; 2] = [200, 300];

pub(super) fn credential(
    fixture: &Fixture,
    tenant_id: u64,
    user_id: u64,
    token: &[u8],
) -> DeviceCredential {
    DeviceCredential {
        tenant_id,
        user_id,
        device_id: generate_id128(),
        expires_at_ms: fixture.expires_at_ms,
        token_sha256: Sha256::digest(token).into(),
    }
}

struct GroupFixture {
    base: Fixture,
    charlie: DeviceCredential,
    laptop: DeviceCredential,
}

impl GroupFixture {
    async fn new() -> Self {
        let base = empty_fixture().await;
        let charlie = credential(&base, 1, 300, CHARLIE);
        let laptop = credential(&base, 1, 100, ALICE_LAPTOP);
        Self {
            base,
            charlie,
            laptop,
        }
    }

    async fn start(&self, mode: OpenMode, extra: &[DeviceCredential]) -> RunningGateway {
        let credentials = self
            .base
            .credentials()
            .into_iter()
            .chain([self.charlie, self.laptop])
            .chain(extra.iter().copied());
        self.base
            .start_with_credentials(mode, Limits::default(), "127.0.0.1:0", credentials)
            .await
    }

    async fn establish_friendships(&self, gateway: &RunningGateway) {
        let mut alice = login(gateway, self.base.alice, ALICE).await;
        let [_, bob] = self.base.credentials();
        befriend(gateway, &mut alice, 2, bob, BOB).await;
        befriend(gateway, &mut alice, 3, self.charlie, CHARLIE).await;
    }
}

async fn befriend(
    gateway: &RunningGateway,
    alice: &mut TlsTransport,
    id: u64,
    peer: DeviceCredential,
    token: &[u8],
) {
    pending(apply(alice, id, peer.user_id).await);
    let mut socket = login(gateway, peer.device_id, token).await;
    accepted(accept(&mut socket, 2, 100).await);
}

async fn create(socket: &mut TlsTransport, req: u64, key: Id128, members: &[u64]) -> DecodedBody {
    request(
        socket,
        req,
        MsgType::GroupCreate,
        &GroupCreateRequest {
            request_id: Some(key.to_vec().into()),
            member_user_ids: members.to_vec(),
        },
    )
    .await
}

pub(super) fn group_id(reply: DecodedBody) -> u64 {
    let DecodedBody::GroupCreateAck(ack) = reply else {
        panic!("expected GROUP_CREATE_ACK, got {reply:?}");
    };
    ack.conversation_id.filter(|&id| id != 0).unwrap()
}

async fn catalog_counts(db: &MySqlPool) -> (i64, i64, i64, i64) {
    sqlx::query_as("SELECT (SELECT COUNT(*) FROM conversations), (SELECT COUNT(*) FROM members), (SELECT COUNT(*) FROM account_events), (SELECT COUNT(*) FROM group_creations)")
        .fetch_one(db).await.unwrap()
}

pub(super) async fn assert_one_three_person_group(db: &MySqlPool, id: u64) {
    assert_eq!(
        catalog_counts(db).await,
        (3, 7, 7, 1),
        "two directs and a three-member group"
    );
    let groups: Vec<(u64, u64)> =
        sqlx::query_as("SELECT tenant_id, conversation_id FROM conversations WHERE kind = ?")
            .bind(ConversationKind::Group as i32)
            .fetch_all(db)
            .await
            .unwrap();
    assert_eq!(groups, [(1, id)]);
    for table in ["members", "account_events"] {
        let rows: Vec<(u64, u64)> = sqlx::query_as(&format!(
            "SELECT user_id, account_sequence FROM {table} WHERE tenant_id = 1 AND conversation_id = ? ORDER BY user_id, account_sequence"
        )).bind(id).fetch_all(db).await.unwrap();
        assert_eq!(rows, [(100, 3), (200, 2), (300, 2)], "{table}");
    }
}

pub(super) async fn open_client(path: &Path, device: DeviceCredential) -> Client {
    Client::open(
        path,
        Identity {
            tenant_id: device.tenant_id,
            user_id: device.user_id,
            device_id: device.device_id,
        },
    )
    .await
    .unwrap()
}

async fn observe(path: &Path) -> SqliteConnection {
    SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(path).read_only(true))
        .await
        .unwrap()
}

async fn assert_group_view(client: &mut Client, id: u64, latest: Option<u64>, total: usize) {
    let page = client.list_conversations(0, 100).await.unwrap();
    assert!(page.next_after.is_none());
    assert_eq!(page.items.len(), total);
    let mut groups = page
        .items
        .iter()
        .filter(|summary| summary.state.kind() == ConversationKind::Group);
    let group = groups.next().expect("group missing from account view");
    assert_eq!(group.state.conversation_id, Some(id));
    assert!(groups.next().is_none());
    let sequence = group
        .latest_cached_message
        .as_ref()
        .map(|view| view.position.as_ref().unwrap().create_seq.unwrap());
    assert_eq!(sequence, latest);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_groups_discovered_by_account_sync_and_recover_offline_messages() {
    let fixture = GroupFixture::new().await;
    let gateway = fixture.start(OpenMode::Initialize, &[]).await;
    fixture.establish_friendships(&gateway).await;
    let [a, b] = fixture.base.credentials();
    let disk = tempfile::tempdir().unwrap();
    let path = disk.path().join("bob.db");
    let mut bob = open_client(&path, b).await;
    let mut observer = observe(&path).await;
    let mut socket = ObservedTls::new(&gateway);
    let before_group = sync_through(&mut bob, &mut socket, &mut observer, BOB, 0, 1).await;
    drop(socket);
    drop(bob);

    let alice = open_client(&disk.path().join("alice.db"), a).await;
    let key = generate_id128();
    let group = alice
        .create_group(&mut gateway.client(), ALICE, key, &MEMBERS)
        .await
        .unwrap();
    let mut alice_socket = login(&gateway, a.device_id, ALICE).await;
    send(
        &mut alice_socket,
        2,
        message_key(group),
        "first while Bob is offline",
    )
    .await;

    let mut bob = open_client(&path, b).await;
    let mut socket = ObservedTls::new(&gateway);
    let message_cursor = sync_through(&mut bob, &mut socket, &mut observer, BOB, group, 1).await;
    assert_eq!(socket.first_cursor(0), Some(before_group.as_slice()));
    assert_eq!(socket.first_cursor(group), None);
    let account_cursor = saved_cursor(&mut observer, 0, 2).await.unwrap();
    assert_group_view(&mut bob, group, Some(1), 2).await;
    let first = bob.messages(group).await.unwrap();
    assert_eq!(first.len(), 1);
    drop(socket);
    drop(bob);

    let mut charlie = login(&gateway, fixture.charlie.device_id, CHARLIE).await;
    send(
        &mut charlie,
        2,
        message_key(group),
        "last while Bob is offline",
    )
    .await;
    gateway.stop().await;
    let resumed = fixture.start(OpenMode::Resume, &[]).await;
    let mut bob = open_client(&path, b).await;
    let mut socket = ObservedTls::new(&resumed);
    sync_through(&mut bob, &mut socket, &mut observer, BOB, group, 2).await;
    assert_eq!(socket.first_cursor(0), Some(account_cursor.as_slice()));
    assert_eq!(socket.first_cursor(group), Some(message_cursor.as_slice()));
    assert_group_view(&mut bob, group, Some(2), 2).await;
    let messages = bob.messages(group).await.unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0], first[0]);
    for (view, (seq, sender, value)) in messages.iter().zip([
        (1, 100, "first while Bob is offline"),
        (2, 300, "last while Bob is offline"),
    ]) {
        assert_eq!(view.position.as_ref().unwrap().create_seq, Some(seq));
        assert_eq!(view.sender_id, Some(sender));
        assert!(
            matches!(view.content.as_ref().unwrap().value.as_ref(), Some(message_content::Value::Text(text)) if text.text.as_deref() == Some(value))
        );
    }
    drop(socket);
    let group_state = bob
        .list_conversations(0, 100)
        .await
        .unwrap()
        .items
        .into_iter()
        .find(|summary| summary.state.conversation_id == Some(group))
        .unwrap()
        .state;
    let mut read_socket = resumed.client();
    assert_eq!(
        bob.unread_count(&mut read_socket, BOB, &group_state)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        bob.read_position(&mut read_socket, BOB, &group_state)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        bob.advance_read_position(&mut read_socket, BOB, &group_state, 2)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        bob.unread_count(&mut read_socket, BOB, &group_state)
            .await
            .unwrap(),
        0
    );
    drop(read_socket);

    // The other two members start with empty local stores, exercising account snapshots.
    for (device, token, total) in [(a, ALICE, 3), (fixture.charlie, CHARLIE, 2)] {
        let path = disk.path().join(format!("snapshot-{}", device.user_id));
        let mut client = open_client(&path, device).await;
        let mut db = observe(&path).await;
        let mut socket = ObservedTls::new(&resumed);
        sync_through(&mut client, &mut socket, &mut db, token, group, 2).await;
        assert_eq!(socket.first_cursor(0), None);
        assert_group_view(&mut client, group, Some(2), total).await;
    }

    let before_removal = saved_cursor(&mut observer, 0, 2).await.unwrap();
    resumed
        .service
        .set_member_access(1, group, 200, false)
        .await
        .unwrap();
    let mut socket = ObservedTls::new(&resumed);
    sync_through(&mut bob, &mut socket, &mut observer, BOB, 0, 3).await;
    assert_eq!(socket.first_cursor(0), Some(before_removal.as_slice()));
    let remaining = bob.list_conversations(0, 100).await.unwrap();
    assert_eq!(remaining.items.len(), 1);
    assert_eq!(remaining.items[0].state.kind(), ConversationKind::Direct);
    assert!(bob.messages(group).await.unwrap().is_empty());
    assert!(matches!(
        bob.read_position(&mut resumed.client(), BOB, &group_state).await,
        Err(iam_client::Error::Server(error)) if error.code() == ErrorCode::PermissionDenied
    ));
    assert!(matches!(
        bob.unread_count(&mut resumed.client(), BOB, &group_state).await,
        Err(iam_client::Error::Server(error)) if error.code() == ErrorCode::PermissionDenied
    ));
    resumed.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_groups_concurrent_create_from_two_devices_reuses_membership_and_events() {
    let fixture = GroupFixture::new().await;
    let gateway = fixture.start(OpenMode::Initialize, &[]).await;
    fixture.establish_friendships(&gateway).await;
    let db = MySqlPool::connect(fixture.base.database.as_str())
        .await
        .unwrap();
    let mut phone = login(&gateway, fixture.base.alice, ALICE).await;
    let mut laptop = login(&gateway, fixture.laptop.device_id, ALICE_LAPTOP).await;
    let key = generate_id128();
    let (first, second) = competing_requests(
        &db,
        create(&mut phone, 2, key, &MEMBERS),
        create(&mut laptop, 2, key, &[300, 200, 300]),
    )
    .await;
    let id = group_id(first);
    assert_eq!(group_id(second), id);
    expect_error(
        create(&mut laptop, 3, key, &[200, 300, 400]).await,
        ErrorCode::IdempotencyConflict,
    );
    assert_eq!(group_id(create(&mut phone, 3, key, &MEMBERS).await), id);
    assert_one_three_person_group(&db, id).await;
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_groups_retry_after_disconnect_before_commit() {
    group_commit_fault(CommitPhase::Before).await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_groups_retry_after_tidb_commits_and_loses_reply() {
    group_commit_fault(CommitPhase::After).await;
}

async fn group_commit_fault(phase: CommitPhase) {
    let mut fixture = GroupFixture::new().await;
    let db = MySqlPool::connect(fixture.base.database.as_str())
        .await
        .unwrap();
    let proxy = CommitProxy::start(&fixture.base.database).await;
    fixture.base.database = proxy.url.clone();
    let gateway = fixture.start(OpenMode::Initialize, &[]).await;
    fixture.establish_friendships(&gateway).await;
    let mut phone = login(&gateway, fixture.base.alice, ALICE).await;
    let key = generate_id128();
    let (checkpoint, disconnect) = proxy.arm(phase);
    let creating = tokio::spawn(async move { create(&mut phone, 2, key, &MEMBERS).await });
    timeout(WAIT, checkpoint).await.unwrap().unwrap().unwrap();
    let original: Option<u64> =
        sqlx::query_scalar("SELECT conversation_id FROM conversations WHERE kind = ?")
            .bind(ConversationKind::Group as i32)
            .fetch_optional(&db)
            .await
            .unwrap();
    match phase {
        CommitPhase::Before => {
            assert_eq!(original, None);
            assert_eq!(catalog_counts(&db).await, (2, 4, 4, 0));
        }
        CommitPhase::After => assert_one_three_person_group(&db, original.unwrap()).await,
    }
    disconnect.send(()).unwrap();
    let reply = creating.await.unwrap();
    let DecodedBody::Error(error) = reply else {
        panic!("expected uncertain GROUP_CREATE, got {reply:?}");
    };
    let info = error.error.unwrap();
    assert_eq!(info.code(), ErrorCode::ResultUnavailable);
    assert_eq!(
        info.action(),
        iam_protocol::pb::RetryAction::RetrySameRequest
    );
    gateway.stop().await;

    let resumed = fixture.start(OpenMode::Resume, &[]).await;
    let mut laptop = login(&resumed, fixture.laptop.device_id, ALICE_LAPTOP).await;
    let id = group_id(create(&mut laptop, 2, key, &[300, 200]).await);
    if let Some(original) = original {
        assert_eq!(id, original);
    }
    assert_eq!(group_id(create(&mut laptop, 3, key, &MEMBERS).await), id);
    assert_one_three_person_group(&db, id).await;
    resumed.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_groups_require_confirmed_local_friends_and_honor_revocation() {
    let fixture = GroupFixture::new().await;
    let db = MySqlPool::connect(fixture.base.database.as_str())
        .await
        .unwrap();
    let pending_peer = credential(&fixture.base, 1, 400, b"pending");
    let other_tenant = credential(&fixture.base, 2, 500, b"elsewhere");
    let other_bob = credential(&fixture.base, 2, 200, b"other-bob");
    let other_alice = credential(&fixture.base, 2, 100, b"other-alice");
    let gateway = fixture
        .start(
            OpenMode::Initialize,
            &[pending_peer, other_tenant, other_bob, other_alice],
        )
        .await;
    fixture.establish_friendships(&gateway).await;
    let mut alice = login(&gateway, fixture.base.alice, ALICE).await;
    pending(apply(&mut alice, 2, 400).await);
    expect_error(
        create(&mut alice, 3, generate_id128(), &[200, 400]).await,
        ErrorCode::PermissionDenied,
    );
    expect_error(
        create(&mut alice, 4, generate_id128(), &[200, 500]).await,
        ErrorCode::PermissionDenied,
    );
    assert_eq!(catalog_counts(&db).await, (2, 4, 4, 0));
    let key = generate_id128();
    let id = group_id(create(&mut alice, 5, key, &MEMBERS).await);
    let mut bob = login(&gateway, fixture.base.bob, BOB).await;
    let mut charlie = login(&gateway, fixture.charlie.device_id, CHARLIE).await;
    let mut third = login(&gateway, pending_peer.device_id, b"pending").await;
    let mut foreign = login(&gateway, other_bob.device_id, b"other-bob").await;
    let mut foreign_creator = login(&gateway, other_alice.device_id, b"other-alice").await;
    expect_error(
        create(&mut foreign_creator, 2, key, &MEMBERS).await,
        ErrorCode::PermissionDenied,
    );
    expect_error(
        create(&mut third, 2, key, &MEMBERS).await,
        ErrorCode::PermissionDenied,
    );
    assert_one_three_person_group(&db, id).await;
    send(&mut alice, 6, message_key(id), "before revocation").await;
    let DecodedBody::Push(push) = receive(&mut bob).await else {
        panic!("Bob missed the group push");
    };
    let before_revoke = push.context.unwrap();
    assert_eq!(
        before_revoke.target.as_ref().unwrap().value,
        Some(sync_target::Value::Conversation(id))
    );
    assert!(matches!(receive(&mut charlie).await, DecodedBody::Push(_)));
    tokio::join!(
        assert_nonmember(&mut third, id),
        assert_nonmember(&mut foreign, id)
    );

    gateway
        .service
        .set_member_access(1, id, 200, false)
        .await
        .unwrap();
    assert!(gateway
        .service
        .set_member_access(1, id, 400, true)
        .await
        .is_err());
    assert_eq!(group_id(create(&mut alice, 7, key, &MEMBERS).await), id);
    send(&mut alice, 8, message_key(id), "after Bob lost access").await;
    assert!(matches!(receive(&mut charlie).await, DecodedBody::Push(_)));
    assert_nonmember(&mut bob, id).await;
    let reply = request(
        &mut bob,
        10,
        MsgType::History,
        &iam_protocol::pb::HistoryRequest {
            context: Some(before_revoke),
            before_create_seq: None,
            page_token: None,
            limit: Some(iam_protocol::pb::PageLimit {
                max_items: Some(10),
                max_bytes: Some(256 * 1024),
            }),
        },
    )
    .await;
    assert!(
        matches!(&reply, DecodedBody::HistoryAck(ack)
        if matches!(&ack.value, Some(iam_protocol::pb::history_response::Value::Error(info))
            if info.code() == ErrorCode::PermissionDenied)),
        "{reply:?}"
    );
    let users: Vec<u64> = sqlx::query_scalar(
        "SELECT user_id FROM members WHERE tenant_id = 1 AND conversation_id = ? ORDER BY user_id",
    )
    .bind(id)
    .fetch_all(&db)
    .await
    .unwrap();
    assert_eq!(users, [100, 300]);
    let texts: Vec<Vec<u8>> = sqlx::query_scalar("SELECT `text` FROM messages WHERE tenant_id = 1 AND conversation_id = ? ORDER BY `sequence`")
        .bind(id).fetch_all(&db).await.unwrap();
    assert_eq!(
        texts,
        [b"before revocation".as_slice(), b"after Bob lost access"]
    );
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_groups_limit_counts_distinct_members_including_creator() {
    let base = empty_fixture().await;
    let [alice_device, _] = base.credentials();
    let peers: Vec<_> = (1..=31)
        .map(|id| {
            let token = format!("group-limit-{id}");
            (credential(&base, 1, id, token.as_bytes()), token)
        })
        .collect();
    let credentials = std::iter::once(alice_device).chain(peers.iter().map(|(device, _)| *device));
    let gateway = base
        .start_with_credentials(
            OpenMode::Initialize,
            Limits::default(),
            "127.0.0.1:0",
            credentials,
        )
        .await;
    let mut alice = login(&gateway, base.alice, ALICE).await;
    for (device, token) in &peers {
        befriend(
            &gateway,
            &mut alice,
            device.user_id + 1,
            *device,
            token.as_bytes(),
        )
        .await;
    }
    for (i, invalid) in [
        vec![1, 1],
        vec![0, 1, 2],
        vec![100, 1, 2],
        (1..=32).collect(),
    ]
    .iter()
    .enumerate()
    {
        expect_error(
            create(&mut alice, 33 + i as u64, generate_id128(), invalid).await,
            ErrorCode::InvalidArgument,
        );
    }
    let mut repeated: Vec<u64> = (1..=31).rev().collect();
    repeated.push(31);
    let key = generate_id128();
    let id = group_id(create(&mut alice, 37, key, &repeated).await);
    let unique: Vec<u64> = (1..=31).collect();
    assert_eq!(group_id(create(&mut alice, 38, key, &unique).await), id);
    let roster = request(
        &mut alice,
        39,
        MsgType::GroupMembers,
        &iam_protocol::pb::GroupMembersRequest {
            conversation_id: Some(id),
        },
    )
    .await;
    assert!(matches!(roster, DecodedBody::GroupMembersAck(list)
        if list.owner_user_id == Some(100) && list.members.len() == 32));
    let db = MySqlPool::connect(base.database.as_str()).await.unwrap();
    assert_eq!(catalog_counts(&db).await, (32, 94, 94, 1));
    for table in ["members", "account_events"] {
        let users: Vec<u64> = sqlx::query_scalar(&format!(
            "SELECT user_id FROM {table} WHERE tenant_id = 1 AND conversation_id = ? ORDER BY user_id"
        )).bind(id).fetch_all(&db).await.unwrap();
        assert_eq!(
            users,
            unique.iter().copied().chain([100]).collect::<Vec<_>>(),
            "{table}"
        );
    }
    let disk = tempfile::tempdir().unwrap();
    let path = disk.path().join("member.db");
    let (device, token) = &peers[0];
    let mut client = open_client(&path, *device).await;
    let mut observer = observe(&path).await;
    let mut socket = ObservedTls::new(&gateway);
    sync_through(
        &mut client,
        &mut socket,
        &mut observer,
        token.as_bytes(),
        id,
        0,
    )
    .await;
    assert_group_view(&mut client, id, None, 2).await;
    gateway.stop().await;
}

#[path = "group_invites.rs"]
mod invites;
