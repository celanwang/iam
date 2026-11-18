use super::*;
use iam_protocol::pb::{
    snapshot_item, snapshot_response, sync_response, sync_target, ConversationState, Empty,
    PageLimit, ReadPositionRequest, RetryAction, SnapshotRequest, SyncRequest, SyncTarget,
};
use sqlx::MySqlPool;

const BOB_SECOND: &[u8] = b"read-position-bob-second";
const BOB_OTHER_TENANT: &[u8] = b"read-position-bob-other-tenant";

async fn login(gateway: &RunningGateway, device: Id128, token: &[u8]) -> TlsTransport {
    let mut socket = gateway.client();
    socket.reconnect().await.unwrap();
    assert!(matches!(
        auth(&mut socket, 1, device, token).await,
        auth_response::Value::Session(_)
    ));
    socket
}

async fn scope(socket: &mut TlsTransport) -> ConversationState {
    let limit = Some(PageLimit {
        max_items: Some(10),
        max_bytes: Some(256 * 1024),
    });
    let DecodedBody::SyncAck(ack) = request(
        socket,
        2,
        MsgType::Sync,
        &SyncRequest {
            target: Some(SyncTarget {
                value: Some(sync_target::Value::Account(Empty {})),
            }),
            limit,
            ..SyncRequest::default()
        },
    )
    .await
    else {
        panic!("expected SYNC_ACK");
    };
    let Some(sync_response::Value::Reset(reset)) = ack.value else {
        panic!("expected account reset, got {ack:?}");
    };
    let DecodedBody::SnapshotAck(ack) = request(
        socket,
        3,
        MsgType::Snapshot,
        &SnapshotRequest {
            page_token: reset.snapshot_token,
            limit,
        },
    )
    .await
    else {
        panic!("expected SNAPSHOT_ACK");
    };
    let Some(snapshot_response::Value::Page(mut page)) = ack.value else {
        panic!("expected account snapshot, got {ack:?}");
    };
    assert_eq!(page.items.len(), 1);
    let Some(snapshot_item::Value::Conversation(state)) = page.items.remove(0).value else {
        panic!("expected a conversation");
    };
    state
}

async fn device_client(dir: &std::path::Path, file: &str, credential: DeviceCredential) -> Client {
    Client::open(
        dir.join(file),
        Identity {
            tenant_id: credential.tenant_id,
            user_id: credential.user_id,
            device_id: credential.device_id,
        },
    )
    .await
    .unwrap()
}

fn assert_error<T: std::fmt::Debug>(result: iam_client::Result<T>, code: ErrorCode) {
    assert!(
        matches!(&result, Err(iam_client::Error::Server(error)) if error.code() == code),
        "{result:?}"
    );
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_read_positions_merge_and_follow_membership_and_history() {
    let fixture = Fixture::new().await;
    let [alice_device, bob_device] = fixture.credentials();
    let second_device = DeviceCredential {
        device_id: generate_id128(),
        token_sha256: Sha256::digest(BOB_SECOND).into(),
        ..bob_device
    };
    let other_tenant = DeviceCredential {
        tenant_id: 2,
        device_id: generate_id128(),
        token_sha256: Sha256::digest(BOB_OTHER_TENANT).into(),
        ..bob_device
    };
    let credentials = [alice_device, bob_device, second_device, other_tenant];
    let gateway = fixture
        .start_with_credentials(
            OpenMode::Initialize,
            Limits::default(),
            "127.0.0.1:0",
            credentials,
        )
        .await;
    let mut alice = login(&gateway, fixture.alice, ALICE).await;
    send(&mut alice, 2, key(), "one").await;
    send(&mut alice, 3, key(), "two").await;
    let initial = scope(&mut login(&gateway, fixture.bob, BOB).await).await;
    let dir = tempfile::tempdir().unwrap();
    let bob = device_client(dir.path(), "bob.db", bob_device).await;
    let second = device_client(dir.path(), "second.db", second_device).await;
    let other = device_client(dir.path(), "other.db", other_tenant).await;
    let mut first_wire = gateway.client();
    let mut second_wire = gateway.client();
    assert_eq!(
        bob.read_position(&mut first_wire, BOB, &initial)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        second
            .read_position(&mut second_wire, BOB_SECOND, &initial)
            .await
            .unwrap(),
        None
    );

    let (low, high) = tokio::join!(
        bob.advance_read_position(&mut first_wire, BOB, &initial, 1),
        second.advance_read_position(&mut second_wire, BOB_SECOND, &initial, 2),
    );
    assert!(matches!(low.unwrap(), 1 | 2));
    assert_eq!(high.unwrap(), 2);
    assert_eq!(
        bob.advance_read_position(&mut first_wire, BOB, &initial, 1)
            .await
            .unwrap(),
        2
    );
    assert_error(
        second
            .advance_read_position(&mut second_wire, BOB_SECOND, &initial, 3)
            .await,
        ErrorCode::InvalidArgument,
    );
    let alice_client = device_client(dir.path(), "alice.db", alice_device).await;
    assert_eq!(
        alice_client
            .read_position(&mut gateway.client(), ALICE, &initial)
            .await
            .unwrap(),
        None
    );
    assert_error(
        other
            .read_position(&mut gateway.client(), BOB_OTHER_TENANT, &initial)
            .await,
        ErrorCode::PermissionDenied,
    );
    gateway.service.create_direct(2, 1, 100, 200).await.unwrap();
    let other_scope =
        scope(&mut login(&gateway, other_tenant.device_id, BOB_OTHER_TENANT).await).await;
    assert_ne!(other_scope.history_id, initial.history_id);
    assert_eq!(
        other
            .read_position(&mut gateway.client(), BOB_OTHER_TENANT, &other_scope)
            .await
            .unwrap(),
        None
    );
    assert_error(
        other
            .advance_read_position(&mut gateway.client(), BOB_OTHER_TENANT, &initial, 1)
            .await,
        ErrorCode::ContextChanged,
    );

    gateway.stop().await;
    let gateway = fixture
        .start_with_credentials(
            OpenMode::Resume,
            Limits::default(),
            "127.0.0.1:0",
            credentials,
        )
        .await;
    let mut wire = gateway.client();
    assert_eq!(
        second
            .read_position(&mut wire, BOB_SECOND, &initial)
            .await
            .unwrap(),
        Some(2)
    );
    gateway
        .service
        .set_member_access(1, 1, 200, false)
        .await
        .unwrap();
    assert_error(
        second.read_position(&mut wire, BOB_SECOND, &initial).await,
        ErrorCode::PermissionDenied,
    );
    assert_error(
        second
            .advance_read_position(&mut wire, BOB_SECOND, &initial, 1)
            .await,
        ErrorCode::PermissionDenied,
    );
    gateway
        .service
        .set_member_access(1, 1, 200, true)
        .await
        .unwrap();
    let renewed = scope(&mut login(&gateway, fixture.bob, BOB).await).await;
    assert_ne!(renewed.access_version, initial.access_version);
    assert_eq!(
        second
            .read_position(&mut wire, BOB_SECOND, &renewed)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        second
            .advance_read_position(&mut wire, BOB_SECOND, &renewed, 1)
            .await
            .unwrap(),
        1
    );
    assert_error(
        second.read_position(&mut wire, BOB_SECOND, &initial).await,
        ErrorCode::ContextChanged,
    );
    assert_error(
        second
            .advance_read_position(&mut wire, BOB_SECOND, &initial, 2)
            .await,
        ErrorCode::ContextChanged,
    );
    assert_eq!(
        second
            .read_position(&mut wire, BOB_SECOND, &renewed)
            .await
            .unwrap(),
        Some(1)
    );

    gateway.stop().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let periods: Vec<(u64, u64)> = sqlx::query_as(
        "SELECT access_version, read_through FROM read_positions
         WHERE tenant_id = 1 AND conversation_id = 1 AND user_id = 200 ORDER BY access_version",
    )
    .fetch_all(&db)
    .await
    .unwrap();
    assert_eq!(
        periods,
        [
            (initial.access_version.unwrap(), 2),
            (renewed.access_version.unwrap(), 1),
        ]
    );
    let mut tx = db.begin().await.unwrap();
    sqlx::query("DELETE FROM messages WHERE tenant_id = 1 AND conversation_id = 1")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE conversations SET history_id = ?, next_sequence = 1 WHERE tenant_id = 1 AND conversation_id = 1")
        .bind(generate_id128().to_vec()).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();

    let gateway = fixture
        .start_with_credentials(
            OpenMode::Resume,
            Limits::default(),
            "127.0.0.1:0",
            credentials,
        )
        .await;
    let reset = scope(&mut login(&gateway, fixture.bob, BOB).await).await;
    assert_eq!(reset.access_version, renewed.access_version);
    assert_ne!(reset.history_id, renewed.history_id);
    let mut wire = gateway.client();
    assert_error(
        bob.read_position(&mut wire, BOB, &renewed).await,
        ErrorCode::ContextChanged,
    );
    assert_error(
        bob.advance_read_position(&mut wire, BOB, &renewed, 1).await,
        ErrorCode::ContextChanged,
    );
    assert_eq!(
        bob.read_position(&mut wire, BOB, &reset).await.unwrap(),
        None
    );
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_read_position_retries_after_losing_commit_reply() {
    let mut fixture = Fixture::new().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let proxy = CommitProxy::start(&fixture.database).await;
    fixture.database = proxy.url.clone();
    let gateway = fixture.start(OpenMode::Initialize, Limits::default()).await;
    let mut alice = login(&gateway, fixture.alice, ALICE).await;
    send(&mut alice, 2, key(), "first").await;
    let mut bob = login(&gateway, fixture.bob, BOB).await;
    let scope = scope(&mut bob).await;
    let position = ReadPositionRequest {
        conversation_id: scope.conversation_id,
        history_id: scope.history_id,
        access_version: scope.access_version,
        read_through: Some(1),
    };
    let (checkpoint, disconnect) = proxy.arm(CommitPhase::After);
    let initial = position.clone();
    let saving =
        tokio::spawn(async move { request(&mut bob, 4, MsgType::ReadPosition, &initial).await });
    timeout(WAIT, checkpoint).await.unwrap().unwrap().unwrap();
    assert!(!saving.is_finished());
    let committed: u64 = sqlx::query_scalar("SELECT read_through FROM read_positions")
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(committed, 1);
    disconnect.send(()).unwrap();
    assert!(matches!(saving.await.unwrap(), DecodedBody::Error(error)
        if error.error.as_ref().is_some_and(|info| info.code() == ErrorCode::ResultUnavailable
            && info.action() == RetryAction::RetrySameRequest)));
    let mut bob = login(&gateway, fixture.bob, BOB).await;
    assert!(
        matches!(request(&mut bob, 2, MsgType::ReadPosition, &position).await,
        DecodedBody::ReadPositionAck(ack) if ack.read_through == Some(1))
    );
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_unread_count_respects_sender_retention_and_member_period() {
    let fixture = Fixture::new().await;
    let [alice_device, bob_device] = fixture.credentials();
    let second_device = DeviceCredential {
        device_id: generate_id128(),
        token_sha256: Sha256::digest(BOB_SECOND).into(),
        ..bob_device
    };
    let gateway = fixture
        .start_with_credentials(
            OpenMode::Initialize,
            Limits::default(),
            "127.0.0.1:0",
            [alice_device, bob_device, second_device],
        )
        .await;
    let mut alice = login(&gateway, fixture.alice, ALICE).await;
    let mut bob_socket = login(&gateway, fixture.bob, BOB).await;
    let initial = scope(&mut bob_socket).await;
    let dir = tempfile::tempdir().unwrap();
    let bob = device_client(dir.path(), "bob.db", bob_device).await;
    let second = device_client(dir.path(), "second.db", second_device).await;
    let mut wire = gateway.client();
    assert_eq!(bob.unread_count(&mut wire, BOB, &initial).await.unwrap(), 0);

    let mut second_socket = login(&gateway, second_device.device_id, BOB_SECOND).await;
    send(&mut alice, 2, key(), "below retention floor").await;
    send(
        &mut second_socket,
        2,
        key(),
        "written by the reader's other device",
    )
    .await;
    send(&mut alice, 3, key(), "within retention floor").await;
    send(&mut alice, 4, key(), "after reading").await;
    assert_eq!(bob.unread_count(&mut wire, BOB, &initial).await.unwrap(), 3);
    gateway.service.set_retention_floor(1, 1, 2).await.unwrap();
    assert_eq!(bob.unread_count(&mut wire, BOB, &initial).await.unwrap(), 2);
    assert_eq!(
        second
            .advance_read_position(&mut gateway.client(), BOB_SECOND, &initial, 3)
            .await
            .unwrap(),
        3
    );
    assert_eq!(bob.unread_count(&mut wire, BOB, &initial).await.unwrap(), 1);

    let bad_history = ConversationState {
        history_id: Some(generate_id128().to_vec().into()),
        ..initial.clone()
    };
    assert_error(
        bob.unread_count(&mut wire, BOB, &bad_history).await,
        ErrorCode::ContextChanged,
    );
    gateway
        .service
        .set_member_access(1, 1, 200, false)
        .await
        .unwrap();
    assert_error(
        bob.unread_count(&mut wire, BOB, &initial).await,
        ErrorCode::PermissionDenied,
    );
    gateway
        .service
        .set_member_access(1, 1, 200, true)
        .await
        .unwrap();
    assert_error(
        bob.unread_count(&mut wire, BOB, &initial).await,
        ErrorCode::ContextChanged,
    );

    let renewed = scope(&mut login(&gateway, fixture.bob, BOB).await).await;
    assert_ne!(initial.access_version, renewed.access_version);
    assert_eq!(bob.unread_count(&mut wire, BOB, &renewed).await.unwrap(), 2);
    gateway.stop().await;
}
