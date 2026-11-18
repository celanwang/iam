use super::super::removal::{committed, denied, generation, removed, Peer};
use super::*;
use iam_protocol::pb::{
    group_invite_result_ack::Value as Outcome, history_response, Empty, GroupInvitation,
    GroupInviteDecideRequest, GroupInviteInboxRequest, GroupInviteRequest, HistoryRequest,
    PageLimit, RetryAction, StreamContext,
};

const PENDING: Outcome = Outcome::Pending(Empty {});
const REJECTED: Outcome = Outcome::Rejected(Empty {});
const GUEST: &[u8] = b"group-invite-guest";
const SECOND_DEVICE: &[u8] = b"group-invite-guest-tablet";

struct InviteFixture {
    group: GroupFixture,
    guest: DeviceCredential,
    tablet: DeviceCredential,
}

impl InviteFixture {
    async fn new() -> Self {
        let group = GroupFixture::new().await;
        let guest = credential(&group.base, 1, 400, GUEST);
        let tablet = credential(&group.base, 1, 400, SECOND_DEVICE);
        Self {
            group,
            guest,
            tablet,
        }
    }

    async fn start(&self, mode: OpenMode, extra: &[DeviceCredential]) -> RunningGateway {
        let mut credentials = vec![self.guest, self.tablet];
        credentials.extend_from_slice(extra);
        self.group.start(mode, &credentials).await
    }

    async fn create(&self, gateway: &RunningGateway) -> (Peer, Peer, u64) {
        self.group.establish_friendships(gateway).await;
        let mut owner = Peer::open(gateway, self.group.base.alice, ALICE).await;
        let mut guest = Peer::open(gateway, self.guest.device_id, GUEST).await;
        let attempt = generate_id128();
        owner.apply(400, attempt).await;
        guest.accept(100, attempt).await;
        let group = group_id(owner.group(generate_id128()).await);
        (owner, guest, group)
    }

    async fn db(&self) -> MySqlPool {
        MySqlPool::connect(self.group.base.database.as_str())
            .await
            .unwrap()
    }
}

// Check membership and the account event stream independently of the invitation receipt.
async fn membership(db: &MySqlPool, group: u64, user: u64) -> (Option<u64>, Vec<Option<u64>>) {
    let current = sqlx::query_scalar(
        "SELECT account_sequence FROM members WHERE tenant_id = 1 AND conversation_id = ? AND user_id = ?",
    ).bind(group).bind(user).fetch_optional(db).await.unwrap();
    let events = sqlx::query_scalar(
        "SELECT access_version FROM account_events WHERE tenant_id = 1 AND conversation_id = ? AND user_id = ? ORDER BY account_sequence",
    ).bind(group).bind(user).fetch_all(db).await.unwrap();
    (current, events)
}

async fn expect_history_error(reader: &mut Peer, context: &StreamContext, code: ErrorCode) {
    let reply = reader
        .call(
            MsgType::History,
            &HistoryRequest {
                context: Some(context.clone()),
                limit: Some(PageLimit {
                    max_items: Some(4),
                    max_bytes: Some(16384),
                }),
                ..Default::default()
            },
        )
        .await;
    assert!(
        matches!(&reply, DecodedBody::HistoryAck(ack) if matches!(
            &ack.value, Some(history_response::Value::Error(error)) if error.code() == code
        )),
        "{reply:?}"
    );
}

impl Peer {
    async fn invite_reply(&mut self, group: u64, user: u64, key: Id128) -> DecodedBody {
        self.call(
            MsgType::GroupInvite,
            &GroupInviteRequest {
                conversation_id: Some(group),
                target_user_id: Some(user),
                request_id: Some(key.to_vec().into()),
            },
        )
        .await
    }

    async fn decision_reply(&mut self, group: u64, key: Id128, accept: bool) -> DecodedBody {
        self.call(
            MsgType::GroupInviteDecide,
            &GroupInviteDecideRequest {
                conversation_id: Some(group),
                request_id: Some(key.to_vec().into()),
                accept: Some(accept),
            },
        )
        .await
    }

    async fn invite(&mut self, group: u64, user: u64, key: Id128) -> Outcome {
        let reply = self.invite_reply(group, user, key).await;
        let DecodedBody::GroupInviteAck(ack) = reply else {
            panic!("expected GROUP_INVITE_ACK, got {reply:?}");
        };
        ack.value.unwrap()
    }

    async fn decide_invite(&mut self, group: u64, key: Id128, accept: bool) -> Outcome {
        decision_outcome(self.decision_reply(group, key, accept).await)
    }

    async fn invitations(&mut self, after: u64) -> Vec<GroupInvitation> {
        let reply = self
            .call(
                MsgType::GroupInviteInbox,
                &GroupInviteInboxRequest {
                    after_conversation_id: Some(after),
                },
            )
            .await;
        let DecodedBody::GroupInviteInboxAck(ack) = reply else {
            panic!("expected invitation inbox: {reply:?}")
        };
        assert!(ack.next_after_conversation_id.is_none());
        ack.invitations
    }
}

fn decision_outcome(reply: DecodedBody) -> Outcome {
    let DecodedBody::GroupInviteDecideAck(ack) = reply else {
        panic!("expected GROUP_INVITE_DECIDE_ACK, got {reply:?}");
    };
    ack.value.unwrap()
}

fn entry(group: u64, key: Id128) -> GroupInvitation {
    GroupInvitation {
        conversation_id: Some(group),
        inviter_user_id: Some(100),
        request_id: Some(key.to_vec().into()),
    }
}

fn invite_error(reply: DecodedBody, code: ErrorCode, action: RetryAction) {
    let DecodedBody::Error(error) = reply else {
        panic!("expected {code:?}, got {reply:?}")
    };
    let info = error.error.unwrap();
    assert_eq!((info.code(), info.action()), (code, action));
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_group_invites_lost_accept_commit_recovers_on_another_device_and_syncs_history() {
    check_accept_fault(CommitPhase::After).await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_group_invites_uncommitted_accept_remains_pending_until_retry() {
    check_accept_fault(CommitPhase::Before).await;
}

async fn check_accept_fault(phase: CommitPhase) {
    let mut f = InviteFixture::new().await;
    let db = f.db().await;
    let proxy = CommitProxy::start(&f.group.base.database).await;
    f.group.base.database = proxy.url.clone();
    let gateway = f.start(OpenMode::Initialize, &[]).await;
    let (mut owner, mut guest, group) = f.create(&gateway).await;
    let previous = owner.send(&message_key(group), "before invitation").await;
    committed(&previous, 1);
    let key = generate_id128();
    assert_eq!(owner.invite(group, 400, key).await, PENDING);
    assert_eq!(guest.invitations(0).await, [entry(group, key)]);
    let old_send = message_key(group);
    let refusal = guest.send(&old_send, "still pending").await;
    denied(&refusal);
    assert_eq!(membership(&db, group, 400).await, (None, vec![]));

    let (checkpoint, disconnect) = proxy.arm(phase);
    let accepting = tokio::spawn(async move { guest.decision_reply(group, key, true).await });
    timeout(WAIT, checkpoint).await.unwrap().unwrap().unwrap();
    let joined = (Some(2), vec![Some(2)]);
    let observed = membership(&db, group, 400).await;
    match phase {
        CommitPhase::Before => assert_eq!(observed, (None, vec![])),
        CommitPhase::After => assert_eq!(observed, joined),
    }
    disconnect.send(()).unwrap();
    invite_error(
        accepting.await.unwrap(),
        ErrorCode::ResultUnavailable,
        RetryAction::RetrySameRequest,
    );
    gateway.stop().await;

    let resumed = f.start(OpenMode::Resume, &[]).await;
    let disk = tempfile::tempdir().unwrap();
    let path = disk.path().join("new-device.db");
    let mut tablet = open_client(&path, f.tablet).await;
    let mut socket = resumed.client();
    let inbox = tablet
        .group_invite_inbox(&mut socket, SECOND_DEVICE, 0)
        .await
        .unwrap();
    match phase {
        CommitPhase::Before => assert_eq!(inbox.invitations, [entry(group, key)]),
        CommitPhase::After => assert!(inbox.invitations.is_empty()),
    }
    assert_eq!(
        tablet
            .decide_group_invite(&mut socket, SECOND_DEVICE, group, key, true)
            .await
            .unwrap()
            .value,
        Some(Outcome::ConversationId(group))
    );
    assert!(tablet
        .group_invite_inbox(&mut socket, SECOND_DEVICE, 0)
        .await
        .unwrap()
        .invitations
        .is_empty());
    drop(socket);
    let mut observer = observe(&path).await;
    let mut wire = ObservedTls::new(&resumed);
    sync_through(
        &mut tablet,
        &mut wire,
        &mut observer,
        SECOND_DEVICE,
        group,
        1,
    )
    .await;
    assert_eq!(wire.first_cursor(0), None);
    assert_eq!(wire.first_cursor(group), None);
    assert_group_view(&mut tablet, group, Some(1), 2).await;
    super::super::removal::assert_only_message(&tablet.messages(group).await.unwrap(), &previous);
    let group_state = tablet
        .list_conversations(0, 100)
        .await
        .unwrap()
        .items
        .into_iter()
        .find(|summary| summary.state.conversation_id == Some(group))
        .unwrap()
        .state;
    assert_eq!(group_state.access_version, Some(2));
    let mut guest = Peer::open(&resumed, f.guest.device_id, GUEST).await;
    assert_eq!(guest.send_status(&old_send).await, refusal);
    committed(&guest.send(&message_key(group), "joined").await, 2);
    let owner_client =
        open_client(&disk.path().join("owner.db"), f.group.base.credentials()[0]).await;
    assert_eq!(
        owner_client
            .invite_group_member(&mut resumed.client(), ALICE, group, 400, key)
            .await
            .unwrap()
            .value,
        Some(Outcome::ConversationId(group))
    );
    assert_eq!(membership(&db, group, 400).await, joined);
    resumed.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_group_invites_two_devices_accept_once_and_old_keys_cannot_restore_access() {
    let f = InviteFixture::new().await;
    let gateway = f.start(OpenMode::Initialize, &[]).await;
    let db = f.db().await;
    let (mut owner, mut guest, group) = f.create(&gateway).await;
    let mut tablet = Peer::open(&gateway, f.tablet.device_id, SECOND_DEVICE).await;
    let key = generate_id128();
    assert_eq!(owner.invite(group, 400, key).await, PENDING);
    let (one, two) = competing_requests(
        &db,
        guest.decision_reply(group, key, true),
        tablet.decision_reply(group, key, true),
    )
    .await;
    for reply in [one, two] {
        assert_eq!(decision_outcome(reply), Outcome::ConversationId(group));
    }
    let joined = membership(&db, group, 400).await;
    assert_eq!(joined, (Some(2), vec![Some(2)]));
    committed(
        &owner.send(&message_key(group), "first membership").await,
        1,
    );
    let original = guest.push_context(group).await;
    assert_eq!(original.access_version, Some(2));

    gateway
        .service
        .set_member_access(1, group, 400, false)
        .await
        .unwrap();
    assert_eq!(
        owner.invite(group, 400, key).await,
        Outcome::ConversationId(group)
    );
    assert_eq!(
        guest.decide_invite(group, key, false).await,
        Outcome::ConversationId(group)
    );
    denied(&guest.send(&message_key(group), "after revocation").await);
    expect_history_error(&mut guest, &original, ErrorCode::PermissionDenied).await;
    assert_eq!(
        membership(&db, group, 400).await,
        (None, vec![Some(2), None])
    );

    let fresh = generate_id128();
    assert_eq!(owner.invite(group, 400, fresh).await, PENDING);
    // An old acceptance cannot settle the fresh invitation.
    assert_eq!(
        tablet.decide_invite(group, key, true).await,
        Outcome::ConversationId(group)
    );
    assert_eq!(guest.invitations(0).await, [entry(group, fresh)]);
    denied(&guest.send(&message_key(group), "still pending again").await);
    assert_eq!(
        guest.decide_invite(group, fresh, true).await,
        Outcome::ConversationId(group)
    );
    committed(
        &owner.send(&message_key(group), "second membership").await,
        2,
    );
    let current = guest.push_context(group).await;
    assert_eq!(current.epoch, original.epoch);
    assert_eq!(current.access_version, Some(4));
    expect_history_error(&mut guest, &original, ErrorCode::ContextChanged).await;
    let mut sequences = guest
        .history(&current)
        .await
        .into_iter()
        .map(|m| m.position.unwrap().create_seq.unwrap())
        .collect::<Vec<_>>();
    sequences.sort_unstable();
    assert_eq!(sequences, [1, 2]);
    assert_eq!(
        membership(&db, group, 400).await,
        (Some(4), vec![Some(2), None, Some(4)])
    );
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_group_invites_check_friendship_and_both_block_directions_at_each_write() {
    let f = InviteFixture::new().await;
    let gateway = f.start(OpenMode::Initialize, &[]).await;
    let db = f.db().await;
    let (mut owner, mut guest, group) = f.create(&gateway).await;
    // Exercise a pending invitation independently of permission to issue a new one.
    let key = generate_id128();
    assert_eq!(owner.invite(group, 400, key).await, PENDING);
    for owner_blocks in [true, false] {
        if owner_blocks {
            owner.set_block(400, true).await;
        } else {
            guest.set_block(100, true).await;
        }
        expect_error(
            guest.decision_reply(group, key, true).await,
            ErrorCode::PermissionDenied,
        );
        assert_eq!(guest.invitations(0).await, [entry(group, key)]);
        let other_group = group_id(owner.group(generate_id128()).await);
        expect_error(
            owner.invite_reply(other_group, 400, generate_id128()).await,
            ErrorCode::PermissionDenied,
        );
        if owner_blocks {
            owner.set_block(400, false).await;
        } else {
            guest.set_block(100, false).await;
        }
    }
    let friendship = owner.status(400).await.unwrap();
    removed(
        guest.remove(100, generation(&friendship)).await,
        friendship.conversation_id.unwrap(),
    );
    expect_error(
        guest.decision_reply(group, key, true).await,
        ErrorCode::PermissionDenied,
    );
    let other_group = group_id(owner.group(generate_id128()).await);
    expect_error(
        owner.invite_reply(other_group, 400, generate_id128()).await,
        ErrorCode::PermissionDenied,
    );
    assert_eq!(membership(&db, group, 400).await, (None, vec![]));
    // Rejecting one's own invitation does not require the prior friendship to be restored.
    assert_eq!(guest.decide_invite(group, key, false).await, REJECTED);
    assert_eq!(owner.invite(group, 400, key).await, REJECTED);
    let renewal = generate_id128();
    owner.apply(400, renewal).await;
    guest.accept(100, renewal).await;
    assert_eq!(guest.decide_invite(group, key, true).await, REJECTED);
    let fresh = generate_id128();
    assert_eq!(owner.invite(group, 400, fresh).await, PENDING);
    assert_eq!(
        guest.decide_invite(group, fresh, true).await,
        Outcome::ConversationId(group)
    );
    guest.set_block(100, true).await;
    assert_eq!(
        owner.invite(group, 400, fresh).await,
        Outcome::ConversationId(group)
    );
    assert_eq!(
        guest.decide_invite(group, fresh, false).await,
        Outcome::ConversationId(group)
    );
    committed(
        &guest.send(&message_key(group), "existing membership").await,
        1,
    );
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_group_invites_only_owner_can_issue_and_only_recipient_can_settle() {
    let f = InviteFixture::new().await;
    let stranger = credential(&f.group.base, 1, 500, b"outsider");
    let gateway = f.start(OpenMode::Initialize, &[stranger]).await;
    let db = f.db().await;
    let (mut owner, mut guest, group) = f.create(&gateway).await;
    let mut member = Peer::open(&gateway, f.group.base.bob, BOB).await;
    let mut outsider = Peer::open(&gateway, stranger.device_id, b"outsider").await;
    let friendship = generate_id128();
    member.apply(400, friendship).await;
    guest.accept(200, friendship).await;
    expect_error(
        member.invite_reply(group, 400, generate_id128()).await,
        ErrorCode::PermissionDenied,
    );
    expect_error(
        outsider.invite_reply(group, 400, generate_id128()).await,
        ErrorCode::PermissionDenied,
    );
    expect_error(
        owner.invite_reply(group, 500, generate_id128()).await,
        ErrorCode::PermissionDenied,
    );
    let direct = owner.status(400).await.unwrap().conversation_id.unwrap();
    expect_error(
        owner.invite_reply(direct, 400, generate_id128()).await,
        ErrorCode::PermissionDenied,
    );
    let other_group = group_id(owner.group(generate_id128()).await);
    let key = generate_id128();
    assert_eq!(owner.invite(group, 400, key).await, PENDING);
    let busy = generate_id128();
    invite_error(
        owner.invite_reply(group, 400, busy).await,
        ErrorCode::InProgress,
        RetryAction::None,
    );
    expect_error(
        owner.invite_reply(group, 200, key).await,
        ErrorCode::IdempotencyConflict,
    );
    expect_error(
        owner.invite_reply(other_group, 400, key).await,
        ErrorCode::IdempotencyConflict,
    );
    for other in [&mut owner, &mut member, &mut outsider] {
        assert!(other.invitations(0).await.is_empty());
        for accept in [true, false] {
            expect_error(
                other.decision_reply(group, key, accept).await,
                ErrorCode::NotFound,
            );
        }
    }
    expect_error(
        guest.decision_reply(other_group, key, true).await,
        ErrorCode::NotFound,
    );
    assert_eq!(guest.invitations(0).await, [entry(group, key)]);
    let second = generate_id128();
    assert_eq!(owner.invite(other_group, 400, second).await, PENDING);
    let mut rows = vec![entry(group, key), entry(other_group, second)];
    rows.sort_by_key(|item| item.conversation_id);
    assert_eq!(guest.invitations(0).await, rows);
    assert_eq!(
        guest.invitations(rows[0].conversation_id.unwrap()).await,
        rows[1..]
    );
    assert_eq!(guest.decide_invite(group, key, false).await, REJECTED);
    invite_error(
        owner.invite_reply(group, 400, busy).await,
        ErrorCode::InProgress,
        RetryAction::None,
    );
    expect_error(
        outsider.decision_reply(group, key, true).await,
        ErrorCode::NotFound,
    );
    assert_eq!(guest.invitations(0).await, [entry(other_group, second)]);
    gateway
        .service
        .set_member_access(1, other_group, 100, false)
        .await
        .unwrap();
    expect_error(
        owner.invite_reply(other_group, 400, generate_id128()).await,
        ErrorCode::PermissionDenied,
    );
    expect_error(
        guest.decision_reply(other_group, second, true).await,
        ErrorCode::PermissionDenied,
    );
    assert_eq!(guest.invitations(0).await, [entry(other_group, second)]);
    assert_eq!(
        guest.decide_invite(other_group, second, false).await,
        REJECTED
    );
    assert_eq!(membership(&db, other_group, 400).await, (None, vec![]));
    assert_eq!(membership(&db, group, 400).await, (None, vec![]));
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_group_invites_last_seat_goes_to_one_recipient_and_other_stays_pending() {
    let base = empty_fixture().await;
    let [a, _] = base.credentials();
    let peers: Vec<_> = (1..=33)
        .map(|id| {
            let token = format!("invitation-seat-{id}");
            (credential(&base, 1, id, token.as_bytes()), token)
        })
        .collect();
    let credentials = std::iter::once(a).chain(peers.iter().map(|(device, _)| *device));
    let gateway = base
        .start_with_credentials(
            OpenMode::Initialize,
            Limits::default(),
            "127.0.0.1:0",
            credentials,
        )
        .await;
    let db = MySqlPool::connect(base.database.as_str()).await.unwrap();
    let mut socket = login(&gateway, a.device_id, ALICE).await;
    for (device, token) in &peers {
        befriend(
            &gateway,
            &mut socket,
            device.user_id + 1,
            *device,
            token.as_bytes(),
        )
        .await;
    }
    let initial: Vec<_> = (1..=30).collect();
    let group = group_id(create(&mut socket, 35, generate_id128(), &initial).await);
    drop(socket);
    let mut owner = Peer::open(&gateway, a.device_id, ALICE).await;
    let keys = [generate_id128(), generate_id128()];
    for (id, key) in [(31, keys[0]), (32, keys[1])] {
        assert_eq!(owner.invite(group, id, key).await, PENDING);
    }
    let mut left = Peer::open(&gateway, peers[30].0.device_id, peers[30].1.as_bytes()).await;
    let mut right = Peer::open(&gateway, peers[31].0.device_id, peers[31].1.as_bytes()).await;
    let (a, b) = competing_requests(
        &db,
        left.decision_reply(group, keys[0], true),
        right.decision_reply(group, keys[1], true),
    )
    .await;
    let (winner, loser, granted, overflow, waiting, retry_key) = match a {
        DecodedBody::GroupInviteDecideAck(_) => (31, 32, a, b, &mut right, keys[1]),
        _ => (32, 31, b, a, &mut left, keys[0]),
    };
    assert_eq!(decision_outcome(granted), Outcome::ConversationId(group));
    invite_error(overflow, ErrorCode::ResourceLimit, RetryAction::None);
    assert_eq!(
        membership(&db, group, winner).await,
        (Some(2), vec![Some(2)])
    );
    assert_eq!(membership(&db, group, loser).await, (None, vec![]));
    let roster = || {
        sqlx::query_scalar::<_, u64>("SELECT user_id FROM members WHERE tenant_id = 1 AND conversation_id = ? ORDER BY user_id").bind(group).fetch_all(&db)
    };
    let expected: Vec<u64> = (1..=30).chain([winner, 100]).collect();
    assert_eq!(roster().await.unwrap(), expected);
    assert_eq!(waiting.invitations(0).await, [entry(group, retry_key)]);
    invite_error(
        waiting.decision_reply(group, retry_key, true).await,
        ErrorCode::ResourceLimit,
        RetryAction::None,
    );
    denied(&waiting.send(&message_key(group), "no seat yet").await);
    let another = generate_id128();
    assert_eq!(owner.invite(group, 33, another).await, PENDING);
    gateway
        .service
        .set_member_access(1, group, 1, false)
        .await
        .unwrap();
    assert_eq!(
        waiting.decide_invite(group, retry_key, true).await,
        Outcome::ConversationId(group)
    );
    let expected: Vec<u64> = (2..=32).chain([100]).collect();
    assert_eq!(roster().await.unwrap(), expected);
    assert_eq!(
        membership(&db, group, loser).await,
        (Some(2), vec![Some(2)])
    );
    assert!(waiting.invitations(0).await.is_empty());
    let events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM account_events WHERE tenant_id = 1 AND conversation_id = ?",
    )
    .bind(group)
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(events, 34);
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_group_invites_same_ids_and_invitation_key_are_isolated_between_tenants() {
    let f = InviteFixture::new().await;
    let creds = [
        credential(&f.group.base, 2, 100, b"foreign-owner"),
        credential(&f.group.base, 2, 200, b"foreign-bob"),
        credential(&f.group.base, 2, 300, b"foreign-charlie"),
        credential(&f.group.base, 2, 400, b"foreign-guest"),
    ];
    let db = f.db().await;
    let gateway = f.start(OpenMode::Initialize, &creds).await;
    let (_, _, group) = f.create(&gateway).await;
    let mut foreign = Peer::open(&gateway, creds[0].device_id, b"foreign-owner").await;
    for (peer, token) in creds[1..].iter().zip([
        b"foreign-bob".as_slice(),
        b"foreign-charlie",
        b"foreign-guest",
    ]) {
        let key = generate_id128();
        foreign.apply(peer.user_id, key).await;
        Peer::open(&gateway, peer.device_id, token)
            .await
            .accept(100, key)
            .await;
    }
    let other_group = group_id(foreign.group(generate_id128()).await);
    gateway.stop().await;
    // Both groups are still empty of messages and invitations. Arrange an exact ID collision
    // in the isolated, stopped catalog so a query missing tenant_id cannot pass accidentally.
    let mut tx = db.begin().await.unwrap();
    for (table, count) in [
        ("conversations", 1),
        ("members", 3),
        ("account_events", 3),
        ("group_creations", 1),
    ] {
        let affected = sqlx::query(&format!(
            "UPDATE {table} SET conversation_id = ? WHERE tenant_id = 2 AND conversation_id = ?"
        ))
        .bind(group)
        .bind(other_group)
        .execute(&mut *tx)
        .await
        .unwrap()
        .rows_affected();
        assert_eq!(affected, count);
    }
    tx.commit().await.unwrap();
    let resumed = f.start(OpenMode::Resume, &creds).await;
    let mut owner = Peer::open(&resumed, f.group.base.alice, ALICE).await;
    let mut guest = Peer::open(&resumed, f.guest.device_id, GUEST).await;
    let mut foreign = Peer::open(&resumed, creds[0].device_id, b"foreign-owner").await;
    let mut foreign_guest = Peer::open(&resumed, creds[3].device_id, b"foreign-guest").await;
    let key = generate_id128();
    assert_eq!(owner.invite(group, 400, key).await, PENDING);
    assert!(foreign_guest.invitations(0).await.is_empty());
    expect_error(
        foreign_guest.decision_reply(group, key, true).await,
        ErrorCode::NotFound,
    );
    assert_eq!(foreign.invite(group, 400, key).await, PENDING);
    assert_eq!(
        foreign_guest.decide_invite(group, key, false).await,
        REJECTED
    );
    assert_eq!(guest.invitations(0).await, [entry(group, key)]);
    assert_eq!(
        guest.decide_invite(group, key, true).await,
        Outcome::ConversationId(group)
    );
    assert_eq!(foreign.invite(group, 400, key).await, REJECTED);
    committed(
        &guest.send(&message_key(group), "local acceptance").await,
        1,
    );
    denied(
        &foreign_guest
            .send(&message_key(group), "foreign rejection")
            .await,
    );
    resumed.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_group_invites_pending_key_cannot_grant_access_after_an_intervening_membership() {
    let f = InviteFixture::new().await;
    let gateway = f.start(OpenMode::Initialize, &[]).await;
    let db = f.db().await;
    let (mut owner, mut guest, group) = f.create(&gateway).await;
    let first = generate_id128();
    assert_eq!(owner.invite(group, 400, first).await, PENDING);
    assert_eq!(
        guest.decide_invite(group, first, true).await,
        Outcome::ConversationId(group)
    );
    gateway
        .service
        .set_member_access(1, group, 400, false)
        .await
        .unwrap();
    let earlier = generate_id128();
    assert_eq!(owner.invite(group, 400, earlier).await, PENDING);
    // Give the member access independently, then end that intervening membership.
    gateway
        .service
        .set_member_access(1, group, 400, true)
        .await
        .unwrap();
    gateway
        .service
        .set_member_access(1, group, 400, false)
        .await
        .unwrap();
    assert!(guest.invitations(0).await.is_empty());
    expect_error(
        guest.decision_reply(group, earlier, true).await,
        ErrorCode::ContextChanged,
    );
    expect_error(
        owner.invite_reply(group, 400, earlier).await,
        ErrorCode::ContextChanged,
    );
    assert_eq!(
        membership(&db, group, 400).await,
        (None, vec![Some(2), None, Some(4), None])
    );
    let new_key = generate_id128();
    assert_eq!(owner.invite(group, 400, new_key).await, PENDING);
    expect_error(
        guest.decision_reply(group, earlier, true).await,
        ErrorCode::ContextChanged,
    );
    assert_eq!(guest.invitations(0).await, [entry(group, new_key)]);
    assert_eq!(
        guest.decide_invite(group, new_key, true).await,
        Outcome::ConversationId(group)
    );
    assert_eq!(
        membership(&db, group, 400).await,
        (Some(6), vec![Some(2), None, Some(4), None, Some(6)])
    );
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_group_invites_rejoin_after_public_leave_and_owner_removal() {
    let f = InviteFixture::new().await;
    let gateway = f.start(OpenMode::Initialize, &[]).await;
    let db = f.db().await;
    let (mut writer, _, group) = f.create(&gateway).await;
    let prior = writer
        .send(&message_key(group), "before all memberships")
        .await;
    committed(&prior, 1);
    drop(writer);
    let disk = tempfile::tempdir().unwrap();
    let owner = open_client(&disk.path().join("owner.db"), f.group.base.credentials()[0]).await;
    let guest = open_client(&disk.path().join("guest.db"), f.guest).await;
    let mut saved_leave = None;
    let mut saved_removal = None;
    for version in [2, 4, 6] {
        let key = generate_id128();
        assert_eq!(
            owner
                .invite_group_member(&mut gateway.client(), ALICE, group, 400, key)
                .await
                .unwrap()
                .value,
            Some(PENDING)
        );
        assert_eq!(
            guest
                .decide_group_invite(&mut gateway.client(), GUEST, group, key, true)
                .await
                .unwrap()
                .value,
            Some(Outcome::ConversationId(group))
        );
        let roster = guest
            .group_members(&mut gateway.client(), GUEST, group)
            .await
            .unwrap();
        let member = *roster
            .members
            .iter()
            .find(|m| m.user_id == Some(400))
            .unwrap();
        assert_eq!(member.access_version, Some(version));
        if version == 6 {
            break;
        }
        if version == 2 {
            let path = disk.path().join("guest-tablet.db");
            let mut tablet = open_client(&path, f.tablet).await;
            let mut observer = observe(&path).await;
            let mut wire = ObservedTls::new(&gateway);
            sync_through(
                &mut tablet,
                &mut wire,
                &mut observer,
                SECOND_DEVICE,
                group,
                1,
            )
            .await;
            drop(wire);
            let state = tablet
                .list_conversations(0, 10)
                .await
                .unwrap()
                .items
                .into_iter()
                .find(|summary| summary.state.conversation_id == Some(group))
                .unwrap()
                .state;
            assert_eq!(state.access_version, member.access_version);
            tablet
                .leave_group(&mut gateway.client(), SECOND_DEVICE, &state)
                .await
                .unwrap();
            saved_leave = Some(state);
        } else {
            owner
                .remove_group_member(&mut gateway.client(), ALICE, &roster, &member)
                .await
                .unwrap();
            saved_removal = Some((roster, member));
        }
        assert_eq!(
            guest
                .decide_group_invite(&mut gateway.client(), GUEST, group, key, true)
                .await
                .unwrap()
                .value,
            Some(Outcome::ConversationId(group))
        );
        assert_eq!(
            owner
                .invite_group_member(&mut gateway.client(), ALICE, group, 400, key)
                .await
                .unwrap()
                .value,
            Some(Outcome::ConversationId(group))
        );
        assert!(membership(&db, group, 400).await.0.is_none());
        let mut outside = Peer::open(&gateway, f.guest.device_id, GUEST).await;
        denied(&outside.send(&message_key(group), "outside group").await);
    }
    // Both old commands replay their completed departure, including from another device.
    guest
        .leave_group(&mut gateway.client(), GUEST, &saved_leave.unwrap())
        .await
        .unwrap();
    let (roster, member) = saved_removal.unwrap();
    owner
        .remove_group_member(&mut gateway.client(), ALICE, &roster, &member)
        .await
        .unwrap();
    assert_eq!(
        membership(&db, group, 400).await,
        (Some(6), vec![Some(2), None, Some(4), None, Some(6)])
    );
    let mut reader = Peer::open(&gateway, f.guest.device_id, GUEST).await;
    let mut writer = Peer::open(&gateway, f.group.base.alice, ALICE).await;
    committed(&writer.send(&message_key(group), "after rejoin").await, 2);
    let context = reader.push_context(group).await;
    assert_eq!(context.access_version, Some(6));
    let history = reader.history(&context).await;
    assert_eq!(history.len(), 2);
    let old = history
        .into_iter()
        .find(|view| view.position.as_ref().unwrap().create_seq == Some(1))
        .unwrap();
    super::super::removal::assert_only_message(&[old], &prior);
    gateway.stop().await;
}
