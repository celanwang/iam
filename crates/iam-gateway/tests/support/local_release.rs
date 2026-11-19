use super::*;
use iam_protocol::pb::{friend_attempt_ack, group_invite_result_ack};
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::process::{Child, ChildStderr, Command};

const CHARLIE: &[u8] = b"local-release-charlie";

struct ProcessConfig {
    directory: tempfile::TempDir,
    roots: rustls::RootCertStore,
    database: Url,
    store: Id128,
    users: [Identity; 3],
}

impl ProcessConfig {
    async fn new() -> Self {
        Self::in_database(support::isolated_database().await, generate_id128())
    }

    fn in_database(database: Url, store: Id128) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let identity = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(identity.cert.der().clone()).unwrap();
        std::fs::write(directory.path().join("chain.pem"), identity.cert.pem()).unwrap();
        std::fs::write(
            directory.path().join("key.pem"),
            identity.signing_key.serialize_pem(),
        )
        .unwrap();
        let users = [100, 200, 300].map(|user_id| Identity {
            tenant_id: 1,
            user_id,
            device_id: generate_id128(),
        });
        let expires = iam_protocol::id128_timestamp_ms(&generate_id128()) + 600_000;
        let mut credentials = String::new();
        for (identity, token) in users.iter().zip([ALICE, BOB, CHARLIE]) {
            writeln!(
                credentials,
                "{} {} {} {} {}",
                identity.tenant_id,
                identity.user_id,
                hex::encode(identity.device_id),
                expires,
                hex::encode(Sha256::digest(token)),
            )
            .unwrap();
        }
        std::fs::write(directory.path().join("devices.txt"), credentials).unwrap();
        Self {
            directory,
            roots,
            database,
            store,
            users,
        }
    }

    fn file(&self, name: &str) -> PathBuf {
        self.directory.path().join(name)
    }

    fn command(&self, mode: &str, bind: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_iam-gateway"));
        command
            .env("IAM_TLS_CERT", self.file("chain.pem"))
            .env("IAM_TLS_KEY", self.file("key.pem"))
            .env("IAM_DEVICE_CREDENTIALS", self.file("devices.txt"))
            .env("IAM_GATEWAY_BIND", bind)
            .env("IAM_TIDB_URL", self.database.as_str())
            .env("IAM_STORE_ID", hex::encode(self.store))
            .env("IAM_TIDB_MODE", mode)
            .kill_on_drop(true);
        command
    }

    async fn start(&self, mode: &str, bind: &str) -> (Child, String) {
        Self::start_command(self.command(mode, bind)).await
    }

    async fn start_command(mut command: Command) -> (Child, String) {
        let mut child = command.stdout(Stdio::piped()).spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let line = timeout(WAIT, BufReader::new(stdout).lines().next_line())
            .await
            .expect("gateway did not report readiness")
            .unwrap()
            .expect("gateway stopped before listening");
        let address = line
            .strip_prefix("IAM gateway listening on ")
            .expect("unexpected gateway readiness line")
            .to_owned();
        (child, address)
    }

    fn transport(&self, address: &str) -> TlsTransport {
        TlsTransport::new(
            address.to_owned(),
            "localhost".try_into().unwrap(),
            self.roots.clone(),
        )
    }
}

async fn gateway_log(logs: &mut Lines<BufReader<ChildStderr>>, wait: Duration) -> String {
    timeout(wait, logs.next_line())
        .await
        .expect("gateway did not report its state")
        .unwrap()
        .expect("gateway exited without reporting its state")
}

async fn observer(path: &Path) -> SqliteConnection {
    SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(path).read_only(true))
        .await
        .unwrap()
}

async fn connect_as(
    config: &ProcessConfig,
    address: &str,
    identity: Identity,
    token: &[u8],
) -> TlsTransport {
    let mut transport = config.transport(address);
    transport.reconnect().await.unwrap();
    assert!(matches!(
        auth(&mut transport, 1, identity.device_id, token).await,
        auth_response::Value::Session(_)
    ));
    transport
}

async fn establish(
    config: &ProcessConfig,
    address: &str,
    alice: &Client,
    peer: &Client,
    peer_id: u64,
    peer_token: &[u8],
) -> u64 {
    let key = generate_id128();
    let mut alice_tls = config.transport(address);
    let mut peer_tls = config.transport(address);
    let attempt = alice
        .apply_friend(&mut alice_tls, ALICE, peer_id, key)
        .await
        .unwrap();
    assert!(matches!(
        attempt.value,
        Some(friend_attempt_ack::Value::Pending(_))
    ));
    let inbox = peer
        .friend_inbox(&mut peer_tls, peer_token, 0)
        .await
        .unwrap();
    assert!(inbox.requests.iter().any(|pending| {
        pending.from_user_id == Some(100) && pending.request_id.as_deref() == Some(key.as_slice())
    }));
    match peer
        .decide_friend(&mut peer_tls, peer_token, 100, key, true)
        .await
        .unwrap()
        .value
    {
        Some(friend_attempt_ack::Value::ConversationId(id)) => id,
        other => panic!("expected an accepted friendship, got {other:?}"),
    }
}

async fn send_from_local_queue(
    client: &mut Client,
    path: &Path,
    mut transport: TlsTransport,
    token: &[u8],
    conversation: u64,
) {
    let key = client
        .enqueue(conversation, "group remains available".to_owned())
        .await
        .unwrap();
    let mut db = observer(path).await;
    let (stop, mut submissions) = mpsc::channel(1);
    let saved = async {
        loop {
            let done: bool = sqlx::query_scalar(
                "SELECT outcome IS NOT NULL FROM outbox WHERE conversation = ? AND client_id = ?",
            )
            .bind(conversation.to_be_bytes().to_vec())
            .bind(key.client_msg_id.as_deref().unwrap())
            .fetch_one(&mut db)
            .await
            .unwrap();
            if done {
                drop(stop);
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    };
    let (result, ()) = timeout(WAIT, async {
        tokio::join!(
            client.run(&mut transport, token, Options::default(), &mut submissions),
            saved
        )
    })
    .await
    .expect("queued message did not reach a saved result");
    result.unwrap();
    assert!(matches!(
        client
            .outcome(&key)
            .await
            .unwrap()
            .and_then(|outcome| outcome.value),
        Some(send_outcome::Value::Committed(_))
    ));
}

#[tokio::test]
#[ignore = "requires real local TiDB and starts the gateway binary"]
async fn tidb_tls_local_release_recovers_after_gateway_process_exit() {
    let config = ProcessConfig::new().await;
    let [alice_identity, bob_identity, charlie_identity] = config.users;
    let (mut gateway, address) = config.start("fresh", "127.0.0.1:0").await;
    let alice_path = config.file("alice.db");
    let mut alice = Client::open(&alice_path, alice_identity).await.unwrap();
    let bob_path = config.file("bob.db");
    let mut bob = Client::open(&bob_path, bob_identity).await.unwrap();
    let charlie = Client::open(config.file("charlie.db"), charlie_identity)
        .await
        .unwrap();
    let direct = establish(&config, &address, &alice, &bob, 200, BOB).await;
    establish(&config, &address, &alice, &charlie, 300, CHARLIE).await;
    let group = alice
        .create_group(
            &mut config.transport(&address),
            ALICE,
            generate_id128(),
            &[200, 300],
        )
        .await
        .unwrap();

    let mut sender = connect_as(&config, &address, alice_identity, ALICE).await;
    let first = message_key(direct);
    send(&mut sender, 2, first.clone(), "first direct").await;
    let mut bob_db = observer(&bob_path).await;
    let mut bob_wire = ObservedTls {
        tls: config.transport(&address),
        syncs: Vec::new(),
    };
    let cursor = sync_through(&mut bob, &mut bob_wire, &mut bob_db, BOB, direct, 1).await;
    let account_cursor = saved_cursor(&mut bob_db, 0, 2).await.unwrap();
    drop(bob);
    drop(bob_wire);

    send(&mut sender, 3, message_key(direct), "while Bob is offline").await;
    send(&mut sender, 4, message_key(group), "first group").await;
    let first_status = SendStatusRequest { key: Some(first) };
    let DecodedBody::SendStatusAck(before) =
        request(&mut sender, 5, MsgType::SendStatus, &first_status).await
    else {
        panic!("expected SEND_STATUS_ACK before restart");
    };
    assert!(matches!(
        before
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.value.as_ref()),
        Some(send_outcome::Value::Committed(_))
    ));

    gateway.kill().await.unwrap();
    drop(sender);
    let (mut resumed, resumed_address) = config.start("resume", &address).await;
    assert_eq!(resumed_address, address);
    let mut bob = Client::open(&bob_path, bob_identity).await.unwrap();
    let mut bob_wire = ObservedTls {
        tls: config.transport(&address),
        syncs: Vec::new(),
    };
    sync_through(&mut bob, &mut bob_wire, &mut bob_db, BOB, direct, 2).await;
    assert_eq!(bob_wire.first_cursor(0), Some(account_cursor.as_slice()));
    assert_eq!(bob_wire.first_cursor(direct), Some(cursor.as_slice()));
    assert_eq!(bob.messages(direct).await.unwrap().len(), 2);
    assert_eq!(bob.messages(group).await.unwrap().len(), 1);
    drop(bob_wire);
    let group_state = bob
        .list_conversations(0, 100)
        .await
        .unwrap()
        .items
        .into_iter()
        .find(|item| item.state.conversation_id == Some(group))
        .unwrap()
        .state;
    let mut bob_tls = config.transport(&address);
    assert_eq!(
        bob.unread_count(&mut bob_tls, BOB, &group_state)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        bob.advance_read_position(&mut bob_tls, BOB, &group_state, 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        bob.unread_count(&mut bob_tls, BOB, &group_state)
            .await
            .unwrap(),
        0
    );

    let mut sender = connect_as(&config, &address, alice_identity, ALICE).await;
    let DecodedBody::SendStatusAck(after) =
        request(&mut sender, 2, MsgType::SendStatus, &first_status).await
    else {
        panic!("expected SEND_STATUS_ACK after restart");
    };
    assert_eq!(after.outcome, before.outcome);
    assert!(bob
        .set_user_block(&mut bob_tls, BOB, 100, generate_id128(), true)
        .await
        .unwrap());
    let DecodedBody::SendAck(blocked) = request(
        &mut sender,
        3,
        MsgType::Send,
        &SendRequest {
            key: Some(message_key(direct)),
            content: Some(MessageContent {
                value: Some(message_content::Value::Text(TextContent {
                    text: Some("blocked direct".to_owned()),
                })),
            }),
        },
    )
    .await
    else {
        panic!("expected a send outcome after applying a block");
    };
    assert!(matches!(
        blocked.outcome.and_then(|outcome| outcome.value),
        Some(send_outcome::Value::RejectedFinal(error)) if error.code() == ErrorCode::PermissionDenied
    ));
    drop(sender);
    send_from_local_queue(
        &mut alice,
        &alice_path,
        config.transport(&address),
        ALICE,
        group,
    )
    .await;

    let roster = bob.group_members(&mut bob_tls, BOB, group).await.unwrap();
    assert_eq!(roster.owner_user_id, Some(alice_identity.user_id));
    assert_eq!(roster.members.len(), 3);
    bob.leave_group(&mut bob_tls, BOB, &group_state)
        .await
        .unwrap();
    assert!(matches!(
        bob.group_members(&mut bob_tls, BOB, group).await,
        Err(iam_client::Error::Server(error)) if error.code() == ErrorCode::PermissionDenied
    ));
    let mut bob_wire = ObservedTls {
        tls: config.transport(&address),
        syncs: Vec::new(),
    };
    sync_through(&mut bob, &mut bob_wire, &mut bob_db, BOB, 0, 3).await;
    assert!(bob.messages(group).await.unwrap().is_empty());
    assert_eq!(
        bob.list_conversations(0, 100)
            .await
            .unwrap()
            .items
            .into_iter()
            .map(|item| item.state.conversation_id.unwrap())
            .collect::<Vec<_>>(),
        [direct]
    );
    drop(bob_wire);

    assert!(!bob
        .set_user_block(&mut bob_tls, BOB, 100, generate_id128(), false)
        .await
        .unwrap());
    let invitation = generate_id128();
    let mut alice_tls = config.transport(&address);
    assert!(matches!(
        alice
            .invite_group_member(&mut alice_tls, ALICE, group, 200, invitation)
            .await
            .unwrap()
            .value,
        Some(group_invite_result_ack::Value::Pending(_))
    ));
    let pending = bob.group_invite_inbox(&mut bob_tls, BOB, 0).await.unwrap();
    assert!(pending.invitations.iter().any(|invite| {
        invite.conversation_id == Some(group)
            && invite.inviter_user_id == Some(alice_identity.user_id)
            && invite.request_id.as_deref() == Some(invitation.as_slice())
    }));
    assert_eq!(
        bob.decide_group_invite(&mut bob_tls, BOB, group, invitation, true)
            .await
            .unwrap()
            .value,
        Some(group_invite_result_ack::Value::ConversationId(group))
    );

    let mut bob_wire = ObservedTls {
        tls: config.transport(&address),
        syncs: Vec::new(),
    };
    sync_through(&mut bob, &mut bob_wire, &mut bob_db, BOB, group, 2).await;
    assert_eq!(bob.messages(group).await.unwrap().len(), 2);
    drop(bob_wire);
    bob.leave_group(&mut bob_tls, BOB, &group_state)
        .await
        .unwrap();
    let rejoined = bob.group_members(&mut bob_tls, BOB, group).await.unwrap();
    let bob_membership = rejoined
        .members
        .iter()
        .find(|member| member.user_id == Some(bob_identity.user_id))
        .expect("old leave must not remove the rejoined member");
    assert_ne!(bob_membership.access_version, group_state.access_version);
    resumed.kill().await.unwrap();
}

#[tokio::test]
#[ignore = "requires real local TiDB and starts the gateway binary"]
async fn tidb_tls_local_release_preflights_before_initializing() {
    let config = ProcessConfig::new().await;
    let wrong_key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let wrong_key_path = config.file("wrong-key.pem");
    std::fs::write(&wrong_key_path, wrong_key.signing_key.serialize_pem()).unwrap();
    let bad_credentials = config.file("bad-devices.txt");
    std::fs::write(&bad_credentials, "invalid credentials\n").unwrap();
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let mut tls = config.command("fresh", "127.0.0.1:0");
    tls.env("IAM_TLS_KEY", wrong_key_path);
    let mut credentials = config.command("fresh", "127.0.0.1:0");
    credentials.env("IAM_DEVICE_CREDENTIALS", bad_credentials);
    let binding = config.command("fresh", &occupied.local_addr().unwrap().to_string());
    for (mut command, reason) in [
        (tls, "invalid IAM_TLS_CERT or IAM_TLS_KEY"),
        (credentials, "invalid device credential on line 1"),
        (binding, "cannot bind IAM_GATEWAY_BIND"),
    ] {
        let failed = timeout(WAIT, command.output()).await.unwrap().unwrap();
        let error = String::from_utf8_lossy(&failed.stderr);
        assert!(!failed.status.success(), "{error}");
        assert!(error.contains(reason), "{error}");
        assert!(failed.stdout.is_empty());
    }

    let db = sqlx::MySqlPool::connect(config.database.as_str())
        .await
        .unwrap();
    let tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()",
    )
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(tables, 0, "startup preflight mutated an empty database");
    let (mut gateway, address) = config.start("fresh", "127.0.0.1:0").await;
    connect_as(&config, &address, config.users[0], ALICE).await;
    gateway.kill().await.unwrap();

    let mut wrong_store = config.command("resume", &address);
    wrong_store.env("IAM_STORE_ID", hex::encode(generate_id128()));
    let failed = timeout(WAIT, wrong_store.output()).await.unwrap().unwrap();
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("protocol store ID does not match"));
    assert!(failed.stdout.is_empty());
    let failed = timeout(WAIT, config.command("fresh", &address).output())
        .await
        .unwrap()
        .unwrap();
    assert!(!failed.status.success(), "fresh accepted an existing store");
    assert!(failed.stdout.is_empty());
    let (mut resumed, _) = config.start("resume", &address).await;
    connect_as(&config, &address, config.users[0], ALICE).await;
    resumed.kill().await.unwrap();
}

#[tokio::test]
#[ignore = "requires real local TiDB and starts the gateway binary"]
async fn tidb_tls_local_release_reports_storage_outage_and_recovery() {
    let config = ProcessConfig::new().await;
    let proxy = CommitProxy::start(&config.database).await;
    let proxy_address = ("127.0.0.1", proxy.url.port().unwrap());
    let mut command = config.command("fresh", "127.0.0.1:0");
    command
        .env("IAM_TIDB_URL", proxy.url.as_str())
        .stderr(Stdio::piped());
    let (mut gateway, address) = ProcessConfig::start_command(command).await;
    let mut logs = BufReader::new(gateway.stderr.take().unwrap()).lines();
    assert_eq!(gateway_log(&mut logs, WAIT).await, "IAM gateway ready");

    let mut session = connect_as(&config, &address, config.users[0], ALICE).await;
    drop(proxy);
    let unhealthy = gateway_log(&mut logs, WAIT * 2).await;
    assert!(
        unhealthy.starts_with("IAM gateway not ready reason="),
        "{unhealthy}"
    );
    assert!(gateway.try_wait().unwrap().is_none());
    let listener = TcpListener::bind(proxy_address).await.unwrap();
    let _proxy = CommitProxy::start_on(&config.database, listener);
    assert_eq!(gateway_log(&mut logs, WAIT).await, "IAM gateway ready");
    assert!(matches!(
        request(
            &mut session,
            2,
            MsgType::Heartbeat,
            &iam_protocol::pb::HeartbeatRequest {}
        )
        .await,
        DecodedBody::HeartbeatAck(_)
    ));
    gateway.kill().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires real local TiDB and starts the gateway binary"]
async fn tidb_tls_local_release_sigterm_before_commit_reports_drain_timeout() {
    check_sigterm(CommitPhase::Before).await;
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires real local TiDB and starts the gateway binary"]
async fn tidb_tls_local_release_sigterm_after_commit_recovers_lost_receipt() {
    check_sigterm(CommitPhase::After).await;
}

#[cfg(unix)]
async fn check_sigterm(phase: CommitPhase) {
    let config = ProcessConfig::new().await;
    let [alice, bob, _] = config.users;
    let proxy = CommitProxy::start(&config.database).await;
    let mut command = config.command("fresh", "127.0.0.1:0");
    command
        .env("IAM_TIDB_URL", proxy.url.as_str())
        .stderr(Stdio::piped());
    let (mut gateway, address) = ProcessConfig::start_command(command).await;
    let mut logs = BufReader::new(gateway.stderr.take().unwrap()).lines();
    assert_eq!(gateway_log(&mut logs, WAIT).await, "IAM gateway ready");
    let alice_client = Client::open(config.file("alice.db"), alice).await.unwrap();
    let bob_path = config.file("bob.db");
    let mut bob_client = Client::open(&bob_path, bob).await.unwrap();
    let direct = establish(&config, &address, &alice_client, &bob_client, 200, BOB).await;

    let mut sender = connect_as(&config, &address, alice, ALICE).await;
    send(&mut sender, 2, message_key(direct), "before drain").await;
    let mut db = observer(&bob_path).await;
    let mut wire = ObservedTls {
        tls: config.transport(&address),
        syncs: Vec::new(),
    };
    let cursor = sync_through(&mut bob_client, &mut wire, &mut db, BOB, direct, 1).await;
    drop(wire);

    let key = message_key(direct);
    let send_body = send_request(key.clone(), "during drain");
    let frame = encode_frame(MsgType::Send, &Head { req_id: Some(3) }, &send_body).unwrap();
    let (reached, release) = proxy.arm(phase);
    let sending = tokio::spawn(async move { sender.exchange(frame).await });
    timeout(WAIT, reached).await.unwrap().unwrap().unwrap();
    assert!(Command::new("kill")
        .args(["-TERM", &gateway.id().unwrap().to_string()])
        .status()
        .await
        .unwrap()
        .success());
    let started = gateway_log(&mut logs, WAIT).await;
    assert!(started.starts_with("IAM gateway draining "), "{started}");
    assert!(started.ends_with(" in_flight=1"), "{started}");
    assert!(gateway.try_wait().unwrap().is_none());
    assert!(tokio::net::TcpStream::connect(&address).await.is_err());

    let (drained, expected_exit) = match phase {
        CommitPhase::Before => ("IAM gateway drain timed out ", false),
        CommitPhase::After => {
            release.send(()).unwrap();
            ("IAM gateway drained ", true)
        }
    };
    let finished = gateway_log(&mut logs, WAIT * 2).await;
    assert!(finished.starts_with(drained), "{finished}");
    if !expected_exit {
        assert!(finished.ends_with(" in_flight=1"), "{finished}");
    }
    let exit = timeout(WAIT, gateway.wait()).await.unwrap().unwrap();
    assert_eq!(exit.success(), expected_exit);

    let response = timeout(WAIT, sending).await.unwrap().unwrap();
    match phase {
        CommitPhase::Before => assert!(response.is_err()),
        CommitPhase::After => assert!(matches!(
            decode(&response.unwrap()),
            DecodedBody::SendAck(ack)
                if matches!(ack.outcome.as_ref().and_then(|outcome| outcome.value.as_ref()),
                    Some(send_outcome::Value::Unresolved(result))
                        if result.error.as_ref().unwrap().code() == ErrorCode::ResultUnavailable)
        )),
    }
    drop(proxy);

    let (mut resumed, _) = config.start("resume", &address).await;
    let mut sender = connect_as(&config, &address, alice, ALICE).await;
    let DecodedBody::SendStatusAck(status) = request(
        &mut sender,
        2,
        MsgType::SendStatus,
        &SendStatusRequest { key: Some(key) },
    )
    .await
    else {
        panic!("expected SEND_STATUS_ACK");
    };
    let status = status.outcome.unwrap().value.unwrap();
    assert!(match phase {
        CommitPhase::Before => matches!(&status, send_outcome::Value::Unresolved(_)),
        CommitPhase::After => matches!(&status, send_outcome::Value::Committed(_)),
    });
    let DecodedBody::SendAck(replay) = request(&mut sender, 3, MsgType::Send, &send_body).await
    else {
        panic!("expected SEND_ACK");
    };
    let Some(send_outcome::Value::Committed(receipt)) = replay.outcome.unwrap().value else {
        panic!("send did not settle after restart");
    };
    if let send_outcome::Value::Committed(original) = status {
        assert_eq!(original, receipt);
    }
    assert_eq!(receipt.position.unwrap().create_seq, Some(2));

    wire = ObservedTls {
        tls: config.transport(&address),
        syncs: Vec::new(),
    };
    sync_through(&mut bob_client, &mut wire, &mut db, BOB, direct, 2).await;
    assert_eq!(wire.first_cursor(direct), Some(cursor.as_slice()));
    assert_eq!(bob_client.messages(direct).await.unwrap().len(), 2);
    resumed.kill().await.unwrap();
}

#[tokio::test]
#[ignore = "requires real local TiDB and starts the gateway binary"]
async fn tidb_tls_local_release_upgrades_old_store_and_recovers_receipt() {
    let fixture = Fixture::new().await;
    let config = ProcessConfig::in_database(fixture.database.clone(), fixture.store);
    let [alice, bob, _] = config.users;
    let db = sqlx::MySqlPool::connect(config.database.as_str())
        .await
        .unwrap();
    decisions::old_friend_store(&fixture, &db).await;

    let key = message_key(42);
    let message_id = generate_id128();
    let accepted_at = iam_protocol::id128_timestamp_ms(&message_id) as i64;
    let mut tx = db.begin().await.unwrap();
    sqlx::query(
        "INSERT INTO messages
         (tenant_id, conversation_id, message_id, user_id, device_id, client_msg_id,
          `sequence`, `text`, accepted_at)
         VALUES (1, 42, ?, 100, ?, ?, 1, ?, ?)",
    )
    .bind(message_id.as_slice())
    .bind(alice.device_id.as_slice())
    .bind(key.client_msg_id.as_deref().unwrap())
    .bind(b"before upgrade".as_slice())
    .bind(accepted_at)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE conversations SET next_sequence = 2 WHERE tenant_id = 1 AND conversation_id = 42",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let failed = timeout(WAIT, config.command("resume", "127.0.0.1:0").output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !failed.status.success(),
        "resume must require explicit upgrade"
    );
    for _ in 0..2 {
        TidbService::upgrade_local(config.database.as_str(), config.store)
            .await
            .unwrap();
    }

    let (mut gateway, address) = config.start("resume", "127.0.0.1:0").await;
    let mut sender = connect_as(&config, &address, alice, ALICE).await;
    let DecodedBody::SendStatusAck(status) = request(
        &mut sender,
        2,
        MsgType::SendStatus,
        &SendStatusRequest { key: Some(key) },
    )
    .await
    else {
        panic!("expected the saved receipt from the upgraded store");
    };
    let Some(send_outcome::Value::Committed(receipt)) =
        status.outcome.and_then(|outcome| outcome.value)
    else {
        panic!("original send no longer committed");
    };
    assert_eq!(
        receipt.r#ref.unwrap().message_id.as_deref(),
        Some(message_id.as_slice())
    );
    assert_eq!(receipt.position.unwrap().create_seq, Some(1));
    assert_eq!(receipt.accepted_at_ms, Some(accepted_at));

    let path = config.file("upgraded-bob.db");
    let mut reader = Client::open(&path, bob).await.unwrap();
    let mut bob_db = observer(&path).await;
    let mut wire = ObservedTls {
        tls: config.transport(&address),
        syncs: Vec::new(),
    };
    sync_through(&mut reader, &mut wire, &mut bob_db, BOB, 42, 1).await;
    let messages = reader.messages(42).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert!(matches!(
        messages[0].content.as_ref().unwrap().value.as_ref(),
        Some(message_content::Value::Text(text)) if text.text.as_deref() == Some("before upgrade")
    ));
    gateway.kill().await.unwrap();
}
