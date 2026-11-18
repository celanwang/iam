use super::*;
use iam_protocol::pb::friend_attempt_ack;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

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
            use std::fmt::Write;
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
            database: support::isolated_database().await,
            store: generate_id128(),
            users,
        }
    }

    fn file(&self, name: &str) -> PathBuf {
        self.directory.path().join(name)
    }

    async fn start(&self, mode: &str, bind: &str) -> (Child, String) {
        let mut child = Command::new(env!("CARGO_BIN_EXE_iam-gateway"))
            .env("IAM_TLS_CERT", self.file("chain.pem"))
            .env("IAM_TLS_KEY", self.file("key.pem"))
            .env("IAM_DEVICE_CREDENTIALS", self.file("devices.txt"))
            .env("IAM_GATEWAY_BIND", bind)
            .env("IAM_TIDB_URL", self.database.as_str())
            .env("IAM_STORE_ID", hex::encode(self.store))
            .env("IAM_TIDB_MODE", mode)
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
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

async fn observer(path: &Path) -> SqliteConnection {
    SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(path).read_only(true))
        .await
        .unwrap()
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

#[tokio::test]
#[ignore = "requires real local TiDB and starts the gateway binary"]
async fn tidb_tls_local_release_recovers_after_gateway_process_exit() {
    let config = ProcessConfig::new().await;
    let [alice_identity, bob_identity, charlie_identity] = config.users;
    let (mut gateway, address) = config.start("fresh", "127.0.0.1:0").await;
    let alice = Client::open(config.file("alice.db"), alice_identity)
        .await
        .unwrap();
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

    let mut sender = config.transport(&address);
    sender.reconnect().await.unwrap();
    assert!(matches!(
        auth(&mut sender, 1, alice_identity.device_id, ALICE).await,
        auth_response::Value::Session(_)
    ));
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

    let mut sender = config.transport(&address);
    sender.reconnect().await.unwrap();
    assert!(matches!(
        auth(&mut sender, 1, alice_identity.device_id, ALICE).await,
        auth_response::Value::Session(_)
    ));
    let DecodedBody::SendStatusAck(after) =
        request(&mut sender, 2, MsgType::SendStatus, &first_status).await
    else {
        panic!("expected SEND_STATUS_ACK after restart");
    };
    assert_eq!(after.outcome, before.outcome);
    resumed.kill().await.unwrap();
}
