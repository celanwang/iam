#[path = "support/capacity.rs"]
mod capacity;
#[allow(dead_code)]
#[path = "../../iam-store/tests/support/commit_proxy.rs"]
mod commit_proxy;
#[path = "../../iam-store/tests/support/mod.rs"]
mod support;

use commit_proxy::{CommitPhase, CommitProxy};
use iam_client::{Client, Identity, Options, TlsTransport, Transport};
use iam_core::{DeviceCredential, Engine};
use iam_gateway::{Gateway, Limits};
use iam_protocol::pb::{
    auth_response, message_content, reconnect_response, send_outcome, AuthRequest, ErrorCode, Head,
    MessageContent, MessageKey, ReconnectRequest, SendRequest, SendStatusRequest, Session,
    TextContent,
};
use iam_protocol::{
    decode_frame, encode_frame, generate_id128, DecodedBody, FrameDecoder, Id128, MsgType,
};
use iam_store::{OpenMode, TidbService};
use prost::Message;
use sha2::{Digest, Sha256};
use sqlx::{sqlite::SqliteConnectOptions, Connection, SqliteConnection};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Instant};
use tokio_rustls::{
    rustls::{
        self,
        pki_types::{CertificateDer, PrivatePkcs8KeyDer},
    },
    TlsConnector,
};
use url::Url;

const WAIT: Duration = Duration::from_secs(10);
const ALICE: &[u8] = b"gateway-alice-test";
const BOB: &[u8] = b"gateway-bob-test";

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_reloads_certificates_and_rotates_only_changed_devices() {
    let fixture = Fixture::new().await;
    let gateway = fixture.start(OpenMode::Initialize, Limits::default()).await;
    let mut alice = gateway.client();
    let mut bob = gateway.client();
    alice.reconnect().await.unwrap();
    bob.reconnect().await.unwrap();
    let auth_response::Value::Session(bob_session) = auth(&mut bob, 1, fixture.bob, BOB).await
    else {
        panic!("expected Bob's session");
    };
    assert!(matches!(
        auth(&mut alice, 1, fixture.alice, ALICE).await,
        auth_response::Value::Session(_)
    ));
    let [alice_credential, bob_credential] = fixture.credentials();
    let rotated = DeviceCredential {
        token_sha256: Sha256::digest(b"rotated-bob").into(),
        ..bob_credential
    };
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert = certificate.cert.der().clone();
    let private_key = PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der());
    let reload = |credentials| {
        gateway.gateway.reload(
            vec![cert.clone()],
            private_key.clone_key().into(),
            credentials,
        )
    };

    assert!(reload(vec![rotated, rotated]).is_err());
    assert!(gateway
        .gateway
        .reload(
            vec![cert.clone()],
            fixture.key.clone_key().into(),
            vec![alice_credential],
        )
        .is_err());
    let mut before_reload = gateway.client();
    before_reload.reconnect().await.unwrap();
    send(&mut alice, 2, key(), "before reload").await;
    assert!(matches!(receive(&mut bob).await, DecodedBody::Push(_)));

    reload(vec![alice_credential, rotated]).unwrap();
    expect_close(&mut bob, ErrorCode::Unauthenticated).await;
    assert!(before_reload.reconnect().await.is_err());
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.clone()).unwrap();
    let mut bob = TlsTransport::new(
        gateway.address.clone(),
        "localhost".try_into().unwrap(),
        roots,
    );
    bob.reconnect().await.unwrap();
    let resumed = request(
        &mut bob,
        1,
        MsgType::Reconnect,
        &resume(&bob_session, fixture.bob),
    )
    .await;
    assert!(matches!(resumed, DecodedBody::ReconnectAck(reply)
        if matches!(&reply.value, Some(reconnect_response::Value::Error(error))
            if error.code() == ErrorCode::Unauthenticated)));
    assert!(matches!(
        auth(&mut bob, 2, fixture.bob, BOB).await,
        auth_response::Value::Error(error) if error.code() == ErrorCode::Unauthenticated
    ));
    assert!(matches!(
        auth(&mut bob, 3, fixture.bob, b"rotated-bob").await,
        auth_response::Value::Session(_)
    ));
    send(&mut alice, 3, key(), "after reload").await;
    assert!(matches!(receive(&mut bob).await, DecodedBody::Push(_)));

    gateway.gateway.revoke_device(1, 200, fixture.bob);
    expect_close(&mut bob, ErrorCode::Unauthenticated).await;
    let renewed = DeviceCredential {
        expires_at_ms: rotated.expires_at_ms + 1_000,
        ..rotated
    };
    reload(vec![alice_credential, renewed]).unwrap();
    bob.reconnect().await.unwrap();
    assert!(matches!(
        auth(&mut bob, 1, fixture.bob, b"rotated-bob").await,
        auth_response::Value::Error(error) if error.code() == ErrorCode::Unauthenticated
    ));
    reload(vec![]).unwrap();
    expect_close(&mut alice, ErrorCode::Unauthenticated).await;
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_authenticates_and_delivers_then_replaces_and_revokes() {
    let fixture = Fixture::new().await;
    let gateway = fixture.start(OpenMode::Initialize, Limits::default()).await;
    let mut bad_name = gateway.client_with_name("wrong.example");
    assert!(bad_name.reconnect().await.is_err());
    let tls = rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_root_certificates(gateway.roots.clone())
        .with_no_client_auth();
    let socket = tokio::net::TcpStream::connect(&gateway.address)
        .await
        .unwrap();
    let mut malformed = TlsConnector::from(Arc::new(tls))
        .connect("localhost".try_into().unwrap(), socket)
        .await
        .unwrap();
    let mut oversized = [0; iam_protocol::HEADER_LEN];
    oversized[0] = 1;
    oversized[1] = MsgType::Auth as u8;
    oversized[8..].copy_from_slice(&u32::MAX.to_be_bytes());
    malformed.write_all(&oversized).await.unwrap();
    malformed.flush().await.unwrap();
    let mut byte = [0];
    assert!(!matches!(
        timeout(
            WAIT,
            tokio::io::AsyncReadExt::read(&mut malformed, &mut byte)
        )
        .await
        .unwrap(),
        Ok(1)
    ));
    let mut alice = gateway.client();
    alice.reconnect().await.unwrap();
    assert!(matches!(
        auth(&mut alice, 1, fixture.bob, ALICE).await,
        auth_response::Value::Error(error) if error.code() == ErrorCode::Unauthenticated
    ));
    assert!(matches!(
        auth(&mut alice, 2, fixture.alice, b"invalid").await,
        auth_response::Value::Error(error) if error.code() == ErrorCode::Unauthenticated
    ));
    assert!(matches!(
        auth(&mut alice, 3, fixture.alice, ALICE).await,
        auth_response::Value::Session(_)
    ));

    let mut bob = gateway.client();
    bob.reconnect().await.unwrap();
    let auth_response::Value::Session(session) = auth(&mut bob, 1, fixture.bob, BOB).await else {
        panic!("expected Bob's session");
    };
    let key = key();
    send(&mut alice, 4, key, "online").await;
    assert!(matches!(receive(&mut bob).await, DecodedBody::Push(_)));

    let mut replacement = gateway.client();
    replacement.reconnect().await.unwrap();
    let result = request(
        &mut replacement,
        1,
        MsgType::Reconnect,
        &resume(&session, fixture.bob),
    )
    .await;
    assert!(matches!(result, DecodedBody::ReconnectAck(reply)
        if matches!(reply.value, Some(reconnect_response::Value::Session(_)))));
    expect_close(&mut bob, ErrorCode::SessionReplaced).await;
    gateway.gateway.revoke_device(1, 200, fixture.bob);
    expect_close(&mut replacement, ErrorCode::Unauthenticated).await;
    replacement.reconnect().await.unwrap();
    assert!(matches!(
        request(&mut replacement, 1, MsgType::Reconnect, &resume(&session, fixture.bob)).await,
        DecodedBody::ReconnectAck(reply)
            if matches!(&reply.value, Some(reconnect_response::Value::Error(error))
                if error.code() == ErrorCode::Unauthenticated)
    ));
    assert!(matches!(
        auth(&mut replacement, 2, fixture.bob, BOB).await,
        auth_response::Value::Error(error) if error.code() == ErrorCode::Unauthenticated
    ));
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_recovers_last_dropped_push_and_reconnects_after_gateway_restart() {
    let fixture = Fixture::new().await;
    let gateway = fixture
        .start(
            OpenMode::Initialize,
            Limits {
                push_bytes: 1,
                ..Limits::default()
            },
        )
        .await;
    let mut bob = gateway.client();
    bob.reconnect().await.unwrap();
    assert!(matches!(
        auth(&mut bob, 1, fixture.bob, BOB).await,
        auth_response::Value::Session(_)
    ));
    let mut alice = gateway.client();
    alice.reconnect().await.unwrap();
    assert!(matches!(
        auth(&mut alice, 1, fixture.alice, ALICE).await,
        auth_response::Value::Session(_)
    ));

    let first = key();
    send(&mut alice, 2, first.clone(), "dropped push").await;
    assert!(timeout(Duration::from_millis(150), bob.receive())
        .await
        .is_err());
    drop(bob);

    let local = tempfile::tempdir().unwrap();
    let path = local.path().join("bob.db");
    let identity = Identity {
        tenant_id: 1,
        user_id: 200,
        device_id: fixture.bob,
    };
    let mut client = Client::open(&path, identity).await.unwrap();
    let options = Options {
        sync_interval: Duration::from_millis(100),
        ..Options::default()
    };
    let mut transport = gateway.client();
    let (keep_running, mut input) = mpsc::channel(1);
    let client_task = tokio::spawn(async move {
        client
            .run(&mut transport, BOB, options, &mut input)
            .await
            .unwrap();
    });
    let mut observer = SqliteConnection::connect_with(
        &SqliteConnectOptions::new().filename(&path).read_only(true),
    )
    .await
    .unwrap();
    wait_messages(&mut observer, 1).await;

    send(&mut alice, 3, key(), "last push").await;
    wait_messages(&mut observer, 2).await;
    let address = gateway.address.clone();
    gateway.stop().await;
    let restarted = fixture
        .start_at(OpenMode::Resume, Limits::default(), &address)
        .await;
    let mut alice = restarted.client();
    alice.reconnect().await.unwrap();
    assert!(matches!(
        auth(&mut alice, 1, fixture.alice, ALICE).await,
        auth_response::Value::Session(_)
    ));
    let result = request(
        &mut alice,
        2,
        MsgType::SendStatus,
        &SendStatusRequest { key: Some(first) },
    )
    .await;
    assert!(matches!(result, DecodedBody::SendStatusAck(reply)
        if matches!(reply.outcome.as_ref().and_then(|result| result.value.as_ref()), Some(send_outcome::Value::Committed(_)))));
    send(&mut alice, 3, key(), "after restart").await;
    wait_messages(&mut observer, 3).await;
    drop(keep_running);
    timeout(WAIT, client_task).await.unwrap().unwrap();
    restarted.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_recovers_committed_send_after_client_disconnect() {
    let mut fixture = Fixture::new().await;
    let proxy = CommitProxy::start(&fixture.database).await;
    fixture.database = proxy.url.clone();
    let gateway = fixture
        .start(
            OpenMode::Initialize,
            Limits {
                requests: 1,
                ..Limits::default()
            },
        )
        .await;
    let mut alice = gateway.client();
    let mut bob = gateway.client();
    alice.reconnect().await.unwrap();
    bob.reconnect().await.unwrap();
    assert!(matches!(
        auth(&mut alice, 1, fixture.alice, ALICE).await,
        auth_response::Value::Session(_)
    ));
    assert!(matches!(
        auth(&mut bob, 1, fixture.bob, BOB).await,
        auth_response::Value::Session(_)
    ));

    let key = key();
    let (committed, release) = proxy.arm(CommitPhase::After);
    let original_key = key.clone();
    let sending = tokio::spawn(async move {
        send(&mut alice, 2, original_key, "committed before disconnect").await;
    });
    timeout(WAIT, committed).await.unwrap().unwrap().unwrap();
    sending.abort();
    assert!(sending.await.unwrap_err().is_cancelled());
    assert!(matches!(
        request(&mut bob, 2, MsgType::Heartbeat, &iam_protocol::pb::HeartbeatRequest {}).await,
        DecodedBody::Error(error) if error.error.as_ref().unwrap().code() == ErrorCode::Overloaded
    ));
    release.send(()).unwrap();

    let mut alice = gateway.client();
    alice.reconnect().await.unwrap();
    assert!(matches!(
        auth(&mut alice, 1, fixture.alice, ALICE).await,
        auth_response::Value::Session(_)
    ));
    let deadline = Instant::now() + WAIT;
    let mut request_id = 2;
    loop {
        let result = request(
            &mut alice,
            request_id,
            MsgType::SendStatus,
            &SendStatusRequest {
                key: Some(key.clone()),
            },
        )
        .await;
        match result {
            DecodedBody::SendStatusAck(reply) => {
                assert_eq!(reply.key.as_ref(), Some(&key));
                assert!(matches!(
                    reply.outcome.unwrap().value,
                    Some(send_outcome::Value::Committed(_))
                ));
                break;
            }
            DecodedBody::Error(error)
                if error.error.as_ref().unwrap().code() == ErrorCode::Overloaded =>
            {
                assert!(Instant::now() < deadline);
                request_id += 1;
                sleep(Duration::from_millis(25)).await;
            }
            other => panic!("unexpected send status {other:?}"),
        }
    }
    gateway.stop().await;
}

struct Fixture {
    database: Url,
    store: Id128,
    alice: Id128,
    bob: Id128,
    certificate: CertificateDer<'static>,
    key: PrivatePkcs8KeyDer<'static>,
    roots: rustls::RootCertStore,
    expires_at_ms: i64,
}

impl Fixture {
    async fn new() -> Self {
        let identity = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let certificate = identity.cert.der().clone();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate.clone()).unwrap();
        Self {
            database: support::isolated_database().await,
            store: generate_id128(),
            alice: generate_id128(),
            bob: generate_id128(),
            certificate,
            key: PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der()),
            roots,
            expires_at_ms: iam_protocol::id128_timestamp_ms(&generate_id128()) as i64 + 600_000,
        }
    }

    async fn start(&self, mode: OpenMode, limits: Limits) -> RunningGateway {
        self.start_at(mode, limits, "127.0.0.1:0").await
    }

    async fn start_at(&self, mode: OpenMode, limits: Limits, address: &str) -> RunningGateway {
        self.start_with_credentials(mode, limits, address, self.credentials())
            .await
    }

    async fn start_with_credentials(
        &self,
        mode: OpenMode,
        limits: Limits,
        address: &str,
        credentials: impl IntoIterator<Item = DeviceCredential>,
    ) -> RunningGateway {
        let mut engine = Engine::new(0);
        engine.store.seed_direct(1, 1, 100, 200);
        engine
            .store
            .configure_device_credentials(credentials)
            .unwrap();

        let service = TidbService::connect_local(self.database.as_str(), self.store, engine, mode)
            .await
            .unwrap();
        let gateway = Gateway::new(
            service.clone(),
            vec![self.certificate.clone()],
            self.key.clone_key().into(),
            limits,
        )
        .unwrap();
        let listener = TcpListener::bind(address).await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (stop, stopped) = oneshot::channel();
        let server = gateway.clone();
        let task = tokio::spawn(async move {
            server
                .serve(listener, async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        RunningGateway {
            gateway,
            service,
            roots: self.roots.clone(),
            address,
            stop,
            task,
        }
    }

    fn credentials(&self) -> [DeviceCredential; 2] {
        [(100, self.alice, ALICE), (200, self.bob, BOB)].map(|(user_id, device_id, token)| {
            DeviceCredential {
                tenant_id: 1,
                user_id,
                device_id,
                expires_at_ms: self.expires_at_ms,
                token_sha256: Sha256::digest(token).into(),
            }
        })
    }
}

struct RunningGateway {
    gateway: Arc<Gateway>,
    service: TidbService,
    roots: rustls::RootCertStore,
    address: String,
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl RunningGateway {
    fn client(&self) -> TlsTransport {
        self.client_with_name("localhost")
    }

    fn client_with_name(&self, name: &'static str) -> TlsTransport {
        TlsTransport::new(
            self.address.clone(),
            name.try_into().unwrap(),
            self.roots.clone(),
        )
    }

    async fn stop(self) {
        self.stop.send(()).unwrap();
        timeout(WAIT, self.task).await.unwrap().unwrap();
    }
}

async fn request(
    transport: &mut TlsTransport,
    id: u64,
    kind: MsgType,
    body: &impl Message,
) -> DecodedBody {
    let bytes = encode_frame(kind, &Head { req_id: Some(id) }, body).unwrap();
    let bytes = timeout(WAIT, transport.exchange(bytes))
        .await
        .unwrap()
        .unwrap();
    decode(&bytes)
}

async fn receive(transport: &mut TlsTransport) -> DecodedBody {
    let bytes = timeout(WAIT, transport.receive()).await.unwrap().unwrap();
    decode(&bytes)
}

fn decode(bytes: &[u8]) -> DecodedBody {
    let mut decoder = FrameDecoder::new();
    decoder.push(bytes);
    decode_frame(&decoder.next_frame().unwrap().unwrap())
        .unwrap()
        .body
}

async fn auth(
    transport: &mut TlsTransport,
    id: u64,
    device: Id128,
    token: &[u8],
) -> auth_response::Value {
    let DecodedBody::AuthAck(ack) = request(
        transport,
        id,
        MsgType::Auth,
        &AuthRequest {
            device_id: Some(device.to_vec().into()),
            access_token: Some(token.to_vec().into()),
            requested_capabilities: vec![],
        },
    )
    .await
    else {
        panic!("expected AUTH_ACK")
    };
    ack.value.unwrap()
}

fn resume(session: &Session, device: Id128) -> ReconnectRequest {
    ReconnectRequest {
        device_id: Some(device.to_vec().into()),
        previous_connection_id: session.connection_id.clone(),
        resume_token: session.resume_token.clone(),
        requested_capabilities: vec![],
    }
}

async fn expect_close(transport: &mut TlsTransport, code: ErrorCode) {
    assert!(matches!(receive(transport).await, DecodedBody::Error(error)
        if error.close_connection == Some(true) && error.error.as_ref().unwrap().code() == code));
    assert!(timeout(WAIT, transport.receive()).await.unwrap().is_err());
}

fn key() -> MessageKey {
    MessageKey {
        conversation_id: Some(1),
        client_msg_id: Some(generate_id128().to_vec().into()),
    }
}

async fn send(transport: &mut TlsTransport, id: u64, key: MessageKey, text: &str) {
    let result = request(
        transport,
        id,
        MsgType::Send,
        &SendRequest {
            key: Some(key),
            content: Some(MessageContent {
                value: Some(message_content::Value::Text(TextContent {
                    text: Some(text.to_owned()),
                })),
            }),
        },
    )
    .await;
    assert!(matches!(result, DecodedBody::SendAck(reply)
        if matches!(reply.outcome.as_ref().and_then(|result| result.value.as_ref()), Some(send_outcome::Value::Committed(_)))));
}

async fn wait_messages(client: &mut SqliteConnection, count: i64) {
    let deadline = Instant::now() + WAIT;
    loop {
        let saved: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
            .fetch_one(&mut *client)
            .await
            .unwrap();
        if saved == count {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "did not recover {count} messages"
        );
        sleep(Duration::from_millis(25)).await;
    }
}
