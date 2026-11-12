mod support;

#[path = "support/commit_proxy.rs"]
#[allow(dead_code)]
mod commit_proxy;

use commit_proxy::{CommitPhase, CommitProxy};
use iam_client::{Client, Identity, Options, Submission, Transport};
use iam_core::{Engine, TokenSigner};
use iam_protocol::pb::{
    change, message_content, send_outcome, snapshot_response, sync_response, sync_target,
    AuthRequest, ErrorCode, Head, MessageContent, MessageKey, ResetReason, SendRequest,
    SendStatusRequest, TextContent,
};
use iam_protocol::{
    decode_frame, encode_frame, generate_id128, DecodedBody, Frame, FrameDecoder, Id128, MsgType,
};
use iam_store::{OpenMode, TidbService};
use prost::Message;
use sqlx::{Connection, MySqlPool, SqliteConnection};
use std::process::Stdio;
use std::{io, sync::Arc, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, Mutex};

const WAIT: Duration = Duration::from_secs(10);
const LONG_INTERVAL: Duration = Duration::from_secs(3_600);

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_client_recovers_lost_send_receipt_after_restart() {
    check_send_recovery(Fault::AfterSend).await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_client_retries_original_send_after_restart() {
    check_send_recovery(Fault::BeforeSend).await;
}

async fn check_send_recovery(fault: Fault) {
    let resend = matches!(fault, Fault::BeforeSend);
    let server = server().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alice.db");
    let alice = identity(100);
    let mut client = Client::open(&path, alice).await.unwrap();
    let a = client.enqueue(1, "first".into()).await.unwrap();
    let b = client.enqueue(1, "second".into()).await.unwrap();
    let mut options = options(LONG_INTERVAL);
    options.retry_min = Duration::from_secs(1);
    options.retry_max = options.retry_min;
    let mut running = Running::start(client, &server, b"alice", options, Some(fault));
    let failed = running
        .wait_for(|event| matches!(event, Event::DroppedSend(_)))
        .await;
    assert!(matches!(failed, Event::DroppedSend(key) if key == a));
    let (mut client, first_wire) = running.stop().await;
    assert_eq!(
        first_wire.sends,
        vec![SendCall::Send(a.clone(), "first".into())]
    );
    assert!(client.outcome(&a).await.unwrap().is_none());
    assert!(client.outcome(&b).await.unwrap().is_none());
    drop(client);
    server.lock().await.disconnect(first_wire.cid);

    let client = Client::open(&path, alice).await.unwrap();
    let mut running = Running::start(client, &server, b"alice", options, None);
    running
        .wait_for(|event| {
            matches!(event, Event::Reply(reply)
        if matches!(reply.as_ref(), DecodedBody::SendAck(ack) if ack.key.as_ref() == Some(&b)))
        })
        .await;
    running.wait_for_idle().await;
    let (client, wire) = running.stop().await;
    let mut expected = vec![SendCall::Status(a.clone())];
    if resend {
        expected.push(SendCall::Send(a.clone(), "first".into()));
    }
    expected.push(SendCall::Send(b.clone(), "second".into()));
    assert_eq!(wire.sends, expected);
    drop(client);

    let mut client = Client::open(&path, alice).await.unwrap();
    let seq = |outcome: iam_protocol::pb::SendOutcome| match outcome.value {
        Some(send_outcome::Value::Committed(receipt)) => {
            receipt.position.unwrap().create_seq.unwrap()
        }
        other => panic!("expected committed, got {other:?}"),
    };
    assert_eq!(seq(client.outcome(&a).await.unwrap().unwrap()), 1);
    assert_eq!(seq(client.outcome(&b).await.unwrap().unwrap()), 2);
    let service = server.lock().await;
    let cid = wire.cid.unwrap();
    assert_eq!(service.conversation(cid, 1).await.unwrap().next_sequence, 3);
    let page = service.read_messages(cid, 1, None, 100).await.unwrap();
    assert_eq!(page.messages.len(), 2);
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_client_timer_recovers_the_last_lost_push_without_reconnect() {
    let server = server().await;
    let mut sender = sender(&server).await;
    let directory = tempfile::tempdir().unwrap();
    let client = Client::open(directory.path().join("bob.db"), identity(200))
        .await
        .unwrap();
    let mut bob = Running::start(
        client,
        &server,
        b"bob",
        options(Duration::from_millis(200)),
        None,
    );
    let (cid, original_cursor) = bob.initial_conversation_cursor().await;
    bob.wait_for_idle().await;
    let pushes = sender.send_text("last push").await;
    assert_eq!(pushes.len(), 1);
    assert_eq!(pushes[0].0, cid);
    // Drop the only PUSH. No later message, inbound notification, or disconnect is generated.
    drop(pushes);

    bob.wait_for_sync_through(1).await;
    bob.wait_for_idle().await;
    let (mut client, wire) = bob.stop().await;
    assert_eq!(wire.connections, 1);
    assert!(
        wire.syncs
            .iter()
            .any(|call| call.target == 1
                && call.cursor.as_deref() == Some(original_cursor.as_slice()))
    );
    assert_eq!(
        client
            .messages(1)
            .await
            .unwrap()
            .iter()
            .map(text)
            .collect::<Vec<_>>(),
        ["last push"]
    );
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_client_reauthenticates_and_syncs_missed_pages_after_disconnect() {
    check_reconnect(false).await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_client_resumes_from_the_saved_cursor_when_a_page_token_expires() {
    check_reconnect(true).await;
}

async fn check_reconnect(expire_page: bool) {
    let database = support::isolated_database().await;
    let server = open_server(
        database.as_str(),
        generate_id128(),
        seeded(),
        OpenMode::Initialize,
    )
    .await;
    let fault = if expire_page {
        Some(Fault::ExpirePage(
            MySqlPool::connect(database.as_str()).await.unwrap(),
        ))
    } else {
        None
    };
    let mut sender = sender(&server).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("bob.db");
    let device = identity(200);
    let client = Client::open(&path, device).await.unwrap();
    let mut bob = Running::start(client, &server, b"bob", options(LONG_INTERVAL), fault);
    let (cid, original_cursor) = bob.initial_conversation_cursor().await;
    bob.wait_for_idle().await;

    server.lock().await.disconnect(Some(cid));
    assert!(sender.send_text("missed one").await.is_empty());
    assert!(sender.send_text("missed two").await.is_empty());
    bob.inbound
        .send(Err(io::Error::from(io::ErrorKind::ConnectionReset)))
        .unwrap();
    let first_page = bob.wait_for_sync_through(1).await;
    if expire_page {
        bob.wait_for(|event| {
            matches!(event, Event::Reply(body)
            if matches!(body.as_ref(), DecodedBody::SyncAck(ack)
                if matches!(&ack.value, Some(sync_response::Value::Error(error))
                    if error.code() == ErrorCode::PageTokenExpired)))
        })
        .await;
    }
    bob.wait_for_sync_through(2).await;
    bob.wait_for_idle().await;
    let (client, wire) = bob.stop().await;
    assert_eq!(wire.connections, 2);
    assert_eq!(wire.devices, vec![device.device_id, device.device_id]);
    let resumed = &wire.syncs[2..];
    assert_eq!(resumed.len(), if expire_page { 4 } else { 3 });
    assert_eq!(resumed[0].target, 0);
    assert!(resumed[0].cursor.is_some());
    assert_eq!(
        resumed[1],
        SyncCall {
            target: 1,
            cursor: Some(original_cursor),
            page: false
        }
    );
    assert_eq!(
        resumed[2],
        SyncCall {
            target: 1,
            cursor: None,
            page: true
        }
    );
    if expire_page {
        assert_eq!(
            resumed[3],
            SyncCall {
                target: 1,
                cursor: first_page.next_cursor.map(|cursor| cursor.to_vec()),
                page: false,
            }
        );
    }

    drop(client);
    let mut client = Client::open(&path, device).await.unwrap();
    assert_eq!(
        client
            .messages(1)
            .await
            .unwrap()
            .iter()
            .map(text)
            .collect::<Vec<_>>(),
        ["missed one", "missed two"]
    );
}

type Server = Arc<Mutex<TidbService>>;
type Pushes = Vec<(Id128, Vec<u8>)>;

async fn server() -> Server {
    let database = support::isolated_database().await;
    open_server(
        database.as_str(),
        generate_id128(),
        seeded(),
        OpenMode::Initialize,
    )
    .await
}

fn seeded() -> Engine {
    let mut engine = Engine::new(0);
    engine.store.seed_user(1, 100, b"alice".to_vec());
    engine.store.seed_user(1, 200, b"bob".to_vec());
    engine.store.seed_direct(1, 1, 100, 200);
    engine
}

async fn open_server(database: &str, store_id: Id128, engine: Engine, mode: OpenMode) -> Server {
    Arc::new(Mutex::new(
        TidbService::connect_local(database, store_id, engine, mode)
            .await
            .unwrap(),
    ))
}

fn identity(user_id: u64) -> Identity {
    Identity {
        tenant_id: 1,
        user_id,
        device_id: generate_id128(),
    }
}

fn options(sync_interval: Duration) -> Options {
    Options {
        sync_interval,
        retry_min: Duration::from_millis(20),
        retry_max: Duration::from_millis(100),
        page_items: 1,
        ..Options::default()
    }
}

fn text(message: &iam_protocol::pb::MessageView) -> &str {
    let Some(message_content::Value::Text(text)) = &message.content.as_ref().unwrap().value else {
        panic!("expected text message");
    };
    text.text.as_deref().unwrap()
}

enum Fault {
    BeforeSend,
    AfterSend,
    ExpirePage(MySqlPool),
}

#[derive(Debug, PartialEq)]
enum SendCall {
    Send(MessageKey, String),
    Status(MessageKey),
}

#[derive(Debug, PartialEq, Eq)]
struct SyncCall {
    target: u64,
    cursor: Option<Vec<u8>>,
    page: bool,
}

enum Event {
    DroppedSend(MessageKey),
    Reply(Box<DecodedBody>),
    Idle,
    Inbound,
}

struct LocalTransport {
    server: Server,
    cid: Option<Id128>,
    connections: usize,
    devices: Vec<Id128>,
    sends: Vec<SendCall>,
    syncs: Vec<SyncCall>,
    fault: Option<Fault>,
    events: mpsc::UnboundedSender<Event>,
    inbound: mpsc::UnboundedReceiver<io::Result<Vec<u8>>>,
}

impl Transport for LocalTransport {
    async fn reconnect(&mut self) -> io::Result<()> {
        self.server.lock().await.disconnect(self.cid.take());
        self.connections += 1;
        Ok(())
    }

    async fn exchange(&mut self, request: Vec<u8>) -> io::Result<Vec<u8>> {
        let frame = frame(&request);
        let decoded = decode_frame(&frame).unwrap();
        let send_key = match decoded.body {
            DecodedBody::Auth(auth) => {
                self.devices
                    .push(auth.device_id.unwrap().as_ref().try_into().unwrap());
                None
            }
            DecodedBody::Send(send) => {
                let key = send.key.unwrap();
                let Some(message_content::Value::Text(text)) = send.content.unwrap().value else {
                    panic!("expected text");
                };
                self.sends
                    .push(SendCall::Send(key.clone(), text.text.unwrap()));
                Some(key)
            }
            DecodedBody::SendStatus(status) => {
                self.sends.push(SendCall::Status(status.key.unwrap()));
                None
            }
            DecodedBody::Sync(sync) => {
                if sync.page_token.is_some() && matches!(self.fault, Some(Fault::ExpirePage(_))) {
                    let Some(Fault::ExpirePage(db)) = self.fault.take() else {
                        unreachable!()
                    };
                    // Advance the real TiDB clock watermark past the page token's lifetime.
                    // The saved incremental cursor remains valid.
                    sqlx::query("UPDATE store_control SET max_observed_now_ms = max_observed_now_ms + 960000,
                        closed_through_client_ms = closed_through_client_ms + 960000 WHERE singleton = 1")
                        .execute(&db).await.unwrap();
                }
                self.syncs.push(SyncCall {
                    target: match sync.target.unwrap().value.unwrap() {
                        sync_target::Value::Account(_) => 0,
                        sync_target::Value::Conversation(id) => id,
                    },
                    cursor: sync.cursor.map(|cursor| cursor.to_vec()),
                    page: sync.page_token.is_some(),
                });
                None
            }
            _ => None,
        };
        let fault = send_key.as_ref().and_then(|_| self.fault.take());
        if matches!(fault, Some(Fault::BeforeSend)) {
            self.events
                .send(Event::DroppedSend(send_key.unwrap()))
                .unwrap();
            return Err(io::ErrorKind::ConnectionReset.into());
        }
        let (cid, output) = self
            .server
            .lock()
            .await
            .handle_frame(self.cid, &frame)
            .await
            .unwrap();
        self.cid = cid;
        let [bytes] = output.responses.try_into().expect("one response");
        let response = decode_frame(&self::frame(&bytes)).unwrap().body;
        if matches!(fault, Some(Fault::AfterSend)) {
            assert!(matches!(response, DecodedBody::SendAck(ack)
                if matches!(ack.outcome.as_ref().unwrap().value, Some(send_outcome::Value::Committed(_)))));
            self.events
                .send(Event::DroppedSend(send_key.unwrap()))
                .unwrap();
            return Err(io::ErrorKind::ConnectionReset.into());
        }
        self.events.send(Event::Reply(Box::new(response))).unwrap();
        Ok(bytes)
    }

    async fn receive(&mut self) -> io::Result<Vec<u8>> {
        let _ = self.events.send(Event::Idle);
        let frame = self.inbound.recv().await.expect("test input stays open");
        let _ = self.events.send(Event::Inbound);
        frame
    }
}

struct Running {
    submissions: mpsc::Sender<Submission>,
    inbound: mpsc::UnboundedSender<io::Result<Vec<u8>>>,
    events: mpsc::UnboundedReceiver<Event>,
    task: tokio::task::JoinHandle<(Client, LocalTransport, iam_client::Result<()>)>,
}

impl Running {
    fn start(
        mut client: Client,
        server: &Server,
        token: &'static [u8],
        options: Options,
        fault: Option<Fault>,
    ) -> Self {
        let (submissions, mut commands) = mpsc::channel(8);
        let (event_tx, events) = mpsc::unbounded_channel();
        let (inbound, incoming) = mpsc::unbounded_channel();
        let mut wire = LocalTransport {
            server: server.clone(),
            cid: None,
            connections: 0,
            devices: Vec::new(),
            sends: Vec::new(),
            syncs: Vec::new(),
            fault,
            events: event_tx,
            inbound: incoming,
        };
        let task = tokio::spawn(async move {
            let result = client.run(&mut wire, token, options, &mut commands).await;
            (client, wire, result)
        });
        Self {
            submissions,
            inbound,
            events,
            task,
        }
    }

    async fn wait_for(&mut self, expected: impl Fn(&Event) -> bool) -> Event {
        tokio::time::timeout(WAIT, async {
            loop {
                let event = self
                    .events
                    .recv()
                    .await
                    .expect("client exited unexpectedly");
                if expected(&event) {
                    return event;
                }
            }
        })
        .await
        .expect("timed out waiting for the client")
    }

    async fn wait_for_idle(&mut self) {
        self.wait_for(|event| matches!(event, Event::Idle)).await;
    }

    async fn wait_for_sync_through(&mut self, through: u64) -> iam_protocol::pb::SyncPage {
        let Event::Reply(body) = self.wait_for(|event| {
            matches!(event, Event::Reply(body)
                if matches!(body.as_ref(), DecodedBody::SyncAck(ack)
                    if matches!(&ack.value, Some(sync_response::Value::Page(page))
                        if page.covered_through == Some(through) && page.changes.iter().any(
                            |change| matches!(change.payload, Some(change::Payload::MessageUpsert(_)))))))
        })
        .await else { unreachable!() };
        let DecodedBody::SyncAck(ack) = *body else {
            unreachable!()
        };
        let Some(sync_response::Value::Page(page)) = ack.value else {
            unreachable!()
        };
        page
    }

    async fn initial_conversation_cursor(&mut self) -> (Id128, Vec<u8>) {
        let Event::Reply(auth) = self.wait_for(|event| {
            matches!(event, Event::Reply(body) if matches!(body.as_ref(), DecodedBody::AuthAck(_)))
        }).await else { unreachable!() };
        let DecodedBody::AuthAck(auth) = *auth else {
            unreachable!()
        };
        let Some(iam_protocol::pb::auth_response::Value::Session(session)) = auth.value else {
            panic!("expected session");
        };
        let Event::Reply(snapshot) = self
            .wait_for(|event| {
                matches!(event,
            Event::Reply(body) if matches!(body.as_ref(), DecodedBody::SnapshotAck(ack)
                if matches!(&ack.value, Some(snapshot_response::Value::Page(page))
                    if matches!(&page.context.as_ref().unwrap().target.as_ref().unwrap().value,
                        Some(sync_target::Value::Conversation(1))))))
            })
            .await
        else {
            unreachable!()
        };
        let DecodedBody::SnapshotAck(snapshot) = *snapshot else {
            unreachable!()
        };
        let Some(snapshot_response::Value::Page(page)) = snapshot.value else {
            unreachable!()
        };
        (
            session.connection_id.unwrap().as_ref().try_into().unwrap(),
            page.resume_cursor.unwrap().to_vec(),
        )
    }

    async fn stop(self) -> (Client, LocalTransport) {
        drop(self.submissions);
        let (client, wire, result) = tokio::time::timeout(WAIT, self.task)
            .await
            .expect("client did not stop")
            .unwrap();
        result.unwrap();
        (client, wire)
    }
}

struct Sender {
    server: Server,
    cid: Option<Id128>,
    req: u64,
}

async fn sender(server: &Server) -> Sender {
    authenticate(server, b"alice", generate_id128()).await.0
}

async fn authenticate(server: &Server, token: &[u8], device: Id128) -> (Sender, Pushes) {
    let mut sender = Sender {
        server: server.clone(),
        cid: None,
        req: 0,
    };
    let (reply, pushes) = sender
        .call(
            MsgType::Auth,
            &AuthRequest {
                access_token: Some(token.to_vec().into()),
                device_id: Some(device.to_vec().into()),
                requested_capabilities: Vec::new(),
            },
        )
        .await;
    assert!(matches!(reply, DecodedBody::AuthAck(ack)
        if matches!(ack.value, Some(iam_protocol::pb::auth_response::Value::Session(_)))));
    (sender, pushes)
}

impl Sender {
    async fn send_text(&mut self, text: &str) -> Pushes {
        self.send_with_id(generate_id128(), text).await
    }

    async fn send_with_id(&mut self, id: Id128, text: &str) -> Pushes {
        let (response, pushes) = self
            .call(
                MsgType::Send,
                &SendRequest {
                    key: Some(message_key(id)),
                    content: Some(MessageContent {
                        value: Some(message_content::Value::Text(TextContent {
                            text: Some(text.into()),
                        })),
                    }),
                },
            )
            .await;
        assert!(matches!(response, DecodedBody::SendAck(ack)
            if matches!(ack.outcome.as_ref().unwrap().value, Some(send_outcome::Value::Committed(_)))));
        pushes
    }

    async fn call(&mut self, kind: MsgType, body: &impl Message) -> (DecodedBody, Pushes) {
        self.req += 1;
        let bytes = encode_frame(
            kind,
            &Head {
                req_id: Some(self.req),
            },
            body,
        )
        .unwrap();
        let (cid, output) = self
            .server
            .lock()
            .await
            .handle_frame(self.cid, &frame(&bytes))
            .await
            .unwrap();
        self.cid = Some(cid.expect("authenticated sender"));
        let [response] = output.responses.try_into().expect("one response");
        (decode_frame(&frame(&response)).unwrap().body, output.pushes)
    }
}

fn message_key(id: Id128) -> MessageKey {
    MessageKey {
        conversation_id: Some(1),
        client_msg_id: Some(id.to_vec().into()),
    }
}

fn frame(bytes: &[u8]) -> Frame {
    let mut decoder = FrameDecoder::new();
    decoder.push(bytes);
    let frame = decoder.next_frame().unwrap().unwrap();
    decoder.finish().unwrap();
    frame
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_client_applies_duplicate_and_out_of_order_pushes_without_advancing_cursor() {
    let server = server().await;
    let mut sender = sender(&server).await;
    let directory = tempfile::tempdir().unwrap();
    let client = Client::open(directory.path().join("bob.db"), identity(200))
        .await
        .unwrap();
    let mut bob = Running::start(client, &server, b"bob", options(LONG_INTERVAL), None);
    let (cid, original_cursor) = bob.initial_conversation_cursor().await;
    bob.wait_for_idle().await;

    let mut frames = Vec::new();
    for text in ["missing", "second", "third"] {
        let [(recipient, push)] = sender.send_text(text).await.try_into().expect("one push");
        assert_eq!(recipient, cid);
        frames.push(push);
    }
    for index in [2, 1, 2] {
        bob.inbound.send(Ok(frames[index].clone())).unwrap();
        bob.wait_for(|event| matches!(event, Event::Inbound)).await;
    }
    bob.wait_for_idle().await;
    let (mut client, wire) = bob.stop().await;
    assert_eq!(wire.syncs.len(), 2, "the periodic sync has not run");
    assert_eq!(message_texts(&mut client, 1).await, ["second", "third"]);

    let mut bob = Running::start(client, &server, b"bob", options(LONG_INTERVAL), None);
    bob.wait_for_sync_through(3).await;
    bob.wait_for_idle().await;
    let (mut client, wire) = bob.stop().await;
    assert_eq!(
        wire.syncs[1].cursor.as_deref(),
        Some(original_cursor.as_slice())
    );
    assert_eq!(
        message_texts(&mut client, 1).await,
        ["missing", "second", "third"]
    );
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_client_stops_reconnecting_after_the_same_device_replaces_it() {
    let server = server().await;
    let directory = tempfile::tempdir().unwrap();
    let device = identity(200);
    let client = Client::open(directory.path().join("bob.db"), device)
        .await
        .unwrap();
    let mut bob = Running::start(client, &server, b"bob", options(LONG_INTERVAL), None);
    let (cid, _) = bob.initial_conversation_cursor().await;
    bob.wait_for_idle().await;

    let (new_peer, notices) = authenticate(&server, b"bob", device.device_id).await;
    let [(old, error)] = notices.try_into().expect("one replacement notice");
    assert_eq!(old, cid);
    bob.inbound.send(Ok(error)).unwrap();
    let (_, wire, result) = tokio::time::timeout(WAIT, bob.task).await.unwrap().unwrap();
    assert!(matches!(result, Err(iam_client::Error::Server(error))
        if error.code() == ErrorCode::SessionReplaced));
    assert_eq!(wire.connections, 1);
    assert!(server
        .lock()
        .await
        .conversation(new_peer.cid.unwrap(), 1)
        .await
        .is_ok());
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_client_recovers_conversations_while_the_account_cursor_is_expired() {
    let database = support::isolated_database().await;
    let mut engine = seeded();
    engine.store.seed_direct(1, 2, 100, 200);
    let server = open_server(
        database.as_str(),
        generate_id128(),
        engine,
        OpenMode::Initialize,
    )
    .await;
    let mut sender = sender(&server).await;
    sender.send_text("old history").await;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("bob.db");
    let device = identity(200);
    let client = Client::open(&path, device).await.unwrap();
    let mut bob = Running::start(client, &server, b"bob", options(LONG_INTERVAL), None);
    bob.initial_conversation_cursor().await;
    bob.wait_for_idle().await;
    let (mut client, _) = bob.stop().await;
    assert_eq!(message_texts(&mut client, 1).await, ["old history"]);
    drop(client);

    let pool = MySqlPool::connect(database.as_str()).await.unwrap();
    let (token_key, now): (Vec<u8>, i64) = sqlx::query_as(
        "SELECT token_key, max_observed_now_ms FROM store_control WHERE singleton = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let mut local =
        SqliteConnection::connect_with(&sqlx::sqlite::SqliteConnectOptions::new().filename(&path))
            .await
            .unwrap();
    let account_target = 0_u64.to_be_bytes().to_vec();
    let account_cursor: Vec<u8> = sqlx::query_scalar("SELECT cursor FROM streams WHERE target = ?")
        .bind(&account_target)
        .fetch_one(&mut local)
        .await
        .unwrap();
    let signer = TokenSigner::new(token_key.try_into().unwrap());
    let mut expired = signer.verify_cursor(&account_cursor, now).unwrap();
    expired.issued_at_ms = now - 31 * 86_400_000;
    sqlx::query("UPDATE streams SET cursor = ? WHERE target = ?")
        .bind(signer.sign_cursor(&expired))
        .bind(&account_target)
        .execute(&mut local)
        .await
        .unwrap();
    sqlx::query("UPDATE streams SET cursor = ? WHERE target = ?")
        .bind(&b"invalid credential"[..])
        .bind(2_u64.to_be_bytes().to_vec())
        .execute(&mut local)
        .await
        .unwrap();
    local.close().await.unwrap();

    // Simulate a TiDB restore with a new conversation history and a lower upper bound.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("DELETE FROM messages WHERE tenant_id = 1 AND conversation_id = 1")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE conversations SET history_id = ?, next_sequence = 1 WHERE tenant_id = 1 AND conversation_id = 1")
        .bind(generate_id128().to_vec()).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();

    let mut client = Client::open(&path, device).await.unwrap();
    let queued = client.enqueue(2, "still sending".into()).await.unwrap();
    let mut bob = Running::start(client, &server, b"bob", options(LONG_INTERVAL), None);
    bob.wait_for(|event| {
        matches!(event, Event::Reply(body)
        if matches!(body.as_ref(), DecodedBody::SyncAck(ack)
            if matches!(&ack.value, Some(sync_response::Value::Error(error))
                if error.code() == ErrorCode::InvalidCursor)))
    })
    .await;
    bob.wait_for(|event| {
        matches!(event, Event::Reply(body)
        if matches!(body.as_ref(), DecodedBody::SendAck(ack) if ack.key.as_ref() == Some(&queued)))
    })
    .await;
    bob.wait_for_idle().await;
    let (mut client, wire) = bob.stop().await;
    let calls: Vec<_> = wire.syncs.iter().filter(|call| call.target == 2).collect();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0].cursor.as_deref(),
        Some(b"invalid credential".as_slice())
    );
    assert!(calls[1].cursor.is_none());
    assert!(message_texts(&mut client, 1).await.is_empty());
    assert!(matches!(
        client.outcome(&queued).await.unwrap().unwrap().value,
        Some(send_outcome::Value::Committed(_))
    ));
    drop(client);
    let mut client = Client::open(&path, device).await.unwrap();
    assert_eq!(
        client.recovery(0).await.unwrap(),
        Some(iam_client::Recovery {
            reason: ResetReason::CursorExpired,
            data_loss_possible: false,
            pending: true,
        })
    );
    assert_eq!(
        client.recovery(1).await.unwrap(),
        Some(iam_client::Recovery {
            reason: ResetReason::HistoryReset,
            data_loss_possible: true,
            pending: false,
        })
    );
}

async fn message_texts(client: &mut Client, target: u64) -> Vec<String> {
    client
        .messages(target)
        .await
        .unwrap()
        .iter()
        .map(|message| text(message).to_owned())
        .collect()
}

const CRASH_DB: &str = "IAM_CLIENT_CRASH_TEST_DB";
const CRASH_IDS: &str = "IAM_CLIENT_CRASH_TEST_IDS";
const CRASH_TEXT: &str = "committed before push";

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_client_recovers_after_service_crashes_between_commit_and_push() {
    if let Ok(database) = std::env::var(CRASH_DB) {
        let ids: Vec<Id128> = std::env::var(CRASH_IDS)
            .unwrap()
            .split_whitespace()
            .map(|id| hex::decode(id).unwrap().try_into().unwrap())
            .collect();
        let [store_id, alice, bob, key] = ids.try_into().unwrap();
        let service = open_server(&database, store_id, seeded(), OpenMode::Resume).await;
        let (mut sender, _) = authenticate(&service, b"alice", alice).await;
        let (receiver, _) = authenticate(&service, b"bob", bob).await;
        assert!(service
            .lock()
            .await
            .conversation(receiver.cid.unwrap(), 1)
            .await
            .is_ok());
        println!("SERVICE_READY");
        let mut commands = BufReader::new(tokio::io::stdin()).lines();
        assert_eq!(commands.next_line().await.unwrap().as_deref(), Some("send"));
        sender.send_with_id(key, CRASH_TEXT).await;
        panic!("the service returned from SEND before it was killed");
    }

    let database = support::isolated_database().await;
    let store_id = generate_id128();
    let service = open_server(database.as_str(), store_id, seeded(), OpenMode::Initialize).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("bob.db");
    let bob_id = identity(200);
    let client = Client::open(&path, bob_id).await.unwrap();
    let mut bob = Running::start(client, &service, b"bob", options(LONG_INTERVAL), None);
    let (_, saved_cursor) = bob.initial_conversation_cursor().await;
    bob.wait_for_idle().await;
    let (mut client, wire) = bob.stop().await;
    assert!(client.messages(1).await.unwrap().is_empty());
    drop((client, wire, service));

    let proxy = CommitProxy::start(&database).await;
    let alice_id = generate_id128();
    let key = generate_id128();
    let ids = [store_id, alice_id, bob_id.device_id, key]
        .map(hex::encode)
        .join(" ");
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tidb_client_recovers_after_service_crashes_between_commit_and_push",
            "--ignored",
            "--nocapture",
        ])
        .env(CRASH_DB, proxy.url.as_str())
        .env(CRASH_IDS, ids)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap()).lines();
    tokio::time::timeout(WAIT, async {
        while let Some(line) = output.next_line().await.unwrap() {
            if line.contains("SERVICE_READY") {
                return;
            }
        }
        panic!("the child service exited before authentication");
    })
    .await
    .unwrap();

    let observer = MySqlPool::connect(database.as_str()).await.unwrap();
    let (committed, release) = proxy.arm(CommitPhase::After);
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"send\n")
        .await
        .unwrap();
    tokio::time::timeout(WAIT, committed)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // TiDB has committed, but the child is still blocked waiting for the COMMIT reply.
    // TidbService cannot produce the SEND_ACK or the receiver's PUSH until that reply arrives.
    let checkpoint: Vec<(Vec<u8>, Vec<u8>, u64)> = sqlx::query_as(
        "SELECT client_msg_id, message_id, `sequence` FROM messages WHERE tenant_id = 1 AND conversation_id = 1"
    ).fetch_all(&observer).await.unwrap();
    let [(client_msg_id, message_id, sequence)] = checkpoint.as_slice() else {
        panic!("expected one committed message");
    };
    assert_eq!(client_msg_id.as_slice(), key);
    assert_eq!(*sequence, 1);
    child.start_kill().unwrap();
    assert!(!tokio::time::timeout(WAIT, child.wait())
        .await
        .unwrap()
        .unwrap()
        .success());
    release.send(()).unwrap();

    let service = open_server(database.as_str(), store_id, seeded(), OpenMode::Resume).await;
    let (mut alice, _) = authenticate(&service, b"alice", alice_id).await;
    let (status, pushes) = alice
        .call(
            MsgType::SendStatus,
            &SendStatusRequest {
                key: Some(message_key(key)),
            },
        )
        .await;
    assert!(pushes.is_empty());
    let DecodedBody::SendStatusAck(status) = status else {
        panic!("expected SEND_STATUS_ACK");
    };
    assert_eq!(status.key, Some(message_key(key)));
    let Some(send_outcome::Value::Committed(receipt)) = status.outcome.unwrap().value else {
        panic!("expected the original receipt");
    };
    assert_eq!(
        receipt.r#ref.as_ref().unwrap().message_id.as_deref(),
        Some(message_id.as_slice())
    );

    let client = Client::open(&path, bob_id).await.unwrap();
    let mut bob = Running::start(client, &service, b"bob", options(LONG_INTERVAL), None);
    bob.wait_for_sync_through(1).await;
    bob.wait_for_idle().await;
    let (mut client, wire) = bob.stop().await;
    assert_eq!(
        wire.syncs[1].cursor.as_deref(),
        Some(saved_cursor.as_slice())
    );
    let [received] = client
        .messages(1)
        .await
        .unwrap()
        .try_into()
        .expect("one recovered message");
    assert_eq!(received.r#ref, receipt.r#ref);
    assert_eq!(received.position, receipt.position);
    assert_eq!(received.accepted_at_ms, receipt.accepted_at_ms);
    assert_eq!(text(&received), CRASH_TEXT);
}
