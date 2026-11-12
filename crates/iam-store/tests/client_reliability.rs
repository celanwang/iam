mod support;

use iam_client::{Client, Identity, Options, Submission, Transport};
use iam_core::Engine;
use iam_protocol::pb::{
    change, message_content, send_outcome, snapshot_response, sync_response, sync_target,
    AuthRequest, Head, MessageContent, MessageKey, SendRequest, TextContent,
};
use iam_protocol::{
    decode_frame, encode_frame, generate_id128, DecodedBody, Frame, FrameDecoder, Id128, MsgType,
};
use iam_store::{OpenMode, TidbService};
use prost::Message;
use std::{io, sync::Arc, time::Duration};
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
    if fault == Fault::BeforeSend {
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
    let server = server().await;
    let mut sender = sender(&server).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("bob.db");
    let device = identity(200);
    let client = Client::open(&path, device).await.unwrap();
    let mut bob = Running::start(client, &server, b"bob", options(LONG_INTERVAL), None);
    let (cid, original_cursor) = bob.initial_conversation_cursor().await;
    bob.wait_for_idle().await;

    server.lock().await.disconnect(Some(cid));
    assert!(sender.send_text("missed one").await.is_empty());
    assert!(sender.send_text("missed two").await.is_empty());
    bob.inbound
        .send(Err(io::Error::from(io::ErrorKind::ConnectionReset)))
        .unwrap();
    bob.wait_for_sync_through(2).await;
    bob.wait_for_idle().await;
    let (client, wire) = bob.stop().await;
    assert_eq!(wire.connections, 2);
    assert_eq!(wire.devices, vec![device.device_id, device.device_id]);
    let resumed = &wire.syncs[2..];
    assert_eq!(resumed.len(), 3);
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
    let mut engine = Engine::new(0);
    engine.store.seed_user(1, 100, b"alice".to_vec());
    engine.store.seed_user(1, 200, b"bob".to_vec());
    engine.store.seed_direct(1, 1, 100, 200);
    Arc::new(Mutex::new(
        TidbService::connect_local(
            database.as_str(),
            generate_id128(),
            engine,
            OpenMode::Initialize,
        )
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    BeforeSend,
    AfterSend,
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
        if fault == Some(Fault::BeforeSend) {
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
        if fault == Some(Fault::AfterSend) {
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
        self.inbound.recv().await.expect("test input stays open")
    }
}

struct Running {
    submissions: mpsc::Sender<Submission>,
    inbound: mpsc::UnboundedSender<io::Result<Vec<u8>>>,
    events: mpsc::UnboundedReceiver<Event>,
    task: tokio::task::JoinHandle<(Client, LocalTransport)>,
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
            client
                .run(&mut wire, token, options, &mut commands)
                .await
                .unwrap();
            (client, wire)
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

    async fn wait_for_sync_through(&mut self, through: u64) {
        self.wait_for(|event| {
            matches!(event, Event::Reply(body)
                if matches!(body.as_ref(), DecodedBody::SyncAck(ack)
                    if matches!(&ack.value, Some(sync_response::Value::Page(page))
                        if page.covered_through == Some(through) && page.changes.iter().any(
                            |change| matches!(change.payload, Some(change::Payload::MessageUpsert(_)))))))
        })
        .await;
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
        tokio::time::timeout(WAIT, self.task)
            .await
            .expect("client did not stop")
            .unwrap()
    }
}

struct Sender {
    server: Server,
    cid: Option<Id128>,
    req: u64,
}

async fn sender(server: &Server) -> Sender {
    let mut sender = Sender {
        server: server.clone(),
        cid: None,
        req: 0,
    };
    sender
        .call(
            MsgType::Auth,
            &AuthRequest {
                access_token: Some(b"alice".as_slice().into()),
                device_id: Some(generate_id128().to_vec().into()),
                requested_capabilities: Vec::new(),
            },
        )
        .await;
    sender
}

impl Sender {
    async fn send_text(&mut self, text: &str) -> Pushes {
        self.call(
            MsgType::Send,
            &SendRequest {
                key: Some(MessageKey {
                    conversation_id: Some(1),
                    client_msg_id: Some(generate_id128().to_vec().into()),
                }),
                content: Some(MessageContent {
                    value: Some(message_content::Value::Text(TextContent {
                        text: Some(text.into()),
                    })),
                }),
            },
        )
        .await
    }

    async fn call(&mut self, kind: MsgType, body: &impl Message) -> Pushes {
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
        if kind == MsgType::Send {
            assert!(
                matches!(decode_frame(&frame(&output.responses[0])).unwrap().body,
                DecodedBody::SendAck(ack) if matches!(ack.outcome.as_ref().unwrap().value,
                    Some(send_outcome::Value::Committed(_))))
            );
        }
        output.pushes
    }
}

fn frame(bytes: &[u8]) -> Frame {
    let mut decoder = FrameDecoder::new();
    decoder.push(bytes);
    let frame = decoder.next_frame().unwrap().unwrap();
    decoder.finish().unwrap();
    frame
}
