//! Opt-in, in-process protocol benchmark against real local TiDB.

#[path = "perf_locks.rs"]
mod locks;
use super::support;
use crate::{OpenMode, TidbService};
use iam_client::{Client, Identity as ClientIdentity, Options, Transport};
use iam_core::Engine;
use iam_protocol::pb::{
    auth_response, message_content, send_outcome, snapshot_response, sync_response, sync_target,
    AuthRequest, Head, MessageContent, MessageKey, PageLimit, SendRequest, SnapshotRequest,
    SyncRequest, SyncTarget, TextContent,
};
use iam_protocol::{
    decode_frame, encode_frame, generate_id128, DecodedBody, FrameDecoder, Id128, MsgType,
};
use prost::Message;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, SqliteConnection};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    io,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot, Barrier};

const DEVICES: usize = 4;
const WARMUP: usize = 8;
const WAVES: usize = 160;
const SENDS_PER_WAVE: usize = 3;
const REQUESTS_PER_BACKGROUND_WAVE: usize = 4;
const BACKLOG: u64 = 24;
const PAGE_ITEMS: u32 = 8;
const TOKEN: &[u8] = b"reader";

type Samples = BTreeMap<(&'static str, &'static str), Vec<Duration>>;

tokio::task_local! {
    static SPANS: RefCell<Spans>;
}

#[derive(Default)]
pub(crate) struct Spans {
    timings: Vec<(&'static str, Duration)>,
    batch: Option<(u64, usize, Duration, Duration)>,
}

impl Spans {
    pub fn duration(&self, metric: &str) -> Duration {
        self.timings
            .iter()
            .find(|(name, _)| *name == metric)
            .map_or(Duration::ZERO, |(_, elapsed)| *elapsed)
    }
}

pub(crate) async fn capture<T>(future: impl std::future::Future<Output = T>) -> (T, Spans) {
    SPANS
        .scope(RefCell::new(Spans::default()), async {
            let result = future.await;
            (result, SPANS.with(|spans| spans.take()))
        })
        .await
}

pub(crate) fn record(metric: &'static str, start: Instant) {
    let _ = SPANS.try_with(|spans| spans.borrow_mut().timings.push((metric, start.elapsed())));
}

pub(crate) fn record_batch(
    id: u64,
    size: usize,
    transaction: Duration,
    control_sql: Duration,
    elapsed: Duration,
    queued: Duration,
) {
    let _ = SPANS.try_with(|spans| {
        let mut spans = spans.borrow_mut();
        spans.timings.push(("clock_request", elapsed));
        spans.timings.push(("clock_queue", queued));
        spans.batch = Some((id, size, transaction, control_sql));
    });
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Load {
    Send,
    Auth,
    EmptySync,
    Reconnect,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "profiles real local TiDB; run by exact name with no concurrent load"]
async fn profile_sync_contention() {
    tokio::task::LocalSet::new()
        .run_until(async {
            for load in [Load::Send, Load::Auth, Load::EmptySync, Load::Reconnect] {
                for batching in [false, true] {
                    tokio::time::timeout(Duration::from_secs(90), run_case(load, batching))
                        .await
                        .unwrap_or_else(|_| panic!("{load:?} batching={batching} timed out"));
                }
            }
        })
        .await;
}

async fn run_case(load: Load, batching: bool) {
    let database = support::isolated_database().await;
    let id = generate_id128();
    let mut writer = Peer::open(database.as_str(), id, OpenMode::Initialize).await;
    writer.service.set_clock_batching(batching);
    writer.auth(b"writer", generate_id128()).await;
    let workers = if load == Load::Send { 0 } else { DEVICES };
    let barrier = Arc::new(Barrier::new(workers + 1));
    let directory = tempfile::tempdir().unwrap();
    let mut readers = Vec::new();
    let mut service = writer.service.clone();
    service.set_clock_batching(batching);
    for index in 0..workers {
        let mut peer = Peer::new(service.clone());
        let device = generate_id128();
        let root = directory.path().join(index.to_string());
        std::fs::create_dir(&root).unwrap();
        let empty = peer.initial_cursor(device).await;
        let baseline = root.join("baseline.db");
        let identity = ClientIdentity {
            tenant_id: 1,
            user_id: 200,
            device_id: device,
        };
        if load == Load::Reconnect {
            let original = root.join("initial.db");
            let mut client = Client::open(&original, identity).await.unwrap();
            run_receiver(&mut client, &mut peer, 0).await;
            drop(client);
            sqlite_backup(&original, &baseline).await;
        }
        readers.push((peer, empty, identity, root, baseline));
    }
    if load == Load::Reconnect {
        for sequence in 1..=BACKLOG {
            writer.send(2, &format!("missed {sequence}")).await;
        }
    }

    let readers: Vec<_> = readers
        .into_iter()
        .map(|(mut peer, empty, identity, root, baseline)| {
            let barrier = barrier.clone();
            tokio::task::spawn_local(async move {
                for wave in 0..(WARMUP + WAVES) {
                    let mut receiver = if load == Load::Reconnect {
                        let path = root.join(format!("wave-{wave}.db"));
                        std::fs::copy(&baseline, &path).unwrap();
                        Some(Client::open(&path, identity).await.unwrap())
                    } else {
                        None
                    };
                    peer.measured = wave >= WARMUP;
                    barrier.wait().await;
                    match load {
                        Load::Auth => {
                            for _ in 0..REQUESTS_PER_BACKGROUND_WAVE {
                                peer.auth(TOKEN, identity.device_id).await;
                            }
                        }
                        Load::EmptySync => {
                            for _ in 0..REQUESTS_PER_BACKGROUND_WAVE {
                                let response = peer.call(MsgType::Sync, &empty).await;
                                assert!(matches!(response, DecodedBody::SyncAck(ack)
                                if matches!(&ack.value, Some(sync_response::Value::Page(page))
                                    if page.changes.is_empty() && page.next_page_token.is_none())));
                            }
                        }
                        Load::Reconnect => {
                            let started = Instant::now();
                            let client = receiver.as_mut().unwrap();
                            run_receiver(client, &mut peer, BACKLOG).await;
                            peer.sample("RECONNECT", "persisted_catchup", started.elapsed());
                            let messages = client.messages(2).await.unwrap();
                            assert_eq!(messages.len(), BACKLOG as usize);
                            assert!(messages.iter().enumerate().all(|(index, message)| message
                                .position
                                .as_ref()
                                .unwrap()
                                .create_seq
                                == Some(index as u64 + 1)));
                        }
                        Load::Send => unreachable!(),
                    }
                    barrier.wait().await;
                }
                (peer.samples, peer.batches)
            })
        })
        .collect();

    for _ in 0..WARMUP {
        barrier.wait().await;
        for _ in 0..SENDS_PER_WAVE {
            writer.send(1, "warmup").await;
        }
        barrier.wait().await;
    }

    let (stop, observer) = locks::observe(database.clone()).await;
    let before = locks::scrape().await;
    writer.measured = true;
    let started = Instant::now();
    for _ in 0..WAVES {
        barrier.wait().await;
        for _ in 0..SENDS_PER_WAVE {
            writer.send(1, "send probe").await;
        }
        barrier.wait().await;
    }
    let elapsed = started.elapsed();
    stop.send(()).unwrap();
    let after = locks::scrape().await;
    let observed = observer.await.unwrap();

    println!(
        "CASE {load:?} batching={batching} waves={WAVES} sends_per_wave={SENDS_PER_WAVE} background={workers} elapsed_s={:.3}",
        elapsed.as_secs_f64()
    );
    let mut samples = writer.samples;
    let mut batches = writer.batches;
    for reader in readers {
        let (read_samples, read_batches) = reader.await.unwrap();
        batches.extend(read_batches);
        for (key, values) in read_samples {
            samples.entry(key).or_default().extend(values);
        }
    }
    if !batches.is_empty() {
        println!(
            "CLOCK_BATCHES n={} requests={} max_size={}",
            batches.len(),
            batches.values().map(|(size, _, _)| size).sum::<usize>(),
            batches.values().map(|(size, _, _)| size).max().unwrap()
        );
        for (_, (_, transaction, control_sql)) in batches {
            samples
                .entry(("BATCH", "clock_tx"))
                .or_default()
                .push(transaction);
            samples
                .entry(("BATCH", "control_sql"))
                .or_default()
                .push(control_sql);
        }
    }
    for ((command, metric), mut values) in samples {
        values.sort_unstable();
        let millis = |p: usize| values[(values.len() * p).div_ceil(100) - 1].as_secs_f64() * 1000.0;
        println!(
            "{command} {metric} n={} p50_ms={:.3} p95_ms={:.3} p99_ms={:.3}",
            values.len(),
            millis(50),
            millis(95),
            millis(99)
        );
    }
    observed.report();
    if let (Some(before), Some(after)) = (before, after) {
        after.report_since(&before);
    }
}

fn engine() -> Engine {
    let mut engine = Engine::new(0);
    engine.store.seed_user(1, 100, b"writer".to_vec());
    engine.store.seed_user(1, 200, TOKEN.to_vec());
    engine.store.seed_direct(1, 1, 100, 300);
    engine.store.seed_direct(1, 2, 100, 200);
    engine
}

struct Peer {
    service: TidbService,
    cid: Option<Id128>,
    req: u64,
    measured: bool,
    samples: Samples,
    batches: BTreeMap<u64, (usize, Duration, Duration)>,
    receiver: Option<ReceiverProbe>,
}

struct ReceiverProbe {
    through: u64,
    last_page: bool,
    done: Option<oneshot::Sender<()>>,
}

impl Peer {
    async fn open(database: &str, id: Id128, mode: OpenMode) -> Self {
        let service = TidbService::connect_local(database, id, engine(), mode)
            .await
            .unwrap();
        Self::new(service)
    }

    fn new(service: TidbService) -> Self {
        Self {
            service,
            cid: None,
            req: 0,
            measured: false,
            samples: Samples::new(),
            batches: BTreeMap::new(),
            receiver: None,
        }
    }

    fn sample(&mut self, command: &'static str, metric: &'static str, value: Duration) {
        if self.measured {
            self.samples
                .entry((command, metric))
                .or_default()
                .push(value);
        }
    }

    async fn auth(&mut self, token: &[u8], device: Id128) {
        self.service.disconnect(self.cid.take());
        self.req = 0;
        let response = self
            .call(
                MsgType::Auth,
                &AuthRequest {
                    access_token: Some(token.to_vec().into()),
                    device_id: Some(device.to_vec().into()),
                    requested_capabilities: vec![],
                },
            )
            .await;
        assert!(matches!(response, DecodedBody::AuthAck(ack)
            if matches!(&ack.value, Some(auth_response::Value::Session(_)))));
    }

    async fn initial_cursor(&mut self, device: Id128) -> SyncRequest {
        self.auth(TOKEN, device).await;
        let mut request = SyncRequest {
            target: Some(SyncTarget {
                value: Some(sync_target::Value::Conversation(2)),
            }),
            limit: Some(page_limit()),
            ..SyncRequest::default()
        };
        let DecodedBody::SyncAck(ack) = self.call(MsgType::Sync, &request).await else {
            panic!("SYNC_ACK");
        };
        let Some(sync_response::Value::Reset(reset)) = ack.value else {
            panic!("initial sync reset");
        };
        let DecodedBody::SnapshotAck(ack) = self
            .call(
                MsgType::Snapshot,
                &SnapshotRequest {
                    page_token: reset.snapshot_token,
                    limit: request.limit,
                },
            )
            .await
        else {
            panic!("SNAPSHOT_ACK");
        };
        let Some(snapshot_response::Value::Page(page)) = ack.value else {
            panic!("initial snapshot");
        };
        request.cursor = Some(page.resume_cursor.unwrap());
        request
    }

    async fn send(&mut self, target: u64, text: &str) {
        let response = self
            .call(
                MsgType::Send,
                &SendRequest {
                    key: Some(MessageKey {
                        conversation_id: Some(target),
                        client_msg_id: Some(generate_id128().to_vec().into()),
                    }),
                    content: Some(MessageContent {
                        value: Some(message_content::Value::Text(TextContent {
                            text: Some(text.into()),
                        })),
                    }),
                },
            )
            .await;
        assert!(
            matches!(&response, DecodedBody::SendAck(ack)
            if matches!(ack.outcome.as_ref().unwrap().value, Some(send_outcome::Value::Committed(_)))),
            "send did not commit: {response:?}"
        );
    }

    async fn call(&mut self, kind: MsgType, body: &impl Message) -> DecodedBody {
        self.req += 1;
        let bytes = encode_frame(
            kind,
            &Head {
                req_id: Some(self.req),
            },
            body,
        )
        .unwrap();
        self.exchange_wire(bytes).await.1
    }

    async fn exchange_wire(&mut self, bytes: Vec<u8>) -> (Vec<u8>, DecodedBody) {
        let frame = frame(&bytes);
        let kind = match decode_frame(&frame).unwrap().msg_type {
            MsgType::Send => "SEND",
            MsgType::Auth => "AUTH",
            MsgType::Sync => "SYNC",
            MsgType::Snapshot => "SNAPSHOT",
            other => panic!("unexpected benchmark request: {other:?}"),
        };
        let started = Instant::now();
        let call = self.service.handle_frame(self.cid, &frame);
        let (result, spans) = if self.measured {
            capture(call).await
        } else {
            (call.await, Spans::default())
        };
        let elapsed = started.elapsed();
        let (cid, output) = result.unwrap();
        self.cid = cid;
        let [response] = output.responses.try_into().expect("one response");
        self.sample(kind, "request", elapsed);
        if let (Some((_, total)), Some((_, control))) = (
            spans
                .timings
                .iter()
                .find(|(metric, _)| *metric == "send_tx"),
            spans
                .timings
                .iter()
                .find(|(metric, _)| *metric == "control_sql"),
        ) {
            self.sample(kind, "send_tx_other", total.saturating_sub(*control));
        }
        if let Some((id, size, transaction, control_sql)) = spans.batch {
            self.batches.insert(id, (size, transaction, control_sql));
        }
        for (metric, value) in spans.timings {
            self.sample(kind, metric, value);
        }
        let decoded = decode_frame(&self::frame(&response)).unwrap().body;
        (response, decoded)
    }
}

impl Transport for Peer {
    async fn reconnect(&mut self) -> io::Result<()> {
        self.service.disconnect(self.cid.take());
        Ok(())
    }

    async fn exchange(&mut self, request: Vec<u8>) -> io::Result<Vec<u8>> {
        let (bytes, body) = self.exchange_wire(request).await;
        if let Some(probe) = &mut self.receiver {
            let final_page = match body {
                DecodedBody::SnapshotAck(ack) if probe.through == 0 => matches!(ack.value,
                    Some(snapshot_response::Value::Page(page))
                    if page.resume_cursor.is_some() && is_receiver_stream(page.context.as_ref())),
                DecodedBody::SyncAck(ack) => matches!(ack.value,
                    Some(sync_response::Value::Page(page)) if page.covered_through == Some(probe.through)
                        && page.next_page_token.is_none() && is_receiver_stream(page.context.as_ref())),
                _ => false,
            };
            probe.last_page |= final_page;
        }
        Ok(bytes)
    }

    async fn receive(&mut self) -> io::Result<Vec<u8>> {
        if let Some(probe) = &mut self.receiver {
            if probe.last_page {
                if let Some(done) = probe.done.take() {
                    let _ = done.send(());
                }
            }
        }
        std::future::pending().await
    }
}

fn is_receiver_stream(context: Option<&iam_protocol::pb::StreamContext>) -> bool {
    context
        .and_then(|ctx| ctx.target.as_ref())
        .and_then(|target| target.value.as_ref())
        == Some(&sync_target::Value::Conversation(2))
}

async fn run_receiver(client: &mut Client, peer: &mut Peer, through: u64) {
    let (done, caught_up) = oneshot::channel();
    peer.receiver = Some(ReceiverProbe {
        through,
        last_page: false,
        done: Some(done),
    });
    let (_submissions, mut commands) = mpsc::channel(1);
    let options = Options {
        sync_interval: Duration::from_secs(3600),
        page_items: PAGE_ITEMS,
        ..Options::default()
    };
    tokio::select! {
        result = client.run(peer, TOKEN, options, &mut commands) => panic!("receiver stopped: {result:?}"),
        result = caught_up => result.unwrap(),
    }
    peer.receiver = None;
}

async fn sqlite_backup(source: &Path, destination: &Path) {
    let mut db = SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(source))
        .await
        .unwrap();
    sqlx::query("VACUUM INTO ?")
        .bind(destination.to_str().unwrap())
        .execute(&mut db)
        .await
        .unwrap();
    db.close().await.unwrap();
}

fn page_limit() -> PageLimit {
    PageLimit {
        max_items: Some(PAGE_ITEMS),
        max_bytes: Some(256 * 1024),
    }
}

fn frame(bytes: &[u8]) -> iam_protocol::Frame {
    let mut decoder = FrameDecoder::new();
    decoder.push(bytes);
    let frame = decoder.next_frame().unwrap().unwrap();
    decoder.finish().unwrap();
    frame
}
