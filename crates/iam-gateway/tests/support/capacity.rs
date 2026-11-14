use super::*;
use iam_client::Submission;
use iam_protocol::{FixedHeader, HEADER_LEN};
use sqlx::MySqlPool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::task::LocalSet;
use tokio::time::sleep_until;
use tokio_rustls::client::TlsStream;

const SENDERS: usize = 4;
const BACKLOG: u64 = 24;
const SEND_WINDOW: Duration = Duration::from_secs(6);
const BURST_OFFSET: Duration = Duration::from_millis(750);
const RECOVERY_LIMIT: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "capacity profile; requires an idle real local TiDB"]
async fn tidb_tls_gateway_capacity() {
    for (devices, rate) in [(0, 100), (0, 200), (8, 100), (32, 100), (64, 100)] {
        reconnect_profile(devices, rate, None).await;
    }
    slow_reader_profile(false).await;
    slow_reader_profile(true).await;
}

#[cfg(feature = "profile")]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "detailed profile; requires an idle real local TiDB"]
async fn tidb_tls_gateway_reconnect_profile() {
    for devices in [0, 32, 64] {
        reconnect_profile(devices, 100, None).await;
    }
}

#[cfg(feature = "profile")]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "clock window comparison; requires an idle real local TiDB"]
async fn tidb_tls_gateway_clock_windows() {
    for (round, windows) in [[1, 2, 4], [2, 4, 1], [4, 1, 2]].into_iter().enumerate() {
        for devices in [0, 32, 64] {
            for window in windows {
                println!("round={} devices={devices} window_ms={window}", round + 1);
                reconnect_profile(devices, 100, Some(window)).await;
            }
        }
    }
}

async fn reconnect_profile(devices: usize, rate: u64, clock_window_ms: Option<u64>) {
    let fixture = Fixture::new().await;
    let senders = credentials(&fixture, 100, SENDERS);
    let receivers = credentials(&fixture, 400, devices);
    let source = credentials(&fixture, 300, 1)[0];
    let gateway = fixture
        .start_gateway(
            OpenMode::Initialize,
            Limits::default(),
            "127.0.0.1:0",
            senders.iter().chain(&receivers).copied().chain([source]),
            clock_window_ms,
        )
        .await;
    let mut writers = open_senders(&gateway, &senders).await;
    let mut recovering = Vec::new();
    let directory = tempfile::tempdir().unwrap();
    if devices != 0 {
        // Provision the second direct conversation through the service's normal application API.
        gateway.service.create_direct(1, 2, 300, 400).await.unwrap();
        let mut source = open_sender(&gateway, &source).await;
        // Seed position 1 so both account and conversation cursors exist before the disconnect.
        assert!(matches!(
            timed_send(&mut source, 2, "before disconnect").await,
            SendResult::Committed
        ));
        for (number, credential) in receivers.iter().enumerate() {
            let path = directory.path().join(format!("{number}.db"));
            let identity = Identity {
                tenant_id: 1,
                user_id: credential.user_id,
                device_id: credential.device_id,
            };
            let mut receiver = RecoveryClient::new(&gateway, &path, identity, 2);
            receiver.sync_through(1, None).await;
            recovering.push(receiver);
        }
        for _ in 0..BACKLOG {
            assert!(matches!(
                timed_send(&mut source, 2, "missed while offline").await,
                SendResult::Committed
            ));
        }
    }
    warm(&mut writers, "warm up").await;
    #[cfg(feature = "profile")]
    iam_gateway::profile::start();
    #[cfg(feature = "profile")]
    let clock_start = gateway.service.clock_counts();
    let start = Instant::now() + Duration::from_millis(100);
    let local = LocalSet::new();
    let mut recoveries = tokio::task::JoinSet::new();
    for mut receiver in recovering {
        recoveries.spawn_local_on(
            async move {
                let wave = start + BURST_OFFSET;
                sleep_until(wave).await;
                receiver.sync_through(BACKLOG + 1, Some(wave)).await
            },
            &local,
        );
    }
    let label = format!("reconnect-{devices}-at-{rate}");
    let (sends, report) = local
        .run_until(async {
            tokio::join!(
                fixed_rate(writers, start, rate, "gateway capacity", 1),
                async {
                    let mut report = RecoveryReport::default();
                    while let Some(result) = recoveries.join_next().await {
                        report.add(result.unwrap());
                    }
                    report
                }
            )
        })
        .await;
    assert_eq!(report.recovered.len(), devices);
    let burst_end =
        start + BURST_OFFSET + report.recovered.iter().max().copied().unwrap_or_default();
    sends.report(&label, Some((start + BURST_OFFSET, burst_end)));
    if devices != 0 {
        report.print(&label);
    }
    #[cfg(feature = "profile")]
    {
        let clock_end = gateway.service.clock_counts();
        let mut groups = std::collections::BTreeMap::<_, Vec<_>>::new();
        for (kind, stage, duration) in iam_gateway::profile::finish() {
            groups
                .entry((kind.map(|kind| kind as u8), stage))
                .or_default()
                .push(duration);
        }
        let clock_requests: usize = groups
            .iter()
            .filter(|((_, stage), _)| *stage == "clock_queue")
            .map(|(_, times)| times.len())
            .sum();
        let attempted = clock_end.attempted - clock_start.attempted;
        let committed = clock_end.committed - clock_start.committed;
        println!("case={label} clock_requests={clock_requests} clock_attempted={attempted} clock_committed={committed}");
        assert_eq!(
            attempted, committed,
            "failed clock transactions in capacity run"
        );
        if devices == 0 {
            assert_eq!((clock_requests, committed), (0, 0));
        } else {
            assert!(committed > 0 && committed <= clock_requests as u64);
        }
        for ((kind, stage), samples) in groups {
            println!(
                "case={label} command={:?} stage={stage} n={} ms={}",
                kind.and_then(MsgType::from_u8),
                samples.len(),
                percentiles(samples)
            );
        }
    }
    gateway.stop().await;
    remove_database(&fixture).await;
}

async fn slow_reader_profile(slow: bool) {
    let fixture = Fixture::new().await;
    let senders = credentials(&fixture, 100, SENDERS);
    let receivers = credentials(&fixture, 200, 2);
    let gateway = fixture
        .start_with_credentials(
            OpenMode::Initialize,
            Limits::default(),
            "127.0.0.1:0",
            senders.iter().chain(&receivers).copied(),
        )
        .await;
    let text = "x".repeat(64 * 1024);
    let mut writers = open_senders(&gateway, &senders).await;
    warm(&mut writers, &text).await;
    let directory = tempfile::tempdir().unwrap();
    let identity = Identity {
        tenant_id: 1,
        user_id: 200,
        device_id: receivers[0].device_id,
    };
    let mut recovery =
        RecoveryClient::new(&gateway, &directory.path().join("receiver.db"), identity, 1);
    let before = 2 * SENDERS as u64;
    recovery.sync_through(before, None).await;
    let receiver = open_raw_reader(&gateway, receivers[0], slow).await;
    let fast_peer = open_raw_reader(&gateway, receivers[1], false).await;
    let start = Instant::now() + Duration::from_millis(100);
    let mut fast_pushes = tokio::spawn(drain_pushes(fast_peer));
    let mut target_pushes = tokio::spawn(async move {
        if slow {
            // Bypass TlsTransport, whose background task would keep reading the real socket.
            sleep_until(start + SEND_WINDOW + Duration::from_secs(1)).await;
        }
        drain_pushes(receiver).await
    });
    let sends = fixed_rate(writers, start, 70, &text, 1).await;
    let fast = timeout(WAIT, &mut fast_pushes).await.unwrap().unwrap();
    let target = timeout(WAIT, &mut target_pushes).await.unwrap().unwrap();
    assert!(
        fast.count > 0 && !fast.closed,
        "healthy receiver stopped receiving"
    );
    if slow {
        assert!(
            target.closed && target.count < fast.count,
            "slow receiver did not saturate the real socket: {target:?}"
        );
    }
    let label = if slow {
        "paused-reader"
    } else {
        "reading-control"
    };
    let last_sequence = before + sends.committed.len() as u64;
    let recovered = recovery.sync_through(last_sequence, None).await;
    sends.report(label, None);
    println!(
        "case={label} healthy_pushes={} target_pushes={} target_closed={} recovery_ms={:.2}",
        fast.count,
        target.count,
        target.closed,
        ms(recovered.elapsed)
    );
    gateway.stop().await;
    remove_database(&fixture).await;
}

fn credentials(fixture: &Fixture, user_id: u64, count: usize) -> Vec<DeviceCredential> {
    (0..count)
        .map(|_| {
            let device_id = generate_id128();
            DeviceCredential {
                tenant_id: 1,
                user_id,
                device_id,
                expires_at_ms: fixture.expires_at_ms,
                token_sha256: Sha256::digest(device_id).into(),
            }
        })
        .collect()
}

struct Sender {
    transport: TlsTransport,
    req_id: u64,
}

async fn open_senders(gateway: &RunningGateway, credentials: &[DeviceCredential]) -> Vec<Sender> {
    let mut senders = Vec::with_capacity(credentials.len());
    for credential in credentials {
        senders.push(open_sender(gateway, credential).await);
    }
    senders
}

async fn open_sender(gateway: &RunningGateway, credential: &DeviceCredential) -> Sender {
    let mut transport = gateway.client();
    timeout(WAIT, transport.reconnect()).await.unwrap().unwrap();
    assert!(matches!(
        auth(
            &mut transport,
            1,
            credential.device_id,
            &credential.device_id
        )
        .await,
        auth_response::Value::Session(_)
    ));
    Sender {
        transport,
        req_id: 1,
    }
}

async fn warm(senders: &mut [Sender], text: &str) {
    for _ in 0..2 {
        for sender in &mut *senders {
            assert!(matches!(
                timed_send(sender, 1, text).await,
                SendResult::Committed
            ));
        }
    }
}

#[derive(Debug)]
enum SendResult {
    Committed,
    Rejected,
    TimedOut,
    Other,
}

async fn timed_send(sender: &mut Sender, conversation: u64, text: &str) -> SendResult {
    sender.req_id += 1;
    let mut message_key = key();
    message_key.conversation_id = Some(conversation);
    let body = SendRequest {
        key: Some(message_key.clone()),
        content: Some(MessageContent {
            value: Some(message_content::Value::Text(TextContent {
                text: Some(text.to_owned()),
            })),
        }),
    };
    let bytes = encode_frame(
        MsgType::Send,
        &Head {
            req_id: Some(sender.req_id),
        },
        &body,
    )
    .unwrap();
    match timeout(WAIT, sender.transport.exchange(bytes)).await {
        Err(_) => SendResult::TimedOut,
        Ok(Err(_)) => SendResult::Other,
        Ok(Ok(bytes)) => {
            let mut decoder = FrameDecoder::new();
            decoder.push(&bytes);
            let frame = decode_frame(&decoder.next_frame().unwrap().unwrap()).unwrap();
            assert_eq!(frame.head.req_id, Some(sender.req_id));
            match frame.body {
                DecodedBody::SendAck(reply) => {
                    assert_eq!(reply.key, Some(message_key));
                    match reply.outcome.unwrap().value.unwrap() {
                        send_outcome::Value::Committed(_) => SendResult::Committed,
                        send_outcome::Value::RejectedFinal(_) => SendResult::Rejected,
                        send_outcome::Value::Unresolved(_) => SendResult::Other,
                    }
                }
                DecodedBody::Error(error)
                    if matches!(
                        error.error.as_ref().unwrap().code(),
                        ErrorCode::Overloaded | ErrorCode::RateLimited | ErrorCode::ResourceLimit
                    ) =>
                {
                    SendResult::Rejected
                }
                _ => SendResult::Other,
            }
        }
    }
}

#[derive(Default)]
struct SendReport {
    planned: usize,
    missed: usize,
    rejected: usize,
    timed_out: usize,
    other: usize,
    schedule_lag: Vec<Duration>,
    committed: Vec<(Instant, Duration, Duration)>,
}

impl SendReport {
    fn report(&self, label: &str, burst: Option<(Instant, Instant)>) {
        let actual = self.planned - self.missed;
        assert_eq!(
            actual,
            self.committed.len() + self.rejected + self.timed_out + self.other
        );
        assert!(self.committed.len() >= 100, "too few committed samples");
        let request = self
            .committed
            .iter()
            .map(|&(_, request, _)| request)
            .collect();
        let scheduled = self
            .committed
            .iter()
            .map(|&(_, _, scheduled)| scheduled)
            .collect();
        let burst_request: Vec<_> = self
            .committed
            .iter()
            .filter_map(|&(start, request, _)| {
                burst
                    .filter(|&(from, to)| start >= from && start <= to)
                    .map(|_| request)
            })
            .collect();
        let burst_count = burst_request.len();
        println!("case={label} planned={} sent={actual} committed={} missed={} rejected={} timeout={} other={} request_ms={} scheduled_ms={} scheduler_lag_ms={} burst_committed={burst_count} burst_request_ms={}",
            self.planned, self.committed.len(), self.missed, self.rejected, self.timed_out, self.other,
            percentiles(request), percentiles(scheduled), percentiles(self.schedule_lag.clone()), percentiles(burst_request));
        assert_eq!(
            self.rejected + self.timed_out + self.other,
            0,
            "foreground sends failed"
        );
    }
}

async fn fixed_rate(
    mut available: Vec<Sender>,
    start: Instant,
    rate: u64,
    text: &str,
    conversation: u64,
) -> SendReport {
    let offered = (SEND_WINDOW.as_secs() * rate) as usize;
    let text: Arc<str> = text.into();
    let mut pending = tokio::task::JoinSet::new();
    let mut report = SendReport {
        planned: offered,
        ..SendReport::default()
    };
    for n in 0..offered {
        let due = start + Duration::from_micros(n as u64 * 1_000_000 / rate);
        sleep_until(due).await;
        while let Some(completed) = pending.try_join_next() {
            if let Some(sender) = record(&mut report, completed.unwrap()) {
                available.push(sender);
            }
        }
        let Some(mut sender) = available.pop() else {
            report.missed += 1;
            continue;
        };
        let text = text.clone();
        pending.spawn(async move {
            let sent = Instant::now();
            let result = timed_send(&mut sender, conversation, &text).await;
            (sender, result, due, sent, Instant::now())
        });
    }
    while let Some(completed) = pending.join_next().await {
        record(&mut report, completed.unwrap());
    }
    report
}

fn record(
    report: &mut SendReport,
    result: (Sender, SendResult, Instant, Instant, Instant),
) -> Option<Sender> {
    let (sender, outcome, due, sent, done) = result;
    report.schedule_lag.push(sent - due);
    match outcome {
        SendResult::Committed => report.committed.push((sent, done - sent, done - due)),
        SendResult::Rejected => report.rejected += 1,
        SendResult::TimedOut => report.timed_out += 1,
        SendResult::Other => report.other += 1,
    }
    matches!(outcome, SendResult::Committed | SendResult::Rejected).then_some(sender)
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn percentiles(mut samples: Vec<Duration>) -> String {
    if samples.is_empty() {
        return "-".into();
    }
    samples.sort_unstable();
    let at = |p| ms(samples[(samples.len() * p as usize).div_ceil(100) - 1]);
    format!("{:.2}/{:.2}/{:.2}", at(50), at(95), at(99))
}

struct RecoveryClient {
    path: std::path::PathBuf,
    identity: Identity,
    endpoint: String,
    roots: rustls::RootCertStore,
    target: u64,
}

impl RecoveryClient {
    fn new(
        gateway: &RunningGateway,
        path: &std::path::Path,
        identity: Identity,
        target: u64,
    ) -> Self {
        Self {
            path: path.to_path_buf(),
            identity,
            endpoint: gateway.address.clone(),
            roots: gateway.roots.clone(),
            target,
        }
    }

    async fn sync_through(&mut self, expected: u64, wave: Option<Instant>) -> RecoveryResult {
        let mut transport = MeasuredTransport {
            inner: TlsTransport::new(
                self.endpoint.clone(),
                "localhost".try_into().unwrap(),
                self.roots.clone(),
            ),
            stats: RecoveryStats::default(),
        };
        let mut client = Client::open(&self.path, self.identity).await.unwrap();
        let mut observer = SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(&self.path)
                .read_only(true),
        )
        .await
        .unwrap();
        let (stop, mut submissions) = mpsc::channel::<Submission>(1);
        let target = self.target.to_be_bytes().to_vec();
        let start = Instant::now();
        let token = self.identity.device_id;
        let run = client.run(
            &mut transport,
            &token,
            Options {
                sync_interval: Duration::from_secs(60),
                page_items: 8,
                ..Options::default()
            },
            &mut submissions,
        );
        let observe = async move {
            loop {
                let covered: Option<Vec<u8>> =
                    sqlx::query_scalar("SELECT covered FROM streams WHERE target = ?")
                        .bind(&target)
                        .fetch_optional(&mut observer)
                        .await
                        .unwrap();
                if covered.as_deref() == Some(&expected.to_be_bytes()) {
                    let end = Instant::now();
                    drop(stop);
                    return end;
                }
                sleep(Duration::from_millis(5)).await;
            }
        };
        let (result, end) = timeout(RECOVERY_LIMIT, async { tokio::join!(run, observe) })
            .await
            .expect("recovery timed out");
        result.unwrap();
        let messages = client.messages(self.target).await.unwrap();
        let positions: Vec<_> = messages
            .iter()
            .map(|view| view.position.as_ref().unwrap().create_seq.unwrap())
            .collect();
        assert_eq!(positions, (1..=expected).collect::<Vec<_>>());
        RecoveryResult {
            elapsed: end - wave.unwrap_or(start),
            stats: transport.stats,
        }
    }
}

#[derive(Default)]
struct RecoveryStats {
    connected: Vec<Duration>,
    authenticated: Vec<Duration>,
    connect_failures: usize,
    auth_rejections: usize,
    sync_rejections: usize,
}

struct RecoveryResult {
    elapsed: Duration,
    stats: RecoveryStats,
}

#[derive(Default)]
struct RecoveryReport {
    recovered: Vec<Duration>,
    stats: RecoveryStats,
}

impl RecoveryReport {
    fn add(&mut self, mut result: RecoveryResult) {
        self.recovered.push(result.elapsed);
        self.stats.connected.append(&mut result.stats.connected);
        self.stats
            .authenticated
            .append(&mut result.stats.authenticated);
        self.stats.connect_failures += result.stats.connect_failures;
        self.stats.auth_rejections += result.stats.auth_rejections;
        self.stats.sync_rejections += result.stats.sync_rejections;
    }

    fn print(self, label: &str) {
        println!("case={label} recovered={} connected={} connect_failures={} authenticated={} auth_rejections={} sync_rejections={} connect_ms={} auth_ms={} catchup_ms={} catchup_max_ms={:.2}",
            self.recovered.len(), self.stats.connected.len(), self.stats.connect_failures,
            self.stats.authenticated.len(), self.stats.auth_rejections, self.stats.sync_rejections,
            percentiles(self.stats.connected), percentiles(self.stats.authenticated),
            percentiles(self.recovered.clone()), ms(self.recovered.into_iter().max().unwrap()));
    }
}

struct MeasuredTransport {
    inner: TlsTransport,
    stats: RecoveryStats,
}

impl Transport for MeasuredTransport {
    async fn reconnect(&mut self) -> std::io::Result<()> {
        let start = Instant::now();
        let result = self.inner.reconnect().await;
        match result {
            Ok(()) => self.stats.connected.push(start.elapsed()),
            Err(_) => self.stats.connect_failures += 1,
        }
        result
    }

    async fn exchange(&mut self, request: Vec<u8>) -> std::io::Result<Vec<u8>> {
        let kind = MsgType::from_u8(request[1]);
        let start = Instant::now();
        let bytes = self.inner.exchange(request).await?;
        let elapsed = start.elapsed();
        match (kind, decode(&bytes)) {
            (Some(MsgType::Auth), DecodedBody::AuthAck(ack))
                if matches!(ack.value, Some(auth_response::Value::Session(_))) =>
            {
                self.stats.authenticated.push(elapsed);
            }
            (Some(MsgType::Auth), DecodedBody::AuthAck(_) | DecodedBody::Error(_)) => {
                self.stats.auth_rejections += 1;
            }
            (Some(MsgType::Sync | MsgType::Snapshot | MsgType::History), DecodedBody::Error(_)) => {
                self.stats.sync_rejections += 1;
            }
            _ => {}
        }
        Ok(bytes)
    }

    async fn receive(&mut self) -> std::io::Result<Vec<u8>> {
        self.inner.receive().await
    }
}

async fn open_raw_reader(
    gateway: &RunningGateway,
    credential: DeviceCredential,
    limited: bool,
) -> TlsStream<TcpStream> {
    let socket = TcpSocket::new_v4().unwrap();
    if limited {
        socket.set_recv_buffer_size(4096).unwrap();
    }
    let socket = socket
        .connect(gateway.address.parse().unwrap())
        .await
        .unwrap();
    let config = rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_root_certificates(gateway.roots.clone())
        .with_no_client_auth();
    let mut stream = TlsConnector::from(Arc::new(config))
        .connect("localhost".try_into().unwrap(), socket)
        .await
        .unwrap();
    let auth = AuthRequest {
        device_id: Some(credential.device_id.to_vec().into()),
        access_token: Some(credential.device_id.to_vec().into()),
        requested_capabilities: vec![],
    };
    let bytes = encode_frame(MsgType::Auth, &Head { req_id: Some(1) }, &auth).unwrap();
    stream.write_all(&bytes).await.unwrap();
    stream.flush().await.unwrap();
    let reply = timeout(WAIT, raw_frame(&mut stream))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(decode(&reply), DecodedBody::AuthAck(ack)
        if matches!(ack.value, Some(auth_response::Value::Session(_)))));
    stream
}

#[derive(Debug, Default)]
struct PushResult {
    count: usize,
    closed: bool,
}

async fn drain_pushes(mut stream: TlsStream<TcpStream>) -> PushResult {
    let mut result = PushResult::default();
    loop {
        match timeout(Duration::from_secs(2), raw_frame(&mut stream)).await {
            Ok(Ok(bytes)) => {
                assert!(matches!(decode(&bytes), DecodedBody::Push(_)));
                result.count += 1;
            }
            Ok(Err(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) =>
            {
                result.closed = true;
                return result;
            }
            Ok(Err(error)) => panic!("failed to read push: {error}"),
            Err(_) => return result,
        }
    }
}

async fn raw_frame(stream: &mut TlsStream<TcpStream>) -> std::io::Result<Vec<u8>> {
    let mut header = [0; HEADER_LEN];
    stream.read_exact(&mut header).await?;
    let header = FixedHeader::decode(&header).map_err(std::io::Error::other)?;
    let mut bytes = Vec::with_capacity(header.total_len());
    header.encode(&mut bytes);
    bytes.resize(header.total_len(), 0);
    stream.read_exact(&mut bytes[HEADER_LEN..]).await?;
    Ok(bytes)
}

async fn remove_database(fixture: &Fixture) {
    let pool = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let name = fixture.database.path().trim_start_matches('/');
    assert!(
        name.starts_with("iam_test_")
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    );
    sqlx::query(&format!("DROP DATABASE `{name}`"))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}
