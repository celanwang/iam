//! Single-instance TCP/TLS gateway for the TiDB message service.

use iam_core::{DeviceCredential, EngineReply};
use iam_protocol::pb::{ErrorCode, ProtocolError, RetryAction};
use iam_protocol::{
    encode_protocol_error, FixedHeader, Frame, FrameDecoder, Id128, MsgType, ProtocolErrorKind,
    FRAME_HARD_MAX, HEADER_LEN,
};
use iam_store::TidbService;
use prost::Message;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{timeout, timeout_at, Instant};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::{rustls, TlsAcceptor};

const AUTH_TIMEOUT: Duration = Duration::from_secs(10);
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(15);
const RESPONSE_SLOTS: usize = 4;
const PUSH_SLOTS: usize = 16;

#[derive(Clone, Debug)]
pub struct Limits {
    pub connections: usize,
    pub connections_per_ip: usize,
    pub handshakes: usize,
    pub auth_requests: usize,
    pub requests: usize,
    pub recovery_requests: usize,
    pub read_bytes: usize,
    pub push_bytes: usize,
    pub response_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            connections: 1024,
            connections_per_ip: 128,
            handshakes: 64,
            auth_requests: 32,
            requests: 64,
            recovery_requests: 16,
            read_bytes: 16 * 1024 * 1024,
            push_bytes: 8 * 1024 * 1024,
            response_bytes: 16 * 1024 * 1024,
        }
    }
}

pub struct Gateway {
    service: TidbService,
    acceptor: Mutex<TlsAcceptor>,
    limits: Limits,
    peers: Mutex<HashMap<Id128, Arc<Peer>>>,
    sources: Mutex<HashMap<IpAddr, usize>>,
    handshakes: Arc<Semaphore>,
    auth: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    recovery: Arc<Semaphore>,
    reads: Arc<Semaphore>,
    pushes: Arc<Semaphore>,
    responses: Arc<Semaphore>,
}

struct Peer {
    binding: Mutex<Binding>,
    responses: mpsc::Sender<Queued>,
    pushes: mpsc::Sender<Queued>,
    close: watch::Sender<Option<Close>>,
}

#[derive(Default)]
struct Binding {
    cid: Option<Id128>,
    closed: bool,
}

#[derive(Clone)]
enum Close {
    Now,
    Drain,
    Error(Arc<[u8]>),
}

struct Queued {
    bytes: Vec<u8>,
    deadline: Instant,
    _budget: OwnedSemaphorePermit,
}

impl Gateway {
    pub fn new(
        service: TidbService,
        certificates: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        limits: Limits,
    ) -> io::Result<Arc<Self>> {
        if !service.uses_device_credentials() {
            return Err(invalid("gateway requires device credentials"));
        }
        let counts = [
            limits.connections,
            limits.connections_per_ip,
            limits.handshakes,
            limits.auth_requests,
            limits.requests,
            limits.recovery_requests,
            limits.read_bytes,
            limits.push_bytes,
            limits.response_bytes,
        ];
        if counts.iter().any(|&n| n == 0 || n > Semaphore::MAX_PERMITS)
            || limits.read_bytes < FRAME_HARD_MAX as usize
            || limits.response_bytes < FRAME_HARD_MAX as usize
        {
            return Err(invalid("invalid gateway limits"));
        }
        Ok(Arc::new(Self {
            service,
            acceptor: Mutex::new(tls_acceptor(certificates, key)?),
            handshakes: budget(limits.handshakes),
            auth: budget(limits.auth_requests),
            requests: budget(limits.requests),
            recovery: budget(limits.recovery_requests),
            reads: budget(limits.read_bytes),
            pushes: budget(limits.push_bytes),
            responses: budget(limits.response_bytes),
            limits,
            peers: Mutex::new(HashMap::new()),
            sources: Mutex::new(HashMap::new()),
        }))
    }

    /// Apply a complete configuration after validating its certificate and credentials.
    pub fn reload(
        &self,
        certificates: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        credentials: Vec<DeviceCredential>,
    ) -> io::Result<()> {
        let acceptor = tls_acceptor(certificates, key)?;
        // Serialize with session publication so a racing AUTH cannot publish a removed credential.
        let peers = self.peers.lock().unwrap();
        let mut current_acceptor = self.acceptor.lock().unwrap();
        let invalidated = self
            .service
            .reload_device_credentials(credentials)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        for cid in invalidated {
            if let Some(peer) = peers.get(&cid) {
                peer.close(unauthenticated());
            }
        }
        *current_acceptor = acceptor;
        Ok(())
    }

    /// Apply a revocation from the application and close the associated socket.
    pub fn revoke_device(&self, tenant: u64, user: u64, device: Id128) {
        if let Some(cid) = self.service.revoke_device(tenant, user, device) {
            if let Some(peer) = self.peers.lock().unwrap().get(&cid) {
                peer.close(unauthenticated());
            }
        }
    }

    pub async fn serve(
        self: &Arc<Self>,
        listener: TcpListener,
        shutdown: impl Future<Output = ()>,
    ) -> io::Result<()> {
        let (stop, stopped) = watch::channel(false);
        let mut connections = JoinSet::new();
        tokio::pin!(shutdown);
        let result = loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break Ok(()),
                _ = connections.join_next(), if !connections.is_empty() => {},
                accepted = listener.accept() => {
                    let (socket, address) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => break Err(error),
                    };
                    if connections.len() >= self.limits.connections {
                        continue;
                    }
                    let Ok(handshake) = self.handshakes.clone().try_acquire_owned() else {
                        continue;
                    };
                    let Some(source) = self.admit_source(address.ip()) else {
                        continue;
                    };
                    let gateway = self.clone();
                    let stopped = stopped.clone();
                    connections.spawn(async move {
                        let _source = source;
                        gateway.connect(socket, handshake, stopped).await;
                    });
                }
            }
        };
        drop(listener);
        stop.send_replace(true);
        let deadline = Instant::now() + DRAIN_TIMEOUT;
        if timeout_at(deadline, async {
            while connections.join_next().await.is_some() {}
            for (pool, capacity) in [
                (&self.auth, self.limits.auth_requests),
                (&self.requests, self.limits.requests),
                (&self.recovery, self.limits.recovery_requests),
            ] {
                let _ = pool.acquire_many(capacity as u32).await;
            }
        })
        .await
        .is_err()
        {
            connections.shutdown().await;
        }
        result
    }

    fn admit_source(self: &Arc<Self>, ip: IpAddr) -> Option<Source> {
        let mut sources = self.sources.lock().unwrap();
        let count = sources.entry(ip).or_default();
        if *count == self.limits.connections_per_ip {
            return None;
        }
        *count += 1;
        Some(Source {
            gateway: self.clone(),
            ip,
        })
    }

    async fn connect(
        self: Arc<Self>,
        socket: TcpStream,
        handshake: OwnedSemaphorePermit,
        mut stopped: watch::Receiver<bool>,
    ) {
        let _ = socket.set_nodelay(true);
        let acceptor = self.acceptor.lock().unwrap().clone();
        let stream = tokio::select! {
            _ = stopping(&mut stopped) => return,
            stream = timeout(IO_TIMEOUT, acceptor.accept(socket)) => {
                let Ok(Ok(stream)) = stream else { return };
                stream
            }
        };
        let mut stream = stream;
        stream.get_mut().1.set_buffer_limit(Some(32 * 1024));
        drop(handshake);

        let (responses, response_rx) = mpsc::channel(RESPONSE_SLOTS);
        let (pushes, push_rx) = mpsc::channel(PUSH_SLOTS);
        let (close, close_rx) = watch::channel(None);
        let peer = Arc::new(Peer {
            binding: Mutex::default(),
            responses,
            pushes,
            close,
        });
        let _connection = Connection {
            gateway: self.clone(),
            peer: peer.clone(),
        };
        let (read, write) = tokio::io::split(stream);
        let reader = self.read_requests(read, &peer, stopped);
        let writer = write_frames(write, response_rx, push_rx, close_rx);
        tokio::pin!(reader, writer);
        tokio::select! {
            _ = &mut writer => {},
            close = &mut reader => {
                peer.close(close);
                writer.await;
            }
        }
    }

    async fn read_requests(
        self: &Arc<Self>,
        mut read: impl AsyncRead + Unpin,
        peer: &Arc<Peer>,
        mut stopped: watch::Receiver<bool>,
    ) -> Close {
        let mut closed = peer.close.subscribe();
        let mut cid = None;
        let mut expires = None;
        let mut idle = Instant::now() + AUTH_TIMEOUT;
        loop {
            let read_frame = self.read_frame(&mut read, idle);
            let (frame, bytes) = tokio::select! {
                biased;
                _ = closing(&mut closed) => return Close::Now,
                _ = stopping(&mut stopped) => return Close::Drain,
                _ = expiry(expires) => return unauthenticated(),
                frame = read_frame => match frame {
                    Ok(frame) => frame,
                    Err(close) => return close,
                }
            };
            if cid.is_some() {
                idle = Instant::now() + IDLE_TIMEOUT;
            }
            let pool = match MsgType::from_u8(frame.header.msg_type) {
                Some(MsgType::Auth | MsgType::Reconnect) => &self.auth,
                Some(MsgType::Sync | MsgType::Snapshot | MsgType::History) => &self.recovery,
                _ => &self.requests,
            };
            let Ok(inflight) = pool.clone().try_acquire_owned() else {
                let result = self.service.reject_overloaded(cid, &frame);
                if let Err(close) = self.publish(peer, cid, result) {
                    return close;
                }
                continue;
            };
            let gateway = self.clone();
            let target = peer.clone();
            let (done, result) = oneshot::channel();
            tokio::spawn(async move {
                let _inflight = inflight;
                let _bytes = bytes;
                let reply = gateway.service.handle_frame(cid, &frame).await;
                let _ = done.send(gateway.publish(&target, cid, reply));
            });
            let deadline = if cid.is_none() {
                idle
            } else {
                Instant::now() + REQUEST_TIMEOUT
            };
            match tokio::select! {
                biased;
                _ = closing(&mut closed) => return Close::Now,
                _ = expiry(expires) => return unauthenticated(),
                result = timeout_at(deadline, result) => result,
            } {
                Ok(Ok(Ok(Some((connection, expires_at))))) if cid.is_none() => {
                    cid = Some(connection);
                    expires = Some(deadline_for(expires_at));
                    idle = Instant::now() + IDLE_TIMEOUT;
                }
                Ok(Ok(Ok(_))) => {}
                Ok(Ok(Err(close))) => return close,
                _ => return Close::Now,
            }
        }
    }

    async fn read_frame(
        &self,
        read: &mut (impl AsyncRead + Unpin),
        idle: Instant,
    ) -> Result<(Frame, OwnedSemaphorePermit), Close> {
        let mut raw = [0; HEADER_LEN];
        match timeout_at(idle, read.read(&mut raw[..1])).await {
            Ok(Ok(1)) => {}
            _ => return Err(Close::Now),
        }
        let deadline = idle.min(Instant::now() + IO_TIMEOUT);
        read_exact(read, &mut raw[1..], deadline).await?;
        let header = FixedHeader::decode(&raw).map_err(|error| match error {
            ProtocolErrorKind::UnsupportedVersion(_) => {
                protocol_close(ErrorCode::UnsupportedVersion, Some(raw[1]))
            }
            ProtocolErrorKind::UnsupportedFlags(_) => {
                protocol_close(ErrorCode::UnsupportedFlags, Some(raw[1]))
            }
            _ => Close::Now,
        })?;
        let bytes = timeout_at(
            deadline,
            self.reads
                .clone()
                .acquire_many_owned(header.total_len() as u32),
        )
        .await
        .map_err(|_| Close::Now)?
        .map_err(|_| Close::Now)?;
        let mut head = vec![0; header.head_len as usize];
        let mut body = vec![0; header.body_len as usize];
        read_exact(read, &mut head, deadline).await?;
        read_exact(read, &mut body, deadline).await?;
        Ok((Frame { header, head, body }, bytes))
    }

    fn publish(
        &self,
        peer: &Arc<Peer>,
        previous: Option<Id128>,
        reply: iam_store::Result<EngineReply>,
    ) -> Result<Option<(Id128, i64)>, Close> {
        let (cid, output) = reply.map_err(|_| Close::Now)?;
        let mut peers = self.peers.lock().unwrap();
        for (target, bytes) in output.pushes {
            if let Some(recipient) = peers.get(&target) {
                if is_terminal(&bytes) {
                    recipient.close(Close::Error(bytes.into()));
                } else if recipient.close.borrow().is_none() {
                    let _ = queue(&recipient.pushes, &self.pushes, bytes);
                }
            }
        }
        let mut binding = peer.binding.lock().unwrap();
        if binding.closed || peer.close.borrow().is_some() {
            if cid != previous {
                self.service.disconnect(cid);
            }
            return Err(Close::Now);
        }
        let mut new_session = None;
        if let Some(new) = cid.filter(|_| cid != previous) {
            let expires_at = match self.service.gateway_session_expiry(new) {
                Ok(expires_at) => expires_at,
                Err(code) => {
                    self.service.disconnect(Some(new));
                    return Err(if code == ErrorCode::SessionReplaced {
                        protocol_close(code, None)
                    } else {
                        unauthenticated()
                    });
                }
            };
            binding.cid = Some(new);
            new_session = Some((new, expires_at));
        }
        for bytes in output.responses {
            if is_terminal(&bytes) {
                return Err(Close::Error(bytes.into()));
            }
            queue(&peer.responses, &self.responses, bytes).map_err(|_| Close::Now)?;
        }
        if let Some((cid, _)) = new_session {
            peers.insert(cid, peer.clone());
        }
        Ok(new_session)
    }
}

impl Peer {
    fn close(&self, reason: Close) {
        self.close.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(reason);
            true
        });
    }
}

struct Connection {
    gateway: Arc<Gateway>,
    peer: Arc<Peer>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let mut peers = self.gateway.peers.lock().unwrap();
        let mut binding = self.peer.binding.lock().unwrap();
        binding.closed = true;
        if let Some(cid) = binding.cid.take() {
            peers.remove(&cid);
            self.gateway.service.disconnect(Some(cid));
        }
    }
}

struct Source {
    gateway: Arc<Gateway>,
    ip: IpAddr,
}

impl Drop for Source {
    fn drop(&mut self) {
        let mut sources = self.gateway.sources.lock().unwrap();
        if let Some(count) = sources.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                sources.remove(&self.ip);
            }
        }
    }
}

fn budget(size: usize) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(size))
}

fn queue(sender: &mpsc::Sender<Queued>, budget: &Arc<Semaphore>, bytes: Vec<u8>) -> Result<(), ()> {
    let permit = budget
        .clone()
        .try_acquire_many_owned(bytes.len() as u32)
        .map_err(|_| ())?;
    sender
        .try_send(Queued {
            bytes,
            deadline: Instant::now() + IO_TIMEOUT,
            _budget: permit,
        })
        .map_err(|_| ())
}

async fn write_frames(
    mut write: impl AsyncWrite + Unpin,
    mut responses: mpsc::Receiver<Queued>,
    mut pushes: mpsc::Receiver<Queued>,
    mut closed: watch::Receiver<Option<Close>>,
) {
    loop {
        tokio::select! {
            biased;
            close = closing(&mut closed) => {
                match close {
                    Close::Now => return,
                    Close::Error(bytes) => {
                        let _ = write_frame(&mut write, &bytes, Instant::now() + IO_TIMEOUT).await;
                    }
                    Close::Drain => {
                        while let Ok(frame) = responses.try_recv() {
                            if write_frame(&mut write, &frame.bytes, frame.deadline).await.is_err() {
                                return;
                            }
                        }
                    }
                }
                let _ = timeout(IO_TIMEOUT, write.shutdown()).await;
                return;
            }
            Some(frame) = responses.recv() => {
                if write_frame(&mut write, &frame.bytes, frame.deadline).await.is_err() {
                    return;
                }
            }
            Some(frame) = pushes.recv() => {
                if frame.deadline > Instant::now()
                    && write_frame(&mut write, &frame.bytes, frame.deadline).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn write_frame(
    write: &mut (impl AsyncWrite + Unpin),
    bytes: &[u8],
    deadline: Instant,
) -> io::Result<()> {
    timeout_at(deadline, async {
        write.write_all(bytes).await?;
        write.flush().await
    })
    .await?
}

async fn read_exact(
    read: &mut (impl AsyncRead + Unpin),
    bytes: &mut [u8],
    deadline: Instant,
) -> Result<(), Close> {
    timeout_at(deadline, read.read_exact(bytes))
        .await
        .map_err(|_| Close::Now)?
        .map(|_| ())
        .map_err(|_| Close::Now)
}

async fn stopping(stopped: &mut watch::Receiver<bool>) {
    let _ = stopped.wait_for(|stop| *stop).await;
}

async fn closing(closed: &mut watch::Receiver<Option<Close>>) -> Close {
    closed
        .wait_for(Option::is_some)
        .await
        .ok()
        .and_then(|close| close.clone())
        .unwrap_or(Close::Now)
}

fn is_terminal(bytes: &[u8]) -> bool {
    if bytes.get(1) != Some(&(MsgType::Error as u8)) {
        return false;
    }
    let mut decoder = FrameDecoder::new();
    decoder.push(bytes);
    decoder
        .next_frame()
        .ok()
        .flatten()
        .and_then(|frame| ProtocolError::decode(frame.body.as_slice()).ok())
        .is_some_and(|error| error.close_connection == Some(true))
}

fn protocol_close(code: ErrorCode, kind: Option<u8>) -> Close {
    error_close(code, RetryAction::None, kind)
}

fn unauthenticated() -> Close {
    error_close(
        ErrorCode::Unauthenticated,
        RetryAction::Reauthenticate,
        None,
    )
}

fn error_close(code: ErrorCode, action: RetryAction, kind: Option<u8>) -> Close {
    encode_protocol_error(None, kind.map(u32::from), code, action, true, None)
        .map_or(Close::Now, |bytes| Close::Error(bytes.into()))
}

fn deadline_for(expires_at: i64) -> Instant {
    let remaining = (UNIX_EPOCH + Duration::from_millis(expires_at as u64))
        .duration_since(SystemTime::now())
        .unwrap_or_default();
    Instant::now() + remaining
}

async fn expiry(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn tls_acceptor(
    certificates: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> io::Result<TlsAcceptor> {
    let tls = rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(io::Error::other)?;
    Ok(TlsAcceptor::from(Arc::new(tls)))
}
