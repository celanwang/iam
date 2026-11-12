use std::{
    io,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::{JoinHandle, JoinSet},
};
use url::Url;

#[derive(Clone, Copy, Debug)]
pub enum CommitPhase {
    Before,
    After,
}

struct Gate {
    phase: CommitPhase,
    reached: oneshot::Sender<io::Result<()>>,
    release: oneshot::Receiver<()>,
}

pub struct CommitProxy {
    pub url: Url,
    gate: Arc<Mutex<Option<Gate>>>,
    task: JoinHandle<()>,
}

impl CommitProxy {
    pub async fn start(database: &Url) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let upstream = (
            database
                .host_str()
                .unwrap()
                .trim_matches(['[', ']'])
                .to_owned(),
            database.port().unwrap_or(3306),
        );
        let mut url = database.clone();
        url.set_host(Some("127.0.0.1")).unwrap();
        url.set_port(Some(listener.local_addr().unwrap().port()))
            .unwrap();
        // The test proxy observes protocol packets on loopback only.
        url.query_pairs_mut().append_pair("ssl-mode", "DISABLED");

        let gate = Arc::new(Mutex::new(None));
        let task = tokio::spawn(serve(listener, upstream, Arc::clone(&gate)));
        Self { url, gate, task }
    }

    pub fn arm(
        &self,
        phase: CommitPhase,
    ) -> (oneshot::Receiver<io::Result<()>>, oneshot::Sender<()>) {
        let (reached, wait_for_commit) = oneshot::channel();
        let (drop_connection, release) = oneshot::channel();
        let mut gate = self.gate.lock().unwrap();
        assert!(gate.is_none());
        *gate = Some(Gate {
            phase,
            reached,
            release,
        });
        (wait_for_commit, drop_connection)
    }
}

impl Drop for CommitProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(listener: TcpListener, upstream: (String, u16), gate: Arc<Mutex<Option<Gate>>>) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (client, _) = accepted.expect("accept loopback test connection");
                connections.spawn(bridge(client, upstream.clone(), Arc::clone(&gate)));
            }
            finished = connections.join_next(), if !connections.is_empty() => {
                match finished.unwrap().expect("test proxy connection task") {
                    Ok(()) => {}
                    Err(error) if matches!(
                        error.kind(),
                        io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
                    ) => {}
                    Err(error) => panic!("test proxy connection failed: {error}"),
                }
            }
        }
    }
}

async fn bridge(
    mut client: TcpStream,
    upstream: (String, u16),
    gate: Arc<Mutex<Option<Gate>>>,
) -> io::Result<()> {
    let mut server = TcpStream::connect((upstream.0.as_str(), upstream.1)).await?;
    let (mut requests, mut replies) = client.split();
    let (mut server_replies, mut server_requests) = server.split();

    let (commit, gate) = tokio::select! {
        request = forward_requests(&mut requests, &mut server_requests, &gate) => request?,
        result = tokio::io::copy(&mut server_replies, &mut replies) => {
            result?;
            return Ok(());
        }
    };

    // The forwarder paused before sending COMMIT. For the after case, wait for
    // TiDB's actual OK without forwarding it to the client.
    let result = match gate.phase {
        CommitPhase::Before => Ok(()),
        CommitPhase::After => {
            confirm_commit(&commit, &mut server_requests, &mut server_replies).await
        }
    };
    let ready = result.is_ok();
    let _ = gate.reached.send(result);
    if ready {
        let _ = gate.release.await;
    }
    Ok(())
}

async fn forward_requests(
    client: &mut (impl AsyncRead + Unpin),
    server: &mut (impl AsyncWrite + Unpin),
    gate: &Mutex<Option<Gate>>,
) -> io::Result<(Packet, Gate)> {
    loop {
        let packet = Packet::read(client).await?;
        // SQLx sends the top-level COMMIT as a sequence-zero COM_QUERY.
        if packet.header[3] == 0 && packet.payload == b"\x03COMMIT" {
            if let Some(gate) = gate.lock().unwrap().take() {
                return Ok((packet, gate));
            }
        }
        packet.write(server).await?;
    }
}

async fn confirm_commit(
    commit: &Packet,
    server: &mut (impl AsyncWrite + Unpin),
    replies: &mut (impl AsyncRead + Unpin),
) -> io::Result<()> {
    commit.write(server).await?;
    let response = Packet::read(replies).await?;
    if response.header[3] != 1 || response.payload.len() < 7 || response.payload[0] != 0 {
        return Err(io::Error::other("TiDB did not return OK for COMMIT"));
    }
    Ok(())
}

struct Packet {
    header: [u8; 4],
    payload: Vec<u8>,
}

impl Packet {
    async fn read(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Self> {
        let mut header = [0; 4];
        reader.read_exact(&mut header).await?;
        let size = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        let mut payload = vec![0; size];
        reader.read_exact(&mut payload).await?;
        Ok(Self { header, payload })
    }

    async fn write(&self, writer: &mut (impl AsyncWrite + Unpin)) -> io::Result<()> {
        writer.write_all(&self.header).await?;
        writer.write_all(&self.payload).await
    }
}
