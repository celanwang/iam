use crate::{wire, Transport};
use iam_protocol::{DecodedBody, FixedHeader, HEADER_LEN};
use std::{io, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, WriteHalf},
    net::TcpStream,
    sync::mpsc,
    task::JoinHandle,
};
use tokio_rustls::{client::TlsStream, rustls, TlsConnector};

/// A direct TCP connection with TLS certificate and server name verification.
pub struct TlsTransport {
    endpoint: String,
    name: rustls::pki_types::ServerName<'static>,
    connector: TlsConnector,
    connection: Option<Connection>,
}

struct Connection {
    writer: WriteHalf<TlsStream<TcpStream>>,
    reader: JoinHandle<()>,
    replies: mpsc::Receiver<io::Result<Vec<u8>>>,
    pushes: mpsc::Receiver<Vec<u8>>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

impl TlsTransport {
    pub fn new(
        endpoint: impl Into<String>,
        name: rustls::pki_types::ServerName<'static>,
        roots: rustls::RootCertStore,
    ) -> Self {
        let tls = rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots)
            .with_no_client_auth();
        Self {
            endpoint: endpoint.into(),
            name,
            connector: TlsConnector::from(Arc::new(tls)),
            connection: None,
        }
    }
}

impl Transport for TlsTransport {
    async fn reconnect(&mut self) -> io::Result<()> {
        self.connection = None;
        let socket = TcpStream::connect(&self.endpoint).await?;
        socket.set_nodelay(true)?;
        let stream = self.connector.connect(self.name.clone(), socket).await?;
        let (reader, writer) = tokio::io::split(stream);
        let (replies, reply_rx) = mpsc::channel(1);
        let (pushes, push_rx) = mpsc::channel(8);
        let reader = tokio::spawn(read_frames(reader, replies, pushes));
        self.connection = Some(Connection {
            writer,
            reader,
            replies: reply_rx,
            pushes: push_rx,
        });
        Ok(())
    }

    async fn exchange(&mut self, request: Vec<u8>) -> io::Result<Vec<u8>> {
        let conn = self.connection.as_mut().ok_or_else(disconnected)?;
        conn.writer.write_all(&request).await?;
        conn.writer.flush().await?;
        conn.replies.recv().await.ok_or_else(disconnected)?
    }

    async fn receive(&mut self) -> io::Result<Vec<u8>> {
        let conn = self.connection.as_mut().ok_or_else(disconnected)?;
        tokio::select! {
            biased;
            reply = conn.replies.recv() => reply.ok_or_else(disconnected)?,
            Some(push) = conn.pushes.recv() => Ok(push),
        }
    }
}

async fn read_frames(
    mut reader: impl AsyncRead + Unpin,
    replies: mpsc::Sender<io::Result<Vec<u8>>>,
    pushes: mpsc::Sender<Vec<u8>>,
) {
    loop {
        match read_frame(&mut reader).await {
            Ok((bytes, true)) => {
                let _ = pushes.try_send(bytes);
            }
            Ok((bytes, false)) => {
                if replies.send(Ok(bytes)).await.is_err() {
                    return;
                }
            }
            Err(error) => {
                let _ = replies.send(Err(error)).await;
                return;
            }
        }
    }
}

async fn read_frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<(Vec<u8>, bool)> {
    let mut raw = [0; HEADER_LEN];
    reader.read_exact(&mut raw).await?;
    let header = FixedHeader::decode(&raw).map_err(invalid)?;
    let mut bytes = vec![0; header.total_len()];
    bytes[..HEADER_LEN].copy_from_slice(&raw);
    reader.read_exact(&mut bytes[HEADER_LEN..]).await?;
    let frame = wire::decode(&bytes).map_err(invalid)?;
    Ok((bytes, matches!(frame.body, DecodedBody::Push(_))))
}

fn invalid(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn disconnected() -> io::Error {
    io::Error::from(io::ErrorKind::NotConnected)
}
