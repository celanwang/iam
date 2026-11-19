use iam_core::{DeviceCredential, Engine};
use iam_gateway::{Gateway, GatewayTls, Limits};
use iam_protocol::parse_id128_bytes;
use iam_store::{OpenMode, TidbService};
use std::{error::Error, io, net::SocketAddr};
use tokio::net::{lookup_host, TcpSocket};
use tokio_rustls::rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let files = ConfigFiles {
        certificates: required("IAM_TLS_CERT")?,
        key: required("IAM_TLS_KEY")?,
        credentials: required("IAM_DEVICE_CREDENTIALS")?,
    };
    let config = files.read()?;
    let tls = GatewayTls::new(config.certificates, config.key)
        .map_err(|error| format!("invalid IAM_TLS_CERT or IAM_TLS_KEY: {error}"))?;
    let mut engine = Engine::new(0);
    engine
        .store
        .configure_device_credentials(config.credentials)?;
    let mode = match required("IAM_TIDB_MODE")?.as_str() {
        "fresh" => OpenMode::Initialize,
        "resume" => OpenMode::Resume,
        _ => return Err("IAM_TIDB_MODE must be fresh or resume".into()),
    };
    let url = required("IAM_TIDB_URL")?;
    let store_id = parse_id(&required("IAM_STORE_ID")?)?;
    let socket = bind_socket(&required("IAM_GATEWAY_BIND")?)
        .await
        .map_err(|error| format!("cannot bind IAM_GATEWAY_BIND: {error}"))?;
    let service = TidbService::connect_local(&url, store_id, engine, mode).await?;
    let gateway = Gateway::with_tls(service, tls, Limits::default())?;
    let listener = socket.listen(1024)?;
    let shutdown = shutdown_signal()?;
    let address = listener.local_addr()?;
    #[cfg(unix)]
    let reload = {
        let mut hangup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
        let gateway = gateway.clone();
        tokio::spawn(async move {
            while hangup.recv().await.is_some() {
                let files = files.clone();
                match tokio::task::spawn_blocking(move || files.read()).await {
                    Ok(Ok(config)) => {
                        match gateway.reload(config.certificates, config.key, config.credentials) {
                            Ok(()) => eprintln!("IAM gateway configuration reloaded"),
                            Err(error) => eprintln!("IAM gateway reload rejected: {error}"),
                        }
                    }
                    Ok(Err(error)) => eprintln!("IAM gateway reload rejected: {error}"),
                    Err(_) => eprintln!("IAM gateway reload task failed"),
                }
            }
        })
    };
    println!("IAM gateway listening on {address}");
    let result = gateway.serve(listener, shutdown).await;
    #[cfg(unix)]
    reload.abort();
    result?;
    Ok(())
}

async fn bind_socket(bind: &str) -> io::Result<TcpSocket> {
    let mut error = io::Error::new(io::ErrorKind::InvalidInput, "no addresses to bind");
    for address in lookup_host(bind).await? {
        match bind_address(address) {
            Ok(socket) => return Ok(socket),
            Err(cause) => error = cause,
        }
    }
    Err(error)
}

fn bind_address(address: SocketAddr) -> io::Result<TcpSocket> {
    let socket = if address.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    #[cfg(unix)]
    socket.set_reuseaddr(true)?;
    socket.bind(address)?;
    Ok(socket)
}

#[derive(Clone)]
struct ConfigFiles {
    certificates: String,
    key: String,
    credentials: String,
}

struct Config {
    certificates: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    credentials: Vec<DeviceCredential>,
}

impl ConfigFiles {
    fn read(&self) -> Result<Config, String> {
        let certificates = CertificateDer::pem_file_iter(&self.certificates)
            .and_then(|entries| entries.collect())
            .map_err(|_| "cannot read IAM_TLS_CERT")?;
        let key = PrivateKeyDer::from_pem_file(&self.key).map_err(|_| "cannot read IAM_TLS_KEY")?;
        let credentials = std::fs::read_to_string(&self.credentials)
            .map_err(|_| "cannot read IAM_DEVICE_CREDENTIALS")?;
        Ok(Config {
            certificates,
            key,
            credentials: parse_credentials(&credentials)?,
        })
    }
}

#[cfg(unix)]
fn shutdown_signal() -> io::Result<impl std::future::Future<Output = ()>> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    Ok(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = term.recv() => {},
        }
    })
}

#[cfg(not(unix))]
fn shutdown_signal() -> io::Result<impl std::future::Future<Output = ()>> {
    Ok(async {
        let _ = tokio::signal::ctrl_c().await;
    })
}

fn required(name: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(name).map_err(|_| format!("{name} must be set and valid UTF-8").into())
}

fn parse_id(value: &str) -> Result<iam_protocol::Id128, Box<dyn Error>> {
    Ok(parse_id128_bytes(&hex::decode(value.replace('-', ""))?)?)
}

fn parse_credentials(contents: &str) -> Result<Vec<DeviceCredential>, String> {
    contents
        .lines()
        .enumerate()
        .filter_map(|(line, text)| {
            let text = text.trim();
            if text.is_empty() || text.starts_with('#') {
                return None;
            }
            Some(
                parse_credential(text)
                    .map_err(|_| format!("invalid device credential on line {}", line + 1)),
            )
        })
        .collect()
}

fn parse_credential(text: &str) -> Result<DeviceCredential, Box<dyn Error>> {
    let fields: Vec<_> = text.split_whitespace().collect();
    let [tenant, user, device, expiry, digest] = fields.as_slice() else {
        return Err("expected tenant, user, device, expiry, token digest".into());
    };
    let token_sha256 = hex::decode(digest)?
        .try_into()
        .map_err(|_| "invalid token digest")?;
    Ok(DeviceCredential {
        tenant_id: tenant.parse()?,
        user_id: user.parse()?,
        device_id: parse_id(device)?,
        expires_at_ms: expiry.parse()?,
        token_sha256,
    })
}
