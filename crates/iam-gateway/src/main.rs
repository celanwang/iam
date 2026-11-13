use iam_core::{DeviceCredential, Engine};
use iam_gateway::{Gateway, Limits};
use iam_protocol::parse_id128_bytes;
use iam_store::{OpenMode, TidbService};
use std::{error::Error, io};
use tokio::net::TcpListener;
use tokio_rustls::rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut engine = Engine::new(0);
    let credentials = std::fs::read_to_string(required("IAM_DEVICE_CREDENTIALS")?)?;
    engine
        .store
        .configure_device_credentials(parse_credentials(&credentials)?)?;
    let mode = match required("IAM_TIDB_MODE")?.as_str() {
        "fresh" => OpenMode::Initialize,
        "resume" => OpenMode::Resume,
        _ => return Err("IAM_TIDB_MODE must be fresh or resume".into()),
    };
    let url = required("IAM_TIDB_URL")?;
    let store_id = parse_id(&required("IAM_STORE_ID")?)?;
    let certificates =
        CertificateDer::pem_file_iter(required("IAM_TLS_CERT")?)?.collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(required("IAM_TLS_KEY")?)?;
    let service = TidbService::connect_local(&url, store_id, engine, mode).await?;
    let gateway = Gateway::new(service, certificates, key, Limits::default())?;
    let listener = TcpListener::bind(required("IAM_GATEWAY_BIND")?).await?;
    println!("IAM gateway listening on {}", listener.local_addr()?);
    let shutdown = shutdown_signal()?;
    gateway.serve(listener, shutdown).await?;
    Ok(())
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

fn parse_credentials(contents: &str) -> Result<Vec<DeviceCredential>, Box<dyn Error>> {
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
                    .map_err(|_| format!("invalid device credential on line {}", line + 1).into()),
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
