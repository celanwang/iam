//! Local native text-store console. This is not a TCP gateway or wire SEND implementation.
use iam_core::Engine;
use iam_protocol::pb::{AuthRequest, Head};
use iam_protocol::{generate_id128, validate_id128, DecodedBody, DecodedFrame, Id128, MsgType};
use iam_store_tidb::{OpenMode, SendLookup, TidbTextService};
use std::error::Error;
use tokio::io::{AsyncBufReadExt, BufReader};

fn required_env(name: &str) -> Result<String, Box<dyn Error>> {
    // VarError::NotUnicode can contain the original value, including credentials.
    std::env::var(name).map_err(|_| format!("{name} must be set and valid UTF-8").into())
}

fn number(name: &str, default: u64) -> Result<u64, Box<dyn Error>> {
    let value = match std::env::var(name) {
        Ok(value) => value.parse()?,
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    if value == 0 {
        return Err(format!("{name} must be nonzero").into());
    }
    Ok(value)
}

fn id(value: &str) -> Result<Id128, Box<dyn Error>> {
    Ok(validate_id128(&hex::decode(value.replace('-', ""))?)?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = required_env("IAM_TIDB_URL")?;
    let token = required_env("IAM_ACCESS_TOKEN")?;
    if token.is_empty() {
        return Err("IAM_ACCESS_TOKEN must be nonempty".into());
    }
    // These are server-side seed settings, not values accepted from text commands.
    let tenant = number("IAM_TENANT_ID", 1)?;
    let user = number("IAM_USER_ID", 100)?;
    let conversation = number("IAM_CONVERSATION_ID", 1)?;
    let mut engine = Engine::new(0);
    engine.store.seed_user(tenant, user, token.into_bytes());
    engine
        .store
        .seed_direct(tenant, conversation, user, if user == 1 { 2 } else { 1 });
    let mode = match std::env::var("IAM_TIDB_MODE").as_deref() {
        Ok("fresh") => OpenMode::FreshDatabase,
        Ok("read-only") | Err(std::env::VarError::NotPresent) => OpenMode::ReadOnly,
        _ => return Err("IAM_TIDB_MODE must be fresh or read-only".into()),
    };
    let mut service = TidbTextService::connect_local(&url, engine, mode).await?;
    println!("native text storage; mode={mode:?}; not wire committed/SYNC");
    println!(
        "commands: auth TOKEN DEVICE_UUID | init | id | send UUID TEXT | status UUID | page [BEFORE] [LIMIT] | stop | quit"
    );
    let mut connection = None;
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        // Do not trim the line: trailing/leading text bytes participate in retry comparison.
        let (command, arguments) = line.split_once(' ').unwrap_or((line.as_str(), ""));
        // A failed command does not terminate the console or imply a logical final rejection.
        let result: Result<(), Box<dyn Error>> = async {
            match command {
                "auth" => {
                    let mut parts = arguments.split_whitespace();
                    let token = parts.next().ok_or("auth requires token and device UUID")?;
                    let device = id(parts.next().ok_or("auth requires device UUID")?)?;
                    if parts.next().is_some() {
                        return Err("unexpected auth arguments".into());
                    }
                    let request = DecodedFrame {
                        msg_type: MsgType::Auth,
                        head: Head {
                            req_id: Some(1),
                        },
                        body: DecodedBody::Auth(AuthRequest {
                            access_token: Some(token.as_bytes().to_vec().into()),
                            device_id: Some(device.to_vec().into()),
                            requested_capabilities: vec![],
                        }),
                    };
                    service.disconnect(connection.take());
                    let (authenticated, _) = service.handle_session(None, &request).await?;
                    connection = Some(authenticated.ok_or("authentication failed")?);
                    println!("authenticated");
                }
                "init" => {
                    let initialized = service
                        .initialize_conversation(connection.ok_or("authenticate first")?, conversation)
                        .await?;
                    println!(
                        "history={} next={} created={}",
                        hex::encode(initialized.history_id),
                        initialized.next_sequence,
                        initialized.created
                    );
                }
                "id" => println!("{}", hex::encode(generate_id128())),
                "send" => {
                    let (client_id, text) = arguments
                        .split_once(' ')
                        .ok_or("send requires UUID and text")?;
                    let saved = service
                        .send_text(
                            connection.ok_or("authenticate first")?,
                            conversation,
                            id(client_id)?,
                            text,
                        )
                        .await?;
                    println!(
                        "{} {:?}",
                        if saved.replayed { "REPLAYED" } else { "STORED" },
                        saved.receipt
                    );
                }
                "status" => {
                    match service
                        .query_send(
                            connection.ok_or("authenticate first")?,
                            conversation,
                            id(arguments)?,
                        )
                        .await?
                    {
                        SendLookup::Found(receipt) => println!("FOUND {receipt:?}"),
                        SendLookup::Unknown => {
                            println!("UNKNOWN (not a final rejection or permission to resend)");
                        }
                    }
                }
                "page" => {
                    let mut parts = arguments.split_whitespace();
                    let before = parts.next().map(str::parse).transpose()?;
                    let limit = parts.next().map(str::parse).transpose()?.unwrap_or(20);
                    if parts.next().is_some() {
                        return Err("unexpected page arguments".into());
                    }
                    let page = service
                        .read_messages(
                            connection.ok_or("authenticate first")?,
                            conversation,
                            before,
                            limit,
                        )
                        .await?;
                    println!(
                        "history={} next_before={:?}",
                        hex::encode(page.history_id),
                        page.next_before
                    );
                    for message in page.messages {
                        println!("{message:?}");
                    }
                }
                "stop" => {
                    service.stop_accepting();
                    println!("new acceptance disabled");
                }
                "quit" => return Ok(()),
                "" => {}
                _ => return Err("unknown command".into()),
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            eprintln!("attempt error: {error}");
        }
        if command == "quit" {
            break;
        }
    }
    service.disconnect(connection);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn console_accepts_raw_and_formatted_uuid_v7() {
        assert_eq!(
            id("018bcfe5680070008000000000000001").unwrap(),
            id("018bcfe5-6800-7000-8000-000000000001").unwrap(),
        );
    }

    #[test]
    fn console_rejects_other_versions_and_malformed_ids() {
        for value in [
            "018bcfe5680040008000000000000001",
            "018bcfe568007000c000000000000001",
            "018bcfe568007000800000000000000",
            "not-a-message-id",
        ] {
            assert!(id(value).is_err(), "unexpectedly accepted {value}");
        }
    }
}
