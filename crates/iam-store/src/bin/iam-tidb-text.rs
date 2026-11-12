//! Local console for the TiDB-backed protocol service.
use iam_core::Engine;
use iam_protocol::pb::{
    auth_response, message_content, send_outcome, AuthRequest, ErrorCode, Head, MessageContent,
    MessageKey, SendOutcome, SendRequest, SendStatusRequest, TextContent,
};
use iam_protocol::{
    decode_frame, encode_frame, generate_id128, validate_id128, DecodedBody, FrameDecoder, Id128,
    MsgType,
};
use iam_store::{OpenMode, TidbService};
use prost::Message;
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
    let store_id = id(&required_env("IAM_STORE_ID")?)?;
    let mode = required_env("IAM_TIDB_MODE")?;
    if mode == "upgrade" {
        TidbService::upgrade_local(&url, store_id).await?;
        println!("TiDB storage upgraded");
        return Ok(());
    }
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
    let mode = match mode.as_str() {
        "fresh" => OpenMode::Initialize,
        "resume" => OpenMode::Resume,
        _ => return Err("IAM_TIDB_MODE must be fresh, upgrade or resume".into()),
    };
    let mut service = TidbService::connect_local(&url, store_id, engine, mode).await?;
    println!("TiDB text console; mode={mode:?}");
    println!(
        "commands: auth TOKEN DEVICE_UUID | init | id | send UUID TEXT | status UUID | page [BEFORE] [LIMIT] | stop | quit"
    );
    let mut connection = None;
    let mut request_id = 0;
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
                    let request = AuthRequest {
                        access_token: Some(token.as_bytes().to_vec().into()),
                        device_id: Some(device.to_vec().into()),
                        requested_capabilities: vec![],
                    };
                    service.disconnect(connection.take());
                    request_id = 0;
                    let body = exchange(
                        &mut service,
                        &mut connection,
                        &mut request_id,
                        MsgType::Auth,
                        &request,
                    )
                    .await?;
                    if !matches!(body, DecodedBody::AuthAck(response)
                        if matches!(response.value, Some(auth_response::Value::Session(_))))
                    {
                        return Err("authentication failed".into());
                    }
                    println!("authenticated");
                }
                "init" => {
                    let initialized = service
                        .conversation(connection.ok_or("authenticate first")?, conversation)
                        .await?;
                    println!(
                        "history={} next={}",
                        hex::encode(initialized.history_id),
                        initialized.next_sequence,
                    );
                }
                "id" => println!("{}", hex::encode(generate_id128())),
                "send" => {
                    let (client_id, text) = arguments
                        .split_once(' ')
                        .ok_or("send requires UUID and text")?;
                    connection.ok_or("authenticate first")?;
                    let request = SendRequest {
                        key: Some(message_key(conversation, id(client_id)?)),
                        content: Some(MessageContent {
                            value: Some(message_content::Value::Text(TextContent {
                                text: Some(text.to_owned()),
                            })),
                        }),
                    };
                    let body = exchange(
                        &mut service,
                        &mut connection,
                        &mut request_id,
                        MsgType::Send,
                        &request,
                    )
                    .await?;
                    let DecodedBody::SendAck(response) = body else {
                        return Err("expected SEND_ACK".into());
                    };
                    print_outcome(response.outcome, "STORED")?;
                }
                "status" => {
                    connection.ok_or("authenticate first")?;
                    let request = SendStatusRequest {
                        key: Some(message_key(conversation, id(arguments)?)),
                    };
                    let body = exchange(
                        &mut service,
                        &mut connection,
                        &mut request_id,
                        MsgType::SendStatus,
                        &request,
                    )
                    .await?;
                    let DecodedBody::SendStatusAck(response) = body else {
                        return Err("expected SEND_STATUS_ACK".into());
                    };
                    print_outcome(response.outcome, "FOUND")?;
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

fn message_key(conversation: u64, client_id: Id128) -> MessageKey {
    MessageKey {
        conversation_id: Some(conversation),
        client_msg_id: Some(client_id.to_vec().into()),
    }
}

async fn exchange(
    service: &mut TidbService,
    connection: &mut Option<Id128>,
    request_id: &mut u64,
    kind: MsgType,
    body: &impl Message,
) -> Result<DecodedBody, Box<dyn Error>> {
    *request_id = request_id.checked_add(1).ok_or("request ID exhausted")?;
    let bytes = encode_frame(
        kind,
        &Head {
            req_id: Some(*request_id),
        },
        body,
    )?;
    let mut decoder = FrameDecoder::new();
    decoder.push(&bytes);
    let frame = decoder.next_frame()?.ok_or("incomplete request")?;
    let (cid, output) = service.handle_frame(*connection, &frame).await?;
    *connection = cid;
    let [response] = output.responses.as_slice() else {
        return Err("expected one response".into());
    };
    let mut decoder = FrameDecoder::new();
    decoder.push(response);
    let response = decode_frame(&decoder.next_frame()?.ok_or("incomplete response")?)?;
    match response.body {
        DecodedBody::Error(error) => Err(format!("protocol error: {:?}", error.error).into()),
        body => Ok(body),
    }
}

fn print_outcome(outcome: Option<SendOutcome>, success: &str) -> Result<(), Box<dyn Error>> {
    match outcome
        .and_then(|outcome| outcome.value)
        .ok_or("missing send outcome")?
    {
        send_outcome::Value::Committed(receipt) => println!("{success} {receipt:?}"),
        send_outcome::Value::RejectedFinal(error) => println!("REJECTED {:?}", error.code()),
        send_outcome::Value::Unresolved(result) => {
            let code = result.error.ok_or("missing unresolved error")?.code();
            if code == ErrorCode::NotFound {
                println!("UNKNOWN");
            } else {
                println!("UNRESOLVED {code:?}");
            }
        }
    }
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
