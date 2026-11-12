use crate::protocol_store::{unresolved, Decision, ProtocolStore, SendKey};
use crate::{Error, Identity, Result};
use iam_core::{CoreError, Engine, EngineOutput, EngineReply, StorageRequest};
use iam_protocol::pb::{
    change, message_content, send_outcome, Change, ErrorCode, MessageContent, MessageKey,
    RetryAction, SendOutcome, SendResponse, SendStatusResponse, SnapshotResponse, SyncResponse,
};
use iam_protocol::{
    decode_frame, encode_frame, encode_protocol_error, validate_id128, DecodedBody, DecodedFrame,
    Frame, Id128, MsgType,
};
use prost::Message;

#[derive(Clone, Copy, Debug)]
pub enum ProtocolOpenMode {
    /// Creates a fresh authority using the Engine's seeded conversations.
    Initialize,
    /// The caller has verified this is the same continuously authoritative database.
    Resume,
}

/// Wire handler for authenticated sessions backed by TiDB.
pub struct TidbProtocolService {
    engine: Engine,
    database: ProtocolStore,
}

impl TidbProtocolService {
    pub async fn connect_local(
        url: &str,
        store_id: Id128,
        engine: Engine,
        mode: ProtocolOpenMode,
    ) -> Result<Self> {
        if !engine.store.connections.is_empty() || engine.store.recovery_incomplete {
            return Err(Error::Configuration(
                "expected a fresh engine for the TiDB protocol",
            ));
        }
        let seeds = matches!(mode, ProtocolOpenMode::Initialize).then_some(&engine.store);
        let database = ProtocolStore::open(url, store_id, seeds).await?;
        Ok(Self { engine, database })
    }

    pub fn disconnect(&mut self, connection_id: Option<Id128>) {
        self.engine.disconnect(connection_id);
    }

    pub async fn handle_frame(
        &mut self,
        connection_id: Option<Id128>,
        frame: &Frame,
    ) -> Result<EngineReply> {
        let Ok(decoded) = decode_frame(frame) else {
            // The Engine owns malformed-frame and command-specific error routing.
            return Ok(self.engine.handle_frame(connection_id, frame)?);
        };
        match &decoded.body {
            DecodedBody::Auth(_) | DecodedBody::Reconnect(_) | DecodedBody::Heartbeat(_) => {
                self.engine.store.set_now(self.database.now().await?);
                return Ok(self.engine.handle(connection_id, &decoded)?);
            }
            DecodedBody::PushAck(_) => return Ok(self.engine.handle(connection_id, &decoded)?),
            _ if !decoded.msg_type.is_client_to_server() => {
                return Ok(self.engine.handle(connection_id, &decoded)?);
            }
            _ => {}
        }
        let cid = match self
            .engine
            .prepare_storage_request(connection_id, &decoded)?
        {
            StorageRequest::Ready(cid) => cid,
            StorageRequest::Respond(reply) => return Ok(reply),
        };
        let session = &self.engine.store.connections[&cid];
        let identity = Identity {
            tenant_id: session.tenant_id,
            user_id: session.user_id,
            device_id: session.device_id,
        };
        match &decoded.body {
            DecodedBody::Send(req) => {
                let content = req.content.as_ref().filter(|content| {
                    matches!(
                        content.value.as_ref(),
                        Some(message_content::Value::Text(text)) if text.text.is_some()
                    )
                });
                let Some(content) = content else {
                    return wire_error(cid, &decoded, ErrorCode::InvalidArgument);
                };
                self.send(cid, identity, &decoded, req.key.as_ref(), Some(content))
                    .await
            }
            DecodedBody::SendStatus(req) => {
                self.send(cid, identity, &decoded, req.key.as_ref(), None)
                    .await
            }
            DecodedBody::Sync(req) => {
                let value = self.database.sync(identity, req).await;
                reply(
                    cid,
                    &decoded,
                    MsgType::SyncAck,
                    &SyncResponse { value: Some(value) },
                )
            }
            DecodedBody::Snapshot(req) => {
                let value = self.database.snapshot(identity, req).await;
                reply(
                    cid,
                    &decoded,
                    MsgType::SnapshotAck,
                    &SnapshotResponse { value: Some(value) },
                )
            }
            _ => wire_error(cid, &decoded, ErrorCode::UnsupportedCommand),
        }
    }

    async fn send(
        &mut self,
        cid: Id128,
        identity: Identity,
        frame: &DecodedFrame,
        key: Option<&MessageKey>,
        content: Option<&MessageContent>,
    ) -> Result<EngineReply> {
        let Some(key) = key else {
            return wire_error(cid, frame, ErrorCode::InvalidArgument);
        };
        let Some((conversation, client_id)) = key.conversation_id.filter(|id| *id != 0).zip(
            key.client_msg_id
                .as_deref()
                .and_then(|bytes| validate_id128(bytes).ok()),
        ) else {
            return wire_error(cid, frame, ErrorCode::InvalidArgument);
        };
        let session = self.engine.store.connections.get_mut(&cid).unwrap();
        if content.is_some() {
            if session
                .inflight_send
                .get(&conversation)
                .is_some_and(|id| *id != client_id)
            {
                let value = unresolved(ErrorCode::InProgress, RetryAction::QuerySend, false);
                return send_reply(cid, frame, key, SendOutcome { value: Some(value) });
            }
            session.inflight_send.insert(conversation, client_id);
        }

        let scope = SendKey {
            owner: identity,
            conversation,
            client_id,
        };
        let Decision { outcome, delivery } = self
            .database
            .decide(&scope, content)
            .await
            .unwrap_or_else(|_| Decision {
                outcome: SendOutcome {
                    value: Some(unresolved(
                        ErrorCode::ResultUnavailable,
                        RetryAction::QuerySend,
                        false,
                    )),
                },
                delivery: None,
            });
        let release = match outcome.value.as_ref() {
            Some(send_outcome::Value::Committed(_) | send_outcome::Value::RejectedFinal(_)) => true,
            Some(send_outcome::Value::Unresolved(result)) => result.acceptance_closed == Some(true),
            None => false,
        };
        if release && session.inflight_send.get(&conversation) == Some(&client_id) {
            session.inflight_send.remove(&conversation);
        }
        let (cid_out, mut output) = send_reply(cid, frame, key, outcome)?;
        if let Some(delivery) = delivery {
            let change = Change {
                event_seq: Some(delivery.stream.upper),
                payload: Some(change::Payload::MessageUpsert(delivery.message)),
            };
            // The response and the durable change are available even if push encoding fails.
            let _ = self.engine.push_change(
                &mut output,
                cid,
                identity.tenant_id,
                &delivery.members,
                &delivery.stream.context(),
                &change,
            );
        }
        Ok((cid_out, output))
    }
}

fn send_reply(
    cid: Id128,
    frame: &DecodedFrame,
    key: &MessageKey,
    outcome: SendOutcome,
) -> Result<EngineReply> {
    if frame.msg_type == MsgType::SendStatus {
        reply(
            cid,
            frame,
            MsgType::SendStatusAck,
            &SendStatusResponse {
                key: Some(key.clone()),
                outcome: Some(outcome),
            },
        )
    } else {
        reply(
            cid,
            frame,
            MsgType::SendAck,
            &SendResponse {
                key: Some(key.clone()),
                outcome: Some(outcome),
            },
        )
    }
}

fn reply(
    cid: Id128,
    frame: &DecodedFrame,
    kind: MsgType,
    body: &impl Message,
) -> Result<EngineReply> {
    let bytes = encode_frame(kind, &frame.head, body).map_err(CoreError::from)?;
    Ok((
        Some(cid),
        EngineOutput {
            responses: vec![bytes],
            pushes: vec![],
        },
    ))
}

fn wire_error(cid: Id128, frame: &DecodedFrame, code: ErrorCode) -> Result<EngineReply> {
    let bytes = encode_protocol_error(
        frame.head.req_id,
        Some(frame.msg_type as u32),
        code,
        RetryAction::None,
        false,
        None,
    )
    .map_err(CoreError::from)?;
    Ok((
        Some(cid),
        EngineOutput {
            responses: vec![bytes],
            pushes: vec![],
        },
    ))
}
