use crate::clock::ClockBatch;
use crate::store::{unresolved, Decision, FriendAttempt, Scope, SendKey, TidbStore};
use crate::{ConversationInfo, Error, Identity, Result, TextPage};
use iam_core::{CoreError, DeviceCredential, Engine, EngineOutput, EngineReply, StorageRequest};
use iam_protocol::pb::{
    change, friend_attempt_ack, friend_request_ack, message_content, send_outcome, Change, Empty,
    ErrorCode, FriendAcceptAck, FriendAttemptAck, FriendRequestAck, GroupCreateAck,
    HistoryResponse, MessageContent, MessageKey, RetryAction, SendOutcome, SendResponse,
    SendStatusResponse, SnapshotResponse, SyncResponse,
};
use iam_protocol::{
    decode_frame, encode_frame, encode_protocol_error, validate_id128, DecodedBody, DecodedFrame,
    Frame, Id128, MsgType,
};
use prost::Message;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub enum OpenMode {
    /// Creates a fresh authority using the Engine's seeded conversations.
    Initialize,
    /// The caller has verified this is the same continuously authoritative database.
    Resume,
}

/// Wire handler for authenticated sessions backed by TiDB.
#[derive(Clone)]
pub struct TidbService {
    engine: Arc<Mutex<Engine>>,
    database: Arc<TidbStore>,
    accepting: Arc<AtomicBool>,
    clock: ClockBatch,
    #[cfg(test)]
    clock_batching: bool,
}

impl TidbService {
    /// Upgrade a stopped local store, keeping its identity and cursors.
    pub async fn upgrade_local(url: &str, store_id: Id128) -> Result<()> {
        TidbStore::upgrade(url, store_id).await
    }

    /// Trusted application operation. Wire clients cannot create conversations through this API.
    /// A pair with an assigned conversation can only be opened under its canonical ID.
    pub async fn create_direct(
        &self,
        tenant: u64,
        conversation: u64,
        a: u64,
        b: u64,
    ) -> Result<()> {
        self.database
            .create_direct(tenant, conversation, a, b)
            .await
    }

    /// Trusted application operation for the original participants of a conversation.
    pub async fn set_member_access(
        &self,
        tenant: u64,
        conversation: u64,
        user: u64,
        allow: bool,
    ) -> Result<()> {
        self.database
            .set_member_access(tenant, conversation, user, allow)
            .await
    }

    /// Advance the oldest readable message position while retaining records for send deduplication.
    pub async fn set_retention_floor(
        &self,
        tenant: u64,
        conversation: u64,
        floor: u64,
    ) -> Result<()> {
        self.database
            .set_retention_floor(tenant, conversation, floor)
            .await
    }

    pub async fn connect_local(
        url: &str,
        store_id: Id128,
        engine: Engine,
        mode: OpenMode,
    ) -> Result<Self> {
        Self::open(url, store_id, engine, mode, crate::clock::WINDOW).await
    }

    #[cfg(feature = "profile")]
    #[doc(hidden)]
    pub async fn connect_local_profiled(
        url: &str,
        store_id: Id128,
        engine: Engine,
        mode: OpenMode,
        window_ms: u64,
    ) -> Result<Self> {
        if !matches!(window_ms, 1 | 2 | 4) {
            return Err(Error::Configuration(
                "profile clock window must be 1, 2 or 4 ms",
            ));
        }
        Self::open(
            url,
            store_id,
            engine,
            mode,
            Duration::from_millis(window_ms),
        )
        .await
    }

    #[cfg(feature = "profile")]
    #[doc(hidden)]
    pub fn clock_counts(&self) -> crate::profile::ClockCounts {
        self.clock.counts()
    }

    async fn open(
        url: &str,
        store_id: Id128,
        engine: Engine,
        mode: OpenMode,
        window: Duration,
    ) -> Result<Self> {
        if !engine.store.connections.is_empty() || engine.store.recovery_incomplete {
            return Err(Error::Configuration(
                "expected a fresh engine for the TiDB protocol",
            ));
        }
        let seeds = matches!(mode, OpenMode::Initialize).then_some(&engine.store);
        let database = Arc::new(TidbStore::open(url, store_id, seeds).await?);
        let clock = ClockBatch::start(database.clone(), window);
        Ok(Self {
            engine: Arc::new(Mutex::new(engine)),
            database,
            accepting: Arc::new(AtomicBool::new(true)),
            clock,
            #[cfg(test)]
            clock_batching: true,
        })
    }

    pub fn disconnect(&self, connection_id: Option<Id128>) {
        self.engine.lock().unwrap().disconnect(connection_id);
    }

    pub fn uses_device_credentials(&self) -> bool {
        self.engine.lock().unwrap().store.uses_device_credentials()
    }

    pub fn reload_device_credentials(
        &self,
        credentials: Vec<DeviceCredential>,
    ) -> Result<Vec<Id128>> {
        self.engine
            .lock()
            .unwrap()
            .store
            .reload_device_credentials(credentials)
            .map_err(Error::Configuration)
    }

    pub fn gateway_session_expiry(&self, cid: Id128) -> std::result::Result<i64, ErrorCode> {
        let engine = self.engine.lock().unwrap();
        let expires_at = engine
            .store
            .session_expires_at(cid)
            .ok_or(ErrorCode::SessionReplaced)?;
        if !engine.store.session_authorized(cid) {
            return Err(ErrorCode::Unauthenticated);
        }
        Ok(expires_at)
    }

    pub fn revoke_device(&self, tenant: u64, user: u64, device: Id128) -> Option<Id128> {
        self.engine
            .lock()
            .unwrap()
            .store
            .revoke_device(tenant, user, device)
    }

    pub fn reject_overloaded(&self, cid: Option<Id128>, frame: &Frame) -> Result<EngineReply> {
        Ok(self.engine.lock().unwrap().reject_overloaded(cid, frame)?)
    }

    pub fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn set_clock_batching(&mut self, enabled: bool) {
        self.clock_batching = enabled;
    }

    async fn request_time(&self) -> Result<i64> {
        #[cfg(test)]
        if !self.clock_batching {
            return self.database.now().await;
        }
        self.clock.now().await
    }

    /// Reads the current position of an authenticated member's conversation.
    pub async fn conversation(&self, cid: Id128, id: u64) -> Result<ConversationInfo> {
        let identity = Self::identity(&self.engine.lock().unwrap(), cid)?;
        let stream = self
            .database
            .stream(identity, Scope::Conversation(id))
            .await?;
        Ok(ConversationInfo {
            history_id: stream.epoch,
            next_sequence: stream.upper + 1,
        })
    }

    /// Local console read ordered by creation position, with an exclusive bound.
    pub async fn read_messages(
        &self,
        cid: Id128,
        id: u64,
        before: Option<u64>,
        limit: u32,
    ) -> Result<TextPage> {
        if limit == 0 || limit > 100 || before == Some(0) {
            return Err(Error::InvalidRequest(
                "page limit must be 1..100 and before must be nonzero",
            ));
        }
        let identity = Self::identity(&self.engine.lock().unwrap(), cid)?;
        self.database.page(identity, id, before, limit).await
    }

    fn identity(engine: &Engine, cid: Id128) -> Result<Identity> {
        let session = engine
            .store
            .connections
            .get(&cid)
            .ok_or(Error::Unauthenticated)?;
        if !engine.store.session_authorized(cid) {
            return Err(Error::Unauthenticated);
        }
        Ok(Identity {
            tenant_id: session.tenant_id,
            user_id: session.user_id,
            device_id: session.device_id,
        })
    }

    /// Calls on different connections may overlap. The caller processes each connection in order.
    pub async fn handle_frame(
        &self,
        connection_id: Option<Id128>,
        frame: &Frame,
    ) -> Result<EngineReply> {
        let Ok(decoded) = decode_frame(frame) else {
            return Ok(self
                .engine
                .lock()
                .unwrap()
                .handle_frame(connection_id, frame)?);
        };
        match &decoded.body {
            DecodedBody::Auth(_) | DecodedBody::Reconnect(_) | DecodedBody::Heartbeat(_) => {
                let time = if matches!(decoded.body, DecodedBody::Heartbeat(_)) {
                    self.database.now().await
                } else {
                    self.request_time().await
                };
                let now = match time {
                    Ok(now) => now,
                    Err(error) if !matches!(decoded.body, DecodedBody::Heartbeat(_)) => {
                        return clock_error(connection_id, &decoded, error);
                    }
                    Err(error) => return Err(error),
                };
                let mut engine = self.engine.lock().unwrap();
                engine.store.set_now(now);
                return Ok(engine.handle(connection_id, &decoded)?);
            }
            DecodedBody::PushAck(_) => {
                return Ok(self
                    .engine
                    .lock()
                    .unwrap()
                    .handle(connection_id, &decoded)?);
            }
            _ if !decoded.msg_type.is_client_to_server() => {
                return Ok(self
                    .engine
                    .lock()
                    .unwrap()
                    .handle(connection_id, &decoded)?);
            }
            _ => {}
        }
        let (cid, identity) = {
            let mut engine = self.engine.lock().unwrap();
            let cid = match engine.prepare_storage_request(connection_id, &decoded)? {
                StorageRequest::Ready(cid) => cid,
                StorageRequest::Respond(reply) => return Ok(reply),
            };
            (cid, Self::identity(&engine, cid)?)
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
                let now = match self.request_time().await {
                    Ok(now) => now,
                    Err(error) => return clock_error(Some(cid), &decoded, error),
                };
                if let Some(error) = self.check_read_session(cid, &decoded)? {
                    return Ok(error);
                }
                let value = self.database.sync_at(identity, req, now).await;
                if let Some(error) = self.check_read_session(cid, &decoded)? {
                    return Ok(error);
                }
                reply(
                    cid,
                    &decoded,
                    MsgType::SyncAck,
                    &SyncResponse { value: Some(value) },
                )
            }
            DecodedBody::Snapshot(req) => {
                let value = self.database.snapshot(identity, req).await;
                if let Some(error) = self.check_read_session(cid, &decoded)? {
                    return Ok(error);
                }
                reply(
                    cid,
                    &decoded,
                    MsgType::SnapshotAck,
                    &SnapshotResponse { value: Some(value) },
                )
            }
            DecodedBody::History(req) => {
                let value = self.database.history(identity, req).await;
                if let Some(error) = self.check_read_session(cid, &decoded)? {
                    return Ok(error);
                }
                reply(
                    cid,
                    &decoded,
                    MsgType::HistoryAck,
                    &HistoryResponse { value: Some(value) },
                )
            }
            DecodedBody::FriendRequest(req) => {
                let Some(target) = req.target_user_id else {
                    return wire_error(cid, &decoded, ErrorCode::InvalidArgument);
                };
                let result = self
                    .database
                    .request_friend(identity, target, |write| {
                        self.admit_membership(cid, write, Some(target))
                    })
                    .await
                    .map(|conversation| FriendRequestAck {
                        value: Some(conversation.map_or(
                            friend_request_ack::Value::Pending(Empty {}),
                            friend_request_ack::Value::ConversationId,
                        )),
                    });
                self.membership_reply(cid, &decoded, MsgType::FriendRequestAck, result)
            }
            DecodedBody::FriendList(req) => {
                let Some(after) = req.after_user_id else {
                    return wire_error(cid, &decoded, ErrorCode::InvalidArgument);
                };
                let result = self.database.incoming_friends(identity, after).await;
                self.membership_reply(cid, &decoded, MsgType::FriendListAck, result)
            }
            DecodedBody::FriendAccept(req) => {
                let Some(sender) = req.from_user_id else {
                    return wire_error(cid, &decoded, ErrorCode::InvalidArgument);
                };
                let result = self
                    .database
                    .accept_friend(identity, sender, |write| {
                        self.admit_membership(cid, write, None)
                    })
                    .await
                    .map(|conversation| FriendAcceptAck {
                        conversation_id: Some(conversation),
                    });
                self.membership_reply(cid, &decoded, MsgType::FriendAcceptAck, result)
            }
            DecodedBody::FriendApply(req) => {
                let Some((target, id)) = req
                    .target_user_id
                    .zip(request_id128(req.request_id.as_deref()))
                else {
                    return wire_error(cid, &decoded, ErrorCode::InvalidArgument);
                };
                let result = self
                    .database
                    .apply_friend(identity, target, Some(id), |write| {
                        self.admit_membership(cid, write, Some(target))
                    })
                    .await
                    .map(friend_attempt_ack);
                self.membership_reply(cid, &decoded, MsgType::FriendApplyAck, result)
            }
            DecodedBody::FriendInbox(req) => {
                let Some(after) = req.after_user_id else {
                    return wire_error(cid, &decoded, ErrorCode::InvalidArgument);
                };
                let result = self.database.friend_inbox(identity, after, false).await;
                self.membership_reply(cid, &decoded, MsgType::FriendInboxAck, result)
            }
            DecodedBody::FriendDecide(req) => {
                let Some(((sender, id), accept)) = req
                    .from_user_id
                    .zip(request_id128(req.request_id.as_deref()))
                    .zip(req.accept)
                else {
                    return wire_error(cid, &decoded, ErrorCode::InvalidArgument);
                };
                let result = self
                    .database
                    .decide_friend(identity, sender, Some(id), accept, |write| {
                        self.admit_membership(cid, write, None)
                    })
                    .await
                    .map(friend_attempt_ack);
                self.membership_reply(cid, &decoded, MsgType::FriendDecideAck, result)
            }
            DecodedBody::GroupCreate(req) => {
                let Some(request_id) = request_id128(req.request_id.as_deref()) else {
                    return wire_error(cid, &decoded, ErrorCode::InvalidArgument);
                };
                let result = self
                    .database
                    .create_group(identity, request_id, &req.member_user_ids, |write| {
                        self.admit_membership(cid, write, None)
                    })
                    .await
                    .map(|conversation_id| GroupCreateAck {
                        conversation_id: Some(conversation_id),
                    });
                self.membership_reply(cid, &decoded, MsgType::GroupCreateAck, result)
            }
            _ => wire_error(cid, &decoded, ErrorCode::UnsupportedCommand),
        }
    }

    fn admit_membership(&self, cid: Id128, write: bool, new_target: Option<u64>) -> Result<()> {
        let engine = self.engine.lock().unwrap();
        let identity = Self::identity(&engine, cid)?;
        if write {
            if !self.accepting.load(Ordering::Relaxed) {
                return Err(Error::MembershipUnavailable);
            }
            if new_target
                .is_some_and(|target| !engine.store.has_account(identity.tenant_id, target))
            {
                return Err(Error::FriendNotFound);
            }
        }
        Ok(())
    }

    fn membership_reply(
        &self,
        cid: Id128,
        frame: &DecodedFrame,
        kind: MsgType,
        result: Result<impl Message>,
    ) -> Result<EngineReply> {
        if let Some(error) = self.check_read_session(cid, frame)? {
            return Ok(error);
        }
        match result {
            Ok(body) => reply(cid, frame, kind, &body),
            Err(Error::InvalidRequest(_)) => wire_error(cid, frame, ErrorCode::InvalidArgument),
            Err(Error::FriendNotFound) => wire_error(cid, frame, ErrorCode::NotFound),
            Err(Error::FriendChanged) => wire_error(cid, frame, ErrorCode::ContextChanged),
            Err(Error::FriendKeyConflict | Error::GroupKeyConflict) => {
                wire_error(cid, frame, ErrorCode::IdempotencyConflict)
            }
            Err(Error::FriendInProgress) => wire_error(cid, frame, ErrorCode::InProgress),
            Err(Error::PermissionDenied) => wire_error(cid, frame, ErrorCode::PermissionDenied),
            Err(Error::Unauthenticated) => protocol_error(
                None,
                frame,
                ErrorCode::Unauthenticated,
                RetryAction::Reauthenticate,
            ),
            Err(_) => protocol_error(
                Some(cid),
                frame,
                ErrorCode::ResultUnavailable,
                RetryAction::RetrySameRequest,
            ),
        }
    }

    fn check_read_session(&self, cid: Id128, frame: &DecodedFrame) -> Result<Option<EngineReply>> {
        if Self::identity(&self.engine.lock().unwrap(), cid).is_ok() {
            return Ok(None);
        }
        protocol_error(
            None,
            frame,
            ErrorCode::Unauthenticated,
            RetryAction::Reauthenticate,
        )
        .map(Some)
    }

    async fn send(
        &self,
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
        {
            let mut engine = self.engine.lock().unwrap();
            Self::identity(&engine, cid)?;
            let session = engine.store.connections.get_mut(&cid).unwrap();
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
        }

        let scope = SendKey {
            owner: identity,
            conversation,
            client_id,
        };
        let Decision { outcome, delivery } = self
            .database
            .decide(&scope, content, self.accepting.load(Ordering::Relaxed))
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
        let mut engine = self.engine.lock().unwrap();
        if let Some(session) = engine.store.connections.get_mut(&cid) {
            if release && session.inflight_send.get(&conversation) == Some(&client_id) {
                session.inflight_send.remove(&conversation);
            }
        }
        let (cid_out, mut output) = send_reply(cid, frame, key, outcome)?;
        if let Some(delivery) = delivery {
            let change = Change {
                event_seq: Some(delivery.stream.upper),
                payload: Some(change::Payload::MessageUpsert(delivery.message)),
            };
            // The response and the durable change are available even if push encoding fails.
            let mut stream = delivery.stream;
            for (user, version) in delivery.members {
                stream.access_version = version;
                let _ = engine.push_change(
                    &mut output,
                    cid,
                    identity.tenant_id,
                    &[user],
                    &stream.context(),
                    &change,
                );
            }
        }
        Ok((cid_out, output))
    }
}

fn request_id128(bytes: Option<&[u8]>) -> Option<Id128> {
    bytes.and_then(|bytes| validate_id128(bytes).ok())
}

fn friend_attempt_ack(result: FriendAttempt) -> FriendAttemptAck {
    use friend_attempt_ack::Value;
    FriendAttemptAck {
        value: Some(match result {
            FriendAttempt::Pending => Value::Pending(Empty {}),
            FriendAttempt::Accepted(id) => Value::ConversationId(id),
            FriendAttempt::Rejected => Value::Rejected(Empty {}),
        }),
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

fn clock_error(cid: Option<Id128>, frame: &DecodedFrame, error: Error) -> Result<EngineReply> {
    let code = if matches!(error, Error::ClockOverloaded) {
        ErrorCode::Overloaded
    } else {
        ErrorCode::InternalError
    };
    protocol_error(cid, frame, code, RetryAction::RetrySameRequest)
}

fn wire_error(cid: Id128, frame: &DecodedFrame, code: ErrorCode) -> Result<EngineReply> {
    protocol_error(Some(cid), frame, code, RetryAction::None)
}

fn protocol_error(
    cid: Option<Id128>,
    frame: &DecodedFrame,
    code: ErrorCode,
    action: RetryAction,
) -> Result<EngineReply> {
    let bytes = encode_protocol_error(
        frame.head.req_id,
        Some(frame.msg_type as u32),
        code,
        action,
        false,
        None,
    )
    .map_err(CoreError::from)?;
    Ok((
        cid,
        EngineOutput {
            responses: vec![bytes],
            pushes: vec![],
        },
    ))
}
