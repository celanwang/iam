use crate::store::{
    active_message_view, stream_context_account, stream_context_for_conv, ConnState, IdemKey,
    IdempotencyRecord, MemoryStore, ALLOW_SKEW_MS,
};
use crate::token::{
    HistoryToken, ResumeToken, SnapshotToken, SyncCursor, SyncPageToken, TokenError, TokenSigner,
};
use iam_protocol::pb::{
    change, send_outcome, snapshot_item, snapshot_response, sync_response, sync_target,
    AuthRequest, AuthResponse, Change, ErrorCode, ErrorInfo, Head, HeartbeatAck, HistoryPage,
    HistoryRequest, HistoryResponse, MessageKey, MessageRef, PageLimit, ProtocolLimits, Push,
    ReconnectRequest, ReconnectResponse, ResetReason, ResetRequired, RetryAction, SendOutcome,
    SendReceipt, SendRequest, SendResponse, SendStatusRequest, SendStatusResponse, SendUnresolved,
    Session, SnapshotPage, SnapshotRequest, SnapshotResponse, SyncPage, SyncRequest, SyncResponse,
};
use iam_protocol::{
    encode_frame, encode_protocol_error, error_info, generate_id128, validate_id128,
    validate_message_content, DecodedBody, DecodedFrame, Id128, MsgType, ProtocolErrorKind,
};
use prost::Message;
use std::collections::VecDeque;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error(transparent)]
    Protocol(#[from] ProtocolErrorKind),
    #[error("{0}")]
    Msg(&'static str),
}

#[derive(Debug, Default)]
pub struct EngineOutput {
    pub responses: Vec<Vec<u8>>,
    pub pushes: Vec<(Id128, Vec<u8>)>,
}

struct SyncRound {
    after: u64,
    upper: u64,
    started_ms: i64,
    max_bytes: usize,
}

pub struct Engine {
    pub store: MemoryStore,
    pub tokens: TokenSigner,
}

impl Engine {
    pub fn new(now_ms: i64) -> Self {
        let mut key = [0u8; 32];
        getrandom::fill(&mut key).expect("secure randomness unavailable");
        Self {
            store: MemoryStore::new(now_ms),
            tokens: TokenSigner::new(key),
        }
    }

    /// Entry point for a complete frame supplied by a bounded transport decoder.
    pub fn handle_frame(
        &mut self,
        connection_id: Option<Id128>,
        frame: &iam_protocol::Frame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        match iam_protocol::decode_frame(frame) {
            Ok(decoded) => self.handle(connection_id, &decoded),
            Err(error) => {
                let head = if iam_protocol::has_duplicate_field(&frame.head, 1) == Ok(false) {
                    Head::decode(frame.head.as_slice()).unwrap_or_default()
                } else {
                    Head::default()
                };
                let kind = MsgType::from_u8(frame.header.msg_type);
                let (code, mut close) = match error {
                    ProtocolErrorKind::UnsupportedCommand(_) => {
                        (ErrorCode::UnsupportedCommand, false)
                    }
                    ProtocolErrorKind::UnsupportedContent => (ErrorCode::UnsupportedContent, false),
                    ProtocolErrorKind::UnsupportedCapability => {
                        (ErrorCode::UnsupportedCapability, false)
                    }
                    ProtocolErrorKind::InvalidArgument(_) => (ErrorCode::InvalidArgument, false),
                    ProtocolErrorKind::UnsupportedVersion(_) => {
                        (ErrorCode::UnsupportedVersion, true)
                    }
                    ProtocolErrorKind::UnsupportedFlags(_) => (ErrorCode::UnsupportedFlags, true),
                    ProtocolErrorKind::ResourceLimit(_) => (ErrorCode::ResourceLimit, true),
                    _ => (ErrorCode::MalformedFrame, true),
                };
                if let Some(kind) = kind {
                    close |= iam_protocol::validate_head(kind, &head).is_err()
                        || !kind.is_client_to_server();
                    let envelope = DecodedFrame {
                        msg_type: kind,
                        head,
                        body: DecodedBody::Heartbeat(iam_protocol::pb::HeartbeatRequest {}),
                    };
                    if !close {
                        if !matches!(kind, MsgType::Auth | MsgType::Reconnect) {
                            let cid = match self.require_auth(connection_id, &envelope)? {
                                AuthGate::Ok(cid) => cid,
                                AuthGate::Respond(out) => return Ok(out),
                            };
                            if kind != MsgType::PushAck {
                                if let Err(out) = self.check_req_id(cid, &envelope) {
                                    return Ok(out);
                                }
                            }
                            match kind {
                                MsgType::Sync => {
                                    return self.sync_err(cid, &envelope, code, RetryAction::None)
                                }
                                MsgType::Snapshot => {
                                    return self.snapshot_err(
                                        cid,
                                        &envelope,
                                        code,
                                        RetryAction::None,
                                    )
                                }
                                MsgType::History => {
                                    return self.history_err(
                                        cid,
                                        &envelope,
                                        code,
                                        RetryAction::None,
                                    )
                                }
                                _ => {}
                            }
                        } else if connection_id.is_none() {
                            let error = error_info(code, RetryAction::None);
                            let bytes = if kind == MsgType::Auth {
                                self.encode_response(
                                    MsgType::AuthAck,
                                    head.req_id,
                                    &AuthResponse {
                                        value: Some(iam_protocol::pb::auth_response::Value::Error(
                                            error,
                                        )),
                                    },
                                )?
                            } else {
                                self.encode_response(
                                    MsgType::ReconnectAck,
                                    head.req_id,
                                    &ReconnectResponse {
                                        value: Some(
                                            iam_protocol::pb::reconnect_response::Value::Error(
                                                error,
                                            ),
                                        ),
                                    },
                                )?
                            };
                            return Ok((
                                None,
                                EngineOutput {
                                    responses: vec![bytes],
                                    pushes: vec![],
                                },
                            ));
                        }
                    }
                }
                if close {
                    self.disconnect(connection_id);
                }
                let bytes = encode_protocol_error(
                    head.req_id.filter(|r| *r != 0),
                    Some(frame.header.msg_type as u32),
                    code,
                    RetryAction::None,
                    close,
                    None,
                )?;
                Ok((
                    if close { None } else { connection_id },
                    EngineOutput {
                        responses: vec![bytes],
                        pushes: vec![],
                    },
                ))
            }
        }
    }

    /// Called when the transport closes, including unsuccessful error delivery.
    pub fn disconnect(&mut self, connection_id: Option<Id128>) {
        if let Some(cid) = connection_id {
            if let Some(conn) = self.store.connections.remove(&cid) {
                self.store
                    .device_conn
                    .remove(&(conn.tenant_id, conn.user_id, conn.device_id));
            }
        }
    }

    pub fn handle(
        &mut self,
        connection_id: Option<Id128>,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        if !frame.msg_type.is_client_to_server()
            || iam_protocol::validate_head(frame.msg_type, &frame.head).is_err()
        {
            return self.wire_error(
                frame,
                connection_id,
                ErrorCode::InvalidArgument,
                RetryAction::None,
                true,
                None,
            );
        }
        if !matches!(frame.body, DecodedBody::Auth(_) | DecodedBody::Reconnect(_)) {
            let cid = match self.require_auth(connection_id, frame)? {
                AuthGate::Ok(cid) => cid,
                AuthGate::Respond(out) => return Ok(out),
            };
            if frame.msg_type != MsgType::PushAck
                && frame.head.req_id.unwrap_or(0) <= self.store.connections[&cid].last_req_id
            {
                return self.wire_error(
                    frame,
                    Some(cid),
                    ErrorCode::InvalidArgument,
                    RetryAction::None,
                    true,
                    None,
                );
            }
        }
        // Entrance overload applies before command processors (except AUTH/RECONNECT bootstrap).
        if self.store.entrance_overloaded
            && !matches!(
                frame.body,
                DecodedBody::Auth(_) | DecodedBody::Reconnect(_) | DecodedBody::Error(_)
            )
        {
            if frame.msg_type != MsgType::PushAck {
                if let Some(cid) = connection_id {
                    if let Err(out) = self.check_req_id(cid, frame) {
                        return Ok(out);
                    }
                }
            }
            return self.wire_error(
                frame,
                connection_id,
                ErrorCode::RateLimited,
                RetryAction::RetrySameRequest,
                false,
                Some("entrance overloaded"),
            );
        }
        match &frame.body {
            DecodedBody::Auth(req) => self.handle_auth(connection_id, req, frame),
            DecodedBody::Reconnect(req) => self.handle_reconnect(connection_id, req, frame),
            DecodedBody::Heartbeat(_) => self.handle_heartbeat(connection_id, frame),
            DecodedBody::Send(req) => self.handle_send(connection_id, req, frame),
            DecodedBody::SendStatus(req) => self.handle_send_status(connection_id, req, frame),
            DecodedBody::Sync(req) => self.handle_sync(connection_id, req, frame),
            DecodedBody::Snapshot(req) => self.handle_snapshot(connection_id, req, frame),
            DecodedBody::History(req) => self.handle_history(connection_id, req, frame),
            DecodedBody::PushAck(ack) => self.handle_push_ack(connection_id, ack.push_id, frame),
            DecodedBody::Error(_) => self.wire_error(
                frame,
                connection_id,
                ErrorCode::InvalidArgument,
                RetryAction::None,
                true,
                Some("client must not send ERROR"),
            ),
            _ => self.wire_error(
                frame,
                connection_id,
                ErrorCode::InvalidArgument,
                RetryAction::None,
                true,
                None,
            ),
        }
    }

    fn wire_error(
        &mut self,
        frame: &DecodedFrame,
        connection_id: Option<Id128>,
        code: ErrorCode,
        action: RetryAction,
        close: bool,
        detail: Option<&str>,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let connection_id = if close {
            self.disconnect(connection_id);
            None
        } else {
            connection_id
        };
        let req_id = frame.head.req_id.filter(|&r| r != 0);
        let bytes = encode_protocol_error(
            req_id,
            Some(frame.msg_type as u8 as u32),
            code,
            action,
            close,
            detail,
        )?;
        Ok((
            connection_id,
            EngineOutput {
                responses: vec![bytes],
                pushes: vec![],
            },
        ))
    }

    fn negotiate_caps(requested: &[String]) -> Result<Vec<String>, CoreError> {
        if requested.len() > 32 {
            return Err(CoreError::Msg("too many capabilities"));
        }
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for c in requested {
            if c.is_empty()
                || c.len() > 32
                || !c
                    .chars()
                    .all(|ch| matches!(ch, 'a'..='z' | '0'..='9' | '_'))
                || !c.starts_with(|ch: char| ch.is_ascii_lowercase())
            {
                return Err(CoreError::Msg("bad capability"));
            }
            if !seen.insert(c.clone()) {
                return Err(CoreError::Msg("duplicate capability"));
            }
            if c == "push_ack" {
                out.push(c.clone());
            }
        }
        out.sort();
        Ok(out)
    }

    fn replace_device_connection(
        &mut self,
        owner: (u64, u64, Id128),
        new_cid: Id128,
    ) -> Result<Vec<(Id128, Vec<u8>)>, CoreError> {
        let mut pushes = Vec::new();
        if let Some(old) = self.store.device_conn.insert(owner, new_cid) {
            if old != new_cid {
                if let Some(_old_conn) = self.store.connections.remove(&old) {
                    let err = encode_protocol_error(
                        None,
                        None,
                        ErrorCode::SessionReplaced,
                        RetryAction::None,
                        true,
                        None,
                    )?;
                    pushes.push((old, err));
                }
            }
        }
        Ok(pushes)
    }

    fn build_session(
        &self,
        tenant_id: u64,
        user_id: u64,
        device_id: Id128,
        connection_id: Id128,
        caps: Vec<String>,
        resume: Vec<u8>,
    ) -> Session {
        Session {
            tenant_id: Some(tenant_id),
            user_id: Some(user_id),
            device_id: Some(device_id.to_vec().into()),
            connection_id: Some(connection_id.to_vec().into()),
            resume_token: Some(resume.into()),
            enabled_capabilities: caps,
            server_time_ms: Some(self.store.now_ms),
            limits: Some(default_limits()),
        }
    }

    fn handle_auth(
        &mut self,
        connection_id: Option<Id128>,
        req: &AuthRequest,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        if connection_id.is_some() {
            return self.wire_error(
                frame,
                connection_id,
                ErrorCode::InvalidArgument,
                RetryAction::None,
                false,
                Some("already authenticated"),
            );
        }
        let caps = match Self::negotiate_caps(&req.requested_capabilities) {
            Ok(c) => c,
            Err(_) => {
                let body = AuthResponse {
                    value: Some(iam_protocol::pb::auth_response::Value::Error(error_info(
                        ErrorCode::InvalidArgument,
                        RetryAction::None,
                    ))),
                };
                let out = self.encode_response(MsgType::AuthAck, frame.head.req_id, &body)?;
                return Ok((
                    None,
                    EngineOutput {
                        responses: vec![out],
                        pushes: vec![],
                    },
                ));
            }
        };
        let token = match req.access_token.as_ref() {
            Some(t) if (1..=4096).contains(&t.len()) => t.as_ref(),
            _ => {
                let body = AuthResponse {
                    value: Some(iam_protocol::pb::auth_response::Value::Error(error_info(
                        ErrorCode::Unauthenticated,
                        RetryAction::Reauthenticate,
                    ))),
                };
                let out = self.encode_response(MsgType::AuthAck, frame.head.req_id, &body)?;
                return Ok((
                    None,
                    EngineOutput {
                        responses: vec![out],
                        pushes: vec![],
                    },
                ));
            }
        };
        let device_id = match req.device_id.as_ref().map(|b| validate_id128(b.as_ref())) {
            Some(Ok(id)) => id,
            _ => {
                let body = AuthResponse {
                    value: Some(iam_protocol::pb::auth_response::Value::Error(error_info(
                        ErrorCode::InvalidArgument,
                        RetryAction::None,
                    ))),
                };
                let out = self.encode_response(MsgType::AuthAck, frame.head.req_id, &body)?;
                return Ok((
                    None,
                    EngineOutput {
                        responses: vec![out],
                        pushes: vec![],
                    },
                ));
            }
        };
        let user = match self.store.users_by_token.get(token).cloned() {
            Some(u) => u,
            None => {
                let body = AuthResponse {
                    value: Some(iam_protocol::pb::auth_response::Value::Error(error_info(
                        ErrorCode::Unauthenticated,
                        RetryAction::Reauthenticate,
                    ))),
                };
                let out = self.encode_response(MsgType::AuthAck, frame.head.req_id, &body)?;
                return Ok((
                    None,
                    EngineOutput {
                        responses: vec![out],
                        pushes: vec![],
                    },
                ));
            }
        };

        let connection_id = generate_id128();
        let auth_epoch = generate_id128();
        let pushes = self
            .replace_device_connection((user.tenant_id, user.user_id, device_id), connection_id)?;
        self.store.connections.insert(
            connection_id,
            ConnState {
                connection_id,
                user_id: user.user_id,
                tenant_id: user.tenant_id,
                device_id,
                auth_epoch,
                last_req_id: frame.head.req_id.unwrap_or(0),
                push_ack: caps.iter().any(|c| c == "push_ack"),
                next_push_id: 1,
                outbound: VecDeque::new(),
                inflight_send: Default::default(),
            },
        );
        let resume = self.tokens.sign_resume(&ResumeToken {
            tenant_id: user.tenant_id,
            user_id: user.user_id,
            device_id,
            auth_epoch,
            issued_at_ms: self.store.now_ms,
        });
        let session = self.build_session(
            user.tenant_id,
            user.user_id,
            device_id,
            connection_id,
            caps,
            resume,
        );
        let body = AuthResponse {
            value: Some(iam_protocol::pb::auth_response::Value::Session(session)),
        };
        let out = self.encode_response(MsgType::AuthAck, frame.head.req_id, &body)?;
        Ok((
            Some(connection_id),
            EngineOutput {
                responses: vec![out],
                pushes,
            },
        ))
    }

    fn handle_reconnect(
        &mut self,
        connection_id: Option<Id128>,
        req: &ReconnectRequest,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        if connection_id.is_some() {
            return self.wire_error(
                frame,
                connection_id,
                ErrorCode::InvalidArgument,
                RetryAction::None,
                false,
                Some("already authenticated"),
            );
        }
        let caps = match Self::negotiate_caps(&req.requested_capabilities) {
            Ok(c) => c,
            Err(_) => {
                let body = ReconnectResponse {
                    value: Some(iam_protocol::pb::reconnect_response::Value::Error(
                        error_info(ErrorCode::InvalidArgument, RetryAction::None),
                    )),
                };
                let out = self.encode_response(MsgType::ReconnectAck, frame.head.req_id, &body)?;
                return Ok((
                    None,
                    EngineOutput {
                        responses: vec![out],
                        pushes: vec![],
                    },
                ));
            }
        };
        let device_id = match req.device_id.as_ref().map(|b| validate_id128(b.as_ref())) {
            Some(Ok(id)) => id,
            _ => {
                let body = ReconnectResponse {
                    value: Some(iam_protocol::pb::reconnect_response::Value::Error(
                        error_info(ErrorCode::InvalidArgument, RetryAction::None),
                    )),
                };
                let out = self.encode_response(MsgType::ReconnectAck, frame.head.req_id, &body)?;
                return Ok((
                    None,
                    EngineOutput {
                        responses: vec![out],
                        pushes: vec![],
                    },
                ));
            }
        };
        let resume_bytes = match req.resume_token.as_ref() {
            Some(t) if (1..=4096).contains(&t.len()) => t.as_ref(),
            _ => {
                let body = ReconnectResponse {
                    value: Some(iam_protocol::pb::reconnect_response::Value::Error(
                        error_info(ErrorCode::Unauthenticated, RetryAction::Reauthenticate),
                    )),
                };
                let out = self.encode_response(MsgType::ReconnectAck, frame.head.req_id, &body)?;
                return Ok((
                    None,
                    EngineOutput {
                        responses: vec![out],
                        pushes: vec![],
                    },
                ));
            }
        };
        let resume = match self.tokens.verify_resume(resume_bytes, self.store.now_ms) {
            Ok(r) if r.device_id == device_id => r,
            _ => {
                let body = ReconnectResponse {
                    value: Some(iam_protocol::pb::reconnect_response::Value::Error(
                        error_info(ErrorCode::Unauthenticated, RetryAction::Reauthenticate),
                    )),
                };
                let out = self.encode_response(MsgType::ReconnectAck, frame.head.req_id, &body)?;
                return Ok((
                    None,
                    EngineOutput {
                        responses: vec![out],
                        pushes: vec![],
                    },
                ));
            }
        };
        if self.store.revoked_auth_epochs.contains(&resume.auth_epoch) {
            let body = ReconnectResponse {
                value: Some(iam_protocol::pb::reconnect_response::Value::Error(
                    error_info(ErrorCode::Unauthenticated, RetryAction::Reauthenticate),
                )),
            };
            let out = self.encode_response(MsgType::ReconnectAck, frame.head.req_id, &body)?;
            return Ok((
                None,
                EngineOutput {
                    responses: vec![out],
                    pushes: vec![],
                },
            ));
        }

        let connection_id = generate_id128();
        let pushes = self.replace_device_connection(
            (resume.tenant_id, resume.user_id, device_id),
            connection_id,
        )?;
        self.store.connections.insert(
            connection_id,
            ConnState {
                connection_id,
                user_id: resume.user_id,
                tenant_id: resume.tenant_id,
                device_id,
                auth_epoch: resume.auth_epoch,
                last_req_id: frame.head.req_id.unwrap_or(0),
                push_ack: caps.iter().any(|c| c == "push_ack"),
                next_push_id: 1,
                outbound: VecDeque::new(),
                inflight_send: Default::default(),
            },
        );
        let new_resume = self.tokens.sign_resume(&ResumeToken {
            tenant_id: resume.tenant_id,
            user_id: resume.user_id,
            device_id,
            auth_epoch: resume.auth_epoch,
            issued_at_ms: self.store.now_ms,
        });
        let session = self.build_session(
            resume.tenant_id,
            resume.user_id,
            device_id,
            connection_id,
            caps,
            new_resume,
        );
        let body = ReconnectResponse {
            value: Some(iam_protocol::pb::reconnect_response::Value::Session(
                session,
            )),
        };
        let out = self.encode_response(MsgType::ReconnectAck, frame.head.req_id, &body)?;
        Ok((
            Some(connection_id),
            EngineOutput {
                responses: vec![out],
                pushes,
            },
        ))
    }

    fn handle_heartbeat(
        &mut self,
        connection_id: Option<Id128>,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let cid = match self.require_auth(connection_id, frame)? {
            AuthGate::Ok(c) => c,
            AuthGate::Respond(r) => return Ok(r),
        };
        if let Err(r) = self.check_req_id(cid, frame) {
            return Ok(r);
        }
        let body = HeartbeatAck {
            server_time_ms: Some(self.store.now_ms),
        };
        let out = self.encode_response(MsgType::HeartbeatAck, frame.head.req_id, &body)?;
        Ok((
            Some(cid),
            EngineOutput {
                responses: vec![out],
                pushes: vec![],
            },
        ))
    }

    fn handle_send(
        &mut self,
        connection_id: Option<Id128>,
        req: &SendRequest,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let cid = match self.require_auth(connection_id, frame)? {
            AuthGate::Ok(c) => c,
            AuthGate::Respond(r) => return Ok(r),
        };
        if let Err(r) = self.check_req_id(cid, frame) {
            return Ok(r);
        }
        let conn = self
            .store
            .connections
            .get(&cid)
            .cloned()
            .ok_or(CoreError::Msg("no conn"))?;

        let key = match req.key.as_ref() {
            Some(k) => k,
            None => {
                return self.wire_error(
                    frame,
                    Some(cid),
                    ErrorCode::InvalidArgument,
                    RetryAction::None,
                    false,
                    Some("missing key"),
                );
            }
        };
        let content = match req.content.as_ref() {
            Some(c) => c,
            None => {
                return self.wire_error(
                    frame,
                    Some(cid),
                    ErrorCode::InvalidArgument,
                    RetryAction::None,
                    false,
                    Some("missing content"),
                );
            }
        };
        if !matches!(content.value.as_ref(), Some(iam_protocol::pb::message_content::Value::Text(t)) if t.text.is_some())
        {
            return self.wire_error(
                frame,
                Some(cid),
                ErrorCode::InvalidArgument,
                RetryAction::None,
                false,
                None,
            );
        }

        let conversation_id = key.conversation_id.unwrap_or(0);
        if conversation_id == 0 {
            return self.wire_error(
                frame,
                Some(cid),
                ErrorCode::InvalidArgument,
                RetryAction::None,
                false,
                Some("bad conversation"),
            );
        }
        let client_msg_id = match key
            .client_msg_id
            .as_ref()
            .map(|b| validate_id128(b.as_ref()))
        {
            Some(Ok(id)) => id,
            _ => {
                return self.wire_error(
                    frame,
                    Some(cid),
                    ErrorCode::InvalidArgument,
                    RetryAction::None,
                    false,
                    Some("bad client_msg_id"),
                );
            }
        };

        if let Some(existing) = conn.inflight_send.get(&conversation_id) {
            if *existing != client_msg_id {
                return self.send_ack(
                    cid,
                    frame,
                    key,
                    unresolved(ErrorCode::InProgress, RetryAction::QuerySend, false),
                );
            }
        }
        if let Some(c) = self.store.connections.get_mut(&cid) {
            c.inflight_send.insert(conversation_id, client_msg_id);
        }

        let idem = IdemKey {
            tenant_id: conn.tenant_id,
            user_id: conn.user_id,
            device_id: conn.device_id,
            conversation_id,
            client_msg_id,
        };
        let digest = MemoryStore::content_digest(content);
        let (t, accept_until, query_until) = MemoryStore::windows_for_client_msg(&client_msg_id);
        let now = self.store.now_ms;

        if now >= query_until {
            return self.send_ack(cid, frame, key, self.expired_outcome(accept_until));
        }

        if let Some(rec) = self.store.idempotency.get(&idem).cloned() {
            return self.replay_idem(cid, frame, key, &rec, &digest);
        }

        // Clock rollback or incomplete recovery: stop new accepts.
        if self.store.clock_unreliable() || self.store.recovery_incomplete {
            return self.send_ack(
                cid,
                frame,
                key,
                unresolved(
                    ErrorCode::ResultUnavailable,
                    RetryAction::QuerySend,
                    self.store.close_watermark_ms >= accept_until,
                ),
            );
        }

        if t > now + ALLOW_SKEW_MS {
            return self.send_ack(
                cid,
                frame,
                key,
                unresolved(ErrorCode::ClockSkew, RetryAction::None, false),
            );
        }
        if now >= accept_until {
            if self.store.close_watermark_ms >= accept_until {
                let outcome = SendOutcome {
                    value: Some(send_outcome::Value::RejectedFinal(ErrorInfo {
                        code: Some(ErrorCode::RequestExpired as i32),
                        action: Some(RetryAction::None as i32),
                        retry_after_ms: None,
                        detail: None,
                    })),
                };
                self.store.idempotency.insert(
                    idem,
                    IdempotencyRecord::RejectedFinal {
                        code: ErrorCode::RequestExpired as i32,
                        digest,
                        accept_until_ms: accept_until,
                        query_until_ms: query_until,
                    },
                );
                return self.send_ack(cid, frame, key, outcome);
            }
            return self.send_ack(
                cid,
                frame,
                key,
                unresolved(ErrorCode::InProgress, RetryAction::QuerySend, false),
            );
        }

        if let Err(error) = validate_message_content(content) {
            let code = if matches!(error, ProtocolErrorKind::ResourceLimit(_)) {
                ErrorCode::ResourceLimit
            } else {
                ErrorCode::InvalidArgument
            };
            self.store.idempotency.insert(
                idem,
                IdempotencyRecord::RejectedFinal {
                    code: code as i32,
                    digest,
                    accept_until_ms: accept_until,
                    query_until_ms: query_until,
                },
            );
            return self.send_ack(
                cid,
                frame,
                key,
                SendOutcome {
                    value: Some(send_outcome::Value::RejectedFinal(error_info(
                        code,
                        RetryAction::None,
                    ))),
                },
            );
        }
        let Some(conv) = self
            .store
            .conversations
            .get_mut(&(conn.tenant_id, conversation_id))
        else {
            let outcome = SendOutcome {
                value: Some(send_outcome::Value::RejectedFinal(ErrorInfo {
                    code: Some(ErrorCode::PermissionDenied as i32),
                    action: Some(RetryAction::None as i32),
                    retry_after_ms: None,
                    detail: None,
                })),
            };
            self.store.idempotency.insert(
                idem,
                IdempotencyRecord::RejectedFinal {
                    code: ErrorCode::PermissionDenied as i32,
                    digest,
                    accept_until_ms: accept_until,
                    query_until_ms: query_until,
                },
            );
            return self.send_ack(cid, frame, key, outcome);
        };
        if !conv.members.contains(&conn.user_id) {
            let outcome = SendOutcome {
                value: Some(send_outcome::Value::RejectedFinal(ErrorInfo {
                    code: Some(ErrorCode::PermissionDenied as i32),
                    action: Some(RetryAction::None as i32),
                    retry_after_ms: None,
                    detail: None,
                })),
            };
            self.store.idempotency.insert(
                idem,
                IdempotencyRecord::RejectedFinal {
                    code: ErrorCode::PermissionDenied as i32,
                    digest,
                    accept_until_ms: accept_until,
                    query_until_ms: query_until,
                },
            );
            return self.send_ack(cid, frame, key, outcome);
        }

        let create_seq = conv.next_event_seq;
        conv.next_create_seq = create_seq + 1;
        let event_seq = conv.next_event_seq;
        conv.next_event_seq += 1;
        let history_id = conv.history_id;
        let message_id = generate_id128();
        let accepted_at = now;
        let view = active_message_view(
            conversation_id,
            message_id,
            history_id,
            create_seq,
            conn.user_id,
            accepted_at,
            content.clone(),
        );
        let change = Change {
            event_seq: Some(event_seq),
            payload: Some(change::Payload::MessageUpsert(view.clone())),
        };
        conv.events.push(crate::store::StoredEvent {
            event_seq,
            change: change.clone(),
        });
        let members: Vec<u64> = conv.members.iter().copied().collect();

        self.store.messages.insert(
            (conn.tenant_id, conversation_id, message_id),
            crate::store::StoredMessage {
                view: view.clone(),
                content_digest: digest,
            },
        );
        self.store.idempotency.insert(
            idem,
            IdempotencyRecord::Committed {
                receipt_message_id: message_id,
                create_seq,
                history_id,
                accepted_at_ms: accepted_at,
                digest,
                accept_until_ms: accept_until,
                query_until_ms: query_until,
            },
        );
        let receipt = SendReceipt {
            r#ref: Some(MessageRef {
                conversation_id: Some(conversation_id),
                message_id: Some(message_id.to_vec().into()),
            }),
            position: Some(iam_protocol::pb::MessagePosition {
                history_id: Some(history_id.to_vec().into()),
                create_seq: Some(create_seq),
            }),
            accepted_at_ms: Some(accepted_at),
        };
        let (cid_out, mut output) = self.send_ack(
            cid,
            frame,
            key,
            SendOutcome {
                value: Some(send_outcome::Value::Committed(receipt)),
            },
        )?;
        let ctx = stream_context_for_conv(
            self.store
                .conversations
                .get(&(conn.tenant_id, conversation_id))
                .unwrap(),
        );
        for uid in members {
            let targets: Vec<Id128> = self
                .store
                .connections
                .values()
                .filter(|c| {
                    c.tenant_id == conn.tenant_id && c.user_id == uid && c.connection_id != cid
                })
                .map(|c| c.connection_id)
                .collect();
            for peer_cid in targets {
                let push_id = {
                    let peer = self.store.connections.get_mut(&peer_cid).unwrap();
                    let id = peer.next_push_id;
                    peer.next_push_id += 1;
                    id
                };
                let push = Push {
                    push_id: Some(push_id),
                    context: Some(ctx.clone()),
                    changes: vec![change.clone()],
                };
                let bytes = encode_frame(MsgType::Push, &Head { req_id: None }, &push)?;
                output.pushes.push((peer_cid, bytes));
            }
        }
        Ok((cid_out, output))
    }

    fn replay_idem(
        &mut self,
        cid: Id128,
        frame: &DecodedFrame,
        key: &MessageKey,
        rec: &IdempotencyRecord,
        digest: &[u8; 32],
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        match rec {
            IdempotencyRecord::Committed {
                receipt_message_id,
                create_seq,
                history_id,
                accepted_at_ms,
                digest: d,
                ..
            } => {
                if d != digest {
                    return self.send_ack(
                        cid,
                        frame,
                        key,
                        unresolved(ErrorCode::IdempotencyConflict, RetryAction::QuerySend, true),
                    );
                }
                let conversation_id = key.conversation_id.unwrap();
                let receipt = SendReceipt {
                    r#ref: Some(MessageRef {
                        conversation_id: Some(conversation_id),
                        message_id: Some(receipt_message_id.to_vec().into()),
                    }),
                    position: Some(iam_protocol::pb::MessagePosition {
                        history_id: Some(history_id.to_vec().into()),
                        create_seq: Some(*create_seq),
                    }),
                    accepted_at_ms: Some(*accepted_at_ms),
                };
                self.send_ack(
                    cid,
                    frame,
                    key,
                    SendOutcome {
                        value: Some(send_outcome::Value::Committed(receipt)),
                    },
                )
            }
            IdempotencyRecord::RejectedFinal {
                code, digest: d, ..
            } => {
                if d != digest {
                    return self.send_ack(
                        cid,
                        frame,
                        key,
                        unresolved(ErrorCode::IdempotencyConflict, RetryAction::QuerySend, true),
                    );
                }
                self.send_ack(
                    cid,
                    frame,
                    key,
                    SendOutcome {
                        value: Some(send_outcome::Value::RejectedFinal(ErrorInfo {
                            code: Some(*code),
                            action: Some(RetryAction::None as i32),
                            retry_after_ms: None,
                            detail: None,
                        })),
                    },
                )
            }
        }
    }

    fn handle_send_status(
        &mut self,
        connection_id: Option<Id128>,
        req: &SendStatusRequest,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let cid = match self.require_auth(connection_id, frame)? {
            AuthGate::Ok(c) => c,
            AuthGate::Respond(r) => return Ok(r),
        };
        if let Err(r) = self.check_req_id(cid, frame) {
            return Ok(r);
        }
        let conn = self
            .store
            .connections
            .get(&cid)
            .cloned()
            .ok_or(CoreError::Msg("no conn"))?;
        let key = match req.key.as_ref() {
            Some(k) => k,
            None => {
                return self.wire_error(
                    frame,
                    Some(cid),
                    ErrorCode::InvalidArgument,
                    RetryAction::None,
                    false,
                    None,
                );
            }
        };
        let conversation_id = key.conversation_id.unwrap_or(0);
        if conversation_id == 0 {
            return self.wire_error(
                frame,
                Some(cid),
                ErrorCode::InvalidArgument,
                RetryAction::None,
                false,
                None,
            );
        }
        let client_msg_id = match key
            .client_msg_id
            .as_ref()
            .map(|b| validate_id128(b.as_ref()))
        {
            Some(Ok(id)) => id,
            _ => {
                return self.wire_error(
                    frame,
                    Some(cid),
                    ErrorCode::InvalidArgument,
                    RetryAction::None,
                    false,
                    None,
                );
            }
        };
        let idem = IdemKey {
            tenant_id: conn.tenant_id,
            user_id: conn.user_id,
            device_id: conn.device_id,
            conversation_id,
            client_msg_id,
        };
        let (t, accept_until, query_until) = MemoryStore::windows_for_client_msg(&client_msg_id);
        let now = self.store.now_ms;
        let outcome = if now >= query_until {
            self.expired_outcome(accept_until)
        } else if let Some(rec) = self.store.idempotency.get(&idem).cloned() {
            match rec {
                IdempotencyRecord::Committed {
                    receipt_message_id,
                    create_seq,
                    history_id,
                    accepted_at_ms,
                    ..
                } => SendOutcome {
                    value: Some(send_outcome::Value::Committed(SendReceipt {
                        r#ref: Some(MessageRef {
                            conversation_id: Some(conversation_id),
                            message_id: Some(receipt_message_id.to_vec().into()),
                        }),
                        position: Some(iam_protocol::pb::MessagePosition {
                            history_id: Some(history_id.to_vec().into()),
                            create_seq: Some(create_seq),
                        }),
                        accepted_at_ms: Some(accepted_at_ms),
                    })),
                },
                IdempotencyRecord::RejectedFinal { code, .. } => SendOutcome {
                    value: Some(send_outcome::Value::RejectedFinal(ErrorInfo {
                        code: Some(code),
                        action: Some(RetryAction::None as i32),
                        retry_after_ms: None,
                        detail: None,
                    })),
                },
            }
        } else if self.store.clock_unreliable() || self.store.recovery_incomplete {
            unresolved(
                ErrorCode::ResultUnavailable,
                RetryAction::QuerySend,
                self.store.close_watermark_ms >= accept_until,
            )
        } else if t > now + ALLOW_SKEW_MS {
            unresolved(ErrorCode::ClockSkew, RetryAction::None, false)
        } else if now < accept_until {
            unresolved(ErrorCode::NotFound, RetryAction::RetrySend, false)
        } else if self.store.close_watermark_ms >= accept_until {
            SendOutcome {
                value: Some(send_outcome::Value::RejectedFinal(ErrorInfo {
                    code: Some(ErrorCode::RequestExpired as i32),
                    action: Some(RetryAction::None as i32),
                    retry_after_ms: None,
                    detail: None,
                })),
            }
        } else {
            unresolved(ErrorCode::InProgress, RetryAction::QuerySend, false)
        };
        if should_release_send_barrier(&outcome) {
            if let Some(c) = self.store.connections.get_mut(&cid) {
                if c.inflight_send.get(&conversation_id) == Some(&client_msg_id) {
                    c.inflight_send.remove(&conversation_id);
                }
            }
        }
        let body = SendStatusResponse {
            key: Some(key.clone()),
            outcome: Some(outcome),
        };
        let out = self.encode_response(MsgType::SendStatusAck, frame.head.req_id, &body)?;
        Ok((
            Some(cid),
            EngineOutput {
                responses: vec![out],
                pushes: vec![],
            },
        ))
    }

    fn expired_outcome(&self, accept_until: i64) -> SendOutcome {
        if self.store.close_watermark_ms >= accept_until {
            unresolved(ErrorCode::ResultExpired, RetryAction::None, true)
        } else if self.store.recovery_incomplete || self.store.clock_unreliable() {
            unresolved(ErrorCode::ResultUnavailable, RetryAction::QuerySend, false)
        } else {
            unresolved(ErrorCode::InProgress, RetryAction::QuerySend, false)
        }
    }

    fn require_page_limit(
        &self,
        limit: &Option<PageLimit>,
    ) -> Result<(usize, usize), (ErrorCode, RetryAction)> {
        let Some(limit) = limit.as_ref() else {
            return Err((ErrorCode::InvalidArgument, RetryAction::None));
        };
        let items = limit.max_items.unwrap_or(0);
        let bytes = limit.max_bytes.unwrap_or(0);
        if items == 0 || bytes == 0 {
            return Err((ErrorCode::InvalidArgument, RetryAction::None));
        }
        if items > 100 || bytes > 256 * 1024 {
            return Err((ErrorCode::ResourceLimit, RetryAction::RetrySameRequest));
        }
        Ok((items as usize, bytes as usize))
    }

    fn handle_sync(
        &mut self,
        connection_id: Option<Id128>,
        req: &SyncRequest,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let cid = match self.require_auth(connection_id, frame)? {
            AuthGate::Ok(c) => c,
            AuthGate::Respond(r) => return Ok(r),
        };
        if let Err(r) = self.check_req_id(cid, frame) {
            return Ok(r);
        }
        let conn = self
            .store
            .connections
            .get(&cid)
            .cloned()
            .ok_or(CoreError::Msg("no conn"))?;

        if req.cursor.is_some() && req.page_token.is_some() {
            return self.sync_err(cid, frame, ErrorCode::InvalidArgument, RetryAction::None);
        }
        let (max_items, max_bytes) = match self.require_page_limit(&req.limit) {
            Ok(v) => v,
            Err((code, action)) => return self.sync_err(cid, frame, code, action),
        };

        let target = match req.target.as_ref().and_then(|t| t.value.as_ref()) {
            Some(v) => v,
            None => {
                return self.sync_err(cid, frame, ErrorCode::InvalidArgument, RetryAction::None)
            }
        };

        if matches!(target, sync_target::Value::Conversation(0)) {
            return self.sync_err(cid, frame, ErrorCode::InvalidArgument, RetryAction::None);
        }
        // Cold start: no cursor and no page_token => reset + snapshot
        if req.cursor.is_none() && req.page_token.is_none() {
            return self.sync_reset(cid, frame, &conn, target, ResetReason::InitialSync, false);
        }

        match target {
            sync_target::Value::Conversation(conversation_id) => self.sync_conversation_stream(
                cid,
                frame,
                &conn,
                *conversation_id,
                req,
                (max_items, max_bytes),
            ),
            sync_target::Value::Account(_) => {
                self.sync_account_stream(cid, frame, &conn, req, max_items, max_bytes)
            }
        }
    }

    fn sync_reset(
        &self,
        cid: Id128,
        frame: &DecodedFrame,
        conn: &ConnState,
        target: &sync_target::Value,
        reason: ResetReason,
        data_loss_possible: bool,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let now = self.store.now_ms;
        let snap = match target {
            sync_target::Value::Account(_) => {
                let stream = self
                    .store
                    .account_streams
                    .get(&(conn.tenant_id, conn.user_id))
                    .ok_or(CoreError::Msg("no account stream"))?;
                SnapshotToken {
                    tenant_id: conn.tenant_id,
                    user_id: conn.user_id,
                    device_id: conn.device_id,
                    is_account: true,
                    conversation_id: 0,
                    snapshot_id: generate_id128(),
                    epoch: stream.epoch,
                    access_version: 0,
                    upper_bound: stream.next_event_seq.saturating_sub(1),
                    from_create_seq: 0,
                    through_create_seq: 0,
                    scanned_items: 0,
                    created_at_ms: now,
                }
            }
            sync_target::Value::Conversation(conversation_id) => {
                let conv = match self
                    .store
                    .conversations
                    .get(&(conn.tenant_id, *conversation_id))
                {
                    Some(c) if c.members.contains(&conn.user_id) => c,
                    _ => {
                        return self.sync_err(
                            cid,
                            frame,
                            ErrorCode::PermissionDenied,
                            RetryAction::None,
                        );
                    }
                };
                let mut msgs = self.store.message_views_at(
                    conn.tenant_id,
                    *conversation_id,
                    conv.next_event_seq.saturating_sub(1),
                );
                if msgs.len() > 100 {
                    msgs.drain(..msgs.len() - 100);
                }
                let through = msgs
                    .last()
                    .and_then(|m| m.position.as_ref())
                    .and_then(|p| p.create_seq)
                    .unwrap_or(0);
                let from = if through == 0 {
                    0
                } else {
                    msgs.first()
                        .and_then(|m| m.position.as_ref())
                        .and_then(|p| p.create_seq)
                        .unwrap_or(1)
                };
                SnapshotToken {
                    tenant_id: conn.tenant_id,
                    user_id: conn.user_id,
                    device_id: conn.device_id,
                    is_account: false,
                    conversation_id: *conversation_id,
                    snapshot_id: generate_id128(),
                    epoch: conv.history_id,
                    access_version: conv.access_version,
                    upper_bound: conv.next_event_seq.saturating_sub(1),
                    from_create_seq: from,
                    through_create_seq: through,
                    scanned_items: 0,
                    created_at_ms: now,
                }
            }
        };
        let body = SyncResponse {
            value: Some(sync_response::Value::Reset(ResetRequired {
                reason: Some(reason as i32),
                snapshot_token: Some(self.tokens.sign_snapshot(&snap).into()),
                data_loss_possible: Some(data_loss_possible),
            })),
        };
        let out = self.encode_response(MsgType::SyncAck, frame.head.req_id, &body)?;
        Ok((
            Some(cid),
            EngineOutput {
                responses: vec![out],
                pushes: vec![],
            },
        ))
    }

    fn sync_conversation_stream(
        &mut self,
        cid: Id128,
        frame: &DecodedFrame,
        conn: &ConnState,
        conversation_id: u64,
        req: &SyncRequest,
        limit: (usize, usize),
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let (max_items, max_bytes) = limit;
        let conv = match self
            .store
            .conversations
            .get(&(conn.tenant_id, conversation_id))
            .cloned()
        {
            Some(c) if c.members.contains(&conn.user_id) => c,
            _ => {
                return self.sync_err(cid, frame, ErrorCode::PermissionDenied, RetryAction::None);
            }
        };
        let now = self.store.now_ms;
        let (after, upper, round_start) = if let Some(pt) = req.page_token.as_ref() {
            match self.tokens.verify_page(pt.as_ref(), now) {
                Ok(p)
                    if !p.cursor.is_account
                        && p.cursor.tenant_id == conn.tenant_id
                        && p.cursor.user_id == conn.user_id
                        && p.cursor.device_id == conn.device_id
                        && p.cursor.conversation_id == conversation_id =>
                {
                    if p.cursor.epoch != conv.history_id
                        || p.cursor.access_version != conv.access_version
                    {
                        let reason = if p.cursor.epoch != conv.history_id {
                            ResetReason::HistoryReset
                        } else {
                            ResetReason::AccessChanged
                        };
                        return self.sync_reset(
                            cid,
                            frame,
                            conn,
                            &sync_target::Value::Conversation(conversation_id),
                            reason,
                            false,
                        );
                    }
                    if p.upper_bound >= conv.next_event_seq {
                        return self.sync_reset(
                            cid,
                            frame,
                            conn,
                            &sync_target::Value::Conversation(conversation_id),
                            ResetReason::HistoryReset,
                            true,
                        );
                    }
                    (p.scanned_through, p.upper_bound, p.round_started_ms)
                }
                Ok(_) => {
                    return self.sync_err(
                        cid,
                        frame,
                        ErrorCode::InvalidToken,
                        RetryAction::ResumeSync,
                    );
                }
                Err(TokenError::Expired) => {
                    return self.sync_err(
                        cid,
                        frame,
                        ErrorCode::PageTokenExpired,
                        RetryAction::ResumeSync,
                    );
                }
                Err(TokenError::Invalid) => {
                    return self.sync_err(
                        cid,
                        frame,
                        ErrorCode::InvalidToken,
                        RetryAction::ResumeSync,
                    );
                }
            }
        } else if let Some(cur) = req.cursor.as_ref() {
            match self.tokens.verify_cursor(cur.as_ref(), now) {
                Ok(c)
                    if !c.is_account
                        && c.tenant_id == conn.tenant_id
                        && c.user_id == conn.user_id
                        && c.device_id == conn.device_id
                        && c.conversation_id == conversation_id =>
                {
                    if c.epoch != conv.history_id {
                        return self.sync_reset(
                            cid,
                            frame,
                            conn,
                            &sync_target::Value::Conversation(conversation_id),
                            ResetReason::HistoryReset,
                            false,
                        );
                    }
                    if c.access_version != conv.access_version {
                        return self.sync_reset(
                            cid,
                            frame,
                            conn,
                            &sync_target::Value::Conversation(conversation_id),
                            ResetReason::AccessChanged,
                            false,
                        );
                    }
                    let upper = conv.next_event_seq.saturating_sub(1);
                    if c.covered_through > upper {
                        return self.sync_reset(
                            cid,
                            frame,
                            conn,
                            &sync_target::Value::Conversation(conversation_id),
                            ResetReason::HistoryReset,
                            true,
                        );
                    }
                    (c.covered_through, upper, now)
                }
                Ok(_) => {
                    return self.sync_err(
                        cid,
                        frame,
                        ErrorCode::InvalidCursor,
                        RetryAction::RestartSync,
                    );
                }
                Err(TokenError::Expired) => {
                    return self.sync_reset(
                        cid,
                        frame,
                        conn,
                        &sync_target::Value::Conversation(conversation_id),
                        ResetReason::CursorExpired,
                        false,
                    );
                }
                Err(TokenError::Invalid) => {
                    return self.sync_err(
                        cid,
                        frame,
                        ErrorCode::InvalidCursor,
                        RetryAction::RestartSync,
                    );
                }
            }
        } else {
            unreachable!("cold start handled earlier");
        };

        let changes = conv
            .events
            .iter()
            .filter(|ev| ev.event_seq > after && ev.event_seq <= upper)
            .take(max_items)
            .map(|ev| ev.change.clone())
            .collect();
        self.finish_sync_page(
            cid,
            frame,
            conn,
            stream_context_for_conv(&conv),
            changes,
            SyncRound {
                after,
                upper,
                started_ms: round_start,
                max_bytes,
            },
        )
    }

    fn sync_account_stream(
        &mut self,
        cid: Id128,
        frame: &DecodedFrame,
        conn: &ConnState,
        req: &SyncRequest,
        max_items: usize,
        max_bytes: usize,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let stream = self
            .store
            .account_streams
            .get(&(conn.tenant_id, conn.user_id))
            .cloned()
            .ok_or(CoreError::Msg("no account stream"))?;
        let now = self.store.now_ms;
        let (after, upper, round_start) = if let Some(pt) = req.page_token.as_ref() {
            match self.tokens.verify_page(pt.as_ref(), now) {
                Ok(p)
                    if p.cursor.is_account
                        && p.cursor.tenant_id == conn.tenant_id
                        && p.cursor.user_id == conn.user_id
                        && p.cursor.device_id == conn.device_id =>
                {
                    if p.cursor.epoch != stream.epoch || p.upper_bound >= stream.next_event_seq {
                        return self.sync_reset(
                            cid,
                            frame,
                            conn,
                            &sync_target::Value::Account(iam_protocol::pb::Empty {}),
                            ResetReason::HistoryReset,
                            p.upper_bound >= stream.next_event_seq,
                        );
                    }
                    (p.scanned_through, p.upper_bound, p.round_started_ms)
                }
                Ok(_) => {
                    return self.sync_err(
                        cid,
                        frame,
                        ErrorCode::InvalidToken,
                        RetryAction::ResumeSync,
                    );
                }
                Err(TokenError::Expired) => {
                    return self.sync_err(
                        cid,
                        frame,
                        ErrorCode::PageTokenExpired,
                        RetryAction::ResumeSync,
                    );
                }
                Err(TokenError::Invalid) => {
                    return self.sync_err(
                        cid,
                        frame,
                        ErrorCode::InvalidToken,
                        RetryAction::ResumeSync,
                    );
                }
            }
        } else if let Some(cur) = req.cursor.as_ref() {
            match self.tokens.verify_cursor(cur.as_ref(), now) {
                Ok(c)
                    if c.is_account
                        && c.tenant_id == conn.tenant_id
                        && c.user_id == conn.user_id
                        && c.device_id == conn.device_id =>
                {
                    if c.epoch != stream.epoch {
                        return self.sync_reset(
                            cid,
                            frame,
                            conn,
                            &sync_target::Value::Account(iam_protocol::pb::Empty {}),
                            ResetReason::HistoryReset,
                            false,
                        );
                    }
                    let upper = stream.next_event_seq.saturating_sub(1);
                    if c.covered_through > upper {
                        return self.sync_reset(
                            cid,
                            frame,
                            conn,
                            &sync_target::Value::Account(iam_protocol::pb::Empty {}),
                            ResetReason::HistoryReset,
                            true,
                        );
                    }
                    (c.covered_through, upper, now)
                }
                Ok(_) => {
                    return self.sync_err(
                        cid,
                        frame,
                        ErrorCode::InvalidCursor,
                        RetryAction::RestartSync,
                    );
                }
                Err(TokenError::Expired) => {
                    return self.sync_reset(
                        cid,
                        frame,
                        conn,
                        &sync_target::Value::Account(iam_protocol::pb::Empty {}),
                        ResetReason::CursorExpired,
                        false,
                    );
                }
                Err(TokenError::Invalid) => {
                    return self.sync_err(
                        cid,
                        frame,
                        ErrorCode::InvalidCursor,
                        RetryAction::RestartSync,
                    );
                }
            }
        } else {
            unreachable!("cold start handled earlier");
        };

        let changes = stream
            .events
            .iter()
            .filter(|ev| ev.event_seq > after && ev.event_seq <= upper)
            .take(max_items)
            .map(|ev| ev.change.clone())
            .collect();
        self.finish_sync_page(
            cid,
            frame,
            conn,
            stream_context_account(stream.epoch),
            changes,
            SyncRound {
                after,
                upper,
                started_ms: round_start,
                max_bytes,
            },
        )
    }

    fn finish_sync_page(
        &self,
        cid: Id128,
        frame: &DecodedFrame,
        conn: &ConnState,
        context: iam_protocol::pb::StreamContext,
        mut changes: Vec<Change>,
        round: SyncRound,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let SyncRound {
            after,
            upper,
            started_ms: round_start,
            max_bytes,
        } = round;
        let had_changes = !changes.is_empty();
        loop {
            if had_changes && changes.is_empty() {
                return self.sync_err(
                    cid,
                    frame,
                    ErrorCode::PageBudgetTooSmall,
                    RetryAction::RetrySameRequest,
                );
            }
            let covered = changes
                .last()
                .and_then(|c| c.event_seq)
                .unwrap_or(upper.max(after));
            let is_account = matches!(
                context.target.as_ref().and_then(|t| t.value.as_ref()),
                Some(sync_target::Value::Account(_))
            );
            let conversation_id = match context.target.as_ref().and_then(|t| t.value.as_ref()) {
                Some(sync_target::Value::Conversation(id)) => *id,
                _ => 0,
            };
            let cursor = SyncCursor {
                tenant_id: conn.tenant_id,
                user_id: conn.user_id,
                device_id: conn.device_id,
                conversation_id,
                epoch: validate_id128(context.epoch.as_ref().unwrap())?,
                access_version: context.access_version.unwrap_or(0),
                covered_through: covered,
                issued_at_ms: self.store.now_ms,
                is_account,
            };
            let next_page_token = (covered < upper).then(|| {
                self.tokens
                    .sign_page(&SyncPageToken {
                        cursor: cursor.clone(),
                        upper_bound: upper,
                        scanned_through: covered,
                        round_started_ms: round_start,
                    })
                    .into()
            });
            let body = SyncResponse {
                value: Some(sync_response::Value::Page(SyncPage {
                    context: Some(context.clone()),
                    changes: changes.clone(),
                    covered_through: Some(covered),
                    upper_bound: Some(upper),
                    next_cursor: Some(self.tokens.sign_cursor(&cursor).into()),
                    next_page_token,
                })),
            };
            if body.encoded_len() <= max_bytes {
                let out = self.encode_response(MsgType::SyncAck, frame.head.req_id, &body)?;
                return Ok((
                    Some(cid),
                    EngineOutput {
                        responses: vec![out],
                        pushes: vec![],
                    },
                ));
            }
            if changes.pop().is_none() {
                return self.sync_err(
                    cid,
                    frame,
                    ErrorCode::PageBudgetTooSmall,
                    RetryAction::RetrySameRequest,
                );
            }
        }
    }

    fn handle_snapshot(
        &mut self,
        connection_id: Option<Id128>,
        req: &SnapshotRequest,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let cid = match self.require_auth(connection_id, frame)? {
            AuthGate::Ok(c) => c,
            AuthGate::Respond(r) => return Ok(r),
        };
        if let Err(r) = self.check_req_id(cid, frame) {
            return Ok(r);
        }
        let conn = self
            .store
            .connections
            .get(&cid)
            .cloned()
            .ok_or(CoreError::Msg("no conn"))?;
        if let Err((code, action)) = self.require_page_limit(&req.limit) {
            return self.snapshot_err(cid, frame, code, action);
        }
        let max_items = req.limit.as_ref().unwrap().max_items.unwrap() as usize;
        let token_bytes = match req.page_token.as_ref() {
            Some(t) => t.as_ref(),
            None => {
                return self.snapshot_err(cid, frame, ErrorCode::InvalidArgument, RetryAction::None)
            }
        };
        let mut snap = match self.tokens.verify_snapshot(token_bytes, self.store.now_ms) {
            Ok(s)
                if s.tenant_id == conn.tenant_id
                    && s.user_id == conn.user_id
                    && s.device_id == conn.device_id =>
            {
                s
            }
            Ok(_) => {
                return self.snapshot_err(
                    cid,
                    frame,
                    ErrorCode::InvalidToken,
                    RetryAction::RestartSync,
                );
            }
            Err(TokenError::Expired) => {
                return self.snapshot_err(
                    cid,
                    frame,
                    ErrorCode::PageTokenExpired,
                    RetryAction::RestartSync,
                );
            }
            Err(TokenError::Invalid) => {
                return self.snapshot_err(
                    cid,
                    frame,
                    ErrorCode::InvalidToken,
                    RetryAction::RestartSync,
                );
            }
        };

        let (context, history_window, all_items) = if snap.is_account {
            let stream = &self.store.account_streams[&(conn.tenant_id, conn.user_id)];
            if stream.epoch != snap.epoch {
                return self.snapshot_err(
                    cid,
                    frame,
                    ErrorCode::ContextChanged,
                    RetryAction::RestartSync,
                );
            }
            let mut states = std::collections::BTreeMap::new();
            for event in stream
                .events
                .iter()
                .filter(|e| e.event_seq <= snap.upper_bound)
            {
                match event.change.payload.as_ref() {
                    Some(change::Payload::ConversationUpsert(state)) => {
                        states.insert(state.conversation_id.unwrap(), state.clone());
                    }
                    Some(change::Payload::ConversationRemove(id)) => {
                        states.remove(id);
                    }
                    _ => {}
                }
            }
            // Never disclose a frozen view after its access context has been revoked.
            for (id, state) in &states {
                match self.store.conversations.get(&(conn.tenant_id, *id)) {
                    Some(c)
                        if c.members.contains(&conn.user_id)
                            && state.history_id.as_deref() == Some(c.history_id.as_slice())
                            && state.access_version == Some(c.access_version) => {}
                    _ => {
                        return self.snapshot_err(
                            cid,
                            frame,
                            ErrorCode::ContextChanged,
                            RetryAction::RestartSync,
                        )
                    }
                }
            }
            let items = states
                .into_values()
                .map(|v| iam_protocol::pb::SnapshotItem {
                    value: Some(snapshot_item::Value::Conversation(v)),
                })
                .collect::<Vec<_>>();
            (stream_context_account(stream.epoch), None, items)
        } else {
            let conv = match self
                .store
                .conversations
                .get(&(conn.tenant_id, snap.conversation_id))
            {
                Some(c) if c.members.contains(&conn.user_id) => c,
                _ => {
                    return self.snapshot_err(
                        cid,
                        frame,
                        ErrorCode::PermissionDenied,
                        RetryAction::None,
                    )
                }
            };
            if conv.history_id != snap.epoch || conv.access_version != snap.access_version {
                return self.snapshot_err(
                    cid,
                    frame,
                    ErrorCode::ContextChanged,
                    RetryAction::RestartSync,
                );
            }
            let items = self
                .store
                .message_views_at(conn.tenant_id, snap.conversation_id, snap.upper_bound)
                .into_iter()
                .filter(|m| {
                    m.position
                        .as_ref()
                        .and_then(|p| p.create_seq)
                        .is_some_and(|seq| {
                            seq >= snap.from_create_seq && seq <= snap.through_create_seq
                        })
                })
                .map(|m| iam_protocol::pb::SnapshotItem {
                    value: Some(snapshot_item::Value::Message(m)),
                })
                .collect::<Vec<_>>();
            (
                stream_context_for_conv(conv),
                Some(iam_protocol::pb::HistoryWindow {
                    from_create_seq: Some(snap.from_create_seq),
                    through_create_seq: Some(snap.through_create_seq),
                }),
                items,
            )
        };
        let start = usize::try_from(snap.scanned_items).unwrap_or(usize::MAX);
        if start > all_items.len() {
            return self.snapshot_err(
                cid,
                frame,
                ErrorCode::InvalidToken,
                RetryAction::RestartSync,
            );
        }
        let max_bytes = req.limit.as_ref().unwrap().max_bytes.unwrap() as usize;
        let mut end = start.saturating_add(max_items).min(all_items.len());
        loop {
            snap.scanned_items = end as u64;
            let next_page_token =
                (end < all_items.len()).then(|| self.tokens.sign_snapshot(&snap).into());
            let resume_cursor = (end == all_items.len()).then(|| {
                self.tokens
                    .sign_cursor(&SyncCursor {
                        tenant_id: conn.tenant_id,
                        user_id: conn.user_id,
                        device_id: conn.device_id,
                        conversation_id: snap.conversation_id,
                        epoch: snap.epoch,
                        access_version: snap.access_version,
                        covered_through: snap.upper_bound,
                        issued_at_ms: self.store.now_ms,
                        is_account: snap.is_account,
                    })
                    .into()
            });
            let body = SnapshotResponse {
                value: Some(snapshot_response::Value::Page(SnapshotPage {
                    snapshot_id: Some(snap.snapshot_id.to_vec().into()),
                    context: Some(context.clone()),
                    upper_bound: Some(snap.upper_bound),
                    history_window,
                    items: all_items[start..end].to_vec(),
                    next_page_token,
                    resume_cursor,
                })),
            };
            if body.encoded_len() <= max_bytes && (end > start || start == all_items.len()) {
                let out = self.encode_response(MsgType::SnapshotAck, frame.head.req_id, &body)?;
                return Ok((
                    Some(cid),
                    EngineOutput {
                        responses: vec![out],
                        pushes: vec![],
                    },
                ));
            }
            if end == start {
                return self.snapshot_err(
                    cid,
                    frame,
                    ErrorCode::PageBudgetTooSmall,
                    RetryAction::RetrySameRequest,
                );
            }
            end -= 1;
        }
    }

    fn handle_history(
        &mut self,
        connection_id: Option<Id128>,
        req: &HistoryRequest,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let cid = match self.require_auth(connection_id, frame)? {
            AuthGate::Ok(c) => c,
            AuthGate::Respond(r) => return Ok(r),
        };
        if let Err(r) = self.check_req_id(cid, frame) {
            return Ok(r);
        }
        let conn = self
            .store
            .connections
            .get(&cid)
            .cloned()
            .ok_or(CoreError::Msg("no conn"))?;
        if let Err((code, action)) = self.require_page_limit(&req.limit) {
            return self.history_err(cid, frame, code, action);
        }
        let max_items = req.limit.as_ref().unwrap().max_items.unwrap() as usize;
        if req.page_token.is_some() && req.before_create_seq.is_some() {
            return self.history_err(cid, frame, ErrorCode::InvalidArgument, RetryAction::None);
        }
        let conversation_id = match req
            .context
            .as_ref()
            .and_then(|c| c.target.as_ref())
            .and_then(|t| t.value.as_ref())
        {
            Some(sync_target::Value::Conversation(id)) => *id,
            _ => {
                return self.history_err(cid, frame, ErrorCode::InvalidArgument, RetryAction::None)
            }
        };
        let conv = match self
            .store
            .conversations
            .get(&(conn.tenant_id, conversation_id))
            .cloned()
        {
            Some(c) if c.members.contains(&conn.user_id) => c,
            _ => {
                return self.history_err(
                    cid,
                    frame,
                    ErrorCode::PermissionDenied,
                    RetryAction::None,
                );
            }
        };
        let context = req.context.as_ref().unwrap();
        if context
            .epoch
            .as_ref()
            .is_none_or(|v| validate_id128(v).is_err())
            || context.access_version.unwrap_or(0) == 0
        {
            return self.history_err(cid, frame, ErrorCode::InvalidArgument, RetryAction::None);
        }
        if context.epoch.as_deref() != Some(conv.history_id.as_slice())
            || context.access_version != Some(conv.access_version)
        {
            return self.history_err(
                cid,
                frame,
                ErrorCode::ContextChanged,
                RetryAction::RestartSync,
            );
        }
        let now = self.store.now_ms;
        let (before, upper, retention_floor, round_started, scanned) = if let Some(pt) =
            req.page_token.as_ref()
        {
            match self.tokens.verify_history(pt.as_ref(), now) {
                Ok(h)
                    if h.tenant_id == conn.tenant_id
                        && h.user_id == conn.user_id
                        && h.device_id == conn.device_id
                        && h.conversation_id == conversation_id =>
                {
                    if h.epoch != conv.history_id || h.access_version != conv.access_version {
                        return self.history_err(
                            cid,
                            frame,
                            ErrorCode::ContextChanged,
                            RetryAction::RestartSync,
                        );
                    }
                    if h.upper_bound >= conv.next_event_seq {
                        return self.history_err(
                            cid,
                            frame,
                            ErrorCode::PageTokenExpired,
                            RetryAction::RestartHistory,
                        );
                    }
                    (
                        h.scanned_through_seq,
                        h.upper_bound,
                        h.retention_floor_seq,
                        h.round_started_ms,
                        true,
                    )
                }
                Ok(_) => {
                    return self.history_err(
                        cid,
                        frame,
                        ErrorCode::InvalidToken,
                        RetryAction::RestartHistory,
                    );
                }
                Err(TokenError::Expired) => {
                    return self.history_err(
                        cid,
                        frame,
                        ErrorCode::PageTokenExpired,
                        RetryAction::RestartHistory,
                    );
                }
                Err(TokenError::Invalid) => {
                    return self.history_err(
                        cid,
                        frame,
                        ErrorCode::InvalidToken,
                        RetryAction::RestartHistory,
                    );
                }
            }
        } else {
            let before = req.before_create_seq.unwrap_or(u64::MAX);
            if before == 0 {
                return self.history_err(cid, frame, ErrorCode::InvalidArgument, RetryAction::None);
            }
            let upper = conv.next_event_seq.saturating_sub(1);
            (before, upper, 1u64, now, false)
        };
        let _ = scanned;
        let candidates: Vec<_> = self
            .store
            .message_views_at(conn.tenant_id, conversation_id, upper)
            .into_iter()
            .rev()
            .filter(|m| {
                m.position
                    .as_ref()
                    .and_then(|p| p.create_seq)
                    .is_some_and(|seq| seq < before && seq >= retention_floor)
            })
            .collect();
        let max_bytes = req.limit.as_ref().unwrap().max_bytes.unwrap() as usize;
        let mut count = candidates.len().min(max_items);
        loop {
            let next_page_token = if count > 0 && count < candidates.len() {
                Some(
                    self.tokens
                        .sign_history(&HistoryToken {
                            tenant_id: conn.tenant_id,
                            user_id: conn.user_id,
                            device_id: conn.device_id,
                            conversation_id,
                            epoch: conv.history_id,
                            access_version: conv.access_version,
                            before_create_seq: before,
                            upper_bound: upper,
                            retention_floor_seq: retention_floor,
                            scanned_through_seq: candidates[count - 1]
                                .position
                                .as_ref()
                                .unwrap()
                                .create_seq
                                .unwrap(),
                            round_started_ms: round_started,
                        })
                        .into(),
                )
            } else {
                None
            };
            let body = HistoryResponse {
                value: Some(iam_protocol::pb::history_response::Value::Page(
                    HistoryPage {
                        context: Some(stream_context_for_conv(&conv)),
                        upper_bound: Some(upper),
                        retention_floor_seq: Some(retention_floor),
                        messages: candidates[..count].to_vec(),
                        next_page_token,
                    },
                )),
            };
            if body.encoded_len() <= max_bytes && (count > 0 || candidates.is_empty()) {
                let out = self.encode_response(MsgType::HistoryAck, frame.head.req_id, &body)?;
                return Ok((
                    Some(cid),
                    EngineOutput {
                        responses: vec![out],
                        pushes: vec![],
                    },
                ));
            }
            if count == 0 {
                return self.history_err(
                    cid,
                    frame,
                    ErrorCode::PageBudgetTooSmall,
                    RetryAction::RetrySameRequest,
                );
            }
            count -= 1;
        }
    }

    fn handle_push_ack(
        &mut self,
        connection_id: Option<Id128>,
        push_id: Option<u64>,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let cid = match self.require_auth(connection_id, frame)? {
            AuthGate::Ok(c) => c,
            AuthGate::Respond(r) => return Ok(r),
        };
        let conn = self
            .store
            .connections
            .get(&cid)
            .ok_or(CoreError::Msg("no conn"))?;
        if !conn.push_ack {
            return self.wire_error(
                frame,
                Some(cid),
                ErrorCode::UnsupportedCapability,
                RetryAction::None,
                false,
                None,
            );
        }
        if push_id.unwrap_or(0) == 0 {
            return self.wire_error(
                frame,
                Some(cid),
                ErrorCode::InvalidArgument,
                RetryAction::None,
                false,
                None,
            );
        }
        Ok((Some(cid), EngineOutput::default()))
    }

    fn require_auth(
        &mut self,
        connection_id: Option<Id128>,
        frame: &DecodedFrame,
    ) -> Result<AuthGate, CoreError> {
        let Some(cid) = connection_id else {
            let out = self.wire_error(
                frame,
                None,
                ErrorCode::Unauthenticated,
                RetryAction::Reauthenticate,
                false,
                None,
            )?;
            return Ok(AuthGate::Respond(out));
        };
        if !self.store.connections.contains_key(&cid) {
            let out = self.wire_error(
                frame,
                None,
                ErrorCode::Unauthenticated,
                RetryAction::Reauthenticate,
                false,
                None,
            )?;
            return Ok(AuthGate::Respond(out));
        }
        if self
            .store
            .revoked_auth_epochs
            .contains(&self.store.connections[&cid].auth_epoch)
        {
            return Ok(AuthGate::Respond(self.wire_error(
                frame,
                Some(cid),
                ErrorCode::Unauthenticated,
                RetryAction::Reauthenticate,
                true,
                None,
            )?));
        }
        Ok(AuthGate::Ok(cid))
    }

    fn sync_err(
        &self,
        cid: Id128,
        frame: &DecodedFrame,
        code: ErrorCode,
        action: RetryAction,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let body = SyncResponse {
            value: Some(sync_response::Value::Error(error_info(code, action))),
        };
        let out = self.encode_response(MsgType::SyncAck, frame.head.req_id, &body)?;
        Ok((
            Some(cid),
            EngineOutput {
                responses: vec![out],
                pushes: vec![],
            },
        ))
    }

    fn snapshot_err(
        &self,
        cid: Id128,
        frame: &DecodedFrame,
        code: ErrorCode,
        action: RetryAction,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let body = SnapshotResponse {
            value: Some(snapshot_response::Value::Error(error_info(code, action))),
        };
        let out = self.encode_response(MsgType::SnapshotAck, frame.head.req_id, &body)?;
        Ok((
            Some(cid),
            EngineOutput {
                responses: vec![out],
                pushes: vec![],
            },
        ))
    }

    fn history_err(
        &self,
        cid: Id128,
        frame: &DecodedFrame,
        code: ErrorCode,
        action: RetryAction,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let body = HistoryResponse {
            value: Some(iam_protocol::pb::history_response::Value::Error(
                error_info(code, action),
            )),
        };
        let out = self.encode_response(MsgType::HistoryAck, frame.head.req_id, &body)?;
        Ok((
            Some(cid),
            EngineOutput {
                responses: vec![out],
                pushes: vec![],
            },
        ))
    }

    fn send_ack(
        &mut self,
        cid: Id128,
        frame: &DecodedFrame,
        key: &MessageKey,
        outcome: SendOutcome,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        if let Some(conversation_id) = key.conversation_id {
            if should_release_send_barrier(&outcome) {
                if let Some(c) = self.store.connections.get_mut(&cid) {
                    if c.inflight_send
                        .get(&conversation_id)
                        .map(|id| id.as_slice())
                        == key.client_msg_id.as_deref()
                    {
                        c.inflight_send.remove(&conversation_id);
                    }
                }
            }
        }
        let body = SendResponse {
            key: Some(key.clone()),
            outcome: Some(outcome),
        };
        let out = self.encode_response(MsgType::SendAck, frame.head.req_id, &body)?;
        Ok((
            Some(cid),
            EngineOutput {
                responses: vec![out],
                pushes: vec![],
            },
        ))
    }

    fn check_req_id(
        &mut self,
        cid: Id128,
        frame: &DecodedFrame,
    ) -> Result<(), (Option<Id128>, EngineOutput)> {
        let req_id = match frame.head.req_id {
            Some(r) if r != 0 => r,
            _ => {
                return Err(self
                    .wire_error(
                        frame,
                        Some(cid),
                        ErrorCode::InvalidArgument,
                        RetryAction::None,
                        true,
                        Some("req_id"),
                    )
                    .unwrap());
            }
        };
        let conn = self.store.connections.get_mut(&cid).unwrap();
        if req_id <= conn.last_req_id {
            return Err(self
                .wire_error(
                    frame,
                    Some(cid),
                    ErrorCode::InvalidArgument,
                    RetryAction::None,
                    true,
                    Some("req_id not increasing"),
                )
                .unwrap());
        }
        conn.last_req_id = req_id;
        Ok(())
    }

    fn encode_response(
        &self,
        msg_type: MsgType,
        req_id: Option<u64>,
        body: &impl Message,
    ) -> Result<Vec<u8>, CoreError> {
        let head = Head { req_id };
        Ok(encode_frame(msg_type, &head, body)?)
    }
}

enum AuthGate {
    Ok(Id128),
    Respond((Option<Id128>, EngineOutput)),
}

fn default_limits() -> ProtocolLimits {
    ProtocolLimits {
        max_frame_bytes: Some(1024 * 1024),
        max_text_bytes: Some(64 * 1024),
        max_page_items: Some(100),
        max_page_bytes: Some(256 * 1024),
        max_inflight_requests: Some(32),
        heartbeat_interval_ms: Some(30_000),
        idle_timeout_ms: Some(90_000),
    }
}

fn should_release_send_barrier(outcome: &SendOutcome) -> bool {
    match outcome.value.as_ref() {
        Some(send_outcome::Value::Committed(_)) | Some(send_outcome::Value::RejectedFinal(_)) => {
            true
        }
        Some(send_outcome::Value::Unresolved(u)) => u.acceptance_closed == Some(true),
        None => false,
    }
}

fn unresolved(code: ErrorCode, action: RetryAction, closed: bool) -> SendOutcome {
    SendOutcome {
        value: Some(send_outcome::Value::Unresolved(SendUnresolved {
            error: Some(ErrorInfo {
                code: Some(code as i32),
                action: Some(action as i32),
                retry_after_ms: None,
                detail: None,
            }),
            acceptance_closed: Some(closed),
        })),
    }
}
