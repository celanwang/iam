use crate::store::{
    active_message_view, stream_context_for_conv, ConnState, IdemKey, IdempotencyRecord,
    MemoryStore, ALLOW_SKEW_MS,
};
use crate::token::{SyncCursor, SyncPageToken, TokenError, TokenSigner};
use iam_protocol::pb::{
    change, send_outcome, sync_response, sync_target, AuthRequest, AuthResponse, Change, ErrorCode,
    ErrorInfo, Head, HeartbeatAck, MessageKey, MessageRef, PageLimit, ProtocolLimits, Push,
    RetryAction, SendOutcome, SendReceipt, SendRequest, SendResponse, SendStatusRequest,
    SendStatusResponse, SendUnresolved, Session, SyncPage, SyncRequest, SyncResponse,
};
use iam_protocol::{
    encode_frame, generate_id128, validate_id128, validate_message_content, DecodedBody,
    DecodedFrame, Id128, MsgType, ProtocolErrorKind,
};
use prost::Message;
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
    /// Frames to deliver to the requesting connection.
    pub responses: Vec<Vec<u8>>,
    /// Frames to deliver to other connections: (connection_id, frame)
    pub pushes: Vec<(Id128, Vec<u8>)>,
}

pub struct Engine {
    pub store: MemoryStore,
    pub tokens: TokenSigner,
}

impl Engine {
    pub fn new(now_ms: i64) -> Self {
        Self {
            store: MemoryStore::new(now_ms),
            tokens: TokenSigner::new([7u8; 32]),
        }
    }

    pub fn handle(
        &mut self,
        connection_id: Option<Id128>,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        match &frame.body {
            DecodedBody::Auth(req) => self.handle_auth(req, frame),
            DecodedBody::Heartbeat(_) => self.handle_heartbeat(connection_id, frame),
            DecodedBody::Send(req) => self.handle_send(connection_id, req, frame),
            DecodedBody::SendStatus(req) => self.handle_send_status(connection_id, req, frame),
            DecodedBody::Sync(req) => self.handle_sync(connection_id, req, frame),
            DecodedBody::PushAck(ack) => self.handle_push_ack(connection_id, ack.push_id, frame),
            _ => Err(CoreError::Msg("unsupported in this phase")),
        }
    }

    fn handle_auth(
        &mut self,
        req: &AuthRequest,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let token = req
            .access_token
            .as_ref()
            .ok_or(CoreError::Msg("missing token"))?;
        let device = req
            .device_id
            .as_ref()
            .ok_or(CoreError::Msg("missing device"))?;
        let device_id = validate_id128(device)?;
        let user = self
            .store
            .users_by_token
            .get(token.as_ref())
            .cloned()
            .ok_or(CoreError::Msg("bad token"))?;

        let mut caps: Vec<String> = req
            .requested_capabilities
            .iter()
            .filter(|c| *c == "push_ack")
            .cloned()
            .collect();
        caps.sort();
        caps.dedup();

        let connection_id = generate_id128();
        if let Some(old) = self.store.device_conn.insert(device_id, connection_id) {
            self.store.connections.remove(&old);
        }
        self.store.connections.insert(
            connection_id,
            ConnState {
                connection_id,
                user_id: user.user_id,
                tenant_id: user.tenant_id,
                device_id,
                last_req_id: 0,
                push_ack: caps.iter().any(|c| c == "push_ack"),
                next_push_id: 1,
                outbound: VecDeque::new(),
                inflight_send: Default::default(),
            },
        );

        let session = Session {
            tenant_id: Some(user.tenant_id),
            user_id: Some(user.user_id),
            device_id: Some(device_id.to_vec().into()),
            connection_id: Some(connection_id.to_vec().into()),
            resume_token: Some(b"resume".to_vec().into()),
            enabled_capabilities: caps,
            server_time_ms: Some(self.store.now_ms),
            limits: Some(default_limits()),
        };
        let body = AuthResponse {
            value: Some(iam_protocol::pb::auth_response::Value::Session(session)),
        };
        let out = self.encode_response(MsgType::AuthAck, frame.head.req_id, &body)?;
        Ok((Some(connection_id), EngineOutput { responses: vec![out], pushes: vec![] }))
    }

    fn handle_heartbeat(
        &mut self,
        connection_id: Option<Id128>,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let cid = connection_id.ok_or(CoreError::Msg("unauthenticated"))?;
        self.check_req_id(cid, frame)?;
        let body = HeartbeatAck {
            server_time_ms: Some(self.store.now_ms),
        };
        let out = self.encode_response(MsgType::HeartbeatAck, frame.head.req_id, &body)?;
        Ok((Some(cid), EngineOutput { responses: vec![out], pushes: vec![] }))
    }

    fn handle_send(
        &mut self,
        connection_id: Option<Id128>,
        req: &SendRequest,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let cid = connection_id.ok_or(CoreError::Msg("unauthenticated"))?;
        self.check_req_id(cid, frame)?;
        let conn = self
            .store
            .connections
            .get(&cid)
            .cloned()
            .ok_or(CoreError::Msg("no conn"))?;

        let key = req.key.as_ref().ok_or(CoreError::Msg("missing key"))?;
        let content = req
            .content
            .as_ref()
            .ok_or(CoreError::Msg("missing content"))?;
        validate_message_content(content)?;

        let conversation_id = key.conversation_id.unwrap_or(0);
        if conversation_id == 0 {
            return Err(CoreError::Msg("bad conversation"));
        }
        let client_msg_id = validate_id128(key.client_msg_id.as_ref().map(|b| b.as_ref()).unwrap_or(&[]))?;

        // serial barrier: same device/conversation allows only one unresolved submit key
        if let Some(existing) = conn.inflight_send.get(&conversation_id) {
            if *existing != client_msg_id {
                let outcome = unresolved(
                    ErrorCode::InProgress,
                    RetryAction::QuerySend,
                    false,
                );
                return self.send_ack(cid, frame, key, outcome);
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

        if let Some(rec) = self.store.idempotency.get(&idem).cloned() {
            return self.replay_idem(cid, frame, key, &rec, &digest, now, query_until);
        }

        if t > now + ALLOW_SKEW_MS {
            let outcome = unresolved(ErrorCode::ClockSkew, RetryAction::None, false);
            return self.send_ack(cid, frame, key, outcome);
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
            let outcome = unresolved(ErrorCode::InProgress, RetryAction::QuerySend, false);
            return self.send_ack(cid, frame, key, outcome);
        }

        let conv = self
            .store
            .conversations
            .get_mut(&conversation_id)
            .ok_or(CoreError::Msg("no conversation"))?;
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

        let create_seq = conv.next_create_seq;
        conv.next_create_seq += 1;
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
        let access_version = conv.access_version;
        let members: Vec<u64> = conv.members.iter().copied().collect();

        self.store.messages.insert(
            (conversation_id, message_id),
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
        let outcome = SendOutcome {
            value: Some(send_outcome::Value::Committed(receipt)),
        };

        let (cid_out, mut output) = self.send_ack(cid, frame, key, outcome)?;
        // push to other online members
        let ctx = stream_context_for_conv(self.store.conversations.get(&conversation_id).unwrap());
        for uid in members {
            if uid == conn.user_id {
                continue;
            }
            let targets: Vec<(Id128, bool)> = self
                .store
                .connections
                .values()
                .filter(|c| c.user_id == uid)
                .map(|c| (c.connection_id, c.push_ack))
                .collect();
            for (peer_cid, _) in targets {
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
                let head = Head { req_id: None };
                let bytes = encode_frame(MsgType::Push, &head, &push)?;
                output.pushes.push((peer_cid, bytes));
            }
        }
        let _ = access_version;
        Ok((cid_out, output))
    }

    fn replay_idem(
        &mut self,
        cid: Id128,
        frame: &DecodedFrame,
        key: &MessageKey,
        rec: &IdempotencyRecord,
        digest: &[u8; 32],
        now: i64,
        _query_until: i64,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        match rec {
            IdempotencyRecord::Committed {
                receipt_message_id,
                create_seq,
                history_id,
                accepted_at_ms,
                digest: d,
                query_until_ms,
                ..
            } => {
                if now >= *query_until_ms && self.store.close_watermark_ms >= *query_until_ms {
                    let outcome = unresolved(ErrorCode::ResultExpired, RetryAction::None, true);
                    return self.send_ack(cid, frame, key, outcome);
                }
                if d != digest {
                    let outcome =
                        unresolved(ErrorCode::IdempotencyConflict, RetryAction::QuerySend, true);
                    return self.send_ack(cid, frame, key, outcome);
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
                let outcome = SendOutcome {
                    value: Some(send_outcome::Value::Committed(receipt)),
                };
                self.send_ack(cid, frame, key, outcome)
            }
            IdempotencyRecord::RejectedFinal {
                code,
                digest: d,
                query_until_ms,
                ..
            } => {
                if d != digest {
                    let outcome =
                        unresolved(ErrorCode::IdempotencyConflict, RetryAction::QuerySend, true);
                    return self.send_ack(cid, frame, key, outcome);
                }
                if now >= *query_until_ms {
                    let outcome = unresolved(ErrorCode::ResultExpired, RetryAction::None, true);
                    return self.send_ack(cid, frame, key, outcome);
                }
                let outcome = SendOutcome {
                    value: Some(send_outcome::Value::RejectedFinal(ErrorInfo {
                        code: Some(*code),
                        action: Some(RetryAction::None as i32),
                        retry_after_ms: None,
                        detail: None,
                    })),
                };
                self.send_ack(cid, frame, key, outcome)
            }
        }
    }

    fn handle_send_status(
        &mut self,
        connection_id: Option<Id128>,
        req: &SendStatusRequest,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let cid = connection_id.ok_or(CoreError::Msg("unauthenticated"))?;
        self.check_req_id(cid, frame)?;
        let conn = self
            .store
            .connections
            .get(&cid)
            .cloned()
            .ok_or(CoreError::Msg("no conn"))?;
        let key = req.key.as_ref().ok_or(CoreError::Msg("missing key"))?;
        let conversation_id = key.conversation_id.unwrap_or(0);
        let client_msg_id = validate_id128(key.client_msg_id.as_ref().map(|b| b.as_ref()).unwrap_or(&[]))?;
        let idem = IdemKey {
            tenant_id: conn.tenant_id,
            user_id: conn.user_id,
            device_id: conn.device_id,
            conversation_id,
            client_msg_id,
        };
        let (_, accept_until, query_until) = MemoryStore::windows_for_client_msg(&client_msg_id);
        let now = self.store.now_ms;
        let outcome = if let Some(rec) = self.store.idempotency.get(&idem).cloned() {
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
        let _ = query_until;
        let body = SendStatusResponse {
            key: Some(key.clone()),
            outcome: Some(outcome),
        };
        let out = self.encode_response(MsgType::SendStatusAck, frame.head.req_id, &body)?;
        Ok((Some(cid), EngineOutput { responses: vec![out], pushes: vec![] }))
    }

    fn handle_sync(
        &mut self,
        connection_id: Option<Id128>,
        req: &SyncRequest,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let cid = connection_id.ok_or(CoreError::Msg("unauthenticated"))?;
        self.check_req_id(cid, frame)?;
        let conn = self
            .store
            .connections
            .get(&cid)
            .cloned()
            .ok_or(CoreError::Msg("no conn"))?;
        let target = req.target.as_ref().ok_or(CoreError::Msg("missing target"))?;
        let conversation_id = match target.value.as_ref() {
            Some(sync_target::Value::Conversation(id)) => *id,
            Some(sync_target::Value::Account(_)) => {
                let body = SyncResponse {
                    value: Some(sync_response::Value::Error(ErrorInfo {
                        code: Some(ErrorCode::InvalidArgument as i32),
                        action: Some(RetryAction::None as i32),
                        retry_after_ms: None,
                        detail: Some("account sync deferred".into()),
                    })),
                };
                let out = self.encode_response(MsgType::SyncAck, frame.head.req_id, &body)?;
                return Ok((Some(cid), EngineOutput { responses: vec![out], pushes: vec![] }));
            }
            None => return Err(CoreError::Msg("empty target")),
        };

        let conv = self
            .store
            .conversations
            .get(&conversation_id)
            .ok_or(CoreError::Msg("no conversation"))?
            .clone();
        if !conv.members.contains(&conn.user_id) {
            let body = SyncResponse {
                value: Some(sync_response::Value::Error(ErrorInfo {
                    code: Some(ErrorCode::PermissionDenied as i32),
                    action: Some(RetryAction::None as i32),
                    retry_after_ms: None,
                    detail: None,
                })),
            };
            let out = self.encode_response(MsgType::SyncAck, frame.head.req_id, &body)?;
            return Ok((Some(cid), EngineOutput { responses: vec![out], pushes: vec![] }));
        }

        let now = self.store.now_ms;
        let limit = req.limit.clone().unwrap_or(PageLimit {
            max_items: Some(100),
            max_bytes: Some(256 * 1024),
        });
        let max_items = limit.max_items.unwrap_or(100) as usize;

        let (after, upper, round_start) = if let Some(pt) = req.page_token.as_ref() {
            match self.tokens.verify_page(pt.as_ref(), now) {
                Ok(p) => {
                    if p.cursor.user_id != conn.user_id
                        || p.cursor.device_id != conn.device_id
                        || p.cursor.conversation_id != conversation_id
                        || p.cursor.epoch != conv.history_id
                    {
                        return self.sync_err(cid, frame, ErrorCode::InvalidToken, RetryAction::ResumeSync);
                    }
                    (p.scanned_through, p.upper_bound, p.round_started_ms)
                }
                Err(TokenError::Expired) => {
                    return self.sync_err(cid, frame, ErrorCode::PageTokenExpired, RetryAction::ResumeSync);
                }
                Err(TokenError::Invalid) => {
                    return self.sync_err(cid, frame, ErrorCode::InvalidToken, RetryAction::ResumeSync);
                }
            }
        } else if let Some(cur) = req.cursor.as_ref() {
            match self.tokens.verify_cursor(cur.as_ref(), now) {
                Ok(c) => {
                    if c.user_id != conn.user_id
                        || c.device_id != conn.device_id
                        || c.conversation_id != conversation_id
                        || c.epoch != conv.history_id
                    {
                        return self.sync_err(cid, frame, ErrorCode::InvalidCursor, RetryAction::RestartSync);
                    }
                    let upper = conv.next_event_seq.saturating_sub(1);
                    (c.covered_through, upper, now)
                }
                Err(TokenError::Expired) => {
                    // reset required — deferred snapshot; return error for this phase
                    return self.sync_err(cid, frame, ErrorCode::InvalidCursor, RetryAction::RestartSync);
                }
                Err(TokenError::Invalid) => {
                    return self.sync_err(cid, frame, ErrorCode::InvalidCursor, RetryAction::RestartSync);
                }
            }
        } else {
            // first sync: empty cursor => start from 0 with fixed upper
            let upper = conv.next_event_seq.saturating_sub(1);
            (0, upper, now)
        };

        let mut changes = Vec::new();
        let mut covered = after;
        for ev in &conv.events {
            if ev.event_seq <= after {
                continue;
            }
            if ev.event_seq > upper {
                break;
            }
            if changes.len() >= max_items {
                break;
            }
            changes.push(ev.change.clone());
            covered = ev.event_seq;
        }
        // empty filtered: still advance if there is room under upper
        if changes.is_empty() && after < upper {
            covered = (after + max_items as u64).min(upper);
        } else if changes.is_empty() {
            covered = after.min(upper);
        }

        let next_cursor = self.tokens.sign_cursor(&SyncCursor {
            user_id: conn.user_id,
            device_id: conn.device_id,
            conversation_id,
            epoch: conv.history_id,
            access_version: conv.access_version,
            covered_through: covered,
            issued_at_ms: now,
        });
        let next_page = if covered < upper {
            Some(self.tokens.sign_page(&SyncPageToken {
                cursor: SyncCursor {
                    user_id: conn.user_id,
                    device_id: conn.device_id,
                    conversation_id,
                    epoch: conv.history_id,
                    access_version: conv.access_version,
                    covered_through: covered,
                    issued_at_ms: now,
                },
                upper_bound: upper,
                scanned_through: covered,
                round_started_ms: round_start,
            }))
        } else {
            None
        };

        let page = SyncPage {
            context: Some(stream_context_for_conv(&conv)),
            changes,
            covered_through: Some(covered),
            upper_bound: Some(upper),
            next_cursor: Some(next_cursor.into()),
            next_page_token: next_page.map(|p| p.into()),
        };
        let body = SyncResponse {
            value: Some(sync_response::Value::Page(page)),
        };
        let out = self.encode_response(MsgType::SyncAck, frame.head.req_id, &body)?;
        Ok((Some(cid), EngineOutput { responses: vec![out], pushes: vec![] }))
    }

    fn handle_push_ack(
        &mut self,
        connection_id: Option<Id128>,
        _push_id: Option<u64>,
        _frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let cid = connection_id.ok_or(CoreError::Msg("unauthenticated"))?;
        let conn = self
            .store
            .connections
            .get(&cid)
            .ok_or(CoreError::Msg("no conn"))?;
        if !conn.push_ack {
            return Err(CoreError::Msg("push_ack not enabled"));
        }
        Ok((Some(cid), EngineOutput::default()))
    }

    fn sync_err(
        &self,
        cid: Id128,
        frame: &DecodedFrame,
        code: ErrorCode,
        action: RetryAction,
    ) -> Result<(Option<Id128>, EngineOutput), CoreError> {
        let body = SyncResponse {
            value: Some(sync_response::Value::Error(ErrorInfo {
                code: Some(code as i32),
                action: Some(action as i32),
                retry_after_ms: None,
                detail: None,
            })),
        };
        let out = self.encode_response(MsgType::SyncAck, frame.head.req_id, &body)?;
        Ok((Some(cid), EngineOutput { responses: vec![out], pushes: vec![] }))
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
                    c.inflight_send.remove(&conversation_id);
                }
            }
        }
        let body = SendResponse {
            key: Some(key.clone()),
            outcome: Some(outcome),
        };
        let out = self.encode_response(MsgType::SendAck, frame.head.req_id, &body)?;
        Ok((Some(cid), EngineOutput { responses: vec![out], pushes: vec![] }))
    }

    fn check_req_id(&mut self, cid: Id128, frame: &DecodedFrame) -> Result<(), CoreError> {
        let req_id = frame
            .head
            .req_id
            .ok_or(CoreError::Msg("missing req_id"))?;
        if req_id == 0 {
            return Err(CoreError::Msg("req_id zero"));
        }
        let conn = self
            .store
            .connections
            .get_mut(&cid)
            .ok_or(CoreError::Msg("no conn"))?;
        if req_id <= conn.last_req_id {
            return Err(CoreError::Msg("req_id not increasing"));
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

use std::collections::VecDeque;
