//! In-process closed-loop helpers for protocol integration tests.

use iam_core::Engine;
use iam_protocol::pb::{
    message_content, send_outcome, snapshot_response, sync_response, sync_target, AuthRequest,
    Empty, Head, HistoryRequest, MessageContent, MessageKey, PageLimit, ReconnectRequest,
    SendRequest, SnapshotRequest, StreamContext, SyncRequest, SyncTarget, TextContent,
};
use iam_protocol::{
    decode_frame, encode_frame, generate_id128, DecodedBody, FrameDecoder, Id128, MsgType,
};

use std::collections::HashMap;

fn default_limit() -> PageLimit {
    PageLimit {
        max_items: Some(100),
        max_bytes: Some(256 * 1024),
    }
}

pub struct Peer {
    pub connection_id: Option<Id128>,
    pub device_id: Id128,
    pub resume_token: Option<Vec<u8>>,
    pub inbox: Vec<Vec<u8>>,
    next_req: u64,
}

impl Peer {
    pub fn new() -> Self {
        Self {
            connection_id: None,
            device_id: generate_id128(),
            resume_token: None,
            inbox: Vec::new(),
            next_req: 0,
        }
    }

    pub fn next_req_id(&mut self) -> u64 {
        self.next_req += 1;
        self.next_req
    }
}

pub struct LoopWorld {
    pub engine: Engine,
    pub peers: HashMap<&'static str, Peer>,
    pushes: HashMap<Id128, Vec<Vec<u8>>>,
}

impl LoopWorld {
    pub fn new(now_ms: i64) -> Self {
        let mut engine = Engine::new(now_ms);
        engine.store.seed_user(1, 100, b"token-a".to_vec());
        engine.store.seed_user(1, 200, b"token-b".to_vec());
        engine.store.seed_direct(1, 1, 100, 200);
        let mut peers = HashMap::new();
        peers.insert("a", Peer::new());
        peers.insert("b", Peer::new());
        Self {
            engine,
            peers,
            pushes: HashMap::new(),
        }
    }

    pub fn auth(&mut self, name: &'static str, token: &[u8]) {
        let (req_id, device_id) = {
            let peer = self.peers.get_mut(name).unwrap();
            (peer.next_req_id(), peer.device_id)
        };
        let body = AuthRequest {
            access_token: Some(token.to_vec().into()),
            device_id: Some(device_id.to_vec().into()),
            requested_capabilities: vec!["push_ack".into()],
        };
        let bytes = encode_frame(
            MsgType::Auth,
            &Head {
                req_id: Some(req_id),
            },
            &body,
        )
        .unwrap();
        let (cid, out) = self.dispatch(None, &bytes);
        let ack = out.responses.into_iter().next().expect("auth ack");
        let resume = match decode_body_auth(&ack) {
            Some(tok) => tok,
            None => None,
        };
        let peer = self.peers.get_mut(name).unwrap();
        peer.connection_id = cid;
        peer.resume_token = resume;
        self.collect_pushes(out.pushes);
    }

    pub fn reconnect(&mut self, name: &'static str) -> Vec<u8> {
        let (req_id, device_id, resume) = {
            let peer = self.peers.get_mut(name).unwrap();
            (
                peer.next_req_id(),
                peer.device_id,
                peer.resume_token.clone().expect("resume token"),
            )
        };
        let body = ReconnectRequest {
            resume_token: Some(resume.into()),
            device_id: Some(device_id.to_vec().into()),
            previous_connection_id: None,
            requested_capabilities: vec!["push_ack".into()],
        };
        let bytes = encode_frame(
            MsgType::Reconnect,
            &Head {
                req_id: Some(req_id),
            },
            &body,
        )
        .unwrap();
        let old_cid = self.peers.get(name).unwrap().connection_id;
        let (cid, out) = self.dispatch(None, &bytes);
        let ack = out.responses.into_iter().next().expect("reconnect ack");
        let peer = self.peers.get_mut(name).unwrap();
        peer.connection_id = cid;
        if let Some(tok) = decode_body_reconnect(&ack) {
            peer.resume_token = Some(tok);
        }
        self.collect_pushes(out.pushes);
        let _ = old_cid;
        ack
    }

    pub fn send_text(
        &mut self,
        name: &'static str,
        conversation_id: u64,
        client_msg_id: Id128,
        text: &str,
    ) -> Vec<u8> {
        let (req_id, cid) = {
            let peer = self.peers.get_mut(name).unwrap();
            (peer.next_req_id(), peer.connection_id)
        };
        let body = SendRequest {
            key: Some(MessageKey {
                conversation_id: Some(conversation_id),
                client_msg_id: Some(client_msg_id.to_vec().into()),
            }),
            content: Some(MessageContent {
                value: Some(message_content::Value::Text(TextContent {
                    text: Some(text.into()),
                })),
            }),
        };
        let bytes = encode_frame(
            MsgType::Send,
            &Head {
                req_id: Some(req_id),
            },
            &body,
        )
        .unwrap();
        let (_, out) = self.dispatch(cid, &bytes);
        let resp = out.responses.into_iter().next().expect("send ack");
        self.collect_pushes(out.pushes);
        resp
    }

    pub fn sync_conversation(
        &mut self,
        name: &'static str,
        conversation_id: u64,
        cursor: Option<Vec<u8>>,
    ) -> Vec<u8> {
        let (req_id, cid) = {
            let peer = self.peers.get_mut(name).unwrap();
            (peer.next_req_id(), peer.connection_id)
        };
        let body = SyncRequest {
            target: Some(SyncTarget {
                value: Some(sync_target::Value::Conversation(conversation_id)),
            }),
            cursor: cursor.map(|c| c.into()),
            page_token: None,
            limit: Some(default_limit()),
        };
        let bytes = encode_frame(
            MsgType::Sync,
            &Head {
                req_id: Some(req_id),
            },
            &body,
        )
        .unwrap();
        let (_, out) = self.dispatch(cid, &bytes);
        out.responses.into_iter().next().expect("sync ack")
    }

    /// Cold-start recovery: SYNC reset → SNAPSHOT → resume cursor, then incremental SYNC page.
    pub fn recover_conversation_latest(
        &mut self,
        name: &'static str,
        conversation_id: u64,
    ) -> iam_protocol::pb::SyncPage {
        let sync = self.sync_conversation(name, conversation_id, None);
        let reset = match parse_sync_ack(&sync).value.unwrap() {
            sync_response::Value::Reset(r) => r,
            other => panic!("expected reset, got {other:?}"),
        };
        let mut token = reset.snapshot_token.expect("snapshot token").to_vec();
        let resume = loop {
            let (req_id, cid) = {
                let peer = self.peers.get_mut(name).unwrap();
                (peer.next_req_id(), peer.connection_id)
            };
            let body = SnapshotRequest {
                page_token: Some(token.clone().into()),
                limit: Some(default_limit()),
            };
            let bytes = encode_frame(
                MsgType::Snapshot,
                &Head {
                    req_id: Some(req_id),
                },
                &body,
            )
            .unwrap();
            let (_, out) = self.dispatch(cid, &bytes);
            let resp = out.responses.into_iter().next().expect("snapshot ack");
            match parse_snapshot_ack(&resp).value.unwrap() {
                snapshot_response::Value::Page(page) => {
                    if let Some(next) = page.next_page_token {
                        token = next.to_vec();
                        continue;
                    }
                    break page.resume_cursor.expect("resume cursor").to_vec();
                }
                other => panic!("expected snapshot page {other:?}"),
            }
        };
        let sync2 = self.sync_conversation(name, conversation_id, Some(resume));
        match parse_sync_ack(&sync2).value.unwrap() {
            sync_response::Value::Page(page) => page,
            other => panic!("expected page after resume {other:?}"),
        }
    }

    pub fn drain_pushes(&mut self, name: &'static str) -> Vec<Vec<u8>> {
        let cid = self.peers.get(name).unwrap().connection_id.unwrap();
        self.pushes.remove(&cid).unwrap_or_default()
    }

    pub fn drop_pushes(&mut self, name: &'static str) {
        let cid = self.peers.get(name).unwrap().connection_id.unwrap();
        self.pushes.remove(&cid);
    }

    pub fn sync_account(&mut self, name: &'static str, cursor: Option<Vec<u8>>) -> Vec<u8> {
        let (req_id, cid) = {
            let peer = self.peers.get_mut(name).unwrap();
            (peer.next_req_id(), peer.connection_id)
        };
        let body = SyncRequest {
            target: Some(SyncTarget {
                value: Some(sync_target::Value::Account(Empty {})),
            }),
            cursor: cursor.map(|c| c.into()),
            page_token: None,
            limit: Some(default_limit()),
        };
        let bytes = encode_frame(
            MsgType::Sync,
            &Head {
                req_id: Some(req_id),
            },
            &body,
        )
        .unwrap();
        let (_, out) = self.dispatch(cid, &bytes);
        out.responses.into_iter().next().expect("sync ack")
    }

    pub fn history(
        &mut self,
        name: &'static str,
        conversation_id: u64,
        before_create_seq: Option<u64>,
    ) -> Vec<u8> {
        let (req_id, cid) = {
            let peer = self.peers.get_mut(name).unwrap();
            (peer.next_req_id(), peer.connection_id)
        };
        let conv = self
            .engine
            .store
            .conversations
            .get(&(1, conversation_id))
            .unwrap();
        let body = HistoryRequest {
            context: Some(StreamContext {
                target: Some(SyncTarget {
                    value: Some(sync_target::Value::Conversation(conversation_id)),
                }),
                epoch: Some(conv.history_id.to_vec().into()),
                access_version: Some(conv.access_version),
            }),
            before_create_seq,
            page_token: None,
            limit: Some(default_limit()),
        };
        let bytes = encode_frame(
            MsgType::History,
            &Head {
                req_id: Some(req_id),
            },
            &body,
        )
        .unwrap();
        let (_, out) = self.dispatch(cid, &bytes);
        out.responses.into_iter().next().expect("history ack")
    }

    pub fn dispatch_raw(
        &mut self,
        connection_id: Option<Id128>,
        bytes: &[u8],
    ) -> (Option<Id128>, iam_core::EngineOutput) {
        self.dispatch(connection_id, bytes)
    }

    fn collect_pushes(&mut self, pushes: Vec<(Id128, Vec<u8>)>) {
        for (cid, frame) in pushes {
            self.pushes.entry(cid).or_default().push(frame);
        }
    }

    fn dispatch(
        &mut self,
        connection_id: Option<Id128>,
        bytes: &[u8],
    ) -> (Option<Id128>, iam_core::EngineOutput) {
        let mut dec = FrameDecoder::new();
        dec.push(bytes);
        let frame = dec.next_frame().unwrap().unwrap();
        let decoded = decode_frame(&frame).unwrap();
        self.engine.handle(connection_id, &decoded).unwrap()
    }
}

fn decode_body_auth(bytes: &[u8]) -> Option<Option<Vec<u8>>> {
    let mut dec = FrameDecoder::new();
    dec.push(bytes);
    let frame = dec.next_frame().unwrap().unwrap();
    match decode_frame(&frame).unwrap().body {
        DecodedBody::AuthAck(r) => match r.value {
            Some(iam_protocol::pb::auth_response::Value::Session(s)) => {
                Some(s.resume_token.map(|t| t.to_vec()))
            }
            _ => Some(None),
        },
        _ => None,
    }
}

fn decode_body_reconnect(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut dec = FrameDecoder::new();
    dec.push(bytes);
    let frame = dec.next_frame().unwrap().unwrap();
    match decode_frame(&frame).unwrap().body {
        DecodedBody::ReconnectAck(r) => match r.value {
            Some(iam_protocol::pb::reconnect_response::Value::Session(s)) => {
                s.resume_token.map(|t| t.to_vec())
            }
            _ => None,
        },
        _ => None,
    }
}

pub fn parse_send_ack(bytes: &[u8]) -> iam_protocol::pb::SendResponse {
    let mut dec = FrameDecoder::new();
    dec.push(bytes);
    let frame = dec.next_frame().unwrap().unwrap();
    match decode_frame(&frame).unwrap().body {
        DecodedBody::SendAck(r) => r,
        _ => panic!("expected send ack"),
    }
}

pub fn parse_sync_ack(bytes: &[u8]) -> iam_protocol::pb::SyncResponse {
    let mut dec = FrameDecoder::new();
    dec.push(bytes);
    let frame = dec.next_frame().unwrap().unwrap();
    match decode_frame(&frame).unwrap().body {
        DecodedBody::SyncAck(r) => r,
        _ => panic!("expected sync ack"),
    }
}

pub fn parse_snapshot_ack(bytes: &[u8]) -> iam_protocol::pb::SnapshotResponse {
    let mut dec = FrameDecoder::new();
    dec.push(bytes);
    let frame = dec.next_frame().unwrap().unwrap();
    match decode_frame(&frame).unwrap().body {
        DecodedBody::SnapshotAck(r) => r,
        _ => panic!("expected snapshot ack"),
    }
}

pub fn parse_history_ack(bytes: &[u8]) -> iam_protocol::pb::HistoryResponse {
    let mut dec = FrameDecoder::new();
    dec.push(bytes);
    let frame = dec.next_frame().unwrap().unwrap();
    match decode_frame(&frame).unwrap().body {
        DecodedBody::HistoryAck(r) => r,
        _ => panic!("expected history ack"),
    }
}

pub fn parse_error(bytes: &[u8]) -> iam_protocol::pb::ProtocolError {
    let mut dec = FrameDecoder::new();
    dec.push(bytes);
    let frame = dec.next_frame().unwrap().unwrap();
    match decode_frame(&frame).unwrap().body {
        DecodedBody::Error(r) => r,
        other => panic!("expected ERROR frame, got {other:?}"),
    }
}

pub fn committed_message_id(resp: &iam_protocol::pb::SendResponse) -> Id128 {
    match resp.outcome.as_ref().unwrap().value.as_ref().unwrap() {
        send_outcome::Value::Committed(r) => {
            let b = r.r#ref.as_ref().unwrap().message_id.as_ref().unwrap();
            let mut id = [0u8; 16];
            id.copy_from_slice(b);
            id
        }
        other => panic!("expected committed, got {other:?}"),
    }
}
