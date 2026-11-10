//! In-process closed-loop helpers for protocol integration tests.

use iam_core::Engine;
use iam_protocol::pb::{
    message_content, send_outcome, sync_target, AuthRequest, Head, MessageContent, MessageKey,
    SendRequest, SyncRequest, SyncTarget, TextContent,
};
use iam_protocol::{
    decode_frame, encode_frame, generate_id128, DecodedBody, FrameDecoder, Id128, MsgType,
};

use std::collections::HashMap;

pub struct Peer {
    pub connection_id: Option<Id128>,
    pub device_id: Id128,
    pub inbox: Vec<Vec<u8>>,
    next_req: u64,
}

impl Peer {
    pub fn new() -> Self {
        Self {
            connection_id: None,
            device_id: generate_id128(),
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
    /// pending pushes keyed by connection
    pushes: HashMap<Id128, Vec<Vec<u8>>>,
}

impl LoopWorld {
    pub fn new(now_ms: i64) -> Self {
        let mut engine = Engine::new(now_ms);
        engine.store.seed_user(1, 100, b"token-a".to_vec());
        engine.store.seed_user(1, 200, b"token-b".to_vec());
        engine.store.seed_direct(1, 100, 200);
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
        let head = Head {
            req_id: Some(req_id),
        };
        let bytes = encode_frame(MsgType::Auth, &head, &body).unwrap();
        let (cid, out) = self.dispatch(None, &bytes);
        self.peers.get_mut(name).unwrap().connection_id = cid;
        self.collect_pushes(out.pushes);
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
        let head = Head {
            req_id: Some(req_id),
        };
        let bytes = encode_frame(MsgType::Send, &head, &body).unwrap();
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
            limit: None,
        };
        let head = Head {
            req_id: Some(req_id),
        };
        let bytes = encode_frame(MsgType::Sync, &head, &body).unwrap();
        let (_, out) = self.dispatch(cid, &bytes);
        out.responses.into_iter().next().expect("sync ack")
    }

    pub fn drain_pushes(&mut self, name: &'static str) -> Vec<Vec<u8>> {
        let cid = self.peers.get(name).unwrap().connection_id.unwrap();
        self.pushes.remove(&cid).unwrap_or_default()
    }

    pub fn drop_pushes(&mut self, name: &'static str) {
        let cid = self.peers.get(name).unwrap().connection_id.unwrap();
        self.pushes.remove(&cid);
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
