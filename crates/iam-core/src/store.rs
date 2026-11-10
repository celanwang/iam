use iam_protocol::pb::{
    Change, ConversationKind, ConversationState, MessageContent, MessagePosition, MessageRef,
    MessageState, MessageView, StreamContext, SyncTarget,
};
use iam_protocol::{generate_id128, id128_timestamp_ms, Id128};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};

pub const ALLOW_SKEW_MS: i64 = 120_000;
pub const ACCEPT_WINDOW_MS: i64 = 86_400_000;
pub const QUERY_EXTRA_MS: i64 = 604_800_000; // 7 days after accept_until

#[derive(Debug, Clone)]
pub struct UserRecord {
    pub tenant_id: u64,
    pub user_id: u64,
    pub access_token: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Conversation {
    pub conversation_id: u64,
    pub kind: ConversationKind,
    pub history_id: Id128,
    pub access_version: u64,
    pub members: HashSet<u64>,
    pub next_create_seq: u64,
    pub next_event_seq: u64,
    pub events: Vec<StoredEvent>,
}

#[derive(Debug, Clone)]
pub struct StoredEvent {
    pub event_seq: u64,
    pub change: Change,
}

#[derive(Debug, Clone)]
pub struct StoredMessage {
    pub view: MessageView,
    pub content_digest: [u8; 32],
}

#[derive(Debug, Clone)]
pub enum IdempotencyRecord {
    Committed {
        receipt_message_id: Id128,
        create_seq: u64,
        history_id: Id128,
        accepted_at_ms: i64,
        digest: [u8; 32],
        accept_until_ms: i64,
        query_until_ms: i64,
    },
    RejectedFinal {
        code: i32,
        digest: [u8; 32],
        accept_until_ms: i64,
        query_until_ms: i64,
    },
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct IdemKey {
    pub tenant_id: u64,
    pub user_id: u64,
    pub device_id: Id128,
    pub conversation_id: u64,
    pub client_msg_id: Id128,
}

#[derive(Debug, Clone)]
pub struct ConnState {
    pub connection_id: Id128,
    pub user_id: u64,
    pub tenant_id: u64,
    pub device_id: Id128,
    pub last_req_id: u64,
    pub push_ack: bool,
    pub next_push_id: u64,
    pub outbound: VecDeque<Vec<u8>>,
    /// conversation_id -> client_msg_id currently blocking serial send
    pub inflight_send: HashMap<u64, Id128>,
}

#[derive(Default)]
pub struct MemoryStore {
    pub users_by_token: HashMap<Vec<u8>, UserRecord>,
    pub conversations: HashMap<u64, Conversation>,
    pub messages: HashMap<(u64, Id128), StoredMessage>,
    pub idempotency: HashMap<IdemKey, IdempotencyRecord>,
    pub connections: HashMap<Id128, ConnState>,
    pub device_conn: HashMap<Id128, Id128>,
    pub close_watermark_ms: i64,
    pub now_ms: i64,
}

impl MemoryStore {
    pub fn new(now_ms: i64) -> Self {
        Self {
            now_ms,
            ..Default::default()
        }
    }

    pub fn seed_user(&mut self, tenant_id: u64, user_id: u64, token: Vec<u8>) {
        self.users_by_token.insert(
            token.clone(),
            UserRecord {
                tenant_id,
                user_id,
                access_token: token,
            },
        );
    }

    pub fn seed_direct(&mut self, conversation_id: u64, a: u64, b: u64) -> Id128 {
        let history_id = generate_id128();
        let mut members = HashSet::new();
        members.insert(a);
        members.insert(b);
        self.conversations.insert(
            conversation_id,
            Conversation {
                conversation_id,
                kind: ConversationKind::Direct,
                history_id,
                access_version: 1,
                members,
                next_create_seq: 1,
                next_event_seq: 1,
                events: Vec::new(),
            },
        );
        history_id
    }

    pub fn set_now(&mut self, now_ms: i64) {
        self.now_ms = now_ms;
    }

    pub fn content_digest(content: &MessageContent) -> [u8; 32] {
        // branch field number (text=1) + utf8 length + bytes
        let text = match content.value.as_ref() {
            Some(iam_protocol::pb::message_content::Value::Text(t)) => {
                t.text.as_deref().unwrap_or("")
            }
            _ => "",
        };
        let mut h = Sha256::new();
        h.update(1u32.to_be_bytes());
        h.update((text.len() as u32).to_be_bytes());
        h.update(text.as_bytes());
        h.finalize().into()
    }

    pub fn windows_for_client_msg(client_msg_id: &Id128) -> (i64, i64, i64) {
        let t = id128_timestamp_ms(client_msg_id) as i64;
        let accept_until = t + ACCEPT_WINDOW_MS;
        let query_until = t + ACCEPT_WINDOW_MS + QUERY_EXTRA_MS;
        (t, accept_until, query_until)
    }
}

pub fn conversation_state(c: &Conversation) -> ConversationState {
    ConversationState {
        conversation_id: Some(c.conversation_id),
        kind: Some(c.kind as i32),
        history_id: Some(c.history_id.to_vec().into()),
        access_version: Some(c.access_version),
    }
}

pub fn stream_context_for_conv(c: &Conversation) -> StreamContext {
    StreamContext {
        target: Some(SyncTarget {
            value: Some(iam_protocol::pb::sync_target::Value::Conversation(
                c.conversation_id,
            )),
        }),
        epoch: Some(c.history_id.to_vec().into()),
        access_version: Some(c.access_version),
    }
}

pub fn active_message_view(
    conversation_id: u64,
    message_id: Id128,
    history_id: Id128,
    create_seq: u64,
    sender_id: u64,
    accepted_at_ms: i64,
    content: MessageContent,
) -> MessageView {
    MessageView {
        r#ref: Some(MessageRef {
            conversation_id: Some(conversation_id),
            message_id: Some(message_id.to_vec().into()),
        }),
        position: Some(MessagePosition {
            history_id: Some(history_id.to_vec().into()),
            create_seq: Some(create_seq),
        }),
        revision_seq: Some(create_seq),
        sender_id: Some(sender_id),
        accepted_at_ms: Some(accepted_at_ms),
        state: Some(MessageState::Active as i32),
        content: Some(content),
    }
}
