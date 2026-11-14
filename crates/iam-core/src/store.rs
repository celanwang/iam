use iam_protocol::pb::{
    change, Change, ConversationKind, ConversationState, Empty, MessageContent, MessagePosition,
    MessageRef, MessageState, MessageView, StreamContext, SyncTarget,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceCredential {
    pub tenant_id: u64,
    pub user_id: u64,
    pub device_id: Id128,
    pub expires_at_ms: i64,
    pub token_sha256: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct Conversation {
    pub tenant_id: u64,
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
    pub auth_epoch: Id128,
    pub authorization_expires_at_ms: Option<i64>,
    pub last_req_id: u64,
    pub push_ack: bool,
    pub next_push_id: u64,
    pub outbound: VecDeque<Vec<u8>>,
    /// conversation_id -> client_msg_id currently blocking serial send
    pub inflight_send: HashMap<u64, Id128>,
}

#[derive(Debug, Clone)]
pub struct AccountStream {
    pub epoch: Id128,
    pub next_event_seq: u64,
    pub events: Vec<StoredEvent>,
}

#[derive(Default)]
pub struct MemoryStore {
    pub users_by_token: HashMap<Vec<u8>, UserRecord>,
    pub conversations: HashMap<(u64, u64), Conversation>,
    pub messages: HashMap<(u64, u64, Id128), StoredMessage>,
    pub idempotency: HashMap<IdemKey, IdempotencyRecord>,
    pub connections: HashMap<Id128, ConnState>,
    pub device_conn: HashMap<(u64, u64, Id128), Id128>,
    pub account_streams: HashMap<(u64, u64), AccountStream>,
    pub revoked_auth_epochs: HashSet<Id128>,
    pub(crate) device_credentials: Option<HashMap<[u8; 32], (Id128, DeviceCredential)>>,
    pub(crate) credential_devices: HashMap<(u64, u64, Id128), [u8; 32]>,
    /// Monotonic close proof watermark over client_msg time coverage (ms).
    pub close_watermark_ms: i64,
    pub now_ms: i64,
    /// Highest `now_ms` ever observed; used to detect clock rollback.
    pub max_observed_now_ms: i64,
    /// When true, authoritative records may be incomplete — new accepts stop.
    pub recovery_incomplete: bool,
    /// Testable entrance overload switch (RATE_LIMITED / OVERLOADED).
    pub entrance_overloaded: bool,
}

impl MemoryStore {
    /// Enables device-bound credentials for a fresh network service.
    pub fn configure_device_credentials(
        &mut self,
        credentials: impl IntoIterator<Item = DeviceCredential>,
    ) -> Result<(), &'static str> {
        if !self.connections.is_empty() || self.device_credentials.is_some() {
            return Err("credentials require a fresh service");
        }
        self.apply_device_credentials(credentials)?;
        Ok(())
    }

    /// Replace the complete credential list and return sessions whose authorization changed.
    pub fn reload_device_credentials(
        &mut self,
        credentials: impl IntoIterator<Item = DeviceCredential>,
    ) -> Result<Vec<Id128>, &'static str> {
        if !self.uses_device_credentials() {
            return Err("device credentials are not configured");
        }
        self.apply_device_credentials(credentials)
    }

    fn apply_device_credentials(
        &mut self,
        credentials: impl IntoIterator<Item = DeviceCredential>,
    ) -> Result<Vec<Id128>, &'static str> {
        let mut tokens = HashMap::new();
        let mut devices = HashMap::new();
        let mut retained_revocations = HashSet::new();
        for c in credentials {
            if c.tenant_id == 0
                || c.user_id == 0
                || iam_protocol::validate_id128(&c.device_id).is_err()
                || c.expires_at_ms <= 0
                || devices
                    .insert((c.tenant_id, c.user_id, c.device_id), c.token_sha256)
                    .is_some()
                || tokens.contains_key(&c.token_sha256)
            {
                return Err("invalid or duplicate device credential");
            }
            let previous = self
                .device_credentials
                .as_ref()
                .and_then(|old| old.get(&c.token_sha256));
            let epoch = match previous {
                Some((epoch, old)) if *old == c => *epoch,
                _ => generate_id128(),
            };
            if previous.is_some_and(|(old_epoch, _)| self.revoked_auth_epochs.contains(old_epoch)) {
                retained_revocations.insert(epoch);
            }
            tokens.insert(c.token_sha256, (epoch, c));
        }
        let now = self.authorization_time();
        let invalidated = self
            .connections
            .values()
            .filter_map(|conn| {
                let current = devices
                    .get(&(conn.tenant_id, conn.user_id, conn.device_id))
                    .and_then(|digest| tokens.get(digest));
                if let Some((epoch, credential)) =
                    current.filter(|(epoch, _)| *epoch == conn.auth_epoch)
                {
                    return (credential.expires_at_ms <= now
                        || retained_revocations.contains(epoch))
                    .then_some(conn.connection_id);
                }
                // A previously admitted session may still be awaiting gateway cleanup.
                retained_revocations.insert(conn.auth_epoch);
                Some(conn.connection_id)
            })
            .collect::<Vec<_>>();
        self.revoked_auth_epochs = retained_revocations;
        self.credential_devices = devices;
        self.device_credentials = Some(tokens);
        Ok(invalidated)
    }

    pub fn uses_device_credentials(&self) -> bool {
        self.device_credentials.is_some()
    }

    pub(crate) fn credential_for_token(&self, token: &[u8]) -> Option<(Id128, DeviceCredential)> {
        let digest: [u8; 32] = Sha256::digest(token).into();
        self.device_credentials.as_ref()?.get(&digest).copied()
    }

    pub fn session_expires_at(&self, cid: Id128) -> Option<i64> {
        self.connections.get(&cid)?.authorization_expires_at_ms
    }

    pub(crate) fn credential(
        &self,
        tenant: u64,
        user: u64,
        device: Id128,
        epoch: Id128,
    ) -> Option<DeviceCredential> {
        let digest = self.credential_devices.get(&(tenant, user, device))?;
        let (current, credential) = self.device_credentials.as_ref()?.get(digest)?;
        (*current == epoch).then_some(*credential)
    }

    pub fn session_authorized(&self, cid: Id128) -> bool {
        self.connections.get(&cid).is_some_and(|conn| {
            conn.authorized_at(&self.revoked_auth_epochs, self.authorization_time())
        })
    }

    pub fn revoke_device(&mut self, tenant: u64, user: u64, device: Id128) -> Option<Id128> {
        let owner = (tenant, user, device);
        let digest = self.credential_devices.get(&owner)?;
        let (epoch, _) = self.device_credentials.as_ref()?.get(digest)?;
        self.revoked_auth_epochs.insert(*epoch);
        self.device_conn.get(&owner).copied()
    }

    pub(crate) fn authorization_valid(
        &self,
        tenant: u64,
        user: u64,
        device: Id128,
        epoch: Id128,
    ) -> bool {
        !self.revoked_auth_epochs.contains(&epoch)
            && (!self.uses_device_credentials()
                || self
                    .credential(tenant, user, device, epoch)
                    .is_some_and(|c| c.expires_at_ms > self.authorization_time()))
    }

    pub(crate) fn authorization_time(&self) -> i64 {
        self.now_ms
            .max(self.max_observed_now_ms)
            .max(wall_time_ms())
    }

    pub fn new(now_ms: i64) -> Self {
        Self {
            now_ms,
            max_observed_now_ms: now_ms,
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
        self.ensure_account_stream(tenant_id, user_id);
    }

    pub fn ensure_account_stream(&mut self, tenant_id: u64, user_id: u64) {
        self.account_streams
            .entry((tenant_id, user_id))
            .or_insert_with(|| AccountStream {
                epoch: generate_id128(),
                next_event_seq: 1,
                events: Vec::new(),
            });
    }

    pub fn seed_direct(&mut self, tenant_id: u64, conversation_id: u64, a: u64, b: u64) -> Id128 {
        let history_id = generate_id128();
        let mut members = HashSet::new();
        members.insert(a);
        members.insert(b);
        let conv = Conversation {
            tenant_id,
            conversation_id,
            kind: ConversationKind::Direct,
            history_id,
            access_version: 1,
            members: members.clone(),
            next_create_seq: 1,
            next_event_seq: 1,
            events: Vec::new(),
        };
        self.conversations
            .insert((tenant_id, conversation_id), conv.clone());
        for uid in [a, b] {
            self.ensure_account_stream(tenant_id, uid);
            self.push_account_upsert(uid, &conv);
        }
        history_id
    }

    pub fn push_account_upsert(&mut self, user_id: u64, conv: &Conversation) {
        let stream = self
            .account_streams
            .get_mut(&(conv.tenant_id, user_id))
            .unwrap();
        let event_seq = stream.next_event_seq;
        stream.next_event_seq += 1;
        stream.events.push(StoredEvent {
            event_seq,
            change: Change {
                event_seq: Some(event_seq),
                payload: Some(change::Payload::ConversationUpsert(conversation_state(
                    conv,
                ))),
            },
        });
    }

    pub fn set_now(&mut self, now_ms: i64) {
        self.now_ms = now_ms;
        if now_ms > self.max_observed_now_ms {
            self.max_observed_now_ms = now_ms;
        }
    }

    /// Returns true when server time went backwards vs the highest observed clock.
    pub fn clock_unreliable(&self) -> bool {
        self.now_ms < self.max_observed_now_ms
    }

    pub fn advance_close_watermark(&mut self, watermark_ms: i64) {
        if watermark_ms > self.close_watermark_ms {
            self.close_watermark_ms = watermark_ms;
        }
    }

    /// Drop idempotency rows whose query window is fully closed and covered by watermark.
    pub fn cleanup_expired_idempotency(&mut self) {
        let now = self.now_ms;
        let wm = self.close_watermark_ms;
        if self.recovery_incomplete || self.clock_unreliable() {
            return;
        }
        self.idempotency.retain(|_, rec| {
            let query_until = match rec {
                IdempotencyRecord::Committed { query_until_ms, .. }
                | IdempotencyRecord::RejectedFinal { query_until_ms, .. } => *query_until_ms,
            };
            // Keep unless server reached query_until AND watermark covers that range.
            !(now >= query_until && wm >= query_until - QUERY_EXTRA_MS)
        });
    }

    pub fn content_digest(content: &MessageContent) -> [u8; 32] {
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

    /// Rebuild a fixed historical view from the retained authoritative event log.
    pub fn message_views_at(
        &self,
        tenant_id: u64,
        conversation_id: u64,
        upper: u64,
    ) -> Vec<MessageView> {
        let mut views = HashMap::new();
        if let Some(conv) = self.conversations.get(&(tenant_id, conversation_id)) {
            for ev in conv.events.iter().filter(|ev| ev.event_seq <= upper) {
                match ev.change.payload.as_ref() {
                    Some(change::Payload::MessageUpsert(view)) => {
                        if let Some(id) = view.r#ref.as_ref().and_then(|r| r.message_id.clone()) {
                            views.insert(id, view.clone());
                        }
                    }
                    Some(change::Payload::MessageRemove(r)) => {
                        if let Some(id) = &r.message_id {
                            views.remove(id);
                        }
                    }
                    _ => {}
                }
            }
        }
        let mut views: Vec<_> = views.into_values().collect();
        views.sort_by_key(|v| v.position.as_ref().and_then(|p| p.create_seq).unwrap_or(0));
        views
    }

    pub fn messages_in_conversation_ordered(
        &self,
        tenant_id: u64,
        conversation_id: u64,
    ) -> Vec<&StoredMessage> {
        let mut msgs: Vec<_> = self
            .messages
            .iter()
            .filter(|((tid, cid, _), _)| *tid == tenant_id && *cid == conversation_id)
            .map(|(_, m)| m)
            .collect();
        msgs.sort_by_key(|m| {
            m.view
                .position
                .as_ref()
                .and_then(|p| p.create_seq)
                .unwrap_or(0)
        });
        msgs
    }
}

impl ConnState {
    pub(crate) fn authorized_at(&self, revoked: &HashSet<Id128>, now_ms: i64) -> bool {
        !revoked.contains(&self.auth_epoch)
            && self
                .authorization_expires_at_ms
                .is_none_or(|deadline| now_ms < deadline)
    }
}

fn wall_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| duration.as_millis().try_into().ok())
        .unwrap_or(i64::MAX)
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

pub fn stream_context_account(epoch: Id128) -> StreamContext {
    StreamContext {
        target: Some(SyncTarget {
            value: Some(iam_protocol::pb::sync_target::Value::Account(Empty {})),
        }),
        epoch: Some(epoch.to_vec().into()),
        access_version: None,
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
