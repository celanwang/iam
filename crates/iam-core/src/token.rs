use chacha20poly1305::{
    aead::{Aead, KeyInit},
    XChaCha20Poly1305, XNonce,
};
use iam_protocol::Id128;

const CURSOR_TTL_MS: i64 = 30 * 24 * 60 * 60 * 1000;
const PAGE_TTL_MS: i64 = 15 * 60 * 1000;
const RESUME_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1000;
const SNAPSHOT_TTL_MS: i64 = 15 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncCursor {
    pub tenant_id: u64,
    pub user_id: u64,
    pub device_id: Id128,
    /// 0 when `is_account`
    pub conversation_id: u64,
    pub epoch: Id128,
    pub access_version: u64,
    pub covered_through: u64,
    pub issued_at_ms: i64,
    pub is_account: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncPageToken {
    pub cursor: SyncCursor,
    pub upper_bound: u64,
    pub scanned_through: u64,
    pub round_started_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeToken {
    pub tenant_id: u64,
    pub user_id: u64,
    pub device_id: Id128,
    pub auth_epoch: Id128,
    pub issued_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotToken {
    pub tenant_id: u64,
    pub user_id: u64,
    pub device_id: Id128,
    pub is_account: bool,
    pub conversation_id: u64,
    pub snapshot_id: Id128,
    pub epoch: Id128,
    pub access_version: u64,
    pub upper_bound: u64,
    pub from_create_seq: u64,
    pub through_create_seq: u64,
    pub scanned_items: u64,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryToken {
    pub tenant_id: u64,
    pub user_id: u64,
    pub device_id: Id128,
    pub conversation_id: u64,
    pub epoch: Id128,
    pub access_version: u64,
    pub before_create_seq: u64,
    pub upper_bound: u64,
    pub retention_floor_seq: u64,
    pub scanned_through_seq: u64,
    pub round_started_ms: i64,
}

pub struct TokenSigner {
    key: [u8; 32],
    verification_keys: Vec<(u32, [u8; 32])>,
    /// Key identifier bound into every token (protocol key-id obligation).
    pub key_id: u32,
}

impl TokenSigner {
    pub fn new(key: [u8; 32]) -> Self {
        Self::with_key_id(key, 1)
    }

    pub fn with_key_id(key: [u8; 32], key_id: u32) -> Self {
        Self {
            key,
            key_id,
            verification_keys: vec![(key_id, key)],
        }
    }

    fn sign(&self, payload: &[u8]) -> Vec<u8> {
        let mut nonce = [0u8; 24];
        getrandom::fill(&mut nonce).expect("secure randomness unavailable");
        let cipher = XChaCha20Poly1305::new((&self.key).into());
        let ciphertext = cipher
            .encrypt(XNonce::from_slice(&nonce), payload)
            .expect("bounded token encryption");
        let mut out = nonce.to_vec();
        out.extend_from_slice(&ciphertext);
        out
    }

    fn verify_prefix(&self, bytes: &[u8], magic: &[u8; 4]) -> Result<Vec<u8>, TokenError> {
        if bytes.len() < 24 + 16 + 8 || bytes.len() > 4096 {
            return Err(TokenError::Invalid);
        }
        for (key_id, key) in &self.verification_keys {
            let cipher = XChaCha20Poly1305::new(key.into());
            let Ok(payload) = cipher.decrypt(XNonce::from_slice(&bytes[..24]), &bytes[24..]) else {
                continue;
            };
            if payload.len() < 8 || &payload[..4] != magic {
                return Err(TokenError::Invalid);
            }
            let kid = u32::from_be_bytes(payload[4..8].try_into().unwrap());
            if kid != *key_id {
                return Err(TokenError::Invalid);
            }
            return Ok(payload);
        }
        Err(TokenError::Invalid)
    }

    /// Retain a retired key until all tokens issued with it have expired.
    pub fn retain_verification_key(
        &mut self,
        key_id: u32,
        key: [u8; 32],
    ) -> Result<(), TokenError> {
        if self.verification_keys.len() >= 16
            || self.verification_keys.iter().any(|(id, _)| *id == key_id)
        {
            return Err(TokenError::Invalid);
        }
        self.verification_keys.push((key_id, key));
        Ok(())
    }

    fn push_header(&self, payload: &mut Vec<u8>, magic: &[u8; 4]) {
        payload.extend_from_slice(magic);
        payload.extend_from_slice(&self.key_id.to_be_bytes());
    }

    pub fn sign_cursor(&self, c: &SyncCursor) -> Vec<u8> {
        let mut payload = Vec::new();
        self.push_header(&mut payload, b"cur1");
        payload.push(if c.is_account { 1 } else { 0 });
        payload.extend_from_slice(&c.tenant_id.to_be_bytes());
        payload.extend_from_slice(&c.user_id.to_be_bytes());
        payload.extend_from_slice(&c.device_id);
        payload.extend_from_slice(&c.conversation_id.to_be_bytes());
        payload.extend_from_slice(&c.epoch);
        payload.extend_from_slice(&c.access_version.to_be_bytes());
        payload.extend_from_slice(&c.covered_through.to_be_bytes());
        payload.extend_from_slice(&c.issued_at_ms.to_be_bytes());
        self.sign(&payload)
    }

    pub fn verify_cursor(&self, bytes: &[u8], now_ms: i64) -> Result<SyncCursor, TokenError> {
        let (token, expired) = self.verify_cursor_with_expiry(bytes, now_ms)?;
        if expired {
            return Err(TokenError::Expired);
        }
        Ok(token)
    }

    /// Authenticates the token while retaining its scope even after expiration.
    pub fn verify_cursor_with_expiry(
        &self,
        bytes: &[u8],
        now_ms: i64,
    ) -> Result<(SyncCursor, bool), TokenError> {
        let payload = self.verify_prefix(bytes, b"cur1")?;
        if payload.len() != 4 + 4 + 1 + 8 + 8 + 16 + 8 + 16 + 8 + 8 + 8 {
            return Err(TokenError::Invalid);
        }
        let mut o = 8;
        let is_account = payload[o] == 1;
        o += 1;
        let tenant_id = read_u64(&payload, &mut o);
        let user_id = read_u64(&payload, &mut o);
        let mut device_id = [0u8; 16];
        device_id.copy_from_slice(&payload[o..o + 16]);
        o += 16;
        let conversation_id = read_u64(&payload, &mut o);
        let mut epoch = [0u8; 16];
        epoch.copy_from_slice(&payload[o..o + 16]);
        o += 16;
        let access_version = read_u64(&payload, &mut o);
        let covered_through = read_u64(&payload, &mut o);
        let issued_at_ms = read_i64(&payload, &mut o);
        if issued_at_ms < 0 || issued_at_ms > now_ms {
            return Err(TokenError::Invalid);
        }
        let expired = now_ms
            >= issued_at_ms
                .checked_add(CURSOR_TTL_MS)
                .ok_or(TokenError::Invalid)?;
        Ok((
            SyncCursor {
                tenant_id,
                user_id,
                device_id,
                conversation_id,
                epoch,
                access_version,
                covered_through,
                issued_at_ms,
                is_account,
            },
            expired,
        ))
    }

    pub fn sign_page(&self, p: &SyncPageToken) -> Vec<u8> {
        let mut payload = Vec::new();
        self.push_header(&mut payload, b"pg01");
        let cur = self.sign_cursor(&p.cursor);
        payload.extend_from_slice(&(cur.len() as u32).to_be_bytes());
        payload.extend_from_slice(&cur);
        payload.extend_from_slice(&p.upper_bound.to_be_bytes());
        payload.extend_from_slice(&p.scanned_through.to_be_bytes());
        payload.extend_from_slice(&p.round_started_ms.to_be_bytes());
        self.sign(&payload)
    }

    pub fn verify_page(&self, bytes: &[u8], now_ms: i64) -> Result<SyncPageToken, TokenError> {
        let (token, expired) = self.verify_page_with_expiry(bytes, now_ms)?;
        if expired {
            return Err(TokenError::Expired);
        }
        Ok(token)
    }

    /// Authenticates the token while retaining its scope even after expiration.
    pub fn verify_page_with_expiry(
        &self,
        bytes: &[u8],
        now_ms: i64,
    ) -> Result<(SyncPageToken, bool), TokenError> {
        let payload = self.verify_prefix(bytes, b"pg01")?;
        if payload.len() < 12 {
            return Err(TokenError::Invalid);
        }
        let cur_len = u32::from_be_bytes(payload[8..12].try_into().unwrap()) as usize;
        if payload.len() != 12 + cur_len + 8 + 8 + 8 {
            return Err(TokenError::Invalid);
        }
        let (cursor, cursor_expired) =
            self.verify_cursor_with_expiry(&payload[12..12 + cur_len], now_ms)?;
        let mut o = 12 + cur_len;
        let upper_bound = read_u64(&payload, &mut o);
        let scanned_through = read_u64(&payload, &mut o);
        let round_started_ms = read_i64(&payload, &mut o);
        if round_started_ms < 0 || round_started_ms > now_ms {
            return Err(TokenError::Invalid);
        }
        let expired = cursor_expired
            || now_ms
                >= round_started_ms
                    .checked_add(PAGE_TTL_MS)
                    .ok_or(TokenError::Invalid)?;
        Ok((
            SyncPageToken {
                cursor,
                upper_bound,
                scanned_through,
                round_started_ms,
            },
            expired,
        ))
    }

    pub fn sign_resume(&self, r: &ResumeToken) -> Vec<u8> {
        let mut payload = Vec::new();
        self.push_header(&mut payload, b"rs01");
        payload.extend_from_slice(&r.tenant_id.to_be_bytes());
        payload.extend_from_slice(&r.user_id.to_be_bytes());
        payload.extend_from_slice(&r.device_id);
        payload.extend_from_slice(&r.auth_epoch);
        payload.extend_from_slice(&r.issued_at_ms.to_be_bytes());
        self.sign(&payload)
    }

    pub fn verify_resume(&self, bytes: &[u8], now_ms: i64) -> Result<ResumeToken, TokenError> {
        let payload = self.verify_prefix(bytes, b"rs01")?;
        if payload.len() != 4 + 4 + 8 + 8 + 16 + 16 + 8 {
            return Err(TokenError::Invalid);
        }
        let mut o = 8;
        let tenant_id = read_u64(&payload, &mut o);
        let user_id = read_u64(&payload, &mut o);
        let mut device_id = [0u8; 16];
        device_id.copy_from_slice(&payload[o..o + 16]);
        o += 16;
        let mut auth_epoch = [0u8; 16];
        auth_epoch.copy_from_slice(&payload[o..o + 16]);
        o += 16;
        let issued_at_ms = read_i64(&payload, &mut o);
        if issued_at_ms < 0 || issued_at_ms > now_ms {
            return Err(TokenError::Invalid);
        }
        if now_ms
            >= issued_at_ms
                .checked_add(RESUME_TTL_MS)
                .ok_or(TokenError::Invalid)?
        {
            return Err(TokenError::Expired);
        }
        Ok(ResumeToken {
            tenant_id,
            user_id,
            device_id,
            auth_epoch,
            issued_at_ms,
        })
    }

    pub fn sign_snapshot(&self, s: &SnapshotToken) -> Vec<u8> {
        let mut payload = Vec::new();
        self.push_header(&mut payload, b"sn01");
        payload.push(if s.is_account { 1 } else { 0 });
        payload.extend_from_slice(&s.tenant_id.to_be_bytes());
        payload.extend_from_slice(&s.user_id.to_be_bytes());
        payload.extend_from_slice(&s.device_id);
        payload.extend_from_slice(&s.conversation_id.to_be_bytes());
        payload.extend_from_slice(&s.snapshot_id);
        payload.extend_from_slice(&s.epoch);
        payload.extend_from_slice(&s.access_version.to_be_bytes());
        payload.extend_from_slice(&s.upper_bound.to_be_bytes());
        payload.extend_from_slice(&s.from_create_seq.to_be_bytes());
        payload.extend_from_slice(&s.through_create_seq.to_be_bytes());
        payload.extend_from_slice(&s.scanned_items.to_be_bytes());
        payload.extend_from_slice(&s.created_at_ms.to_be_bytes());
        self.sign(&payload)
    }

    pub fn verify_snapshot(&self, bytes: &[u8], now_ms: i64) -> Result<SnapshotToken, TokenError> {
        let (token, expired) = self.verify_snapshot_with_expiry(bytes, now_ms)?;
        if expired {
            return Err(TokenError::Expired);
        }
        Ok(token)
    }

    /// Authenticates the token while retaining its scope even after expiration.
    pub fn verify_snapshot_with_expiry(
        &self,
        bytes: &[u8],
        now_ms: i64,
    ) -> Result<(SnapshotToken, bool), TokenError> {
        let payload = self.verify_prefix(bytes, b"sn01")?;
        if payload.len() != 4 + 4 + 1 + 8 + 8 + 16 + 8 + 16 + 16 + 8 + 8 + 8 + 8 + 8 + 8 {
            return Err(TokenError::Invalid);
        }
        let mut o = 8;
        let is_account = payload[o] == 1;
        o += 1;
        let tenant_id = read_u64(&payload, &mut o);
        let user_id = read_u64(&payload, &mut o);
        let mut device_id = [0u8; 16];
        device_id.copy_from_slice(&payload[o..o + 16]);
        o += 16;
        let conversation_id = read_u64(&payload, &mut o);
        let mut snapshot_id = [0u8; 16];
        snapshot_id.copy_from_slice(&payload[o..o + 16]);
        o += 16;
        let mut epoch = [0u8; 16];
        epoch.copy_from_slice(&payload[o..o + 16]);
        o += 16;
        let access_version = read_u64(&payload, &mut o);
        let upper_bound = read_u64(&payload, &mut o);
        let from_create_seq = read_u64(&payload, &mut o);
        let through_create_seq = read_u64(&payload, &mut o);
        let scanned_items = read_u64(&payload, &mut o);
        let created_at_ms = read_i64(&payload, &mut o);
        if created_at_ms < 0 || created_at_ms > now_ms {
            return Err(TokenError::Invalid);
        }
        let expired = now_ms
            >= created_at_ms
                .checked_add(SNAPSHOT_TTL_MS)
                .ok_or(TokenError::Invalid)?;
        Ok((
            SnapshotToken {
                tenant_id,
                user_id,
                device_id,
                is_account,
                conversation_id,
                snapshot_id,
                epoch,
                access_version,
                upper_bound,
                from_create_seq,
                through_create_seq,
                scanned_items,
                created_at_ms,
            },
            expired,
        ))
    }

    pub fn sign_history(&self, h: &HistoryToken) -> Vec<u8> {
        let mut payload = Vec::new();
        self.push_header(&mut payload, b"hi01");
        payload.extend_from_slice(&h.tenant_id.to_be_bytes());
        payload.extend_from_slice(&h.user_id.to_be_bytes());
        payload.extend_from_slice(&h.device_id);
        payload.extend_from_slice(&h.conversation_id.to_be_bytes());
        payload.extend_from_slice(&h.epoch);
        payload.extend_from_slice(&h.access_version.to_be_bytes());
        payload.extend_from_slice(&h.before_create_seq.to_be_bytes());
        payload.extend_from_slice(&h.upper_bound.to_be_bytes());
        payload.extend_from_slice(&h.retention_floor_seq.to_be_bytes());
        payload.extend_from_slice(&h.scanned_through_seq.to_be_bytes());
        payload.extend_from_slice(&h.round_started_ms.to_be_bytes());
        self.sign(&payload)
    }

    pub fn verify_history(&self, bytes: &[u8], now_ms: i64) -> Result<HistoryToken, TokenError> {
        let (token, expired) = self.verify_history_with_expiry(bytes, now_ms)?;
        if expired {
            return Err(TokenError::Expired);
        }
        Ok(token)
    }

    pub fn verify_history_with_expiry(
        &self,
        bytes: &[u8],
        now_ms: i64,
    ) -> Result<(HistoryToken, bool), TokenError> {
        let payload = self.verify_prefix(bytes, b"hi01")?;
        if payload.len() != 4 + 4 + 8 + 8 + 16 + 8 + 16 + 8 + 8 + 8 + 8 + 8 + 8 {
            return Err(TokenError::Invalid);
        }
        let mut o = 8;
        let tenant_id = read_u64(&payload, &mut o);
        let user_id = read_u64(&payload, &mut o);
        let mut device_id = [0u8; 16];
        device_id.copy_from_slice(&payload[o..o + 16]);
        o += 16;
        let conversation_id = read_u64(&payload, &mut o);
        let mut epoch = [0u8; 16];
        epoch.copy_from_slice(&payload[o..o + 16]);
        o += 16;
        let access_version = read_u64(&payload, &mut o);
        let before_create_seq = read_u64(&payload, &mut o);
        let upper_bound = read_u64(&payload, &mut o);
        let retention_floor_seq = read_u64(&payload, &mut o);
        let scanned_through_seq = read_u64(&payload, &mut o);
        let round_started_ms = read_i64(&payload, &mut o);
        if round_started_ms < 0 || round_started_ms > now_ms {
            return Err(TokenError::Invalid);
        }
        let expired = now_ms
            >= round_started_ms
                .checked_add(PAGE_TTL_MS)
                .ok_or(TokenError::Invalid)?;
        Ok((
            HistoryToken {
                tenant_id,
                user_id,
                device_id,
                conversation_id,
                epoch,
                access_version,
                before_create_seq,
                upper_bound,
                retention_floor_seq,
                scanned_through_seq,
                round_started_ms,
            },
            expired,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenError {
    Invalid,
    Expired,
}

fn read_u64(buf: &[u8], o: &mut usize) -> u64 {
    let v = u64::from_be_bytes(buf[*o..*o + 8].try_into().unwrap());
    *o += 8;
    v
}

fn read_i64(buf: &[u8], o: &mut usize) -> i64 {
    let v = i64::from_be_bytes(buf[*o..*o + 8].try_into().unwrap());
    *o += 8;
    v
}
