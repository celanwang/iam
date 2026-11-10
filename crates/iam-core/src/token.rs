use hmac::{Hmac, Mac};
use iam_protocol::Id128;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const CURSOR_TTL_MS: i64 = 30 * 24 * 60 * 60 * 1000;
const PAGE_TTL_MS: i64 = 15 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncCursor {
    pub user_id: u64,
    pub device_id: Id128,
    pub conversation_id: u64,
    pub epoch: Id128,
    pub access_version: u64,
    pub covered_through: u64,
    pub issued_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncPageToken {
    pub cursor: SyncCursor,
    pub upper_bound: u64,
    pub scanned_through: u64,
    pub round_started_ms: i64,
}

pub struct TokenSigner {
    key: [u8; 32],
}

impl TokenSigner {
    pub fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    pub fn sign_cursor(&self, c: &SyncCursor) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"cur1");
        payload.extend_from_slice(&c.user_id.to_be_bytes());
        payload.extend_from_slice(&c.device_id);
        payload.extend_from_slice(&c.conversation_id.to_be_bytes());
        payload.extend_from_slice(&c.epoch);
        payload.extend_from_slice(&c.access_version.to_be_bytes());
        payload.extend_from_slice(&c.covered_through.to_be_bytes());
        payload.extend_from_slice(&c.issued_at_ms.to_be_bytes());
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("hmac key");
        mac.update(&payload);
        let sig = mac.finalize().into_bytes();
        payload.extend_from_slice(&sig);
        payload
    }

    pub fn verify_cursor(&self, bytes: &[u8], now_ms: i64) -> Result<SyncCursor, TokenError> {
        if bytes.len() < 4 + 8 + 16 + 8 + 16 + 8 + 8 + 8 + 32 {
            return Err(TokenError::Invalid);
        }
        if &bytes[..4] != b"cur1" {
            return Err(TokenError::Invalid);
        }
        let (payload, sig) = bytes.split_at(bytes.len() - 32);
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("hmac key");
        mac.update(payload);
        mac.verify_slice(sig).map_err(|_| TokenError::Invalid)?;
        let mut o = 4;
        let user_id = read_u64(payload, &mut o);
        let mut device_id = [0u8; 16];
        device_id.copy_from_slice(&payload[o..o + 16]);
        o += 16;
        let conversation_id = read_u64(payload, &mut o);
        let mut epoch = [0u8; 16];
        epoch.copy_from_slice(&payload[o..o + 16]);
        o += 16;
        let access_version = read_u64(payload, &mut o);
        let covered_through = read_u64(payload, &mut o);
        let issued_at_ms = read_i64(payload, &mut o);
        if now_ms >= issued_at_ms + CURSOR_TTL_MS {
            return Err(TokenError::Expired);
        }
        Ok(SyncCursor {
            user_id,
            device_id,
            conversation_id,
            epoch,
            access_version,
            covered_through,
            issued_at_ms,
        })
    }

    pub fn sign_page(&self, p: &SyncPageToken) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"pg01");
        let cur = self.sign_cursor(&p.cursor);
        payload.extend_from_slice(&(cur.len() as u32).to_be_bytes());
        payload.extend_from_slice(&cur);
        payload.extend_from_slice(&p.upper_bound.to_be_bytes());
        payload.extend_from_slice(&p.scanned_through.to_be_bytes());
        payload.extend_from_slice(&p.round_started_ms.to_be_bytes());
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("hmac key");
        mac.update(&payload);
        let sig = mac.finalize().into_bytes();
        payload.extend_from_slice(&sig);
        payload
    }

    pub fn verify_page(&self, bytes: &[u8], now_ms: i64) -> Result<SyncPageToken, TokenError> {
        if bytes.len() < 4 + 4 + 32 {
            return Err(TokenError::Invalid);
        }
        if &bytes[..4] != b"pg01" {
            return Err(TokenError::Invalid);
        }
        let (payload, sig) = bytes.split_at(bytes.len() - 32);
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("hmac key");
        mac.update(payload);
        mac.verify_slice(sig).map_err(|_| TokenError::Invalid)?;
        let cur_len = u32::from_be_bytes(payload[4..8].try_into().unwrap()) as usize;
        if payload.len() < 8 + cur_len + 8 + 8 + 8 {
            return Err(TokenError::Invalid);
        }
        let cursor = self.verify_cursor(&payload[8..8 + cur_len], now_ms)?;
        let mut o = 8 + cur_len;
        let upper_bound = read_u64(payload, &mut o);
        let scanned_through = read_u64(payload, &mut o);
        let round_started_ms = read_i64(payload, &mut o);
        if now_ms >= round_started_ms + PAGE_TTL_MS {
            return Err(TokenError::Expired);
        }
        Ok(SyncPageToken {
            cursor,
            upper_bound,
            scanned_through,
            round_started_ms,
        })
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
