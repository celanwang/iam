use crate::error::{ProtocolErrorKind, Result};
use uuid::Uuid;

pub type Id128 = [u8; 16];

pub fn generate_id128() -> Id128 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before Unix epoch")
        .as_millis();
    assert!(millis < (1u128 << 48), "UUID timestamp overflow");
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).expect("secure randomness unavailable");
    id[..6].copy_from_slice(&(millis as u64).to_be_bytes()[2..]);
    id[6] = (id[6] & 0x0f) | 0x70;
    id[8] = (id[8] & 0x3f) | 0x80;
    id
}

pub fn validate_id128(bytes: &[u8]) -> Result<Id128> {
    if bytes.len() != 16 {
        return Err(ProtocolErrorKind::InvalidArgument("id128 length"));
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(bytes);
    let uuid = Uuid::from_bytes(arr);
    if uuid.get_version_num() != 7 {
        return Err(ProtocolErrorKind::InvalidArgument("id128 version"));
    }
    if uuid.is_nil() || uuid.as_u128() == u128::MAX {
        return Err(ProtocolErrorKind::InvalidArgument("id128 reserved"));
    }
    // variant must be RFC 4122 (10xxxxxx)
    let variant_nibble = arr[8] >> 6;
    if variant_nibble != 0b10 {
        return Err(ProtocolErrorKind::InvalidArgument("id128 variant"));
    }
    Ok(arr)
}

pub fn parse_id128_bytes(bytes: &[u8]) -> Result<Id128> {
    validate_id128(bytes)
}

pub fn id128_to_string(id: &Id128) -> String {
    Uuid::from_bytes(*id).hyphenated().to_string()
}

pub fn id128_timestamp_ms(id: &Id128) -> u64 {
    u64::from_be_bytes([0, 0, id[0], id[1], id[2], id[3], id[4], id[5]])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_doc_test_id() {
        let bytes = hex::decode("018bcfe5680070008000000000000001").unwrap();
        let id = validate_id128(&bytes).unwrap();
        assert_eq!(id128_timestamp_ms(&id), 1_700_000_000_000);
        assert_eq!(id128_to_string(&id), "018bcfe5-6800-7000-8000-000000000001");
    }
}
