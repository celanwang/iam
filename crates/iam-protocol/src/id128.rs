use crate::error::{ProtocolErrorKind, Result};
use uuid::Uuid;

pub type Id128 = [u8; 16];

pub fn generate_id128() -> Id128 {
    Uuid::now_v7().into_bytes()
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
    let ms = ((id[0] as u64) << 40)
        | ((id[1] as u64) << 32)
        | ((id[2] as u64) << 24)
        | ((id[3] as u64) << 16)
        | ((id[4] as u64) << 8)
        | (id[5] as u64);
    ms
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_doc_test_id() {
        let bytes = hex::decode("018bcfe5680070008000000000000001").unwrap();
        let id = validate_id128(&bytes).unwrap();
        assert_eq!(id128_timestamp_ms(&id), 1_700_000_000_000);
        assert_eq!(
            id128_to_string(&id),
            "018bcfe5-6800-7000-8000-000000000001"
        );
    }
}
