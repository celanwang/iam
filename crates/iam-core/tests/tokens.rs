use iam_core::{SyncCursor, SyncPageToken, TokenError, TokenSigner};
use iam_protocol::generate_id128;

fn sample_cursor(issued_at: i64) -> SyncCursor {
    SyncCursor {
        tenant_id: 1,
        user_id: 100,
        device_id: generate_id128(),
        conversation_id: 1,
        epoch: generate_id128(),
        access_version: 1,
        covered_through: 3,
        issued_at_ms: issued_at,
        is_account: false,
    }
}

#[test]
fn cursor_roundtrip_and_key_id_binding() {
    let signer = TokenSigner::with_key_id([9u8; 32], 7);
    let cur = sample_cursor(1_700_000_000_000);
    let bytes = signer.sign_cursor(&cur);
    let got = signer.verify_cursor(&bytes, 1_700_000_000_000).unwrap();
    assert_eq!(got.user_id, cur.user_id);
    assert_eq!(got.covered_through, 3);

    let other = TokenSigner::with_key_id([9u8; 32], 8);
    assert!(other.verify_cursor(&bytes, 1_700_000_000_000).is_err());
}

#[test]
fn tampered_cursor_rejected() {
    let signer = TokenSigner::new([1u8; 32]);
    let mut bytes = signer.sign_cursor(&sample_cursor(1_700_000_000_000));
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    assert!(signer.verify_cursor(&bytes, 1_700_000_000_000).is_err());
}

#[test]
fn purpose_mismatch_rejected() {
    let signer = TokenSigner::new([1u8; 32]);
    let page = signer.sign_page(&SyncPageToken {
        cursor: sample_cursor(1_700_000_000_000),
        upper_bound: 10,
        scanned_through: 3,
        round_started_ms: 1_700_000_000_000,
    });
    // page token must not verify as cursor
    assert!(signer.verify_cursor(&page, 1_700_000_000_000).is_err());
}

#[test]
fn expired_cursor_rejected() {
    let signer = TokenSigner::new([1u8; 32]);
    let issued = 1_700_000_000_000i64;
    let bytes = signer.sign_cursor(&sample_cursor(issued));
    let later = issued + 31 * 24 * 60 * 60 * 1000;
    assert!(matches!(
        signer.verify_cursor(&bytes, later),
        Err(TokenError::Expired)
    ));
}

#[test]
fn encryption_is_randomized_and_rotated_keys_remain_readable() {
    let old = TokenSigner::with_key_id([3; 32], 10);
    let cur = sample_cursor(1_700_000_000_000);
    let a = old.sign_cursor(&cur);
    let b = old.sign_cursor(&cur);
    assert_ne!(a, b);
    assert!(!a.windows(16).any(|w| w == cur.device_id));
    let mut rotated = TokenSigner::with_key_id([4; 32], 11);
    assert_eq!(
        rotated.verify_cursor(&a, cur.issued_at_ms),
        Err(TokenError::Invalid)
    );
    rotated.retain_verification_key(10, [3; 32]).unwrap();
    assert_eq!(rotated.verify_cursor(&a, cur.issued_at_ms).unwrap(), cur);
    assert!(old
        .verify_cursor(&rotated.sign_cursor(&cur), cur.issued_at_ms)
        .is_err());
}

#[test]
fn cursor_expires_at_exact_deadline_and_rejects_future_issuance() {
    let signer = TokenSigner::new([1; 32]);
    let cur = sample_cursor(1_700_000_000_000);
    let bytes = signer.sign_cursor(&cur);
    assert_eq!(
        signer.verify_cursor(&bytes, cur.issued_at_ms - 1),
        Err(TokenError::Invalid)
    );
    assert_eq!(
        signer.verify_cursor(&bytes, cur.issued_at_ms + 30 * 86_400_000),
        Err(TokenError::Expired)
    );
}
