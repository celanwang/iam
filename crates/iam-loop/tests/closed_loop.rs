use iam_loop::{committed_message_id, parse_send_ack, parse_sync_ack, LoopWorld};
use iam_protocol::pb::{send_outcome, sync_response};
use iam_protocol::{generate_id128, id128_timestamp_ms};
use uuid::Uuid;

fn client_msg_at(now_ms: i64) -> [u8; 16] {
    // Build UUIDv7-like id with fixed timestamp bits from now_ms
    let mut bytes = Uuid::now_v7().into_bytes();
    bytes[0] = ((now_ms >> 40) & 0xff) as u8;
    bytes[1] = ((now_ms >> 32) & 0xff) as u8;
    bytes[2] = ((now_ms >> 24) & 0xff) as u8;
    bytes[3] = ((now_ms >> 16) & 0xff) as u8;
    bytes[4] = ((now_ms >> 8) & 0xff) as u8;
    bytes[5] = (now_ms & 0xff) as u8;
    // force version 7 / variant
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    assert_eq!(id128_timestamp_ms(&bytes), now_ms as u64);
    bytes
}

#[test]
fn send_push_and_sync_after_drop() {
    let now = 1_700_000_000_000i64;
    let mut world = LoopWorld::new(now);
    world.auth("a", b"token-a");
    world.auth("b", b"token-b");

    let cmid = client_msg_at(now);
    let ack = world.send_text("a", 1, cmid, "hello");
    let resp = parse_send_ack(&ack);
    let mid = committed_message_id(&resp);

    // B should have a push
    let pushes = world.drain_pushes("b");
    assert_eq!(pushes.len(), 1);

    // Drop scenario: clear pushes and sync
    world.drop_pushes("b");
    let sync = world.sync_conversation("b", 1, None);
    let sync_resp = parse_sync_ack(&sync);
    match sync_resp.value.unwrap() {
        sync_response::Value::Page(page) => {
            assert!(!page.changes.is_empty());
            assert_eq!(page.covered_through, page.upper_bound);
            let upsert = page.changes[0].payload.as_ref().unwrap();
            match upsert {
                iam_protocol::pb::change::Payload::MessageUpsert(v) => {
                    let got = v.r#ref.as_ref().unwrap().message_id.as_ref().unwrap();
                    assert_eq!(&got[..], &mid[..]);
                }
                _ => panic!("expected message upsert"),
            }
        }
        other => panic!("expected page {other:?}"),
    }
}

#[test]
fn idempotent_retry_same_message_id() {
    let now = 1_700_000_000_000i64;
    let mut world = LoopWorld::new(now);
    world.auth("a", b"token-a");
    world.auth("b", b"token-b");
    let cmid = client_msg_at(now);
    let ack1 = world.send_text("a", 1, cmid, "hello");
    let mid1 = committed_message_id(&parse_send_ack(&ack1));
    let ack2 = world.send_text("a", 1, cmid, "hello");
    let mid2 = committed_message_id(&parse_send_ack(&ack2));
    assert_eq!(mid1, mid2);
}

#[test]
fn same_key_different_content_conflicts() {
    let now = 1_700_000_000_000i64;
    let mut world = LoopWorld::new(now);
    world.auth("a", b"token-a");
    let cmid = client_msg_at(now);
    let _ = world.send_text("a", 1, cmid, "hello");
    let ack = world.send_text("a", 1, cmid, "other");
    let resp = parse_send_ack(&ack);
    match resp.outcome.unwrap().value.unwrap() {
        send_outcome::Value::Unresolved(u) => {
            assert_eq!(
                u.error.as_ref().unwrap().code,
                Some(iam_protocol::pb::ErrorCode::IdempotencyConflict as i32)
            );
            assert_eq!(u.acceptance_closed, Some(true));
        }
        other => panic!("expected unresolved conflict {other:?}"),
    }
}

#[test]
fn serial_blocks_second_key_while_first_inflight_unresolved() {
    // Simulate by sending second before clearing inflight — engine marks inflight on check_req_id
    // After commit, inflight is cleared; to test barrier we inject manually.
    let now = 1_700_000_000_000i64;
    let mut world = LoopWorld::new(now);
    world.auth("a", b"token-a");
    let cid = world.peers.get("a").unwrap().connection_id.unwrap();
    let cmid_old = client_msg_at(now - 1000);
    if let Some(conn) = world.engine.store.connections.get_mut(&cid) {
        conn.inflight_send.insert(1, cmid_old);
    }
    let cmid_new = client_msg_at(now);
    let ack = world.send_text("a", 1, cmid_new, "second");
    let resp = parse_send_ack(&ack);
    match resp.outcome.unwrap().value.unwrap() {
        send_outcome::Value::Unresolved(u) => {
            assert_eq!(
                u.error.as_ref().unwrap().code,
                Some(iam_protocol::pb::ErrorCode::InProgress as i32)
            );
        }
        other => panic!("expected in progress {other:?}"),
    }
}

#[test]
fn generate_id_smoke() {
    let _ = generate_id128();
}
