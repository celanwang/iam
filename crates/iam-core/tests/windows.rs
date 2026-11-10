use iam_core::Engine;
use iam_protocol::pb::{
    message_content, send_outcome, AuthRequest, Head, MessageContent, MessageKey, SendRequest,
    TextContent,
};
use iam_protocol::{
    decode_frame, encode_frame, id128_timestamp_ms, DecodedBody, FrameDecoder, MsgType,
};
use uuid::Uuid;

const T: i64 = 1_700_000_000_000;
const ACCEPT_UNTIL: i64 = T + 86_400_000;
const QUERY_UNTIL: i64 = T + 691_200_000;

fn client_msg_at(now_ms: i64) -> [u8; 16] {
    let mut bytes = Uuid::now_v7().into_bytes();
    bytes[0] = ((now_ms >> 40) & 0xff) as u8;
    bytes[1] = ((now_ms >> 32) & 0xff) as u8;
    bytes[2] = ((now_ms >> 24) & 0xff) as u8;
    bytes[3] = ((now_ms >> 16) & 0xff) as u8;
    bytes[4] = ((now_ms >> 8) & 0xff) as u8;
    bytes[5] = (now_ms & 0xff) as u8;
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    assert_eq!(id128_timestamp_ms(&bytes), now_ms as u64);
    bytes
}

fn auth(engine: &mut Engine) -> [u8; 16] {
    let device = Uuid::now_v7().into_bytes();
    let body = AuthRequest {
        access_token: Some(b"token-a".to_vec().into()),
        device_id: Some(device.to_vec().into()),
        requested_capabilities: vec![],
    };
    let bytes = encode_frame(MsgType::Auth, &Head { req_id: Some(1) }, &body).unwrap();
    let mut dec = FrameDecoder::new();
    dec.push(&bytes);
    let frame = decode_frame(&dec.next_frame().unwrap().unwrap()).unwrap();
    let (cid, _) = engine.handle(None, &frame).unwrap();
    cid.unwrap()
}

fn send(
    engine: &mut Engine,
    cid: [u8; 16],
    req_id: u64,
    cmid: [u8; 16],
    text: &str,
) -> iam_protocol::pb::SendResponse {
    let body = SendRequest {
        key: Some(MessageKey {
            conversation_id: Some(1),
            client_msg_id: Some(cmid.to_vec().into()),
        }),
        content: Some(MessageContent {
            value: Some(message_content::Value::Text(TextContent {
                text: Some(text.into()),
            })),
        }),
    };
    let bytes = encode_frame(MsgType::Send, &Head { req_id: Some(req_id) }, &body).unwrap();
    let mut dec = FrameDecoder::new();
    dec.push(&bytes);
    let frame = decode_frame(&dec.next_frame().unwrap().unwrap()).unwrap();
    let (_, out) = engine.handle(Some(cid), &frame).unwrap();
    match decode_frame({
        let mut d = FrameDecoder::new();
        d.push(&out.responses[0]);
        &d.next_frame().unwrap().unwrap()
    })
    .unwrap()
    .body
    {
        DecodedBody::SendAck(r) => r,
        other => panic!("expected send ack {other:?}"),
    }
}

fn seeded() -> Engine {
    let mut engine = Engine::new(T);
    engine.store.seed_user(1, 100, b"token-a".to_vec());
    engine.store.seed_user(1, 200, b"token-b".to_vec());
    engine.store.seed_direct(1, 100, 200);
    engine
}

#[test]
fn allow_at_skew_boundary_and_reject_beyond() {
    // Start clock at the skew boundary so max_observed does not mark rollback.
    let mut engine = Engine::new(T - 120_000);
    engine.store.seed_user(1, 100, b"token-a".to_vec());
    engine.store.seed_user(1, 200, b"token-b".to_vec());
    engine.store.seed_direct(1, 100, 200);
    let cid = auth(&mut engine);
    let ok = send(&mut engine, cid, 2, client_msg_at(T), "ok");
    assert!(matches!(
        ok.outcome.unwrap().value.unwrap(),
        send_outcome::Value::Committed(_)
    ));

    let mut engine = Engine::new(T - 120_001);
    engine.store.seed_user(1, 100, b"token-a".to_vec());
    engine.store.seed_user(1, 200, b"token-b".to_vec());
    engine.store.seed_direct(1, 100, 200);
    let cid = auth(&mut engine);
    let bad = send(&mut engine, cid, 2, client_msg_at(T), "skew");
    match bad.outcome.unwrap().value.unwrap() {
        send_outcome::Value::Unresolved(u) => {
            assert_eq!(
                u.error.unwrap().code,
                Some(iam_protocol::pb::ErrorCode::ClockSkew as i32)
            );
            assert_eq!(u.acceptance_closed, Some(false));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn exactly_at_accept_until_needs_close_watermark() {
    let mut engine = Engine::new(ACCEPT_UNTIL);
    engine.store.seed_user(1, 100, b"token-a".to_vec());
    engine.store.seed_user(1, 200, b"token-b".to_vec());
    engine.store.seed_direct(1, 100, 200);
    let cid = auth(&mut engine);
    let open = send(&mut engine, cid, 2, client_msg_at(T), "late");
    match open.outcome.unwrap().value.unwrap() {
        send_outcome::Value::Unresolved(u) => {
            assert_eq!(
                u.error.unwrap().code,
                Some(iam_protocol::pb::ErrorCode::InProgress as i32)
            );
        }
        other => panic!("{other:?}"),
    }

    // Unresolved without close keeps serial barrier; clear to probe watermark path.
    engine
        .store
        .connections
        .get_mut(&cid)
        .unwrap()
        .inflight_send
        .clear();
    engine.store.advance_close_watermark(ACCEPT_UNTIL);
    let closed = send(&mut engine, cid, 3, client_msg_at(T), "late2");
    match closed.outcome.unwrap().value.unwrap() {
        send_outcome::Value::RejectedFinal(e) => {
            assert_eq!(
                e.code,
                Some(iam_protocol::pb::ErrorCode::RequestExpired as i32)
            );
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn query_until_with_watermark_returns_result_expired() {
    let mut engine = seeded();
    engine.store.set_now(T);
    let cid = auth(&mut engine);
    let cmid = client_msg_at(T);
    let committed = send(&mut engine, cid, 2, cmid, "hello");
    assert!(matches!(
        committed.outcome.unwrap().value.unwrap(),
        send_outcome::Value::Committed(_)
    ));

    engine.store.set_now(QUERY_UNTIL);
    engine.store.advance_close_watermark(QUERY_UNTIL);
    let again = send(&mut engine, cid, 3, cmid, "hello");
    match again.outcome.unwrap().value.unwrap() {
        send_outcome::Value::Unresolved(u) => {
            assert_eq!(
                u.error.unwrap().code,
                Some(iam_protocol::pb::ErrorCode::ResultExpired as i32)
            );
            assert_eq!(u.acceptance_closed, Some(true));
        }
        other => panic!("{other:?}"),
    }
    engine.store.cleanup_expired_idempotency();
    assert!(engine.store.idempotency.is_empty());
}

#[test]
fn clock_rollback_stops_new_accepts() {
    let mut engine = seeded();
    engine.store.set_now(T);
    let cid = auth(&mut engine);
    engine.store.set_now(T - 1); // rollback without raising max_observed
    let resp = send(&mut engine, cid, 2, client_msg_at(T), "x");
    match resp.outcome.unwrap().value.unwrap() {
        send_outcome::Value::Unresolved(u) => {
            assert_eq!(
                u.error.unwrap().code,
                Some(iam_protocol::pb::ErrorCode::ResultUnavailable as i32)
            );
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn recovery_incomplete_stops_new_accepts() {
    let mut engine = seeded();
    engine.store.set_now(T);
    let cid = auth(&mut engine);
    engine.store.recovery_incomplete = true;
    let resp = send(&mut engine, cid, 2, client_msg_at(T), "x");
    match resp.outcome.unwrap().value.unwrap() {
        send_outcome::Value::Unresolved(u) => {
            assert_eq!(
                u.error.unwrap().code,
                Some(iam_protocol::pb::ErrorCode::ResultUnavailable as i32)
            );
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn retry_after_accept_window_still_returns_receipt() {
    let mut engine = seeded();
    engine.store.set_now(T);
    let cid = auth(&mut engine);
    let cmid = client_msg_at(T);
    let first = send(&mut engine, cid, 2, cmid, "hello");
    let mid1 = match first.outcome.unwrap().value.unwrap() {
        send_outcome::Value::Committed(r) => r.r#ref.unwrap().message_id.unwrap(),
        other => panic!("{other:?}"),
    };
    engine.store.set_now(ACCEPT_UNTIL + 1);
    let second = send(&mut engine, cid, 3, cmid, "hello");
    match second.outcome.unwrap().value.unwrap() {
        send_outcome::Value::Committed(r) => {
            assert_eq!(r.r#ref.unwrap().message_id.unwrap(), mid1);
        }
        other => panic!("{other:?}"),
    }
}
