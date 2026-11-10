use iam_loop::{
    parse_error, parse_history_ack, parse_sync_ack, LoopWorld,
};
use iam_protocol::pb::{
    change, history_response, sync_response, sync_target, ErrorCode, Head, HeartbeatRequest,
    PushAck, RetryAction,
};
use iam_protocol::{encode_frame, MsgType};

#[test]
fn error_unauthenticated_heartbeat() {
    let mut world = LoopWorld::new(1_700_000_000_000);
    let bytes = encode_frame(
        MsgType::Heartbeat,
        &Head { req_id: Some(1) },
        &HeartbeatRequest {},
    )
    .unwrap();
    let (_, out) = world.dispatch_raw(None, &bytes);
    let err = parse_error(&out.responses[0]);
    assert_eq!(
        err.error.as_ref().unwrap().code,
        Some(ErrorCode::Unauthenticated as i32)
    );
    assert_eq!(
        err.error.as_ref().unwrap().action,
        Some(RetryAction::Reauthenticate as i32)
    );
    assert_eq!(err.close_connection, Some(false));
}

#[test]
fn error_req_id_not_increasing_closes() {
    let mut world = LoopWorld::new(1_700_000_000_000);
    world.auth("a", b"token-a");
    let cid = world.peers.get("a").unwrap().connection_id;
    let ok = encode_frame(
        MsgType::Heartbeat,
        &Head { req_id: Some(2) },
        &HeartbeatRequest {},
    )
    .unwrap();
    let (_, out) = world.dispatch_raw(cid, &ok);
    assert!(!out.responses.is_empty());
    let again = encode_frame(
        MsgType::Heartbeat,
        &Head { req_id: Some(2) },
        &HeartbeatRequest {},
    )
    .unwrap();
    let (_, out) = world.dispatch_raw(cid, &again);
    let err = parse_error(&out.responses[0]);
    assert_eq!(
        err.error.as_ref().unwrap().code,
        Some(ErrorCode::InvalidArgument as i32)
    );
    assert_eq!(err.close_connection, Some(true));
}

#[test]
fn error_unknown_command() {
    let mut world = LoopWorld::new(1_700_000_000_000);
    world.auth("a", b"token-a");
    let cid = world.peers.get("a").unwrap().connection_id;
    // Server-only AUTH_ACK used as client command → unsupported command.
    let body = iam_protocol::pb::AuthResponse {
        value: Some(iam_protocol::pb::auth_response::Value::Error(
            iam_protocol::pb::ErrorInfo {
                code: Some(ErrorCode::InternalError as i32),
                action: Some(RetryAction::None as i32),
                retry_after_ms: None,
                detail: None,
            },
        )),
    };
    let bytes = encode_frame(MsgType::AuthAck, &Head { req_id: Some(2) }, &body).unwrap();
    let (_, out) = world.dispatch_raw(cid, &bytes);
    let err = parse_error(&out.responses[0]);
    assert_eq!(
        err.error.as_ref().unwrap().code,
        Some(ErrorCode::UnsupportedCommand as i32)
    );
    assert_eq!(err.close_connection, Some(false));
}

#[test]
fn error_push_ack_without_capability() {
    let mut world = LoopWorld::new(1_700_000_000_000);
    // auth without push_ack
    let (req_id, device_id) = {
        let peer = world.peers.get_mut("a").unwrap();
        (peer.next_req_id(), peer.device_id)
    };
    let body = iam_protocol::pb::AuthRequest {
        access_token: Some(b"token-a".to_vec().into()),
        device_id: Some(device_id.to_vec().into()),
        requested_capabilities: vec![],
    };
    let bytes = encode_frame(
        MsgType::Auth,
        &Head {
            req_id: Some(req_id),
        },
        &body,
    )
    .unwrap();
    let (cid, _) = world.dispatch_raw(None, &bytes);
    world.peers.get_mut("a").unwrap().connection_id = cid;

    let bytes = encode_frame(
        MsgType::PushAck,
        &Head { req_id: None },
        &PushAck { push_id: Some(1) },
    )
    .unwrap();
    let (_, out) = world.dispatch_raw(cid, &bytes);
    let err = parse_error(&out.responses[0]);
    assert_eq!(
        err.error.as_ref().unwrap().code,
        Some(ErrorCode::UnsupportedCapability as i32)
    );
    assert_eq!(err.close_connection, Some(false));
}

#[test]
fn entrance_overloaded_rate_limited() {
    let mut world = LoopWorld::new(1_700_000_000_000);
    world.auth("a", b"token-a");
    world.engine.store.entrance_overloaded = true;
    let cid = world.peers.get("a").unwrap().connection_id;
    let bytes = encode_frame(
        MsgType::Heartbeat,
        &Head { req_id: Some(2) },
        &HeartbeatRequest {},
    )
    .unwrap();
    let (_, out) = world.dispatch_raw(cid, &bytes);
    let err = parse_error(&out.responses[0]);
    assert_eq!(
        err.error.as_ref().unwrap().code,
        Some(ErrorCode::RateLimited as i32)
    );
}

#[test]
fn account_sync_cold_start_reset_then_snapshot_conversations() {
    let mut world = LoopWorld::new(1_700_000_000_000);
    world.auth("a", b"token-a");
    let sync = world.sync_account("a", None);
    match parse_sync_ack(&sync).value.unwrap() {
        sync_response::Value::Reset(r) => {
            assert!(r.snapshot_token.is_some());
            assert_eq!(
                r.reason,
                Some(iam_protocol::pb::ResetReason::InitialSync as i32)
            );
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn history_returns_committed_messages_descending() {
    let now = 1_700_000_000_000i64;
    let mut world = LoopWorld::new(now);
    world.auth("a", b"token-a");
    world.auth("b", b"token-b");
    // two messages
    for (i, text) in [(0, "one"), (1, "two")].iter() {
        let mut bytes = uuid::Uuid::now_v7().into_bytes();
        let t = now + *i;
        bytes[0] = ((t >> 40) & 0xff) as u8;
        bytes[1] = ((t >> 32) & 0xff) as u8;
        bytes[2] = ((t >> 24) & 0xff) as u8;
        bytes[3] = ((t >> 16) & 0xff) as u8;
        bytes[4] = ((t >> 8) & 0xff) as u8;
        bytes[5] = (t & 0xff) as u8;
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        let _ = world.send_text("a", 1, bytes, text);
    }
    let hist = world.history("a", 1, None);
    match parse_history_ack(&hist).value.unwrap() {
        history_response::Value::Page(page) => {
            assert!(page.messages.len() >= 2);
            let s0 = page.messages[0]
                .position
                .as_ref()
                .unwrap()
                .create_seq
                .unwrap();
            let s1 = page.messages[1]
                .position
                .as_ref()
                .unwrap()
                .create_seq
                .unwrap();
            assert!(s0 > s1);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn tampered_sync_cursor_rejected() {
    let mut world = LoopWorld::new(1_700_000_000_000);
    world.auth("a", b"token-a");
    let page = world.recover_conversation_latest("a", 1);
    let mut cursor = page.next_cursor.unwrap().to_vec();
    let last = cursor.len() - 1;
    cursor[last] ^= 0xff;
    let sync = world.sync_conversation("a", 1, Some(cursor));
    match parse_sync_ack(&sync).value.unwrap() {
        sync_response::Value::Error(e) => {
            assert_eq!(e.code, Some(ErrorCode::InvalidCursor as i32));
        }
        other => panic!("{other:?}"),
    }
}

#[allow(dead_code)]
fn _touch(c: change::Payload, t: sync_target::Value) {
    let _ = (c, t);
}
