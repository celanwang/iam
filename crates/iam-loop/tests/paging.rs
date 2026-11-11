use iam_loop::{parse_history_ack, parse_snapshot_ack, parse_sync_ack, LoopWorld};
use iam_protocol::{encode_frame, generate_id128, pb::*, FrameDecoder, MsgType};
use prost::Message;

const NOW: i64 = 1_700_000_000_000;
fn world() -> LoopWorld {
    let mut w = LoopWorld::new(NOW);
    w.auth("a", b"token-a");
    w
}
fn send(w: &mut LoopWorld, text: &str) {
    let mut id = generate_id128();
    id[..6].copy_from_slice(&NOW.to_be_bytes()[2..]);
    let ack = w.send_text("a", 1, id, text);
    assert!(matches!(
        iam_loop::parse_send_ack(&ack).outcome.unwrap().value,
        Some(send_outcome::Value::Committed(_))
    ));
}
fn limit(items: u32, bytes: u32) -> Option<PageLimit> {
    Some(PageLimit {
        max_items: Some(items),
        max_bytes: Some(bytes),
    })
}
fn call(w: &mut LoopWorld, kind: MsgType, req: &impl Message) -> Vec<u8> {
    let peer = w.peers.get_mut("a").unwrap();
    let id = peer.next_req_id();
    let cid = peer.connection_id;
    let bytes = encode_frame(kind, &Head { req_id: Some(id) }, req).unwrap();
    w.dispatch_raw(cid, &bytes).1.responses.remove(0)
}
fn snapshot_token(w: &mut LoopWorld, account: bool) -> Vec<u8> {
    let bytes = if account {
        w.sync_account("a", None)
    } else {
        w.sync_conversation("a", 1, None)
    };
    match parse_sync_ack(&bytes).value.unwrap() {
        sync_response::Value::Reset(r) => r.snapshot_token.unwrap().to_vec(),
        other => panic!("{other:?}"),
    }
}
fn snapshot(w: &mut LoopWorld, token: Vec<u8>, items: u32, bytes: u32) -> snapshot_response::Value {
    parse_snapshot_ack(&call(
        w,
        MsgType::Snapshot,
        &SnapshotRequest {
            page_token: Some(token.into()),
            limit: limit(items, bytes),
        },
    ))
    .value
    .unwrap()
}
fn context(w: &LoopWorld) -> StreamContext {
    {
        let c = &w.engine.store.conversations[&(1, 1)];
        StreamContext {
            target: Some(SyncTarget {
                value: Some(sync_target::Value::Conversation(1)),
            }),
            epoch: Some(c.history_id.to_vec().into()),
            access_version: Some(c.access_version),
        }
    }
}
fn assert_budget(frame: &[u8], bytes: usize) {
    let mut d = FrameDecoder::new();
    d.push(frame);
    assert!(d.next_frame().unwrap().unwrap().body.len() <= bytes);
}

#[test]
fn snapshot_and_history_keep_their_original_upper_bound() {
    let mut w = world();
    send(&mut w, "first");
    send(&mut w, "second");
    let token = snapshot_token(&mut w, false);
    let first = match snapshot(&mut w, token.clone(), 1, 4096) {
        snapshot_response::Value::Page(p) => p,
        other => panic!("{other:?}"),
    };
    let ctx = context(&w);
    let h = call(
        &mut w,
        MsgType::History,
        &HistoryRequest {
            context: Some(ctx.clone()),
            limit: limit(1, 4096),
            ..Default::default()
        },
    );
    let h = match parse_history_ack(&h).value.unwrap() {
        history_response::Value::Page(p) => p,
        other => panic!("{other:?}"),
    };
    send(&mut w, "third");
    let repeated = match snapshot(&mut w, token, 1, 4096) {
        snapshot_response::Value::Page(p) => p,
        other => panic!("{other:?}"),
    };
    assert_eq!(first.items, repeated.items);
    let second = match snapshot(&mut w, first.next_page_token.unwrap().to_vec(), 100, 4096) {
        snapshot_response::Value::Page(p) => p,
        other => panic!("{other:?}"),
    };
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.upper_bound, Some(2));
    assert!(second.resume_cursor.is_some());
    let next = call(
        &mut w,
        MsgType::History,
        &HistoryRequest {
            context: Some(ctx),
            page_token: h.next_page_token,
            limit: limit(100, 4096),
            ..Default::default()
        },
    );
    match parse_history_ack(&next).value.unwrap() {
        history_response::Value::Page(p) => {
            assert_eq!(p.upper_bound, Some(2));
            assert_eq!(p.messages.len(), 1);
            assert_eq!(p.messages[0].position.as_ref().unwrap().create_seq, Some(1));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn account_snapshot_is_sorted_frozen_and_rechecks_permissions() {
    let mut w = world();
    w.engine.store.seed_direct(1, 3, 100, 200);
    w.engine.store.seed_direct(1, 2, 100, 200);
    let token = snapshot_token(&mut w, true);
    let first = match snapshot(&mut w, token.clone(), 1, 4096) {
        snapshot_response::Value::Page(p) => p,
        other => panic!("{other:?}"),
    };
    w.engine.store.seed_direct(1, 4, 100, 200);
    let rest = match snapshot(
        &mut w,
        first.next_page_token.clone().unwrap().to_vec(),
        100,
        4096,
    ) {
        snapshot_response::Value::Page(p) => p,
        other => panic!("{other:?}"),
    };
    let ids: Vec<_> = rest
        .items
        .iter()
        .map(|i| match i.value.as_ref().unwrap() {
            snapshot_item::Value::Conversation(c) => c.conversation_id.unwrap(),
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids, vec![2, 3]);
    w.engine
        .store
        .conversations
        .get_mut(&(1, 2))
        .unwrap()
        .members
        .remove(&100);
    assert!(
        matches!(snapshot(&mut w, token, 100, 4096), snapshot_response::Value::Error(e) if e.code == Some(ErrorCode::ContextChanged as i32))
    );
}

#[test]
fn every_page_budget_includes_envelope_context_and_tokens() {
    let mut w = world();
    let cursor = w.recover_conversation_latest("a", 1).next_cursor;
    for _ in 0..4 {
        send(&mut w, &"x".repeat(600));
    }
    let token = snapshot_token(&mut w, false);
    let ctx = context(&w);
    let snap = call(
        &mut w,
        MsgType::Snapshot,
        &SnapshotRequest {
            page_token: Some(token.clone().into()),
            limit: limit(100, 1200),
        },
    );
    assert_budget(&snap, 1200);
    assert!(matches!(
        parse_snapshot_ack(&snap).value,
        Some(snapshot_response::Value::Page(_))
    ));
    let hist = call(
        &mut w,
        MsgType::History,
        &HistoryRequest {
            context: Some(ctx.clone()),
            limit: limit(100, 1200),
            ..Default::default()
        },
    );
    assert_budget(&hist, 1200);
    assert!(matches!(
        parse_history_ack(&hist).value,
        Some(history_response::Value::Page(_))
    ));
    let sync = call(
        &mut w,
        MsgType::Sync,
        &SyncRequest {
            target: ctx.target,
            cursor,
            page_token: None,
            limit: limit(100, 1200),
        },
    );
    assert_budget(&sync, 1200);
    assert!(matches!(
        parse_sync_ack(&sync).value,
        Some(sync_response::Value::Page(_))
    ));
    assert!(
        matches!(snapshot(&mut w, token, 100, 1), snapshot_response::Value::Error(e) if e.code == Some(ErrorCode::PageBudgetTooSmall as i32))
    );
}

#[test]
fn stale_history_context_is_rejected_on_first_page() {
    let mut w = world();
    let mut ctx = context(&w);
    ctx.access_version = Some(2);
    let result = call(
        &mut w,
        MsgType::History,
        &HistoryRequest {
            context: Some(ctx),
            limit: limit(100, 4096),
            ..Default::default()
        },
    );
    assert!(
        matches!(parse_history_ack(&result).value, Some(history_response::Value::Error(e)) if e.code == Some(ErrorCode::ContextChanged as i32))
    );
}
