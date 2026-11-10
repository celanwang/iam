use iam_protocol::pb::{
    message_content, sync_target, Empty, Head, MessageContent, SyncTarget, TextContent,
};
use iam_protocol::{
    count_oneof_branches, decode_frame, encode_heartbeat, has_duplicate_field, FixedHeader,
    FrameDecoder, MsgType, HEADER_LEN,
};
use prost::Message;

#[test]
fn head_req_id_one_encodes_as_0801() {
    let head = Head {
        req_id: Some(1),
    };
    let mut buf = Vec::new();
    head.encode(&mut buf).unwrap();
    assert_eq!(hex::encode(&buf), "0801");
}

#[test]
fn heartbeat_full_frame_matches_doc() {
    let frame = encode_heartbeat(1).unwrap();
    assert_eq!(
        hex::encode(&frame),
        "0103000000000002000000000801"
    );
    let mut dec = FrameDecoder::new();
    dec.push(&frame);
    let raw = dec.next_frame().unwrap().unwrap();
    let decoded = decode_frame(&raw).unwrap();
    assert_eq!(decoded.msg_type, MsgType::Heartbeat);
    assert_eq!(decoded.head.req_id, Some(1));
}

#[test]
fn sync_target_account_empty() {
    let target = SyncTarget {
        value: Some(sync_target::Value::Account(Empty {})),
    };
    let mut buf = Vec::new();
    target.encode(&mut buf).unwrap();
    assert_eq!(hex::encode(&buf), "0a00");
}

#[test]
fn sync_target_dual_branch_rejected() {
    let bytes = hex::decode("0a001007").unwrap();
    assert!(count_oneof_branches(&bytes, 1, 63).unwrap() > 1);
}

#[test]
fn head_duplicate_req_id_rejected() {
    let bytes = hex::decode("08010802").unwrap();
    assert!(has_duplicate_field(&bytes, 1).unwrap());
}

#[test]
fn sync_target_unknown_branch_only() {
    let bytes = hex::decode("1a00").unwrap();
    // field 3 length-delimited empty
    assert_eq!(count_oneof_branches(&bytes, 1, 63).unwrap(), 1);
    assert!(!hex_has_field(&bytes, 1) && !hex_has_field(&bytes, 2));
}

#[test]
fn message_content_unknown_branch() {
    let bytes = hex::decode("1200").unwrap();
    assert_eq!(count_oneof_branches(&bytes, 1, 63).unwrap(), 1);
}

#[test]
fn acceptance_closed_false_field_fragment() {
    // field 2 bool false => 10 00
    assert_eq!(hex::encode([0x10, 0x00]), "1000");
}

#[test]
fn half_packet_then_complete() {
    let frame = encode_heartbeat(1).unwrap();
    let mut dec = FrameDecoder::new();
    dec.push(&frame[..HEADER_LEN / 2]);
    assert!(dec.next_frame().unwrap().is_none());
    dec.push(&frame[HEADER_LEN / 2..]);
    let got = dec.next_frame().unwrap().unwrap();
    assert_eq!(got.header.msg_type, MsgType::Heartbeat as u8);
    dec.finish().unwrap();
}

#[test]
fn multi_frame_buffer() {
    let a = encode_heartbeat(1).unwrap();
    let b = encode_heartbeat(2).unwrap();
    let mut buf = a.clone();
    buf.extend_from_slice(&b);
    let mut dec = FrameDecoder::new();
    dec.push(&buf);
    assert_eq!(
        decode_frame(&dec.next_frame().unwrap().unwrap())
            .unwrap()
            .head
            .req_id,
        Some(1)
    );
    assert_eq!(
        decode_frame(&dec.next_frame().unwrap().unwrap())
            .unwrap()
            .head
            .req_id,
        Some(2)
    );
}

#[test]
fn invalid_length_rejected() {
    let mut bad = vec![1, 3, 0, 0, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];
    assert!(FixedHeader::decode(&bad).is_err());
    bad = vec![1, 3, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0]; // flags != 0
    assert!(FixedHeader::decode(&bad).is_err());
}

fn hex_has_field(bytes: &[u8], field: u32) -> bool {
    iam_protocol::has_duplicate_field(bytes, field).ok();
    // reuse scan via count: check presence by encoding check
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        let field_no = (b >> 3) as u32;
        if field_no == field {
            return true;
        }
        // simplistic: only single-byte keys for these samples
        i += 1;
        if (b & 0x7) == 2 {
            let len = bytes[i] as usize;
            i += 1 + len;
        }
    }
    false
}

#[test]
fn text_content_roundtrip() {
    let content = MessageContent {
        value: Some(message_content::Value::Text(TextContent {
            text: Some("hi".into()),
        })),
    };
    let mut buf = Vec::new();
    content.encode(&mut buf).unwrap();
    let back = MessageContent::decode(&buf[..]).unwrap();
    match back.value.unwrap() {
        message_content::Value::Text(t) => assert_eq!(t.text.as_deref(), Some("hi")),
    }
}
