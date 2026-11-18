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
    let head = Head { req_id: Some(1) };
    let mut buf = Vec::new();
    head.encode(&mut buf).unwrap();
    assert_eq!(hex::encode(&buf), "0801");
}

#[test]
fn heartbeat_full_frame_matches_doc() {
    let frame = encode_heartbeat(1).unwrap();
    assert_eq!(hex::encode(&frame), "0103000000000002000000000801");
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

fn raw_body(kind: MsgType, body: &[u8]) -> iam_protocol::Result<iam_protocol::DecodedBody> {
    iam_protocol::decode_body(kind, body)
}

#[test]
fn friend_wire_commands_and_empty_pending_list() {
    use iam_protocol::pb::*;
    use iam_protocol::{encode_frame, DecodedBody};

    let head = Head { req_id: Some(1) };
    let bytes = encode_frame(
        MsgType::FriendRequest,
        &head,
        &FriendRequest {
            target_user_id: Some(42),
        },
    )
    .unwrap();
    assert_eq!(hex::encode(bytes), "0114000000000002000000020801082a");

    let page = FriendListAck {
        from_user_ids: vec![],
        next_after_user_id: None,
    };
    let bytes = encode_frame(MsgType::FriendListAck, &head, &page).unwrap();
    assert_eq!(hex::encode(&bytes), "0117000000000002000000000801");
    let mut decoder = FrameDecoder::new();
    decoder.push(&bytes);
    let frame = decoder.next_frame().unwrap().unwrap();
    assert!(
        matches!(decode_frame(&frame).unwrap().body, DecodedBody::FriendListAck(ack) if ack == page)
    );
    assert!(!MsgType::FriendListAck.is_client_to_server());

    assert!(matches!(
        raw_body(MsgType::FriendList, &[8, 0]).unwrap(),
        DecodedBody::FriendList(FriendListRequest {
            after_user_id: Some(0)
        })
    ));
    assert!(matches!(
        raw_body(MsgType::FriendRequestAck, &[10, 0]).unwrap(),
        DecodedBody::FriendRequestAck(FriendRequestAck {
            value: Some(friend_request_ack::Value::Pending(_)),
        })
    ));
    assert!(matches!(
        raw_body(MsgType::FriendAcceptAck, &[8, 42]).unwrap(),
        DecodedBody::FriendAcceptAck(FriendAcceptAck {
            conversation_id: Some(42)
        })
    ));
}

#[test]
fn friend_wire_rejects_conflicting_fields_and_invalid_results() {
    for (kind, body) in [
        (MsgType::FriendRequest, &[8, 1, 8, 2][..]),
        (MsgType::FriendRequestAck, &[10, 0, 16, 42]),
        (MsgType::FriendRequestAck, &[16, 0]),
        (MsgType::FriendListAck, &[8, 2, 8, 1]),
        (MsgType::FriendListAck, &[8, 1, 16, 1]),
        (MsgType::FriendAcceptAck, &[8, 0]),
    ] {
        assert!(raw_body(kind, body).is_err(), "{kind:?} {body:?}");
    }
}

#[test]
fn friend_decision_wire_preserves_an_explicit_rejection() {
    use iam_protocol::pb::*;
    use iam_protocol::DecodedBody;

    let id = hex::decode("018bcfe5680070008000000000000001").unwrap();
    let reject = FriendDecideRequest {
        from_user_id: Some(42),
        request_id: Some(id.into()),
        accept: Some(false),
    };
    let body = reject.encode_to_vec();
    assert_eq!(
        hex::encode(&body),
        "082a1210018bcfe56800700080000000000000011800"
    );
    assert!(matches!(
        raw_body(MsgType::FriendDecide, &body).unwrap(),
        DecodedBody::FriendDecide(value) if value == reject
    ));
    // An applied request can be pending. A decision response always has a terminal outcome.
    assert!(raw_body(MsgType::FriendApplyAck, &[10, 0]).is_ok());
    assert!(raw_body(MsgType::FriendDecideAck, &[10, 0]).is_err());
    assert!(matches!(
        raw_body(MsgType::FriendDecideAck, &[26, 0]).unwrap(),
        DecodedBody::FriendDecideAck(FriendAttemptAck {
            value: Some(friend_attempt_ack::Value::Rejected(_)),
        })
    ));
}

#[test]
fn friend_inbox_validates_and_preserves_request_identity() {
    use iam_protocol::pb::*;
    use iam_protocol::{encode_frame, DecodedBody};

    let request_id = iam_protocol::generate_id128().to_vec();
    let mut page = FriendInboxAck {
        requests: vec![FriendApplicant {
            from_user_id: Some(u64::MAX),
            request_id: Some(request_id.into()),
        }],
        next_after_user_id: None,
    };
    let head = Head { req_id: Some(1) };
    for expected in [FriendInboxAck::default(), page.clone()] {
        let bytes = encode_frame(MsgType::FriendInboxAck, &head, &expected).unwrap();
        let mut decoder = FrameDecoder::new();
        decoder.push(&bytes);
        let frame = decoder.next_frame().unwrap().unwrap();
        assert!(
            matches!(decode_frame(&frame).unwrap().body, DecodedBody::FriendInboxAck(ack) if ack == expected)
        );
    }
    page.requests[0].request_id = None;
    assert!(raw_body(MsgType::FriendInboxAck, &page.encode_to_vec()).is_err());
    page.requests[0].request_id = Some(vec![0; 16].into());
    assert!(raw_body(MsgType::FriendInboxAck, &page.encode_to_vec()).is_err());
}

#[test]
fn group_create_wire_round_trip_and_required_ack_id() {
    use iam_protocol::pb::{GroupCreateAck, GroupCreateRequest};
    use iam_protocol::{encode_frame, DecodedBody};

    let id = hex::decode("018bcfe5680070008000000000000001").unwrap();
    let request = GroupCreateRequest {
        request_id: Some(id.into()),
        member_user_ids: vec![7, 8],
    };
    let head = Head { req_id: Some(1) };
    let bytes = encode_frame(MsgType::GroupCreate, &head, &request).unwrap();
    assert_eq!(
        hex::encode(&bytes),
        "01200000000000020000001608010a10018bcfe568007000800000000000000110071008"
    );
    let mut decoder = FrameDecoder::new();
    decoder.push(&bytes);
    let frame = decoder.next_frame().unwrap().unwrap();
    assert!(
        matches!(decode_frame(&frame).unwrap().body, DecodedBody::GroupCreate(value) if value == request)
    );
    assert!(MsgType::GroupCreate.is_client_to_server());
    assert!(!MsgType::GroupCreateAck.is_client_to_server());
    assert!(matches!(
        raw_body(MsgType::GroupCreateAck, &[8, 42]).unwrap(),
        DecodedBody::GroupCreateAck(GroupCreateAck {
            conversation_id: Some(42)
        })
    ));
    assert!(raw_body(MsgType::GroupCreateAck, &[8, 0]).is_err());
    assert!(raw_body(MsgType::GroupCreateAck, &[8, 42, 8, 43]).is_err());
}

#[test]
fn friend_removal_wire_requires_a_valid_status_generation() {
    use iam_protocol::pb::*;
    use iam_protocol::{encode_frame, DecodedBody};

    let generation = hex::decode("018bcfe5680070008000000000000001").unwrap();
    let remove = FriendRemoveRequest {
        peer_user_id: Some(42),
        generation: Some(generation.clone().into()),
    };
    let bytes = encode_frame(MsgType::FriendRemove, &Head { req_id: Some(1) }, &remove).unwrap();
    assert_eq!(
        hex::encode(&bytes),
        "0126000000000002000000140801082a1210018bcfe5680070008000000000000001"
    );
    let mut decoder = FrameDecoder::new();
    decoder.push(&bytes);
    let frame = decoder.next_frame().unwrap().unwrap();
    assert!(
        matches!(decode_frame(&frame).unwrap().body, DecodedBody::FriendRemove(value) if value == remove)
    );
    assert!(matches!(
        raw_body(MsgType::FriendStatusAck, &[10, 0]).unwrap(),
        DecodedBody::FriendStatusAck(FriendStatusAck {
            value: Some(friend_status_ack::Value::NotFriends(_)),
        })
    ));

    let status = |id: Option<Vec<u8>>| FriendStatusAck {
        value: Some(friend_status_ack::Value::Relationship(FriendRelationship {
            conversation_id: Some(42),
            generation: id.map(Into::into),
        })),
    };
    assert!(raw_body(
        MsgType::FriendStatusAck,
        &status(Some(generation)).encode_to_vec()
    )
    .is_ok());
    assert!(raw_body(MsgType::FriendStatusAck, &status(None).encode_to_vec()).is_err());
    assert!(raw_body(
        MsgType::FriendStatusAck,
        &status(Some(vec![0; 16])).encode_to_vec()
    )
    .is_err());
    assert!(raw_body(MsgType::FriendRemoveAck, &[8, 0]).is_err());
}

#[test]
fn user_block_wire_preserves_an_explicit_unblock() {
    use iam_protocol::pb::*;
    use iam_protocol::{encode_frame, DecodedBody};

    let request = UserBlockRequest {
        peer_user_id: Some(42),
        request_id: Some(
            hex::decode("018bcfe5680070008000000000000001")
                .unwrap()
                .into(),
        ),
        blocked: Some(false),
    };
    let bytes = encode_frame(MsgType::UserBlock, &Head { req_id: Some(1) }, &request).unwrap();
    assert_eq!(
        hex::encode(&bytes),
        "012a000000000002000000160801082a1210018bcfe56800700080000000000000011800"
    );
    let mut decoder = FrameDecoder::new();
    decoder.push(&bytes);
    let frame = decoder.next_frame().unwrap().unwrap();
    assert!(
        matches!(decode_frame(&frame).unwrap().body, DecodedBody::UserBlock(value) if value == request)
    );
    assert!(matches!(
        raw_body(MsgType::UserBlock, &[8, 42]).unwrap(),
        DecodedBody::UserBlock(UserBlockRequest {
            request_id: None,
            blocked: None,
            ..
        })
    ));
    assert!(matches!(
        raw_body(MsgType::UserBlockAck, &[8, 0]).unwrap(),
        DecodedBody::UserBlockAck(UserBlockAck {
            blocked: Some(false)
        })
    ));
    assert!(raw_body(MsgType::UserBlockAck, &[]).is_err());
    assert!(raw_body(MsgType::UserBlockAck, &[8, 2]).is_err());
    assert!(raw_body(MsgType::UserBlock, &[8, 42, 24, 0, 24, 1]).is_err());
}

#[test]
fn group_invite_wire_preserves_decline_and_validates_inbox() {
    use iam_protocol::pb::*;
    use iam_protocol::{encode_frame, DecodedBody};

    let id: prost::bytes::Bytes = hex::decode("018bcfe5680070008000000000000001")
        .unwrap()
        .into();
    let decide = GroupInviteDecideRequest {
        conversation_id: Some(42),
        request_id: Some(id.clone()),
        accept: Some(false),
    };
    let head = Head { req_id: Some(1) };
    let bytes = encode_frame(MsgType::GroupInviteDecide, &head, &decide).unwrap();
    assert_eq!(
        hex::encode(&bytes),
        "0134000000000002000000160801082a1210018bcfe56800700080000000000000011800"
    );
    let mut decoder = FrameDecoder::new();
    decoder.push(&bytes);
    let frame = decoder.next_frame().unwrap().unwrap();
    assert!(
        matches!(decode_frame(&frame).unwrap().body, DecodedBody::GroupInviteDecide(value) if value == decide)
    );
    assert!(raw_body(MsgType::GroupInviteDecideAck, &[10, 0]).is_err());
    assert!(raw_body(MsgType::GroupInviteDecideAck, &[26, 0]).is_ok());
    assert!(raw_body(MsgType::GroupInviteInboxAck, &[]).is_ok());

    let mut page = GroupInviteInboxAck {
        invitations: vec![GroupInvitation {
            conversation_id: Some(42),
            inviter_user_id: Some(1),
            request_id: Some(id),
        }],
        next_after_conversation_id: None,
    };
    assert!(
        matches!(raw_body(MsgType::GroupInviteInboxAck, &page.encode_to_vec()).unwrap(), DecodedBody::GroupInviteInboxAck(decoded) if decoded == page)
    );
    page.invitations.push(page.invitations[0].clone());
    assert!(raw_body(MsgType::GroupInviteInboxAck, &page.encode_to_vec()).is_err());
    page.invitations.pop();
    page.invitations[0].inviter_user_id = Some(0);
    assert!(raw_body(MsgType::GroupInviteInboxAck, &page.encode_to_vec()).is_err());
}

#[test]
fn recursive_wire_validation_rejects_duplicates_and_multiple_branches() {
    // Duplicate SendRequest.key and nested MessageKey.conversation_id.
    for body in [&[0x0a, 0, 0x0a, 0][..], &[0x0a, 4, 8, 1, 8, 1][..]] {
        assert!(matches!(
            raw_body(MsgType::Send, body),
            Err(iam_protocol::ProtocolErrorKind::MalformedFrame(_))
        ));
    }
    // AUTH_ACK contains both a known and an unknown oneof branch.
    let err = iam_protocol::pb::ErrorInfo {
        code: Some(1),
        action: Some(1),
        ..Default::default()
    }
    .encode_to_vec();
    let mut response = vec![0x12, err.len() as u8];
    response.extend(err);
    response.extend([0x1a, 0]);
    assert!(matches!(
        raw_body(MsgType::AuthAck, &response),
        Err(iam_protocol::ProtocolErrorKind::MalformedFrame(_))
    ));
}

#[test]
fn scalar_wire_values_are_not_truncated_or_coerced() {
    // ProtocolError.close_connection = 2 and PageLimit.max_items > uint32.
    assert!(matches!(
        raw_body(MsgType::Error, &[0x18, 2]),
        Err(iam_protocol::ProtocolErrorKind::MalformedFrame(_))
    ));
    assert!(matches!(
        raw_body(MsgType::Sync, &[0x22, 6, 8, 0x80, 0x80, 0x80, 0x80, 0x10]),
        Err(iam_protocol::ProtocolErrorKind::MalformedFrame(_))
    ));
    let mut overflow = vec![8];
    overflow.extend([0xff; 9]);
    overflow.push(2);
    assert!(raw_body(MsgType::HeartbeatAck, &overflow).is_err());
    assert!(raw_body(MsgType::HeartbeatAck, &[0]).is_err());
}

#[test]
fn unknown_write_fields_and_unknown_response_branches_are_not_discarded() {
    assert!(matches!(
        raw_body(MsgType::Send, &[0xa0, 6, 1]),
        Err(iam_protocol::ProtocolErrorKind::UnsupportedContent)
    ));
    assert!(matches!(
        raw_body(MsgType::AuthAck, &[0x1a, 0]),
        Err(iam_protocol::ProtocolErrorKind::UnsupportedCapability)
    ));
    // An ordinary unknown field is allowed beside a valid heartbeat response.
    assert!(raw_body(MsgType::HeartbeatAck, &[8, 1, 0xa0, 6, 1]).is_ok());
}

#[test]
fn missing_required_response_fields_and_field_floods_are_rejected() {
    assert!(raw_body(MsgType::SendStatusAck, &[0x0a, 0]).is_err());
    let mut flood = Vec::new();
    for _ in 0..16385 {
        flood.extend([0xa0, 6, 1]);
    }
    assert!(matches!(
        raw_body(MsgType::Auth, &flood),
        Err(iam_protocol::ProtocolErrorKind::MalformedFrame(_))
    ));
}

#[test]
fn bounded_frame_decoder_stays_failed_after_overflow() {
    let mut d = FrameDecoder::new();
    d.push(&vec![0; iam_protocol::FRAME_HARD_MAX as usize + 1]);
    assert!(d.next_frame().is_err());
    d.push(&encode_heartbeat(1).unwrap());
    assert!(d.next_frame().is_err());
    assert!(d.finish().is_err());
}

#[test]
fn contradictory_send_outcome_is_rejected() {
    use iam_protocol::pb::*;
    let id = iam_protocol::generate_id128().to_vec();
    let body = SendStatusResponse {
        key: Some(MessageKey {
            conversation_id: Some(1),
            client_msg_id: Some(id.into()),
        }),
        outcome: Some(SendOutcome {
            value: Some(send_outcome::Value::Unresolved(SendUnresolved {
                error: Some(iam_protocol::error_info(
                    ErrorCode::ResultExpired,
                    RetryAction::None,
                )),
                acceptance_closed: Some(false),
            })),
        }),
    };
    assert!(raw_body(MsgType::SendStatusAck, &body.encode_to_vec()).is_err());
}
