use iam_protocol::pb::{ReadPositionAck, ReadPositionRequest};
use iam_protocol::{decode_body, generate_id128, DecodedBody, MsgType, ProtocolErrorKind};
use prost::Message;

#[test]
fn empty_and_nonempty_read_position_acknowledgements() {
    assert!(matches!(
        decode_body(MsgType::ReadPositionAck, &[]).unwrap(),
        DecodedBody::ReadPositionAck(ReadPositionAck { read_through: None })
    ));
    let ack = ReadPositionAck {
        read_through: Some(u64::MAX),
    };
    assert!(matches!(
        decode_body(MsgType::ReadPositionAck, &ack.encode_to_vec()).unwrap(),
        DecodedBody::ReadPositionAck(decoded) if decoded == ack
    ));
    assert!(matches!(
        decode_body(MsgType::ReadPositionAck, &[8, 0]),
        Err(ProtocolErrorKind::InvalidArgument(_))
    ));
}

#[test]
fn read_position_requests_reject_unknown_fields() {
    let request = ReadPositionRequest {
        conversation_id: Some(1),
        history_id: Some(generate_id128().to_vec().into()),
        access_version: Some(1),
        read_through: Some(1),
    };
    let mut bytes = request.encode_to_vec();
    assert!(matches!(
        decode_body(MsgType::ReadPosition, &bytes).unwrap(),
        DecodedBody::ReadPosition(decoded) if decoded == request
    ));
    bytes.extend_from_slice(&[40, 1]);
    assert!(matches!(
        decode_body(MsgType::ReadPosition, &bytes),
        Err(ProtocolErrorKind::UnsupportedContent)
    ));
}
