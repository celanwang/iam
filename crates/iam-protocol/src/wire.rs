use crate::error::{ProtocolErrorKind, Result};
use crate::frame::{FixedHeader, Frame, HEADER_LEN};
use crate::msg_type::MsgType;
use crate::pb::{
    AuthRequest, AuthResponse, Head, HeartbeatAck, HeartbeatRequest, HistoryRequest,
    HistoryResponse, ProtocolError, Push, PushAck, ReconnectRequest, ReconnectResponse,
    SendRequest, SendResponse, SendStatusRequest, SendStatusResponse, SnapshotRequest,
    SnapshotResponse, SyncRequest, SyncResponse,
};
use crate::validate::validate_head;
use prost::Message;

#[derive(Debug, Clone)]
pub enum DecodedBody {
    Auth(AuthRequest),
    AuthAck(AuthResponse),
    Heartbeat(HeartbeatRequest),
    HeartbeatAck(HeartbeatAck),
    Reconnect(ReconnectRequest),
    ReconnectAck(ReconnectResponse),
    Send(SendRequest),
    SendAck(SendResponse),
    Push(Push),
    PushAck(PushAck),
    Sync(SyncRequest),
    SyncAck(SyncResponse),
    Error(ProtocolError),
    SendStatus(SendStatusRequest),
    SendStatusAck(SendStatusResponse),
    Snapshot(SnapshotRequest),
    SnapshotAck(SnapshotResponse),
    History(HistoryRequest),
    HistoryAck(HistoryResponse),
}

#[derive(Debug, Clone)]
pub struct DecodedFrame {
    pub msg_type: MsgType,
    pub head: Head,
    pub body: DecodedBody,
}

fn decode_pb<T: Message + Default + prost::Name>(bytes: &[u8], fields: &mut usize) -> Result<T> {
    crate::wire_check::check(&T::full_name(), bytes, fields)?;
    T::decode(bytes).map_err(|e| ProtocolErrorKind::Decode(e.to_string()))
}

/// Scan protobuf for duplicate occurrence of a singular field number.
pub fn has_duplicate_field(bytes: &[u8], field: u32) -> Result<bool> {
    let mut seen = false;
    let mut i = 0;
    while i < bytes.len() {
        let (key, n) = read_varint(&bytes[i..])?;
        i += n;
        let field_no = (key >> 3) as u32;
        let wire = (key & 0x7) as u8;
        if field_no == field {
            if seen {
                return Ok(true);
            }
            seen = true;
        }
        i += skip_value(&bytes[i..], wire)?;
    }
    Ok(false)
}

/// Count set oneof branch field numbers in range lo..=hi (inclusive).
pub fn count_oneof_branches(bytes: &[u8], lo: u32, hi: u32) -> Result<usize> {
    let mut count = 0;
    let mut i = 0;
    while i < bytes.len() {
        let (key, n) = read_varint(&bytes[i..])?;
        i += n;
        let field_no = (key >> 3) as u32;
        let wire = (key & 0x7) as u8;
        if field_no >= lo && field_no <= hi {
            count += 1;
        }
        i += skip_value(&bytes[i..], wire)?;
    }
    Ok(count)
}

fn read_varint(bytes: &[u8]) -> Result<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0;
    for (i, b) in bytes.iter().enumerate() {
        if i >= 10 || (i == 9 && *b > 1) {
            return Err(ProtocolErrorKind::MalformedFrame("varint overflow"));
        }
        result |= ((*b as u64) & 0x7f) << shift;
        if *b & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }
    Err(ProtocolErrorKind::MalformedFrame("truncated varint"))
}

fn skip_value(bytes: &[u8], wire: u8) -> Result<usize> {
    match wire {
        0 => {
            let (_, n) = read_varint(bytes)?;
            Ok(n)
        }
        1 => {
            if bytes.len() < 8 {
                return Err(ProtocolErrorKind::MalformedFrame("short fixed64"));
            }
            Ok(8)
        }
        2 => {
            let (len, n) = read_varint(bytes)?;
            let len = usize::try_from(len)
                .map_err(|_| ProtocolErrorKind::MalformedFrame("length overflow"))?;
            if len > bytes.len().saturating_sub(n) {
                return Err(ProtocolErrorKind::MalformedFrame("short len-delim"));
            }
            Ok(n + len)
        }
        5 => {
            if bytes.len() < 4 {
                return Err(ProtocolErrorKind::MalformedFrame("short fixed32"));
            }
            Ok(4)
        }
        _ => Err(ProtocolErrorKind::MalformedFrame("bad wire type")),
    }
}

pub fn decode_body(msg_type: MsgType, body: &[u8]) -> Result<DecodedBody> {
    decode_body_counted(msg_type, body, &mut 0)
}

fn decode_body_counted(msg_type: MsgType, body: &[u8], fields: &mut usize) -> Result<DecodedBody> {
    if body.is_empty() {
        if msg_type.allows_empty_body() {
            return Ok(DecodedBody::Heartbeat(HeartbeatRequest {}));
        }
        return Err(ProtocolErrorKind::InvalidArgument("empty body"));
    }
    let decoded = match msg_type {
        MsgType::Auth => DecodedBody::Auth(decode_pb(body, fields)?),
        MsgType::AuthAck => DecodedBody::AuthAck(decode_pb(body, fields)?),
        MsgType::Heartbeat => DecodedBody::Heartbeat(decode_pb(body, fields)?),
        MsgType::HeartbeatAck => DecodedBody::HeartbeatAck(decode_pb(body, fields)?),
        MsgType::Reconnect => DecodedBody::Reconnect(decode_pb(body, fields)?),
        MsgType::ReconnectAck => DecodedBody::ReconnectAck(decode_pb(body, fields)?),
        MsgType::Send => DecodedBody::Send(decode_pb(body, fields)?),
        MsgType::SendAck => DecodedBody::SendAck(decode_pb(body, fields)?),
        MsgType::Push => DecodedBody::Push(decode_pb(body, fields)?),
        MsgType::PushAck => DecodedBody::PushAck(decode_pb(body, fields)?),
        MsgType::Sync => DecodedBody::Sync(decode_pb(body, fields)?),
        MsgType::SyncAck => DecodedBody::SyncAck(decode_pb(body, fields)?),
        MsgType::Error => DecodedBody::Error(decode_pb(body, fields)?),
        MsgType::SendStatus => DecodedBody::SendStatus(decode_pb(body, fields)?),
        MsgType::SendStatusAck => DecodedBody::SendStatusAck(decode_pb(body, fields)?),
        MsgType::Snapshot => DecodedBody::Snapshot(decode_pb(body, fields)?),
        MsgType::SnapshotAck => DecodedBody::SnapshotAck(decode_pb(body, fields)?),
        MsgType::History => DecodedBody::History(decode_pb(body, fields)?),
        MsgType::HistoryAck => DecodedBody::HistoryAck(decode_pb(body, fields)?),
    };
    crate::validate::validate_response(&decoded)?;
    Ok(decoded)
}

pub fn decode_frame(frame: &Frame) -> Result<DecodedFrame> {
    frame.header.validate_lengths()?;
    if frame.head.len() != frame.header.head_len as usize
        || frame.body.len() != frame.header.body_len as usize
    {
        return Err(ProtocolErrorKind::MalformedFrame("frame length mismatch"));
    }
    let msg_type = MsgType::from_u8(frame.header.msg_type)
        .ok_or(ProtocolErrorKind::UnsupportedCommand(frame.header.msg_type))?;

    if frame.header.body_len == 0 && !msg_type.allows_empty_body() {
        return Err(ProtocolErrorKind::InvalidArgument("empty body"));
    }
    if msg_type.allows_empty_body() && frame.header.body_len != 0 {
        return Err(ProtocolErrorKind::InvalidArgument(
            "heartbeat body must be empty",
        ));
    }

    let mut fields = 0;
    let head = if frame.head.is_empty() {
        Head { req_id: None }
    } else {
        decode_pb::<Head>(&frame.head, &mut fields)?
    };
    validate_head(msg_type, &head)?;

    let body = decode_body_counted(msg_type, &frame.body, &mut fields)?;
    Ok(DecodedFrame {
        msg_type,
        head,
        body,
    })
}

pub fn encode_frame(msg_type: MsgType, head: &Head, body: &impl Message) -> Result<Vec<u8>> {
    let head_bytes = if head.req_id.is_none() && msg_type.allows_zero_req_id() {
        Vec::new()
    } else {
        let mut buf = Vec::new();
        head.encode(&mut buf)
            .map_err(|e| ProtocolErrorKind::Decode(e.to_string()))?;
        buf
    };
    let mut body_bytes = Vec::new();
    if !msg_type.allows_empty_body() {
        body.encode(&mut body_bytes)
            .map_err(|e| ProtocolErrorKind::Decode(e.to_string()))?;
    }
    let header = FixedHeader::new(msg_type, head_bytes.len() as u32, body_bytes.len() as u32);
    header.validate_lengths()?;
    let mut out = Vec::with_capacity(HEADER_LEN + head_bytes.len() + body_bytes.len());
    header.encode(&mut out);
    out.extend_from_slice(&head_bytes);
    out.extend_from_slice(&body_bytes);
    Ok(out)
}

pub fn encode_heartbeat(req_id: u64) -> Result<Vec<u8>> {
    let head = Head {
        req_id: Some(req_id),
    };
    encode_frame(MsgType::Heartbeat, &head, &HeartbeatRequest {})
}
