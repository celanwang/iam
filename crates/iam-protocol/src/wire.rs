use crate::error::{ProtocolErrorKind, Result};
use crate::frame::{FixedHeader, Frame, HEADER_LEN};
use crate::msg_type::MsgType;
use crate::pb::{
    self, AuthRequest, AuthResponse, Head, HeartbeatAck, HeartbeatRequest, HistoryRequest,
    HistoryResponse, ProtocolError, Push, PushAck, ReconnectRequest, ReconnectResponse,
    SendRequest, SendResponse, SendStatusRequest, SendStatusResponse, SnapshotRequest,
    SnapshotResponse, SyncRequest, SyncResponse,
};
use crate::validate::{validate_head, validate_message_content, validate_sync_target};
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

fn decode_pb<T: Message + Default>(bytes: &[u8]) -> Result<T> {
    // Reject duplicate singular fields / multi-branch at a basic level by
    // scanning tags when needed for critical messages; prost merge is last-wins.
    // For Head we enforce via scan_no_duplicate_varint_field.
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
        if i >= 10 {
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
            let len = len as usize;
            if bytes.len() < n + len {
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
    if body.is_empty() {
        if msg_type.allows_empty_body() {
            return Ok(DecodedBody::Heartbeat(HeartbeatRequest {}));
        }
        return Err(ProtocolErrorKind::InvalidArgument("empty body"));
    }
    Ok(match msg_type {
        MsgType::Auth => DecodedBody::Auth(decode_pb(body)?),
        MsgType::AuthAck => DecodedBody::AuthAck(decode_pb(body)?),
        MsgType::Heartbeat => DecodedBody::Heartbeat(decode_pb(body)?),
        MsgType::HeartbeatAck => DecodedBody::HeartbeatAck(decode_pb(body)?),
        MsgType::Reconnect => DecodedBody::Reconnect(decode_pb(body)?),
        MsgType::ReconnectAck => DecodedBody::ReconnectAck(decode_pb(body)?),
        MsgType::Send => {
            if count_oneof_branches(body, 1, 63)? > 1 {
                // SendRequest itself isn't a pure oneof; check content later
            }
            let req: SendRequest = decode_pb(body)?;
            if let Some(content) = req.content.as_ref() {
                // unknown branch only: field present but value None after decode
                let content_bytes = extract_submessage(body, 2)?;
                if let Some(cb) = content_bytes {
                    let branches = count_oneof_branches(&cb, 1, 63)?;
                    if branches == 0 {
                        // might be empty message -> missing text
                    } else if branches == 1 {
                        // check if field 1
                        if has_unknown_only_branch(&cb, 1)? {
                            return Err(ProtocolErrorKind::UnsupportedContent);
                        }
                    } else {
                        return Err(ProtocolErrorKind::MalformedFrame("multi content branch"));
                    }
                }
                validate_message_content(content)?;
            }
            DecodedBody::Send(req)
        }
        MsgType::SendAck => DecodedBody::SendAck(decode_pb(body)?),
        MsgType::Push => DecodedBody::Push(decode_pb(body)?),
        MsgType::PushAck => DecodedBody::PushAck(decode_pb(body)?),
        MsgType::Sync => {
            let req: SyncRequest = decode_pb(body)?;
            if let Some(target) = req.target.as_ref() {
                let target_bytes = extract_submessage(body, 1)?;
                if let Some(tb) = target_bytes {
                    let branches = count_oneof_branches(&tb, 1, 63)?;
                    if branches > 1 {
                        return Err(ProtocolErrorKind::MalformedFrame("multi sync target"));
                    }
                    if branches == 1 && has_unknown_only_branch(&tb, /*known*/ 0)? {
                        // unknown only if neither 1 nor 2
                        if !field_present(&tb, 1)? && !field_present(&tb, 2)? {
                            return Err(ProtocolErrorKind::InvalidArgument("unknown sync target"));
                        }
                    }
                }
                validate_sync_target(target)?;
            }
            DecodedBody::Sync(req)
        }
        MsgType::SyncAck => DecodedBody::SyncAck(decode_pb(body)?),
        MsgType::Error => DecodedBody::Error(decode_pb(body)?),
        MsgType::SendStatus => DecodedBody::SendStatus(decode_pb(body)?),
        MsgType::SendStatusAck => DecodedBody::SendStatusAck(decode_pb(body)?),
        MsgType::Snapshot => DecodedBody::Snapshot(decode_pb(body)?),
        MsgType::SnapshotAck => DecodedBody::SnapshotAck(decode_pb(body)?),
        MsgType::History => DecodedBody::History(decode_pb(body)?),
        MsgType::HistoryAck => DecodedBody::HistoryAck(decode_pb(body)?),
    })
}

fn field_present(bytes: &[u8], field: u32) -> Result<bool> {
    let mut i = 0;
    while i < bytes.len() {
        let (key, n) = read_varint(&bytes[i..])?;
        i += n;
        let field_no = (key >> 3) as u32;
        let wire = (key & 0x7) as u8;
        if field_no == field {
            return Ok(true);
        }
        i += skip_value(&bytes[i..], wire)?;
    }
    Ok(false)
}

fn has_unknown_only_branch(bytes: &[u8], _known_hint: u32) -> Result<bool> {
    // true if any branch field in 1..=63 exists that isn't field 1 or 2 for SyncTarget,
    // or isn't field 1 for MessageContent. Caller decides known set via field_present.
    let mut i = 0;
    while i < bytes.len() {
        let (key, n) = read_varint(&bytes[i..])?;
        i += n;
        let field_no = (key >> 3) as u32;
        let wire = (key & 0x7) as u8;
        if (1..=63).contains(&field_no) && field_no != 1 && field_no != 2 {
            return Ok(true);
        }
        i += skip_value(&bytes[i..], wire)?;
    }
    Ok(false)
}

fn extract_submessage(bytes: &[u8], field: u32) -> Result<Option<Vec<u8>>> {
    let mut i = 0;
    while i < bytes.len() {
        let (key, n) = read_varint(&bytes[i..])?;
        i += n;
        let field_no = (key >> 3) as u32;
        let wire = (key & 0x7) as u8;
        if field_no == field {
            if wire != 2 {
                return Err(ProtocolErrorKind::MalformedFrame("expected len-delim"));
            }
            let (len, n) = read_varint(&bytes[i..])?;
            i += n;
            let len = len as usize;
            if bytes.len() < i + len {
                return Err(ProtocolErrorKind::MalformedFrame("short submessage"));
            }
            return Ok(Some(bytes[i..i + len].to_vec()));
        }
        i += skip_value(&bytes[i..], wire)?;
    }
    Ok(None)
}

pub fn decode_frame(frame: &Frame) -> Result<DecodedFrame> {
    let msg_type = MsgType::from_u8(frame.header.msg_type)
        .ok_or(ProtocolErrorKind::UnsupportedCommand(frame.header.msg_type))?;

    if frame.header.body_len == 0 && !msg_type.allows_empty_body() {
        return Err(ProtocolErrorKind::InvalidArgument("empty body"));
    }
    if msg_type.allows_empty_body() && frame.header.body_len != 0 {
        return Err(ProtocolErrorKind::InvalidArgument("heartbeat body must be empty"));
    }

    if !frame.head.is_empty() {
        if has_duplicate_field(&frame.head, 1)? {
            return Err(ProtocolErrorKind::MalformedFrame("duplicate req_id"));
        }
    }

    let head = if frame.head.is_empty() {
        Head { req_id: None }
    } else {
        decode_pb::<Head>(&frame.head)?
    };
    validate_head(msg_type, &head)?;

    let body = decode_body(msg_type, &frame.body)?;
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
