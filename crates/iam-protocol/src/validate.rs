use crate::error::{ProtocolErrorKind, Result};
use crate::msg_type::MsgType;
use crate::pb::{self, ErrorCode, Head, MessageContent, MessageState, RetryAction, SyncTarget};
use crate::{parse_id128_bytes, FRAME_HARD_MAX, HEAD_HARD_MAX};

pub fn validate_head(msg_type: MsgType, head: &Head) -> Result<()> {
    match head.req_id {
        Some(0) if !msg_type.allows_zero_req_id() => {
            Err(ProtocolErrorKind::InvalidArgument("req_id zero"))
        }
        None if !msg_type.allows_zero_req_id() => {
            Err(ProtocolErrorKind::InvalidArgument("req_id missing"))
        }
        Some(0) | None => Ok(()),
        Some(_) => Ok(()),
    }
}

pub fn validate_message_content(content: &MessageContent) -> Result<()> {
    match content.value.as_ref() {
        Some(pb::message_content::Value::Text(t)) => {
            let text = t
                .text
                .as_ref()
                .ok_or(ProtocolErrorKind::InvalidArgument("text missing"))?;
            if text.is_empty() {
                return Err(ProtocolErrorKind::InvalidArgument("text empty"));
            }
            if text.len() > 64 * 1024 {
                return Err(ProtocolErrorKind::ResourceLimit("text too large"));
            }
            if !text.is_ascii() && std::str::from_utf8(text.as_bytes()).is_err() {
                return Err(ProtocolErrorKind::MalformedFrame("invalid utf8"));
            }
            // String from prost is already UTF-8
            Ok(())
        }
        None => Err(ProtocolErrorKind::UnsupportedContent),
    }
}

pub fn validate_sync_target(target: &SyncTarget) -> Result<()> {
    match target.value.as_ref() {
        Some(pb::sync_target::Value::Account(_)) => Ok(()),
        Some(pb::sync_target::Value::Conversation(id)) if *id != 0 => Ok(()),
        Some(pb::sync_target::Value::Conversation(_)) => {
            Err(ProtocolErrorKind::InvalidArgument("conversation id zero"))
        }
        None => Err(ProtocolErrorKind::InvalidArgument("sync target missing")),
    }
}

pub fn validate_error_code(code: ErrorCode) -> Result<()> {
    if code == ErrorCode::Unspecified {
        Err(ProtocolErrorKind::InvalidArgument("error code unspecified"))
    } else {
        Ok(())
    }
}

pub fn validate_retry_action(action: RetryAction) -> Result<()> {
    if action == RetryAction::Unspecified {
        Err(ProtocolErrorKind::InvalidArgument("retry action unspecified"))
    } else {
        Ok(())
    }
}

pub fn validate_message_state(state: MessageState) -> Result<()> {
    if state == MessageState::Unspecified {
        Err(ProtocolErrorKind::InvalidArgument("message state unspecified"))
    } else {
        Ok(())
    }
}

pub fn validate_id_field(bytes: &[u8]) -> Result<[u8; 16]> {
    parse_id128_bytes(bytes)
}

pub fn default_limits_ok(max_frame: u32, max_head: u32) -> bool {
    max_frame <= FRAME_HARD_MAX && max_head <= HEAD_HARD_MAX
}
