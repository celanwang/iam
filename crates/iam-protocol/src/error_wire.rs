//! Helpers to build wire-level `ProtocolError` frames.

use crate::pb::{ErrorCode, ErrorInfo, Head, ProtocolError, RetryAction};
use crate::wire::encode_frame;
use crate::{MsgType, Result};

pub fn encode_protocol_error(
    req_id: Option<u64>,
    ref_msg_type: Option<u32>,
    code: ErrorCode,
    action: RetryAction,
    close_connection: bool,
    detail: Option<&str>,
) -> Result<Vec<u8>> {
    let body = ProtocolError {
        ref_msg_type,
        error: Some(ErrorInfo {
            code: Some(code as i32),
            action: Some(action as i32),
            retry_after_ms: None,
            detail: detail.map(|s| s.chars().take(256).collect()),
        }),
        close_connection: Some(close_connection),
    };
    let head = Head { req_id };
    encode_frame(MsgType::Error, &head, &body)
}

pub fn error_info(code: ErrorCode, action: RetryAction) -> ErrorInfo {
    ErrorInfo {
        code: Some(code as i32),
        action: Some(action as i32),
        retry_after_ms: None,
        detail: None,
    }
}
