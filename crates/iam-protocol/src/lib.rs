//! Frame codec and Protobuf types for the IM message protocol.

mod error;
mod error_wire;
mod frame;
mod id128;
mod msg_type;
mod validate;
mod wire;
mod wire_check;

pub use error::{ProtocolErrorKind, Result};
pub use error_wire::{encode_protocol_error, error_info};
pub use frame::{FixedHeader, Frame, FrameDecoder, FRAME_HARD_MAX, HEADER_LEN, HEAD_HARD_MAX};
pub use id128::{
    generate_id128, id128_timestamp_ms, id128_to_string, parse_id128_bytes, validate_id128, Id128,
};
pub use msg_type::MsgType;
pub use validate::{validate_head, validate_message_content, validate_sync_target};
pub use wire::{
    count_oneof_branches, decode_body, decode_frame, encode_frame, encode_heartbeat,
    has_duplicate_field, DecodedBody, DecodedFrame,
};

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/iam.protocol.rs"));
}
