//! Validate wire occurrences before prost can merge or discard them.
use crate::{ProtocolErrorKind as E, Result};
use prost::Message;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FileDescriptorSet,
};
use std::{
    collections::{HashMap, HashSet},
    sync::OnceLock,
};

fn schema() -> &'static HashMap<String, DescriptorProto> {
    static SCHEMA: OnceLock<HashMap<String, DescriptorProto>> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        let files = FileDescriptorSet::decode(
            include_bytes!(concat!(env!("OUT_DIR"), "/protocol.bin")).as_slice(),
        )
        .expect("built descriptor");
        files
            .file
            .into_iter()
            .flat_map(|f| {
                let package = f.package.unwrap();
                f.message_type
                    .into_iter()
                    .map(move |m| (format!("{package}.{}", m.name.as_deref().unwrap()), m))
            })
            .collect()
    })
}

pub(crate) fn check(name: &str, bytes: &[u8], total: &mut usize) -> Result<()> {
    let response = name.ends_with("Response")
        || name.ends_with("Ack")
        || name.ends_with(".Push")
        || name.ends_with(".ProtocolError");
    visit(
        name,
        bytes,
        1,
        total,
        name.ends_with(".SendRequest"),
        response,
    )
}

fn varint(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let b = *bytes
            .get(*offset)
            .ok_or(E::MalformedFrame("truncated varint"))?;
        *offset += 1;
        if shift == 63 && b > 1 {
            return Err(E::MalformedFrame("varint overflow"));
        }
        value |= u64::from(b & 127) << shift;
        if b < 128 {
            return Ok(value);
        }
    }
    Err(E::MalformedFrame("varint overflow"))
}

fn visit(
    name: &str,
    bytes: &[u8],
    depth: usize,
    total: &mut usize,
    strict_write: bool,
    response: bool,
) -> Result<()> {
    if depth > 16 {
        return Err(E::MalformedFrame("nesting limit"));
    }
    let desc = schema().get(name).expect("known message descriptor");
    let short = desc.name.as_deref().unwrap();
    let branch_start = if short == "Change" {
        Some(2)
    } else if desc
        .field
        .iter()
        .any(|f| f.oneof_index.is_some() && !f.proto3_optional.unwrap_or(false))
    {
        Some(1)
    } else {
        None
    };
    let mut offset = 0;
    let mut seen = HashSet::new();
    let mut branches = 0;
    let mut unknown = false;
    let mut unknown_branch = false;
    while offset < bytes.len() {
        *total += 1;
        if *total > 16384 {
            return Err(E::MalformedFrame("field count limit"));
        }
        let key = varint(bytes, &mut offset)?;
        let number = key >> 3;
        let wire = key & 7;
        if number == 0 || number > 0x1fff_ffff {
            return Err(E::MalformedFrame("invalid field number"));
        }
        let field = desc.field.iter().find(|f| f.number == Some(number as i32));
        if branch_start.is_some_and(|lo| number >= lo && number <= 63) {
            branches += 1;
            if branches > 1 {
                return Err(E::MalformedFrame("multiple oneof branches"));
            }
            if field.is_none() {
                unknown_branch = true;
            }
        }
        if let Some(f) = field {
            if f.label() != Label::Repeated && !seen.insert(number) {
                return Err(E::MalformedFrame("duplicate singular field"));
            }
            let expected = match f.r#type() {
                Type::String | Type::Bytes | Type::Message => 2,
                Type::Fixed64 | Type::Sfixed64 | Type::Double => 1,
                Type::Fixed32 | Type::Sfixed32 | Type::Float => 5,
                _ => 0,
            };
            if wire != expected {
                return Err(E::MalformedFrame("wrong wire type"));
            }
        } else {
            unknown = true;
        }
        match wire {
            0 => {
                let value = varint(bytes, &mut offset)?;
                if let Some(f) = field {
                    if (f.r#type() == Type::Bool && value > 1)
                        || (f.r#type() == Type::Uint32 && value > u32::MAX as u64)
                        || (f.r#type() == Type::Enum
                            && value > i32::MAX as u64
                            && value < i32::MIN as i64 as u64)
                    {
                        return Err(E::MalformedFrame("scalar overflow"));
                    }
                    if response {
                        if f.r#type() == Type::Int64 && value > i64::MAX as u64 {
                            return Err(E::InvalidArgument("negative timestamp"));
                        }
                        if f.r#type() == Type::Enum {
                            if value == 0 {
                                return Err(E::InvalidArgument("unspecified enum"));
                            }
                            let max = match f
                                .type_name
                                .as_deref()
                                .unwrap_or("")
                                .rsplit('.')
                                .next()
                                .unwrap()
                            {
                                "ErrorCode" => 26,
                                "RetryAction" => 8,
                                "MessageState" => 3,
                                "ConversationKind" => 2,
                                "ResetReason" => 4,
                                _ => 0,
                            };
                            if value > max {
                                return Err(E::UnsupportedCapability);
                            }
                        }
                        if value == 0
                            && matches!(
                                f.name.as_deref(),
                                Some(
                                    "tenant_id"
                                        | "user_id"
                                        | "conversation_id"
                                        | "conversation"
                                        | "sender_id"
                                        | "create_seq"
                                        | "revision_seq"
                                        | "event_seq"
                                        | "push_id"
                                        | "access_version"
                                        | "retention_floor_seq"
                                )
                            )
                        {
                            return Err(E::InvalidArgument("zero identifier or position"));
                        }
                    }
                }
            }
            1 | 2 | 5 => {
                let len = if wire == 2 {
                    usize::try_from(varint(bytes, &mut offset)?)
                        .map_err(|_| E::MalformedFrame("length overflow"))?
                } else if wire == 1 {
                    8
                } else {
                    4
                };
                if len > bytes.len() - offset {
                    return Err(E::MalformedFrame("truncated field"));
                }
                let value = &bytes[offset..offset + len];
                offset += len;
                if let Some(f) = field {
                    if f.r#type() == Type::Message {
                        visit(
                            f.type_name.as_deref().unwrap().trim_start_matches('.'),
                            value,
                            depth + 1,
                            total,
                            strict_write,
                            response,
                        )?;
                    } else if f.r#type() == Type::String {
                        std::str::from_utf8(value)
                            .map_err(|_| E::MalformedFrame("invalid utf8"))?;
                        if response && f.name.as_deref() == Some("detail") && len > 256 {
                            return Err(E::InvalidArgument("detail too long"));
                        }
                    } else if response && f.r#type() == Type::Bytes {
                        let field_name = f.name.as_deref().unwrap();
                        if field_name.ends_with("_id") || field_name == "epoch" {
                            crate::validate_id128(value)?;
                        } else if value.is_empty() || len > 4096 {
                            return Err(E::InvalidArgument("token length"));
                        }
                    }
                }
            }
            _ => return Err(E::MalformedFrame("invalid wire type")),
        }
    }
    if strict_write && unknown {
        return Err(E::UnsupportedContent);
    }
    if unknown_branch {
        return Err(E::UnsupportedCapability);
    }
    if response {
        if branch_start.is_some() && branches == 0 {
            return Err(E::InvalidArgument("missing oneof branch"));
        }
        for f in &desc.field {
            if f.label() == Label::Repeated
                || (f.oneof_index.is_some() && !f.proto3_optional.unwrap_or(false))
            {
                continue;
            }
            if !optional(short, f.name.as_deref().unwrap())
                && !seen.contains(&(f.number.unwrap() as u64))
            {
                return Err(E::InvalidArgument("missing required field"));
            }
        }
    }
    Ok(())
}

fn optional(message: &str, field: &str) -> bool {
    matches!(
        (message, field),
        ("Head", "req_id")
            | ("ErrorInfo", "retry_after_ms" | "detail")
            | ("MessageView", "content")
            | ("StreamContext", "access_version")
            | ("SyncPage", "next_page_token")
            | (
                "SnapshotPage",
                "history_window" | "next_page_token" | "resume_cursor"
            )
            | ("HistoryPage", "next_page_token")
            | ("ProtocolError", "ref_msg_type")
    )
}
