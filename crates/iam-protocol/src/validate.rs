use crate::error::{ProtocolErrorKind, Result};
use crate::msg_type::MsgType;
use crate::pb::{self, Head, MessageContent, SyncTarget};

pub fn validate_head(msg_type: MsgType, head: &Head) -> Result<()> {
    match head.req_id {
        Some(0) if !msg_type.allows_zero_req_id() => {
            Err(ProtocolErrorKind::InvalidArgument("req_id zero"))
        }
        None if !msg_type.allows_zero_req_id() => {
            Err(ProtocolErrorKind::InvalidArgument("req_id missing"))
        }
        Some(r) if r != 0 && matches!(msg_type, MsgType::Push | MsgType::PushAck) => Err(
            ProtocolErrorKind::InvalidArgument("push req_id must be zero"),
        ),
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

// Presence and scalar checks run before decoding. These checks relate fields.
pub(crate) fn validate_response(body: &crate::DecodedBody) -> Result<()> {
    use crate::pb::*;
    use crate::DecodedBody as B;
    match body {
        B::AuthAck(r) => {
            if let Some(auth_response::Value::Session(s)) = &r.value {
                session(s)?;
            }
        }
        B::ReconnectAck(r) => {
            if let Some(reconnect_response::Value::Session(s)) = &r.value {
                session(s)?;
            }
        }
        B::SendAck(r) => outcome(r.key.as_ref().unwrap(), r.outcome.as_ref().unwrap())?,
        B::SendStatusAck(r) => outcome(r.key.as_ref().unwrap(), r.outcome.as_ref().unwrap())?,
        B::Push(p) => {
            if p.changes.is_empty() {
                return invalid("empty push");
            }
            changes(p.context.as_ref().unwrap(), &p.changes, u64::MAX)?;
        }
        B::SyncAck(r) => {
            if let Some(sync_response::Value::Page(p)) = &r.value {
                let covered = p.covered_through.unwrap();
                let upper = p.upper_bound.unwrap();
                if covered > upper || p.next_page_token.is_some() != (covered < upper) {
                    return invalid("sync page boundary");
                }
                changes(p.context.as_ref().unwrap(), &p.changes, covered)?;
            }
        }
        B::SnapshotAck(r) => {
            if let Some(snapshot_response::Value::Page(p)) = &r.value {
                let ctx = p.context.as_ref().unwrap();
                context(ctx)?;
                if p.next_page_token.is_some() == p.resume_cursor.is_some() {
                    return invalid("snapshot continuation");
                }
                let conv = conversation(ctx);
                if conv.is_some() != p.history_window.is_some() {
                    return invalid("snapshot history window");
                }
                if let Some(w) = &p.history_window {
                    let (from, through) =
                        (w.from_create_seq.unwrap(), w.through_create_seq.unwrap());
                    if !((from == 0 && through == 0)
                        || (from > 0 && from <= through && through <= p.upper_bound.unwrap()))
                    {
                        return invalid("snapshot window bounds");
                    }
                }
                let mut last = 0;
                let mut seen = std::collections::HashSet::new();
                for item in &p.items {
                    match &item.value {
                        Some(snapshot_item::Value::Conversation(c)) if conv.is_none() => {
                            if !seen.insert(c.conversation_id.unwrap()) {
                                return invalid("duplicate snapshot conversation");
                            }
                        }
                        Some(snapshot_item::Value::Message(m)) if conv.is_some() => {
                            message(m, ctx, p.upper_bound.unwrap())?;
                            let seq = m.position.as_ref().unwrap().create_seq.unwrap();
                            let w = p.history_window.as_ref().unwrap();
                            if seq <= last
                                || seq < w.from_create_seq.unwrap()
                                || seq > w.through_create_seq.unwrap()
                            {
                                return invalid("snapshot message order");
                            }
                            last = seq;
                        }
                        _ => return invalid("snapshot item target"),
                    }
                }
            }
        }
        B::HistoryAck(r) => {
            if let Some(history_response::Value::Page(p)) = &r.value {
                let ctx = p.context.as_ref().unwrap();
                context(ctx)?;
                if conversation(ctx).is_none() {
                    return invalid("history account target");
                }
                let mut last = None;
                for m in &p.messages {
                    message(m, ctx, p.upper_bound.unwrap())?;
                    let seq = m.position.as_ref().unwrap().create_seq.unwrap();
                    if last.is_some_and(|n| seq >= n) || seq < p.retention_floor_seq.unwrap() {
                        return invalid("history message order");
                    }
                    last = Some(seq);
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn invalid<T>(reason: &'static str) -> Result<T> {
    Err(ProtocolErrorKind::InvalidArgument(reason))
}
fn conversation(ctx: &pb::StreamContext) -> Option<u64> {
    match ctx.target.as_ref().and_then(|t| t.value.as_ref()) {
        Some(pb::sync_target::Value::Conversation(id)) => Some(*id),
        _ => None,
    }
}
fn context(ctx: &pb::StreamContext) -> Result<()> {
    if conversation(ctx).is_some() != ctx.access_version.is_some() {
        return invalid("stream access version");
    }
    Ok(())
}
fn message(m: &pb::MessageView, ctx: &pb::StreamContext, upper: u64) -> Result<()> {
    let position = m.position.as_ref().unwrap();
    if m.r#ref.as_ref().unwrap().conversation_id != conversation(ctx)
        || position.history_id != ctx.epoch
        || m.revision_seq.unwrap() < position.create_seq.unwrap()
        || m.revision_seq.unwrap() > upper
    {
        return invalid("message context or revision");
    }
    if m.state == Some(pb::MessageState::Active as i32) {
        validate_message_content(
            m.content
                .as_ref()
                .ok_or(ProtocolErrorKind::InvalidArgument(
                    "active message content missing",
                ))?,
        )?;
    } else if m.content.is_some() {
        return invalid("removed message contains content");
    }
    Ok(())
}
fn changes(ctx: &pb::StreamContext, list: &[pb::Change], covered: u64) -> Result<()> {
    context(ctx)?;
    let mut last = 0;
    for c in list {
        let seq = c.event_seq.unwrap();
        if seq <= last || seq > covered {
            return invalid("change order or coverage");
        }
        last = seq;
        match &c.payload {
            Some(
                pb::change::Payload::ConversationUpsert(_)
                | pb::change::Payload::ConversationRemove(_),
            ) if conversation(ctx).is_none() => {}
            Some(pb::change::Payload::MessageUpsert(m)) if conversation(ctx).is_some() => {
                message(m, ctx, seq)?;
                if m.revision_seq != c.event_seq {
                    return invalid("change revision");
                }
            }
            Some(pb::change::Payload::MessageRemove(r))
                if r.conversation_id == conversation(ctx) && conversation(ctx).is_some() => {}
            _ => return invalid("change target"),
        }
    }
    Ok(())
}
fn outcome(key: &pb::MessageKey, result: &pb::SendOutcome) -> Result<()> {
    use pb::{send_outcome::Value, ErrorCode as C, RetryAction as A};
    match result.value.as_ref().unwrap() {
        Value::Committed(r) => {
            if r.r#ref.as_ref().unwrap().conversation_id != key.conversation_id {
                return invalid("send receipt conversation");
            }
        }
        Value::RejectedFinal(e) => {
            if !matches!(
                e.code(),
                C::InvalidArgument | C::PermissionDenied | C::ResourceLimit | C::RequestExpired
            ) || e.action() != A::None
                || e.retry_after_ms.is_some()
            {
                return invalid("final rejection semantics");
            }
        }
        Value::Unresolved(u) => {
            let e = u.error.as_ref().unwrap();
            let closed = u.acceptance_closed.unwrap();
            let valid = match e.code() {
                C::NotFound => !closed && e.action() == A::RetrySend,
                C::InProgress => !closed && e.action() == A::QuerySend,
                C::ClockSkew => !closed && e.action() == A::None,
                C::IdempotencyConflict => closed && e.action() == A::QuerySend,
                C::ResultExpired => closed && e.action() == A::None,
                C::ResultUnavailable | C::RateLimited | C::Overloaded | C::InternalError => {
                    e.action() == A::QuerySend
                }
                _ => false,
            };
            if !valid {
                return invalid("unresolved semantics");
            }
        }
    }
    Ok(())
}
fn session(s: &pb::Session) -> Result<()> {
    if s.enabled_capabilities.iter().any(|c| c != "push_ack")
        || s.enabled_capabilities.windows(2).any(|w| w[0] >= w[1])
    {
        return Err(ProtocolErrorKind::UnsupportedCapability);
    }
    let l = s.limits.as_ref().unwrap();
    if [
        l.max_frame_bytes,
        l.max_text_bytes,
        l.max_page_items,
        l.max_page_bytes,
        l.max_inflight_requests,
        l.heartbeat_interval_ms,
        l.idle_timeout_ms,
    ]
    .iter()
    .any(|v| v.unwrap_or(0) == 0)
        || l.max_frame_bytes.unwrap() > crate::FRAME_HARD_MAX
        || l.max_page_bytes.unwrap() >= l.max_frame_bytes.unwrap()
        || l.max_text_bytes.unwrap() >= l.max_page_bytes.unwrap()
        || l.idle_timeout_ms.unwrap() <= l.heartbeat_interval_ms.unwrap()
    {
        return invalid("protocol limit combination");
    }
    Ok(())
}
