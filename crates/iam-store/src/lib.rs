//! A native text-storage application path, not a durable implementation of wire SEND/SYNC.
//! Only the service can issue writes; its identity comes from the existing Engine's sessions.

mod service;
mod store;

pub use service::{OpenMode, TidbTextService};

use iam_core::MemoryStore;
use iam_protocol::pb::{
    message_content, MessageContent, MessagePosition, MessageRef, MessageState, MessageView,
    SendReceipt, TextContent,
};
use iam_protocol::{validate_id128, Id128};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid local configuration: {0}")]
    Configuration(&'static str),
    #[error("session is missing or revoked")]
    Unauthenticated,
    #[error("current session cannot access this conversation")]
    PermissionDenied,
    #[error("invalid request: {0}")]
    InvalidRequest(&'static str),
    #[error("original send has different content")]
    ContentConflict,
    #[error("new acceptance is disabled; missing results remain unknown")]
    ResultUnavailable,
    #[error("client message time is too far ahead")]
    ClockSkew,
    #[error("acceptance deadline reached; this is not a final rejection")]
    AcceptanceExpired,
    #[error("result query deadline reached; no closure proof is implied")]
    QueryExpired,
    #[error("conversation does not exist in storage")]
    MissingConversation,
    #[error("conversation sequence exhausted")]
    SequenceExhausted,
    #[error("stored data is inconsistent: {0}")]
    Integrity(&'static str),
    #[error("database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("transaction outcome is unknown; query the original key")]
    CommitUnknown(#[source] sqlx::Error),
    #[error(transparent)]
    Core(#[from] iam_core::CoreError),
}

#[derive(Debug, Clone)]
pub struct ConversationInfo {
    pub history_id: Id128,
    pub next_sequence: u64,
    /// Only the process that created a conversation can write it in fresh mode.
    pub created: bool,
}

#[derive(Debug, Clone)]
pub struct SavedSend {
    pub receipt: SendReceipt,
    pub replayed: bool,
}

#[derive(Debug, Clone)]
pub enum SendLookup {
    Found(SendReceipt),
    /// Neither a final rejection nor permission to retry as a new acceptance.
    Unknown,
}

#[derive(Debug, Clone)]
pub struct TextPage {
    pub history_id: Id128,
    pub messages: Vec<MessageView>,
    /// Exclusive sequence boundary. This is a native API, not a protocol page token.
    pub next_before: Option<u64>,
}

// Never deserialized from requests. Created afresh from server-side sessions for each operation.
#[derive(Debug, Clone, Copy)]
struct Identity {
    tenant_id: u64,
    user_id: u64,
    device_id: Id128,
}

#[derive(Debug, Clone)]
struct StoredText {
    receipt: SendReceipt,
    user_id: u64,
    text: String,
}

impl StoredText {
    fn view(&self) -> MessageView {
        MessageView {
            r#ref: self.receipt.r#ref.clone(),
            position: self.receipt.position.clone(),
            revision_seq: self.receipt.position.as_ref().and_then(|p| p.create_seq),
            sender_id: Some(self.user_id),
            accepted_at_ms: self.receipt.accepted_at_ms,
            state: Some(MessageState::Active as i32),
            content: Some(text_content(&self.text)),
        }
    }
}

fn text_content(text: &str) -> MessageContent {
    MessageContent {
        value: Some(message_content::Value::Text(TextContent {
            text: Some(text.to_owned()),
        })),
    }
}

fn same_content(a: &str, b: &str) -> bool {
    MemoryStore::content_digest(&text_content(a)) == MemoryStore::content_digest(&text_content(b))
}

fn windows(id: &Id128, now: i64) -> Result<(i64, i64)> {
    validate_id128(id).map_err(|_| Error::InvalidRequest("client_msg_id must be UUIDv7"))?;
    let (time, accept_until, query_until) = MemoryStore::windows_for_client_msg(id);
    if now < 0 {
        return Err(Error::ResultUnavailable);
    }
    if now >= query_until {
        return Err(Error::QueryExpired);
    }
    Ok((time, accept_until))
}

fn receipt(
    conversation_id: u64,
    message_id: Id128,
    history_id: Id128,
    sequence: u64,
    accepted_at: i64,
) -> SendReceipt {
    SendReceipt {
        r#ref: Some(MessageRef {
            conversation_id: Some(conversation_id),
            message_id: Some(message_id.to_vec().into()),
        }),
        position: Some(MessagePosition {
            history_id: Some(history_id.to_vec().into()),
            create_seq: Some(sequence),
        }),
        accepted_at_ms: Some(accepted_at),
    }
}

#[cfg(test)]
mod tests;
