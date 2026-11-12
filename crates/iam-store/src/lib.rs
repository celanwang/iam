//! TiDB storage for the wire protocol and the local text console.

mod service;
mod store;
mod sync;

pub use service::{OpenMode, TidbService};

use iam_protocol::pb::{
    message_content, MessageContent, MessagePosition, MessageRef, MessageState, MessageView,
    SendReceipt, TextContent,
};
use iam_protocol::Id128;

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
