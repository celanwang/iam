//! In-memory authoritative protocol core.

mod engine;
mod store;
mod token;

pub use engine::{CoreError, Engine, EngineOutput, EngineReply, StorageRequest};
pub use store::{DeviceCredential, MemoryStore, ACCEPT_WINDOW_MS, ALLOW_SKEW_MS};
pub use token::{HistoryToken, SnapshotToken, SyncCursor, SyncPageToken, TokenError, TokenSigner};
