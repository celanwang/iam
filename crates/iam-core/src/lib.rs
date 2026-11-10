//! In-memory authoritative protocol core.

mod engine;
mod store;
mod token;

pub use engine::{CoreError, Engine, EngineOutput};
pub use store::MemoryStore;
pub use token::{SyncCursor, TokenSigner};
// advance_close_watermark available via MemoryStore
