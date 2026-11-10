use thiserror::Error;

pub type Result<T> = std::result::Result<T, ProtocolErrorKind>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProtocolErrorKind {
    #[error("malformed frame: {0}")]
    MalformedFrame(&'static str),
    #[error("unsupported version: {0}")]
    UnsupportedVersion(u8),
    #[error("unsupported flags: {0}")]
    UnsupportedFlags(u16),
    #[error("unsupported command: {0}")]
    UnsupportedCommand(u8),
    #[error("resource limit: {0}")]
    ResourceLimit(&'static str),
    #[error("invalid argument: {0}")]
    InvalidArgument(&'static str),
    #[error("unsupported content")]
    UnsupportedContent,
    #[error("decode error: {0}")]
    Decode(String),
}
