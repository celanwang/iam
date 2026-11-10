#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MsgType {
    Auth = 1,
    AuthAck = 2,
    Heartbeat = 3,
    HeartbeatAck = 4,
    Reconnect = 5,
    ReconnectAck = 6,
    Send = 7,
    SendAck = 8,
    Push = 9,
    PushAck = 10,
    Sync = 11,
    SyncAck = 12,
    Error = 13,
    SendStatus = 14,
    SendStatusAck = 15,
    Snapshot = 16,
    SnapshotAck = 17,
    History = 18,
    HistoryAck = 19,
}

impl MsgType {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            1 => Self::Auth,
            2 => Self::AuthAck,
            3 => Self::Heartbeat,
            4 => Self::HeartbeatAck,
            5 => Self::Reconnect,
            6 => Self::ReconnectAck,
            7 => Self::Send,
            8 => Self::SendAck,
            9 => Self::Push,
            10 => Self::PushAck,
            11 => Self::Sync,
            12 => Self::SyncAck,
            13 => Self::Error,
            14 => Self::SendStatus,
            15 => Self::SendStatusAck,
            16 => Self::Snapshot,
            17 => Self::SnapshotAck,
            18 => Self::History,
            19 => Self::HistoryAck,
            _ => return None,
        })
    }

    pub fn allows_empty_body(self) -> bool {
        matches!(self, Self::Heartbeat)
    }

    pub fn allows_zero_req_id(self) -> bool {
        matches!(self, Self::Push | Self::PushAck | Self::Error)
    }

    pub fn is_client_to_server(self) -> bool {
        matches!(
            self,
            Self::Auth
                | Self::Heartbeat
                | Self::Reconnect
                | Self::Send
                | Self::PushAck
                | Self::Sync
                | Self::SendStatus
                | Self::Snapshot
                | Self::History
        )
    }
}
