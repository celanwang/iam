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
    FriendRequest = 20,
    FriendRequestAck = 21,
    FriendList = 22,
    FriendListAck = 23,
    FriendAccept = 24,
    FriendAcceptAck = 25,
    FriendApply = 26,
    FriendApplyAck = 27,
    FriendInbox = 28,
    FriendInboxAck = 29,
    FriendDecide = 30,
    FriendDecideAck = 31,
    GroupCreate = 32,
    GroupCreateAck = 33,
    ReadPosition = 34,
    ReadPositionAck = 35,
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
            20 => Self::FriendRequest,
            21 => Self::FriendRequestAck,
            22 => Self::FriendList,
            23 => Self::FriendListAck,
            24 => Self::FriendAccept,
            25 => Self::FriendAcceptAck,
            26 => Self::FriendApply,
            27 => Self::FriendApplyAck,
            28 => Self::FriendInbox,
            29 => Self::FriendInboxAck,
            30 => Self::FriendDecide,
            31 => Self::FriendDecideAck,
            32 => Self::GroupCreate,
            33 => Self::GroupCreateAck,
            34 => Self::ReadPosition,
            35 => Self::ReadPositionAck,
            _ => return None,
        })
    }

    pub fn allows_empty_body(self) -> bool {
        matches!(
            self,
            Self::Heartbeat | Self::FriendListAck | Self::FriendInboxAck | Self::ReadPositionAck
        )
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
                | Self::ReadPosition
                | Self::FriendRequest
                | Self::FriendList
                | Self::FriendAccept
                | Self::FriendApply
                | Self::FriendInbox
                | Self::FriendDecide
                | Self::GroupCreate
        )
    }
}
