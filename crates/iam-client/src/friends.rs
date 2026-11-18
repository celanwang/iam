use crate::{wire::Wire, Client, Error, Options, Result, Transport};
use iam_protocol::pb::{
    friend_request_ack, friend_status_ack, FriendAcceptRequest, FriendApplyRequest,
    FriendAttemptAck, FriendDecideRequest, FriendInboxAck, FriendInboxRequest, FriendListAck,
    FriendListRequest, FriendRelationship, FriendRemoveRequest, FriendRequest, FriendStatusRequest,
};
use iam_protocol::{validate_id128, DecodedBody, Id128, MsgType};
use prost::Message;

impl Client {
    /// Returns the active friendship and its generation, if any.
    pub async fn friend_status(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        peer_user_id: u64,
    ) -> Result<Option<FriendRelationship>> {
        let response = self
            .friend_call(
                transport,
                access_token,
                MsgType::FriendStatus,
                &FriendStatusRequest {
                    peer_user_id: Some(peer_user_id),
                },
            )
            .await?;
        let DecodedBody::FriendStatusAck(ack) = response else {
            return Err(Error::Invalid("expected FRIEND_STATUS_ACK"));
        };
        match ack.value {
            Some(friend_status_ack::Value::Relationship(relationship)) => Ok(Some(relationship)),
            Some(friend_status_ack::Value::NotFriends(_)) => Ok(None),
            None => Err(Error::Invalid("missing friend status")),
        }
    }

    /// Removes the given friendship generation and returns its original conversation ID.
    /// Reuse the same generation on retry, including after the relationship is restored.
    pub async fn remove_friend(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        peer_user_id: u64,
        generation: Id128,
    ) -> Result<u64> {
        validate_id128(&generation)?;
        match self
            .friend_call(
                transport,
                access_token,
                MsgType::FriendRemove,
                &FriendRemoveRequest {
                    peer_user_id: Some(peer_user_id),
                    generation: Some(generation.to_vec().into()),
                },
            )
            .await?
        {
            DecodedBody::FriendRemoveAck(ack) => ack
                .conversation_id
                .ok_or(Error::Invalid("missing removed conversation ID")),
            _ => Err(Error::Invalid("expected FRIEND_REMOVE_ACK")),
        }
    }

    /// Applies with a caller-owned UUIDv7. Persist this ID before sending and reuse it on retry.
    /// An intentional reapplication after rejection uses a fresh ID.
    pub async fn apply_friend(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        target_user_id: u64,
        request_id: Id128,
    ) -> Result<FriendAttemptAck> {
        validate_id128(&request_id)?;
        match self
            .friend_call(
                transport,
                access_token,
                MsgType::FriendApply,
                &FriendApplyRequest {
                    target_user_id: Some(target_user_id),
                    request_id: Some(request_id.to_vec().into()),
                },
            )
            .await?
        {
            DecodedBody::FriendApplyAck(ack) => Ok(ack),
            _ => Err(Error::Invalid("expected FRIEND_APPLY_ACK")),
        }
    }

    /// Lists pending requests with their IDs. Use zero for the first page.
    pub async fn friend_inbox(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        after_user_id: u64,
    ) -> Result<FriendInboxAck> {
        match self
            .friend_call(
                transport,
                access_token,
                MsgType::FriendInbox,
                &FriendInboxRequest {
                    after_user_id: Some(after_user_id),
                },
            )
            .await?
        {
            DecodedBody::FriendInboxAck(page)
                if page
                    .requests
                    .first()
                    .and_then(|r| r.from_user_id)
                    .is_none_or(|user| user > after_user_id) =>
            {
                Ok(page)
            }
            _ => Err(Error::Invalid(
                "expected FRIEND_INBOX_ACK for the requested range",
            )),
        }
    }

    /// Accepts or rejects one incoming request. If already decided, returns the original result.
    pub async fn decide_friend(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        from_user_id: u64,
        request_id: Id128,
        accept: bool,
    ) -> Result<FriendAttemptAck> {
        validate_id128(&request_id)?;
        match self
            .friend_call(
                transport,
                access_token,
                MsgType::FriendDecide,
                &FriendDecideRequest {
                    from_user_id: Some(from_user_id),
                    request_id: Some(request_id.to_vec().into()),
                    accept: Some(accept),
                },
            )
            .await?
        {
            DecodedBody::FriendDecideAck(ack) => Ok(ack),
            _ => Err(Error::Invalid("expected FRIEND_DECIDE_ACK")),
        }
    }

    /// Sends or retries an outgoing request. An established friendship returns its conversation ID.
    /// This call opens a fresh authenticated connection for the client's device.
    pub async fn request_friend(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        target_user_id: u64,
    ) -> Result<Option<u64>> {
        let response = self
            .friend_call(
                transport,
                access_token,
                MsgType::FriendRequest,
                &FriendRequest {
                    target_user_id: Some(target_user_id),
                },
            )
            .await?;
        match response {
            DecodedBody::FriendRequestAck(ack) => match ack.value {
                Some(friend_request_ack::Value::Pending(_)) => Ok(None),
                Some(friend_request_ack::Value::ConversationId(id)) => Ok(Some(id)),
                None => Err(Error::Invalid("missing friend request state")),
            },
            _ => Err(Error::Invalid("expected FRIEND_REQUEST_ACK")),
        }
    }

    /// Lists pending applicants after an exclusive user ID. Pass zero for the first page.
    /// This call opens a fresh authenticated connection for the client's device.
    pub async fn incoming_friend_requests(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        after_user_id: u64,
    ) -> Result<FriendListAck> {
        let response = self
            .friend_call(
                transport,
                access_token,
                MsgType::FriendList,
                &FriendListRequest {
                    after_user_id: Some(after_user_id),
                },
            )
            .await?;
        match response {
            DecodedBody::FriendListAck(page)
                if page
                    .from_user_ids
                    .first()
                    .is_none_or(|&user| user > after_user_id) =>
            {
                Ok(page)
            }
            _ => Err(Error::Invalid(
                "expected FRIEND_LIST_ACK for the requested range",
            )),
        }
    }

    /// Accepts or retries an incoming request and returns the conversation ID.
    /// This call opens a fresh authenticated connection for the client's device.
    pub async fn accept_friend(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        from_user_id: u64,
    ) -> Result<u64> {
        let response = self
            .friend_call(
                transport,
                access_token,
                MsgType::FriendAccept,
                &FriendAcceptRequest {
                    from_user_id: Some(from_user_id),
                },
            )
            .await?;
        match response {
            DecodedBody::FriendAcceptAck(ack) => ack
                .conversation_id
                .ok_or(Error::Invalid("missing friend conversation ID")),
            _ => Err(Error::Invalid("expected FRIEND_ACCEPT_ACK")),
        }
    }

    async fn friend_call(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        kind: MsgType,
        body: &impl Message,
    ) -> Result<DecodedBody> {
        let mut wire = Wire::new(transport, Options::default());
        wire.authenticate(self.identity, access_token).await?;
        wire.request(kind, body).await
    }
}
