use crate::{wire::Wire, Client, Error, Options, Result, Transport};
use iam_protocol::pb::{
    friend_request_ack, FriendAcceptRequest, FriendListAck, FriendListRequest, FriendRequest,
};
use iam_protocol::{DecodedBody, MsgType};
use prost::Message;

impl Client {
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
