use crate::{wire::Wire, Client, Error, Options, Result, Transport};
use iam_protocol::pb::{
    GroupInviteDecideRequest, GroupInviteInboxAck, GroupInviteInboxRequest, GroupInviteRequest,
    GroupInviteResultAck,
};
use iam_protocol::{validate_id128, DecodedBody, Id128, MsgType};
use prost::Message;

impl Client {
    /// Sends an invitation. Persist the UUIDv7 before sending and reuse it on retry.
    pub async fn invite_group_member(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        conversation_id: u64,
        target_user_id: u64,
        request_id: Id128,
    ) -> Result<GroupInviteResultAck> {
        validate_id128(&request_id)?;
        match self
            .group_invite_call(
                transport,
                access_token,
                MsgType::GroupInvite,
                &GroupInviteRequest {
                    conversation_id: Some(conversation_id),
                    target_user_id: Some(target_user_id),
                    request_id: Some(request_id.to_vec().into()),
                },
            )
            .await?
        {
            DecodedBody::GroupInviteAck(ack) => Ok(ack),
            _ => Err(Error::Invalid("expected GROUP_INVITE_ACK")),
        }
    }

    /// Lists pending group invitations after an exclusive conversation ID. Start with zero.
    pub async fn group_invite_inbox(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        after_conversation_id: u64,
    ) -> Result<GroupInviteInboxAck> {
        match self
            .group_invite_call(
                transport,
                access_token,
                MsgType::GroupInviteInbox,
                &GroupInviteInboxRequest {
                    after_conversation_id: Some(after_conversation_id),
                },
            )
            .await?
        {
            DecodedBody::GroupInviteInboxAck(page)
                if page
                    .invitations
                    .first()
                    .and_then(|row| row.conversation_id)
                    .is_none_or(|id| id > after_conversation_id) =>
            {
                Ok(page)
            }
            _ => Err(Error::Invalid(
                "expected GROUP_INVITE_INBOX_ACK for the requested range",
            )),
        }
    }

    /// Accepts or rejects one invitation. A retry returns its saved decision.
    pub async fn decide_group_invite(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        conversation_id: u64,
        request_id: Id128,
        accept: bool,
    ) -> Result<GroupInviteResultAck> {
        validate_id128(&request_id)?;
        match self
            .group_invite_call(
                transport,
                access_token,
                MsgType::GroupInviteDecide,
                &GroupInviteDecideRequest {
                    conversation_id: Some(conversation_id),
                    request_id: Some(request_id.to_vec().into()),
                    accept: Some(accept),
                },
            )
            .await?
        {
            DecodedBody::GroupInviteDecideAck(ack) => Ok(ack),
            _ => Err(Error::Invalid("expected GROUP_INVITE_DECIDE_ACK")),
        }
    }

    async fn group_invite_call(
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
