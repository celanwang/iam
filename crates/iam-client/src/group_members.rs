use crate::{Client, Error, Options, Result, Transport, Wire};
use iam_protocol::pb::{
    ConversationState, GroupMember, GroupMemberRemoveRequest, GroupMembersAck, GroupMembersRequest,
};
use iam_protocol::{validate_id128, DecodedBody, MsgType};

impl Client {
    /// Fetches the current member roster and fixed owner of a group.
    ///
    /// This opens a fresh authenticated connection. End any sync run on the same device first.
    pub async fn group_members(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        conversation_id: u64,
    ) -> Result<GroupMembersAck> {
        if conversation_id == 0 {
            return Err(Error::Invalid("group conversation ID"));
        }
        let mut wire = Wire::new(transport, Options::default());
        wire.authenticate(self.identity, access_token).await?;
        let response = wire
            .request(
                MsgType::GroupMembers,
                &GroupMembersRequest {
                    conversation_id: Some(conversation_id),
                },
            )
            .await?;
        let DecodedBody::GroupMembersAck(list) = response else {
            return Err(Error::Invalid("expected GROUP_MEMBERS_ACK"));
        };
        if list.conversation_id != Some(conversation_id)
            || !list
                .members
                .iter()
                .any(|member| member.user_id == Some(self.identity.user_id))
        {
            return Err(Error::Invalid("group roster scope"));
        }
        Ok(list)
    }

    /// Leaves the membership period identified by the caller's synchronized group state.
    /// Preserve the original state for retries. The fixed owner cannot use this operation.
    ///
    /// This opens a fresh authenticated connection. End any sync run on the same device first.
    pub async fn leave_group(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        group: &ConversationState,
    ) -> Result<()> {
        self.change_group_membership(
            transport,
            access_token,
            GroupMemberRemoveRequest {
                conversation_id: group.conversation_id,
                history_id: group.history_id.clone(),
                user_id: Some(self.identity.user_id),
                access_version: group.access_version,
            },
        )
        .await
    }

    /// Removes another member in the period identified by an entry from the group's roster.
    /// The active owner can retry with the original roster and entry even after the member rejoins.
    ///
    /// This opens a fresh authenticated connection. End any sync run on the same device first.
    pub async fn remove_group_member(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        roster: &GroupMembersAck,
        member: &GroupMember,
    ) -> Result<()> {
        if member.user_id == Some(self.identity.user_id) {
            return Err(Error::Invalid("the target must be another group member"));
        }
        self.change_group_membership(
            transport,
            access_token,
            GroupMemberRemoveRequest {
                conversation_id: roster.conversation_id,
                history_id: roster.history_id.clone(),
                user_id: member.user_id,
                access_version: member.access_version,
            },
        )
        .await
    }

    async fn change_group_membership(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        request: GroupMemberRemoveRequest,
    ) -> Result<()> {
        if request.conversation_id.unwrap_or(0) == 0
            || request.user_id.unwrap_or(0) == 0
            || request.access_version.unwrap_or(0) == 0
        {
            return Err(Error::Invalid("group member scope"));
        }
        validate_id128(request.history_id.as_deref().unwrap_or_default())?;
        let mut wire = Wire::new(transport, Options::default());
        wire.authenticate(self.identity, access_token).await?;
        match wire.request(MsgType::GroupMemberRemove, &request).await? {
            DecodedBody::GroupMemberRemoveAck(ack)
                if ack.conversation_id == request.conversation_id
                    && ack.user_id == request.user_id
                    && ack.access_version == request.access_version =>
            {
                Ok(())
            }
            _ => Err(Error::Invalid(
                "expected GROUP_MEMBER_REMOVE_ACK for the requested period",
            )),
        }
    }
}
