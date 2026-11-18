use crate::{Client, Error, Options, Result, Transport, Wire};
use iam_protocol::pb::{ConversationState, ReadPositionRequest};
use iam_protocol::{validate_id128, DecodedBody, MsgType};

impl Client {
    /// Queries this user's saved read position in the supplied synced conversation state.
    /// An empty result means no position has been saved in the current access period and history.
    /// This call opens a fresh authenticated connection for the client's device. End any active
    /// sync run on the same device before calling it.
    pub async fn read_position(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        conversation: &ConversationState,
    ) -> Result<Option<u64>> {
        self.read_position_call(transport, access_token, conversation, None)
            .await
    }

    /// Advances this user's read position to at least the given message creation sequence.
    /// The returned sequence includes any higher position saved by another device.
    /// This call opens a fresh authenticated connection for the client's device. End any active
    /// sync run on the same device before calling it.
    pub async fn advance_read_position(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        conversation: &ConversationState,
        read_through: u64,
    ) -> Result<u64> {
        self.read_position_call(transport, access_token, conversation, Some(read_through))
            .await?
            .filter(|saved| *saved >= read_through)
            .ok_or(Error::Invalid(
                "read position acknowledgement did not cover the request",
            ))
    }

    async fn read_position_call(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        conversation: &ConversationState,
        read_through: Option<u64>,
    ) -> Result<Option<u64>> {
        if conversation.conversation_id.unwrap_or(0) == 0
            || conversation.access_version.unwrap_or(0) == 0
            || read_through == Some(0)
        {
            return Err(Error::Invalid("read position scope or sequence"));
        }
        validate_id128(conversation.history_id.as_deref().unwrap_or_default())?;
        let request = ReadPositionRequest {
            conversation_id: conversation.conversation_id,
            history_id: conversation.history_id.clone(),
            access_version: conversation.access_version,
            read_through,
        };
        let mut wire = Wire::new(transport, Options::default());
        wire.authenticate(self.identity, access_token).await?;
        let DecodedBody::ReadPositionAck(ack) =
            wire.request(MsgType::ReadPosition, &request).await?
        else {
            return Err(Error::Invalid("expected READ_POSITION_ACK"));
        };
        Ok(ack.read_through)
    }
}
