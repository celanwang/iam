use crate::wire::RequestResult;
use crate::{Client, Error, Options, Result, Transport, Wire};
use iam_protocol::pb::{ConversationState, ReadPositionRequest, UnreadCountRequest};
use iam_protocol::{validate_id128, DecodedBody, MsgType};

impl Client {
    /// Queries the exact unread count for the supplied synced conversation scope.
    /// This uses current server data. Local messages and previews may be behind.
    /// This call opens a fresh authenticated connection for the client's device. Use
    /// [`crate::CommandSender::unread_count`] while the client is running.
    pub async fn unread_count(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        conversation: &ConversationState,
    ) -> Result<u64> {
        let request = unread_request(conversation)?;
        let mut wire = Wire::new(transport, Options::default());
        wire.authenticate(self.identity, access_token).await?;
        fetch_unread_count(&mut wire, &request)
            .await
            .map_err(|failure| failure.error)
    }

    /// Queries this user's saved read position in the supplied synced conversation state.
    /// An empty result means no position has been saved in the current access period and history.
    /// This call opens a fresh authenticated connection for the client's device. Use
    /// [`crate::CommandSender::read_position`] while the client is running.
    pub async fn read_position(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        conversation: &ConversationState,
    ) -> Result<Option<u64>> {
        let request = read_request(conversation, None)?;
        let mut wire = Wire::new(transport, Options::default());
        wire.authenticate(self.identity, access_token).await?;
        fetch_read_position(&mut wire, &request)
            .await
            .map_err(|failure| failure.error)
    }

    /// Advances this user's read position to at least the given message creation sequence.
    /// The returned sequence includes any higher position saved by another device.
    /// This call opens a fresh authenticated connection for the client's device. Use
    /// [`crate::CommandSender::advance_read_position`] while the client is running.
    pub async fn advance_read_position(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        conversation: &ConversationState,
        read_through: u64,
    ) -> Result<u64> {
        let request = read_request(conversation, Some(read_through))?;
        let mut wire = Wire::new(transport, Options::default());
        wire.authenticate(self.identity, access_token).await?;
        save_read_position(&mut wire, &request)
            .await
            .map_err(|failure| failure.error)
    }
}

pub(crate) fn unread_request(conversation: &ConversationState) -> Result<UnreadCountRequest> {
    validate_scope(conversation)?;
    Ok(UnreadCountRequest {
        conversation_id: conversation.conversation_id,
        history_id: conversation.history_id.clone(),
        access_version: conversation.access_version,
    })
}

pub(crate) fn read_request(
    conversation: &ConversationState,
    read_through: Option<u64>,
) -> Result<ReadPositionRequest> {
    validate_scope(conversation)?;
    if read_through == Some(0) {
        return Err(Error::Invalid("zero read position"));
    }
    Ok(ReadPositionRequest {
        conversation_id: conversation.conversation_id,
        history_id: conversation.history_id.clone(),
        access_version: conversation.access_version,
        read_through,
    })
}

pub(crate) async fn fetch_unread_count(
    wire: &mut Wire<'_, impl Transport>,
    request: &UnreadCountRequest,
) -> RequestResult<u64> {
    let DecodedBody::UnreadCountAck(ack) =
        wire.request_detailed(MsgType::UnreadCount, request).await?
    else {
        return Err(Error::Invalid("expected UNREAD_COUNT_ACK").into());
    };
    ack.count
        .ok_or_else(|| Error::Invalid("missing unread count").into())
}

pub(crate) async fn fetch_read_position(
    wire: &mut Wire<'_, impl Transport>,
    request: &ReadPositionRequest,
) -> RequestResult<Option<u64>> {
    let DecodedBody::ReadPositionAck(ack) = wire
        .request_detailed(MsgType::ReadPosition, request)
        .await?
    else {
        return Err(Error::Invalid("expected READ_POSITION_ACK").into());
    };
    Ok(ack.read_through)
}

pub(crate) async fn save_read_position(
    wire: &mut Wire<'_, impl Transport>,
    request: &ReadPositionRequest,
) -> RequestResult<u64> {
    fetch_read_position(wire, request)
        .await?
        .filter(|&saved| Some(saved) >= request.read_through)
        .ok_or_else(|| {
            Error::Invalid("read position acknowledgement did not cover the request").into()
        })
}

fn validate_scope(conversation: &ConversationState) -> Result<()> {
    if conversation.conversation_id.unwrap_or(0) == 0
        || conversation.access_version.unwrap_or(0) == 0
    {
        return Err(Error::Invalid("conversation scope"));
    }
    validate_id128(conversation.history_id.as_deref().unwrap_or_default())?;
    Ok(())
}
