use crate::{wire::Wire, Client, Error, Options, Result, Transport};
use iam_protocol::pb::UserBlockRequest;
use iam_protocol::{validate_id128, DecodedBody, Id128, MsgType};

impl Client {
    /// Checks only this account's own block of the specified peer.
    pub async fn user_block_status(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        peer_user_id: u64,
    ) -> Result<bool> {
        self.user_block_call(
            transport,
            access_token,
            UserBlockRequest {
                peer_user_id: Some(peer_user_id),
                request_id: None,
                blocked: None,
            },
        )
        .await
    }

    /// Sets this account's block of the peer. Persist the UUIDv7 before the first attempt.
    /// Retries return the original operation's result, even after another change.
    pub async fn set_user_block(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        peer_user_id: u64,
        request_id: Id128,
        blocked: bool,
    ) -> Result<bool> {
        validate_id128(&request_id)?;
        self.user_block_call(
            transport,
            access_token,
            UserBlockRequest {
                peer_user_id: Some(peer_user_id),
                request_id: Some(request_id.to_vec().into()),
                blocked: Some(blocked),
            },
        )
        .await
    }

    async fn user_block_call(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        request: UserBlockRequest,
    ) -> Result<bool> {
        let mut wire = Wire::new(transport, Options::default());
        wire.authenticate(self.identity, access_token).await?;
        match wire.request(MsgType::UserBlock, &request).await? {
            DecodedBody::UserBlockAck(ack) => ack
                .blocked
                .ok_or(Error::Invalid("missing user block state")),
            _ => Err(Error::Invalid("expected USER_BLOCK_ACK")),
        }
    }
}
