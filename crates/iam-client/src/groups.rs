use crate::{wire::Wire, Client, Error, Options, Result, Transport};
use iam_protocol::pb::GroupCreateRequest;
use iam_protocol::{validate_id128, DecodedBody, Id128, MsgType};

impl Client {
    /// Creates a small group with the listed friends and the authenticated creator.
    /// Persist the UUIDv7 before sending and reuse it with the same member set on retry.
    /// This call opens a fresh authenticated connection for the client's device.
    pub async fn create_group(
        &self,
        transport: &mut impl Transport,
        access_token: &[u8],
        request_id: Id128,
        member_user_ids: &[u64],
    ) -> Result<u64> {
        validate_id128(&request_id)?;
        let mut wire = Wire::new(transport, Options::default());
        wire.authenticate(self.identity, access_token).await?;
        let response = wire
            .request(
                MsgType::GroupCreate,
                &GroupCreateRequest {
                    request_id: Some(request_id.to_vec().into()),
                    member_user_ids: member_user_ids.to_vec(),
                },
            )
            .await?;
        match response {
            DecodedBody::GroupCreateAck(ack) => ack
                .conversation_id
                .ok_or(Error::Invalid("missing group conversation ID")),
            _ => Err(Error::Invalid("expected GROUP_CREATE_ACK")),
        }
    }
}
