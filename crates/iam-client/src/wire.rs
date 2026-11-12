use crate::{Error, Identity, Options, Result, Transport};
use iam_protocol::pb::{auth_response, AuthRequest, Head, PageLimit};
use iam_protocol::{decode_frame, encode_frame, DecodedBody, DecodedFrame, FrameDecoder, MsgType};
use prost::Message;
use std::time::Duration;
use tokio::time::timeout;

pub(crate) struct Wire<'a, T> {
    pub transport: &'a mut T,
    timeout: Duration,
    req_id: u64,
    pub limit: PageLimit,
}

impl<'a, T: Transport> Wire<'a, T> {
    pub fn new(transport: &'a mut T, options: Options) -> Self {
        Self {
            transport,
            timeout: options.request_timeout,
            req_id: 0,
            limit: PageLimit {
                max_items: Some(options.page_items),
                max_bytes: Some(256 * 1024),
            },
        }
    }

    pub async fn authenticate(&mut self, identity: Identity, token: &[u8]) -> Result<()> {
        timeout(self.timeout, self.transport.reconnect())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::Transport)?;
        self.req_id = 0;
        let response = self
            .request(
                MsgType::Auth,
                &AuthRequest {
                    access_token: Some(token.to_vec().into()),
                    device_id: Some(identity.device_id.to_vec().into()),
                    requested_capabilities: Vec::new(),
                },
            )
            .await?;
        let DecodedBody::AuthAck(auth) = response else {
            return Err(Error::Invalid("expected AUTH_ACK"));
        };
        match auth.value {
            Some(auth_response::Value::Session(session))
                if session.tenant_id == Some(identity.tenant_id)
                    && session.user_id == Some(identity.user_id)
                    && session.device_id.as_deref() == Some(identity.device_id.as_slice()) =>
            {
                Ok(())
            }
            Some(auth_response::Value::Session(_)) => Err(Error::IdentityMismatch),
            Some(auth_response::Value::Error(error)) => Err(Error::Server(error)),
            None => Err(Error::Invalid("missing auth result")),
        }
    }

    pub async fn request(&mut self, kind: MsgType, body: &impl Message) -> Result<DecodedBody> {
        self.req_id = self
            .req_id
            .checked_add(1)
            .ok_or(Error::Invalid("request ID exhausted"))?;
        let head = Head {
            req_id: Some(self.req_id),
        };
        let request = encode_frame(kind, &head, body)?;
        let bytes = timeout(self.timeout, self.transport.exchange(request))
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::Transport)?;
        let response = decode(&bytes)?;
        if response.head != head {
            return Err(Error::Invalid("response request ID"));
        }
        match response.body {
            DecodedBody::Error(error) => Err(Error::Server(
                error
                    .error
                    .ok_or(Error::Invalid("missing protocol error"))?,
            )),
            body => Ok(body),
        }
    }
}

pub(crate) fn decode(bytes: &[u8]) -> Result<DecodedFrame> {
    let mut decoder = FrameDecoder::new();
    decoder.push(bytes);
    let frame = decoder
        .next_frame()?
        .ok_or(Error::Invalid("incomplete frame"))?;
    if decoder.next_frame()?.is_some() {
        return Err(Error::Invalid("expected one frame"));
    }
    decoder.finish()?;
    Ok(decode_frame(&frame)?)
}
