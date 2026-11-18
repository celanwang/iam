use super::*;
use iam_protocol::pb::{ReadPositionAck, ReadPositionRequest, UnreadCountAck, UnreadCountRequest};

impl TidbService {
    pub(super) async fn read_position(
        &self,
        cid: Id128,
        identity: Identity,
        frame: &DecodedFrame,
        request: &ReadPositionRequest,
    ) -> Result<EngineReply> {
        let result = self
            .database
            .read_position(identity, request, || {
                Self::identity(&self.engine.lock().unwrap(), cid)?;
                if !self.accepting.load(Ordering::Relaxed) {
                    return Err(Error::ReadPositionPaused);
                }
                Ok(())
            })
            .await
            .map(|read_through| ReadPositionAck { read_through });
        self.position_reply(cid, frame, MsgType::ReadPositionAck, result)
    }

    pub(super) async fn unread_count(
        &self,
        cid: Id128,
        identity: Identity,
        frame: &DecodedFrame,
        request: &UnreadCountRequest,
    ) -> Result<EngineReply> {
        let result = self
            .database
            .unread_count(identity, request)
            .await
            .map(|count| UnreadCountAck { count: Some(count) });
        self.position_reply(cid, frame, MsgType::UnreadCountAck, result)
    }

    fn position_reply(
        &self,
        cid: Id128,
        frame: &DecodedFrame,
        kind: MsgType,
        result: Result<impl Message>,
    ) -> Result<EngineReply> {
        if let Some(error) = self.check_read_session(cid, frame)? {
            return Ok(error);
        }
        match result {
            Ok(body) => reply(cid, frame, kind, &body),
            Err(Error::InvalidRequest(_)) => wire_error(cid, frame, ErrorCode::InvalidArgument),
            Err(Error::PermissionDenied) => wire_error(cid, frame, ErrorCode::PermissionDenied),
            Err(Error::ReadPositionContextChanged) => protocol_error(
                Some(cid),
                frame,
                ErrorCode::ContextChanged,
                RetryAction::RestartSync,
            ),
            Err(Error::ReadPositionPaused) => protocol_error(
                Some(cid),
                frame,
                ErrorCode::Overloaded,
                RetryAction::RetrySameRequest,
            ),
            Err(_) => protocol_error(
                Some(cid),
                frame,
                ErrorCode::ResultUnavailable,
                RetryAction::RetrySameRequest,
            ),
        }
    }
}
