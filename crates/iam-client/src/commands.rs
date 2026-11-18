use crate::read_position::{
    fetch_read_position, fetch_unread_count, read_request, save_read_position, unread_request,
};
use crate::wire::RequestResult;
use crate::{Error, Result, Transport, Wire};
use iam_protocol::pb::{ConversationState, ErrorCode, ReadPositionRequest, UnreadCountRequest};
use std::future::Future;
use tokio::sync::{mpsc, oneshot};

#[cfg(test)]
#[path = "command_tests.rs"]
mod tests;

/// Creates a bounded queue for one [`crate::Client::run_with_commands`] call.
///
/// # Panics
///
/// Panics if the capacity is zero or exceeds Tokio's channel limit.
pub fn command_channel(capacity: usize) -> (CommandSender, CommandReceiver) {
    let (sender, receiver) = mpsc::channel(capacity);
    (CommandSender(sender), CommandReceiver(receiver))
}

/// Submits typed read-state operations to the running client on its existing connection.
#[derive(Clone)]
pub struct CommandSender(mpsc::Sender<Command>);

/// The receiver consumed by a single run. Dropping it closes the command queue.
pub struct CommandReceiver(pub(crate) mpsc::Receiver<Command>);

impl CommandSender {
    /// Reads the saved position for the supplied synced conversation scope.
    pub async fn read_position(&self, conversation: &ConversationState) -> Result<Option<u64>> {
        let request = read_request(conversation, None)?;
        self.enqueue(|reply| Command::ReadPosition(request, reply))
            .await
    }

    /// Advances this user's position. After [`Error::CommandOutcomeUnknown`], the caller can
    /// explicitly retry the same position using the same conversation scope.
    pub async fn advance_read_position(
        &self,
        conversation: &ConversationState,
        read_through: u64,
    ) -> Result<u64> {
        let request = read_request(conversation, Some(read_through))?;
        self.enqueue(|reply| Command::AdvanceReadPosition(request, reply))
            .await
    }

    /// Queries the server's current unread count for the supplied scope.
    pub async fn unread_count(&self, conversation: &ConversationState) -> Result<u64> {
        let request = unread_request(conversation)?;
        self.enqueue(|reply| Command::UnreadCount(request, reply))
            .await
    }

    async fn enqueue<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T>>) -> Command,
    ) -> Result<T> {
        let (reply, result) = oneshot::channel();
        self.0
            .send(build(reply))
            .await
            .map_err(|_| Error::CommandsClosed)?;
        result.await.map_err(|_| Error::CommandsClosed)?
    }
}

pub(crate) enum Command {
    ReadPosition(ReadPositionRequest, oneshot::Sender<Result<Option<u64>>>),
    AdvanceReadPosition(ReadPositionRequest, oneshot::Sender<Result<u64>>),
    UnreadCount(UnreadCountRequest, oneshot::Sender<Result<u64>>),
}

impl Command {
    // True means the connection needs to be replaced after delivering the individual result.
    // Authentication errors keep their server info for the run loop's existing policy.
    pub(crate) async fn execute(self, wire: &mut Wire<'_, impl Transport>) -> Result<bool> {
        match self {
            Self::ReadPosition(req, reply) => {
                exchange(reply, false, fetch_read_position(wire, &req)).await
            }
            Self::AdvanceReadPosition(req, reply) => {
                exchange(reply, true, save_read_position(wire, &req)).await
            }
            Self::UnreadCount(req, reply) => {
                exchange(reply, false, fetch_unread_count(wire, &req)).await
            }
        }
    }
}

async fn exchange<T>(
    reply: oneshot::Sender<Result<T>>,
    writes: bool,
    request: impl Future<Output = RequestResult<T>>,
) -> Result<bool> {
    if reply.is_closed() {
        return Ok(false);
    }
    let mut in_flight = InFlight {
        reply: Some(reply),
        writes,
    };
    // The running client owns this exchange. Dropping the caller cannot cancel it.
    let (response, connection) = match request.await {
        Ok(value) => (Ok(value), Ok(false)),
        Err(failure) => {
            let connection = match &failure.error {
                Error::Server(info)
                    if matches!(
                        info.code(),
                        ErrorCode::Unauthenticated | ErrorCode::SessionReplaced
                    ) =>
                {
                    Err(Error::Server(info.clone()))
                }
                _ => Ok(failure.close_connection),
            };
            let unknown = failure.close_connection
                || matches!(&failure.error, Error::Server(info)
                if matches!(info.code(), ErrorCode::ResultUnavailable | ErrorCode::Unauthenticated | ErrorCode::SessionReplaced | ErrorCode::InternalError));
            let error = if writes && unknown {
                Error::CommandOutcomeUnknown(Box::new(failure.error))
            } else {
                failure.error
            };
            (Err(error), connection)
        }
    };
    let _ = in_flight.reply.take().unwrap().send(response);
    connection
}

struct InFlight<T> {
    reply: Option<oneshot::Sender<Result<T>>>,
    writes: bool,
}

impl<T> Drop for InFlight<T> {
    fn drop(&mut self) {
        if self.writes {
            if let Some(reply) = self.reply.take() {
                let _ = reply.send(Err(Error::CommandOutcomeUnknown(Box::new(
                    Error::CommandsClosed,
                ))));
            }
        }
    }
}
