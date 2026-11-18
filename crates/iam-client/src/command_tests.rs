use super::*;
use crate::{decode, Options};
use iam_protocol::pb::{ReadPositionAck, RetryAction, UnreadCountAck};
use iam_protocol::{encode_frame, error_info, generate_id128, DecodedBody, MsgType};
use std::{
    future::{pending, poll_fn, ready},
    io,
    pin::Pin,
    task::Poll,
};

fn state() -> ConversationState {
    ConversationState {
        conversation_id: Some(1),
        history_id: Some(generate_id128().to_vec().into()),
        access_version: Some(1),
        ..Default::default()
    }
}

async fn assert_waiting<T>(mut future: Pin<&mut impl Future<Output = T>>) {
    assert!(poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await);
}

#[tokio::test]
async fn bounded_queue_wakes_new_and_waiting_callers_when_closed() {
    let (sender, receiver) = command_channel(1);
    let state = state();
    let mut accepted = Box::pin(sender.read_position(&state));
    assert_waiting(accepted.as_mut()).await;
    let mut waiting = Box::pin(sender.unread_count(&state));
    assert_waiting(waiting.as_mut()).await;
    assert_eq!(receiver.0.len(), 1);
    drop(receiver);
    assert!(matches!(accepted.await, Err(Error::CommandsClosed)));
    assert!(matches!(waiting.await, Err(Error::CommandsClosed)));
    assert!(matches!(
        sender.unread_count(&state).await,
        Err(Error::CommandsClosed)
    ));
}

struct HeldReply {
    started: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
    ids: Vec<u64>,
}

impl Transport for HeldReply {
    async fn reconnect(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn exchange(&mut self, frame: Vec<u8>) -> io::Result<Vec<u8>> {
        let frame = decode(&frame).unwrap();
        self.ids.push(frame.head.req_id.unwrap());
        if let Some(started) = self.started.take() {
            started.send(()).unwrap();
            self.release.take().unwrap().await.unwrap();
        }
        let bytes = match frame.body {
            DecodedBody::ReadPosition(_) => encode_frame(
                MsgType::ReadPositionAck,
                &frame.head,
                &ReadPositionAck {
                    read_through: Some(2),
                },
            ),
            DecodedBody::UnreadCount(_) => encode_frame(
                MsgType::UnreadCountAck,
                &frame.head,
                &UnreadCountAck { count: Some(3) },
            ),
            _ => panic!("unexpected command"),
        };
        Ok(bytes.unwrap())
    }
}

#[tokio::test]
async fn dropping_a_caller_keeps_the_outstanding_exchange_owned_by_the_client() {
    let (started, observed) = oneshot::channel();
    let (release, held) = oneshot::channel();
    let mut transport = HeldReply {
        started: Some(started),
        release: Some(held),
        ids: vec![],
    };
    let mut wire = Wire::new(&mut transport, Options::default());
    let state = state();

    let (reply, dropped) = oneshot::channel();
    drop(dropped);
    let cancelled = Command::ReadPosition(read_request(&state, None).unwrap(), reply);
    assert!(!cancelled.execute(&mut wire).await.unwrap());
    assert!(wire.transport.ids.is_empty());

    let (reply, result) = oneshot::channel();
    let command = Command::AdvanceReadPosition(read_request(&state, Some(2)).unwrap(), reply);
    let mut running = Box::pin(command.execute(&mut wire));
    assert_waiting(running.as_mut()).await;
    observed.await.unwrap();
    drop(result);
    release.send(()).unwrap();
    assert!(!running.await.unwrap());

    let (reply, result) = oneshot::channel();
    let command = Command::UnreadCount(unread_request(&state).unwrap(), reply);
    assert!(!command.execute(&mut wire).await.unwrap());
    assert_eq!(result.await.unwrap().unwrap(), 3);
    assert_eq!(wire.transport.ids, [1, 2]);
}

#[tokio::test]
async fn aborting_an_in_flight_advance_reports_an_unknown_outcome() {
    let (reply, result) = oneshot::channel::<Result<u64>>();
    let mut running = Box::pin(exchange(reply, true, pending()));
    assert_waiting(running.as_mut()).await;
    drop(running);
    assert!(matches!(
        result.await.unwrap(),
        Err(Error::CommandOutcomeUnknown(error)) if matches!(*error, Error::CommandsClosed)
    ));
}

#[tokio::test]
async fn exchange_errors_keep_business_failures_local_and_preserve_session_replacement() {
    use crate::wire::RequestFailure;

    let (reply, result) = oneshot::channel::<Result<u64>>();
    let failed = Err(Error::Timeout.into());
    assert!(exchange(reply, true, ready(failed)).await.unwrap());
    assert!(
        matches!(result.await.unwrap(), Err(Error::CommandOutcomeUnknown(error)) if matches!(*error, Error::Timeout))
    );

    let (reply, result) = oneshot::channel::<Result<u64>>();
    let failed = Err(RequestFailure {
        error: Error::Server(error_info(ErrorCode::PermissionDenied, RetryAction::None)),
        close_connection: false,
    });
    assert!(!exchange(reply, true, ready(failed)).await.unwrap());
    assert!(
        matches!(result.await.unwrap(), Err(Error::Server(info)) if info.code() == ErrorCode::PermissionDenied)
    );

    let (reply, result) = oneshot::channel::<Result<u64>>();
    let failed = Err(RequestFailure {
        error: Error::Server(error_info(ErrorCode::SessionReplaced, RetryAction::None)),
        close_connection: true,
    });
    let run = exchange(reply, false, ready(failed)).await;
    assert!(matches!(run, Err(Error::Server(info)) if info.code() == ErrorCode::SessionReplaced));
    assert!(
        matches!(result.await.unwrap(), Err(Error::Server(info)) if info.code() == ErrorCode::SessionReplaced)
    );
}
