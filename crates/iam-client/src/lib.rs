//! A durable, single-device text client. The caller supplies transport and credentials.

mod storage;
mod sync;
mod wire;

use iam_protocol::pb::{
    message_content, send_outcome, ErrorCode, ErrorInfo, MessageContent, MessageKey, MessageView,
    ResetReason, RetryAction, SendOutcome, SendRequest, SendStatusRequest, TextContent,
};
use iam_protocol::{
    generate_id128, validate_id128, validate_message_content, DecodedBody, Id128, MsgType,
};
use std::future::Future;
use std::io;
use std::path::Path;
use std::time::Duration;
use storage::{Pending, Storage};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep_until, Instant};
use wire::{decode, Wire};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid client data: {0}")]
    Invalid(&'static str),
    #[error("the account or device does not match the local database")]
    IdentityMismatch,
    #[error("server rejected the request: {0:?}")]
    Server(ErrorInfo),
    #[error(
        "stream {target} requires recovery: {reason:?} (data loss possible: {data_loss_possible})"
    )]
    ResetRequired {
        target: u64,
        reason: ResetReason,
        data_loss_possible: bool,
    },
    #[error("local file operation failed")]
    File(#[from] io::Error),
    #[error("transport operation failed")]
    Transport(#[source] io::Error),
    #[error("transport request timed out")]
    Timeout,
    #[error("local database operation failed")]
    Storage(#[from] sqlx::Error),
    #[error(transparent)]
    Protocol(#[from] iam_protocol::ProtocolErrorKind),
    #[error("cannot decode local state")]
    Decode(#[from] prost::DecodeError),
}

#[derive(Clone, Copy, Debug)]
pub struct Identity {
    pub tenant_id: u64,
    pub user_id: u64,
    pub device_id: Id128,
}

/// Exchanges complete wire frames for a single logical connection. Implementations route pushes
/// separately from request responses. A timed-out exchange is followed by a fresh connection.
pub trait Transport: Send {
    /// Open a new connection, discarding the old connection and its buffered frames.
    fn reconnect(&mut self) -> impl Future<Output = io::Result<()>> + Send;

    fn exchange(&mut self, request: Vec<u8>) -> impl Future<Output = io::Result<Vec<u8>>> + Send;

    /// Cancel-safe wait for an unsolicited PUSH or a disconnect. Pull-only transports may omit it.
    fn receive(&mut self) -> impl Future<Output = io::Result<Vec<u8>>> + Send {
        std::future::pending()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// Maximum wait before starting the next periodic check after a successful sync.
    pub sync_interval: Duration,
    pub retry_min: Duration,
    pub retry_max: Duration,
    pub request_timeout: Duration,
    pub page_items: u32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            sync_interval: Duration::from_secs(30),
            retry_min: Duration::from_millis(250),
            retry_max: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            page_items: 100,
        }
    }
}

/// A submission sent to a running client. The result confirms local persistence.
pub struct Submission {
    pub conversation_id: u64,
    pub text: String,
    pub saved: oneshot::Sender<Result<MessageKey>>,
}

pub struct Client {
    identity: Identity,
    storage: Storage,
}

impl Client {
    /// Opens the local database for exclusive use by this client and binds it to this identity.
    pub async fn open(path: impl AsRef<Path>, identity: Identity) -> Result<Self> {
        validate_id128(&identity.device_id)?;
        if identity.tenant_id == 0 || identity.user_id == 0 {
            return Err(Error::Invalid("account identity"));
        }
        Ok(Self {
            identity,
            storage: Storage::open(path.as_ref(), identity).await?,
        })
    }

    /// Persists the full request before any attempt is made to send it.
    pub async fn enqueue(&mut self, conversation_id: u64, text: String) -> Result<MessageKey> {
        if conversation_id == 0 {
            return Err(Error::Invalid("conversation ID"));
        }
        let content = MessageContent {
            value: Some(message_content::Value::Text(TextContent {
                text: Some(text),
            })),
        };
        validate_message_content(&content)?;
        let id = generate_id128();
        let key = MessageKey {
            conversation_id: Some(conversation_id),
            client_msg_id: Some(id.to_vec().into()),
        };
        let request = SendRequest {
            key: Some(key.clone()),
            content: Some(content),
        };
        self.storage.enqueue(conversation_id, id, &request).await?;
        Ok(key)
    }

    /// Returns the saved result after a queue entry is released, including an unknown closed outcome.
    pub async fn outcome(&mut self, key: &MessageKey) -> Result<Option<SendOutcome>> {
        self.storage.outcome(key).await
    }

    pub async fn messages(&mut self, conversation_id: u64) -> Result<Vec<MessageView>> {
        self.storage.messages(conversation_id).await
    }

    /// Runs while the application is in the foreground. Closing the submission channel stops it.
    /// Each new connection authenticates as the same device and starts an account-first sync.
    pub async fn run(
        &mut self,
        transport: &mut impl Transport,
        access_token: &[u8],
        options: Options,
        submissions: &mut mpsc::Receiver<Submission>,
    ) -> Result<()> {
        if options.sync_interval.is_zero()
            || options.retry_min.is_zero()
            || options.retry_max < options.retry_min
            || options.request_timeout.is_zero()
            || !(1..=100).contains(&options.page_items)
        {
            return Err(Error::Invalid("client options"));
        }
        let mut wire = Wire::new(transport, options);
        let mut online = false;
        let mut connect_at = Instant::now();
        let mut sync_at = connect_at;
        let mut send_at = connect_at;
        let mut reconnect_delay = options.retry_min;
        let mut sync_delay = options.retry_min;
        let mut send_delay = options.retry_min;
        loop {
            if submissions.is_closed() && submissions.is_empty() {
                return Ok(());
            }
            let due = if online {
                sync_at.min(send_at)
            } else {
                connect_at
            };
            let result = tokio::select! {
                biased;
                // Keep the timer independent of a busy push stream.
                _ = sleep_until(due) => {
                    if !online {
                        let result = wire.authenticate(self.identity, access_token).await;
                        if result.is_ok() {
                            online = true;
                            sync_at = Instant::now();
                            send_at = sync_at;
                            sync_delay = options.retry_min;
                            send_delay = options.retry_min;
                        }
                        result
                    } else if sync_at <= send_at {
                        match self.synchronize(&mut wire).await {
                            Ok(()) => {
                                sync_at = Instant::now() + options.sync_interval;
                                sync_delay = options.retry_min;
                                reconnect_delay = options.retry_min;
                                Ok(())
                            }
                            Err(error) if retryable(&error) => {
                                sync_at = backoff(&mut sync_delay, options.retry_max, Some(&error));
                                Ok(())
                            }
                            Err(error) => Err(error),
                        }
                    } else {
                        match self.flush(&mut wire).await {
                            Ok(SendSweep::Progress) => {
                                send_at = Instant::now();
                                send_delay = options.retry_min;
                                Ok(())
                            }
                            Ok(SendSweep::Waiting) => {
                                send_at = backoff(&mut send_delay, options.retry_max, None);
                                Ok(())
                            }
                            Ok(SendSweep::Empty) => {
                                send_at = Instant::now() + options.retry_max;
                                send_delay = options.retry_min;
                                Ok(())
                            }
                            Err(error) if retryable(&error) => {
                                send_at = backoff(&mut send_delay, options.retry_max, Some(&error));
                                Ok(())
                            }
                            Err(error) => Err(error),
                        }
                    }
                }
                submission = submissions.recv() => {
                    let Some(submission) = submission else { return Ok(()); };
                    let saved = self.enqueue(submission.conversation_id, submission.text).await;
                    if saved.is_ok() {
                        send_at = Instant::now();
                    }
                    let _ = submission.saved.send(saved);
                    Ok(())
                }
                push = wire.transport.receive(), if online => {
                    match push {
                        Ok(bytes) => self.apply_push(&bytes).await,
                        Err(error) => Err(Error::Transport(error)),
                    }
                }
            };
            match result {
                Ok(()) => {}
                Err(error)
                    if (online && disconnected(&error))
                        || (!online && (transport_failed(&error) || retryable(&error))) =>
                {
                    online = false;
                    connect_at = backoff(&mut reconnect_delay, options.retry_max, Some(&error));
                }
                Err(error) => return Err(error),
            }
        }
    }
}

impl Client {
    async fn flush(&mut self, wire: &mut Wire<'_, impl Transport>) -> Result<SendSweep> {
        let heads = self.storage.heads().await?;
        if heads.is_empty() {
            return Ok(SendSweep::Empty);
        }
        let mut sweep = SendSweep::Waiting;
        for entry in heads {
            if self.send_head(wire, entry).await? {
                sweep = SendSweep::Progress;
            }
        }
        Ok(sweep)
    }

    async fn send_head(
        &mut self,
        wire: &mut Wire<'_, impl Transport>,
        entry: Pending,
    ) -> Result<bool> {
        let key = entry
            .request
            .key
            .as_ref()
            .ok_or(Error::Invalid("queued key"))?;
        if entry.attempted {
            let status = wire
                .request(
                    MsgType::SendStatus,
                    &SendStatusRequest {
                        key: Some(key.clone()),
                    },
                )
                .await?;
            let DecodedBody::SendStatusAck(status) = status else {
                return Err(Error::Invalid("expected SEND_STATUS_ACK"));
            };
            let outcome = send_outcome_for(key, status.key, status.outcome)?;
            if released(&outcome) {
                self.storage.finish(entry.ordinal, outcome).await?;
                return Ok(true);
            }
            if !matches!(outcome.value, Some(send_outcome::Value::Unresolved(ref result))
                if result.error.as_ref().and_then(|error| error.action) == Some(RetryAction::RetrySend as i32))
            {
                return Ok(false);
            }
        } else {
            // A crash after this write is treated as an uncertain attempt, even before transmission.
            self.storage.mark_attempted(entry.ordinal).await?;
        }
        let response = wire.request(MsgType::Send, &entry.request).await?;
        let DecodedBody::SendAck(response) = response else {
            return Err(Error::Invalid("expected SEND_ACK"));
        };
        let outcome = send_outcome_for(key, response.key, response.outcome)?;
        if released(&outcome) {
            self.storage.finish(entry.ordinal, outcome).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

enum SendSweep {
    Progress,
    Waiting,
    Empty,
}

fn send_outcome_for(
    expected: &MessageKey,
    key: Option<MessageKey>,
    outcome: Option<SendOutcome>,
) -> Result<SendOutcome> {
    if key.as_ref() != Some(expected) {
        return Err(Error::Invalid("send result key"));
    }
    outcome
        .filter(|outcome| outcome.value.is_some())
        .ok_or(Error::Invalid("missing send result"))
}

fn released(outcome: &SendOutcome) -> bool {
    match &outcome.value {
        Some(send_outcome::Value::Committed(_) | send_outcome::Value::RejectedFinal(_)) => true,
        Some(send_outcome::Value::Unresolved(result)) => result.acceptance_closed == Some(true),
        None => false,
    }
}

fn transport_failed(error: &Error) -> bool {
    matches!(error, Error::Transport(_) | Error::Timeout)
}

fn disconnected(error: &Error) -> bool {
    transport_failed(error)
        || matches!(error, Error::Server(info)
            if matches!(info.code(), ErrorCode::Unauthenticated | ErrorCode::SessionReplaced))
}

fn retryable(error: &Error) -> bool {
    matches!(error, Error::Server(info)
        if matches!(info.code(), ErrorCode::RateLimited | ErrorCode::Overloaded | ErrorCode::ResultUnavailable | ErrorCode::InternalError)
            || matches!(info.action(), RetryAction::RestartSync | RetryAction::ResumeSync))
}

fn backoff(delay: &mut Duration, max: Duration, error: Option<&Error>) -> Instant {
    let suggested = match error {
        Some(Error::Server(info)) => Duration::from_millis(info.retry_after_ms.unwrap_or(0).into()),
        _ => Duration::ZERO,
    };
    let due = Instant::now() + (*delay).max(suggested).min(max);
    *delay = delay.saturating_mul(2).min(max);
    due
}
