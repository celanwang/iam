use crate::store::{Append, TidbStore};
use crate::{
    same_content, windows, ConversationInfo, Error, Identity, Result, SavedSend, SendLookup,
    TextPage,
};
use iam_core::{Engine, EngineOutput};
use iam_protocol::{generate_id128, validate_id128, DecodedBody, DecodedFrame, Id128};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone, Copy, Default)]
pub enum OpenMode {
    /// Default after a restart: known results and authorized pages remain readable.
    #[default]
    ReadOnly,
    /// Explicit assertion that this is a new disposable local database, not a restored one.
    /// Both tables must be empty. Existing conversations never gain write permission.
    FreshDatabase,
}

/// Reuses Engine authentication and seeded conversation permissions, but never calls its
/// memory SEND/SYNC implementation. Native results are not wire SendOutcome::Committed.
pub struct TidbTextService {
    engine: Engine,
    store: TidbStore,
    writable: HashSet<(u64, u64)>,
    blocked: AtomicBool,
}

impl TidbTextService {
    /// Only loopback endpoints are accepted. Fresh mode creates the two tables;
    /// read-only mode only validates them. Neither mode rewrites existing records.
    pub async fn connect_local(url: &str, mut engine: Engine, mode: OpenMode) -> Result<Self> {
        if matches!(mode, OpenMode::FreshDatabase) && engine.store.recovery_incomplete {
            return Err(Error::ResultUnavailable);
        }
        let store = TidbStore::connect(url).await?;
        match mode {
            OpenMode::ReadOnly => {
                store.check_schema().await?;
                engine.store.recovery_incomplete = true;
            }
            OpenMode::FreshDatabase => {
                store.install_schema().await?;
                if !store.is_empty().await? {
                    return Err(Error::Configuration(
                        "fresh mode requires both tables to be empty",
                    ));
                }
            }
        }
        engine.store.set_now(store.now().await?);
        Ok(Self {
            engine,
            store,
            writable: HashSet::new(),
            blocked: AtomicBool::new(false),
        })
    }

    /// Only session commands enter the old Engine. SEND/SYNC/SNAPSHOT/HISTORY must not
    /// silently fall back to its unrelated in-memory messages or manufacture durability.
    pub async fn handle_session(
        &mut self,
        connection_id: Option<Id128>,
        frame: &DecodedFrame,
    ) -> Result<(Option<Id128>, EngineOutput)> {
        if !matches!(
            (frame.msg_type, &frame.body),
            (iam_protocol::MsgType::Auth, DecodedBody::Auth(_))
                | (iam_protocol::MsgType::Reconnect, DecodedBody::Reconnect(_))
                | (iam_protocol::MsgType::Heartbeat, DecodedBody::Heartbeat(_))
        ) {
            return Err(Error::InvalidRequest(
                "this entry point accepts only session commands",
            ));
        }
        self.observe_now().await?;
        Ok(self.engine.handle(connection_id, frame)?)
    }

    pub fn disconnect(&mut self, connection_id: Option<Id128>) {
        self.engine.disconnect(connection_id);
    }

    /// Irreversible for this service instance. Known results can still be read.
    pub fn stop_accepting(&mut self) {
        self.engine.store.recovery_incomplete = true;
    }

    pub async fn initialize_conversation(
        &mut self,
        connection_id: Id128,
        conversation_id: u64,
    ) -> Result<ConversationInfo> {
        let identity = self.identity(connection_id)?;
        self.authorize_conversation(identity, conversation_id)?;
        if self.engine.store.recovery_incomplete || self.blocked.load(Ordering::SeqCst) {
            return self.store.conversation(identity, conversation_id).await;
        }
        let guard = AcceptanceGuard {
            blocked: &self.blocked,
            completed: false,
        };
        let result = self.store.initialize(identity, conversation_id).await;
        let initialized = guard.finish(result)?;
        if initialized.created && !self.engine.store.recovery_incomplete {
            self.writable.insert((identity.tenant_id, conversation_id));
        }
        Ok(initialized)
    }

    pub async fn send_text(
        &mut self,
        connection_id: Id128,
        conversation_id: u64,
        client_msg_id: Id128,
        text: &str,
    ) -> Result<SavedSend> {
        let identity = self.identity(connection_id)?;
        if conversation_id == 0 {
            return Err(Error::InvalidRequest("conversation_id must be nonzero"));
        }
        let now = self.observe_now().await?;
        windows(&client_msg_id, now)?;
        // Replay precedes current membership, new-content rules, and recovery gates.
        if let Some(message) = self
            .store
            .lookup(identity, conversation_id, client_msg_id)
            .await?
        {
            if !same_content(&message.text, text) {
                return Err(Error::ContentConflict);
            }
            return Ok(SavedSend {
                receipt: message.receipt,
                replayed: true,
            });
        }
        if self.engine.store.recovery_incomplete
            || self.engine.store.clock_unreliable()
            || self.blocked.load(Ordering::SeqCst)
            || !self
                .writable
                .contains(&(identity.tenant_id, conversation_id))
        {
            return Err(Error::ResultUnavailable);
        }
        self.authorize_conversation(identity, conversation_id)?;
        // Cancellation is not rollback. Drop poisons new acceptance even if no response
        // can be delivered; a later operation may only replay a proven stored result.
        let guard = AcceptanceGuard {
            blocked: &self.blocked,
            completed: false,
        };
        let result = self
            .store
            .append(Append {
                identity,
                conversation_id,
                client_msg_id,
                text,
                message_id: generate_id128(),
                max_observed_now: &mut self.engine.store.max_observed_now_ms,
            })
            .await;
        let appended = guard.finish(result)?;
        self.engine.store.set_now(appended.observed_now);
        Ok(SavedSend {
            receipt: appended.message.receipt,
            replayed: appended.replayed,
        })
    }

    pub async fn query_send(
        &mut self,
        connection_id: Id128,
        conversation_id: u64,
        client_msg_id: Id128,
    ) -> Result<SendLookup> {
        let identity = self.identity(connection_id)?;
        if conversation_id == 0 {
            return Err(Error::InvalidRequest("conversation_id must be nonzero"));
        }
        let now = self.observe_now().await?;
        windows(&client_msg_id, now)?;
        Ok(
            match self
                .store
                .lookup(identity, conversation_id, client_msg_id)
                .await?
            {
                Some(message) => SendLookup::Found(message.receipt),
                None => SendLookup::Unknown,
            },
        )
    }

    /// Append-only descending message pages. Not a replacement for protocol HISTORY,
    /// opaque tokens, fixed historical views, or account/conversation SYNC.
    pub async fn read_messages(
        &self,
        connection_id: Id128,
        conversation_id: u64,
        before: Option<u64>,
        limit: u32,
    ) -> Result<TextPage> {
        let identity = self.identity(connection_id)?;
        self.authorize_conversation(identity, conversation_id)?;
        if limit == 0 || limit > 100 || before == Some(0) {
            return Err(Error::InvalidRequest(
                "page limit must be 1..100 and before must be nonzero",
            ));
        }
        self.store
            .page(identity, conversation_id, before, limit)
            .await
    }

    async fn observe_now(&mut self) -> Result<i64> {
        let now = self.store.now().await?;
        self.engine.store.set_now(now);
        Ok(now)
    }

    fn identity(&self, connection_id: Id128) -> Result<Identity> {
        let connection = self
            .engine
            .store
            .connections
            .get(&connection_id)
            .ok_or(Error::Unauthenticated)?;
        if self
            .engine
            .store
            .revoked_auth_epochs
            .contains(&connection.auth_epoch)
            || connection.tenant_id == 0
            || connection.user_id == 0
            || validate_id128(&connection.device_id).is_err()
        {
            return Err(Error::Unauthenticated);
        }
        Ok(Identity {
            tenant_id: connection.tenant_id,
            user_id: connection.user_id,
            device_id: connection.device_id,
        })
    }

    fn authorize_conversation(&self, identity: Identity, conversation_id: u64) -> Result<()> {
        match self
            .engine
            .store
            .conversations
            .get(&(identity.tenant_id, conversation_id))
        {
            Some(conversation) if conversation.members.contains(&identity.user_id) => Ok(()),
            _ => Err(Error::PermissionDenied),
        }
    }
}

struct AcceptanceGuard<'a> {
    blocked: &'a AtomicBool,
    completed: bool,
}

impl AcceptanceGuard<'_> {
    fn finish<T>(mut self, result: Result<T>) -> Result<T> {
        self.completed = !matches!(
            &result,
            Err(Error::CommitUnknown(_) | Error::Integrity(_) | Error::MissingConversation)
        );
        result
    }
}

impl Drop for AcceptanceGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.blocked.store(true, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_completion_does_not_block_or_reopen_acceptance() {
        let blocked = AtomicBool::new(false);
        AcceptanceGuard {
            blocked: &blocked,
            completed: false,
        }
        .finish(Ok(()))
        .unwrap();
        assert!(!blocked.load(Ordering::SeqCst));
        let rollback: Result<()> = Err(Error::Database(sqlx::Error::RowNotFound));
        assert!(AcceptanceGuard {
            blocked: &blocked,
            completed: false,
        }
        .finish(rollback)
        .is_err());
        assert!(!blocked.load(Ordering::SeqCst));
        drop(AcceptanceGuard {
            blocked: &blocked,
            completed: false,
        });
        assert!(blocked.load(Ordering::SeqCst));
        AcceptanceGuard {
            blocked: &blocked,
            completed: false,
        }
        .finish(Ok(()))
        .unwrap();
        assert!(blocked.load(Ordering::SeqCst));
    }

    #[test]
    fn unknown_or_inconsistent_outcomes_block_new_acceptance() {
        for error in [
            Error::CommitUnknown(sqlx::Error::PoolClosed),
            Error::Integrity("invalid history"),
            Error::MissingConversation,
        ] {
            let blocked = AtomicBool::new(false);
            let result: Result<()> = Err(error);
            assert!(AcceptanceGuard {
                blocked: &blocked,
                completed: false,
            }
            .finish(result)
            .is_err());
            assert!(blocked.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn dropping_a_suspended_write_future_blocks_acceptance() {
        use std::future::Future;
        use std::task::{Context, Waker};

        let blocked = AtomicBool::new(false);
        let mut operation = Box::pin(async {
            let guard = AcceptanceGuard {
                blocked: &blocked,
                completed: false,
            };
            std::future::pending::<()>().await;
            guard.finish(Ok(()))
        });
        assert!(operation
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        drop(operation);
        assert!(blocked.load(Ordering::SeqCst));
    }
}
