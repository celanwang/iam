use crate::store::{Scope, Stream, TidbStore};
use crate::{Error, Identity};
use iam_core::{Engine, HistoryToken, SnapshotToken, SyncCursor, SyncPageToken};
use iam_protocol::pb::{
    change, history_response, snapshot_item, snapshot_response, sync_response, sync_target,
    ErrorCode, HistoryPage, HistoryRequest, HistoryWindow, ResetReason, ResetRequired, RetryAction,
    SnapshotItem, SnapshotPage, SnapshotRequest, SyncPage, SyncRequest,
};
use iam_protocol::{error_info, generate_id128, validate_id128};
use prost::Message;
use sqlx::MySqlConnection;

type ReadError = (ErrorCode, RetryAction);
type ReadResult<T> = Result<T, ReadError>;

const INVALID_ARGUMENT: ReadError = (ErrorCode::InvalidArgument, RetryAction::None);
const INVALID_CURSOR: ReadError = (ErrorCode::InvalidCursor, RetryAction::RestartSync);
const INVALID_PAGE: ReadError = (ErrorCode::InvalidToken, RetryAction::ResumeSync);
const INVALID_SNAPSHOT: ReadError = (ErrorCode::InvalidToken, RetryAction::RestartSync);
const BUDGET_TOO_SMALL: ReadError = (ErrorCode::PageBudgetTooSmall, RetryAction::RetrySameRequest);

struct ReadContext {
    identity: Identity,
    stream: Stream,
    now: i64,
}

impl ReadContext {
    fn cursor(&self, covered: u64) -> SyncCursor {
        SyncCursor {
            tenant_id: self.identity.tenant_id,
            user_id: self.identity.user_id,
            device_id: self.identity.device_id,
            conversation_id: self.stream.scope.conversation_id(),
            epoch: self.stream.epoch,
            access_version: self.stream.access_version,
            covered_through: covered,
            issued_at_ms: self.now,
            is_account: self.stream.scope == Scope::Account,
        }
    }

    fn owns_cursor(&self, cursor: &SyncCursor) -> bool {
        cursor.tenant_id == self.identity.tenant_id
            && cursor.user_id == self.identity.user_id
            && cursor.device_id == self.identity.device_id
            && cursor.conversation_id == self.stream.scope.conversation_id()
            && cursor.is_account == (self.stream.scope == Scope::Account)
    }
}

struct Round {
    after: u64,
    upper: u64,
    started: i64,
}

impl TidbStore {
    pub async fn sync_at(
        &self,
        identity: Identity,
        request: &SyncRequest,
        now: i64,
    ) -> sync_response::Value {
        self.read_sync(identity, request, now)
            .await
            .unwrap_or_else(|(code, action)| sync_response::Value::Error(error_info(code, action)))
    }

    async fn read_sync(
        &self,
        identity: Identity,
        request: &SyncRequest,
        now: i64,
    ) -> ReadResult<sync_response::Value> {
        #[cfg(any(test, feature = "profile"))]
        let start = std::time::Instant::now();
        let mut tx = self.begin_read().await.map_err(read_error)?;
        #[cfg(feature = "profile")]
        crate::profile::record("sync_pool_begin", start.elapsed());
        let page = self.sync_page(&mut tx, identity, request, now).await;
        tx.commit()
            .await
            .map_err(|error| read_error(error.into()))?;
        #[cfg(test)]
        crate::store::perf::record("sync_read_tx", start);
        #[cfg(feature = "profile")]
        crate::profile::record("sync_read_tx", start.elapsed());
        page
    }

    async fn sync_page(
        &self,
        db: &mut MySqlConnection,
        identity: Identity,
        request: &SyncRequest,
        now: i64,
    ) -> ReadResult<sync_response::Value> {
        if request.cursor.is_some() && request.page_token.is_some() {
            return Err(INVALID_ARGUMENT);
        }
        let (items, bytes) = Engine::require_page_limit(&request.limit)?;
        let scope = match request
            .target
            .as_ref()
            .and_then(|target| target.value.as_ref())
        {
            Some(sync_target::Value::Account(_)) => Scope::Account,
            Some(sync_target::Value::Conversation(id)) if *id != 0 => Scope::Conversation(*id),
            _ => return Err(INVALID_ARGUMENT),
        };
        let read = self.read_context(db, identity, scope, now).await?;
        let (cursor, expired, round) = if let Some(token) = &request.page_token {
            let (page, expired) = self
                .tokens
                .verify_page_with_expiry(token, read.now)
                .map_err(|_| INVALID_PAGE)?;
            let round = Round {
                after: page.scanned_through,
                upper: page.upper_bound,
                started: page.round_started_ms,
            };
            if round.after != page.cursor.covered_through || round.after > round.upper {
                return Err(INVALID_PAGE);
            }
            (page.cursor, expired, round)
        } else if let Some(token) = &request.cursor {
            let (cursor, expired) = self
                .tokens
                .verify_cursor_with_expiry(token, read.now)
                .map_err(|_| INVALID_CURSOR)?;
            let round = Round {
                after: cursor.covered_through,
                upper: read.stream.upper,
                started: read.now,
            };
            (cursor, expired, round)
        } else {
            return Ok(self.reset(&read, ResetReason::InitialSync, false));
        };
        let is_page = request.page_token.is_some();
        if !read.owns_cursor(&cursor) {
            return Err(if is_page {
                INVALID_PAGE
            } else {
                INVALID_CURSOR
            });
        }
        let beyond_end = round.after > read.stream.upper || round.upper > read.stream.upper;
        if cursor.epoch != read.stream.epoch || beyond_end {
            return Ok(self.reset(&read, ResetReason::HistoryReset, beyond_end));
        }
        if cursor.access_version != read.stream.access_version {
            return Ok(self.reset(&read, ResetReason::AccessChanged, false));
        }
        if expired || round.after < read.stream.floor - 1 {
            return if is_page {
                Err((ErrorCode::PageTokenExpired, RetryAction::ResumeSync))
            } else {
                Ok(self.reset(&read, ResetReason::CursorExpired, false))
            };
        }
        let changes = Self::changes(db, identity, scope, round.after, round.upper, items)
            .await
            .map_err(read_error)?;
        let mut page = SyncPage {
            context: Some(read.stream.context()),
            upper_bound: Some(round.upper),
            changes,
            ..Default::default()
        };
        loop {
            let covered = round.after + page.changes.len() as u64;
            let cursor = read.cursor(covered);
            page.covered_through = Some(covered);
            page.next_cursor = Some(self.tokens.sign_cursor(&cursor).into());
            page.next_page_token = (covered < round.upper).then(|| {
                self.tokens
                    .sign_page(&SyncPageToken {
                        cursor,
                        upper_bound: round.upper,
                        scanned_through: covered,
                        round_started_ms: round.started,
                    })
                    .into()
            });
            if fits(&page, bytes) {
                return Ok(sync_response::Value::Page(page));
            }
            page.changes.pop();
            if page.changes.is_empty() {
                return Err(BUDGET_TOO_SMALL);
            }
        }
    }

    fn reset(
        &self,
        read: &ReadContext,
        reason: ResetReason,
        loss_possible: bool,
    ) -> sync_response::Value {
        let cursor = read.cursor(0);
        let (from, through) = snapshot_window(&read.stream, read.stream.upper);
        let token = SnapshotToken {
            tenant_id: cursor.tenant_id,
            user_id: cursor.user_id,
            device_id: cursor.device_id,
            is_account: cursor.is_account,
            conversation_id: cursor.conversation_id,
            snapshot_id: generate_id128(),
            epoch: cursor.epoch,
            access_version: cursor.access_version,
            upper_bound: read.stream.upper,
            from_create_seq: from,
            through_create_seq: through,
            scanned_items: 0,
            created_at_ms: read.now,
        };
        sync_response::Value::Reset(ResetRequired {
            reason: Some(reason as i32),
            snapshot_token: Some(self.tokens.sign_snapshot(&token).into()),
            data_loss_possible: Some(loss_possible),
        })
    }

    pub async fn snapshot(
        &self,
        identity: Identity,
        request: &SnapshotRequest,
    ) -> snapshot_response::Value {
        self.read_snapshot(identity, request)
            .await
            .unwrap_or_else(|(code, action)| {
                snapshot_response::Value::Error(error_info(code, action))
            })
    }

    async fn read_snapshot(
        &self,
        identity: Identity,
        request: &SnapshotRequest,
    ) -> ReadResult<snapshot_response::Value> {
        let now = self.now().await.map_err(read_error)?;
        let mut tx = self.begin_read().await.map_err(read_error)?;
        let page = self.snapshot_page(&mut tx, identity, request, now).await;
        tx.commit()
            .await
            .map_err(|error| read_error(error.into()))?;
        page
    }

    async fn snapshot_page(
        &self,
        db: &mut MySqlConnection,
        identity: Identity,
        request: &SnapshotRequest,
        now: i64,
    ) -> ReadResult<snapshot_response::Value> {
        let (items, bytes) = Engine::require_page_limit(&request.limit)?;
        let token = request.page_token.as_ref().ok_or(INVALID_ARGUMENT)?;
        let (mut snap, expired) = self
            .tokens
            .verify_snapshot_with_expiry(token, now)
            .map_err(|_| INVALID_SNAPSHOT)?;
        if snap.tenant_id != identity.tenant_id
            || snap.user_id != identity.user_id
            || snap.device_id != identity.device_id
            || snap.is_account != (snap.conversation_id == 0)
        {
            return Err(INVALID_SNAPSHOT);
        }
        let scope = if snap.is_account {
            Scope::Account
        } else {
            Scope::Conversation(snap.conversation_id)
        };
        let read = self.read_context(db, identity, scope, now).await?;
        if snap.epoch != read.stream.epoch
            || snap.access_version != read.stream.access_version
            || (scope == Scope::Account && snap.upper_bound != read.stream.upper)
        {
            return Err((ErrorCode::ContextChanged, RetryAction::RestartSync));
        }
        if expired || snap.upper_bound > read.stream.upper {
            return Err((ErrorCode::PageTokenExpired, RetryAction::RestartSync));
        }
        let (from, through) = snapshot_window(&read.stream, snap.upper_bound);
        if (snap.from_create_seq, snap.through_create_seq) != (from, through) {
            return Err((ErrorCode::ContextChanged, RetryAction::RestartSync));
        }
        let start = snap.scanned_items;
        let (mut entries, has_more) = if scope == Scope::Account {
            let states = Self::account_snapshot(db, identity, start, items + 1)
                .await
                .map_err(read_error)?;
            let more = states.len() > items;
            (
                states
                    .into_iter()
                    .take(items)
                    .map(snapshot_item::Value::Conversation)
                    .collect::<Vec<_>>(),
                more,
            )
        } else {
            let after = from
                .saturating_sub(1)
                .checked_add(start)
                .filter(|position| *position <= through)
                .ok_or(INVALID_SNAPSHOT)?;
            let changes = Self::changes(db, identity, scope, after, through, items)
                .await
                .map_err(read_error)?;
            let more = after + (changes.len() as u64) < through;
            let entries = changes
                .into_iter()
                .map(|change| match change.payload {
                    Some(change::Payload::MessageUpsert(view)) => {
                        Ok(snapshot_item::Value::Message(view))
                    }
                    _ => Err(INVALID_SNAPSHOT),
                })
                .collect::<ReadResult<Vec<_>>>()?;
            (entries, more)
        };
        let initial_items = entries.len();
        let mut page = SnapshotPage {
            snapshot_id: Some(snap.snapshot_id.to_vec().into()),
            context: Some(read.stream.context()),
            upper_bound: Some(snap.upper_bound),
            history_window: (!snap.is_account).then_some(HistoryWindow {
                from_create_seq: Some(from),
                through_create_seq: Some(through),
            }),
            items: entries
                .drain(..)
                .map(|entry| SnapshotItem { value: Some(entry) })
                .collect(),
            ..Default::default()
        };
        loop {
            let size = page.items.len() as u64;
            let done = !has_more && page.items.len() == initial_items;
            snap.scanned_items = start + size;
            page.next_page_token = (!done).then(|| self.tokens.sign_snapshot(&snap).into());
            page.resume_cursor = done.then(|| {
                self.tokens
                    .sign_cursor(&read.cursor(snap.upper_bound))
                    .into()
            });
            if fits(&page, bytes) {
                return Ok(snapshot_response::Value::Page(page));
            }
            page.items.pop();
            if page.items.is_empty() {
                return Err(BUDGET_TOO_SMALL);
            }
        }
    }

    pub async fn history(
        &self,
        identity: Identity,
        request: &HistoryRequest,
    ) -> history_response::Value {
        self.read_history(identity, request)
            .await
            .map(history_response::Value::Page)
            .unwrap_or_else(|(code, action)| {
                history_response::Value::Error(error_info(code, action))
            })
    }

    async fn read_history(
        &self,
        identity: Identity,
        request: &HistoryRequest,
    ) -> ReadResult<HistoryPage> {
        let (items, bytes) = Engine::require_page_limit(&request.limit)?;
        let context = request.context.as_ref().ok_or(INVALID_ARGUMENT)?;
        let Some(sync_target::Value::Conversation(conversation)) = context
            .target
            .as_ref()
            .and_then(|target| target.value.as_ref())
        else {
            return Err(INVALID_ARGUMENT);
        };
        if *conversation == 0
            || context.access_version.unwrap_or(0) == 0
            || validate_id128(context.epoch.as_deref().unwrap_or_default()).is_err()
            || request.before_create_seq == Some(0)
            || (request.before_create_seq.is_some() && request.page_token.is_some())
        {
            return Err(INVALID_ARGUMENT);
        }
        let now = self.now().await.map_err(read_error)?;
        let mut tx = self.begin_read().await.map_err(read_error)?;
        let read = self
            .read_context(&mut tx, identity, Scope::Conversation(*conversation), now)
            .await?;
        if *context != read.stream.context() {
            return Err((ErrorCode::ContextChanged, RetryAction::RestartSync));
        }
        let invalid = (ErrorCode::InvalidToken, RetryAction::RestartHistory);
        let expired = (ErrorCode::PageTokenExpired, RetryAction::RestartHistory);
        let mut token = if let Some(bytes) = &request.page_token {
            let (token, token_expired) = self
                .tokens
                .verify_history_with_expiry(bytes, now)
                .map_err(|_| invalid)?;
            if token.tenant_id != identity.tenant_id
                || token.user_id != identity.user_id
                || token.device_id != identity.device_id
                || token.conversation_id != *conversation
            {
                return Err(invalid);
            }
            if token.epoch != read.stream.epoch
                || token.access_version != read.stream.access_version
            {
                return Err((ErrorCode::ContextChanged, RetryAction::RestartSync));
            }
            if token_expired
                || token.upper_bound > read.stream.upper
                || token.retention_floor_seq != read.stream.floor
            {
                return Err(expired);
            }
            token
        } else {
            let before = request.before_create_seq.unwrap_or(u64::MAX);
            HistoryToken {
                tenant_id: identity.tenant_id,
                user_id: identity.user_id,
                device_id: identity.device_id,
                conversation_id: *conversation,
                epoch: read.stream.epoch,
                access_version: read.stream.access_version,
                before_create_seq: before,
                upper_bound: read.stream.upper,
                retention_floor_seq: read.stream.floor,
                scanned_through_seq: before,
                round_started_ms: now,
            }
        };
        let mut messages = Self::history_messages(
            &mut tx,
            identity,
            &read.stream,
            token.scanned_through_seq,
            token.upper_bound,
            items + 1,
        )
        .await
        .map_err(read_error)?;
        tx.commit()
            .await
            .map_err(|error| read_error(error.into()))?;
        let available = messages.len();
        messages.truncate(items);
        let mut page = HistoryPage {
            context: Some(read.stream.context()),
            upper_bound: Some(token.upper_bound),
            retention_floor_seq: Some(token.retention_floor_seq),
            messages,
            next_page_token: None,
        };
        loop {
            page.next_page_token = if page.messages.len() < available {
                token.scanned_through_seq = page
                    .messages
                    .last()
                    .and_then(|message| message.position.as_ref())
                    .and_then(|position| position.create_seq)
                    .ok_or(BUDGET_TOO_SMALL)?;
                Some(self.tokens.sign_history(&token).into())
            } else {
                None
            };
            if fits(&page, bytes) {
                return Ok(page);
            }
            if page.messages.pop().is_none() {
                return Err(BUDGET_TOO_SMALL);
            }
        }
    }

    async fn read_context(
        &self,
        db: &mut MySqlConnection,
        identity: Identity,
        scope: Scope,
        now: i64,
    ) -> ReadResult<ReadContext> {
        let stream = self
            .read_stream(db, identity, scope)
            .await
            .map_err(read_error)?;
        Ok(ReadContext {
            identity,
            stream,
            now,
        })
    }
}

fn snapshot_window(stream: &Stream, upper: u64) -> (u64, u64) {
    if stream.scope == Scope::Account || upper < stream.floor {
        (0, 0)
    } else {
        (upper.saturating_sub(99).max(stream.floor), upper)
    }
}

fn read_error(error: Error) -> ReadError {
    match error {
        Error::PermissionDenied => (ErrorCode::PermissionDenied, RetryAction::None),
        _ => (ErrorCode::InternalError, RetryAction::RetrySameRequest),
    }
}

fn fits(page: &impl Message, budget: usize) -> bool {
    // A successful response wraps the page in length-delimited field 1.
    let size = page.encoded_len();
    1 + prost::length_delimiter_len(size) + size <= budget
}
