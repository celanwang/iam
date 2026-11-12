use crate::store::{Scope, Stream, TidbStore};
use crate::{Error, Identity};
use iam_core::{Engine, SnapshotToken, SyncCursor, SyncPageToken};
use iam_protocol::pb::{
    change, snapshot_item, snapshot_response, sync_response, sync_target, Change, ErrorCode,
    HistoryWindow, ResetReason, ResetRequired, RetryAction, SnapshotItem, SnapshotPage,
    SnapshotRequest, SyncPage, SyncRequest,
};
use iam_protocol::{error_info, generate_id128};
use prost::Message;

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
            access_version: self.stream.scope.access_version(),
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
    pub async fn sync(&self, identity: Identity, request: &SyncRequest) -> sync_response::Value {
        self.sync_page(identity, request)
            .await
            .unwrap_or_else(|(code, action)| sync_response::Value::Error(error_info(code, action)))
    }

    async fn sync_page(
        &self,
        identity: Identity,
        request: &SyncRequest,
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
        let read = self.read_context(identity, scope).await?;
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
        if cursor.access_version != scope.access_version() {
            return Ok(self.reset(&read, ResetReason::AccessChanged, false));
        }
        if expired {
            return if is_page {
                Err((ErrorCode::PageTokenExpired, RetryAction::ResumeSync))
            } else {
                Ok(self.reset(&read, ResetReason::CursorExpired, false))
            };
        }
        let entries = self
            .entries(identity, scope, round.after, round.upper, items)
            .await
            .map_err(read_error)?;
        let changes = entries
            .into_iter()
            .map(|entry| {
                let payload = match entry.item {
                    snapshot_item::Value::Conversation(state) => {
                        change::Payload::ConversationUpsert(state)
                    }
                    snapshot_item::Value::Message(message) => {
                        change::Payload::MessageUpsert(message)
                    }
                };
                Change {
                    event_seq: Some(entry.sequence),
                    payload: Some(payload),
                }
            })
            .collect();
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
        let (from, through) = snapshot_window(read.stream.scope, read.stream.upper);
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
        self.snapshot_page(identity, request)
            .await
            .unwrap_or_else(|(code, action)| {
                snapshot_response::Value::Error(error_info(code, action))
            })
    }

    async fn snapshot_page(
        &self,
        identity: Identity,
        request: &SnapshotRequest,
    ) -> ReadResult<snapshot_response::Value> {
        let (items, bytes) = Engine::require_page_limit(&request.limit)?;
        let token = request.page_token.as_ref().ok_or(INVALID_ARGUMENT)?;
        let now = self.now().await.map_err(read_error)?;
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
        let read = ReadContext {
            identity,
            stream: self.stream(identity, scope).await.map_err(read_error)?,
            now,
        };
        if snap.epoch != read.stream.epoch || snap.access_version != scope.access_version() {
            return Err((ErrorCode::ContextChanged, RetryAction::RestartSync));
        }
        if expired || snap.upper_bound > read.stream.upper {
            return Err((ErrorCode::PageTokenExpired, RetryAction::RestartSync));
        }
        let (from, through) = snapshot_window(scope, snap.upper_bound);
        if (snap.from_create_seq, snap.through_create_seq) != (from, through) {
            return Err(INVALID_SNAPSHOT);
        }
        let start = snap.scanned_items;
        let after = from
            .saturating_sub(1)
            .checked_add(start)
            .filter(|position| *position <= snap.upper_bound)
            .ok_or(INVALID_SNAPSHOT)?;
        let entries = self
            .entries(identity, scope, after, snap.upper_bound, items)
            .await
            .map_err(read_error)?;
        let mut page = SnapshotPage {
            snapshot_id: Some(snap.snapshot_id.to_vec().into()),
            context: Some(read.stream.context()),
            upper_bound: Some(snap.upper_bound),
            history_window: (!snap.is_account).then_some(HistoryWindow {
                from_create_seq: Some(from),
                through_create_seq: Some(through),
            }),
            items: entries
                .into_iter()
                .map(|entry| SnapshotItem {
                    value: Some(entry.item),
                })
                .collect(),
            ..Default::default()
        };
        loop {
            let size = page.items.len() as u64;
            let done = after + size == snap.upper_bound;
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

    async fn read_context(&self, identity: Identity, scope: Scope) -> ReadResult<ReadContext> {
        let now = self.now().await.map_err(read_error)?;
        let stream = self.stream(identity, scope).await.map_err(read_error)?;
        Ok(ReadContext {
            identity,
            stream,
            now,
        })
    }
}

fn snapshot_window(scope: Scope, upper: u64) -> (u64, u64) {
    if scope == Scope::Account || upper == 0 {
        (0, 0)
    } else {
        (upper.saturating_sub(99).max(1), upper)
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
