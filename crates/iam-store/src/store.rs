use crate::{
    receipt, text_content, ConversationInfo, Error, Identity, Result, StoredText, TextPage,
};
use iam_core::{MemoryStore, TokenSigner, ACCEPT_WINDOW_MS, ALLOW_SKEW_MS};
use iam_protocol::pb::{
    change, message_content, send_outcome, sync_target, ConversationKind, ConversationState, Empty,
    ErrorCode, MessageContent, MessageView, RetryAction, SendOutcome, SendUnresolved,
    StreamContext, SyncTarget,
};
use iam_protocol::{
    error_info, generate_id128, validate_id128, validate_message_content, Id128, ProtocolErrorKind,
};
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlRow};
use sqlx::{ConnectOptions, MySql, MySqlConnection, MySqlPool, Row, Transaction};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    time::Duration,
};

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../tests/support/mod.rs"]
mod support;

#[cfg(test)]
#[path = "perf.rs"]
pub(crate) mod perf;

#[path = "membership.rs"]
mod membership;
use membership::write_account_event;

const CLOCK_SQL: &str = "SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED)";

const MESSAGES: &str = "SELECT m.conversation_id, m.message_id, m.user_id, m.`sequence`, \
                        m.`text`, m.accepted_at, c.history_id \
                        FROM messages m JOIN conversations c \
                        ON c.tenant_id = m.tenant_id AND c.conversation_id = m.conversation_id";

const CONTROL: &str = "SELECT store_id, token_key, max_observed_now_ms, closed_through_client_ms \
                       FROM store_control WHERE singleton = 1 FOR UPDATE";

const INITIAL_ACCESS: u64 = 1;
const TABLES: [&str; 7] = [
    "store_control",
    "conversations",
    "messages",
    "rejections",
    "members",
    "account_events",
    "conversation_retention",
];

struct DecisionClock {
    now: i64,
    previous: i64,
    watermark: i64,
}

impl DecisionClock {
    fn reliable(&self) -> bool {
        self.now >= self.previous
    }

    fn effective_now(&self) -> i64 {
        self.now.max(self.previous)
    }
}

pub(crate) struct TidbStore {
    pool: MySqlPool,
    id: Id128,
    pub tokens: TokenSigner,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Scope {
    Account,
    Conversation(u64),
}

impl Scope {
    pub fn conversation_id(self) -> u64 {
        match self {
            Self::Account => 0,
            Self::Conversation(id) => id,
        }
    }
}

pub(crate) struct Stream {
    pub scope: Scope,
    pub epoch: Id128,
    pub upper: u64,
    pub access_version: u64,
    pub floor: u64,
}

impl Stream {
    pub fn context(&self) -> StreamContext {
        let target = match self.scope {
            Scope::Account => sync_target::Value::Account(Empty {}),
            Scope::Conversation(id) => sync_target::Value::Conversation(id),
        };
        StreamContext {
            target: Some(SyncTarget {
                value: Some(target),
            }),
            epoch: Some(self.epoch.to_vec().into()),
            access_version: (self.scope != Scope::Account).then_some(self.access_version),
        }
    }
}

pub(crate) struct SendKey {
    pub owner: Identity,
    pub conversation: u64,
    pub client_id: Id128,
}

pub(crate) struct Delivery {
    pub members: Vec<(u64, u64)>,
    pub message: MessageView,
    pub stream: Stream,
}

pub(crate) struct Decision {
    pub outcome: SendOutcome,
    pub delivery: Option<Delivery>,
}

impl Decision {
    fn outcome(value: send_outcome::Value) -> Self {
        Self {
            outcome: SendOutcome { value: Some(value) },
            delivery: None,
        }
    }
}

pub(crate) fn unresolved(
    code: ErrorCode,
    action: RetryAction,
    closed: bool,
) -> send_outcome::Value {
    send_outcome::Value::Unresolved(SendUnresolved {
        error: Some(error_info(code, action)),
        acceptance_closed: Some(closed),
    })
}

fn rejected(code: ErrorCode) -> send_outcome::Value {
    send_outcome::Value::RejectedFinal(error_info(code, RetryAction::None))
}

impl TidbStore {
    pub async fn open(url: &str, id: Id128, seeds: Option<&MemoryStore>) -> Result<Self> {
        validate_id128(&id).map_err(|_| Error::Configuration("invalid protocol store ID"))?;
        let pool = connect_pool(url).await?;
        reject_legacy_names(&pool).await?;
        if seeds.is_some() {
            require_empty_tables(&pool).await?;
            for statement in include_str!("../schema.sql").split(';') {
                if !statement.trim().is_empty() {
                    sqlx::query(statement).execute(&pool).await?;
                }
            }
        }
        check_schema(&pool, false).await?;
        if let Some(seeds) = seeds {
            let mut transaction = pool.begin().await?;
            let result = initialize(&mut transaction, &id, seeds).await;
            finish(transaction, result).await?;
        }

        let mut transaction = pool.begin().await?;
        let row = sqlx::query(CONTROL).fetch_one(&mut *transaction).await?;
        check_store_id(&row, id)?;
        let key: Vec<u8> = row.try_get("token_key")?;
        let key = key
            .try_into()
            .map_err(|_| Error::Integrity("invalid token key"))?;
        control_clock(&row)?;
        let missing_event: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM members m LEFT JOIN account_events e
             USING (tenant_id, user_id, account_sequence)
             WHERE e.access_version IS NULL OR e.conversation_id <> m.conversation_id LIMIT 1",
        )
        .fetch_optional(&mut *transaction)
        .await?;
        if missing_event.is_some() {
            return Err(Error::Integrity(
                "active membership is missing its account event",
            ));
        }
        finish(transaction, Ok(())).await?;
        Ok(Self {
            pool,
            id,
            tokens: TokenSigner::new(key),
        })
    }

    pub async fn now(&self) -> Result<i64> {
        #[cfg(test)]
        let start = std::time::Instant::now();
        let mut tx = self.pool.begin().await?;
        let result = self
            .observe_clock(&mut tx)
            .await
            .map(|clock| clock.effective_now());
        let result = finish(tx, result).await;
        #[cfg(test)]
        perf::record("clock_tx", start);
        result
    }

    pub async fn decide(
        &self,
        key: &SendKey,
        content: Option<&MessageContent>,
        accepting: bool,
    ) -> Result<Decision> {
        #[cfg(test)]
        let start = std::time::Instant::now();
        let mut tx = self.pool.begin().await?;
        let result = self.decide_in(&mut tx, key, content, accepting).await;
        let result = finish(tx, result).await;
        #[cfg(test)]
        perf::record("send_tx", start);
        result
    }

    async fn decide_in(
        &self,
        tx: &mut Transaction<'_, MySql>,
        key: &SendKey,
        content: Option<&MessageContent>,
        accepting: bool,
    ) -> Result<Decision> {
        let clock = self.observe_clock(tx).await?;
        let (time, _, query_until) = MemoryStore::windows_for_client_msg(&key.client_id);
        let closed = time <= clock.watermark;
        let unavailable =
            || unresolved(ErrorCode::ResultUnavailable, RetryAction::QuerySend, closed);
        if clock.effective_now() >= query_until {
            return Ok(Decision::outcome(if closed {
                unresolved(ErrorCode::ResultExpired, RetryAction::None, true)
            } else {
                unavailable()
            }));
        }

        let digest = content.map(MemoryStore::content_digest);
        if let Some(value) = Self::lookup(tx, key, digest).await? {
            return Ok(Decision::outcome(value));
        }
        if !clock.reliable() {
            return Ok(Decision::outcome(unavailable()));
        }
        if time > clock.now.saturating_add(ALLOW_SKEW_MS) {
            return Ok(Decision::outcome(unresolved(
                ErrorCode::ClockSkew,
                RetryAction::None,
                false,
            )));
        }
        if closed {
            return Ok(Decision::outcome(rejected(ErrorCode::RequestExpired)));
        }
        if !accepting {
            return Ok(Decision::outcome(unavailable()));
        }
        let Some(content) = content else {
            return Ok(Decision::outcome(unresolved(
                ErrorCode::NotFound,
                RetryAction::RetrySend,
                false,
            )));
        };
        let digest = digest.expect("present content has a digest");
        if let Err(error) = validate_message_content(content) {
            let code = if matches!(error, ProtocolErrorKind::ResourceLimit(_)) {
                ErrorCode::ResourceLimit
            } else {
                ErrorCode::InvalidArgument
            };
            return Self::record_rejection(tx, key, digest, code).await;
        }
        let members: Vec<(u64, u64)> = sqlx::query_as(
            "SELECT m.user_id, e.access_version FROM members m JOIN account_events e
             USING (tenant_id, user_id, account_sequence)
             WHERE m.tenant_id = ? AND m.conversation_id = ?",
        )
        .bind(key.owner.tenant_id)
        .bind(key.conversation)
        .fetch_all(&mut **tx)
        .await?;
        if !members.iter().any(|(user, _)| *user == key.owner.user_id) {
            return Self::record_rejection(tx, key, digest, ErrorCode::PermissionDenied).await;
        }
        let Some(message_content::Value::Text(text)) = content.value.as_ref() else {
            return Err(Error::Integrity("accepted content is not text"));
        };
        Self::append(tx, key, text.text.as_deref().unwrap(), clock.now, members).await
    }

    async fn observe_clock(&self, tx: &mut Transaction<'_, MySql>) -> Result<DecisionClock> {
        #[cfg(test)]
        let start = std::time::Instant::now();
        let row = sqlx::query(CONTROL).fetch_one(&mut **tx).await;
        #[cfg(test)]
        perf::record("control_sql", start);
        let row = row?;
        check_store_id(&row, self.id)?;
        let (previous, previous_watermark) = control_clock(&row)?;
        let now: i64 = sqlx::query_scalar(CLOCK_SQL).fetch_one(&mut **tx).await?;
        if now < 0 {
            return Err(Error::Integrity("invalid database clock"));
        }
        let reliable = now >= previous;
        let watermark = if reliable {
            closed_through(now)
        } else {
            previous_watermark
        };
        if reliable && (now != previous || watermark != previous_watermark) {
            sqlx::query(
                "UPDATE store_control SET max_observed_now_ms = ?, \
                         closed_through_client_ms = ? WHERE singleton = 1",
            )
            .bind(now)
            .bind(watermark)
            .execute(&mut **tx)
            .await?;
        }
        Ok(DecisionClock {
            now,
            previous,
            watermark,
        })
    }

    async fn lookup(
        tx: &mut Transaction<'_, MySql>,
        key: &SendKey,
        digest: Option<[u8; 32]>,
    ) -> Result<Option<send_outcome::Value>> {
        let query = format!(
            "{MESSAGES} WHERE m.tenant_id = ? AND m.user_id = ? \
                            AND m.device_id = ? AND m.conversation_id = ? AND m.client_msg_id = ?"
        );
        let row = bind_key(&query, key).fetch_optional(&mut **tx).await?;
        let conflict = || unresolved(ErrorCode::IdempotencyConflict, RetryAction::QuerySend, true);
        if let Some(row) = row {
            let message = decode_message(&row)?;
            if digest.is_some_and(|d| {
                d != MemoryStore::content_digest(&crate::text_content(&message.text))
            }) {
                return Ok(Some(conflict()));
            }
            return Ok(Some(send_outcome::Value::Committed(message.receipt)));
        }
        let row = bind_key("SELECT digest, error_code FROM rejections WHERE tenant_id = ? \
                           AND user_id = ? AND device_id = ? AND conversation_id = ? AND client_msg_id = ?", key)
            .fetch_optional(&mut **tx)
            .await?;
        let Some(row) = row else { return Ok(None) };
        let stored_digest: Vec<u8> = row.try_get("digest")?;
        if digest.is_some_and(|d| d.as_slice() != stored_digest) {
            return Ok(Some(conflict()));
        }
        let code = ErrorCode::try_from(row.try_get::<i32, _>("error_code")?)
            .map_err(|_| Error::Integrity("invalid stored rejection"))?;
        if !matches!(
            code,
            ErrorCode::PermissionDenied | ErrorCode::InvalidArgument | ErrorCode::ResourceLimit
        ) {
            return Err(Error::Integrity("invalid stored rejection"));
        }
        Ok(Some(rejected(code)))
    }

    async fn record_rejection(
        tx: &mut Transaction<'_, MySql>,
        key: &SendKey,
        digest: [u8; 32],
        code: ErrorCode,
    ) -> Result<Decision> {
        bind_key("INSERT INTO rejections \
                  (tenant_id, user_id, device_id, conversation_id, client_msg_id, digest, error_code) \
                  VALUES (?, ?, ?, ?, ?, ?, ?)", key)
            .bind(digest.as_slice())
            .bind(code as i32)
            .execute(&mut **tx)
            .await?;
        Ok(Decision::outcome(rejected(code)))
    }

    async fn append(
        tx: &mut Transaction<'_, MySql>,
        key: &SendKey,
        text: &str,
        now: i64,
        members: Vec<(u64, u64)>,
    ) -> Result<Decision> {
        let row = sqlx::query(
            "SELECT history_id, next_sequence FROM conversations \
                               WHERE tenant_id = ? AND conversation_id = ?",
        )
        .bind(key.owner.tenant_id)
        .bind(key.conversation)
        .fetch_one(&mut **tx)
        .await?;
        let conversation = conversation_info(&row)?;
        let sequence = conversation.next_sequence;
        let next = sequence.checked_add(1).ok_or(Error::SequenceExhausted)?;
        let message_id = loop {
            let id = generate_id128();
            let existing: Option<i32> = sqlx::query_scalar("SELECT 1 FROM messages \
                                                           WHERE tenant_id = ? AND conversation_id = ? AND message_id = ?")
                .bind(key.owner.tenant_id)
                .bind(key.conversation)
                .bind(id.as_slice())
                .fetch_optional(&mut **tx)
                .await?;
            if existing.is_none() {
                break id;
            }
        };
        sqlx::query(
            "UPDATE conversations SET next_sequence = ? \
                     WHERE tenant_id = ? AND conversation_id = ?",
        )
        .bind(next)
        .bind(key.owner.tenant_id)
        .bind(key.conversation)
        .execute(&mut **tx)
        .await?;
        bind_key("INSERT INTO messages \
                  (tenant_id, user_id, device_id, conversation_id, client_msg_id, message_id, `sequence`, `text`, accepted_at) \
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", key)
            .bind(message_id.as_slice())
            .bind(sequence)
            .bind(text.as_bytes())
            .bind(now)
            .execute(&mut **tx)
            .await?;
        let saved = StoredText {
            receipt: receipt(
                key.conversation,
                message_id,
                conversation.history_id,
                sequence,
                now,
            ),
            user_id: key.owner.user_id,
            text: text.to_owned(),
        };
        Ok(Decision {
            delivery: Some(Delivery {
                members,
                message: saved.view(),
                stream: Stream {
                    scope: Scope::Conversation(key.conversation),
                    epoch: conversation.history_id,
                    upper: sequence,
                    access_version: 0,
                    floor: 1,
                },
            }),
            outcome: SendOutcome {
                value: Some(send_outcome::Value::Committed(saved.receipt)),
            },
        })
    }

    pub async fn begin_read(&self) -> Result<Transaction<'_, MySql>> {
        Ok(self.pool.begin_with("BEGIN OPTIMISTIC").await?)
    }

    pub async fn stream(&self, identity: Identity, scope: Scope) -> Result<Stream> {
        let mut tx = self.begin_read().await?;
        let result = self.read_stream(&mut tx, identity, scope).await;
        finish(tx, result).await
    }

    pub async fn read_stream(
        &self,
        db: &mut MySqlConnection,
        identity: Identity,
        scope: Scope,
    ) -> Result<Stream> {
        match scope {
            Scope::Account => {
                let upper = account_upper(db, identity.tenant_id, identity.user_id).await?;
                Ok(Stream {
                    scope,
                    epoch: self.id,
                    upper,
                    access_version: 0,
                    floor: 1,
                })
            }
            Scope::Conversation(id) => {
                let row = sqlx::query(
                    "SELECT c.history_id, c.next_sequence, e.access_version,
                            CAST(COALESCE(r.floor_sequence, 1) AS UNSIGNED) AS floor
                     FROM members m
                     JOIN account_events e USING (tenant_id, user_id, account_sequence)
                     JOIN conversations c ON c.tenant_id = m.tenant_id AND c.conversation_id = m.conversation_id
                     LEFT JOIN conversation_retention r ON r.tenant_id = c.tenant_id AND r.conversation_id = c.conversation_id
                     WHERE m.tenant_id = ? AND m.user_id = ? AND m.conversation_id = ?",
                )
                .bind(identity.tenant_id)
                .bind(identity.user_id)
                .bind(id)
                .fetch_optional(db)
                .await?
                .ok_or(Error::PermissionDenied)?;
                let conv = conversation_info(&row)?;
                let floor: u64 = row.try_get("floor")?;
                if floor == 0 || floor > conv.next_sequence {
                    return Err(Error::Integrity("invalid retention floor"));
                }
                Ok(Stream {
                    scope,
                    epoch: conv.history_id,
                    upper: conv.next_sequence - 1,
                    access_version: row.try_get("access_version")?,
                    floor,
                })
            }
        }
    }

    pub async fn changes(
        db: &mut MySqlConnection,
        identity: Identity,
        scope: Scope,
        after: u64,
        through: u64,
        limit: usize,
    ) -> Result<Vec<iam_protocol::pb::Change>> {
        let expected = through
            .checked_sub(after)
            .ok_or(Error::Integrity("invalid stream range"))?
            .min(limit as u64);
        if expected == 0 {
            return Ok(Vec::new());
        }
        let changes = match scope {
            Scope::Account => {
                let rows = sqlx::query(
                    "SELECT account_sequence, conversation_id, history_id, access_version FROM account_events
                     WHERE tenant_id = ? AND user_id = ? AND account_sequence > ? AND account_sequence <= ?
                     ORDER BY account_sequence LIMIT ?",
                )
                .bind(identity.tenant_id).bind(identity.user_id).bind(after).bind(through).bind(expected)
                .fetch_all(db).await?;
                rows.iter()
                    .map(|row| {
                        let payload = if row.try_get::<Option<u64>, _>("access_version")?.is_some()
                        {
                            change::Payload::ConversationUpsert(conversation_state(row)?)
                        } else {
                            change::Payload::ConversationRemove(row.try_get("conversation_id")?)
                        };
                        Ok(iam_protocol::pb::Change {
                            event_seq: Some(row.try_get("account_sequence")?),
                            payload: Some(payload),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?
            }
            Scope::Conversation(id) => {
                let query = format!("{MESSAGES} WHERE m.tenant_id = ? AND m.conversation_id = ?
                                    AND m.`sequence` > ? AND m.`sequence` <= ? ORDER BY m.`sequence` LIMIT ?");
                let rows = sqlx::query(&query)
                    .bind(identity.tenant_id)
                    .bind(id)
                    .bind(after)
                    .bind(through)
                    .bind(expected)
                    .fetch_all(db)
                    .await?;
                rows.iter()
                    .map(|row| {
                        Ok(iam_protocol::pb::Change {
                            event_seq: Some(row.try_get("sequence")?),
                            payload: Some(change::Payload::MessageUpsert(
                                decode_message(row)?.view(),
                            )),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?
            }
        };
        if changes.len() as u64 != expected
            || changes
                .iter()
                .zip(after + 1..=through)
                .any(|(change, sequence)| change.event_seq != Some(sequence))
        {
            return Err(Error::Integrity("gap in committed stream"));
        }
        Ok(changes)
    }

    pub async fn account_snapshot(
        db: &mut MySqlConnection,
        identity: Identity,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<ConversationState>> {
        let rows = sqlx::query(
            "SELECT m.conversation_id, c.history_id, e.access_version FROM members m
             JOIN account_events e USING (tenant_id, user_id, account_sequence)
             JOIN conversations c ON c.tenant_id = m.tenant_id AND c.conversation_id = m.conversation_id
             WHERE m.tenant_id = ? AND m.user_id = ? ORDER BY m.conversation_id LIMIT ? OFFSET ?",
        )
        .bind(identity.tenant_id).bind(identity.user_id).bind(limit as u64).bind(offset)
        .fetch_all(db).await?;
        rows.iter().map(conversation_state).collect()
    }

    pub async fn history_messages(
        db: &mut MySqlConnection,
        identity: Identity,
        stream: &Stream,
        before: u64,
        upper: u64,
        limit: usize,
    ) -> Result<Vec<MessageView>> {
        let query = format!(
            "{MESSAGES} WHERE m.tenant_id = ? AND m.conversation_id = ?
                            AND m.`sequence` >= ? AND m.`sequence` <= ? AND m.`sequence` < ?
                            ORDER BY m.`sequence` DESC LIMIT ?"
        );
        let rows = sqlx::query(&query)
            .bind(identity.tenant_id)
            .bind(stream.scope.conversation_id())
            .bind(stream.floor)
            .bind(upper)
            .bind(before)
            .bind(limit as u64)
            .fetch_all(db)
            .await?;
        rows.iter()
            .map(|row| Ok(decode_message(row)?.view()))
            .collect()
    }

    pub async fn page(
        &self,
        identity: Identity,
        conversation_id: u64,
        before: Option<u64>,
        limit: u32,
    ) -> Result<TextPage> {
        let mut tx = self.begin_read().await?;
        let stream = self
            .read_stream(&mut tx, identity, Scope::Conversation(conversation_id))
            .await?;
        let mut messages = Self::history_messages(
            &mut tx,
            identity,
            &stream,
            before.unwrap_or(u64::MAX),
            stream.upper,
            limit as usize + 1,
        )
        .await?;
        tx.commit().await?;
        let more = messages.len() > limit as usize;
        messages.truncate(limit as usize);
        let next_before = if more {
            messages
                .last()
                .and_then(|m| m.position.as_ref())
                .and_then(|p| p.create_seq)
        } else {
            None
        };
        Ok(TextPage {
            history_id: stream.epoch,
            messages,
            next_before,
        })
    }
}

fn conversation_state(row: &MySqlRow) -> Result<ConversationState> {
    Ok(ConversationState {
        conversation_id: Some(row.try_get("conversation_id")?),
        kind: Some(ConversationKind::Direct as i32),
        history_id: Some(id_column(row, "history_id")?.to_vec().into()),
        access_version: Some(row.try_get("access_version")?),
    })
}

async fn account_upper(db: &mut MySqlConnection, tenant: u64, user: u64) -> Result<u64> {
    Ok(sqlx::query_scalar(
        "SELECT CAST(COALESCE(MAX(account_sequence), 0) AS UNSIGNED)
                          FROM account_events WHERE tenant_id = ? AND user_id = ?",
    )
    .bind(tenant)
    .bind(user)
    .fetch_one(db)
    .await?)
}

pub(crate) fn local_options(url: &str) -> Result<MySqlConnectOptions> {
    let options = MySqlConnectOptions::from_str(url)
        .map_err(|_| Error::Configuration("expected a mysql:// local TiDB URL"))?;
    if !matches!(
        options.get_host(),
        "127.0.0.1" | "localhost" | "::1" | "[::1]"
    ) || options.get_socket().is_some()
    {
        return Err(Error::Configuration(
            "only loopback TCP endpoints are supported",
        ));
    }
    if options.get_database().is_none_or(str::is_empty) {
        return Err(Error::Configuration("select a dedicated local database"));
    }
    Ok(options.disable_statement_logging())
}

async fn connect_pool(url: &str) -> Result<MySqlPool> {
    let options = local_options(url)?;
    let pool = MySqlPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(5))
        .after_connect(|connection, _| {
            Box::pin(async move {
                // Fail against MySQL: compatibility is not evidence of TiDB validation.
                let _: String = sqlx::query_scalar("SELECT tidb_version()")
                    .fetch_one(&mut *connection)
                    .await?;
                sqlx::query("SET SESSION tidb_txn_mode = 'pessimistic'")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await?;
    Ok(pool)
}

async fn reject_legacy_names(pool: &MySqlPool) -> Result<()> {
    let old_table: Option<String> = sqlx::query_scalar(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() \
         AND TABLE_NAME IN ('protocol_store', 'protocol_conversations', 'protocol_messages', \
                            'protocol_rejections', 'protocol_members') LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    if old_table.is_some() {
        return Err(Error::Configuration(
            "database uses legacy protocol table names",
        ));
    }
    Ok(())
}

async fn require_empty_tables(pool: &MySqlPool) -> Result<()> {
    let existing: Vec<String> = sqlx::query_scalar(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()",
    )
    .fetch_all(pool)
    .await?;
    for table in TABLES {
        if existing.iter().any(|name| name.eq_ignore_ascii_case(table)) {
            let row: Option<i32> = sqlx::query_scalar(&format!("SELECT 1 FROM {table} LIMIT 1"))
                .fetch_optional(pool)
                .await?;
            if row.is_some() {
                return Err(Error::Configuration(
                    "initialization requires empty storage tables",
                ));
            }
        }
    }
    Ok(())
}

async fn check_schema(pool: &MySqlPool, legacy_only: bool) -> Result<()> {
    for query in [
        "SELECT singleton, store_id, token_key, max_observed_now_ms, closed_through_client_ms FROM store_control LIMIT 0",
        "SELECT tenant_id, conversation_id, history_id, next_sequence FROM conversations LIMIT 0",
        "SELECT tenant_id, conversation_id, message_id, user_id, device_id, client_msg_id, `sequence`, `text`, accepted_at FROM messages LIMIT 0",
        "SELECT tenant_id, user_id, device_id, conversation_id, client_msg_id, digest, error_code FROM rejections LIMIT 0",
        "SELECT tenant_id, user_id, conversation_id, account_sequence FROM members LIMIT 0",
    ] {
        sqlx::query(query).execute(pool).await?;
    }
    if !legacy_only {
        for query in [
            "SELECT tenant_id, user_id, account_sequence, conversation_id, history_id, access_version FROM account_events LIMIT 0",
            "SELECT tenant_id, conversation_id, floor_sequence FROM conversation_retention LIMIT 0",
        ] {
            sqlx::query(query).execute(pool).await?;
        }
    }
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT table_name, index_name, column_name FROM information_schema.statistics \
         WHERE table_schema = DATABASE() AND table_name IN ('store_control', 'conversations', 'messages', 'rejections', 'members', 'account_events', 'conversation_retention') \
         AND non_unique = 0 ORDER BY table_name, index_name, seq_in_index",
    )
    .fetch_all(pool)
    .await?;
    let mut keys = BTreeMap::<_, Vec<_>>::new();
    for (table, index, column) in rows {
        if legacy_only && matches!(table.as_str(), "account_events" | "conversation_retention") {
            continue;
        }
        keys.entry((table, index)).or_default().push(column);
    }
    let send_key = &[
        "tenant_id",
        "user_id",
        "device_id",
        "conversation_id",
        "client_msg_id",
    ];
    let expected: [(&str, &str, &[&str]); 10] = [
        ("store_control", "PRIMARY", &["singleton"]),
        (
            "conversations",
            "PRIMARY",
            &["tenant_id", "conversation_id"],
        ),
        (
            "messages",
            "PRIMARY",
            &["tenant_id", "conversation_id", "message_id"],
        ),
        (
            "messages",
            "message_order",
            &["tenant_id", "conversation_id", "sequence"],
        ),
        ("messages", "successful_send", send_key),
        ("rejections", "PRIMARY", send_key),
        (
            "members",
            "PRIMARY",
            &["tenant_id", "user_id", "account_sequence"],
        ),
        (
            "members",
            "membership",
            &["tenant_id", "conversation_id", "user_id"],
        ),
        (
            "account_events",
            "PRIMARY",
            &["tenant_id", "user_id", "account_sequence"],
        ),
        (
            "conversation_retention",
            "PRIMARY",
            &["tenant_id", "conversation_id"],
        ),
    ];
    let expected = if legacy_only {
        &expected[..8]
    } else {
        &expected[..]
    };
    if keys.len() != expected.len()
        || expected.iter().any(|(table, index, columns)| {
            keys.get(&(table.to_string(), index.to_string()))
                .is_none_or(|key| key != columns)
        })
    {
        return Err(Error::Integrity(
            "schema does not match the required unique keys",
        ));
    }
    Ok(())
}

async fn finish<T>(transaction: Transaction<'_, MySql>, result: Result<T>) -> Result<T> {
    match result {
        Ok(value) => {
            transaction.commit().await.map_err(Error::CommitUnknown)?;
            Ok(value)
        }
        Err(error) => {
            transaction.rollback().await.map_err(Error::CommitUnknown)?;
            Err(error)
        }
    }
}

fn id_column(row: &MySqlRow, name: &str) -> Result<Id128> {
    let bytes: Vec<u8> = row.try_get(name)?;
    validate_id128(&bytes).map_err(|_| Error::Integrity("invalid UUIDv7 in storage"))
}

fn conversation_info(row: &MySqlRow) -> Result<ConversationInfo> {
    let next_sequence = row.try_get("next_sequence")?;
    if next_sequence == 0 {
        return Err(Error::Integrity("zero next_sequence"));
    }
    Ok(ConversationInfo {
        history_id: id_column(row, "history_id")?,
        next_sequence,
    })
}

fn decode_message(row: &MySqlRow) -> Result<StoredText> {
    let sequence: u64 = row.try_get("sequence")?;
    let accepted_at: i64 = row.try_get("accepted_at")?;
    let conversation_id: u64 = row.try_get("conversation_id")?;
    let user_id: u64 = row.try_get("user_id")?;
    if sequence == 0 || accepted_at < 0 || conversation_id == 0 || user_id == 0 {
        return Err(Error::Integrity("invalid message position or identity"));
    }
    let bytes: Vec<u8> = row.try_get("text")?;
    let text = String::from_utf8(bytes).map_err(|_| Error::Integrity("text is not UTF-8"))?;
    validate_message_content(&text_content(&text))
        .map_err(|_| Error::Integrity("stored text violates protocol limits"))?;
    Ok(StoredText {
        receipt: receipt(
            conversation_id,
            id_column(row, "message_id")?,
            id_column(row, "history_id")?,
            sequence,
            accepted_at,
        ),
        user_id,
        text,
    })
}

fn closed_through(now: i64) -> i64 {
    (now - ACCEPT_WINDOW_MS).max(-1)
}

fn control_clock(row: &sqlx::mysql::MySqlRow) -> Result<(i64, i64)> {
    let previous: i64 = row.try_get("max_observed_now_ms")?;
    let watermark: i64 = row.try_get("closed_through_client_ms")?;
    if previous < 0 || watermark != closed_through(previous) {
        return Err(Error::Integrity(
            "invalid persisted clock or close watermark",
        ));
    }
    Ok((previous, watermark))
}

fn check_store_id(row: &sqlx::mysql::MySqlRow, expected: Id128) -> Result<()> {
    if id_column(row, "store_id")? != expected {
        return Err(Error::Configuration("protocol store ID does not match"));
    }
    Ok(())
}

fn bind_key<'q>(
    query: &'q str,
    key: &'q SendKey,
) -> sqlx::query::Query<'q, MySql, sqlx::mysql::MySqlArguments> {
    sqlx::query(query)
        .bind(key.owner.tenant_id)
        .bind(key.owner.user_id)
        .bind(key.owner.device_id.as_slice())
        .bind(key.conversation)
        .bind(key.client_id.as_slice())
}

async fn initialize(
    tx: &mut Transaction<'_, MySql>,
    id: &Id128,
    seeds: &MemoryStore,
) -> Result<()> {
    let mut token_key = [0u8; 32];
    getrandom::fill(&mut token_key)
        .map_err(|_| Error::Configuration("cannot generate token key"))?;
    let now: i64 = sqlx::query_scalar(CLOCK_SQL).fetch_one(&mut **tx).await?;
    if now < 0 {
        return Err(Error::Integrity("invalid database clock"));
    }
    sqlx::query(
        "INSERT INTO store_control \
                 (singleton, store_id, token_key, max_observed_now_ms, closed_through_client_ms) \
                 VALUES (1, ?, ?, ?, ?)",
    )
    .bind(id.as_slice())
    .bind(token_key.as_slice())
    .bind(now)
    .bind(closed_through(now))
    .execute(&mut **tx)
    .await?;
    for table in TABLES.into_iter().filter(|table| *table != "store_control") {
        let existing: Option<i32> = sqlx::query_scalar(&format!("SELECT 1 FROM {table} LIMIT 1"))
            .fetch_optional(&mut **tx)
            .await?;
        if existing.is_some() {
            return Err(Error::Configuration(
                "protocol bootstrap requires empty tables",
            ));
        }
    }
    let mut conversations = seeds.conversations.values().collect::<Vec<_>>();
    conversations.sort_by_key(|c| (c.tenant_id, c.conversation_id));
    let mut positions = HashMap::<(u64, u64), u64>::new();
    for conv in conversations {
        if conv.tenant_id == 0
            || conv.conversation_id == 0
            || conv.kind != ConversationKind::Direct
            || conv.members.len() != 2
            || conv.members.contains(&0)
            || validate_id128(&conv.history_id).is_err()
        {
            return Err(Error::Configuration(
                "expected nonzero direct conversation seeds",
            ));
        }
        sqlx::query(
            "INSERT INTO conversations (tenant_id, conversation_id, history_id, next_sequence) \
                     VALUES (?, ?, ?, 1)",
        )
        .bind(conv.tenant_id)
        .bind(conv.conversation_id)
        .bind(conv.history_id.as_slice())
        .execute(&mut **tx)
        .await?;
        for &user in &conv.members {
            let position = positions.entry((conv.tenant_id, user)).or_default();
            *position += 1;
            sqlx::query(
                "INSERT INTO members (tenant_id, user_id, conversation_id, account_sequence) \
                         VALUES (?, ?, ?, ?)",
            )
            .bind(conv.tenant_id)
            .bind(user)
            .bind(conv.conversation_id)
            .bind(*position)
            .execute(&mut **tx)
            .await?;
            write_account_event(
                tx,
                conv.tenant_id,
                user,
                *position,
                conv.conversation_id,
                conv.history_id,
                Some(INITIAL_ACCESS),
            )
            .await?;
        }
    }
    Ok(())
}
