use crate::store::{conversation_info, decode_message, finish, id_column, TidbStore, CLOCK_SQL};
use crate::{receipt, Error, Identity, Result, StoredText};
use iam_core::{MemoryStore, TokenSigner, ACCEPT_WINDOW_MS, ALLOW_SKEW_MS};
use iam_protocol::pb::{
    message_content, send_outcome, snapshot_item, sync_target, ConversationKind, ConversationState,
    Empty, ErrorCode, MessageContent, MessageView, RetryAction, SendOutcome, SendUnresolved,
    StreamContext, SyncTarget,
};
use iam_protocol::{
    error_info, generate_id128, validate_id128, validate_message_content, Id128, ProtocolErrorKind,
};
use sqlx::{MySql, MySqlPool, Row, Transaction};
use std::collections::HashMap;

const MESSAGES: &str = "SELECT m.conversation_id, m.message_id, m.user_id, m.`sequence`, \
                        m.`text`, m.accepted_at, c.history_id \
                        FROM protocol_messages m JOIN protocol_conversations c \
                        ON c.tenant_id = m.tenant_id AND c.conversation_id = m.conversation_id";

const CONTROL: &str = "SELECT store_id, token_key, max_observed_now_ms, closed_through_client_ms \
                       FROM protocol_store WHERE singleton = 1 FOR UPDATE";

const ACCESS_VERSION: u64 = 1;

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

pub(crate) struct ProtocolStore {
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

    pub fn access_version(self) -> u64 {
        match self {
            Self::Account => 0,
            Self::Conversation(_) => ACCESS_VERSION,
        }
    }
}

pub(crate) struct Stream {
    pub scope: Scope,
    pub epoch: Id128,
    pub upper: u64,
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
            access_version: (self.scope != Scope::Account).then_some(self.scope.access_version()),
        }
    }
}

pub(crate) struct Entry {
    pub sequence: u64,
    pub item: snapshot_item::Value,
}

pub(crate) struct SendKey {
    pub owner: Identity,
    pub conversation: u64,
    pub client_id: Id128,
}

pub(crate) struct Delivery {
    pub members: Vec<u64>,
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

impl ProtocolStore {
    pub async fn open(url: &str, id: Id128, seeds: Option<&MemoryStore>) -> Result<Self> {
        validate_id128(&id).map_err(|_| Error::Configuration("invalid protocol store ID"))?;
        let pool = TidbStore::connect(url).await?.pool;
        if let Some(seeds) = seeds {
            for statement in include_str!("../protocol-schema.sql").split(';') {
                if !statement.trim().is_empty() {
                    sqlx::query(statement).execute(&pool).await?;
                }
            }
            let mut transaction = pool.begin().await?;
            let result = initialize(&mut transaction, &id, seeds).await;
            finish(transaction, result).await?;
        }

        let mut transaction = pool.begin().await?;
        let row = sqlx::query(CONTROL).fetch_one(&mut *transaction).await?;
        check_store_id(&row, id)?;
        for query in [
            "SELECT tenant_id, conversation_id, history_id, next_sequence FROM protocol_conversations LIMIT 0",
            "SELECT tenant_id, conversation_id, message_id, user_id, device_id, client_msg_id, `sequence`, `text`, accepted_at FROM protocol_messages LIMIT 0",
            "SELECT tenant_id, user_id, device_id, conversation_id, client_msg_id, digest, error_code FROM protocol_rejections LIMIT 0",
            "SELECT tenant_id, user_id, conversation_id, account_sequence FROM protocol_members LIMIT 0",
        ] {
            sqlx::query(query).execute(&mut *transaction).await?;
        }
        let key: Vec<u8> = row.try_get("token_key")?;
        let key = key
            .try_into()
            .map_err(|_| Error::Integrity("invalid token key"))?;
        control_clock(&row)?;
        finish(transaction, Ok(())).await?;
        Ok(Self {
            pool,
            id,
            tokens: TokenSigner::new(key),
        })
    }

    pub async fn now(&self) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        let result = self
            .observe_clock(&mut tx)
            .await
            .map(|clock| clock.effective_now());
        finish(tx, result).await
    }

    pub async fn decide(
        &self,
        key: &SendKey,
        content: Option<&MessageContent>,
    ) -> Result<Decision> {
        let mut tx = self.pool.begin().await?;
        let result = self.decide_in(&mut tx, key, content).await;
        finish(tx, result).await
    }

    async fn decide_in(
        &self,
        tx: &mut Transaction<'_, MySql>,
        key: &SendKey,
        content: Option<&MessageContent>,
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
        let members: Vec<u64> = sqlx::query_scalar(
            "SELECT user_id FROM protocol_members WHERE tenant_id = ? AND conversation_id = ?",
        )
        .bind(key.owner.tenant_id)
        .bind(key.conversation)
        .fetch_all(&mut **tx)
        .await?;
        if !members.contains(&key.owner.user_id) {
            return Self::record_rejection(tx, key, digest, ErrorCode::PermissionDenied).await;
        }
        let Some(message_content::Value::Text(text)) = content.value.as_ref() else {
            return Err(Error::Integrity("accepted content is not text"));
        };
        Self::append(tx, key, text.text.as_deref().unwrap(), clock.now, members).await
    }

    async fn observe_clock(&self, tx: &mut Transaction<'_, MySql>) -> Result<DecisionClock> {
        let row = sqlx::query(CONTROL).fetch_one(&mut **tx).await?;
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
                "UPDATE protocol_store SET max_observed_now_ms = ?, \
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
        let row = bind_key("SELECT digest, error_code FROM protocol_rejections WHERE tenant_id = ? \
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
        bind_key("INSERT INTO protocol_rejections \
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
        members: Vec<u64>,
    ) -> Result<Decision> {
        let row = sqlx::query(
            "SELECT history_id, next_sequence FROM protocol_conversations \
                               WHERE tenant_id = ? AND conversation_id = ?",
        )
        .bind(key.owner.tenant_id)
        .bind(key.conversation)
        .fetch_one(&mut **tx)
        .await?;
        let conversation = conversation_info(&row, false)?;
        let sequence = conversation.next_sequence;
        let next = sequence.checked_add(1).ok_or(Error::SequenceExhausted)?;
        let message_id = loop {
            let id = generate_id128();
            let existing: Option<i32> = sqlx::query_scalar("SELECT 1 FROM protocol_messages \
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
            "UPDATE protocol_conversations SET next_sequence = ? \
                     WHERE tenant_id = ? AND conversation_id = ?",
        )
        .bind(next)
        .bind(key.owner.tenant_id)
        .bind(key.conversation)
        .execute(&mut **tx)
        .await?;
        bind_key("INSERT INTO protocol_messages \
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
                },
            }),
            outcome: SendOutcome {
                value: Some(send_outcome::Value::Committed(saved.receipt)),
            },
        })
    }

    pub async fn stream(&self, identity: Identity, scope: Scope) -> Result<Stream> {
        match scope {
            Scope::Account => {
                let upper: u64 = sqlx::query_scalar("SELECT CAST(COALESCE(MAX(account_sequence), 0) AS UNSIGNED) \
                                                     FROM protocol_members WHERE tenant_id = ? AND user_id = ?")
                    .bind(identity.tenant_id)
                    .bind(identity.user_id)
                    .fetch_one(&self.pool)
                    .await?;
                Ok(Stream {
                    scope,
                    epoch: self.id,
                    upper,
                })
            }
            Scope::Conversation(id) => {
                let row = sqlx::query("SELECT c.history_id, c.next_sequence FROM protocol_conversations c \
                                       JOIN protocol_members m ON m.tenant_id = c.tenant_id AND m.conversation_id = c.conversation_id \
                                       WHERE m.tenant_id = ? AND m.user_id = ? AND m.conversation_id = ?")
                    .bind(identity.tenant_id)
                    .bind(identity.user_id)
                    .bind(id)
                    .fetch_optional(&self.pool)
                    .await?
                    .ok_or(Error::PermissionDenied)?;
                let conv = conversation_info(&row, false)?;
                Ok(Stream {
                    scope,
                    epoch: conv.history_id,
                    upper: conv.next_sequence - 1,
                })
            }
        }
    }

    pub async fn entries(
        &self,
        identity: Identity,
        scope: Scope,
        after: u64,
        through: u64,
        limit: usize,
    ) -> Result<Vec<Entry>> {
        let expected = through
            .checked_sub(after)
            .ok_or(Error::Integrity("invalid stream range"))?
            .min(limit as u64);
        if expected == 0 {
            return Ok(Vec::new());
        }
        let entries = match scope {
            Scope::Account => {
                let rows = sqlx::query(
                    "SELECT m.account_sequence, c.conversation_id, c.history_id FROM protocol_members m \
                     JOIN protocol_conversations c ON c.tenant_id = m.tenant_id AND c.conversation_id = m.conversation_id \
                     WHERE m.tenant_id = ? AND m.user_id = ? AND m.account_sequence > ? AND m.account_sequence <= ? \
                     ORDER BY m.account_sequence LIMIT ?",
                )
                .bind(identity.tenant_id)
                .bind(identity.user_id)
                .bind(after)
                .bind(through)
                .bind(expected)
                .fetch_all(&self.pool)
                .await?;
                rows.iter()
                    .map(|row| {
                        Ok(Entry {
                            sequence: row.try_get("account_sequence")?,
                            item: snapshot_item::Value::Conversation(ConversationState {
                                conversation_id: Some(row.try_get("conversation_id")?),
                                kind: Some(ConversationKind::Direct as i32),
                                history_id: Some(id_column(row, "history_id")?.to_vec().into()),
                                access_version: Some(ACCESS_VERSION),
                            }),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?
            }
            Scope::Conversation(id) => {
                let query = format!("{MESSAGES} WHERE m.tenant_id = ? AND m.conversation_id = ? \
                                    AND m.`sequence` > ? AND m.`sequence` <= ? ORDER BY m.`sequence` LIMIT ?");
                let rows = sqlx::query(&query)
                    .bind(identity.tenant_id)
                    .bind(id)
                    .bind(after)
                    .bind(through)
                    .bind(expected)
                    .fetch_all(&self.pool)
                    .await?;
                rows.iter()
                    .map(|row| {
                        Ok(Entry {
                            sequence: row.try_get("sequence")?,
                            item: snapshot_item::Value::Message(decode_message(row)?.view()),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?
            }
        };
        if entries.len() as u64 != expected
            || entries
                .iter()
                .zip(after + 1..=through)
                .any(|(entry, sequence)| entry.sequence != sequence)
        {
            return Err(Error::Integrity("gap in committed stream"));
        }
        Ok(entries)
    }
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
        "INSERT INTO protocol_store \
                 (singleton, store_id, token_key, max_observed_now_ms, closed_through_client_ms) \
                 VALUES (1, ?, ?, ?, ?)",
    )
    .bind(id.as_slice())
    .bind(token_key.as_slice())
    .bind(now)
    .bind(closed_through(now))
    .execute(&mut **tx)
    .await?;
    for table in [
        "protocol_conversations",
        "protocol_messages",
        "protocol_rejections",
        "protocol_members",
    ] {
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
        sqlx::query("INSERT INTO protocol_conversations (tenant_id, conversation_id, history_id, next_sequence) \
                     VALUES (?, ?, ?, 1)")
            .bind(conv.tenant_id)
            .bind(conv.conversation_id)
            .bind(conv.history_id.as_slice())
            .execute(&mut **tx)
            .await?;
        for &user in &conv.members {
            let position = positions.entry((conv.tenant_id, user)).or_default();
            *position += 1;
            sqlx::query("INSERT INTO protocol_members (tenant_id, user_id, conversation_id, account_sequence) \
                         VALUES (?, ?, ?, ?)")
                .bind(conv.tenant_id)
                .bind(user)
                .bind(conv.conversation_id)
                .bind(*position)
                .execute(&mut **tx)
                .await?;
        }
    }
    Ok(())
}
