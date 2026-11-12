use crate::{
    receipt, same_content, text_content, windows, ConversationInfo, Error, Identity, Result,
    StoredText, TextPage,
};
use iam_protocol::{generate_id128, validate_id128, validate_message_content, Id128};
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlRow};
use sqlx::{ConnectOptions, MySql, MySqlPool, Row, Transaction};
use std::{collections::BTreeMap, str::FromStr, time::Duration};

pub(crate) const CLOCK_SQL: &str =
    "SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED)";
const MESSAGE_SELECT: &str = "SELECT m.conversation_id, m.message_id, m.user_id, \
    m.`sequence`, m.`text`, m.accepted_at, c.history_id \
    FROM messages m JOIN conversations c \
    ON c.tenant_id = m.tenant_id AND c.conversation_id = m.conversation_id";

#[derive(Clone)]
pub(crate) struct TidbStore {
    pub(crate) pool: MySqlPool,
}

pub(crate) struct Append<'a> {
    pub identity: Identity,
    pub conversation_id: u64,
    pub client_msg_id: Id128,
    pub text: &'a str,
    pub message_id: Id128,
    pub max_observed_now: &'a mut i64,
}

pub(crate) struct Appended {
    pub message: StoredText,
    pub replayed: bool,
    pub observed_now: i64,
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

impl TidbStore {
    pub async fn connect(url: &str) -> Result<Self> {
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
        Ok(Self { pool })
    }

    pub async fn install_schema(&self) -> Result<()> {
        // Deliberately no migration ledger: the approved schema has exactly two tables.
        for statement in include_str!("../schema.sql").split(';') {
            if !statement.trim().is_empty() {
                sqlx::query(statement).execute(&self.pool).await?;
            }
        }
        self.check_schema().await
    }

    /// Read-only startup check. A missing column or uniqueness constraint must not
    /// silently turn an existing database into a different persistence contract.
    pub async fn check_schema(&self) -> Result<()> {
        for query in [
            "SELECT tenant_id, conversation_id, history_id, next_sequence FROM conversations LIMIT 0",
            "SELECT tenant_id, conversation_id, message_id, user_id, device_id, client_msg_id, \
             `sequence`, `text`, accepted_at FROM messages LIMIT 0",
        ] {
            sqlx::query(query).execute(&self.pool).await?;
        }
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT table_name, index_name, column_name FROM information_schema.statistics \
             WHERE table_schema = DATABASE() AND table_name IN ('conversations', 'messages') \
             AND non_unique = 0 ORDER BY table_name, index_name, seq_in_index",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut indexes: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
        for (table, index, column) in rows {
            indexes.entry((table, index)).or_default().push(column);
        }
        let required: [(&str, &str, &[&str]); 4] = [
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
            (
                "messages",
                "successful_send",
                &[
                    "tenant_id",
                    "user_id",
                    "device_id",
                    "conversation_id",
                    "client_msg_id",
                ],
            ),
        ];
        if indexes.len() != required.len()
            || required.iter().any(|(table, index, columns)| {
                indexes
                    .get(&(table.to_string(), index.to_string()))
                    .is_none_or(|actual| {
                        !actual
                            .iter()
                            .map(String::as_str)
                            .eq(columns.iter().copied())
                    })
            })
        {
            return Err(Error::Integrity(
                "schema does not match the required tenant-scoped unique keys",
            ));
        }
        Ok(())
    }

    pub async fn is_empty(&self) -> Result<bool> {
        let used: Option<u64> = sqlx::query_scalar(
            "SELECT tenant_id FROM conversations UNION ALL SELECT tenant_id FROM messages LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(used.is_none())
    }

    pub async fn now(&self) -> Result<i64> {
        let now = sqlx::query_scalar(CLOCK_SQL).fetch_one(&self.pool).await?;
        if now < 0 {
            return Err(Error::ResultUnavailable);
        }
        Ok(now)
    }

    pub async fn conversation(
        &self,
        identity: Identity,
        conversation_id: u64,
    ) -> Result<ConversationInfo> {
        let row = sqlx::query(
            "SELECT history_id, next_sequence FROM conversations WHERE tenant_id = ? AND conversation_id = ?",
        )
        .bind(identity.tenant_id)
        .bind(conversation_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(Error::MissingConversation)?;
        conversation_info(&row, false)
    }

    pub async fn initialize(
        &self,
        identity: Identity,
        conversation_id: u64,
    ) -> Result<ConversationInfo> {
        let mut transaction = self.pool.begin().await?;
        let result = Self::initialize_in(&mut transaction, identity, conversation_id).await;
        finish(transaction, result).await
    }

    async fn initialize_in(
        transaction: &mut Transaction<'_, MySql>,
        identity: Identity,
        conversation_id: u64,
    ) -> Result<ConversationInfo> {
        let candidate = generate_id128();
        let created = match sqlx::query(
            "INSERT INTO conversations (tenant_id, conversation_id, history_id, next_sequence) \
             VALUES (?, ?, ?, 1)",
        )
        .bind(identity.tenant_id)
        .bind(conversation_id)
        .bind(candidate.as_slice())
        .execute(&mut **transaction)
        .await
        {
            Ok(_) => true,
            Err(sqlx::Error::Database(error)) if error.is_unique_violation() => false,
            Err(error) => return Err(error.into()),
        };
        let row = sqlx::query(
            "SELECT history_id, next_sequence FROM conversations \
             WHERE tenant_id = ? AND conversation_id = ? FOR UPDATE",
        )
        .bind(identity.tenant_id)
        .bind(conversation_id)
        .fetch_one(&mut **transaction)
        .await?;
        if created {
            let orphan: Option<u64> = sqlx::query_scalar(
                "SELECT `sequence` FROM messages WHERE tenant_id = ? AND conversation_id = ? LIMIT 1",
            )
            .bind(identity.tenant_id)
            .bind(conversation_id)
            .fetch_optional(&mut **transaction)
            .await?;
            if orphan.is_some() {
                return Err(Error::Integrity(
                    "messages lack their original conversation history",
                ));
            }
        }
        conversation_info(&row, created)
    }

    pub async fn lookup(
        &self,
        identity: Identity,
        conversation_id: u64,
        client_msg_id: Id128,
    ) -> Result<Option<StoredText>> {
        let query = format!(
            "{MESSAGE_SELECT} WHERE m.tenant_id = ? AND m.user_id = ? AND m.device_id = ? \
             AND m.conversation_id = ? AND m.client_msg_id = ?"
        );
        let row = sqlx::query(&query)
            .bind(identity.tenant_id)
            .bind(identity.user_id)
            .bind(identity.device_id.as_slice())
            .bind(conversation_id)
            .bind(client_msg_id.as_slice())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(decode_message).transpose()
    }

    pub async fn append(&self, command: Append<'_>) -> Result<Appended> {
        let mut transaction = self.pool.begin().await?;
        let result = Self::append_in(&mut transaction, command).await;
        finish(transaction, result).await
    }

    async fn append_in(
        transaction: &mut Transaction<'_, MySql>,
        command: Append<'_>,
    ) -> Result<Appended> {
        let Append {
            identity,
            conversation_id,
            client_msg_id,
            text,
            message_id,
            max_observed_now,
        } = command;
        let row = sqlx::query(
            "SELECT history_id, next_sequence FROM conversations \
             WHERE tenant_id = ? AND conversation_id = ? FOR UPDATE",
        )
        .bind(identity.tenant_id)
        .bind(conversation_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(Error::MissingConversation)?;
        // Sample after waiting for the conversation lock, not at transaction start.
        let now: i64 = sqlx::query_scalar(CLOCK_SQL)
            .fetch_one(&mut **transaction)
            .await?;
        // Clock observations survive a rolled-back window/content decision too.
        let previous_now = *max_observed_now;
        *max_observed_now = previous_now.max(now);
        let (time, accept_until) = windows(&client_msg_id, now)?;
        let query = format!(
            "{MESSAGE_SELECT} WHERE m.tenant_id = ? AND m.user_id = ? AND m.device_id = ? \
             AND m.conversation_id = ? AND m.client_msg_id = ?"
        );
        if let Some(existing) = sqlx::query(&query)
            .bind(identity.tenant_id)
            .bind(identity.user_id)
            .bind(identity.device_id.as_slice())
            .bind(conversation_id)
            .bind(client_msg_id.as_slice())
            .fetch_optional(&mut **transaction)
            .await?
        {
            let message = decode_message(&existing)?;
            if !same_content(&message.text, text) {
                return Err(Error::ContentConflict);
            }
            return Ok(Appended {
                message,
                replayed: true,
                observed_now: now,
            });
        }
        if now < previous_now {
            return Err(Error::ResultUnavailable);
        }
        if time > now + 120_000 {
            return Err(Error::ClockSkew);
        }
        if now >= accept_until {
            return Err(Error::AcceptanceExpired);
        }
        validate_message_content(&text_content(text))
            .map_err(|_| Error::InvalidRequest("text violates protocol limits"))?;
        let history_id = id_column(&row, "history_id")?;
        let sequence: u64 = row.try_get("next_sequence")?;
        if sequence == 0 {
            return Err(Error::Integrity("zero next_sequence"));
        }
        let next = sequence.checked_add(1).ok_or(Error::SequenceExhausted)?;
        sqlx::query(
            "UPDATE conversations SET next_sequence = ? WHERE tenant_id = ? AND conversation_id = ?",
        )
        .bind(next)
        .bind(identity.tenant_id)
        .bind(conversation_id)
        .execute(&mut **transaction)
        .await?;
        sqlx::query(
            "INSERT INTO messages (tenant_id, conversation_id, message_id, user_id, device_id, \
             client_msg_id, `sequence`, `text`, accepted_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(identity.tenant_id)
        .bind(conversation_id)
        .bind(message_id.as_slice())
        .bind(identity.user_id)
        .bind(identity.device_id.as_slice())
        .bind(client_msg_id.as_slice())
        .bind(sequence)
        .bind(text.as_bytes())
        .bind(now)
        .execute(&mut **transaction)
        .await?;
        Ok(Appended {
            message: StoredText {
                receipt: receipt(conversation_id, message_id, history_id, sequence, now),
                user_id: identity.user_id,
                text: text.to_owned(),
            },
            replayed: false,
            observed_now: now,
        })
    }

    pub async fn page(
        &self,
        identity: Identity,
        conversation_id: u64,
        before: Option<u64>,
        limit: u32,
    ) -> Result<TextPage> {
        let row = sqlx::query(
            "SELECT history_id FROM conversations WHERE tenant_id = ? AND conversation_id = ?",
        )
        .bind(identity.tenant_id)
        .bind(conversation_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(Error::MissingConversation)?;
        let history_id = id_column(&row, "history_id")?;
        let query = format!(
            "{MESSAGE_SELECT} WHERE m.tenant_id = ? AND m.conversation_id = ? \
             AND m.`sequence` < ? ORDER BY m.`sequence` DESC LIMIT ?"
        );
        let rows = sqlx::query(&query)
            .bind(identity.tenant_id)
            .bind(conversation_id)
            .bind(before.unwrap_or(u64::MAX))
            .bind(u64::from(limit) + 1)
            .fetch_all(&self.pool)
            .await?;
        let mut messages: Vec<_> = rows.iter().map(decode_message).collect::<Result<_>>()?;
        if messages.iter().any(|message| {
            message
                .receipt
                .position
                .as_ref()
                .and_then(|p| p.history_id.as_deref())
                != Some(history_id.as_slice())
        }) {
            return Err(Error::Integrity("history changed during page read"));
        }
        let more = messages.len() > limit as usize;
        messages.truncate(limit as usize);
        let next_before = if more {
            messages
                .last()
                .and_then(|m| m.receipt.position.as_ref())
                .and_then(|p| p.create_seq)
        } else {
            None
        };
        Ok(TextPage {
            history_id,
            messages: messages.iter().map(StoredText::view).collect(),
            next_before,
        })
    }
}

pub(crate) async fn finish<T>(transaction: Transaction<'_, MySql>, result: Result<T>) -> Result<T> {
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

pub(crate) fn id_column(row: &MySqlRow, name: &str) -> Result<Id128> {
    let bytes: Vec<u8> = row.try_get(name)?;
    validate_id128(&bytes).map_err(|_| Error::Integrity("invalid UUIDv7 in storage"))
}

pub(crate) fn conversation_info(row: &MySqlRow, created: bool) -> Result<ConversationInfo> {
    let next_sequence = row.try_get("next_sequence")?;
    if next_sequence == 0 {
        return Err(Error::Integrity("zero next_sequence"));
    }
    Ok(ConversationInfo {
        history_id: id_column(row, "history_id")?,
        next_sequence,
        created,
    })
}

pub(crate) fn decode_message(row: &MySqlRow) -> Result<StoredText> {
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
