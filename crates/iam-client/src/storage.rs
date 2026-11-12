use crate::{Error, HistoryRange, Identity, Recovery, Result};
use iam_protocol::pb::{
    change, Change, ConversationState, MessageKey, MessageView, ResetReason, SendOutcome,
    SendRequest, StreamContext,
};
use iam_protocol::{validate_id128, Id128};
use prost::Message;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous};
use sqlx::{Connection, Sqlite, SqliteConnection, Transaction};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::path::Path;

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS identity (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        tenant BLOB NOT NULL, user BLOB NOT NULL, device BLOB NOT NULL
    );
    CREATE TABLE IF NOT EXISTS outbox (
        ordinal INTEGER PRIMARY KEY,
        conversation BLOB NOT NULL, client_id BLOB NOT NULL,
        request BLOB NOT NULL, attempted INTEGER NOT NULL DEFAULT 0, outcome BLOB,
        UNIQUE (conversation, client_id)
    );
    CREATE INDEX IF NOT EXISTS outbox_pending ON outbox (conversation, ordinal)
        WHERE outcome IS NULL;
    CREATE TABLE IF NOT EXISTS conversations (id BLOB PRIMARY KEY, state BLOB NOT NULL);
    CREATE TABLE IF NOT EXISTS streams (
        target BLOB PRIMARY KEY, context BLOB NOT NULL, covered BLOB NOT NULL, cursor BLOB NOT NULL
    );
    CREATE TABLE IF NOT EXISTS recoveries (
        target BLOB PRIMARY KEY, reason INTEGER NOT NULL,
        data_loss_possible INTEGER NOT NULL, pending INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS history_ranges (
        target BLOB NOT NULL, first BLOB NOT NULL, last BLOB NOT NULL,
        unavailable INTEGER NOT NULL, PRIMARY KEY (target, unavailable, first)
    );
    CREATE TABLE IF NOT EXISTS messages (
        conversation BLOB NOT NULL, id BLOB NOT NULL, sequence BLOB NOT NULL,
        revision BLOB NOT NULL, view BLOB NOT NULL, PRIMARY KEY (conversation, id)
    );
    CREATE INDEX IF NOT EXISTS message_sequence ON messages (conversation, sequence);
";

pub(crate) struct Storage {
    db: SqliteConnection,
    _lock: File,
}

pub(crate) struct Pending {
    pub ordinal: i64,
    pub request: SendRequest,
    pub attempted: bool,
}

pub(crate) struct Progress {
    pub context: StreamContext,
    pub covered: u64,
    pub cursor: Vec<u8>,
}

impl Storage {
    pub async fn open(path: &Path, identity: Identity) -> Result<Self> {
        // Resolve aliases to the same path before locking. Keep the lock file in place so
        // every process opens the same inode. SQLite owns its own database and WAL locks.
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let path = path.canonicalize()?;
        let mut lock_path = path.as_os_str().to_owned();
        lock_path.push(".client-lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        lock.try_lock().map_err(std::io::Error::from)?;
        let options = SqliteConnectOptions::new()
            .filename(path)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full);
        let mut db = SqliteConnection::connect_with(&options).await?;
        let mut tx = db.begin().await?;
        sqlx::raw_sql(SCHEMA).execute(&mut *tx).await?;
        let tenant = bytes(identity.tenant_id);
        let user = bytes(identity.user_id);
        sqlx::query("INSERT OR IGNORE INTO identity VALUES (1, ?, ?, ?)")
            .bind(tenant.to_vec())
            .bind(user.to_vec())
            .bind(identity.device_id.to_vec())
            .execute(&mut *tx)
            .await?;
        let saved: (Vec<u8>, Vec<u8>, Vec<u8>) =
            sqlx::query_as("SELECT tenant, user, device FROM identity WHERE singleton = 1")
                .fetch_one(&mut *tx)
                .await?;
        if saved.0 != tenant || saved.1 != user || saved.2 != identity.device_id {
            return Err(Error::IdentityMismatch);
        }
        tx.commit().await?;
        Ok(Self { db, _lock: lock })
    }

    pub async fn enqueue(&mut self, conversation: u64, id: Id128, req: &SendRequest) -> Result<()> {
        sqlx::query("INSERT INTO outbox (conversation, client_id, request) VALUES (?, ?, ?)")
            .bind(bytes(conversation).to_vec())
            .bind(id.to_vec())
            .bind(req.encode_to_vec())
            .execute(&mut self.db)
            .await?;
        Ok(())
    }

    pub async fn heads(&mut self) -> Result<Vec<Pending>> {
        let rows: Vec<(i64, Vec<u8>, bool)> = sqlx::query_as(
            "SELECT ordinal, request, attempted FROM outbox
             WHERE ordinal IN (SELECT MIN(ordinal) FROM outbox WHERE outcome IS NULL GROUP BY conversation)
             ORDER BY ordinal",
        )
        .fetch_all(&mut self.db)
        .await?;
        rows.into_iter()
            .map(|(ordinal, data, attempted)| {
                Ok(Pending {
                    ordinal,
                    request: SendRequest::decode(data.as_slice())?,
                    attempted,
                })
            })
            .collect()
    }

    pub async fn mark_attempted(&mut self, ordinal: i64) -> Result<()> {
        sqlx::query("UPDATE outbox SET attempted = 1 WHERE ordinal = ?")
            .bind(ordinal)
            .execute(&mut self.db)
            .await?;
        Ok(())
    }

    pub async fn finish(&mut self, ordinal: i64, outcome: SendOutcome) -> Result<()> {
        sqlx::query("UPDATE outbox SET outcome = ? WHERE ordinal = ?")
            .bind(outcome.encode_to_vec())
            .bind(ordinal)
            .execute(&mut self.db)
            .await?;
        Ok(())
    }

    pub async fn outcome(&mut self, key: &MessageKey) -> Result<Option<SendOutcome>> {
        let (conversation, id) = key
            .conversation_id
            .zip(key.client_msg_id.as_deref())
            .ok_or(Error::Invalid("message key"))?;
        let value: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT outcome FROM outbox WHERE conversation = ? AND client_id = ?",
        )
        .bind(bytes(conversation).to_vec())
        .bind(id)
        .fetch_optional(&mut self.db)
        .await?
        .flatten();
        value
            .map(|value| Ok(SendOutcome::decode(value.as_slice())?))
            .transpose()
    }

    pub async fn conversations(&mut self) -> Result<Vec<u64>> {
        let ids: Vec<Vec<u8>> = sqlx::query_scalar("SELECT id FROM conversations ORDER BY id")
            .fetch_all(&mut self.db)
            .await?;
        ids.into_iter().map(|id| number(&id)).collect()
    }

    pub async fn messages(&mut self, target: u64) -> Result<Vec<MessageView>> {
        let rows: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT view FROM messages WHERE conversation = ? ORDER BY sequence, id",
        )
        .bind(bytes(target).to_vec())
        .fetch_all(&mut self.db)
        .await?;
        rows.into_iter()
            .map(|row| Ok(MessageView::decode(row.as_slice())?))
            .collect()
    }

    pub async fn progress(&mut self, target: u64) -> Result<Option<Progress>> {
        let row: Option<(Vec<u8>, Vec<u8>, Vec<u8>)> =
            sqlx::query_as("SELECT context, covered, cursor FROM streams WHERE target = ?")
                .bind(bytes(target).to_vec())
                .fetch_optional(&mut self.db)
                .await?;
        row.map(|(context, covered, cursor)| {
            Ok(Progress {
                context: StreamContext::decode(context.as_slice())?,
                covered: number(&covered)?,
                cursor,
            })
        })
        .transpose()
    }

    pub async fn apply(
        &mut self,
        target: u64,
        changes: Vec<Change>,
        progress: Option<&Progress>,
    ) -> Result<()> {
        let mut tx = self.db.begin().await?;
        Self::stage_page(&mut tx, target, changes, progress).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn apply_snapshot(
        &mut self,
        target: u64,
        changes: Vec<Change>,
        progress: &Progress,
        gap: Option<(u64, u64)>,
    ) -> Result<()> {
        let mut tx = self.db.begin().await?;
        if target == 0 {
            let current: HashSet<u64> = changes
                .iter()
                .filter_map(|change| match &change.payload {
                    Some(change::Payload::ConversationUpsert(state)) => state.conversation_id,
                    _ => None,
                })
                .collect();
            let old: Vec<Vec<u8>> = sqlx::query_scalar("SELECT id FROM conversations")
                .fetch_all(&mut *tx)
                .await?;
            for id in old {
                let id = number(&id)?;
                if !current.contains(&id) {
                    clear_conversation(&mut tx, id).await?;
                }
            }
        } else {
            sqlx::query("DELETE FROM messages WHERE conversation = ?")
                .bind(bytes(target).to_vec())
                .execute(&mut *tx)
                .await?;
            let state = ConversationState {
                conversation_id: Some(target),
                kind: Some(iam_protocol::pb::ConversationKind::Direct as i32),
                history_id: progress.context.epoch.clone(),
                access_version: progress.context.access_version,
            };
            sqlx::query("UPDATE conversations SET state = ? WHERE id = ?")
                .bind(state.encode_to_vec())
                .bind(bytes(target).to_vec())
                .execute(&mut *tx)
                .await?;
        }
        Self::stage_page(&mut tx, target, changes, Some(progress)).await?;
        if let Some((first, last)) = gap {
            merge_range(&mut tx, target, first, last, false).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn accepts_push(&mut self, target: u64, context: &StreamContext) -> Result<bool> {
        let Some(progress) = self.progress(target).await? else {
            return Ok(false);
        };
        if &progress.context != context {
            return Ok(false);
        }
        let state: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT state FROM conversations WHERE id = ?")
                .bind(bytes(target).to_vec())
                .fetch_optional(&mut self.db)
                .await?;
        let Some(state) = state else {
            return Ok(false);
        };
        let state = ConversationState::decode(state.as_slice())?;
        Ok(state.history_id == context.epoch && state.access_version == context.access_version)
    }

    pub async fn queue_history(&mut self, target: u64, first: u64, last: u64) -> Result<()> {
        let mut tx = self.db.begin().await?;
        merge_range(&mut tx, target, first, last, false).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn history_ranges(&mut self, target: u64) -> Result<Vec<HistoryRange>> {
        let rows: Vec<(Vec<u8>, Vec<u8>, bool)> = sqlx::query_as(
            "SELECT first, last, unavailable FROM history_ranges WHERE target = ? ORDER BY first",
        )
        .bind(bytes(target).to_vec())
        .fetch_all(&mut self.db)
        .await?;
        rows.into_iter()
            .map(|(first, last, unavailable)| {
                Ok(HistoryRange {
                    from: number(&first)?,
                    through: number(&last)?,
                    unavailable,
                })
            })
            .collect()
    }

    pub async fn history_page(
        &mut self,
        target: u64,
        range: HistoryRange,
        messages: Vec<MessageView>,
        floor: u64,
        remaining: Option<u64>,
    ) -> Result<()> {
        let mut tx = self.db.begin().await?;
        for view in messages {
            save_message(&mut tx, target, view).await?;
        }
        sqlx::query(
            "DELETE FROM history_ranges WHERE target = ? AND first = ? AND unavailable = 0",
        )
        .bind(bytes(target).to_vec())
        .bind(bytes(range.from).to_vec())
        .execute(&mut *tx)
        .await?;
        if floor > range.from {
            merge_range(
                &mut tx,
                target,
                range.from,
                range.through.min(floor - 1),
                true,
            )
            .await?;
        }
        if let Some(last) = remaining {
            merge_range(&mut tx, target, range.from.max(floor), last, false).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn recovery(&mut self, target: u64) -> Result<Option<Recovery>> {
        let row: Option<(i32, bool, bool)> = sqlx::query_as(
            "SELECT reason, data_loss_possible, pending FROM recoveries WHERE target = ?",
        )
        .bind(bytes(target).to_vec())
        .fetch_optional(&mut self.db)
        .await?;
        row.map(|(reason, data_loss_possible, pending)| {
            Ok(Recovery {
                reason: ResetReason::try_from(reason)
                    .map_err(|_| Error::Invalid("local reset reason"))?,
                data_loss_possible,
                pending,
            })
        })
        .transpose()
    }

    /// Discard a view and its cursor together, keeping known conversation targets discoverable.
    /// A cursor awaiting long-offline recovery stays intact for the future history pass.
    pub async fn reset(&mut self, target: u64, recovery: Option<Recovery>) -> Result<()> {
        let mut tx = self.db.begin().await?;
        if let Some(recovery) = recovery {
            sqlx::query(
                "INSERT INTO recoveries VALUES (?, ?, ?, ?)
                 ON CONFLICT (target) DO UPDATE SET reason = excluded.reason, pending = excluded.pending,
                 data_loss_possible = recoveries.data_loss_possible OR excluded.data_loss_possible",
            )
            .bind(bytes(target).to_vec())
            .bind(recovery.reason as i32)
            .bind(recovery.data_loss_possible)
            .bind(recovery.pending)
            .execute(&mut *tx)
            .await?;
        }
        if !recovery.is_some_and(|recovery| recovery.reason == ResetReason::CursorExpired) {
            if target == 0 {
                for statement in [
                    "DELETE FROM conversations",
                    "DELETE FROM messages",
                    "DELETE FROM streams",
                    "DELETE FROM history_ranges",
                ] {
                    sqlx::query(statement).execute(&mut *tx).await?;
                }
            } else {
                clear_view(&mut tx, target).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    async fn stage_page(
        tx: &mut Transaction<'_, Sqlite>,
        target: u64,
        changes: Vec<Change>,
        progress: Option<&Progress>,
    ) -> Result<()> {
        for change in changes {
            match (target, change.payload) {
                (0, Some(change::Payload::ConversationUpsert(state))) => {
                    save_conversation(tx, state).await?;
                }
                (0, Some(change::Payload::ConversationRemove(id))) if id != 0 => {
                    clear_conversation(tx, id).await?;
                }
                (id, Some(change::Payload::MessageUpsert(view))) if id != 0 => {
                    save_message(tx, id, view).await?;
                }
                _ => return Err(Error::Invalid("change outside the supported text stream")),
            }
        }
        if let Some(progress) = progress {
            sqlx::query(
                "INSERT INTO streams VALUES (?, ?, ?, ?)
                 ON CONFLICT (target) DO UPDATE SET context = excluded.context,
                 covered = excluded.covered, cursor = excluded.cursor",
            )
            .bind(bytes(target).to_vec())
            .bind(progress.context.encode_to_vec())
            .bind(bytes(progress.covered).to_vec())
            .bind(progress.cursor.as_slice())
            .execute(&mut **tx)
            .await?;
            sqlx::query("UPDATE recoveries SET pending = 0 WHERE target = ? AND pending = 1")
                .bind(bytes(target).to_vec())
                .execute(&mut **tx)
                .await?;
        }
        Ok(())
    }

    pub async fn hide(&mut self, target: u64) -> Result<()> {
        let mut tx = self.db.begin().await?;
        clear_conversation(&mut tx, target).await?;
        tx.commit().await?;
        Ok(())
    }
}

async fn save_conversation(
    tx: &mut Transaction<'_, Sqlite>,
    state: ConversationState,
) -> Result<()> {
    let id = state
        .conversation_id
        .filter(|id| *id != 0)
        .ok_or(Error::Invalid("conversation ID"))?;
    let previous: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT state FROM conversations WHERE id = ?")
            .bind(bytes(id).to_vec())
            .fetch_optional(&mut **tx)
            .await?;
    if let Some(previous) = previous {
        let previous = ConversationState::decode(previous.as_slice())?;
        if previous.history_id != state.history_id
            || previous.access_version != state.access_version
        {
            // Keep the cursor until conversation SYNC can report why it is no longer valid.
            clear_cached_history(tx, id).await?;
        }
    }
    sqlx::query(
        "INSERT INTO conversations VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET state = excluded.state",
    )
    .bind(bytes(id).to_vec())
    .bind(state.encode_to_vec())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn clear_conversation(tx: &mut Transaction<'_, Sqlite>, id: u64) -> Result<()> {
    sqlx::query("DELETE FROM conversations WHERE id = ?")
        .bind(bytes(id).to_vec())
        .execute(&mut **tx)
        .await?;
    clear_view(tx, id).await
}

async fn clear_view(tx: &mut Transaction<'_, Sqlite>, id: u64) -> Result<()> {
    sqlx::query("DELETE FROM streams WHERE target = ?")
        .bind(bytes(id).to_vec())
        .execute(&mut **tx)
        .await?;
    clear_cached_history(tx, id).await
}

async fn clear_cached_history(tx: &mut Transaction<'_, Sqlite>, id: u64) -> Result<()> {
    for statement in [
        "DELETE FROM messages WHERE conversation = ?",
        "DELETE FROM history_ranges WHERE target = ?",
    ] {
        sqlx::query(statement)
            .bind(bytes(id).to_vec())
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

async fn merge_range(
    tx: &mut Transaction<'_, Sqlite>,
    target: u64,
    first: u64,
    last: u64,
    unavailable: bool,
) -> Result<()> {
    let target = bytes(target).to_vec();
    let adjacent_first = bytes(first.saturating_sub(1)).to_vec();
    let adjacent_last = bytes(last.saturating_add(1)).to_vec();
    let rows: Vec<(Vec<u8>, Vec<u8>)> = sqlx::query_as(
        "SELECT first, last FROM history_ranges WHERE target = ? AND unavailable = ?
         AND first <= ? AND last >= ?",
    )
    .bind(&target)
    .bind(unavailable)
    .bind(&adjacent_last)
    .bind(&adjacent_first)
    .fetch_all(&mut **tx)
    .await?;
    let mut merged = (first, last);
    for (first, last) in rows {
        merged.0 = merged.0.min(number(&first)?);
        merged.1 = merged.1.max(number(&last)?);
    }
    sqlx::query("DELETE FROM history_ranges WHERE target = ? AND unavailable = ? AND first <= ? AND last >= ?")
        .bind(&target).bind(unavailable).bind(adjacent_last).bind(adjacent_first)
        .execute(&mut **tx).await?;
    sqlx::query("INSERT INTO history_ranges VALUES (?, ?, ?, ?)")
        .bind(target)
        .bind(bytes(merged.0).to_vec())
        .bind(bytes(merged.1).to_vec())
        .bind(unavailable)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn save_message(
    tx: &mut Transaction<'_, Sqlite>,
    target: u64,
    view: MessageView,
) -> Result<()> {
    let reference = view
        .r#ref
        .as_ref()
        .ok_or(Error::Invalid("message reference"))?;
    if reference.conversation_id != Some(target) {
        return Err(Error::Invalid("message conversation"));
    }
    let id = validate_id128(reference.message_id.as_deref().unwrap_or_default())?;
    let sequence = view
        .position
        .as_ref()
        .and_then(|p| p.create_seq)
        .ok_or(Error::Invalid("message position"))?;
    let revision = view
        .revision_seq
        .ok_or(Error::Invalid("message revision"))?;
    sqlx::query(
        "INSERT INTO messages VALUES (?, ?, ?, ?, ?)
         ON CONFLICT (conversation, id) DO UPDATE SET sequence = excluded.sequence,
         revision = excluded.revision, view = excluded.view WHERE messages.revision < excluded.revision",
    )
    .bind(bytes(target).to_vec())
    .bind(id.to_vec())
    .bind(bytes(sequence).to_vec())
    .bind(bytes(revision).to_vec())
    .bind(view.encode_to_vec())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

// Network IDs and sequences are unsigned. Big-endian keys preserve their order in SQLite.
fn bytes(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

fn number(value: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(
        value
            .try_into()
            .map_err(|_| Error::Invalid("local sequence"))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use iam_protocol::generate_id128;
    use iam_protocol::pb::{sync_target, MessagePosition, MessageRef, SyncTarget};
    use std::process::Stdio;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, BufReader};

    const CHILD_DB: &str = "IAM_CLIENT_PAGE_CRASH_DB";

    #[tokio::test]
    async fn page_and_cursor_survive_process_crash_together() {
        let identity = Identity {
            tenant_id: 1,
            user_id: 2,
            device_id: [
                0x01, 0x8b, 0xcf, 0xe5, 0x68, 0x00, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, 1,
            ],
        };
        let context = StreamContext {
            target: Some(SyncTarget {
                value: Some(sync_target::Value::Conversation(1)),
            }),
            epoch: Some(identity.device_id.to_vec().into()),
            access_version: Some(1),
        };
        let next = Progress {
            context: context.clone(),
            covered: 1,
            cursor: vec![2],
        };
        let id = generate_id128();
        let message = MessageView {
            r#ref: Some(MessageRef {
                conversation_id: Some(1),
                message_id: Some(id.to_vec().into()),
            }),
            position: Some(MessagePosition {
                history_id: context.epoch.clone(),
                create_seq: Some(1),
            }),
            revision_seq: Some(1),
            ..MessageView::default()
        };
        let change = Change {
            event_seq: Some(1),
            payload: Some(change::Payload::MessageUpsert(message.clone())),
        };

        if let Some(path) = std::env::var_os(CHILD_DB) {
            let mut storage = Storage::open(Path::new(&path), identity).await.unwrap();
            let mut tx = storage.db.begin().await.unwrap();
            Storage::stage_page(&mut tx, 1, vec![change], Some(&next))
                .await
                .unwrap();
            println!("PAGE_STAGED");
            std::future::pending::<()>().await;
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("device.db");
        let mut storage = Storage::open(&path, identity).await.unwrap();
        let previous = Progress {
            context,
            covered: 0,
            cursor: vec![1],
        };
        storage.apply(1, vec![], Some(&previous)).await.unwrap();
        drop(storage);

        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::tests::page_and_cursor_survive_process_crash_together",
                "--nocapture",
            ])
            .env(CHILD_DB, &path)
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(line) = lines.next_line().await.unwrap() {
                if line.contains("PAGE_STAGED") {
                    return;
                }
            }
            panic!("child exited before staging the page");
        })
        .await
        .expect("child did not reach the transaction boundary");
        child.kill().await.unwrap();

        let mut storage = Storage::open(&path, identity).await.unwrap();
        assert_eq!(
            storage.progress(1).await.unwrap().unwrap().cursor,
            previous.cursor
        );
        assert!(storage.messages(1).await.unwrap().is_empty());
        storage.apply(1, vec![change], Some(&next)).await.unwrap();
        drop(storage);

        let mut storage = Storage::open(&path, identity).await.unwrap();
        assert_eq!(
            storage.progress(1).await.unwrap().unwrap().cursor,
            next.cursor
        );
        assert_eq!(storage.messages(1).await.unwrap(), [message]);
    }
}
