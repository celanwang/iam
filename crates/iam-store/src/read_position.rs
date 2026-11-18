use super::membership::lock_authority;
use super::*;
use iam_protocol::pb::ReadPositionRequest;

pub(super) const CURRENT_POSITION_SQL: &str = "
    SELECT m.tenant_id, m.conversation_id, m.user_id, c.history_id, c.next_sequence,
           e.access_version, p.read_through
    FROM members m
    JOIN account_events e USING (tenant_id, user_id, account_sequence)
    JOIN conversations c ON c.tenant_id = m.tenant_id AND c.conversation_id = m.conversation_id
    LEFT JOIN read_positions p ON p.tenant_id = m.tenant_id AND p.user_id = m.user_id
         AND p.conversation_id = m.conversation_id AND p.history_id = c.history_id
         AND p.access_version = e.access_version
    WHERE m.tenant_id = ? AND m.conversation_id = ? AND m.user_id = ?";

pub(super) struct PositionScope {
    pub(super) conversation: u64,
    pub(super) history: Id128,
    pub(super) access_version: u64,
}

impl PositionScope {
    pub(super) fn new(
        conversation: Option<u64>,
        history_id: Option<&[u8]>,
        access_version: Option<u64>,
    ) -> Result<Self> {
        let conversation = conversation.unwrap_or(0);
        let access_version = access_version.unwrap_or(0);
        if conversation == 0 || access_version == 0 {
            return Err(Error::InvalidRequest("invalid conversation scope"));
        }
        let history = validate_id128(history_id.unwrap_or_default())
            .map_err(|_| Error::InvalidRequest("invalid conversation history ID"))?;
        Ok(Self {
            conversation,
            history,
            access_version,
        })
    }

    async fn current(
        &self,
        db: &mut MySqlConnection,
        identity: Identity,
    ) -> Result<(Option<u64>, u64)> {
        // One statement keeps membership, history and the position in the same read view.
        let row = sqlx::query(CURRENT_POSITION_SQL)
            .bind(identity.tenant_id)
            .bind(self.conversation)
            .bind(identity.user_id)
            .fetch_optional(db)
            .await?
            .ok_or(Error::PermissionDenied)?;
        self.validate_current(&row)
    }

    pub(super) fn validate_current(&self, row: &MySqlRow) -> Result<(Option<u64>, u64)> {
        let conversation = conversation_info(row)?;
        let version: u64 = row.try_get("access_version")?;
        if conversation.history_id != self.history || version != self.access_version {
            return Err(Error::ReadPositionContextChanged);
        }
        let upper = conversation.next_sequence - 1;
        let saved: Option<u64> = row.try_get("read_through")?;
        if saved.is_some_and(|position| position == 0 || position > upper) {
            return Err(Error::Integrity(
                "stored read position is outside its history",
            ));
        }
        Ok((saved, upper))
    }

    async fn save(&self, db: &mut MySqlConnection, identity: Identity, through: u64) -> Result<()> {
        sqlx::query(
            "INSERT INTO read_positions
                (tenant_id, conversation_id, user_id, history_id, access_version, read_through)
             VALUES (?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE read_through = VALUES(read_through)",
        )
        .bind(identity.tenant_id)
        .bind(self.conversation)
        .bind(identity.user_id)
        .bind(self.history.as_slice())
        .bind(self.access_version)
        .bind(through)
        .execute(db)
        .await?;
        Ok(())
    }
}

impl TidbStore {
    pub(crate) async fn read_position(
        &self,
        identity: Identity,
        request: &ReadPositionRequest,
        admit_write: impl FnOnce() -> Result<()>,
    ) -> Result<Option<u64>> {
        let scope = PositionScope::new(
            request.conversation_id,
            request.history_id.as_deref(),
            request.access_version,
        )?;
        if request.read_through == Some(0) {
            return Err(Error::InvalidRequest("zero read position"));
        }
        let Some(through) = request.read_through else {
            let mut db = self.pool.acquire().await?;
            let (saved, _) = scope.current(&mut db, identity).await?;
            return Ok(saved);
        };
        let mut tx = self.pool.begin().await?;
        let result = async {
            // Sends and membership changes take this lock before mutating the checked rows.
            // READ COMMITTED makes the following statement see their committed state.
            lock_authority(&mut tx, self.id).await?;
            let (saved, upper) = scope.current(&mut tx, identity).await?;
            if through > upper {
                return Err(Error::InvalidRequest(
                    "read position exceeds the committed sequence",
                ));
            }
            if saved >= Some(through) {
                return Ok(saved);
            }
            admit_write()?;
            scope.save(&mut tx, identity, through).await?;
            Ok(Some(through))
        }
        .await;
        finish(tx, result).await
    }
}

pub(super) async fn check_schema(pool: &MySqlPool) -> Result<()> {
    sqlx::query(
        "SELECT tenant_id, conversation_id, user_id, history_id, access_version, read_through
         FROM read_positions LIMIT 0",
    )
    .execute(pool)
    .await?;
    let key: Vec<(String, String)> = sqlx::query_as(
        "SELECT index_name, column_name FROM information_schema.statistics
         WHERE table_schema = DATABASE() AND table_name = 'read_positions' AND non_unique = 0
         ORDER BY index_name, seq_in_index",
    )
    .fetch_all(pool)
    .await?;
    if key
        .iter()
        .map(|(index, column)| (index.as_str(), column.as_str()))
        .ne([
            ("PRIMARY", "tenant_id"),
            ("PRIMARY", "conversation_id"),
            ("PRIMARY", "user_id"),
            ("PRIMARY", "history_id"),
            ("PRIMARY", "access_version"),
        ])
    {
        return Err(Error::Integrity(
            "schema does not match the read position primary key",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires real local TiDB"]
    async fn tidb_read_position_upgrade_keeps_messages_and_friend_state() {
        let url = super::super::support::isolated_database().await;
        let id = generate_id128();
        let mut seeds = MemoryStore::new(0);
        seeds.seed_direct(1, 1, 100, 200);
        let store = TidbStore::open(url.as_str(), id, Some(&seeds))
            .await
            .unwrap();
        let alice = Identity {
            tenant_id: 1,
            user_id: 100,
            device_id: generate_id128(),
        };
        let bob = Identity {
            user_id: 200,
            device_id: generate_id128(),
            ..alice
        };
        let key = SendKey {
            owner: alice,
            conversation: 1,
            client_id: generate_id128(),
        };
        let message = store
            .decide(&key, Some(&text_content("existing history")), true)
            .await
            .unwrap();
        assert!(matches!(
            message.outcome.value,
            Some(send_outcome::Value::Committed(_))
        ));
        store
            .request_friend(alice, bob.user_id, |_| Ok(()))
            .await
            .unwrap();
        let inbox = store.friend_inbox(bob, 0, false).await.unwrap();
        assert_eq!(inbox.requests.len(), 1);
        let version: u8 = sqlx::query_scalar("SELECT friends_ready FROM store_control")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        let pool = store.pool.clone();
        drop(store);

        sqlx::query("DROP TABLE read_positions")
            .execute(&pool)
            .await
            .unwrap();
        assert!(TidbStore::open(url.as_str(), id, None).await.is_err());
        assert!(TidbStore::upgrade(url.as_str(), generate_id128())
            .await
            .is_err());
        assert!(check_schema(&pool).await.is_err());
        TidbStore::upgrade(url.as_str(), id).await.unwrap();
        TidbStore::upgrade(url.as_str(), id).await.unwrap();

        let resumed = TidbStore::open(url.as_str(), id, None).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM read_positions")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
        let resumed_version: u8 = sqlx::query_scalar("SELECT friends_ready FROM store_control")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(resumed_version, version);
        assert_eq!(resumed.friend_inbox(bob, 0, false).await.unwrap(), inbox);
        assert_eq!(
            resumed.decide(&key, None, true).await.unwrap().outcome,
            message.outcome
        );
    }
}
