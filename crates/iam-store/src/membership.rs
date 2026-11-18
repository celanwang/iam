use super::friend_removal::backfill_friend_generations;
use super::friends::migrate_friend_attempts;
use super::*;

impl TidbStore {
    /// Upgrade a stopped store, retaining all existing cursor positions.
    pub async fn upgrade(url: &str, id: Id128) -> Result<()> {
        let pool = connect_pool(url).await?;
        reject_legacy_names(&pool).await?;
        check_schema(&pool, true).await?;
        let mut tx = pool.begin().await?;
        lock_authority(&mut tx, id).await?;
        finish(tx, Ok(())).await?;

        for statement in include_str!("../schema.sql").split(';').map(str::trim) {
            if statement.starts_with("CREATE TABLE IF NOT EXISTS account_events ")
                || statement.starts_with("CREATE TABLE IF NOT EXISTS read_positions ")
                || statement.starts_with("CREATE TABLE IF NOT EXISTS conversation_retention ")
                || statement.starts_with("CREATE TABLE IF NOT EXISTS direct_pairs ")
                || statement.starts_with("CREATE TABLE IF NOT EXISTS friend_requests ")
                || statement.starts_with("CREATE TABLE IF NOT EXISTS friend_attempts ")
                || statement.starts_with("CREATE TABLE IF NOT EXISTS group_creations ")
                || statement.starts_with("CREATE TABLE IF NOT EXISTS friend_removals ")
                || statement.starts_with("CREATE TABLE IF NOT EXISTS direct_send_blocks ")
            {
                sqlx::query(statement).execute(&pool).await?;
            }
        }
        sqlx::query(
            "ALTER TABLE store_control ADD COLUMN IF NOT EXISTS friends_ready TINYINT UNSIGNED NOT NULL DEFAULT 0",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS kind TINYINT UNSIGNED NOT NULL DEFAULT 1",
        )
        .execute(&pool)
        .await?;
        sqlx::query("ALTER TABLE direct_pairs ADD COLUMN IF NOT EXISTS generation BINARY(16) NULL")
            .execute(&pool)
            .await?;
        check_schema(&pool, false).await?;
        let mut tx = pool.begin().await?;
        lock_authority(&mut tx, id).await?;
        match schema_version(&mut tx).await? {
            STORAGE_SCHEMA_VERSION => return finish(tx, Ok(())).await,
            0 => {
                backfill_account_events(&mut tx).await?;
                backfill_direct_pairs(&mut tx).await?;
                migrate_friend_attempts(&mut tx).await?;
            }
            1 => migrate_friend_attempts(&mut tx).await?,
            2 | 3 => {}
            _ => return Err(Error::Configuration("unrecognized storage schema version")),
        }
        backfill_friend_generations(&mut tx).await?;
        mark_schema_ready(&mut tx).await?;
        finish(tx, Ok(())).await
    }

    pub async fn create_direct(
        &self,
        tenant: u64,
        conversation: u64,
        a: u64,
        b: u64,
    ) -> Result<()> {
        if [tenant, conversation, a, b].contains(&0) || a == b {
            return Err(Error::InvalidRequest("invalid direct conversation"));
        }
        let mut tx = self.pool.begin().await?;
        lock_authority(&mut tx, self.id).await?;
        let pair = ordered_pair(a, b);
        if let Some(existing) = direct_pair(&mut *tx, tenant, pair).await? {
            return if conversation == existing.conversation_id {
                finish(tx, Ok(())).await
            } else {
                Err(Error::InvalidRequest(
                    "pair already has a direct conversation",
                ))
            };
        }
        create_conversation_in(
            &mut tx,
            tenant,
            conversation,
            generate_id128(),
            ConversationKind::Direct,
            &pair,
        )
        .await?;
        insert_direct_pair(&mut tx, tenant, pair, conversation, None).await?;
        finish(tx, Ok(())).await
    }

    pub async fn set_member_access(
        &self,
        tenant: u64,
        conversation: u64,
        user: u64,
        allow: bool,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_authority(&mut tx, self.id).await?;
        let conv = conversation_in(&mut tx, tenant, conversation).await?;
        // Trusted access changes are limited to the original participants.
        let participant: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM account_events WHERE tenant_id = ? AND user_id = ? AND conversation_id = ? LIMIT 1 FOR UPDATE",
        )
        .bind(tenant).bind(user).bind(conversation).fetch_optional(&mut *tx).await?;
        if participant.is_none() {
            return Err(Error::InvalidRequest("user is not a participant"));
        }
        let active: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM members WHERE tenant_id = ? AND conversation_id = ? AND user_id = ? FOR UPDATE",
        )
        .bind(tenant)
        .bind(conversation)
        .bind(user)
        .fetch_optional(&mut *tx)
        .await?;
        if active.is_some() != allow {
            set_access(
                &mut tx,
                tenant,
                conversation,
                user,
                conv.history_id,
                allow,
                false,
            )
            .await?;
        }
        finish(tx, Ok(())).await
    }

    pub async fn set_retention_floor(
        &self,
        tenant: u64,
        conversation: u64,
        floor: u64,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_authority(&mut tx, self.id).await?;
        let conv = conversation_in(&mut tx, tenant, conversation).await?;
        let previous: Option<u64> = sqlx::query_scalar(
            "SELECT floor_sequence FROM conversation_retention WHERE tenant_id = ? AND conversation_id = ?",
        )
        .bind(tenant).bind(conversation).fetch_optional(&mut *tx).await?;
        if floor < previous.unwrap_or(1) || floor > conv.next_sequence {
            return Err(Error::InvalidRequest(
                "retention floor cannot move backwards or past the next message",
            ));
        }
        sqlx::query(
            "INSERT INTO conversation_retention VALUES (?, ?, ?)
             ON DUPLICATE KEY UPDATE floor_sequence = VALUES(floor_sequence)",
        )
        .bind(tenant)
        .bind(conversation)
        .bind(floor)
        .execute(&mut *tx)
        .await?;
        finish(tx, Ok(())).await
    }
}

pub(super) async fn lock_authority(tx: &mut Transaction<'_, MySql>, id: Id128) -> Result<()> {
    let row = sqlx::query(CONTROL).fetch_one(&mut **tx).await?;
    check_store_id(&row, id)?;
    control_clock(&row)?;
    Ok(())
}

pub(super) fn ordered_pair(a: u64, b: u64) -> [u64; 2] {
    [a.min(b), a.max(b)]
}

#[derive(Clone, Copy)]
pub(super) struct DirectPair {
    pub(super) conversation_id: u64,
    pub(super) friends: bool,
    // Retained when a friendship is removed to distinguish it from a never-friended Direct.
    pub(super) generation: Option<Id128>,
}

pub(super) async fn direct_pair<'a>(
    db: impl sqlx::Executor<'a, Database = MySql>,
    tenant: u64,
    [low, high]: [u64; 2],
) -> Result<Option<DirectPair>> {
    // Mutating callers hold the authority lock. Under READ COMMITTED this statement
    // runs after the prior transaction has settled, without taking another lock.
    let row = sqlx::query(
        "SELECT conversation_id, friends, generation FROM direct_pairs WHERE tenant_id = ? AND user_low = ? AND user_high = ?",
    )
    .bind(tenant).bind(low).bind(high).fetch_optional(db).await?;
    row.map(|row| {
        let friends = row.try_get::<bool, _>("friends")?;
        let generation = row
            .try_get::<Option<Vec<u8>>, _>("generation")?
            .map(|id| {
                validate_id128(&id).map_err(|_| Error::Integrity("invalid friend generation"))
            })
            .transpose()?;
        if friends && generation.is_none() {
            return Err(Error::Integrity(
                "active friendship is missing its generation",
            ));
        }
        Ok(DirectPair {
            conversation_id: row.try_get("conversation_id")?,
            friends,
            generation,
        })
    })
    .transpose()
}

pub(super) async fn insert_direct_pair(
    db: &mut MySqlConnection,
    tenant: u64,
    [low, high]: [u64; 2],
    conversation: u64,
    generation: Option<Id128>,
) -> Result<()> {
    sqlx::query("INSERT INTO direct_pairs (tenant_id, user_low, user_high, conversation_id, friends, generation) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(tenant)
        .bind(low)
        .bind(high)
        .bind(conversation)
        .bind(generation.is_some())
        .bind(generation.as_ref().map(|id| id.as_slice()))
        .execute(db)
        .await?;
    Ok(())
}

pub(super) async fn create_conversation_in(
    tx: &mut Transaction<'_, MySql>,
    tenant: u64,
    conversation: u64,
    epoch: Id128,
    kind: ConversationKind,
    participants: &[u64],
) -> Result<()> {
    sqlx::query(
        "INSERT INTO conversations (tenant_id, conversation_id, history_id, next_sequence, kind)
         VALUES (?, ?, ?, 1, ?)",
    )
    .bind(tenant)
    .bind(conversation)
    .bind(epoch.as_slice())
    .bind(kind as u8)
    .execute(&mut **tx)
    .await?;
    for &user in participants {
        set_access(tx, tenant, conversation, user, epoch, true, true).await?;
    }
    Ok(())
}

pub(super) async fn schema_version(db: &mut MySqlConnection) -> Result<u8> {
    Ok(
        sqlx::query_scalar("SELECT friends_ready FROM store_control WHERE singleton = 1")
            .fetch_one(db)
            .await?,
    )
}

async fn backfill_account_events(tx: &mut Transaction<'_, MySql>) -> Result<()> {
    sqlx::query(
        "INSERT INTO account_events
         (tenant_id, user_id, account_sequence, conversation_id, history_id, access_version)
         SELECT m.tenant_id, m.user_id, m.account_sequence, m.conversation_id, c.history_id, 1
         FROM members m
         JOIN conversations c ON c.tenant_id = m.tenant_id AND c.conversation_id = m.conversation_id
         LEFT JOIN account_events e ON e.tenant_id = m.tenant_id AND e.user_id = m.user_id
            AND e.account_sequence = m.account_sequence
         WHERE e.account_sequence IS NULL",
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(super) async fn backfill_direct_pairs(tx: &mut Transaction<'_, MySql>) -> Result<()> {
    let invalid: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM conversations c LEFT JOIN account_events e USING (tenant_id, conversation_id)
         WHERE c.kind = 1
         GROUP BY c.tenant_id, c.conversation_id
         HAVING COUNT(DISTINCT e.user_id) <> 2 OR MIN(e.user_id) = 0 LIMIT 1",
    )
    .fetch_optional(&mut **tx)
    .await?;
    if invalid.is_some() {
        return Err(Error::Integrity(
            "direct conversation must have two original participants",
        ));
    }
    // Include revoked participants and select one canonical ID for legacy duplicate pairs.
    sqlx::query(
        "INSERT INTO direct_pairs (tenant_id, user_low, user_high, conversation_id, friends)
         SELECT tenant_id, user_low, user_high, MIN(conversation_id), FALSE
         FROM (
             SELECT tenant_id, MIN(user_id) AS user_low, MAX(user_id) AS user_high, conversation_id
             FROM account_events GROUP BY tenant_id, conversation_id
         ) AS participants
         JOIN conversations c USING (tenant_id, conversation_id)
         WHERE c.kind = 1
         GROUP BY tenant_id, user_low, user_high",
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(super) async fn mark_schema_ready(tx: &mut Transaction<'_, MySql>) -> Result<()> {
    sqlx::query("UPDATE store_control SET friends_ready = ? WHERE singleton = 1")
        .bind(STORAGE_SCHEMA_VERSION)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn conversation_in(
    db: &mut MySqlConnection,
    tenant: u64,
    conversation: u64,
) -> Result<ConversationInfo> {
    let row = sqlx::query(
        "SELECT history_id, next_sequence FROM conversations WHERE tenant_id = ? AND conversation_id = ?",
    ).bind(tenant).bind(conversation).fetch_optional(db).await?
        .ok_or(Error::InvalidRequest("unknown conversation"))?;
    conversation_info(&row)
}

async fn set_access(
    tx: &mut Transaction<'_, MySql>,
    tenant: u64,
    conversation: u64,
    user: u64,
    epoch: Id128,
    allow: bool,
    initial: bool,
) -> Result<()> {
    let upper: Option<u64> = sqlx::query_scalar(
        "SELECT account_sequence FROM account_events WHERE tenant_id = ? AND user_id = ?
         ORDER BY account_sequence DESC LIMIT 1 FOR UPDATE",
    )
    .bind(tenant)
    .bind(user)
    .fetch_optional(&mut **tx)
    .await?;
    let position = upper
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(Error::SequenceExhausted)?;
    let version = allow.then_some(if initial { INITIAL_ACCESS } else { position });
    write_account_event(tx, tenant, user, position, conversation, epoch, version).await?;
    if allow {
        sqlx::query("INSERT INTO members (tenant_id, user_id, conversation_id, account_sequence) VALUES (?, ?, ?, ?)")
            .bind(tenant).bind(user).bind(conversation).bind(position).execute(&mut **tx).await?;
    } else {
        sqlx::query(
            "DELETE FROM members WHERE tenant_id = ? AND conversation_id = ? AND user_id = ?",
        )
        .bind(tenant)
        .bind(conversation)
        .bind(user)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

async fn write_account_event(
    tx: &mut Transaction<'_, MySql>,
    tenant: u64,
    user: u64,
    position: u64,
    conversation: u64,
    epoch: Id128,
    version: Option<u64>,
) -> Result<()> {
    sqlx::query("INSERT INTO account_events VALUES (?, ?, ?, ?, ?, ?)")
        .bind(tenant)
        .bind(user)
        .bind(position)
        .bind(conversation)
        .bind(epoch.as_slice())
        .bind(version)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(super) async fn allocate_conversation(db: &mut MySqlConnection, tenant: u64) -> Result<u64> {
    // The caller holds the authority lock shared with trusted conversation creation.
    loop {
        let mut bytes = [0u8; 8];
        getrandom::fill(&mut bytes)
            .map_err(|_| Error::Configuration("cannot generate conversation ID"))?;
        let id = u64::from_be_bytes(bytes);
        if id == 0 {
            continue;
        }
        let used: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM conversations WHERE tenant_id = ? AND conversation_id = ? FOR UPDATE",
        )
        .bind(tenant)
        .bind(id)
        .fetch_optional(&mut *db)
        .await?;
        if used.is_none() {
            return Ok(id);
        }
    }
}
