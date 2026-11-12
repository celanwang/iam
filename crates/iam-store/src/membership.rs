use super::*;

impl TidbStore {
    /// Upgrade a stopped pre-account-stream store, retaining all existing cursor positions.
    pub async fn upgrade(url: &str, id: Id128) -> Result<()> {
        let pool = connect_pool(url).await?;
        reject_legacy_names(&pool).await?;
        check_schema(&pool, true).await?;
        let mut tx = pool.begin().await?;
        lock_authority(&mut tx, id).await?;
        tx.commit().await?;

        for statement in include_str!("../schema.sql").split(';').map(str::trim) {
            if statement.starts_with("CREATE TABLE IF NOT EXISTS account_events ")
                || statement.starts_with("CREATE TABLE IF NOT EXISTS conversation_retention ")
            {
                sqlx::query(statement).execute(&pool).await?;
            }
        }
        check_schema(&pool, false).await?;
        let mut tx = pool.begin().await?;
        lock_authority(&mut tx, id).await?;
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
        .execute(&mut *tx)
        .await?;
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
        let epoch = generate_id128();
        sqlx::query("INSERT INTO conversations VALUES (?, ?, ?, 1)")
            .bind(tenant)
            .bind(conversation)
            .bind(epoch.as_slice())
            .execute(&mut *tx)
            .await?;
        for user in [a, b] {
            set_access(&mut tx, tenant, conversation, user, epoch, true, true).await?;
        }
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
        // Only the original two participants may ever be granted access.
        let participant: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM account_events WHERE tenant_id = ? AND user_id = ? AND conversation_id = ? LIMIT 1",
        )
        .bind(tenant).bind(user).bind(conversation).fetch_optional(&mut *tx).await?;
        if participant.is_none() {
            return Err(Error::InvalidRequest("user is not a participant"));
        }
        let active: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM members WHERE tenant_id = ? AND conversation_id = ? AND user_id = ?",
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

async fn lock_authority(tx: &mut Transaction<'_, MySql>, id: Id128) -> Result<()> {
    let row = sqlx::query(CONTROL).fetch_one(&mut **tx).await?;
    check_store_id(&row, id)?;
    control_clock(&row)?;
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
    let position = account_upper(tx, tenant, user)
        .await?
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

pub(super) async fn write_account_event(
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
