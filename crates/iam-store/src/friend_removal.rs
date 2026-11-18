use super::friends::validate_peer;
use super::membership::{direct_pair, lock_authority, ordered_pair};
use super::*;

impl TidbStore {
    pub(crate) async fn friend_status(
        &self,
        owner: Identity,
        peer: u64,
    ) -> Result<Option<(Id128, u64)>> {
        validate_peer(owner, peer)?;
        let pair = ordered_pair(owner.user_id, peer);
        Ok(direct_pair(&self.pool, owner.tenant_id, pair)
            .await?
            .filter(|pair| pair.friends)
            .and_then(|pair| pair.generation.map(|id| (id, pair.conversation_id))))
    }

    pub(crate) async fn remove_friend(
        &self,
        owner: Identity,
        peer: u64,
        generation: Id128,
        admit: impl FnOnce(bool) -> Result<()>,
    ) -> Result<u64> {
        validate_peer(owner, peer)?;
        let tenant = owner.tenant_id;
        let pair = ordered_pair(owner.user_id, peer);
        let mut tx = self.pool.begin().await?;
        lock_authority(&mut tx, self.id).await?;
        if let Some(conversation) = removal_receipt(&mut tx, tenant, pair, generation).await? {
            admit(false)?;
            return finish(tx, Ok(conversation)).await;
        }
        let current = direct_pair(&mut *tx, tenant, pair)
            .await?
            .filter(|p| p.friends && p.generation == Some(generation))
            .ok_or(Error::FriendNotFound)?;
        admit(true)?;

        let [low, high] = pair;
        // Membership events retain the two original participants even after independent
        // access revocation. This also finds non-canonical Directs from older stores.
        let blocked = sqlx::query(
            "INSERT INTO direct_send_blocks (tenant_id, conversation_id, user_low, user_high)
             SELECT DISTINCT a.tenant_id, a.conversation_id, ?, ?
             FROM account_events a
             JOIN conversations c ON c.tenant_id = a.tenant_id AND c.conversation_id = a.conversation_id
             WHERE a.tenant_id = ? AND a.user_id = ? AND c.kind = 1
               AND EXISTS (SELECT 1 FROM account_events b WHERE b.tenant_id = a.tenant_id
                           AND b.user_id = ? AND b.conversation_id = a.conversation_id)",
        )
        .bind(low)
        .bind(high)
        .bind(tenant)
        .bind(low)
        .bind(high)
        .execute(&mut *tx)
        .await?;
        if blocked.rows_affected() == 0 {
            return Err(Error::Integrity(
                "friendship is missing its direct conversation",
            ));
        }
        sqlx::query(
            "INSERT INTO friend_removals (tenant_id, user_low, user_high, generation, conversation_id)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(tenant)
        .bind(low)
        .bind(high)
        .bind(generation.as_slice())
        .bind(current.conversation_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE direct_pairs SET friends = FALSE
             WHERE tenant_id = ? AND user_low = ? AND user_high = ?",
        )
        .bind(tenant)
        .bind(low)
        .bind(high)
        .execute(&mut *tx)
        .await?;
        finish(tx, Ok(current.conversation_id)).await
    }
}

async fn removal_receipt(
    db: &mut MySqlConnection,
    tenant: u64,
    [low, high]: [u64; 2],
    generation: Id128,
) -> Result<Option<u64>> {
    Ok(sqlx::query_scalar(
        "SELECT conversation_id FROM friend_removals
         WHERE tenant_id = ? AND user_low = ? AND user_high = ? AND generation = ?",
    )
    .bind(tenant)
    .bind(low)
    .bind(high)
    .bind(generation.as_slice())
    .fetch_optional(db)
    .await?)
}

pub(super) async fn restart_friendship(
    db: &mut MySqlConnection,
    tenant: u64,
    pair: [u64; 2],
) -> Result<Id128> {
    let generation = loop {
        let id = generate_id128();
        if removal_receipt(db, tenant, pair, id).await?.is_none() {
            break id;
        }
    };
    sqlx::query(
        "DELETE FROM direct_send_blocks WHERE tenant_id = ? AND user_low = ? AND user_high = ?",
    )
    .bind(tenant)
    .bind(pair[0])
    .bind(pair[1])
    .execute(db)
    .await?;
    Ok(generation)
}

pub(super) async fn backfill_friend_generations(tx: &mut Transaction<'_, MySql>) -> Result<()> {
    // Existing active friendships receive a UUIDv7 within their stable, tenant-local pair.
    let prefix = hex::encode(&generate_id128()[..8]);
    sqlx::query(
        "UPDATE direct_pairs
         SET generation = UNHEX(CONCAT(?, '8', LEFT(SHA2(CONCAT_WS(':', tenant_id, user_low, user_high), 256), 15)))
         WHERE friends = TRUE AND generation IS NULL",
    )
    .bind(prefix)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
