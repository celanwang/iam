use super::friends::validate_peer;
use super::membership::{
    direct_pair, lock_authority, ordered_pair, pause_direct_sends, resume_direct_sends,
};
use super::*;

impl TidbStore {
    pub(crate) async fn user_block_status(&self, owner: Identity, peer: u64) -> Result<bool> {
        validate_peer(owner, peer)?;
        is_blocking(&self.pool, owner, peer).await
    }

    pub(crate) async fn set_user_block(
        &self,
        owner: Identity,
        peer: u64,
        request_id: Id128,
        blocked: bool,
        admit: impl FnOnce(bool, Option<u64>) -> Result<()>,
    ) -> Result<bool> {
        validate_peer(owner, peer)?;
        let mut tx = self.pool.begin().await?;
        lock_authority(&mut tx, self.id).await?;
        let saved: Option<(u64, bool)> = sqlx::query_as(
            "SELECT peer_id, blocked FROM user_block_changes
             WHERE tenant_id = ? AND actor_id = ? AND request_id = ?",
        )
        .bind(owner.tenant_id)
        .bind(owner.user_id)
        .bind(request_id.as_slice())
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((old_peer, old_state)) = saved {
            admit(false, None)?;
            return finish(
                tx,
                if (peer, blocked) == (old_peer, old_state) {
                    Ok(old_state)
                } else {
                    Err(Error::UserBlockKeyConflict)
                },
            )
            .await;
        }

        let was_blocked = is_blocking(&mut *tx, owner, peer).await?;
        admit(true, (!was_blocked).then_some(peer))?;
        let pair = ordered_pair(owner.user_id, peer);
        if blocked && !was_blocked {
            sqlx::query("INSERT INTO user_blocks (tenant_id, actor_id, peer_id) VALUES (?, ?, ?)")
                .bind(owner.tenant_id)
                .bind(owner.user_id)
                .bind(peer)
                .execute(&mut *tx)
                .await?;
            pause_direct_sends(&mut tx, owner.tenant_id, pair).await?;
        } else if was_blocked && !blocked {
            sqlx::query(
                "DELETE FROM user_blocks WHERE tenant_id = ? AND actor_id = ? AND peer_id = ?",
            )
            .bind(owner.tenant_id)
            .bind(owner.user_id)
            .bind(peer)
            .execute(&mut *tx)
            .await?;
            // The projection also represents the reverse block and a removed friendship.
            if !pair_is_blocked(&mut tx, owner.tenant_id, pair).await?
                && !direct_pair(&mut *tx, owner.tenant_id, pair)
                    .await?
                    .is_some_and(|p| !p.friends && p.generation.is_some())
            {
                resume_direct_sends(&mut tx, owner.tenant_id, pair).await?;
            }
        }

        sqlx::query(
            "INSERT INTO user_block_changes (tenant_id, actor_id, request_id, peer_id, blocked)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(owner.tenant_id)
        .bind(owner.user_id)
        .bind(request_id.as_slice())
        .bind(peer)
        .bind(blocked)
        .execute(&mut *tx)
        .await?;
        finish(tx, Ok(blocked)).await
    }
}

async fn is_blocking<'a>(
    db: impl sqlx::Executor<'a, Database = MySql>,
    owner: Identity,
    peer: u64,
) -> Result<bool> {
    let row: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM user_blocks WHERE tenant_id = ? AND actor_id = ? AND peer_id = ?",
    )
    .bind(owner.tenant_id)
    .bind(owner.user_id)
    .bind(peer)
    .fetch_optional(db)
    .await?;
    Ok(row.is_some())
}

pub(super) async fn pair_is_blocked(
    db: &mut MySqlConnection,
    tenant: u64,
    [a, b]: [u64; 2],
) -> Result<bool> {
    let row: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM user_blocks WHERE tenant_id = ?
         AND (actor_id, peer_id) IN ((?, ?), (?, ?)) LIMIT 1",
    )
    .bind(tenant)
    .bind(a)
    .bind(b)
    .bind(b)
    .bind(a)
    .fetch_optional(db)
    .await?;
    Ok(row.is_some())
}
