use super::membership::{
    create_direct_in, direct_pair, insert_direct_pair, lock_authority, ordered_pair,
};
use super::*;
use iam_protocol::{pb::FriendListAck, MAX_FRIEND_REQUESTS};

impl TidbStore {
    pub(crate) async fn request_friend(
        &self,
        owner: Identity,
        target: u64,
        admit: impl FnOnce(bool) -> Result<()>,
    ) -> Result<Option<u64>> {
        validate_peer(owner, target)?;
        let tenant = owner.tenant_id;
        let sender = owner.user_id;
        let mut tx = self.pool.begin().await?;
        lock_authority(&mut tx, self.id).await?;
        let relationship = direct_pair(&mut tx, tenant, ordered_pair(sender, target)).await?;
        if let Some((conversation, true)) = relationship {
            admit(false)?;
            return finish(tx, Ok(Some(conversation))).await;
        }
        let pending = has_request(&mut tx, tenant, sender, target).await?;
        admit(!pending)?;
        if !pending {
            sqlx::query("INSERT INTO friend_requests VALUES (?, ?, ?, TRUE)")
                .bind(tenant)
                .bind(target)
                .bind(sender)
                .execute(&mut *tx)
                .await?;
        }
        finish(tx, Ok(None)).await
    }

    pub(crate) async fn incoming_friends(
        &self,
        owner: Identity,
        after_user: u64,
    ) -> Result<FriendListAck> {
        let mut from_user_ids: Vec<u64> = sqlx::query_scalar(
            "SELECT from_user_id FROM friend_requests FORCE INDEX (pending_incoming)
             WHERE tenant_id = ? AND to_user_id = ? AND pending = TRUE AND from_user_id > ?
             ORDER BY from_user_id LIMIT ?",
        )
        .bind(owner.tenant_id)
        .bind(owner.user_id)
        .bind(after_user)
        .bind((MAX_FRIEND_REQUESTS + 1) as u32)
        .fetch_all(&self.pool)
        .await?;
        let next_after_user_id = if from_user_ids.len() > MAX_FRIEND_REQUESTS {
            from_user_ids.truncate(MAX_FRIEND_REQUESTS);
            from_user_ids.last().copied()
        } else {
            None
        };
        Ok(FriendListAck {
            from_user_ids,
            next_after_user_id,
        })
    }

    pub(crate) async fn accept_friend(
        &self,
        owner: Identity,
        sender: u64,
        admit: impl FnOnce(bool) -> Result<()>,
    ) -> Result<u64> {
        validate_peer(owner, sender)?;
        let tenant = owner.tenant_id;
        let recipient = owner.user_id;
        let pair = ordered_pair(sender, recipient);
        let mut tx = self.pool.begin().await?;
        lock_authority(&mut tx, self.id).await?;
        if !has_request(&mut tx, tenant, sender, recipient).await? {
            return Err(Error::FriendNotFound);
        }
        let existing = direct_pair(&mut tx, tenant, pair).await?;
        if let Some((conversation, true)) = existing {
            admit(false)?;
            return finish(tx, Ok(conversation)).await;
        }
        let conversation = if let Some((conversation, false)) = existing {
            let active: Vec<u64> = sqlx::query_scalar(
                "SELECT user_id FROM members WHERE tenant_id = ? AND conversation_id = ?
                 ORDER BY user_id FOR UPDATE",
            )
            .bind(tenant)
            .bind(conversation)
            .fetch_all(&mut *tx)
            .await?;
            if active != pair {
                return Err(Error::PermissionDenied);
            }
            admit(true)?;
            sqlx::query("UPDATE direct_pairs SET friends = TRUE WHERE tenant_id = ? AND user_low = ? AND user_high = ?")
                .bind(tenant).bind(pair[0]).bind(pair[1]).execute(&mut *tx).await?;
            conversation
        } else {
            let conversation = allocate_conversation(&mut tx, tenant).await?;
            admit(true)?;
            create_direct_in(&mut tx, tenant, conversation, generate_id128(), pair).await?;
            insert_direct_pair(&mut tx, tenant, pair, conversation, true).await?;
            conversation
        };
        sqlx::query(
            "UPDATE friend_requests SET pending = FALSE WHERE tenant_id = ?
             AND ((to_user_id = ? AND from_user_id = ?) OR (to_user_id = ? AND from_user_id = ?))",
        )
        .bind(tenant)
        .bind(recipient)
        .bind(sender)
        .bind(sender)
        .bind(recipient)
        .execute(&mut *tx)
        .await?;
        finish(tx, Ok(conversation)).await
    }
}

fn validate_peer(owner: Identity, peer: u64) -> Result<()> {
    if peer == 0 || peer == owner.user_id {
        return Err(Error::InvalidRequest("invalid friend account ID"));
    }
    Ok(())
}

async fn has_request(
    db: &mut MySqlConnection,
    tenant: u64,
    sender: u64,
    recipient: u64,
) -> Result<bool> {
    let request: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM friend_requests WHERE tenant_id = ? AND to_user_id = ? AND from_user_id = ? FOR UPDATE",
    )
    .bind(tenant).bind(recipient).bind(sender).fetch_optional(db).await?;
    Ok(request.is_some())
}

async fn allocate_conversation(db: &mut MySqlConnection, tenant: u64) -> Result<u64> {
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
