use super::membership::{
    allocate_conversation, create_conversation_in, lock_authority, ordered_pair,
};
use super::*;
use iam_protocol::{MAX_GROUP_MEMBERS, MAX_GROUP_MEMBER_IDS};

impl TidbStore {
    pub(crate) async fn create_group(
        &self,
        owner: Identity,
        request_id: Id128,
        member_user_ids: &[u64],
        admit: impl FnOnce(bool) -> Result<()>,
    ) -> Result<u64> {
        if member_user_ids.len() > MAX_GROUP_MEMBER_IDS {
            return Err(Error::InvalidRequest("too many group member IDs"));
        }
        let mut peers = member_user_ids.to_vec();
        peers.sort_unstable();
        peers.dedup();
        let member_set: Vec<u8> = peers.iter().flat_map(|id| id.to_be_bytes()).collect();

        let mut tx = self.pool.begin().await?;
        lock_authority(&mut tx, self.id).await?;
        let saved: Option<(u64, Vec<u8>)> = sqlx::query_as(
            "SELECT conversation_id, member_set FROM group_creations
             WHERE tenant_id = ? AND creator_id = ? AND request_id = ? FOR UPDATE",
        )
        .bind(owner.tenant_id)
        .bind(owner.user_id)
        .bind(request_id.as_slice())
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((conversation, original)) = saved {
            admit(false)?;
            return finish(
                tx,
                if original == member_set {
                    Ok(conversation)
                } else {
                    Err(Error::GroupKeyConflict)
                },
            )
            .await;
        }

        if !(2..MAX_GROUP_MEMBERS).contains(&peers.len())
            || peers.first() == Some(&0)
            || peers.binary_search(&owner.user_id).is_ok()
        {
            return Err(Error::InvalidRequest(
                "expected 2 to 31 other group members",
            ));
        }
        require_friends(&mut tx, owner, &peers).await?;
        let conversation = allocate_conversation(&mut tx, owner.tenant_id).await?;
        admit(true)?;

        peers.push(owner.user_id);
        create_conversation_in(
            &mut tx,
            owner.tenant_id,
            conversation,
            generate_id128(),
            ConversationKind::Group,
            &peers,
        )
        .await?;
        sqlx::query(
            "INSERT INTO group_creations (tenant_id, creator_id, request_id, member_set, conversation_id)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(owner.tenant_id)
        .bind(owner.user_id)
        .bind(request_id.as_slice())
        .bind(member_set)
        .bind(conversation)
        .execute(&mut *tx)
        .await?;
        finish(tx, Ok(conversation)).await
    }
}

async fn require_friends(db: &mut MySqlConnection, creator: Identity, peers: &[u64]) -> Result<()> {
    let pairs = peers
        .iter()
        .map(|&peer| ordered_pair(creator.user_id, peer));
    let mut query = sqlx::QueryBuilder::<MySql>::new(
        "SELECT 1 FROM direct_pairs p WHERE p.friends = TRUE AND p.tenant_id = ",
    );
    query
        .push_bind(creator.tenant_id)
        .push(" AND (p.user_low, p.user_high) IN ");
    query.push_tuples(pairs, |mut values, [low, high]| {
        values.push_bind(low).push_bind(high);
    });
    let friends = query
        .push(
            " AND NOT EXISTS (SELECT 1 FROM user_blocks b WHERE b.tenant_id = p.tenant_id AND
                           ((b.actor_id = p.user_low AND b.peer_id = p.user_high)
                            OR (b.actor_id = p.user_high AND b.peer_id = p.user_low)))
          FOR UPDATE",
        )
        .build()
        .fetch_all(db)
        .await?;
    if friends.len() != peers.len() {
        return Err(Error::PermissionDenied);
    }
    Ok(())
}
