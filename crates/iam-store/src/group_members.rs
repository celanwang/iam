use super::membership::{lock_authority, set_access};
use super::*;
use iam_protocol::pb::{
    GroupMember, GroupMemberRemoveAck, GroupMemberRemoveRequest, GroupMembersAck,
};
use iam_protocol::MAX_GROUP_MEMBERS;

impl TidbStore {
    pub(crate) async fn group_members(
        &self,
        actor: Identity,
        conversation: u64,
    ) -> Result<GroupMembersAck> {
        if conversation == 0 {
            return Err(Error::InvalidRequest("missing group conversation ID"));
        }
        // The caller's membership and all returned rows are checked by the same statement.
        let rows = sqlx::query(
            "SELECT g.creator_id, c.history_id, m.user_id, e.access_version
             FROM group_creations g
             JOIN conversations c USING (tenant_id, conversation_id)
             JOIN members a ON a.tenant_id = g.tenant_id AND a.conversation_id = g.conversation_id
                 AND a.user_id = ?
             JOIN members m ON m.tenant_id = g.tenant_id AND m.conversation_id = g.conversation_id
             JOIN account_events e ON e.tenant_id = m.tenant_id AND e.user_id = m.user_id
                 AND e.account_sequence = m.account_sequence AND e.conversation_id = m.conversation_id
             WHERE g.tenant_id = ? AND g.conversation_id = ? AND c.kind = ?
             ORDER BY m.user_id LIMIT ?",
        )
        .bind(actor.user_id)
        .bind(actor.tenant_id)
        .bind(conversation)
        .bind(ConversationKind::Group as u8)
        .bind(MAX_GROUP_MEMBERS as u64 + 1)
        .fetch_all(&self.pool)
        .await?;
        let first = rows.first().ok_or(Error::PermissionDenied)?;
        if rows.len() > MAX_GROUP_MEMBERS {
            return Err(Error::Integrity("too many active group members"));
        }
        let owner: u64 = first.try_get("creator_id")?;
        let history = id_column(first, "history_id")?;
        let members = rows
            .iter()
            .map(|row| {
                Ok(GroupMember {
                    user_id: Some(row.try_get("user_id")?),
                    access_version: Some(row.try_get("access_version")?),
                })
            })
            .collect::<Result<_>>()?;
        Ok(GroupMembersAck {
            conversation_id: Some(conversation),
            history_id: Some(history.to_vec().into()),
            owner_user_id: Some(owner),
            members,
        })
    }

    pub(crate) async fn remove_group_member(
        &self,
        actor: Identity,
        request: &GroupMemberRemoveRequest,
        admit: impl FnOnce(bool) -> Result<()>,
    ) -> Result<GroupMemberRemoveAck> {
        let scope = MemberPeriod::parse(request)?;
        let mut tx = self.pool.begin().await?;
        let result = async {
            lock_authority(&mut tx, self.id).await?;
            scope.end(&mut tx, actor, admit).await
        }
        .await;
        finish(tx, result).await
    }
}

struct MemberPeriod {
    conversation: u64,
    history: Id128,
    user: u64,
    version: u64,
}

impl MemberPeriod {
    fn parse(request: &GroupMemberRemoveRequest) -> Result<Self> {
        let conversation = request.conversation_id.unwrap_or(0);
        let user = request.user_id.unwrap_or(0);
        let version = request.access_version.unwrap_or(0);
        if [conversation, user, version].contains(&0) {
            return Err(Error::InvalidRequest("invalid group member scope"));
        }
        let history = validate_id128(request.history_id.as_deref().unwrap_or_default())
            .map_err(|_| Error::InvalidRequest("invalid group history ID"))?;
        Ok(Self {
            conversation,
            history,
            user,
            version,
        })
    }

    async fn end(
        &self,
        tx: &mut Transaction<'_, MySql>,
        actor: Identity,
        admit: impl FnOnce(bool) -> Result<()>,
    ) -> Result<GroupMemberRemoveAck> {
        // The shared authority lock orders the following READ COMMITTED read and revoke
        // against SEND, trusted access changes, and other member removals.
        let row = sqlx::query(
            "SELECT g.creator_id, c.history_id, a.user_id IS NOT NULL AS actor_active,
                    m.user_id IS NOT NULL AS target_active, e.access_version
             FROM group_creations g
             JOIN conversations c USING (tenant_id, conversation_id)
             LEFT JOIN members a ON a.tenant_id = g.tenant_id AND a.conversation_id = g.conversation_id
                 AND a.user_id = ?
             LEFT JOIN members m ON m.tenant_id = g.tenant_id AND m.conversation_id = g.conversation_id
                 AND m.user_id = ?
             LEFT JOIN account_events e ON e.tenant_id = m.tenant_id AND e.user_id = m.user_id
                 AND e.account_sequence = m.account_sequence AND e.conversation_id = m.conversation_id
             WHERE g.tenant_id = ? AND g.conversation_id = ? AND c.kind = ?",
        )
        .bind(actor.user_id)
        .bind(self.user)
        .bind(actor.tenant_id)
        .bind(self.conversation)
        .bind(ConversationKind::Group as u8)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(Error::PermissionDenied)?;
        let owner: u64 = row.try_get("creator_id")?;
        let actor_active: bool = row.try_get("actor_active")?;
        let leaving = actor.user_id == self.user;
        if actor.user_id == owner {
            if !actor_active {
                return Err(Error::PermissionDenied);
            }
            if leaving {
                return Err(Error::InvalidRequest("the group owner cannot leave"));
            }
        } else if !leaving {
            return Err(Error::PermissionDenied);
        }
        let target_active: bool = row.try_get("target_active")?;
        let current: Option<u64> = row.try_get("access_version")?;
        if target_active != current.is_some() {
            return Err(Error::Integrity("missing active group member grant"));
        }
        let write = current == Some(self.version);
        if !write && !self.has_ended(tx, actor.tenant_id).await? {
            return Err(if leaving && !actor_active {
                Error::PermissionDenied
            } else if target_active {
                Error::GroupMemberContextChanged
            } else {
                Error::GroupMemberNotFound
            });
        }
        if id_column(&row, "history_id")? != self.history {
            return Err(Error::GroupMemberContextChanged);
        }
        admit(write)?;
        if write {
            set_access(
                tx,
                actor.tenant_id,
                self.conversation,
                self.user,
                self.history,
                false,
                false,
            )
            .await?;
        }
        Ok(GroupMemberRemoveAck {
            conversation_id: Some(self.conversation),
            user_id: Some(self.user),
            access_version: Some(self.version),
        })
    }

    async fn has_ended(&self, db: &mut MySqlConnection, tenant: u64) -> Result<bool> {
        // The next event for this conversation after the exact grant must be a revoke.
        // Neither a later grant nor the current absence of membership is a receipt by itself.
        let ended: Option<bool> = sqlx::query_scalar(
            "SELECT subsequent.access_version IS NULL
             FROM account_events granted
             JOIN account_events subsequent USING (tenant_id, user_id, conversation_id)
             WHERE granted.tenant_id = ? AND granted.user_id = ? AND granted.conversation_id = ?
               AND granted.access_version = ? AND subsequent.account_sequence > granted.account_sequence
             ORDER BY subsequent.account_sequence LIMIT 1",
        )
        .bind(tenant)
        .bind(self.user)
        .bind(self.conversation)
        .bind(self.version)
        .fetch_optional(db)
        .await?;
        Ok(ended == Some(true))
    }
}
