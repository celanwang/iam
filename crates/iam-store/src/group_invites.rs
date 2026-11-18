use super::friends::validate_peer;
use super::groups::require_group_friends;
use super::membership::{lock_authority, set_access};
use super::*;
use iam_protocol::{
    pb::{GroupInvitation, GroupInviteInboxAck},
    MAX_GROUP_INVITES,
};

const PENDING: u8 = 1;
const ACCEPTED: u8 = 2;
const REJECTED: u8 = 3;
const BLOCKED_BY_PENDING: u8 = 4;
const EXPIRED: u8 = 5;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum GroupInviteOutcome {
    Pending,
    Accepted(u64),
    Rejected,
}

impl TidbStore {
    pub(crate) async fn invite_group_member(
        &self,
        owner: Identity,
        conversation: u64,
        recipient: u64,
        request_id: Id128,
        admit: impl FnOnce(bool) -> Result<()>,
    ) -> Result<GroupInviteOutcome> {
        validate_peer(owner, recipient)?;
        validate_conversation(conversation)?;
        let mut tx = self.pool.begin().await?;
        lock_authority(&mut tx, self.id).await?;
        let saved: Option<(u64, u64, u8)> = sqlx::query_as(
            "SELECT conversation_id, recipient_id, outcome FROM group_invites
             WHERE tenant_id = ? AND inviter_id = ? AND request_id = ?",
        )
        .bind(owner.tenant_id)
        .bind(owner.user_id)
        .bind(request_id.as_slice())
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((group, target, status)) = saved {
            admit(false)?;
            return finish(
                tx,
                if (group, target) == (conversation, recipient) {
                    saved_outcome(conversation, status)
                } else {
                    Err(Error::GroupInviteKeyConflict)
                },
            )
            .await;
        }

        let (_, active) = invite_access(
            &mut tx,
            owner.tenant_id,
            conversation,
            owner.user_id,
            recipient,
        )
        .await?;
        let status = if active {
            ACCEPTED
        } else if has_pending(&mut tx, owner.tenant_id, conversation, recipient).await? {
            BLOCKED_BY_PENDING
        } else {
            PENDING
        };
        admit(true)?;
        sqlx::query(
            "INSERT INTO group_invites (tenant_id, inviter_id, request_id, conversation_id, recipient_id, outcome)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(owner.tenant_id)
        .bind(owner.user_id)
        .bind(request_id.as_slice())
        .bind(conversation)
        .bind(recipient)
        .bind(status)
        .execute(&mut *tx)
        .await?;
        // Even a key that lost to a pending invite has a durable result.
        finish(tx, Ok(())).await?;
        saved_outcome(conversation, status)
    }

    pub(crate) async fn group_invite_inbox(
        &self,
        recipient: Identity,
        after: u64,
    ) -> Result<GroupInviteInboxAck> {
        let mut rows: Vec<(u64, u64, Vec<u8>)> = sqlx::query_as(
            "SELECT conversation_id, inviter_id, request_id FROM group_invites FORCE INDEX (pending_invite)
             WHERE tenant_id = ? AND recipient_id = ? AND pending_slot = 1 AND conversation_id > ?
             ORDER BY conversation_id LIMIT ?",
        )
        .bind(recipient.tenant_id)
        .bind(recipient.user_id)
        .bind(after)
        .bind((MAX_GROUP_INVITES + 1) as u32)
        .fetch_all(&self.pool)
        .await?;
        let next = if rows.len() > MAX_GROUP_INVITES {
            rows.truncate(MAX_GROUP_INVITES);
            rows.last().map(|(id, _, _)| *id)
        } else {
            None
        };
        Ok(GroupInviteInboxAck {
            invitations: rows
                .into_iter()
                .map(|(conversation, inviter, request_id)| GroupInvitation {
                    conversation_id: Some(conversation),
                    inviter_user_id: Some(inviter),
                    request_id: Some(request_id.into()),
                })
                .collect(),
            next_after_conversation_id: next,
        })
    }

    pub(crate) async fn decide_group_invite(
        &self,
        recipient: Identity,
        conversation: u64,
        request_id: Id128,
        accept: bool,
        admit: impl FnOnce(bool) -> Result<()>,
    ) -> Result<GroupInviteOutcome> {
        validate_conversation(conversation)?;
        let tenant = recipient.tenant_id;
        let user = recipient.user_id;
        let mut tx = self.pool.begin().await?;
        lock_authority(&mut tx, self.id).await?;
        let (inviter, status): (u64, u8) = sqlx::query_as(
            "SELECT inviter_id, outcome FROM group_invites
             WHERE tenant_id = ? AND recipient_id = ? AND conversation_id = ? AND request_id = ?",
        )
        .bind(tenant)
        .bind(user)
        .bind(conversation)
        .bind(request_id.as_slice())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::GroupInviteNotFound)?;
        if status == BLOCKED_BY_PENDING {
            return Err(Error::GroupInviteNotFound);
        }
        if status != PENDING {
            admit(false)?;
            return finish(tx, saved_outcome(conversation, status)).await;
        }

        let status = if accept {
            let (history, active) =
                invite_access(&mut tx, tenant, conversation, inviter, user).await?;
            admit(true)?;
            if !active {
                set_access(&mut tx, tenant, conversation, user, history, true, false).await?;
            }
            ACCEPTED
        } else {
            admit(true)?;
            REJECTED
        };
        sqlx::query(
            "UPDATE group_invites SET outcome = ? WHERE tenant_id = ? AND inviter_id = ? AND request_id = ?",
        )
        .bind(status).bind(tenant).bind(inviter).bind(request_id.as_slice())
        .execute(&mut *tx).await?;
        finish(tx, saved_outcome(conversation, status)).await
    }
}

pub(super) async fn expire_pending_invite(
    db: &mut MySqlConnection,
    tenant: u64,
    conversation: u64,
    recipient: u64,
) -> Result<()> {
    sqlx::query(
        "UPDATE group_invites SET outcome = ?
         WHERE tenant_id = ? AND recipient_id = ? AND pending_slot = 1 AND conversation_id = ?",
    )
    .bind(EXPIRED)
    .bind(tenant)
    .bind(recipient)
    .bind(conversation)
    .execute(db)
    .await?;
    Ok(())
}

async fn has_pending(
    db: &mut MySqlConnection,
    tenant: u64,
    group: u64,
    recipient: u64,
) -> Result<bool> {
    let row: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM group_invites
         WHERE tenant_id = ? AND recipient_id = ? AND pending_slot = 1 AND conversation_id = ?",
    )
    .bind(tenant)
    .bind(recipient)
    .bind(group)
    .fetch_optional(db)
    .await?;
    Ok(row.is_some())
}

async fn invite_access(
    db: &mut MySqlConnection,
    tenant: u64,
    group: u64,
    inviter: u64,
    recipient: u64,
) -> Result<(Id128, bool)> {
    let row = sqlx::query(
        "SELECT c.history_id, CAST(r.user_id IS NOT NULL AS UNSIGNED) AS recipient_active
         FROM group_creations g
         JOIN conversations c ON c.tenant_id = g.tenant_id AND c.conversation_id = g.conversation_id
         JOIN members o ON o.tenant_id = g.tenant_id
           AND o.conversation_id = g.conversation_id AND o.user_id = g.creator_id
         LEFT JOIN members r ON r.tenant_id = g.tenant_id
           AND r.conversation_id = g.conversation_id AND r.user_id = ?
         WHERE g.tenant_id = ? AND g.conversation_id = ? AND g.creator_id = ? AND c.kind = 2",
    )
    .bind(recipient)
    .bind(tenant)
    .bind(group)
    .bind(inviter)
    .fetch_optional(&mut *db)
    .await?
    .ok_or(Error::PermissionDenied)?;
    require_group_friends(db, tenant, inviter, &[recipient]).await?;
    Ok((
        id_column(&row, "history_id")?,
        row.try_get::<u64, _>("recipient_active")? != 0,
    ))
}

fn validate_conversation(conversation: u64) -> Result<()> {
    if conversation == 0 {
        return Err(Error::InvalidRequest("invalid group conversation ID"));
    }
    Ok(())
}

fn saved_outcome(conversation: u64, status: u8) -> Result<GroupInviteOutcome> {
    match status {
        PENDING => Ok(GroupInviteOutcome::Pending),
        ACCEPTED => Ok(GroupInviteOutcome::Accepted(conversation)),
        REJECTED => Ok(GroupInviteOutcome::Rejected),
        BLOCKED_BY_PENDING => Err(Error::GroupInviteInProgress),
        EXPIRED => Err(Error::GroupInviteExpired),
        _ => Err(Error::Integrity("invalid saved group invitation outcome")),
    }
}
