use super::friend_removal::restart_friendship;
use super::membership::{
    allocate_conversation, create_conversation_in, direct_pair, insert_direct_pair, lock_authority,
    ordered_pair,
};
use super::*;
use iam_protocol::{
    pb::{FriendApplicant, FriendInboxAck, FriendListAck},
    MAX_FRIEND_REQUESTS,
};

const PENDING: u8 = 1;
const ACCEPTED: u8 = 2;
const REJECTED: u8 = 3;
const BLOCKED_BY_PENDING: u8 = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FriendAttempt {
    Pending,
    Accepted(u64),
    Rejected,
}

#[derive(Clone, Copy)]
struct Direction {
    tenant: u64,
    sender: u64,
    recipient: u64,
}

impl TidbStore {
    pub(crate) async fn request_friend(
        &self,
        owner: Identity,
        target: u64,
        admit: impl FnOnce(bool) -> Result<()>,
    ) -> Result<Option<u64>> {
        match self.apply_friend(owner, target, None, admit).await? {
            FriendAttempt::Pending => Ok(None),
            FriendAttempt::Accepted(conversation) => Ok(Some(conversation)),
            FriendAttempt::Rejected => Err(Error::FriendChanged),
        }
    }

    pub(crate) async fn apply_friend(
        &self,
        owner: Identity,
        target: u64,
        request_id: Option<Id128>,
        admit: impl FnOnce(bool) -> Result<()>,
    ) -> Result<FriendAttempt> {
        validate_peer(owner, target)?;
        let direction = Direction {
            tenant: owner.tenant_id,
            sender: owner.user_id,
            recipient: target,
        };
        let mut tx = self.pool.begin().await?;
        lock_authority(&mut tx, self.id).await?;
        let saved = if let Some(id) = request_id {
            sqlx::query(
                "SELECT to_user_id, request_id, outcome, conversation_id FROM friend_attempts
                 WHERE tenant_id = ? AND from_user_id = ? AND request_id = ? FOR UPDATE",
            )
            .bind(direction.tenant)
            .bind(direction.sender)
            .bind(id.as_slice())
            .fetch_optional(&mut *tx)
            .await?
        } else {
            incoming_attempt(&mut tx, direction, None).await?
        };
        if let Some(row) = saved {
            admit(false)?;
            if request_id.is_some() && row.try_get::<u64, _>("to_user_id")? != target {
                return Err(Error::FriendKeyConflict);
            }
            return finish(tx, decode_outcome(&row)).await;
        }

        let pair = ordered_pair(direction.sender, direction.recipient);
        let existing = direct_pair(&mut *tx, direction.tenant, pair).await?;
        let established = existing
            .filter(|pair| pair.friends)
            .map(|pair| pair.conversation_id);
        let legacy = request_id.is_none();
        if legacy {
            if let Some(existing) = established {
                admit(false)?;
                return finish(tx, Ok(FriendAttempt::Accepted(existing))).await;
            }
            if existing.is_some_and(|pair| pair.generation.is_some()) {
                return Err(Error::FriendChanged);
            }
            // A peer-only key gets a single lifetime slot. It cannot open a later round.
            let history: Option<i32> = sqlx::query_scalar(
                "SELECT 1 FROM friend_attempts
                 WHERE tenant_id = ? AND to_user_id = ? AND from_user_id = ?
                 LIMIT 1 FOR UPDATE",
            )
            .bind(direction.tenant)
            .bind(direction.recipient)
            .bind(direction.sender)
            .fetch_optional(&mut *tx)
            .await?;
            if history.is_some() {
                return Err(Error::FriendChanged);
            }
        }

        let status = if established.is_some() {
            ACCEPTED
        } else if !legacy && has_pending(&mut tx, direction).await? {
            BLOCKED_BY_PENDING
        } else {
            PENDING
        };
        admit(true)?;
        let id = request_id.unwrap_or_else(generate_id128);
        sqlx::query(
            "INSERT INTO friend_attempts
             (tenant_id, to_user_id, from_user_id, request_id, outcome, conversation_id, legacy_slot)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(direction.tenant)
        .bind(direction.recipient)
        .bind(direction.sender)
        .bind(id.as_slice())
        .bind(status)
        .bind(established)
        .bind(legacy.then_some(1_u8))
        .execute(&mut *tx)
        .await?;
        // Commit the conflicting key before reporting it so a delayed retry cannot open an attempt.
        finish(tx, Ok(())).await?;
        outcome(status, established)
    }

    pub(crate) async fn incoming_friends(
        &self,
        owner: Identity,
        after: u64,
    ) -> Result<FriendListAck> {
        let page = self.friend_inbox(owner, after, true).await?;
        Ok(FriendListAck {
            from_user_ids: page
                .requests
                .into_iter()
                .map(|applicant| applicant.from_user_id.unwrap())
                .collect(),
            next_after_user_id: page.next_after_user_id,
        })
    }

    pub(crate) async fn friend_inbox(
        &self,
        owner: Identity,
        after_user: u64,
        legacy: bool,
    ) -> Result<FriendInboxAck> {
        let query = if legacy {
            "SELECT from_user_id, request_id FROM friend_attempts FORCE INDEX (legacy_inbox)
             WHERE tenant_id = ? AND to_user_id = ? AND legacy_slot = 1 AND pending_slot = 1
               AND from_user_id > ? ORDER BY from_user_id LIMIT ?"
        } else {
            "SELECT from_user_id, request_id FROM friend_attempts FORCE INDEX (pending_attempt)
             WHERE tenant_id = ? AND to_user_id = ? AND pending_slot = 1
               AND from_user_id > ? ORDER BY from_user_id LIMIT ?"
        };
        let mut requests: Vec<(u64, Vec<u8>)> = sqlx::query_as(query)
            .bind(owner.tenant_id)
            .bind(owner.user_id)
            .bind(after_user)
            .bind((MAX_FRIEND_REQUESTS + 1) as u32)
            .fetch_all(&self.pool)
            .await?;
        let next_after_user_id = if requests.len() > MAX_FRIEND_REQUESTS {
            requests.truncate(MAX_FRIEND_REQUESTS);
            requests.last().map(|(user, _)| *user)
        } else {
            None
        };
        Ok(FriendInboxAck {
            requests: requests
                .into_iter()
                .map(|(user, id)| FriendApplicant {
                    from_user_id: Some(user),
                    request_id: Some(id.into()),
                })
                .collect(),
            next_after_user_id,
        })
    }

    pub(crate) async fn accept_friend(
        &self,
        owner: Identity,
        sender: u64,
        admit: impl FnOnce(bool) -> Result<()>,
    ) -> Result<u64> {
        match self.decide_friend(owner, sender, None, true, admit).await? {
            FriendAttempt::Accepted(conversation) => Ok(conversation),
            FriendAttempt::Rejected => Err(Error::FriendChanged),
            FriendAttempt::Pending => Err(Error::Integrity("friend decision returned pending")),
        }
    }

    pub(crate) async fn decide_friend(
        &self,
        owner: Identity,
        sender: u64,
        request_id: Option<Id128>,
        accept: bool,
        admit: impl FnOnce(bool) -> Result<()>,
    ) -> Result<FriendAttempt> {
        validate_peer(owner, sender)?;
        let direction = Direction {
            tenant: owner.tenant_id,
            sender,
            recipient: owner.user_id,
        };
        let mut tx = self.pool.begin().await?;
        lock_authority(&mut tx, self.id).await?;
        let row = incoming_attempt(&mut tx, direction, request_id)
            .await?
            .ok_or(Error::FriendNotFound)?;
        match decode_outcome(&row) {
            Ok(FriendAttempt::Pending) => {}
            Err(Error::FriendInProgress) => return Err(Error::FriendNotFound),
            result => {
                admit(false)?;
                return finish(tx, result).await;
            }
        }

        if !accept {
            admit(true)?;
            let id: Vec<u8> = row.try_get("request_id")?;
            sqlx::query(
                "UPDATE friend_attempts SET outcome = ?
                 WHERE tenant_id = ? AND to_user_id = ? AND from_user_id = ? AND request_id = ?",
            )
            .bind(REJECTED)
            .bind(direction.tenant)
            .bind(direction.recipient)
            .bind(direction.sender)
            .bind(id)
            .execute(&mut *tx)
            .await?;
            return finish(tx, Ok(FriendAttempt::Rejected)).await;
        }

        let conversation = establish_direct(&mut tx, direction, admit).await?;
        sqlx::query(
            "UPDATE friend_attempts SET outcome = ?, conversation_id = ?
             WHERE tenant_id = ? AND pending_slot = 1
               AND ((to_user_id = ? AND from_user_id = ?) OR (to_user_id = ? AND from_user_id = ?))",
        )
        .bind(ACCEPTED)
        .bind(conversation)
        .bind(direction.tenant)
        .bind(direction.recipient)
        .bind(direction.sender)
        .bind(direction.sender)
        .bind(direction.recipient)
        .execute(&mut *tx)
        .await?;
        finish(tx, Ok(FriendAttempt::Accepted(conversation))).await
    }
}

pub(super) fn validate_peer(owner: Identity, peer: u64) -> Result<()> {
    if peer == 0 || peer == owner.user_id {
        return Err(Error::InvalidRequest("invalid friend account ID"));
    }
    Ok(())
}

fn decode_outcome(row: &MySqlRow) -> Result<FriendAttempt> {
    outcome(row.try_get("outcome")?, row.try_get("conversation_id")?)
}

fn outcome(status: u8, conversation: Option<u64>) -> Result<FriendAttempt> {
    match (status, conversation) {
        (PENDING, None) => Ok(FriendAttempt::Pending),
        (ACCEPTED, Some(id)) if id != 0 => Ok(FriendAttempt::Accepted(id)),
        (REJECTED, None) => Ok(FriendAttempt::Rejected),
        (BLOCKED_BY_PENDING, None) => Err(Error::FriendInProgress),
        _ => Err(Error::Integrity("invalid saved friend outcome")),
    }
}

async fn incoming_attempt(
    db: &mut MySqlConnection,
    key: Direction,
    request_id: Option<Id128>,
) -> Result<Option<MySqlRow>> {
    let row = if let Some(id) = request_id {
        sqlx::query(
            "SELECT request_id, outcome, conversation_id FROM friend_attempts
             WHERE tenant_id = ? AND to_user_id = ? AND from_user_id = ? AND request_id = ?
             FOR UPDATE",
        )
        .bind(key.tenant)
        .bind(key.recipient)
        .bind(key.sender)
        .bind(id.as_slice())
        .fetch_optional(db)
        .await?
    } else {
        sqlx::query(
            "SELECT request_id, outcome, conversation_id FROM friend_attempts
             WHERE tenant_id = ? AND to_user_id = ? AND from_user_id = ? AND legacy_slot = 1
             FOR UPDATE",
        )
        .bind(key.tenant)
        .bind(key.recipient)
        .bind(key.sender)
        .fetch_optional(db)
        .await?
    };
    Ok(row)
}

async fn has_pending(db: &mut MySqlConnection, key: Direction) -> Result<bool> {
    let pending: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM friend_attempts
         WHERE tenant_id = ? AND to_user_id = ? AND from_user_id = ? AND pending_slot = 1
         FOR UPDATE",
    )
    .bind(key.tenant)
    .bind(key.recipient)
    .bind(key.sender)
    .fetch_optional(db)
    .await?;
    Ok(pending.is_some())
}

async fn establish_direct(
    tx: &mut Transaction<'_, MySql>,
    key: Direction,
    admit: impl FnOnce(bool) -> Result<()>,
) -> Result<u64> {
    let pair = ordered_pair(key.sender, key.recipient);
    if let Some(existing) = direct_pair(&mut **tx, key.tenant, pair).await? {
        if existing.friends {
            admit(true)?;
            return Ok(existing.conversation_id);
        }
        let reopening = existing.generation.is_some();
        if !reopening {
            let active: Vec<u64> = sqlx::query_scalar(
                "SELECT user_id FROM members WHERE tenant_id = ? AND conversation_id = ?
                 ORDER BY user_id FOR UPDATE",
            )
            .bind(key.tenant)
            .bind(existing.conversation_id)
            .fetch_all(&mut **tx)
            .await?;
            if active != pair {
                return Err(Error::PermissionDenied);
            }
        }
        admit(true)?;
        let generation = if reopening {
            restart_friendship(tx, key.tenant, pair).await?
        } else {
            generate_id128()
        };
        sqlx::query("UPDATE direct_pairs SET friends = TRUE, generation = ? WHERE tenant_id = ? AND user_low = ? AND user_high = ?")
            .bind(generation.as_slice()).bind(key.tenant).bind(pair[0]).bind(pair[1]).execute(&mut **tx).await?;
        Ok(existing.conversation_id)
    } else {
        let conversation = allocate_conversation(tx, key.tenant).await?;
        admit(true)?;
        create_conversation_in(
            tx,
            key.tenant,
            conversation,
            generate_id128(),
            ConversationKind::Direct,
            &pair,
        )
        .await?;
        insert_direct_pair(tx, key.tenant, pair, conversation, Some(generate_id128())).await?;
        Ok(conversation)
    }
}

pub(super) async fn migrate_friend_attempts(tx: &mut Transaction<'_, MySql>) -> Result<()> {
    let missing: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM friend_requests r LEFT JOIN direct_pairs p
           ON p.tenant_id = r.tenant_id AND p.user_low = LEAST(r.from_user_id, r.to_user_id)
           AND p.user_high = GREATEST(r.from_user_id, r.to_user_id)
         WHERE NOT r.pending AND (p.friends IS NULL OR NOT p.friends) LIMIT 1",
    )
    .fetch_optional(&mut **tx)
    .await?;
    if missing.is_some() {
        return Err(Error::Integrity(
            "legacy accepted request has no friend conversation",
        ));
    }
    // Bind a UUIDv7 timestamp/version prefix. Derive distinct RFC variant tails from each
    // original directed pair so the upgrade can backfill with one set-based statement.
    let prefix = hex::encode(&generate_id128()[..8]);
    sqlx::query(
        "INSERT INTO friend_attempts
         (tenant_id, to_user_id, from_user_id, request_id, outcome, conversation_id, legacy_slot)
         SELECT r.tenant_id, r.to_user_id, r.from_user_id,
                UNHEX(CONCAT(?, '8', LEFT(SHA2(CONCAT_WS(':', r.tenant_id, r.to_user_id, r.from_user_id), 256), 15))),
                IF(r.pending, ?, ?), IF(r.pending, NULL, p.conversation_id), 1
         FROM friend_requests r LEFT JOIN direct_pairs p
           ON p.tenant_id = r.tenant_id AND p.user_low = LEAST(r.from_user_id, r.to_user_id)
           AND p.user_high = GREATEST(r.from_user_id, r.to_user_id)",
    )
    .bind(prefix)
    .bind(PENDING)
    .bind(ACCEPTED)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
