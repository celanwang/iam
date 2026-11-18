use super::read_position::{PositionScope, CURRENT_POSITION_SQL};
use super::*;
use iam_protocol::pb::UnreadCountRequest;
use std::sync::LazyLock;

// The outer scope identifies at most one membership. Probe the existing sequence index
// within that conversation, with the floor, reader position and upper bound as range limits.
static UNREAD_COUNT_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "WITH bounds AS (
             SELECT s.*, CAST(COALESCE(r.floor_sequence, 1) AS UNSIGNED) AS floor,
                    CAST(COALESCE(s.read_through, 0) AS UNSIGNED) AS read_sequence
             FROM ({CURRENT_POSITION_SQL}) s
             LEFT JOIN conversation_retention r USING (tenant_id, conversation_id)
         )
         SELECT /*+ INL_JOIN(msg) */ b.history_id, b.next_sequence, b.access_version,
                b.read_through, b.floor, CAST(COUNT(msg.`sequence`) AS UNSIGNED) AS unread_count
         FROM bounds b
         LEFT JOIN messages msg FORCE INDEX (message_order)
              ON msg.tenant_id = b.tenant_id AND msg.conversation_id = b.conversation_id
             AND msg.`sequence` >= b.floor
             AND msg.`sequence` > b.read_sequence
             AND msg.`sequence` < b.next_sequence
             AND msg.user_id <> b.user_id
             AND b.history_id = ? AND b.access_version = ?
         GROUP BY b.history_id, b.next_sequence, b.access_version, b.read_through, b.floor"
    )
});

impl TidbStore {
    pub(crate) async fn unread_count(
        &self,
        identity: Identity,
        request: &UnreadCountRequest,
    ) -> Result<u64> {
        let scope = PositionScope::new(
            request.conversation_id,
            request.history_id.as_deref(),
            request.access_version,
        )?;
        let row = sqlx::query(&UNREAD_COUNT_SQL)
            .bind(identity.tenant_id)
            .bind(scope.conversation)
            .bind(identity.user_id)
            .bind(scope.history.as_slice())
            .bind(scope.access_version)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(Error::PermissionDenied)?;
        let (_, upper) = scope.validate_current(&row)?;
        let floor: u64 = row.try_get("floor")?;
        if floor == 0 || floor > upper + 1 {
            return Err(Error::Integrity("invalid retention floor"));
        }
        Ok(row.try_get("unread_count")?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires real local TiDB"]
    async fn tidb_unread_count_uses_bounded_message_index() {
        let url = super::super::support::isolated_database().await;
        let mut seeds = MemoryStore::new(0);
        let history = seeds.seed_direct(1, 1, 100, 200);
        let store = TidbStore::open(url.as_str(), generate_id128(), Some(&seeds))
            .await
            .unwrap();
        let sender = Identity {
            tenant_id: 1,
            user_id: 100,
            device_id: generate_id128(),
        };
        let reader = Identity {
            user_id: 200,
            device_id: generate_id128(),
            ..sender
        };
        for _ in 0..5 {
            let outcome = store
                .decide(
                    &SendKey {
                        owner: sender,
                        conversation: 1,
                        client_id: generate_id128(),
                    },
                    Some(&text_content("message")),
                    true,
                )
                .await
                .unwrap()
                .outcome;
            assert!(matches!(
                outcome.value,
                Some(send_outcome::Value::Committed(_))
            ));
        }
        store.set_retention_floor(1, 1, 3).await.unwrap();
        store
            .read_position(
                reader,
                &iam_protocol::pb::ReadPositionRequest {
                    conversation_id: Some(1),
                    history_id: Some(history.to_vec().into()),
                    access_version: Some(1),
                    read_through: Some(3),
                },
                || Ok(()),
            )
            .await
            .unwrap();
        let explain = format!("EXPLAIN ANALYZE {}", UNREAD_COUNT_SQL.as_str());
        let rows = sqlx::query(&explain)
            .bind(1_u64)
            .bind(1_u64)
            .bind(reader.user_id)
            .bind(history.as_slice())
            .bind(1_u64)
            .fetch_all(&store.pool)
            .await
            .unwrap();
        let scan = rows
            .iter()
            .find(|row| {
                row.get::<String, _>("id").contains("IndexRangeScan")
                    && row
                        .get::<String, _>("access object")
                        .contains("message_order")
            })
            .expect("unread count must range-scan message_order");
        let actual: String = scan.get("actRows");
        let details: Vec<u8> = scan.get("operator info");
        println!(
            "message_order returned {actual} rows: {}",
            String::from_utf8_lossy(&details)
        );
        assert_eq!(actual, "2");
    }
}
