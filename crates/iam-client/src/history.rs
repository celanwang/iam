use crate::{retryable, Client, Error, HistoryRange, Result, Transport, Wire};
use iam_protocol::pb::{history_response, ErrorCode, HistoryRequest, StreamContext};
use iam_protocol::{DecodedBody, MsgType};
use std::collections::HashMap;

/// Continuation tickets are disposable. Every committed page leaves an independent restart bound.
#[derive(Default)]
pub(crate) struct HistoryRounds(HashMap<u64, HistoryRound>);

struct HistoryRound {
    range: HistoryRange,
    context: StreamContext,
    token: prost::bytes::Bytes,
}

impl HistoryRounds {
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

impl Client {
    pub(crate) async fn backfill(
        &mut self,
        wire: &mut Wire<'_, impl Transport>,
        rounds: &mut HistoryRounds,
    ) -> Result<bool> {
        let targets = self.storage.conversations().await?;
        rounds.0.retain(|target, _| targets.contains(target));
        let mut did_work = false;
        let mut retry = None;
        for target in targets {
            match self.backfill_one(wire, rounds, target).await {
                Ok(work) => did_work |= work,
                Err(error) if retryable(&error) => {
                    retry.get_or_insert(error);
                }
                Err(error) => return Err(error),
            }
        }
        retry.map_or(Ok(did_work), Err)
    }

    async fn backfill_one(
        &mut self,
        wire: &mut Wire<'_, impl Transport>,
        rounds: &mut HistoryRounds,
        target: u64,
    ) -> Result<bool> {
        let range = self
            .storage
            .history_ranges(target)
            .await?
            .into_iter()
            .rev()
            .find(|range| !range.unavailable);
        let (Some(range), Some(progress)) = (range, self.storage.progress(target).await?) else {
            rounds.0.remove(&target);
            return Ok(false);
        };
        let context = progress.context;
        let page_token = rounds
            .0
            .remove(&target)
            .filter(|round| round.range == range && round.context == context)
            .map(|round| round.token);
        let before = if page_token.is_none() {
            Some(
                range
                    .through
                    .checked_add(1)
                    .ok_or(Error::Invalid("invalid history boundary"))?,
            )
        } else {
            None
        };
        let response = wire
            .request(
                MsgType::History,
                &HistoryRequest {
                    context: Some(context.clone()),
                    before_create_seq: before,
                    page_token: page_token.clone(),
                    limit: Some(wire.limit),
                },
            )
            .await?;
        let DecodedBody::HistoryAck(response) = response else {
            return Err(Error::Invalid("expected HISTORY_ACK"));
        };
        let page = match response
            .value
            .ok_or(Error::Invalid("missing history result"))?
        {
            history_response::Value::Page(page) => page,
            history_response::Value::Error(error) => match error.code() {
                ErrorCode::PermissionDenied => {
                    self.storage.hide(target).await?;
                    return Ok(true);
                }
                ErrorCode::ContextChanged => {
                    return self.refresh_history_context(wire, target).await;
                }
                ErrorCode::InvalidToken | ErrorCode::PageTokenExpired if page_token.is_some() => {
                    return Ok(true);
                }
                _ => return Err(Error::Server(error)),
            },
        };
        if page.context.as_ref() != Some(&context) {
            return Err(Error::Invalid("history response context"));
        }
        if page.upper_bound.unwrap_or(0) < range.through {
            return self.refresh_history_context(wire, target).await;
        }
        let floor = page
            .retention_floor_seq
            .filter(|floor| *floor > 0)
            .ok_or(Error::Invalid("missing history retention floor"))?;
        let from = range.from.max(floor);
        let mut through = range.through;
        let mut messages = Vec::new();
        for view in page.messages {
            let sequence = view
                .position
                .as_ref()
                .and_then(|p| p.create_seq)
                .ok_or(Error::Invalid("missing history position"))?;
            if sequence > through {
                return Err(Error::Invalid("history page did not advance"));
            }
            through = sequence - 1;
            if sequence >= from {
                messages.push(view);
            }
        }
        let remaining = (page.next_page_token.is_some() && from <= through).then_some(through);
        self.storage
            .history_page(target, range, messages, floor, remaining)
            .await?;
        if let (Some(through), Some(token)) = (remaining, page.next_page_token) {
            rounds.0.insert(
                target,
                HistoryRound {
                    range: HistoryRange {
                        from,
                        through,
                        unavailable: false,
                    },
                    context,
                    token,
                },
            );
        }
        Ok(true)
    }

    async fn refresh_history_context(
        &mut self,
        wire: &mut Wire<'_, impl Transport>,
        target: u64,
    ) -> Result<bool> {
        match self.sync_target(wire, target).await {
            Err(Error::Server(error)) if error.code() == ErrorCode::PermissionDenied => {
                self.storage.hide(target).await?;
            }
            Err(error) => {
                self.storage.reset(target, None).await?;
                return Err(error);
            }
            Ok(()) => {}
        }
        Ok(true)
    }
}
