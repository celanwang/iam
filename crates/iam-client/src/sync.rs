use crate::storage::Progress;
use crate::wire::{is_connection_error, server_error};
use crate::{decode, retryable, Client, Error, Recovery, Result, Transport, Wire};
use iam_protocol::pb::{
    change, snapshot_item, snapshot_response, sync_response, sync_target, Change, Empty, ErrorCode,
    ResetReason, SnapshotRequest, StreamContext, SyncRequest, SyncTarget,
};
use iam_protocol::{DecodedBody, MsgType};

impl Client {
    pub(crate) async fn synchronize(&mut self, wire: &mut Wire<'_, impl Transport>) -> Result<()> {
        self.sync_target(wire, 0).await?;
        let mut retry = None;
        for target in self.storage.conversations().await? {
            match self.sync_target(wire, target).await {
                Err(Error::Server(error)) if error.code() == ErrorCode::PermissionDenied => {
                    self.storage.hide(target).await?;
                }
                Err(error) if retryable(&error) => {
                    retry.get_or_insert(error);
                }
                result => result?,
            }
        }
        retry.map_or(Ok(()), Err)
    }

    pub(crate) async fn sync_target(
        &mut self,
        wire: &mut Wire<'_, impl Transport>,
        target: u64,
    ) -> Result<()> {
        let mut saved = self.storage.progress(target).await?;
        let mut page_token = None;
        let mut resumed = false;
        loop {
            let cursor = if page_token.is_none() {
                saved
                    .as_ref()
                    .map(|progress| progress.cursor.clone().into())
            } else {
                None
            };
            let response = wire
                .request(
                    MsgType::Sync,
                    &SyncRequest {
                        target: Some(sync_target(target)),
                        cursor,
                        page_token: page_token.clone(),
                        limit: Some(wire.limit),
                    },
                )
                .await?;
            let DecodedBody::SyncAck(response) = response else {
                return Err(Error::Invalid("expected SYNC_ACK"));
            };
            match response
                .value
                .ok_or(Error::Invalid("missing sync result"))?
            {
                sync_response::Value::Page(page) => {
                    let progress = progress(
                        target,
                        page.context,
                        page.covered_through,
                        page.next_cursor.as_deref(),
                    )?;
                    if let Some(previous) = &saved {
                        if progress.context != previous.context
                            || progress.covered < previous.covered
                            || (page.next_page_token.is_some()
                                && progress.covered == previous.covered)
                        {
                            return Err(Error::Invalid("sync page did not advance its stream"));
                        }
                    }
                    page_token = page.next_page_token;
                    self.storage
                        .apply(target, page.changes, Some(&progress))
                        .await?;
                    if page_token.is_none() {
                        return Ok(());
                    }
                    saved = Some(progress);
                }
                sync_response::Value::Error(error)
                    if page_token.is_some()
                        && !resumed
                        && matches!(
                            error.code(),
                            ErrorCode::PageTokenExpired | ErrorCode::InvalidToken
                        ) =>
                {
                    // The last applied page already has a durable cursor. Start a fresh round there.
                    resumed = true;
                    page_token = None;
                }
                sync_response::Value::Error(error)
                    if saved.is_some()
                        && page_token.is_none()
                        && error.code() == ErrorCode::InvalidCursor =>
                {
                    self.storage.reset(target, None).await?;
                    saved = None;
                }
                sync_response::Value::Error(error) => return Err(Error::Server(error)),
                sync_response::Value::Reset(reset)
                    if saved.is_none() && reset.reason() == ResetReason::InitialSync =>
                {
                    return self
                        .snapshot(wire, target, reset.snapshot_token.as_deref(), None)
                        .await;
                }
                sync_response::Value::Reset(reset) => {
                    let reason = reset.reason();
                    if !matches!(
                        reason,
                        ResetReason::AccessChanged
                            | ResetReason::HistoryReset
                            | ResetReason::CursorExpired
                    ) {
                        return Err(Error::Invalid("unexpected sync reset"));
                    }
                    self.storage
                        .reset(
                            target,
                            Some(Recovery {
                                reason,
                                data_loss_possible: reset.data_loss_possible.unwrap_or_default(),
                                pending: true,
                            }),
                        )
                        .await?;
                    let previous = saved
                        .as_ref()
                        .filter(|_| reason == ResetReason::CursorExpired);
                    return self
                        .snapshot(wire, target, reset.snapshot_token.as_deref(), previous)
                        .await;
                }
            }
        }
    }

    async fn snapshot(
        &mut self,
        wire: &mut Wire<'_, impl Transport>,
        target: u64,
        token: Option<&[u8]>,
        previous: Option<&Progress>,
    ) -> Result<()> {
        let mut request = SnapshotRequest {
            page_token: Some(nonempty(token)?.into()),
            limit: Some(wire.limit),
        };
        let mut snapshot = None;
        let mut changes = Vec::new();
        loop {
            let response = wire.request(MsgType::Snapshot, &request).await?;
            let DecodedBody::SnapshotAck(response) = response else {
                return Err(Error::Invalid("expected SNAPSHOT_ACK"));
            };
            let page = match response.value {
                Some(snapshot_response::Value::Page(page)) => page,
                Some(snapshot_response::Value::Error(error)) => return Err(Error::Server(error)),
                None => return Err(Error::Invalid("missing snapshot result")),
            };
            let scope = (
                page.snapshot_id,
                page.context,
                page.upper_bound,
                page.history_window,
            );
            if snapshot.as_ref().is_some_and(|previous| *previous != scope) {
                return Err(Error::Invalid("snapshot scope changed between pages"));
            }
            changes.extend(
                page.items
                    .into_iter()
                    .map(|item| match item.value {
                        Some(snapshot_item::Value::Conversation(state)) => {
                            Ok(change::Payload::ConversationUpsert(state))
                        }
                        Some(snapshot_item::Value::Message(view)) => {
                            Ok(change::Payload::MessageUpsert(view))
                        }
                        None => Err(Error::Invalid("missing snapshot item")),
                    })
                    .map(|payload| {
                        payload.map(|payload| Change {
                            event_seq: None,
                            payload: Some(payload),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            match page.next_page_token {
                Some(next) if request.page_token.as_ref() == Some(&next) => {
                    return Err(Error::Invalid("snapshot page token did not advance"));
                }
                Some(next) => {
                    snapshot = Some(scope);
                    request.page_token = Some(next);
                }
                None => {
                    let progress =
                        progress(target, scope.1, scope.2, page.resume_cursor.as_deref())?;
                    let gap = previous.zip(scope.3).and_then(|(previous, window)| {
                        let through = match window.from_create_seq {
                            Some(0) => progress.covered,
                            Some(start) => start - 1,
                            None => return None,
                        };
                        (previous.context == progress.context && previous.covered < through)
                            .then(|| (previous.covered + 1, through))
                    });
                    return self
                        .storage
                        .apply_snapshot(target, changes, &progress, gap)
                        .await;
                }
            }
        }
    }

    pub(crate) async fn handle_inbound(&mut self, bytes: &[u8]) -> Result<()> {
        let frame = decode(bytes)?;
        let connection_error = is_connection_error(&frame);
        let push = match frame.body {
            DecodedBody::Push(push) => push,
            DecodedBody::Error(error) if connection_error => return Err(server_error(error)),
            _ => return Err(Error::Invalid("expected PUSH or connection error")),
        };
        let context = push.context.ok_or(Error::Invalid("missing push context"))?;
        let Some(sync_target::Value::Conversation(target)) = context
            .target
            .as_ref()
            .and_then(|target| target.value.as_ref())
        else {
            // The next account SYNC applies membership changes in sequence.
            return Ok(());
        };
        if self.storage.accepts_push(*target, &context).await? {
            self.storage.apply(*target, push.changes, None).await?;
        }
        Ok(())
    }
}

fn sync_target(target: u64) -> SyncTarget {
    SyncTarget {
        value: Some(if target == 0 {
            sync_target::Value::Account(Empty {})
        } else {
            sync_target::Value::Conversation(target)
        }),
    }
}

fn progress(
    target: u64,
    context: Option<StreamContext>,
    covered: Option<u64>,
    cursor: Option<&[u8]>,
) -> Result<Progress> {
    let context = context
        .filter(|context| context.target.as_ref() == Some(&sync_target(target)))
        .ok_or(Error::Invalid("stream target"))?;
    Ok(Progress {
        context,
        covered: covered.ok_or(Error::Invalid("missing stream position"))?,
        cursor: nonempty(cursor)?,
    })
}

fn nonempty(value: Option<&[u8]>) -> Result<Vec<u8>> {
    value
        .filter(|value| !value.is_empty())
        .map(<[u8]>::to_vec)
        .ok_or(Error::Invalid("missing token"))
}
