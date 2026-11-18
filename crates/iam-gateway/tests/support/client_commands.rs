use super::*;
use iam_client::ClientView;
use iam_protocol::pb::change;
use std::io;

#[derive(Default)]
struct WireLog {
    connections: usize,
    auth: usize,
    reconnect_requests: usize,
    positions: Vec<(usize, Option<u64>)>,
    unread_queries: Vec<usize>,
    delivered: Vec<u64>,
    synced: Vec<u64>,
}

impl WireLog {
    fn assert_connections(&self, count: usize) {
        assert_eq!(self.connections, count, "successful TLS connections");
        assert_eq!(self.auth, count, "AUTH exchanges");
        assert_eq!(self.reconnect_requests, 0, "unexpected protocol RECONNECT");
    }
}

#[derive(Debug)]
enum ReplyDelivery {
    Deliver,
    Lose,
}

struct HeldReply {
    position: u64,
    observed: oneshot::Sender<()>,
    release: oneshot::Receiver<ReplyDelivery>,
}

struct ObservedTls {
    tls: TlsTransport,
    log: WireLog,
    drop_push: Option<(u64, oneshot::Sender<()>)>,
    hold: Option<HeldReply>,
}

impl ObservedTls {
    fn new(gateway: &RunningGateway) -> Self {
        Self {
            tls: gateway.client(),
            log: WireLog::default(),
            drop_push: None,
            hold: None,
        }
    }

    fn hold_position_ack(
        &mut self,
        position: u64,
    ) -> (oneshot::Receiver<()>, oneshot::Sender<ReplyDelivery>) {
        let (observed, checkpoint) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        self.hold = Some(HeldReply {
            position,
            observed,
            release: wait,
        });
        (checkpoint, release)
    }
}

fn sequences(changes: &[iam_protocol::pb::Change]) -> impl Iterator<Item = u64> + '_ {
    changes
        .iter()
        .filter_map(|change| match change.payload.as_ref()? {
            change::Payload::MessageUpsert(view) => view.position.as_ref()?.create_seq,
            _ => None,
        })
}

impl Transport for ObservedTls {
    async fn reconnect(&mut self) -> io::Result<()> {
        self.tls.reconnect().await?;
        self.log.connections += 1;
        Ok(())
    }

    async fn exchange(&mut self, bytes: Vec<u8>) -> io::Result<Vec<u8>> {
        let request = decode(&bytes);
        let reply = self.tls.exchange(bytes).await?;
        match request {
            DecodedBody::Auth(_) => self.log.auth += 1,
            DecodedBody::Reconnect(_) => self.log.reconnect_requests += 1,
            DecodedBody::UnreadCount(_) => self.log.unread_queries.push(self.log.connections),
            DecodedBody::ReadPosition(req) => {
                self.log
                    .positions
                    .push((self.log.connections, req.read_through));
                if self
                    .hold
                    .as_ref()
                    .is_some_and(|gate| Some(gate.position) == req.read_through)
                {
                    let gate = self.hold.take().unwrap();
                    assert!(matches!(decode(&reply), DecodedBody::ReadPositionAck(ack)
                        if ack.read_through == Some(gate.position)));
                    gate.observed.send(()).unwrap();
                    if matches!(gate.release.await.unwrap(), ReplyDelivery::Lose) {
                        return Err(io::ErrorKind::ConnectionReset.into());
                    }
                }
            }
            DecodedBody::Sync(_) => {
                if let DecodedBody::SyncAck(ack) = decode(&reply) {
                    if let Some(sync_response::Value::Page(page)) = ack.value {
                        self.log.synced.extend(sequences(&page.changes));
                    }
                }
            }
            _ => {}
        }
        Ok(reply)
    }

    async fn receive(&mut self) -> io::Result<Vec<u8>> {
        loop {
            let bytes = self.tls.receive().await?;
            if let DecodedBody::Push(push) = decode(&bytes) {
                if self
                    .drop_push
                    .as_ref()
                    .is_some_and(|(seq, _)| sequences(&push.changes).any(|n| n == *seq))
                {
                    let (_, dropped) = self.drop_push.take().unwrap();
                    dropped.send(()).unwrap();
                    continue;
                }
                self.log.delivered.extend(sequences(&push.changes));
            }
            return Ok(bytes);
        }
    }
}

async fn wait_for_message(view: &mut ClientView, sequence: u64) {
    timeout(WAIT, async {
        loop {
            if view
                .messages(1)
                .await
                .unwrap()
                .iter()
                .any(|message| message.position.as_ref().unwrap().create_seq == Some(sequence))
            {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("client did not store message sequence {sequence}"));
}

async fn two_device_gateway(fixture: &Fixture) -> (RunningGateway, DeviceCredential) {
    let [a, b] = fixture.credentials();
    let other = DeviceCredential {
        device_id: generate_id128(),
        token_sha256: Sha256::digest(BOB_SECOND).into(),
        ..b
    };
    let gateway = fixture
        .start_with_credentials(
            OpenMode::Initialize,
            Limits::default(),
            "127.0.0.1:0",
            [a, b, other],
        )
        .await;
    (gateway, other)
}

async fn current_scope(view: &mut ClientView) -> ConversationState {
    wait_for_message(view, 1).await;
    let conversations = view.list_conversations(0, 10).await.unwrap().items;
    assert_eq!(conversations.len(), 1);
    conversations.into_iter().next().unwrap().state
}

// Drive the caller up to its first wait without scheduling a timer or releasing the ACK gate.
async fn start_pending<F: std::future::Future>(mut f: std::pin::Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(f.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_client_commands_share_auth_with_push_sync_and_a_committed_send() {
    let fixture = Fixture::new().await;
    let (gateway, other_device) = two_device_gateway(&fixture).await;
    let dir = tempfile::tempdir().unwrap();
    let mut client = device_client(dir.path(), "bob.db", fixture.credentials()[1]).await;
    let mut view = client.open_view().await.unwrap();
    let other = device_client(dir.path(), "second.db", other_device).await;
    let mut alice = login(&gateway, fixture.alice, ALICE).await;
    send(&mut alice, 2, key(), "before login").await;
    let mut transport = ObservedTls::new(&gateway);
    let (dropped, saw_drop) = oneshot::channel();
    transport.drop_push = Some((3, dropped));
    let (submissions, mut input) = mpsc::channel(1);
    let (commands, receiver) = iam_client::command_channel(2);
    let options = Options {
        sync_interval: Duration::from_millis(150),
        ..Options::default()
    };
    let exercise = async {
        let scope = current_scope(&mut view).await;
        assert_eq!(commands.read_position(&scope).await.unwrap(), None);
        assert_eq!(commands.unread_count(&scope).await.unwrap(), 1);
        assert_eq!(commands.advance_read_position(&scope, 1).await.unwrap(), 1);
        assert_error(
            commands.advance_read_position(&scope, 20).await,
            ErrorCode::InvalidArgument,
        );
        let stale = ConversationState {
            history_id: Some(generate_id128().to_vec().into()),
            ..scope.clone()
        };
        let (bad, valid) = tokio::join!(
            commands.unread_count(&stale),
            commands.read_position(&scope)
        );
        assert_error(bad, ErrorCode::ContextChanged);
        assert_eq!(valid.unwrap(), Some(1));
        send(&mut alice, 3, key(), "delivered push").await;
        wait_for_message(&mut view, 2).await;
        send(&mut alice, 4, key(), "dropped push recovered by sync").await;
        saw_drop.await.unwrap();
        wait_for_message(&mut view, 3).await;
        assert_eq!(commands.unread_count(&scope).await.unwrap(), 2);
        assert_eq!(commands.advance_read_position(&scope, 3).await.unwrap(), 3);
        assert_eq!(
            other
                .read_position(&mut gateway.client(), BOB_SECOND, &scope)
                .await
                .unwrap(),
            Some(3)
        );
        assert_eq!(commands.unread_count(&scope).await.unwrap(), 0);

        // The foreground run and its original submission channel stay usable after all handles close.
        drop(commands);
        let (saved, result) = oneshot::channel();
        submissions
            .send(iam_client::Submission {
                conversation_id: 1,
                text: "outbound after commands".into(),
                saved,
            })
            .await
            .unwrap();
        let outbound = result.await.unwrap().unwrap();
        let DecodedBody::Push(push) = receive(&mut alice).await else {
            panic!("expected outbound PUSH");
        };
        assert_eq!(sequences(&push.changes).collect::<Vec<_>>(), [4]);
        let Some(change::Payload::MessageUpsert(view)) = push.changes[0].payload.as_ref() else {
            panic!("expected sent message");
        };
        assert!(
            matches!(view.content.as_ref().unwrap().value.as_ref(), Some(message_content::Value::Text(text)) if text.text.as_deref() == Some("outbound after commands"))
        );
        assert_eq!(view.sender_id, Some(200));
        drop(submissions);
        outbound
    };
    let run = client.run_with_commands(&mut transport, BOB, options, &mut input, receiver);
    let (run_result, outbound) = timeout(WAIT, async { tokio::join!(run, exercise) })
        .await
        .expect("foreground commands, sync and send timed out");
    run_result.unwrap();
    let outcome = client.outcome(&outbound).await.unwrap().unwrap();
    assert!(
        matches!(outcome.value, Some(send_outcome::Value::Committed(receipt)) if receipt.position.as_ref().unwrap().create_seq == Some(4))
    );
    transport.log.assert_connections(1);
    assert!(transport.log.delivered.contains(&2));
    assert!(transport.log.synced.contains(&3));
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_client_commands_lost_committed_ack_reconnects_before_serving_queued_query() {
    let fixture = Fixture::new().await;
    let (gateway, other_device) = two_device_gateway(&fixture).await;
    let dir = tempfile::tempdir().unwrap();
    let mut client = device_client(dir.path(), "bob.db", fixture.credentials()[1]).await;
    let mut view = client.open_view().await.unwrap();
    let other = device_client(dir.path(), "second.db", other_device).await;
    let mut alice = login(&gateway, fixture.alice, ALICE).await;
    for (request_id, text) in [(2, "one"), (3, "two"), (4, "three")] {
        send(&mut alice, request_id, key(), text).await;
    }
    let (stop, mut input) = mpsc::channel(1);
    let (commands, receiver) = iam_client::command_channel(1);
    let mut transport = ObservedTls::new(&gateway);
    let (checkpoint, release) = transport.hold_position_ack(2);
    let (run_result, ()) = timeout(WAIT, async {
        tokio::join!(
            client.run_with_commands(
                &mut transport,
                BOB,
                Options::default(),
                &mut input,
                receiver
            ),
            async {
                let scope = current_scope(&mut view).await;
                let mut write = Box::pin(commands.advance_read_position(&scope, 2));
                start_pending(write.as_mut()).await;
                checkpoint.await.unwrap();
                // TiDB and an independent device already see 2 while this caller is still awaiting its ACK.
                assert_eq!(
                    other
                        .read_position(&mut gateway.client(), BOB_SECOND, &scope)
                        .await
                        .unwrap(),
                    Some(2)
                );
                assert_eq!(
                    other
                        .advance_read_position(&mut gateway.client(), BOB_SECOND, &scope, 3)
                        .await
                        .unwrap(),
                    3
                );
                let mut queued = Box::pin(commands.unread_count(&scope));
                start_pending(queued.as_mut()).await;
                release.send(ReplyDelivery::Lose).unwrap();
                assert!(
                    matches!(write.await, Err(iam_client::Error::CommandOutcomeUnknown(cause))
                if matches!(*cause, iam_client::Error::Transport(_)))
                );
                assert_eq!(queued.await.unwrap(), 0);
                // Retrying the original scope/position reads the actual maximum, not the lost ACK's 2.
                assert_eq!(commands.advance_read_position(&scope, 2).await.unwrap(), 3);
                assert_eq!(commands.read_position(&scope).await.unwrap(), Some(3));
                drop(stop);
            }
        )
    })
    .await
    .expect("client did not recover after the lost ACK");
    run_result.unwrap();
    transport.log.assert_connections(2);
    assert_eq!(transport.log.unread_queries, [2]);
    assert_eq!(
        transport.log.positions,
        [(1, Some(2)), (2, Some(2)), (2, None)]
    );
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_client_commands_canceled_waiters_do_not_misroute_replies_or_write_from_queue() {
    let fixture = Fixture::new().await;
    let gateway = fixture.start(OpenMode::Initialize, Limits::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let mut client = device_client(dir.path(), "bob.db", fixture.credentials()[1]).await;
    let mut view = client.open_view().await.unwrap();
    let mut alice = login(&gateway, fixture.alice, ALICE).await;
    send(&mut alice, 2, key(), "one").await;
    send(&mut alice, 3, key(), "two").await;
    let mut transport = ObservedTls::new(&gateway);
    let (checkpoint, release) = transport.hold_position_ack(1);
    let (stop, mut input) = mpsc::channel(1);
    let (commands, receiver) = iam_client::command_channel(1);
    let (run_result, ()) = timeout(WAIT, async {
        tokio::join!(
            client.run_with_commands(
                &mut transport,
                BOB,
                Options::default(),
                &mut input,
                receiver
            ),
            async {
                let scope = current_scope(&mut view).await;
                let mut inflight = Box::pin(commands.advance_read_position(&scope, 1));
                start_pending(inflight.as_mut()).await;
                checkpoint.await.unwrap();
                drop(inflight);
                let mut canceled = Box::pin(commands.advance_read_position(&scope, 2));
                start_pending(canceled.as_mut()).await; // occupies the one free queue slot
                let mut next = Box::pin(commands.unread_count(&scope));
                start_pending(next.as_mut()).await; // waits for capacity behind the canceled write
                drop(canceled);
                release.send(ReplyDelivery::Deliver).unwrap();
                assert_eq!(next.await.unwrap(), 1);
                assert_eq!(commands.read_position(&scope).await.unwrap(), Some(1));
                drop(stop);
            }
        )
    })
    .await
    .expect("canceled callers blocked the session");
    run_result.unwrap();
    transport.log.assert_connections(1);
    assert_eq!(transport.log.positions, [(1, Some(1)), (1, None)]);
    gateway.stop().await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_tls_client_commands_session_replaced_during_write_leaves_new_login_in_control() {
    let mut fixture = Fixture::new().await;
    let db = MySqlPool::connect(fixture.database.as_str()).await.unwrap();
    let proxy = CommitProxy::start(&fixture.database).await;
    fixture.database = proxy.url.clone();
    let gateway = fixture.start(OpenMode::Initialize, Limits::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let mut client = device_client(dir.path(), "bob.db", fixture.credentials()[1]).await;
    let mut view = client.open_view().await.unwrap();
    let mut alice = login(&gateway, fixture.alice, ALICE).await;
    send(&mut alice, 2, key(), "before device replacement").await;
    let mut transport = ObservedTls::new(&gateway);
    let (stop, mut input) = mpsc::channel(1);
    let (commands, receiver) = iam_client::command_channel(1);
    let exercise = async {
        let scope = current_scope(&mut view).await;
        // Wait until startup recovery has completed before intercepting a TiDB COMMIT.
        assert_eq!(commands.read_position(&scope).await.unwrap(), None);
        let (checkpoint, disconnect) = proxy.arm(CommitPhase::After);
        let mut write = Box::pin(commands.advance_read_position(&scope, 1));
        start_pending(write.as_mut()).await;
        checkpoint.await.unwrap().unwrap();
        let saved: u64 = sqlx::query_scalar("SELECT read_through FROM read_positions")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(saved, 1);
        let mut queued = Box::pin(commands.unread_count(&scope));
        start_pending(queued.as_mut()).await;
        let mut replacement = login(&gateway, fixture.bob, BOB).await;
        let error = write.await.unwrap_err();
        assert!(
            matches!(&error, iam_client::Error::CommandOutcomeUnknown(cause)
            if matches!(cause.as_ref(), iam_client::Error::Server(info) if info.code() == ErrorCode::SessionReplaced)),
            "{error:?}"
        );
        assert!(matches!(
            queued.await,
            Err(iam_client::Error::CommandsClosed)
        ));
        disconnect.send(()).unwrap();
        let query = ReadPositionRequest {
            conversation_id: scope.conversation_id,
            history_id: scope.history_id,
            access_version: scope.access_version,
            read_through: None,
        };
        let reply = request(&mut replacement, 2, MsgType::ReadPosition, &query).await;
        assert!(
            matches!(&reply, DecodedBody::ReadPositionAck(ack) if ack.read_through == Some(1)),
            "{reply:?}"
        );
        drop(stop);
    };
    let run = client.run_with_commands(
        &mut transport,
        BOB,
        Options::default(),
        &mut input,
        receiver,
    );
    let (result, ()) = timeout(WAIT, async { tokio::join!(run, exercise) })
        .await
        .expect("replaced session did not stop");
    assert_error(result, ErrorCode::SessionReplaced);
    transport.log.assert_connections(1);
    assert!(transport.log.unread_queries.is_empty());
    gateway.stop().await;
}
