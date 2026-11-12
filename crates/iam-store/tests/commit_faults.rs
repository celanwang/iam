mod support;

#[path = "support/commit_proxy.rs"]
mod commit_proxy;

use commit_proxy::{CommitPhase, CommitProxy};
use iam_protocol::{
    generate_id128,
    pb::{MessagePosition, MessageRef, SendReceipt},
};
use sqlx::MySqlPool;
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    time::timeout,
};
use url::Url;

const TEXT: &str = "commit fault";
const CONVERSATION: &str = "FROM conversations WHERE tenant_id = 1 AND conversation_id = 1";

#[derive(Clone, Copy, Debug)]
enum Fault {
    Disconnect,
    KillProcess,
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_disconnect_before_commit() {
    check_fault(CommitPhase::Before, Fault::Disconnect).await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_disconnect_after_commit() {
    check_fault(CommitPhase::After, Fault::Disconnect).await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_process_crash_before_commit() {
    check_fault(CommitPhase::Before, Fault::KillProcess).await;
}

#[tokio::test]
#[ignore = "requires real local TiDB"]
async fn tidb_process_crash_after_commit() {
    check_fault(CommitPhase::After, Fault::KillProcess).await;
}

async fn check_fault(phase: CommitPhase, fault: Fault) {
    timeout(Duration::from_secs(30), exercise_fault(phase, fault))
        .await
        .unwrap_or_else(|_| panic!("{phase:?} COMMIT / {fault:?} timed out"));
}

async fn exercise_fault(phase: CommitPhase, fault: Fault) {
    let database = support::isolated_database().await;
    let observer = MySqlPool::connect(database.as_str()).await.unwrap();
    let proxy = CommitProxy::start(&database).await;
    let device = hex::encode(generate_id128());
    let key = generate_id128();
    let send = format!("send {} {TEXT}", hex::encode(key));
    let status = format!("status {}", hex::encode(key));
    let mut writer = Console::start(&proxy.url, "fresh", &device).await;
    assert!(writer
        .command("init")
        .await
        .ends_with("next=1 created=true"));

    let (reached, disconnect) = proxy.arm(phase);
    writer.write(&send).await;
    tokio::select! {
        result = reached => result.expect("proxy reached COMMIT").expect("commit checkpoint"),
        line = writer.response() => panic!("send returned before the fault: {line}"),
    }

    let checkpoint = DatabaseState::read(&observer).await;
    let receipt = match phase {
        CommitPhase::Before => {
            assert_eq!(checkpoint.next_sequence, 1);
            assert!(checkpoint.messages.is_empty());
            None
        }
        CommitPhase::After => {
            assert_eq!(checkpoint.next_sequence, 2);
            let [(client_id, message_id, sequence, accepted_at, text)] =
                checkpoint.messages.as_slice()
            else {
                panic!("expected exactly one committed message: {checkpoint:?}");
            };
            assert_eq!(client_id, &key);
            assert_eq!(*sequence, 1);
            assert_eq!(text, TEXT.as_bytes());
            Some(SendReceipt {
                r#ref: Some(MessageRef {
                    conversation_id: Some(1),
                    message_id: Some(message_id.clone().into()),
                }),
                position: Some(MessagePosition {
                    history_id: Some(checkpoint.history_id.clone().into()),
                    create_seq: Some(*sequence),
                }),
                accepted_at_ms: Some(*accepted_at),
            })
        }
    };
    let expected_status = match &receipt {
        Some(receipt) => format!("FOUND {receipt:?}"),
        None => "UNKNOWN (not a final rejection or permission to resend)".to_owned(),
    };

    match fault {
        Fault::Disconnect => {
            disconnect.send(()).unwrap();
            assert_eq!(
                writer.response().await,
                "attempt error: transaction outcome is unknown; query the original key"
            );
            assert_eq!(writer.command(&status).await, expected_status);
            let new_send = format!("send {} blocked", hex::encode(generate_id128()));
            assert_eq!(
                writer.command(&new_send).await,
                "attempt error: new acceptance is disabled; missing results remain unknown"
            );
            writer.quit().await;
        }
        Fault::KillProcess => {
            assert!(writer.child.try_wait().unwrap().is_none());
            writer.child.start_kill().unwrap();
            assert!(!writer.child.wait().await.unwrap().success());
            // Keep both COMMIT directions paused until the application is dead.
            disconnect.send(()).unwrap();
        }
    }

    // Wait for TiDB to release the original writer's row lock before checking
    // that the transaction left either all of its changes or none.
    let mut probe = observer.begin_with("BEGIN PESSIMISTIC").await.unwrap();
    let sequence: u64 =
        sqlx::query_scalar(&format!("SELECT next_sequence {CONVERSATION} FOR UPDATE"))
            .fetch_one(&mut *probe)
            .await
            .unwrap();
    assert_eq!(sequence, checkpoint.next_sequence);
    probe.rollback().await.unwrap();
    assert_eq!(DatabaseState::read(&observer).await, checkpoint);

    let mut restarted = Console::start(&database, "read-only", &device).await;
    assert_eq!(restarted.command(&status).await, expected_status);
    let expected_retry = match &receipt {
        Some(receipt) => format!("REPLAYED {receipt:?}"),
        None => "attempt error: new acceptance is disabled; missing results remain unknown".into(),
    };
    assert_eq!(restarted.command(&send).await, expected_retry);
    restarted.quit().await;
    assert_eq!(DatabaseState::read(&observer).await, checkpoint);
}

type MessageRow = (Vec<u8>, Vec<u8>, u64, i64, Vec<u8>);

#[derive(Debug, PartialEq)]
struct DatabaseState {
    history_id: Vec<u8>,
    next_sequence: u64,
    messages: Vec<MessageRow>,
}

impl DatabaseState {
    async fn read(pool: &MySqlPool) -> Self {
        let (history_id, next_sequence) =
            sqlx::query_as(&format!("SELECT history_id, next_sequence {CONVERSATION}"))
                .fetch_one(pool)
                .await
                .unwrap();
        let messages = sqlx::query_as(
            "SELECT client_msg_id, message_id, `sequence`, accepted_at, `text` FROM messages \
             WHERE tenant_id = 1 AND conversation_id = 1 ORDER BY `sequence`",
        )
        .fetch_all(pool)
        .await
        .unwrap();
        Self {
            history_id,
            next_sequence,
            messages,
        }
    }
}

struct Console {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    errors: Lines<BufReader<ChildStderr>>,
}

impl Console {
    async fn start(url: &Url, mode: &str, device: &str) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_iam-tidb-text"))
            .env("IAM_TIDB_URL", url.as_str())
            .env("IAM_TIDB_MODE", mode)
            .env("IAM_ACCESS_TOKEN", "fault-test")
            .env("IAM_TENANT_ID", "1")
            .env("IAM_USER_ID", "100")
            .env("IAM_CONVERSATION_ID", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("start iam-tidb-text");
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap()).lines();
        let errors = BufReader::new(child.stderr.take().unwrap()).lines();
        let mut console = Self {
            child,
            input,
            output,
            errors,
        };
        let banner = console.response().await;
        assert!(banner.starts_with("native text storage"), "{banner}");
        assert!(console.response().await.starts_with("commands:"));
        assert_eq!(
            console.command(&format!("auth fault-test {device}")).await,
            "authenticated"
        );
        console
    }

    async fn write(&mut self, command: &str) {
        self.input
            .write_all(format!("{command}\n").as_bytes())
            .await
            .unwrap();
    }

    async fn response(&mut self) -> String {
        tokio::select! {
            line = self.output.next_line() => line,
            line = self.errors.next_line() => line,
        }
        .unwrap()
        .expect("console exited while a response was pending")
    }

    async fn command(&mut self, command: &str) -> String {
        self.write(command).await;
        self.response().await
    }

    async fn quit(mut self) {
        self.write("quit").await;
        assert!(self.child.wait().await.unwrap().success());
    }
}
