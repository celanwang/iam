use crate::{store::TidbStore, Error, Result};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    time::{sleep_until, Instant},
};

pub(crate) const WINDOW: Duration = Duration::from_millis(1);
const MAX_BATCH: usize = 64;
const MAX_PENDING: usize = 128;

#[derive(Clone)]
pub(crate) struct ClockBatch {
    pending: mpsc::Sender<Waiter>,
    #[cfg(feature = "profile")]
    counts: Arc<crate::profile::ClockCounters>,
}

struct Waiter {
    queued_at: Instant,
    reply: oneshot::Sender<SampleResult>,
}

type SampleResult = std::result::Result<Sample, Arc<Error>>;

#[derive(Clone, Debug)]
struct Sample {
    now: i64,
    #[cfg(feature = "profile")]
    spans: Arc<[crate::profile::Span]>,
    #[cfg(test)]
    batch: u64,
    #[cfg(test)]
    size: usize,
    #[cfg(test)]
    transaction: Duration,
    #[cfg(test)]
    control_sql: Duration,
    #[cfg(any(test, feature = "profile"))]
    sealed_at: Instant,
}

impl ClockBatch {
    pub fn start(database: Arc<TidbStore>, window: Duration) -> Self {
        let (pending, requests) = mpsc::channel(MAX_PENDING);
        #[cfg(feature = "profile")]
        let counts = Arc::new(crate::profile::ClockCounters::default());
        #[cfg(feature = "profile")]
        let worker_counts = counts.clone();
        tokio::spawn(run(requests, window, move || {
            let database = database.clone();
            #[cfg(feature = "profile")]
            let counts = worker_counts.clone();
            async move {
                #[cfg(feature = "profile")]
                counts.attempt();
                let result = database.now().await;
                #[cfg(feature = "profile")]
                if result.is_ok() {
                    counts.commit();
                }
                result
            }
        }));
        Self {
            pending,
            #[cfg(feature = "profile")]
            counts,
        }
    }

    #[cfg(feature = "profile")]
    pub fn counts(&self) -> crate::profile::ClockCounts {
        self.counts.snapshot()
    }

    pub async fn now(&self) -> Result<i64> {
        let (reply, result) = oneshot::channel();
        let queued_at = Instant::now();
        self.pending
            .try_send(Waiter { queued_at, reply })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::ClockOverloaded,
                mpsc::error::TrySendError::Closed(_) => Error::ClockStopped,
            })?;
        let sample = result
            .await
            .map_err(|_| Error::ClockStopped)?
            .map_err(Error::ClockSample)?;
        #[cfg(test)]
        crate::store::perf::record_batch(
            sample.batch,
            sample.size,
            sample.transaction,
            sample.control_sql,
            queued_at.elapsed(),
            sample.sealed_at.saturating_duration_since(queued_at),
        );
        #[cfg(feature = "profile")]
        {
            crate::profile::record(
                "clock_queue",
                sample.sealed_at.saturating_duration_since(queued_at),
            );
            for &(name, elapsed) in sample.spans.iter() {
                crate::profile::record(name, elapsed);
            }
        }
        Ok(sample.now)
    }
}

async fn run<F: Future<Output = Result<i64>>>(
    mut requests: mpsc::Receiver<Waiter>,
    window: Duration,
    mut sample_now: impl FnMut() -> F,
) {
    #[cfg(test)]
    let mut batch_id = 0;
    while let Some(first) = requests.recv().await {
        let deadline = first.queued_at + window;
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH {
            // Include queued requests even if a previous transaction consumed the window.
            let next = match requests.try_recv() {
                Ok(waiter) => Some(waiter),
                Err(mpsc::error::TryRecvError::Disconnected) => None,
                Err(mpsc::error::TryRecvError::Empty) => tokio::select! {
                    biased;
                    _ = sleep_until(deadline) => break,
                    next = requests.recv() => next,
                },
            };
            match next {
                Some(waiter) => batch.push(waiter),
                None => break,
            }
        }
        batch.retain(|waiter| !waiter.reply.is_closed());
        if batch.is_empty() {
            continue;
        }
        // The batch is sealed before beginning the transaction. No response can reuse it later.
        #[cfg(any(test, feature = "profile"))]
        let sealed_at = Instant::now();
        let sample = sample_now();
        #[cfg(feature = "profile")]
        let sample = crate::profile::capture(sample);
        #[cfg(test)]
        let (result, spans) = crate::store::perf::capture(sample).await;
        #[cfg(not(test))]
        let result = sample.await;
        #[cfg(test)]
        let (transaction, control_sql) = {
            batch_id += 1;
            (spans.duration("clock_tx"), spans.duration("control_sql"))
        };
        #[cfg(feature = "profile")]
        let (result, profile_spans) = result;
        #[cfg(feature = "profile")]
        let profile_spans: Arc<[_]> = profile_spans.into();
        let result = result
            .map(|now| Sample {
                now,
                #[cfg(feature = "profile")]
                spans: profile_spans,
                #[cfg(test)]
                batch: batch_id,
                #[cfg(test)]
                size: batch.len(),
                #[cfg(test)]
                transaction,
                #[cfg(test)]
                control_sql,
                #[cfg(any(test, feature = "profile"))]
                sealed_at,
            })
            .map_err(Arc::new);
        for waiter in batch {
            let _ = waiter.reply.send(result.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[tokio::test]
    async fn requests_arriving_after_seal_wait_for_the_next_committed_sample() {
        let (pending, requests) = mpsc::channel(MAX_PENDING);
        let (started, mut batches) = mpsc::unbounded_channel();
        let (commit_first, first) = oneshot::channel::<i64>();
        let (commit_second, second) = oneshot::channel::<i64>();
        let mut replies = VecDeque::from([first, second]);
        let worker = tokio::spawn(run(requests, WINDOW, move || {
            started.send(()).unwrap();
            let reply = replies.pop_front().expect("exactly two clock transactions");
            async move { Ok(reply.await.unwrap()) }
        }));

        let submit = || {
            let (reply, result) = oneshot::channel();
            pending
                .try_send(Waiter {
                    queued_at: Instant::now(),
                    reply,
                })
                .expect("clock queue has capacity");
            result
        };
        let (mut a, mut b) = (submit(), submit());
        batches.recv().await.unwrap();
        let mut late = submit();
        assert!(matches!(
            a.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            b.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        commit_first.send(100).unwrap();
        let a = a.await.unwrap().unwrap();
        let b = b.await.unwrap().unwrap();
        assert_eq!((a.now, a.batch, a.size), (100, b.batch, 2));
        assert_eq!(b.now, 100);
        batches.recv().await.unwrap();
        assert!(matches!(
            late.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        commit_second.send(101).unwrap();
        let late = late.await.unwrap().unwrap();
        assert_eq!((late.now, late.batch, late.size), (101, a.batch + 1, 1));
        drop(pending);
        worker.await.unwrap();
    }
}
