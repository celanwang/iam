use std::sync::atomic::{AtomicU64, Ordering};
use std::{cell::RefCell, future::Future, time::Duration};

#[derive(Default)]
pub(crate) struct ClockCounters {
    attempted: AtomicU64,
    committed: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
pub struct ClockCounts {
    pub attempted: u64,
    pub committed: u64,
}

impl ClockCounters {
    pub(crate) fn attempt(&self) {
        self.attempted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn commit(&self) {
        self.committed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> ClockCounts {
        ClockCounts {
            attempted: self.attempted.load(Ordering::Relaxed),
            committed: self.committed.load(Ordering::Relaxed),
        }
    }
}

pub type Span = (&'static str, Duration);

tokio::task_local! {
    static SPANS: RefCell<Vec<Span>>;
}

pub async fn capture<T>(work: impl Future<Output = T>) -> (T, Vec<Span>) {
    SPANS
        .scope(RefCell::default(), async {
            let result = work.await;
            let spans = SPANS.with(|spans| spans.take());
            (result, spans)
        })
        .await
}

pub(crate) fn record(name: &'static str, elapsed: Duration) {
    let _ = SPANS.try_with(|spans| spans.borrow_mut().push((name, elapsed)));
}
