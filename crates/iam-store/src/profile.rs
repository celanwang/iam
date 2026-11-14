use std::{cell::RefCell, future::Future, time::Duration};

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
