//! TiDB key attribution and TiKV waiter histogram, sampled outside the application path.

use sqlx::{Connection, MySqlConnection};
use std::time::Duration;
use tokio::{sync::oneshot, task::JoinHandle};
use url::Url;

pub(super) struct ObservedWaits {
    samples: u64,
    nonempty: u64,
    peak: u64,
    other_nonempty: u64,
}

impl ObservedWaits {
    pub fn report(&self) {
        println!(
            "CONTROL_WAIT_VIEW polls={} nonempty={} peak={} other_nonempty={}",
            self.samples, self.nonempty, self.peak, self.other_nonempty
        );
    }
}

pub(super) async fn observe(database: Url) -> (oneshot::Sender<()>, JoinHandle<ObservedWaits>) {
    let (stop, mut stopped) = oneshot::channel();
    let mut db = MySqlConnection::connect(database.as_str()).await.unwrap();
    let schema = database.path().trim_start_matches('/').to_owned();
    let task = tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_millis(20));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut result = ObservedWaits {
            samples: 0,
            nonempty: 0,
            peak: 0,
            other_nonempty: 0,
        };
        loop {
            tokio::select! {
                biased;
                _ = &mut stopped => return result,
                _ = timer.tick() => {
                    let (count, total): (u64, u64) = sqlx::query_as(
                        "SELECT CAST(COUNT(CASE
                             WHEN JSON_UNQUOTE(JSON_EXTRACT(KEY_INFO, '$.db_name')) = ?
                              AND JSON_UNQUOTE(JSON_EXTRACT(KEY_INFO, '$.table_name')) = 'store_control'
                              AND JSON_UNQUOTE(JSON_EXTRACT(KEY_INFO, '$.handle_value')) = '1'
                             THEN 1 END) AS UNSIGNED), CAST(COUNT(*) AS UNSIGNED)
                         FROM information_schema.data_lock_waits",
                    ).bind(&schema).fetch_one(&mut db).await.unwrap();
                    result.samples += 1;
                    result.nonempty += u64::from(count != 0);
                    result.peak = result.peak.max(count);
                    result.other_nonempty += u64::from(total > count);
                }
            }
        }
    });
    (stop, task)
}

const HISTOGRAM: &str = "tikv_lock_manager_waiter_lifetime_duration";

pub(super) struct Histogram {
    count: u64,
    seconds: f64,
    buckets: Vec<(f64, u64)>,
}

/// Read the single-node TiKV histogram without publishing its metrics port.
pub(super) async fn scrape() -> Option<Histogram> {
    let container = std::env::var_os("IAM_TIDB_PERF_TIKV_CONTAINER")?;
    let output = tokio::process::Command::new("docker")
        .arg("exec")
        .arg(container)
        .args([
            "curl",
            "-fsS",
            "--max-time",
            "5",
            "http://127.0.0.1:20180/metrics",
        ])
        .output()
        .await
        .expect("docker exec to read TiKV metrics");
    assert!(
        output.status.success(),
        "failed to read the TiKV metrics endpoint"
    );

    let contents = std::str::from_utf8(&output.stdout).unwrap();
    let mut count = None;
    let mut seconds = None;
    let mut buckets = Vec::new();
    for line in contents
        .lines()
        .filter_map(|line| line.strip_prefix(HISTOGRAM))
    {
        if let Some(raw) = line.strip_prefix("_count ") {
            count = Some(raw.parse().unwrap());
        } else if let Some(raw) = line.strip_prefix("_sum ") {
            seconds = Some(raw.parse().unwrap());
        } else if let Some(raw) = line.strip_prefix("_bucket{le=\"") {
            let (bound, value) = raw.split_once("\"} ").unwrap();
            let bound = if bound == "+Inf" {
                f64::INFINITY
            } else {
                bound.parse().unwrap()
            };
            buckets.push((bound, value.parse().unwrap()));
        }
    }
    buckets.sort_by(|left, right| left.0.total_cmp(&right.0));
    assert!(!buckets.is_empty(), "TiKV waiter histogram unavailable");
    Some(Histogram {
        count: count.expect("TiKV waiter count"),
        seconds: seconds.expect("TiKV waiter duration"),
        buckets,
    })
}

impl Histogram {
    pub fn report_since(&self, before: &Self) {
        let count = self
            .count
            .checked_sub(before.count)
            .expect("waiter counter reset");
        let seconds = self.seconds - before.seconds;
        assert!(seconds >= 0.0, "waiter sum reset");
        if count == 0 {
            println!("TIKV_WAITERS n=0");
            return;
        }
        assert_eq!(self.buckets.len(), before.buckets.len());
        let delta: Vec<_> = self
            .buckets
            .iter()
            .zip(&before.buckets)
            .map(|(&(bound, after), &(old_bound, old))| {
                assert_eq!(bound, old_bound);
                (bound, after.checked_sub(old).expect("waiter bucket reset"))
            })
            .collect();
        let upper = |p: u64| {
            delta
                .iter()
                .find(|(_, cumulative)| *cumulative >= (count * p).div_ceil(100))
                .expect("waiter percentile bucket")
                .0
                * 1000.0
        };
        println!("TIKV_WAITERS n={count} total_ms={:.3} mean_ms={:.3} p95_upper_ms={:.3} p99_upper_ms={:.3}",
            seconds * 1000.0, seconds * 1000.0 / count as f64, upper(95), upper(99));
    }
}
