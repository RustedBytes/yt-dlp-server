use std::{
    collections::{HashMap, hash_map::Entry},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use async_channel::Sender;
use log::warn;
use time::OffsetDateTime;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{config::Config, jobs::JobRequest, types::JobRecord};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub queue_tx: Sender<JobRequest>,
    pub jobs: Arc<RwLock<HashMap<Uuid, JobRecord>>>,
    pub workers: Arc<WorkerPoolState>,
    pub metrics: Arc<AppMetrics>,
    pub rate_limiter: Arc<RateLimiter>,
    pub cancellations: Arc<CancellationRegistry>,
}

#[derive(Debug)]
pub struct WorkerPoolState {
    expected: usize,
    ready: AtomicUsize,
    failed: AtomicUsize,
    active: Mutex<HashMap<usize, WorkerActivity>>,
}

#[derive(Debug, Default)]
pub struct AppMetrics {
    jobs_started: AtomicUsize,
    jobs_succeeded: AtomicUsize,
    jobs_failed: AtomicUsize,
    jobs_timed_out: AtomicUsize,
    worker_restarts: AtomicUsize,
    total_download_ms: AtomicUsize,
    http_requests_total: AtomicUsize,
    http_requests_failed: AtomicUsize,
    total_request_ms: AtomicUsize,
    webhook_failures: AtomicUsize,
    cleanup_failures: AtomicUsize,
}

#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
    pub jobs_started: usize,
    pub jobs_succeeded: usize,
    pub jobs_failed: usize,
    pub jobs_timed_out: usize,
    pub worker_restarts: usize,
    pub total_download_ms: usize,
    pub http_requests_total: usize,
    pub http_requests_failed: usize,
    pub total_request_ms: usize,
    pub webhook_failures: usize,
    pub cleanup_failures: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct WorkerPoolSnapshot {
    pub expected: usize,
    pub ready: usize,
    pub failed: usize,
}

#[derive(Debug, Clone)]
pub struct WorkerActivitySnapshot {
    pub worker_id: usize,
    pub job_id: Uuid,
    pub url: String,
    pub started_at: OffsetDateTime,
    pub elapsed_ms: u128,
}

#[derive(Debug)]
pub struct RateLimiter {
    limit_per_minute: u64,
    windows: Mutex<HashMap<String, RateLimitWindow>>,
}

#[derive(Debug, Default)]
pub struct CancellationRegistry {
    flags: Mutex<HashMap<Uuid, Arc<AtomicBool>>>,
}

#[derive(Debug)]
struct RateLimitWindow {
    started_at: Instant,
    count: u64,
}

#[derive(Debug, Clone)]
struct WorkerActivity {
    job_id: Uuid,
    url: String,
    started_at: OffsetDateTime,
    started_instant: Instant,
}

impl WorkerPoolState {
    pub fn new(expected: usize) -> Self {
        Self {
            expected,
            ready: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
            active: Mutex::new(HashMap::new()),
        }
    }

    pub fn mark_ready(&self) {
        self.ready.fetch_add(1, Ordering::Relaxed);
    }

    pub fn mark_stopped(&self) {
        if self
            .ready
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |ready| {
                ready.checked_sub(1)
            })
            .is_err()
        {
            warn!("worker pool ready count was already zero while marking worker stopped");
        }
    }

    pub fn mark_active(&self, worker_id: usize, job_id: Uuid, url: String) {
        let mut active = lock_or_recover(&self.active, "worker activity");
        active.insert(
            worker_id,
            WorkerActivity {
                job_id,
                url,
                started_at: OffsetDateTime::now_utc(),
                started_instant: Instant::now(),
            },
        );
    }

    pub fn clear_active(&self, worker_id: usize) {
        let mut active = lock_or_recover(&self.active, "worker activity");
        active.remove(&worker_id);
    }

    pub fn active_snapshot(&self) -> Vec<WorkerActivitySnapshot> {
        let active = lock_or_recover(&self.active, "worker activity");
        let mut snapshot = active
            .iter()
            .map(|(worker_id, activity)| WorkerActivitySnapshot {
                worker_id: *worker_id,
                job_id: activity.job_id,
                url: activity.url.clone(),
                started_at: activity.started_at,
                elapsed_ms: activity.started_instant.elapsed().as_millis(),
            })
            .collect::<Vec<_>>();
        snapshot.sort_by_key(|activity| activity.worker_id);
        snapshot
    }

    pub fn snapshot(&self) -> WorkerPoolSnapshot {
        WorkerPoolSnapshot {
            expected: self.expected,
            ready: self.ready.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.snapshot().ready > 0
    }
}

impl RateLimiter {
    pub fn new(limit_per_minute: u64) -> Self {
        Self {
            limit_per_minute,
            windows: Mutex::new(HashMap::new()),
        }
    }

    pub fn check(&self, bucket: &str) -> RateLimitDecision {
        if self.limit_per_minute == 0 {
            return RateLimitDecision::Allowed;
        }

        let mut windows = lock_or_recover(&self.windows, "rate limiter");
        let now = Instant::now();
        let state = match windows.entry(bucket.to_string()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(RateLimitWindow {
                started_at: now,
                count: 0,
            }),
        };
        let elapsed = now.duration_since(state.started_at);
        if elapsed >= Duration::from_secs(60) {
            state.started_at = now;
            state.count = 0;
        }

        if state.count >= self.limit_per_minute {
            let retry_after = Duration::from_secs(60)
                .checked_sub(now.duration_since(state.started_at))
                .unwrap_or_default()
                .as_secs()
                .max(1);
            return RateLimitDecision::Limited { retry_after };
        }

        state.count += 1;
        RateLimitDecision::Allowed
    }
}

impl CancellationRegistry {
    pub fn flag_for(&self, id: Uuid) -> Arc<AtomicBool> {
        let mut flags = lock_or_recover(&self.flags, "cancellation registry");
        flags
            .entry(id)
            .or_insert_with(|| Arc::new(AtomicBool::new(false)))
            .clone()
    }

    pub fn cancel(&self, id: Uuid) {
        self.flag_for(id).store(true, Ordering::Relaxed);
    }

    pub fn cancel_all(&self) {
        let flags = lock_or_recover(&self.flags, "cancellation registry");
        for flag in flags.values() {
            flag.store(true, Ordering::Relaxed);
        }
    }

    pub fn remove(&self, id: Uuid) {
        let mut flags = lock_or_recover(&self.flags, "cancellation registry");
        flags.remove(&id);
    }
}

fn lock_or_recover<'a, T>(mutex: &'a Mutex<T>, name: &str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!("recovering poisoned mutex name={name}");
            poisoned.into_inner()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitDecision {
    Allowed,
    Limited { retry_after: u64 },
}

impl AppMetrics {
    pub fn record_http_request(&self, elapsed_ms: u128, failed: bool) {
        self.http_requests_total.fetch_add(1, Ordering::Relaxed);
        if failed {
            self.http_requests_failed.fetch_add(1, Ordering::Relaxed);
        }
        self.total_request_ms.fetch_add(
            usize::try_from(elapsed_ms).unwrap_or(usize::MAX),
            Ordering::Relaxed,
        );
    }

    pub fn record_job_started(&self) {
        self.jobs_started.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_job_succeeded(&self, elapsed_ms: u128) {
        self.jobs_succeeded.fetch_add(1, Ordering::Relaxed);
        self.total_download_ms.fetch_add(
            usize::try_from(elapsed_ms).unwrap_or(usize::MAX),
            Ordering::Relaxed,
        );
    }

    pub fn record_job_failed(&self, elapsed_ms: u128) {
        self.jobs_failed.fetch_add(1, Ordering::Relaxed);
        self.total_download_ms.fetch_add(
            usize::try_from(elapsed_ms).unwrap_or(usize::MAX),
            Ordering::Relaxed,
        );
    }

    pub fn record_job_timed_out(&self, elapsed_ms: u128) {
        self.jobs_timed_out.fetch_add(1, Ordering::Relaxed);
        self.record_job_failed(elapsed_ms);
    }

    pub fn record_webhook_failure(&self) {
        self.webhook_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cleanup_failure(&self) {
        self.cleanup_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            jobs_started: self.jobs_started.load(Ordering::Relaxed),
            jobs_succeeded: self.jobs_succeeded.load(Ordering::Relaxed),
            jobs_failed: self.jobs_failed.load(Ordering::Relaxed),
            jobs_timed_out: self.jobs_timed_out.load(Ordering::Relaxed),
            worker_restarts: self.worker_restarts.load(Ordering::Relaxed),
            total_download_ms: self.total_download_ms.load(Ordering::Relaxed),
            http_requests_total: self.http_requests_total.load(Ordering::Relaxed),
            http_requests_failed: self.http_requests_failed.load(Ordering::Relaxed),
            total_request_ms: self.total_request_ms.load(Ordering::Relaxed),
            webhook_failures: self.webhook_failures.load(Ordering::Relaxed),
            cleanup_failures: self.cleanup_failures.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limiter_is_disabled_with_zero_limit() {
        let limiter = RateLimiter::new(0);

        for _ in 0..10 {
            assert_eq!(limiter.check("global"), RateLimitDecision::Allowed);
        }
    }

    #[test]
    fn rate_limiter_rejects_after_window_limit() {
        let limiter = RateLimiter::new(1);

        assert_eq!(limiter.check("a"), RateLimitDecision::Allowed);
        assert!(matches!(
            limiter.check("a"),
            RateLimitDecision::Limited {
                retry_after: 1..=60
            }
        ));
        assert_eq!(limiter.check("b"), RateLimitDecision::Allowed);
    }

    #[test]
    fn cancellation_registry_can_cancel_all_active_flags() {
        let registry = CancellationRegistry::default();
        let first = uuid::Uuid::new_v4();
        let second = uuid::Uuid::new_v4();
        let first_flag = registry.flag_for(first);
        let second_flag = registry.flag_for(second);

        registry.cancel_all();

        assert!(first_flag.load(Ordering::Relaxed));
        assert!(second_flag.load(Ordering::Relaxed));
    }

    #[test]
    fn worker_activity_snapshot_reports_active_jobs_in_worker_order() {
        let workers = WorkerPoolState::new(2);
        let later_id = uuid::Uuid::new_v4();
        let first_id = uuid::Uuid::new_v4();

        workers.mark_active(
            1,
            later_id,
            "https://www.tiktok.com/@user/video/2".to_string(),
        );
        workers.mark_active(
            0,
            first_id,
            "https://www.instagram.com/reel/abc/".to_string(),
        );

        let active = workers.active_snapshot();

        assert_eq!(active.len(), 2);
        assert_eq!(active[0].worker_id, 0);
        assert_eq!(active[0].job_id, first_id);
        assert_eq!(active[1].worker_id, 1);
        assert_eq!(active[1].job_id, later_id);

        workers.clear_active(0);

        let active = workers.active_snapshot();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].worker_id, 1);
    }
}
