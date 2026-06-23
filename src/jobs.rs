use std::{
    collections::HashMap,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use async_channel::Receiver;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{OwnedSemaphorePermit, RwLock, Semaphore},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    config::Config,
    downloader::{DownloadError, DownloadReport, download_url},
    platforms,
    state::{AppMetrics, AppState, CancellationRegistry, WorkerPoolState},
    types::{JobRecord, JobStatus, QueueResponse},
    util::{append_jsonl, hmac_sha256_hex},
};

#[derive(Debug, Clone)]
pub struct JobRequest {
    pub id: Uuid,
    pub url: String,
    pub format: Option<String>,
    pub cookie_profile: Option<String>,
}

pub struct WorkerRuntime {
    shutdown: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
}

#[derive(Clone, Default)]
struct PlatformConcurrencyLimiter {
    limits: Arc<HashMap<String, Arc<Semaphore>>>,
}

impl PlatformConcurrencyLimiter {
    fn from_config(config: &Config) -> Self {
        let limits = config
            .platform_policies
            .iter()
            .filter_map(|(platform, policy)| {
                policy
                    .max_concurrent
                    .map(|limit| (platform.clone(), Arc::new(Semaphore::new(limit))))
            })
            .collect();
        Self {
            limits: Arc::new(limits),
        }
    }

    async fn acquire(&self, url: &str) -> Option<OwnedSemaphorePermit> {
        let platform = platform_for_url(url)?;
        let semaphore = self.limits.get(platform)?;
        match Arc::clone(semaphore).acquire_owned().await {
            Ok(permit) => Some(permit),
            Err(err) => {
                warn!(
                    "platform concurrency limiter closed platform={} error={}",
                    platform, err
                );
                None
            }
        }
    }
}

impl WorkerRuntime {
    pub fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    pub async fn wait(self) {
        for handle in self.handles {
            if let Err(err) = handle.await {
                warn!("download worker task join failed error={}", err);
            }
        }
    }
}

#[derive(Clone)]
pub struct WebhookClient {
    http: reqwest::Client,
    max_attempts: usize,
    initial_backoff: Duration,
    signing_secret: Option<String>,
    dead_letter_jsonl: std::path::PathBuf,
}

impl WebhookClient {
    pub fn from_config(config: &Config) -> anyhow::Result<Self> {
        Self::new(
            config.webhook_timeout_seconds,
            config.webhook_connect_timeout_seconds,
            config.webhook_max_attempts,
            Duration::from_millis(config.webhook_initial_backoff_ms),
            config.webhook_signing_secret.clone(),
            config.webhooks_dead_letter_jsonl.clone(),
        )
    }

    fn new(
        timeout_seconds: u64,
        connect_timeout_seconds: u64,
        max_attempts: usize,
        initial_backoff: Duration,
        signing_secret: Option<String>,
        dead_letter_jsonl: std::path::PathBuf,
    ) -> anyhow::Result<Self> {
        let mut builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
        if timeout_seconds > 0 {
            builder = builder.timeout(Duration::from_secs(timeout_seconds));
        }
        if connect_timeout_seconds > 0 {
            builder = builder.connect_timeout(Duration::from_secs(connect_timeout_seconds));
        }

        let http = builder
            .build()
            .context("failed to build webhook HTTP client")?;

        Ok(Self {
            http,
            max_attempts: max_attempts.max(1),
            initial_backoff,
            signing_secret,
            dead_letter_jsonl,
        })
    }

    async fn send(&self, event: &WebhookEvent, attempt: usize) -> anyhow::Result<()> {
        let record = &event.job;
        let Some(webhook_url) = record.webhook_url.as_deref() else {
            return Ok(());
        };
        let redacted_url = redacted_webhook_url(webhook_url);
        let body = serde_json::to_vec(event).context("failed to serialize webhook event")?;

        let mut request = self
            .http
            .post(webhook_url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("x-download-event-id", event.event_id.to_string())
            .header("x-download-event-type", event.event_type.as_str())
            .header("x-download-delivery-attempt", attempt.to_string())
            .body(body.clone());
        if let Some(secret) = &self.signing_secret {
            request = request.header(
                "x-download-signature",
                format!("sha256={}", hmac_sha256_hex(secret, &body)),
            );
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("failed to send webhook request to {redacted_url}"))?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "webhook endpoint returned HTTP {}",
                response.status()
            ));
        }

        Ok(())
    }

    async fn deliver(&self, record: &JobRecord) -> WebhookDeliveryResult {
        if record.webhook_url.is_none() {
            return WebhookDeliveryResult::Skipped;
        }

        let event = WebhookEvent::from_job(record.clone());
        let error = match self.replay_event(&event).await {
            Ok(report) => {
                return WebhookDeliveryResult::Delivered {
                    event_id: report.event_id,
                    attempts: report.attempts,
                };
            }
            Err(err) => err.to_string(),
        };
        let dead_letter = WebhookDeadLetter {
            event,
            attempts: self.max_attempts,
            failed_at: time::OffsetDateTime::now_utc(),
            error: error.clone(),
        };
        match append_jsonl(&self.dead_letter_jsonl, &dead_letter).await {
            Ok(()) => WebhookDeliveryResult::Failed {
                event_id: dead_letter.event.event_id,
                attempts: dead_letter.attempts,
                error,
            },
            Err(err) => WebhookDeliveryResult::DeadLetterFailed {
                event_id: dead_letter.event.event_id,
                attempts: dead_letter.attempts,
                error,
                dead_letter_error: err.to_string(),
            },
        }
    }

    pub async fn replay_event(&self, event: &WebhookEvent) -> anyhow::Result<WebhookReplayReport> {
        if event.job.webhook_url.is_none() {
            anyhow::bail!("webhook event has no webhook_url");
        }

        let mut last_error = None;
        for attempt in 1..=self.max_attempts {
            match self.send(event, attempt).await {
                Ok(()) => {
                    return Ok(WebhookReplayReport {
                        event_id: event.event_id,
                        attempts: attempt,
                    });
                }
                Err(err) => {
                    last_error = Some(err);
                    if attempt < self.max_attempts && !self.initial_backoff.is_zero() {
                        tokio::time::sleep(backoff_delay(self.initial_backoff, attempt)).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("webhook delivery failed")))
    }

    pub async fn redeliver_job(&self, record: &JobRecord) -> anyhow::Result<WebhookReplayReport> {
        let event = WebhookEvent::from_job(record.clone());
        self.replay_event(&event).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEvent {
    pub event_id: Uuid,
    pub event_type: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
    pub job: JobRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_urls: Option<WebhookArtifactUrls>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookArtifactUrls {
    pub artifacts_url: String,
    pub media_url: String,
    pub media_inline_url: String,
    pub info_json_url: String,
    pub archive_url: String,
}

impl WebhookEvent {
    fn from_job(job: JobRecord) -> Self {
        let artifact_urls = job
            .result
            .as_ref()
            .map(|_| WebhookArtifactUrls::for_job(job.id));
        Self {
            event_id: Uuid::new_v4(),
            event_type: "job.completed".to_string(),
            created_at: time::OffsetDateTime::now_utc(),
            job,
            artifact_urls,
        }
    }
}

impl WebhookArtifactUrls {
    fn for_job(id: Uuid) -> Self {
        Self {
            artifacts_url: format!("/v1/jobs/{id}/artifacts"),
            media_url: format!("/v1/jobs/{id}/media"),
            media_inline_url: format!("/v1/jobs/{id}/media-inline"),
            info_json_url: format!("/v1/jobs/{id}/info-json"),
            archive_url: format!("/v1/jobs/{id}/archive"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookDeadLetter {
    pub event: WebhookEvent,
    pub attempts: usize,
    #[serde(with = "time::serde::rfc3339")]
    pub failed_at: time::OffsetDateTime,
    pub error: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WebhookReplayReport {
    pub event_id: Uuid,
    pub attempts: usize,
}

enum WebhookDeliveryResult {
    Skipped,
    Delivered {
        event_id: Uuid,
        attempts: usize,
    },
    Failed {
        event_id: Uuid,
        attempts: usize,
        error: String,
    },
    DeadLetterFailed {
        event_id: Uuid,
        attempts: usize,
        error: String,
        dead_letter_error: String,
    },
}

pub async fn load_jobs(config: &Config) -> anyhow::Result<HashMap<Uuid, JobRecord>> {
    let mut jobs = HashMap::new();
    load_job_records(&config.submissions_jsonl, &mut jobs).await?;
    load_job_records(&config.results_jsonl, &mut jobs).await?;

    let mut recovered = Vec::new();
    for record in jobs.values_mut() {
        if matches!(record.status, JobStatus::Queued | JobStatus::Running) {
            record.status = JobStatus::Failed;
            record.updated_at = time::OffsetDateTime::now_utc();
            record.error_kind = Some("interrupted".to_string());
            record.error = Some("server restarted before job reached a terminal state".to_string());
            recovered.push(record.clone());
        }
    }

    for record in recovered {
        append_jsonl(&config.results_jsonl, &record)
            .await
            .with_context(|| {
                format!(
                    "failed to append recovered job state to {}",
                    config.results_jsonl.display()
                )
            })?;
    }

    compact_metadata(config, &jobs).await?;
    retain_recent_jobs(&mut jobs, config.job_retention_limit);

    Ok(jobs)
}

pub async fn load_webhook_dead_letters(path: &Path) -> anyhow::Result<Vec<WebhookDeadLetter>> {
    let contents = match tokio::fs::read_to_string(path).await {
        Ok(contents) => contents,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to read {}", path.display()));
        }
    };

    let mut dead_letters = Vec::new();
    for (line_number, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let dead_letter = serde_json::from_str::<WebhookDeadLetter>(line).with_context(|| {
            format!(
                "failed to parse webhook dead letter at {}:{}",
                path.display(),
                line_number + 1
            )
        })?;
        dead_letters.push(dead_letter);
    }

    Ok(dead_letters)
}

pub async fn remove_webhook_dead_letter(path: &Path, event_id: Uuid) -> anyhow::Result<bool> {
    let dead_letters = load_webhook_dead_letters(path).await?;
    let original_len = dead_letters.len();
    let retained = dead_letters
        .into_iter()
        .filter(|dead_letter| dead_letter.event.event_id != event_id)
        .collect::<Vec<_>>();
    if retained.len() == original_len {
        return Ok(false);
    }

    write_webhook_dead_letters(path, &retained).await?;
    Ok(true)
}

async fn write_webhook_dead_letters(
    path: &Path,
    dead_letters: &[WebhookDeadLetter],
) -> anyhow::Result<()> {
    let temp_path = path.with_extension("jsonl.tmp");
    let mut bytes = Vec::new();
    for dead_letter in dead_letters {
        serde_json::to_writer(&mut bytes, dead_letter)?;
        bytes.push(b'\n');
    }
    tokio::fs::write(&temp_path, bytes)
        .await
        .with_context(|| format!("failed to write {}", temp_path.display()))?;
    tokio::fs::rename(&temp_path, path)
        .await
        .with_context(|| format!("failed to replace {}", path.display()))?;
    Ok(())
}

async fn load_job_records(path: &Path, jobs: &mut HashMap<Uuid, JobRecord>) -> anyhow::Result<()> {
    let contents = match tokio::fs::read_to_string(path).await {
        Ok(contents) => contents,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to read {}", path.display()));
        }
    };

    for (line_number, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let record = serde_json::from_str::<JobRecord>(line).with_context(|| {
            format!(
                "failed to parse job record at {}:{}",
                path.display(),
                line_number + 1
            )
        })?;
        jobs.insert(record.id, record);
    }

    Ok(())
}

async fn compact_metadata(config: &Config, jobs: &HashMap<Uuid, JobRecord>) -> anyhow::Result<()> {
    if config.metadata_retention_limit == 0 {
        return Ok(());
    }

    let records = recent_records(jobs, config.metadata_retention_limit);
    let (submissions, results): (Vec<_>, Vec<_>) = records
        .into_iter()
        .partition(|record| matches!(record.status, JobStatus::Queued | JobStatus::Running));

    write_jsonl_records(&config.submissions_jsonl, &submissions).await?;
    write_jsonl_records(&config.results_jsonl, &results).await?;
    Ok(())
}

async fn write_jsonl_records(path: &Path, records: &[JobRecord]) -> anyhow::Result<()> {
    let temp_path = path.with_extension("jsonl.tmp");
    let mut bytes = Vec::new();
    for record in records {
        serde_json::to_writer(&mut bytes, record)?;
        bytes.push(b'\n');
    }
    tokio::fs::write(&temp_path, bytes)
        .await
        .with_context(|| format!("failed to write {}", temp_path.display()))?;
    tokio::fs::rename(&temp_path, path)
        .await
        .with_context(|| format!("failed to replace {}", path.display()))?;
    Ok(())
}

fn retain_recent_jobs(jobs: &mut HashMap<Uuid, JobRecord>, limit: usize) {
    if jobs.len() <= limit {
        return;
    }
    if limit == 0 {
        jobs.clear();
        return;
    }

    let keep = recent_records(jobs, limit)
        .into_iter()
        .map(|record| record.id)
        .collect::<std::collections::HashSet<_>>();
    jobs.retain(|id, _| keep.contains(id));
}

fn recent_records(jobs: &HashMap<Uuid, JobRecord>, limit: usize) -> Vec<JobRecord> {
    let mut records = jobs.values().cloned().collect::<Vec<_>>();
    records.sort_by_key(|record| record.updated_at);
    records.reverse();
    records.truncate(limit);
    records.reverse();
    records
}

#[derive(Debug, thiserror::Error)]
pub enum EnqueueError {
    #[error("download queue is full")]
    QueueFull,
    #[error("download queue is closed")]
    QueueClosed,
    #[error(transparent)]
    Persist(#[from] anyhow::Error),
}

pub async fn enqueue_record(
    state: &AppState,
    record: JobRecord,
) -> Result<QueueResponse, EnqueueError> {
    let id = record.id;
    let request = JobRequest {
        id,
        url: record.url.clone(),
        format: record.format.clone(),
        cookie_profile: record.cookie_profile.clone(),
    };

    if state.queue_tx.is_full() {
        return Err(EnqueueError::QueueFull);
    }

    {
        let mut jobs = state.jobs.write().await;
        jobs.insert(id, record.clone());
    }

    append_jsonl(&state.config.submissions_jsonl, &record)
        .await
        .map_err(EnqueueError::Persist)?;

    match state.queue_tx.try_send(request) {
        Ok(()) => {}
        Err(async_channel::TrySendError::Full(_)) => {
            state.jobs.write().await.remove(&id);
            persist_unqueued_record(
                &state.config.results_jsonl,
                record,
                "download queue became full before the job could be queued",
            )
            .await?;
            return Err(EnqueueError::QueueFull);
        }
        Err(async_channel::TrySendError::Closed(_)) => {
            state.jobs.write().await.remove(&id);
            persist_unqueued_record(
                &state.config.results_jsonl,
                record,
                "download queue closed before the job could be queued",
            )
            .await?;
            return Err(EnqueueError::QueueClosed);
        }
    }

    info!(
        "job queued job_id={} url={} queued={}",
        id,
        record.url,
        state.queue_tx.len()
    );

    Ok(QueueResponse {
        id,
        status: JobStatus::Queued,
        status_url: format!("/v1/jobs/{id}"),
        existing: false,
    })
}

async fn persist_unqueued_record(
    results_jsonl: &Path,
    mut record: JobRecord,
    error: &str,
) -> Result<(), EnqueueError> {
    record.status = JobStatus::Failed;
    record.updated_at = time::OffsetDateTime::now_utc();
    record.error_kind = Some(classify_error(error).to_string());
    record.error = Some(error.to_string());
    append_jsonl(results_jsonl, &record)
        .await
        .map_err(EnqueueError::Persist)
}

pub fn start_workers(
    config: Arc<Config>,
    jobs: Arc<RwLock<HashMap<Uuid, JobRecord>>>,
    workers: Arc<WorkerPoolState>,
    metrics: Arc<AppMetrics>,
    webhooks: Arc<WebhookClient>,
    cancellations: Arc<CancellationRegistry>,
    queue_rx: Receiver<JobRequest>,
) -> WorkerRuntime {
    info!("starting download worker pool workers={}", config.workers);
    let shutdown = Arc::new(AtomicBool::new(false));
    let platform_limiter = PlatformConcurrencyLimiter::from_config(&config);
    let mut handles = Vec::with_capacity(config.workers);
    for worker_id in 0..config.workers {
        let config = Arc::clone(&config);
        let jobs = Arc::clone(&jobs);
        let workers = Arc::clone(&workers);
        let metrics = Arc::clone(&metrics);
        let webhooks = Arc::clone(&webhooks);
        let cancellations = Arc::clone(&cancellations);
        let queue_rx = queue_rx.clone();
        let shutdown = Arc::clone(&shutdown);
        let platform_limiter = platform_limiter.clone();
        workers.mark_ready();
        handles.push(tokio::spawn(async move {
            while let Ok(request) = queue_rx.recv().await {
                if shutdown.load(Ordering::Relaxed) {
                    debug!(
                        "worker stopping before starting queued job worker_id={} job_id={}",
                        worker_id, request.id
                    );
                    break;
                }
                let job_span = tracing::info_span!(
                    "download_job",
                    job_id = %request.id,
                    worker_id,
                    url = request.url.as_str(),
                );
                let _entered = job_span.enter();
                debug!(
                    "worker received job worker_id={} job_id={} url={}",
                    worker_id, request.id, request.url
                );
                let cancel_flag = cancellations.flag_for(request.id);
                if cancel_flag.load(std::sync::atomic::Ordering::Relaxed)
                    || is_job_canceled(&jobs, request.id).await
                {
                    info!("worker skipping canceled job job_id={}", request.id);
                    cancellations.remove(request.id);
                    continue;
                }

                let _platform_permit = platform_limiter.acquire(&request.url).await;
                if shutdown.load(Ordering::Relaxed) {
                    debug!(
                        "worker stopping after platform concurrency wait worker_id={} job_id={}",
                        worker_id, request.id
                    );
                    break;
                }
                if cancel_flag.load(std::sync::atomic::Ordering::Relaxed)
                    || is_job_canceled(&jobs, request.id).await
                {
                    info!(
                        "worker skipping canceled job after platform concurrency wait job_id={}",
                        request.id
                    );
                    cancellations.remove(request.id);
                    continue;
                }

                metrics.record_job_started();
                let job_started = Instant::now();
                workers.mark_active(worker_id, request.id, request.url.clone());
                mark_running(&jobs, request.id).await;

                let result = download_url(
                    &config,
                    request.id,
                    &request.url,
                    request.format.as_deref(),
                    request.cookie_profile.as_deref(),
                    cancel_flag,
                )
                .await;
                let elapsed_ms = job_started.elapsed().as_millis();
                if is_timeout_error(&result) {
                    metrics.record_job_timed_out(elapsed_ms);
                } else if result.is_ok() {
                    metrics.record_job_succeeded(elapsed_ms);
                } else {
                    metrics.record_job_failed(elapsed_ms);
                }

                finish_job(&config, &jobs, &metrics, &webhooks, request.id, result).await;
                workers.clear_active(worker_id);
                cancellations.remove(request.id);
            }
            workers.clear_active(worker_id);
            workers.mark_stopped();
        }));
    }

    WorkerRuntime { shutdown, handles }
}

async fn is_job_canceled(jobs: &RwLock<HashMap<Uuid, JobRecord>>, id: Uuid) -> bool {
    jobs.read()
        .await
        .get(&id)
        .is_some_and(|record| record.status == JobStatus::Canceled)
}

async fn mark_running(jobs: &RwLock<HashMap<Uuid, JobRecord>>, id: Uuid) {
    let mut jobs = jobs.write().await;
    if let Some(record) = jobs.get_mut(&id) {
        record.status = JobStatus::Running;
        record.updated_at = time::OffsetDateTime::now_utc();
        info!("job running job_id={} url={}", id, record.url);
    } else {
        warn!("job missing while marking running job_id={}", id);
    }
}

async fn finish_job(
    config: &Config,
    jobs: &RwLock<HashMap<Uuid, JobRecord>>,
    metrics: &AppMetrics,
    webhooks: &WebhookClient,
    id: Uuid,
    result: Result<DownloadReport, DownloadError>,
) {
    let mut final_record = None;
    {
        let mut jobs = jobs.write().await;
        if let Some(record) = jobs.get_mut(&id) {
            record.updated_at = time::OffsetDateTime::now_utc();
            match result {
                Ok(report) => {
                    record.status = JobStatus::Succeeded;
                    record.result = Some(report.metadata);
                    record.attempts = report.attempts;
                    record.attempt_errors = report.attempt_errors;
                    record.error_kind = None;
                    record.error = None;
                    info!("job succeeded job_id={}", id);
                }
                Err(err) => {
                    record.status = if is_canceled_error(&err) {
                        JobStatus::Canceled
                    } else {
                        JobStatus::Failed
                    };
                    record.attempts = err.attempts;
                    record.attempt_errors = err.attempt_errors.clone();
                    record.error_kind = Some(classify_error(&err.to_string()).to_string());
                    record.error = Some(err.to_string());
                    warn!("job failed job_id={} error={}", id, err);
                }
            }
            final_record = Some(record.clone());
        }
    }

    if let Some(record) = final_record {
        if let Err(err) = append_jsonl(&config.results_jsonl, &record).await {
            error!(
                "failed to append result metadata job_id={} path={} error={}",
                id,
                config.results_jsonl.display(),
                err
            );
        }
        send_webhook(metrics, webhooks, &record).await;
        enforce_download_storage_limit(config, jobs, metrics).await;
        {
            let mut jobs = jobs.write().await;
            retain_recent_jobs(&mut jobs, config.job_retention_limit);
        }
    } else {
        warn!("job missing while finishing job_id={}", id);
    }
}

async fn send_webhook(metrics: &AppMetrics, webhooks: &WebhookClient, record: &JobRecord) {
    let Some(webhook_url) = record.webhook_url.as_deref() else {
        return;
    };
    let redacted_url = redacted_webhook_url(webhook_url);

    match webhooks.deliver(record).await {
        WebhookDeliveryResult::Skipped => {}
        WebhookDeliveryResult::Delivered { event_id, attempts } => info!(
            "webhook delivered job_id={} event_id={} url={} attempts={}",
            record.id, event_id, redacted_url, attempts
        ),
        WebhookDeliveryResult::Failed {
            event_id,
            attempts,
            error,
        } => {
            metrics.record_webhook_failure();
            warn!(
                "webhook delivery failed job_id={} event_id={} url={} attempts={} error={}",
                record.id, event_id, redacted_url, attempts, error
            );
        }
        WebhookDeliveryResult::DeadLetterFailed {
            event_id,
            attempts,
            error,
            dead_letter_error,
        } => {
            metrics.record_webhook_failure();
            warn!(
                "webhook delivery failed and dead-letter append failed job_id={} event_id={} url={} attempts={} error={} dead_letter_error={}",
                record.id, event_id, redacted_url, attempts, error, dead_letter_error
            );
        }
    }
}

fn redacted_webhook_url(webhook_url: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(webhook_url) else {
        return "<invalid webhook url>".to_string();
    };
    url.set_query(None);
    url.set_fragment(None);
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.to_string()
}

fn backoff_delay(initial_backoff: Duration, attempt: usize) -> Duration {
    let multiplier = 1_u32.checked_shl((attempt - 1).min(16) as u32).unwrap_or(1);
    initial_backoff.saturating_mul(multiplier)
}

fn platform_for_url(url: &str) -> Option<&'static str> {
    let url = reqwest::Url::parse(url).ok()?;
    let host = url.host_str()?;
    platforms::platform_for_host(host)
}

fn is_timeout_error<T, E: std::fmt::Display>(result: &Result<T, E>) -> bool {
    result
        .as_ref()
        .err()
        .is_some_and(|err| err.to_string().contains("timed out after"))
}

fn is_canceled_error(error: &DownloadError) -> bool {
    error.to_string().contains("job canceled")
}

fn classify_error(error: &str) -> &'static str {
    let normalized = error.to_ascii_lowercase();
    if normalized.contains("job canceled") {
        "canceled"
    } else if normalized.contains("timed out") {
        "timeout"
    } else if normalized.contains("429")
        || normalized.contains("too many requests")
        || normalized.contains("rate limit")
    {
        "rate_limited"
    } else if normalized.contains("login")
        || normalized.contains("sign in")
        || normalized.contains("cookie")
        || normalized.contains("private")
    {
        "authentication_required"
    } else if normalized.contains("unsupported url") || normalized.contains("no suitable extractor")
    {
        "unsupported_url"
    } else if normalized.contains("yt-dlp failed") {
        "downloader_failed"
    } else {
        "download_failed"
    }
}

#[derive(Debug)]
struct DownloadStorageCandidate {
    id: Uuid,
    updated_at: time::OffsetDateTime,
    bytes: u64,
    directory: PathBuf,
}

async fn enforce_download_storage_limit(
    config: &Config,
    jobs: &RwLock<HashMap<Uuid, JobRecord>>,
    metrics: &AppMetrics,
) {
    if config.max_download_storage_bytes == 0 {
        return;
    }

    let mut candidates = {
        let jobs = jobs.read().await;
        jobs.values()
            .filter_map(|record| {
                if record.status != JobStatus::Succeeded {
                    return None;
                }
                let result = record.result.as_ref()?;
                let directory = result.media_path.parent()?.to_path_buf();
                if !directory.starts_with(&config.downloads_dir)
                    || directory == config.downloads_dir
                {
                    return None;
                }
                Some(DownloadStorageCandidate {
                    id: record.id,
                    updated_at: record.updated_at,
                    bytes: result.media_bytes,
                    directory,
                })
            })
            .collect::<Vec<_>>()
    };
    let mut total_bytes = candidates
        .iter()
        .map(|candidate| candidate.bytes)
        .sum::<u64>();
    if total_bytes <= config.max_download_storage_bytes {
        return;
    }

    candidates.sort_by_key(|candidate| candidate.updated_at);
    for candidate in candidates {
        if total_bytes <= config.max_download_storage_bytes {
            break;
        }

        match tokio::fs::remove_dir_all(&candidate.directory).await {
            Ok(()) => {
                total_bytes = total_bytes.saturating_sub(candidate.bytes);
                mark_job_deleted(
                    config,
                    jobs,
                    candidate.id,
                    "removed by storage retention limit",
                )
                .await;
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                total_bytes = total_bytes.saturating_sub(candidate.bytes);
                mark_job_deleted(config, jobs, candidate.id, "download directory was missing")
                    .await;
            }
            Err(err) => {
                metrics.record_cleanup_failure();
                warn!(
                    "failed to enforce storage limit job_id={} path={} error={}",
                    candidate.id,
                    candidate.directory.display(),
                    err
                );
            }
        }
    }
}

async fn mark_job_deleted(
    config: &Config,
    jobs: &RwLock<HashMap<Uuid, JobRecord>>,
    id: Uuid,
    reason: &str,
) {
    let mut record = None;
    {
        let mut jobs = jobs.write().await;
        if let Some(job) = jobs.get_mut(&id) {
            job.status = JobStatus::Deleted;
            job.updated_at = time::OffsetDateTime::now_utc();
            job.result = None;
            job.error_kind = Some("cleanup".to_string());
            job.error = Some(reason.to_string());
            record = Some(job.clone());
        }
    }
    if let Some(record) = record
        && let Err(err) = append_jsonl(&config.results_jsonl, &record).await
    {
        warn!(
            "failed to append storage cleanup tombstone job_id={} error={}",
            id, err
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, path::PathBuf};

    use async_channel::bounded;
    use time::OffsetDateTime;

    use super::*;
    use crate::{config::PlatformDownloadPolicy, state::RateLimiter, types::DownloadMetadata};

    fn test_config() -> Config {
        Config {
            addr: SocketAddr::from(([127, 0, 0, 1], 3000)),
            data_dir: PathBuf::from("data"),
            downloads_dir: PathBuf::from("data/downloads"),
            metadata_dir: PathBuf::from("data/metadata"),
            submissions_jsonl: PathBuf::from("data/metadata/download_submissions.jsonl"),
            results_jsonl: PathBuf::from("data/metadata/download_results.jsonl"),
            cors_allowed_origins: Vec::new(),
            api_keys: Vec::new(),
            rate_limit_requests_per_minute: 0,
            job_retention_limit: 1000,
            metadata_retention_limit: 10000,
            workers: 1,
            queue_size: 1,
            body_limit_bytes: 1024,
            request_timeout_seconds: 60,
            rust_log: "info".to_string(),
            yt_dlp_command: "uv".to_string(),
            cookies_path: None,
            cookie_profiles: Default::default(),
            format: None,
            proxy: None,
            platform_policies: Default::default(),
            download_enabled_platforms: crate::platforms::default_enabled_platforms(),
            max_urls_per_request: 100,
            job_timeout_seconds: 300,
            download_max_attempts: 1,
            download_initial_backoff_ms: 0,
            max_download_storage_bytes: 0,
            min_free_disk_bytes: 0,
            webhook_timeout_seconds: 10,
            webhook_connect_timeout_seconds: 5,
            webhook_max_attempts: 1,
            webhook_initial_backoff_ms: 500,
            webhook_signing_secret: None,
            webhooks_dead_letter_jsonl: PathBuf::from("data/metadata/webhooks_dead_letter.jsonl"),
            allow_private_webhook_urls: false,
        }
    }

    fn job_record(url: &str) -> JobRecord {
        let now = OffsetDateTime::now_utc();
        JobRecord {
            id: Uuid::new_v4(),
            status: JobStatus::Queued,
            created_at: now,
            updated_at: now,
            url: url.to_string(),
            url_sha256: None,
            webhook_url: None,
            format: None,
            cookie_profile: None,
            result: None,
            attempts: 0,
            attempt_errors: Vec::new(),
            error_kind: None,
            error: None,
        }
    }

    fn temp_config(name: &str) -> Config {
        let mut config = test_config();
        let dir = temp_dir(name);
        config.data_dir = dir.clone();
        config.downloads_dir = dir.join("downloads");
        config.metadata_dir = dir.join("metadata");
        config.submissions_jsonl = config.metadata_dir.join("download_submissions.jsonl");
        config.results_jsonl = config.metadata_dir.join("download_results.jsonl");
        config.webhooks_dead_letter_jsonl = config.metadata_dir.join("webhooks_dead_letter.jsonl");
        config
    }

    #[test]
    fn webhook_event_includes_stable_artifact_urls_when_job_has_result() {
        let mut record = job_record("https://www.instagram.com/reel/abc/");
        record.status = JobStatus::Succeeded;
        record.result = Some(DownloadMetadata {
            original_url: record.url.clone(),
            webpage_url: Some(record.url.clone()),
            extractor: Some("Instagram".to_string()),
            title: Some("example".to_string()),
            uploader: Some("creator".to_string()),
            duration: Some(12.5),
            extension: Some("mp4".to_string()),
            media_path: PathBuf::from(format!("data/downloads/{}/{}.mp4", record.id, record.id)),
            media_bytes: 5,
            info_json_path: PathBuf::from(format!(
                "data/downloads/{}/{}.info.json",
                record.id, record.id
            )),
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 123,
        });

        let event = WebhookEvent::from_job(record.clone());
        let artifact_urls = event.artifact_urls.as_ref().unwrap();
        let json = serde_json::to_value(&event).unwrap();

        assert_eq!(
            artifact_urls.artifacts_url,
            format!("/v1/jobs/{}/artifacts", record.id)
        );
        assert_eq!(
            artifact_urls.media_url,
            format!("/v1/jobs/{}/media", record.id)
        );
        assert_eq!(
            artifact_urls.media_inline_url,
            format!("/v1/jobs/{}/media-inline", record.id)
        );
        assert_eq!(
            artifact_urls.info_json_url,
            format!("/v1/jobs/{}/info-json", record.id)
        );
        assert_eq!(
            artifact_urls.archive_url,
            format!("/v1/jobs/{}/archive", record.id)
        );
        assert_eq!(
            json["artifact_urls"]["media_url"],
            format!("/v1/jobs/{}/media", record.id)
        );
    }

    #[test]
    fn webhook_event_omits_artifact_urls_when_job_has_no_result() {
        let event = WebhookEvent::from_job(job_record("https://www.instagram.com/reel/abc/"));
        let json = serde_json::to_value(&event).unwrap();

        assert!(event.artifact_urls.is_none());
        assert!(json.get("artifact_urls").is_none());
    }

    #[tokio::test]
    async fn load_jobs_recovers_incomplete_records() {
        let config = temp_config("recover");
        tokio::fs::create_dir_all(&config.metadata_dir)
            .await
            .unwrap();
        let record = job_record("https://www.tiktok.com/@user/video/123");
        append_jsonl(&config.submissions_jsonl, &record)
            .await
            .unwrap();

        let jobs = load_jobs(&config).await.unwrap();
        let recovered = jobs.get(&record.id).unwrap();

        assert_eq!(recovered.status, JobStatus::Failed);
        assert!(
            recovered
                .error
                .as_deref()
                .unwrap()
                .contains("server restarted")
        );

        tokio::fs::remove_dir_all(config.data_dir).await.unwrap();
    }

    #[tokio::test]
    async fn enqueue_record_rejects_full_queue_without_persisting() {
        let config = Arc::new(temp_config("queue-full"));
        tokio::fs::create_dir_all(&config.metadata_dir)
            .await
            .unwrap();
        let (queue_tx, queue_rx) = bounded(1);
        queue_tx
            .try_send(JobRequest {
                id: Uuid::new_v4(),
                url: "https://www.tiktok.com/@busy/video/1".to_string(),
                format: None,
                cookie_profile: None,
            })
            .unwrap();
        let state = AppState {
            config: Arc::clone(&config),
            queue_tx,
            jobs: Arc::new(RwLock::new(HashMap::new())),
            workers: Arc::new(WorkerPoolState::new(1)),
            metrics: Arc::new(AppMetrics::default()),
            rate_limiter: Arc::new(RateLimiter::new(0)),
            cancellations: Arc::new(CancellationRegistry::default()),
        };

        let err = enqueue_record(&state, job_record("https://www.instagram.com/reel/abc/"))
            .await
            .unwrap_err();

        assert!(matches!(err, EnqueueError::QueueFull));
        assert!(!config.submissions_jsonl.exists());

        drop(queue_rx);
        tokio::fs::remove_dir_all(&config.data_dir).await.unwrap();
    }

    #[tokio::test]
    async fn worker_runtime_shutdown_skips_buffered_jobs() {
        let config = Arc::new(temp_config("worker-shutdown"));
        tokio::fs::create_dir_all(&config.metadata_dir)
            .await
            .unwrap();
        let jobs = Arc::new(RwLock::new(HashMap::new()));
        let workers = Arc::new(WorkerPoolState::new(1));
        let metrics = Arc::new(AppMetrics::default());
        let webhooks = Arc::new(WebhookClient::from_config(&config).unwrap());
        let cancellations = Arc::new(CancellationRegistry::default());
        let (queue_tx, queue_rx) = bounded(1);
        let runtime = start_workers(
            Arc::clone(&config),
            Arc::clone(&jobs),
            Arc::clone(&workers),
            Arc::clone(&metrics),
            webhooks,
            cancellations,
            queue_rx,
        );
        let record = job_record("https://www.instagram.com/reel/abc/");
        jobs.write().await.insert(record.id, record.clone());

        runtime.request_shutdown();
        queue_tx
            .try_send(JobRequest {
                id: record.id,
                url: record.url.clone(),
                format: None,
                cookie_profile: None,
            })
            .unwrap();
        queue_tx.close();

        tokio::time::timeout(Duration::from_secs(1), runtime.wait())
            .await
            .unwrap();
        assert_eq!(workers.snapshot().ready, 0);
        assert_eq!(metrics.snapshot().jobs_started, 0);
        assert_eq!(
            jobs.read().await.get(&record.id).unwrap().status,
            JobStatus::Queued
        );

        tokio::fs::remove_dir_all(&config.data_dir).await.unwrap();
    }

    #[tokio::test]
    async fn platform_concurrency_limiter_blocks_same_platform_until_permit_released() {
        let mut config = test_config();
        config.platform_policies.insert(
            "instagram".to_string(),
            PlatformDownloadPolicy {
                cookies_path: None,
                format: None,
                proxy: None,
                job_timeout_seconds: None,
                download_max_attempts: None,
                download_initial_backoff_ms: None,
                max_concurrent: Some(1),
            },
        );
        let limiter = PlatformConcurrencyLimiter::from_config(&config);

        let first = limiter
            .acquire("https://www.instagram.com/reel/abc/")
            .await
            .expect("instagram should have a concurrency permit");
        let second = tokio::time::timeout(
            Duration::from_millis(25),
            limiter.acquire("https://www.instagram.com/reel/def/"),
        )
        .await;
        assert!(second.is_err());

        drop(first);
        let second = tokio::time::timeout(
            Duration::from_secs(1),
            limiter.acquire("https://www.instagram.com/reel/def/"),
        )
        .await
        .unwrap();
        assert!(second.is_some());
    }

    #[tokio::test]
    async fn storage_limit_deletes_oldest_completed_download() {
        let mut config = temp_config("storage-limit");
        config.max_download_storage_bytes = 5;
        tokio::fs::create_dir_all(&config.metadata_dir)
            .await
            .unwrap();
        let jobs = Arc::new(RwLock::new(HashMap::new()));
        let older = completed_record(&config, "older", 4).await;
        let newer = completed_record(&config, "newer", 4).await;
        {
            let mut jobs = jobs.write().await;
            jobs.insert(older.id, older.clone());
            jobs.insert(newer.id, newer.clone());
        }

        enforce_download_storage_limit(&config, &jobs, &AppMetrics::default()).await;

        let jobs = jobs.read().await;
        assert_eq!(jobs.get(&older.id).unwrap().status, JobStatus::Deleted);
        assert_eq!(jobs.get(&newer.id).unwrap().status, JobStatus::Succeeded);
        assert!(!config.downloads_dir.join(older.id.to_string()).exists());
        assert!(config.downloads_dir.join(newer.id.to_string()).exists());

        tokio::fs::remove_dir_all(&config.data_dir).await.unwrap();
    }

    #[test]
    fn timeout_errors_are_classified() {
        let result: Result<(), anyhow::Error> = Err(anyhow!("job timed out after 5 seconds"));

        assert!(is_timeout_error(&result));
    }

    #[test]
    fn classifies_common_download_errors() {
        assert_eq!(classify_error("job timed out after 5 seconds"), "timeout");
        assert_eq!(
            classify_error("HTTP Error 429: Too Many Requests"),
            "rate_limited"
        );
        assert_eq!(
            classify_error("please sign in or pass cookies"),
            "authentication_required"
        );
        assert_eq!(classify_error("Unsupported URL"), "unsupported_url");
        assert_eq!(
            classify_error("yt-dlp failed with status 1"),
            "downloader_failed"
        );
    }

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("yt-dlp-server-{nanos}-{name}"))
    }

    async fn completed_record(config: &Config, label: &str, bytes: u64) -> JobRecord {
        let mut record = job_record(&format!("https://www.youtube.com/shorts/{label}"));
        record.status = JobStatus::Succeeded;
        if label == "older" {
            record.updated_at -= time::Duration::seconds(60);
        }
        let directory = config.downloads_dir.join(record.id.to_string());
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let media_path = directory.join(format!("{}.mp4", record.id));
        let info_json_path = directory.join(format!("{}.info.json", record.id));
        tokio::fs::write(&media_path, vec![b'x'; bytes as usize])
            .await
            .unwrap();
        tokio::fs::write(&info_json_path, b"{}").await.unwrap();
        record.result = Some(DownloadMetadata {
            original_url: record.url.clone(),
            webpage_url: None,
            extractor: None,
            title: None,
            uploader: None,
            duration: None,
            extension: Some("mp4".to_string()),
            media_path,
            media_bytes: bytes,
            info_json_path,
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 1,
        });
        record
    }
}
