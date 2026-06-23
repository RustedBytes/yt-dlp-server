use std::{
    collections::HashMap,
    io::ErrorKind,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use async_channel::Receiver;
use log::{debug, error, info, warn};
use serde::Serialize;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    config::Config,
    downloader::download_url,
    state::{AppMetrics, AppState, WorkerPoolState},
    types::{DownloadMetadata, JobRecord, JobStatus, QueueResponse},
    util::{append_jsonl, hmac_sha256_hex},
};

#[derive(Debug, Clone)]
pub struct JobRequest {
    pub id: Uuid,
    pub url: String,
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
            .header("x-download-event-type", event.event_type)
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
        let mut last_error = None;

        for attempt in 1..=self.max_attempts {
            match self.send(&event, attempt).await {
                Ok(()) => {
                    return WebhookDeliveryResult::Delivered {
                        event_id: event.event_id,
                        attempts: attempt,
                    };
                }
                Err(err) => {
                    last_error = Some(err.to_string());
                    if attempt < self.max_attempts && !self.initial_backoff.is_zero() {
                        tokio::time::sleep(backoff_delay(self.initial_backoff, attempt)).await;
                    }
                }
            }
        }

        let error = last_error.unwrap_or_else(|| "webhook delivery failed".to_string());
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
}

#[derive(Debug, Clone, Serialize)]
pub struct WebhookEvent {
    pub event_id: Uuid,
    pub event_type: &'static str,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
    pub job: JobRecord,
}

impl WebhookEvent {
    fn from_job(job: JobRecord) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            event_type: "job.completed",
            created_at: time::OffsetDateTime::now_utc(),
            job,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct WebhookDeadLetter {
    pub event: WebhookEvent,
    pub attempts: usize,
    #[serde(with = "time::serde::rfc3339")]
    pub failed_at: time::OffsetDateTime,
    pub error: String,
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
    })
}

async fn persist_unqueued_record(
    results_jsonl: &Path,
    mut record: JobRecord,
    error: &str,
) -> Result<(), EnqueueError> {
    record.status = JobStatus::Failed;
    record.updated_at = time::OffsetDateTime::now_utc();
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
    queue_rx: Receiver<JobRequest>,
) {
    info!("starting download worker pool workers={}", config.workers);
    for worker_id in 0..config.workers {
        let config = Arc::clone(&config);
        let jobs = Arc::clone(&jobs);
        let workers = Arc::clone(&workers);
        let metrics = Arc::clone(&metrics);
        let webhooks = Arc::clone(&webhooks);
        let queue_rx = queue_rx.clone();
        workers.mark_ready();
        tokio::spawn(async move {
            while let Ok(request) = queue_rx.recv().await {
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
                metrics.record_job_started();
                let job_started = Instant::now();
                mark_running(&jobs, request.id).await;

                let result = download_url(&config, request.id, &request.url).await;
                let elapsed_ms = job_started.elapsed().as_millis();
                if is_timeout_error(&result) {
                    metrics.record_job_timed_out(elapsed_ms);
                } else if result.is_ok() {
                    metrics.record_job_succeeded(elapsed_ms);
                } else {
                    metrics.record_job_failed(elapsed_ms);
                }

                finish_job(&config, &jobs, &metrics, &webhooks, request.id, result).await;
            }
            workers.mark_stopped();
        });
    }
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
    result: anyhow::Result<DownloadMetadata>,
) {
    let mut final_record = None;
    {
        let mut jobs = jobs.write().await;
        if let Some(record) = jobs.get_mut(&id) {
            record.updated_at = time::OffsetDateTime::now_utc();
            match result {
                Ok(metadata) => {
                    record.status = JobStatus::Succeeded;
                    record.result = Some(metadata);
                    record.error = None;
                    info!("job succeeded job_id={}", id);
                }
                Err(err) => {
                    record.status = JobStatus::Failed;
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

fn is_timeout_error(result: &anyhow::Result<DownloadMetadata>) -> bool {
    result
        .as_ref()
        .err()
        .is_some_and(|err| err.to_string().contains("timed out after"))
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, path::PathBuf};

    use async_channel::bounded;
    use time::OffsetDateTime;

    use super::*;
    use crate::state::RateLimiter;

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
            max_urls_per_request: 100,
            job_timeout_seconds: 300,
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
            webhook_url: None,
            result: None,
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
            })
            .unwrap();
        let state = AppState {
            config: Arc::clone(&config),
            queue_tx,
            jobs: Arc::new(RwLock::new(HashMap::new())),
            workers: Arc::new(WorkerPoolState::new(1)),
            metrics: Arc::new(AppMetrics::default()),
            rate_limiter: Arc::new(RateLimiter::new(0)),
        };

        let err = enqueue_record(&state, job_record("https://www.instagram.com/reel/abc/"))
            .await
            .unwrap_err();

        assert!(matches!(err, EnqueueError::QueueFull));
        assert!(!config.submissions_jsonl.exists());

        drop(queue_rx);
        tokio::fs::remove_dir_all(&config.data_dir).await.unwrap();
    }

    #[test]
    fn timeout_errors_are_classified() {
        let result = Err(anyhow!("job timed out after 5 seconds"));

        assert!(is_timeout_error(&result));
    }

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("yt-dlp-server-{nanos}-{name}"))
    }
}
