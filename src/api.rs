use std::{
    collections::HashSet,
    future::Future,
    io::SeekFrom,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::Context;
use askama::Template;
use axum::{
    Form, Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path as AxumPath, Query, Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use log::{debug, info};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
    time::sleep,
};
use tokio_util::io::ReaderStream;
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    timeout::TimeoutLayer,
};
use uuid::Uuid;

use crate::{
    config::{Config, normalize_cookie_profile_name},
    downloader::check_downloader,
    jobs::{
        EnqueueError, WebhookClient, WebhookDeadLetter, enqueue_record, load_webhook_dead_letters,
        remove_webhook_dead_letter,
    },
    platforms,
    state::{AppState, RateLimitDecision},
    templates::{
        DeadLetterView, DeadLettersTemplate, IndexTemplate, JobAttemptView, JobDetailTemplate,
        JobDetailView, JobListItemView, JobListTemplate, JobResultView, PlatformFilterOption,
    },
    types::{
        BatchQueueResponse, DeleteJobResponse, ErrorResponse, HealthResponse, JobListResponse,
        JobRecord, JobStatus, MetricsResponse, QueueResponse, ReadinessResponse, WorkerHealth,
    },
    util::{append_jsonl, hex_lower, sha256_file_hex, sha256_hex},
};

const MAX_BATCH_JOB_ACTIONS: usize = 500;

pub fn router(state: AppState) -> Router {
    let cors_allowed_origins = state.config.cors_allowed_origins.clone();
    let request_timeout_seconds = state.config.request_timeout_seconds;
    let state_for_middleware = state.clone();
    let mut router = Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route("/ready", get(readiness))
        .route("/metrics", get(metrics))
        .route("/metrics.prometheus", get(metrics_prometheus))
        .route("/openapi.json", get(openapi))
        .route("/v1/config", get(runtime_config))
        .route("/v1/cookie-profiles", get(list_cookie_profiles))
        .route("/v1/platforms", get(list_platforms))
        .route("/v1/queue", get(queue_status))
        .route("/v1/workers", get(worker_status))
        .route(
            "/v1/storage/cleanup",
            get(preview_storage_cleanup).post(run_storage_cleanup),
        )
        .route("/v1/downloads/validate", post(validate_downloads))
        .route("/v1/downloads", post(submit_downloads))
        .route("/v1/webhooks/dead-letters", get(list_webhook_dead_letters))
        .route(
            "/v1/webhooks/dead-letters/{event_id}/replay",
            post(replay_webhook_dead_letter),
        )
        .route(
            "/v1/webhooks/dead-letters/{event_id}",
            delete(dismiss_webhook_dead_letter),
        )
        .route("/downloads-form", post(submit_downloads_form))
        .route("/webhooks/dead-letters", get(webhook_dead_letters_page))
        .route(
            "/webhooks/dead-letters/{event_id}/replay",
            post(replay_webhook_dead_letter_form),
        )
        .route(
            "/webhooks/dead-letters/{event_id}/dismiss",
            post(dismiss_webhook_dead_letter_form),
        )
        .route("/jobs/{id}", get(get_job_page))
        .route("/jobs", get(list_jobs_page))
        .route("/jobs-form/{id}/cancel", post(cancel_job_form))
        .route("/jobs-form/{id}/retry", post(retry_job_form))
        .route("/jobs-form/{id}/webhook", post(redeliver_job_webhook_form))
        .route("/jobs-form/{id}/delete", post(delete_job_form))
        .route("/v1/jobs", get(list_jobs))
        .route("/v1/jobs/summary", get(job_summary))
        .route("/v1/jobs/export", get(export_jobs))
        .route("/v1/jobs/batch/cancel", post(cancel_jobs_batch))
        .route("/v1/jobs/batch/retry", post(retry_jobs_batch))
        .route("/v1/jobs/batch/delete", post(delete_jobs_batch))
        .route("/v1/jobs/{id}", get(get_job))
        .route("/v1/jobs/{id}/wait", get(wait_for_job))
        .route("/v1/jobs/{id}/queue-position", get(get_job_queue_position))
        .route("/v1/jobs/{id}/cancel", post(cancel_job))
        .route("/v1/jobs/{id}/retry", post(retry_job))
        .route("/v1/jobs/{id}/webhook", post(redeliver_job_webhook))
        .route("/v1/jobs/{id}/artifacts", get(get_job_artifacts))
        .route(
            "/v1/jobs/{id}/media",
            get(get_job_media).head(head_job_media),
        )
        .route(
            "/v1/jobs/{id}/media-inline",
            get(get_job_media_inline).head(head_job_media_inline),
        )
        .route(
            "/v1/jobs/{id}/info-json",
            get(get_job_info_json).head(head_job_info_json),
        )
        .route(
            "/v1/jobs/{id}/archive",
            get(get_job_archive).head(head_job_archive),
        )
        .route("/v1/jobs/{id}", delete(delete_job))
        .layer(DefaultBodyLimit::max(state.config.body_limit_bytes))
        .layer(middleware::from_fn_with_state(
            state_for_middleware.clone(),
            rate_limit,
        ))
        .layer(middleware::from_fn_with_state(
            state_for_middleware.clone(),
            require_api_key,
        ))
        .with_state(state);

    if request_timeout_seconds > 0 {
        router = router.layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(request_timeout_seconds),
        ));
    }

    let router = if cors_allowed_origins.is_empty() {
        router
    } else {
        router.layer(cors_layer(&cors_allowed_origins))
    };

    router
        .layer(middleware::from_fn_with_state(
            state_for_middleware,
            track_request_metrics,
        ))
        .layer(middleware::from_fn(add_security_headers))
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("job not found")]
    NotFound,
    #[error("{0}")]
    Conflict(String),
    #[error("requested byte range is not satisfiable")]
    RangeNotSatisfiable { length: u64 },
    #[error("{0}")]
    ServiceUnavailable(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::NotFound => StatusCode::NOT_FOUND,
            ApiError::Conflict(_) => StatusCode::CONFLICT,
            ApiError::RangeNotSatisfiable { .. } => StatusCode::RANGE_NOT_SATISFIABLE,
            ApiError::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let content_range = match &self {
            ApiError::RangeNotSatisfiable { length } => Some(format!("bytes */{length}")),
            _ => None,
        };
        let body = Json(ErrorResponse {
            code: self.code().to_string(),
            message: self.to_string(),
        });
        let mut response = (status, body).into_response();
        if let Some(content_range) = content_range
            && let Ok(value) = HeaderValue::from_str(&content_range)
        {
            response.headers_mut().insert(header::CONTENT_RANGE, value);
        }
        response
    }
}

impl ApiError {
    fn code(&self) -> &'static str {
        match self {
            ApiError::BadRequest(_) => "bad_request",
            ApiError::NotFound => "not_found",
            ApiError::Conflict(_) => "conflict",
            ApiError::RangeNotSatisfiable { .. } => "range_not_satisfiable",
            ApiError::ServiceUnavailable(_) => "service_unavailable",
            ApiError::Internal(_) => "internal_error",
        }
    }
}

#[derive(Debug, Deserialize)]
struct DownloadRequest {
    urls: Vec<String>,
    webhook_url: Option<String>,
    format: Option<String>,
    cookie_profile: Option<String>,
    #[serde(default)]
    force: bool,
}

#[derive(Debug, Deserialize)]
struct DownloadForm {
    urls: String,
    webhook_url: Option<String>,
    format: Option<String>,
    cookie_profile: Option<String>,
    force: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BatchJobActionRequest {
    ids: Vec<Uuid>,
}

#[derive(Debug, Serialize)]
struct BatchJobActionResponse<T> {
    action: &'static str,
    succeeded: Vec<T>,
    failed: Vec<BatchJobActionError>,
}

#[derive(Debug, Serialize)]
struct BatchJobActionError {
    id: Uuid,
    code: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct RuntimeConfigResponse {
    server: RuntimeServerConfig,
    queue: RuntimeQueueConfig,
    download: RuntimeDownloadConfig,
    webhooks: RuntimeWebhookConfig,
    retention: RuntimeRetentionConfig,
}

#[derive(Debug, Serialize)]
struct RuntimeServerConfig {
    bind_addr: String,
    cors_allowed_origins: Vec<String>,
    api_key_auth_enabled: bool,
    rate_limit_requests_per_minute: u64,
}

#[derive(Debug, Serialize)]
struct RuntimeQueueConfig {
    queue_size: usize,
    body_limit_bytes: usize,
    request_timeout_seconds: u64,
}

#[derive(Debug, Serialize)]
struct RuntimeDownloadConfig {
    workers: usize,
    output_dir: String,
    yt_dlp_command: String,
    cookies_configured: bool,
    cookie_profiles: Vec<String>,
    format_configured: bool,
    proxy_configured: bool,
    enabled_platforms: Vec<String>,
    platform_policies: Vec<RuntimePlatformDownloadPolicy>,
    max_urls_per_request: usize,
    job_timeout_seconds: u64,
    max_attempts: usize,
    initial_backoff_ms: u64,
    max_storage_bytes: u64,
    min_free_disk_bytes: u64,
    post_processing_enabled: bool,
    post_processing_command_count: usize,
    object_storage_backend: String,
    object_storage_configured: bool,
    object_storage_public_urls: bool,
}

#[derive(Debug, Serialize)]
struct RuntimePlatformDownloadPolicy {
    platform: String,
    cookies_configured: bool,
    format_configured: bool,
    proxy_configured: bool,
    job_timeout_seconds: Option<u64>,
    max_attempts: Option<usize>,
    initial_backoff_ms: Option<u64>,
    max_concurrent: Option<usize>,
}

#[derive(Debug, Serialize)]
struct RuntimeWebhookConfig {
    timeout_seconds: u64,
    connect_timeout_seconds: u64,
    max_attempts: usize,
    initial_backoff_ms: u64,
    signing_enabled: bool,
    allow_private_webhook_urls: bool,
}

#[derive(Debug, Serialize)]
struct RuntimeRetentionConfig {
    job_retention_limit: usize,
    metadata_retention_limit: usize,
}

#[derive(Debug, Serialize)]
struct CookieProfileListResponse {
    profiles: Vec<CookieProfileResponse>,
}

#[derive(Debug, Serialize)]
struct CookieProfileResponse {
    name: String,
    configured: bool,
    last_job_id: Option<Uuid>,
    last_status: Option<JobStatus>,
    last_status_url: Option<String>,
    last_used_at: Option<String>,
    last_success_job_id: Option<Uuid>,
    last_success_at: Option<String>,
    last_failure_job_id: Option<Uuid>,
    last_failure_at: Option<String>,
    last_error_kind: Option<String>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ListJobsQuery {
    status: Option<String>,
    platform: Option<String>,
    q: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ExportJobsQuery {
    status: Option<String>,
    platform: Option<String>,
    q: Option<String>,
    format: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WaitJobQuery {
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ListDeadLettersQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct StorageCleanupQuery {
    max_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
struct WebhookDeadLetterListResponse {
    dead_letters: Vec<WebhookDeadLetter>,
    total: usize,
    limit: usize,
    offset: usize,
}

#[derive(Debug, Serialize)]
struct WebhookReplayResponse {
    event_id: Uuid,
    delivered: bool,
    attempts: usize,
    removed: bool,
}

#[derive(Debug, Serialize)]
struct WebhookDismissResponse {
    event_id: Uuid,
    removed: bool,
}

#[derive(Debug, Serialize)]
struct JobArtifactsResponse {
    id: Uuid,
    media_url: String,
    media_inline_url: String,
    info_json_url: String,
    archive_url: String,
    media_bytes: u64,
    media_sha256: String,
    media_content_type: String,
    info_json_sha256: String,
    archive_bytes: u64,
    archive_sha256: String,
    extension: Option<String>,
}

#[derive(Debug, Serialize)]
struct PlatformListResponse {
    platforms: Vec<PlatformResponse>,
}

#[derive(Debug, Serialize)]
struct PlatformResponse {
    id: &'static str,
    enabled: bool,
    hosts: &'static [&'static str],
    path_examples: &'static [&'static str],
}

#[derive(Debug, Serialize)]
struct QueueStatusResponse {
    queued: usize,
    capacity: usize,
    available_slots: usize,
    max_urls_per_request: usize,
    workers: WorkerHealth,
}

#[derive(Debug, Serialize)]
struct WorkerStatusResponse {
    workers: WorkerHealth,
    active: Vec<WorkerActivityResponse>,
}

#[derive(Debug, Serialize)]
struct WorkerActivityResponse {
    worker_id: usize,
    job_id: Uuid,
    status_url: String,
    url: String,
    #[serde(with = "time::serde::rfc3339")]
    started_at: OffsetDateTime,
    elapsed_ms: u128,
}

#[derive(Debug, Serialize)]
struct StorageCleanupResponse {
    dry_run: bool,
    max_bytes: Option<u64>,
    current_bytes: u64,
    bytes_to_delete: u64,
    bytes_after: u64,
    jobs_to_delete: Vec<StorageCleanupCandidateResponse>,
    deleted: Vec<DeleteJobResponse>,
    failed: Vec<BatchJobActionError>,
}

#[derive(Debug, Serialize)]
struct StorageCleanupCandidateResponse {
    id: Uuid,
    status_url: String,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
    media_bytes: u64,
    directory: String,
}

#[derive(Debug, Clone)]
struct StorageCleanupCandidate {
    id: Uuid,
    updated_at: OffsetDateTime,
    media_bytes: u64,
    directory: PathBuf,
}

#[derive(Debug, Serialize)]
struct ValidateDownloadsResponse {
    valid: bool,
    urls: Vec<String>,
    url_count: usize,
    max_urls_per_request: usize,
    webhook_url: Option<String>,
    format: Option<String>,
    cookie_profile: Option<String>,
    errors: Vec<ValidationErrorResponse>,
}

#[derive(Debug, Serialize)]
struct ValidationErrorResponse {
    field: &'static str,
    index: Option<usize>,
    value: Option<String>,
    message: String,
}

#[derive(Debug, Serialize)]
struct JobQueuePositionResponse {
    id: Uuid,
    status: JobStatus,
    position: usize,
    queued_count: usize,
}

#[derive(Debug, Serialize)]
struct JobSummaryResponse {
    total: usize,
    queued: usize,
    running: usize,
    succeeded: usize,
    failed: usize,
    canceled: usize,
    deleted: usize,
}

async fn index(State(state): State<AppState>) -> Result<Html<String>, ApiError> {
    render_index(&state, None, None, None).await
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let workers = worker_health(&state);
    debug!(
        "health check worker_ready={} workers={} ready_workers={} failed_workers={} queued={} output_dir={} yt_dlp_command={}",
        workers.ready > 0,
        workers.expected,
        workers.ready,
        workers.failed,
        state.queue_tx.len(),
        state.config.downloads_dir.display(),
        state.config.yt_dlp_command
    );

    Json(HealthResponse {
        status: "ok",
        ready: workers.ready > 0,
        workers,
        queued: state.queue_tx.len(),
        output_dir: state.config.downloads_dir.clone(),
        yt_dlp_command: state.config.yt_dlp_command.clone(),
    })
}

async fn readiness(State(state): State<AppState>) -> Response {
    let workers = worker_health(&state);
    let downloader_check = check_downloader(&state.config).await;
    let downloader_ready = downloader_check.is_ok();
    let ready = workers.ready > 0 && downloader_ready;
    let response = ReadinessResponse {
        status: if ready { "ready" } else { "not_ready" },
        ready,
        downloader_ready,
        downloader_error: downloader_check.err().map(|err| err.to_string()),
        workers,
        queued: state.queue_tx.len(),
    };
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status, Json(response)).into_response()
}

async fn metrics(State(state): State<AppState>) -> Json<MetricsResponse> {
    Json(metrics_snapshot(&state).await)
}

async fn metrics_prometheus(State(state): State<AppState>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        prometheus_metrics(&metrics_snapshot(&state).await),
    )
        .into_response()
}

async fn metrics_snapshot(state: &AppState) -> MetricsResponse {
    let workers = worker_health(state);
    let metrics = state.metrics.snapshot();
    let completed = metrics.jobs_succeeded + metrics.jobs_failed;
    let average_download_ms =
        (completed > 0).then_some(metrics.total_download_ms as f64 / completed as f64);
    let average_request_ms = (metrics.http_requests_total > 0)
        .then_some(metrics.total_request_ms as f64 / metrics.http_requests_total as f64);

    MetricsResponse {
        workers,
        queued: state.queue_tx.len(),
        http_requests_total: metrics.http_requests_total,
        http_requests_failed: metrics.http_requests_failed,
        total_request_ms: metrics.total_request_ms as u128,
        average_request_ms,
        jobs_started: metrics.jobs_started,
        jobs_succeeded: metrics.jobs_succeeded,
        jobs_failed: metrics.jobs_failed,
        jobs_timed_out: metrics.jobs_timed_out,
        worker_restarts: metrics.worker_restarts,
        total_download_ms: metrics.total_download_ms as u128,
        average_download_ms,
        webhook_failures: metrics.webhook_failures,
        cleanup_failures: metrics.cleanup_failures,
        retained_jobs: state.jobs.read().await.len(),
        process_memory_rss_bytes: process_memory_rss_bytes(),
    }
}

async fn openapi() -> Json<Value> {
    Json(openapi_document())
}

async fn runtime_config(State(state): State<AppState>) -> Json<RuntimeConfigResponse> {
    Json(runtime_config_response(&state.config))
}

fn runtime_config_response(config: &Config) -> RuntimeConfigResponse {
    RuntimeConfigResponse {
        server: RuntimeServerConfig {
            bind_addr: config.addr.to_string(),
            cors_allowed_origins: config.cors_allowed_origins.clone(),
            api_key_auth_enabled: !config.api_keys.is_empty(),
            rate_limit_requests_per_minute: config.rate_limit_requests_per_minute,
        },
        queue: RuntimeQueueConfig {
            queue_size: config.queue_size,
            body_limit_bytes: config.body_limit_bytes,
            request_timeout_seconds: config.request_timeout_seconds,
        },
        download: RuntimeDownloadConfig {
            workers: config.workers,
            output_dir: config.downloads_dir.display().to_string(),
            yt_dlp_command: config.yt_dlp_command.clone(),
            cookies_configured: config.cookies_path.is_some(),
            cookie_profiles: config.cookie_profiles.keys().cloned().collect(),
            format_configured: config.format.is_some(),
            proxy_configured: config.proxy.is_some(),
            enabled_platforms: config.download_enabled_platforms.clone(),
            platform_policies: config
                .platform_policies
                .iter()
                .map(|(platform, policy)| RuntimePlatformDownloadPolicy {
                    platform: platform.clone(),
                    cookies_configured: policy.cookies_path.is_some(),
                    format_configured: policy.format.is_some(),
                    proxy_configured: policy.proxy.is_some(),
                    job_timeout_seconds: policy.job_timeout_seconds,
                    max_attempts: policy.download_max_attempts,
                    initial_backoff_ms: policy.download_initial_backoff_ms,
                    max_concurrent: policy.max_concurrent,
                })
                .collect(),
            max_urls_per_request: config.max_urls_per_request,
            job_timeout_seconds: config.job_timeout_seconds,
            max_attempts: config.download_max_attempts,
            initial_backoff_ms: config.download_initial_backoff_ms,
            max_storage_bytes: config.max_download_storage_bytes,
            min_free_disk_bytes: config.min_free_disk_bytes,
            post_processing_enabled: config.post_processing.enabled,
            post_processing_command_count: config.post_processing.commands.len(),
            object_storage_backend: config.object_storage.backend.as_str().to_string(),
            object_storage_configured: config.object_storage.bucket.is_some(),
            object_storage_public_urls: config.object_storage.public_base_url.is_some(),
        },
        webhooks: RuntimeWebhookConfig {
            timeout_seconds: config.webhook_timeout_seconds,
            connect_timeout_seconds: config.webhook_connect_timeout_seconds,
            max_attempts: config.webhook_max_attempts,
            initial_backoff_ms: config.webhook_initial_backoff_ms,
            signing_enabled: config.webhook_signing_secret.is_some(),
            allow_private_webhook_urls: config.allow_private_webhook_urls,
        },
        retention: RuntimeRetentionConfig {
            job_retention_limit: config.job_retention_limit,
            metadata_retention_limit: config.metadata_retention_limit,
        },
    }
}

async fn list_cookie_profiles(State(state): State<AppState>) -> Json<CookieProfileListResponse> {
    let jobs = state.jobs.read().await;
    let profiles = state
        .config
        .cookie_profiles
        .keys()
        .map(|name| cookie_profile_response(name, jobs.values()))
        .collect();
    Json(CookieProfileListResponse { profiles })
}

fn cookie_profile_response<'a>(
    name: &str,
    jobs: impl Iterator<Item = &'a JobRecord>,
) -> CookieProfileResponse {
    let mut last_job: Option<&JobRecord> = None;
    let mut last_success: Option<&JobRecord> = None;
    let mut last_failure: Option<&JobRecord> = None;

    for job in jobs {
        if job.cookie_profile.as_deref() != Some(name) {
            continue;
        }
        if is_newer_job(job, last_job) {
            last_job = Some(job);
        }
        if job.status == JobStatus::Succeeded && is_newer_job(job, last_success) {
            last_success = Some(job);
        }
        if job.status == JobStatus::Failed && is_newer_job(job, last_failure) {
            last_failure = Some(job);
        }
    }

    CookieProfileResponse {
        name: name.to_string(),
        configured: true,
        last_job_id: last_job.map(|job| job.id),
        last_status: last_job.map(|job| job.status.clone()),
        last_status_url: last_job.map(|job| format!("/v1/jobs/{}", job.id)),
        last_used_at: last_job.map(|job| rfc3339(job.updated_at)),
        last_success_job_id: last_success.map(|job| job.id),
        last_success_at: last_success.map(|job| rfc3339(job.updated_at)),
        last_failure_job_id: last_failure.map(|job| job.id),
        last_failure_at: last_failure.map(|job| rfc3339(job.updated_at)),
        last_error_kind: last_failure.and_then(|job| job.error_kind.clone()),
        last_error: last_failure.and_then(|job| job.error.clone()),
    }
}

fn is_newer_job(job: &JobRecord, current: Option<&JobRecord>) -> bool {
    current.is_none_or(|current| {
        job.updated_at > current.updated_at
            || (job.updated_at == current.updated_at && job.id > current.id)
    })
}

async fn list_platforms(State(state): State<AppState>) -> Json<PlatformListResponse> {
    Json(PlatformListResponse {
        platforms: platforms::platform_definitions()
            .iter()
            .map(|platform| PlatformResponse {
                id: platform.id,
                enabled: platforms::is_platform_enabled(
                    platform.id,
                    &state.config.download_enabled_platforms,
                ),
                hosts: platform.hosts,
                path_examples: platform.path_examples,
            })
            .collect(),
    })
}

async fn queue_status(State(state): State<AppState>) -> Json<QueueStatusResponse> {
    let queued = state.queue_tx.len();
    Json(QueueStatusResponse {
        queued,
        capacity: state.config.queue_size,
        available_slots: state.config.queue_size.saturating_sub(queued),
        max_urls_per_request: state.config.max_urls_per_request,
        workers: worker_health(&state),
    })
}

async fn worker_status(State(state): State<AppState>) -> Json<WorkerStatusResponse> {
    Json(WorkerStatusResponse {
        workers: worker_health(&state),
        active: worker_activity_response(&state),
    })
}

async fn preview_storage_cleanup(
    State(state): State<AppState>,
    Query(query): Query<StorageCleanupQuery>,
) -> Result<Json<StorageCleanupResponse>, ApiError> {
    Ok(Json(
        storage_cleanup_response(&state, query.max_bytes, true).await?,
    ))
}

async fn run_storage_cleanup(
    State(state): State<AppState>,
    Query(query): Query<StorageCleanupQuery>,
) -> Result<Json<StorageCleanupResponse>, ApiError> {
    Ok(Json(
        storage_cleanup_response(&state, query.max_bytes, false).await?,
    ))
}

async fn validate_downloads(
    State(state): State<AppState>,
    Json(request): Json<DownloadRequest>,
) -> Json<ValidateDownloadsResponse> {
    Json(validate_download_request(
        &state.config,
        &request.urls,
        request.webhook_url,
        request.format,
        request.cookie_profile,
    ))
}

async fn submit_downloads(
    State(state): State<AppState>,
    Json(request): Json<DownloadRequest>,
) -> Result<Json<BatchQueueResponse>, ApiError> {
    Ok(Json(
        submit_download_jobs(
            &state,
            request.urls,
            request.webhook_url,
            request.format,
            request.cookie_profile,
            request.force,
        )
        .await?,
    ))
}

async fn submit_downloads_form(
    State(state): State<AppState>,
    Form(form): Form<DownloadForm>,
) -> Result<Html<String>, ApiError> {
    let urls = form.urls.lines().map(str::to_string).collect::<Vec<_>>();
    let force = form.force.is_some();
    match submit_download_jobs(
        &state,
        urls,
        form.webhook_url,
        form.format,
        form.cookie_profile,
        force,
    )
    .await
    {
        Ok(response) => render_index(&state, Some(response), None, None).await,
        Err(err) => render_index(&state, None, None, Some(err.to_string())).await,
    }
}

async fn submit_download_jobs(
    state: &AppState,
    urls: Vec<String>,
    webhook_url: Option<String>,
    format: Option<String>,
    cookie_profile: Option<String>,
    force: bool,
) -> Result<BatchQueueResponse, ApiError> {
    ensure_workers_ready(state)?;
    let urls = validate_download_urls(
        &urls,
        state.config.max_urls_per_request,
        &state.config.download_enabled_platforms,
    )?;
    let webhook_url = validate_webhook_url(&state.config, webhook_url)?;
    let format = validate_download_format(format)?;
    let cookie_profile = validate_cookie_profile(&state.config, cookie_profile)?;

    enum SubmitDecision {
        Existing(QueueResponse),
        Queue(String),
    }

    let mut decisions = Vec::with_capacity(urls.len());
    let mut urls_to_queue = 0_usize;
    for url in urls {
        if !force
            && let Some(existing) =
                reusable_successful_job(state, &url, &format, &cookie_profile).await
        {
            decisions.push(SubmitDecision::Existing(existing));
            continue;
        }
        urls_to_queue += 1;
        decisions.push(SubmitDecision::Queue(url));
    }

    let available_slots = state.config.queue_size.saturating_sub(state.queue_tx.len());
    if urls_to_queue > available_slots {
        return Err(ApiError::ServiceUnavailable(format!(
            "download queue does not have enough capacity: requested {} jobs, available {} slots",
            urls_to_queue, available_slots
        )));
    }

    let mut responses = Vec::with_capacity(decisions.len());
    for decision in decisions {
        match decision {
            SubmitDecision::Existing(response) => responses.push(response),
            SubmitDecision::Queue(url) => {
                responses.push(
                    queue_url_job(
                        state,
                        url,
                        webhook_url.clone(),
                        format.clone(),
                        cookie_profile.clone(),
                    )
                    .await?,
                );
            }
        }
    }

    Ok(BatchQueueResponse { jobs: responses })
}

async fn queue_url_job(
    state: &AppState,
    url: String,
    webhook_url: Option<String>,
    format: Option<String>,
    cookie_profile: Option<String>,
) -> Result<QueueResponse, ApiError> {
    let id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();
    let record = JobRecord {
        id,
        status: JobStatus::Queued,
        created_at: now,
        updated_at: now,
        url_sha256: Some(download_url_sha256(&url)),
        url,
        webhook_url,
        format,
        cookie_profile,
        result: None,
        attempts: 0,
        attempt_errors: Vec::new(),
        error_kind: None,
        error: None,
    };
    enqueue_record(state, record).await.map_err(ApiError::from)
}

async fn reusable_successful_job(
    state: &AppState,
    url: &str,
    format: &Option<String>,
    cookie_profile: &Option<String>,
) -> Option<QueueResponse> {
    let url_sha256 = download_url_sha256(url);
    state
        .jobs
        .read()
        .await
        .values()
        .filter(|record| {
            record.status == JobStatus::Succeeded
                && record.result.is_some()
                && record.url == url
                && record_url_sha256_matches(record, &url_sha256)
                && record.format == *format
                && record.cookie_profile == *cookie_profile
        })
        .max_by_key(|record| (record.updated_at, record.id))
        .map(|record| QueueResponse {
            id: record.id,
            status: record.status.clone(),
            status_url: format!("/v1/jobs/{}", record.id),
            existing: true,
        })
}

fn record_url_sha256_matches(record: &JobRecord, expected: &str) -> bool {
    match record.url_sha256.as_deref() {
        Some(actual) => actual == expected,
        None => download_url_sha256(&record.url) == expected,
    }
}

fn download_url_sha256(url: &str) -> String {
    sha256_hex(url.as_bytes())
}

impl From<EnqueueError> for ApiError {
    fn from(err: EnqueueError) -> Self {
        match err {
            EnqueueError::QueueFull => {
                ApiError::ServiceUnavailable("download queue is full".to_string())
            }
            EnqueueError::QueueClosed => {
                ApiError::ServiceUnavailable("download queue is closed".to_string())
            }
            EnqueueError::Persist(source) => ApiError::Internal(source),
        }
    }
}

async fn get_job(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Json<JobRecord>, ApiError> {
    let jobs = state.jobs.read().await;
    let record = jobs.get(&id).cloned().ok_or(ApiError::NotFound)?;
    debug!("job status read job_id={} status={:?}", id, record.status);
    Ok(Json(record))
}

async fn wait_for_job(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
    Query(query): Query<WaitJobQuery>,
) -> Result<Json<JobRecord>, ApiError> {
    Ok(Json(
        wait_for_job_record(&state, id, query.timeout_seconds).await?,
    ))
}

async fn get_job_page(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Html<String>, ApiError> {
    let record = job_record(&state, id).await?;
    render_job_detail(&state, record).await
}

async fn get_job_queue_position(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Json<JobQueuePositionResponse>, ApiError> {
    Ok(Json(job_queue_position(&state, id).await?))
}

async fn job_record(state: &AppState, id: Uuid) -> Result<JobRecord, ApiError> {
    let jobs = state.jobs.read().await;
    jobs.get(&id).cloned().ok_or(ApiError::NotFound)
}

async fn wait_for_job_record(
    state: &AppState,
    id: Uuid,
    timeout_seconds: Option<u64>,
) -> Result<JobRecord, ApiError> {
    let timeout = Duration::from_secs(timeout_seconds.unwrap_or(30).min(55));
    let poll_interval = Duration::from_millis(250);
    let deadline = Instant::now() + timeout;

    loop {
        let record = job_record(state, id).await?;
        if record.status.is_terminal() || Instant::now() >= deadline {
            return Ok(record);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        sleep(poll_interval.min(remaining)).await;
    }
}

async fn job_queue_position(
    state: &AppState,
    id: Uuid,
) -> Result<JobQueuePositionResponse, ApiError> {
    let jobs = state.jobs.read().await;
    let record = jobs.get(&id).cloned().ok_or(ApiError::NotFound)?;
    if record.status != JobStatus::Queued {
        return Err(ApiError::Conflict(format!(
            "job {id} is {}, not queued",
            record.status
        )));
    }

    let mut queued = jobs
        .values()
        .filter(|record| record.status == JobStatus::Queued)
        .collect::<Vec<_>>();
    queued.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.id.as_bytes().cmp(right.id.as_bytes()))
    });
    let position = queued
        .iter()
        .position(|record| record.id == id)
        .map(|position| position + 1)
        .ok_or_else(|| ApiError::Conflict(format!("job {id} is no longer queued")))?;

    Ok(JobQueuePositionResponse {
        id,
        status: JobStatus::Queued,
        position,
        queued_count: queued.len(),
    })
}

async fn list_jobs(
    State(state): State<AppState>,
    Query(query): Query<ListJobsQuery>,
) -> Result<Json<JobListResponse>, ApiError> {
    Ok(Json(list_job_records(&state, query).await?))
}

async fn export_jobs(
    State(state): State<AppState>,
    Query(query): Query<ExportJobsQuery>,
) -> Result<Response, ApiError> {
    let filters = JobRecordFilters::parse(query.status, query.platform, query.q)?;
    let records = collect_job_records(&state, &filters).await;
    let format = JobsExportFormat::parse(query.format.as_deref().unwrap_or("jsonl"))?;
    let (content_type, filename, body) = match format {
        JobsExportFormat::Jsonl => (
            "application/x-ndjson",
            "jobs.jsonl",
            export_jobs_jsonl(&records)?,
        ),
        JobsExportFormat::Csv => (
            "text/csv; charset=utf-8",
            "jobs.csv",
            export_jobs_csv(&records),
        ),
    };

    Ok((
        [
            (header::CONTENT_TYPE, content_type.to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
        ],
        Body::from(body),
    )
        .into_response())
}

async fn job_summary(State(state): State<AppState>) -> Json<JobSummaryResponse> {
    Json(current_job_summary(&state).await)
}

async fn current_job_summary(state: &AppState) -> JobSummaryResponse {
    let jobs = state.jobs.read().await;
    let mut summary = JobSummaryResponse {
        total: jobs.len(),
        queued: 0,
        running: 0,
        succeeded: 0,
        failed: 0,
        canceled: 0,
        deleted: 0,
    };
    for record in jobs.values() {
        match record.status {
            JobStatus::Queued => summary.queued += 1,
            JobStatus::Running => summary.running += 1,
            JobStatus::Succeeded => summary.succeeded += 1,
            JobStatus::Failed => summary.failed += 1,
            JobStatus::Canceled => summary.canceled += 1,
            JobStatus::Deleted => summary.deleted += 1,
        }
    }

    summary
}

async fn batch_job_action<T, F, Fut>(
    action: &'static str,
    ids: Vec<Uuid>,
    mut run: F,
) -> Result<BatchJobActionResponse<T>, ApiError>
where
    F: FnMut(Uuid) -> Fut,
    Fut: Future<Output = Result<T, ApiError>>,
{
    let ids = validate_batch_job_ids(ids)?;
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for id in ids {
        match run(id).await {
            Ok(response) => succeeded.push(response),
            Err(err) => failed.push(BatchJobActionError {
                id,
                code: err.code().to_string(),
                message: err.to_string(),
            }),
        }
    }

    Ok(BatchJobActionResponse {
        action,
        succeeded,
        failed,
    })
}

fn validate_batch_job_ids(ids: Vec<Uuid>) -> Result<Vec<Uuid>, ApiError> {
    if ids.is_empty() {
        return Err(ApiError::BadRequest(
            "ids must contain at least one job id".to_string(),
        ));
    }
    if ids.len() > MAX_BATCH_JOB_ACTIONS {
        return Err(ApiError::BadRequest(format!(
            "ids must contain at most {MAX_BATCH_JOB_ACTIONS} job ids"
        )));
    }

    let mut seen = HashSet::new();
    Ok(ids.into_iter().filter(|id| seen.insert(*id)).collect())
}

async fn list_jobs_page(
    State(state): State<AppState>,
    Query(query): Query<ListJobsQuery>,
) -> Result<Html<String>, ApiError> {
    render_job_list(&state, query).await
}

async fn list_job_records(
    state: &AppState,
    query: ListJobsQuery,
) -> Result<JobListResponse, ApiError> {
    let filters = JobRecordFilters::parse(query.status, query.platform, query.q)?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let offset = query.offset.unwrap_or(0);
    let records = collect_job_records(state, &filters).await;
    let total = records.len();
    let jobs = records.into_iter().skip(offset).take(limit).collect();

    Ok(JobListResponse {
        jobs,
        total,
        limit,
        offset,
    })
}

async fn collect_job_records(state: &AppState, filters: &JobRecordFilters) -> Vec<JobRecord> {
    let mut records = state
        .jobs
        .read()
        .await
        .values()
        .filter(|record| filters.matches(record))
        .cloned()
        .collect::<Vec<_>>();
    records.sort_by_key(|record| record.updated_at);
    records.reverse();
    records
}

async fn list_webhook_dead_letters(
    State(state): State<AppState>,
    Query(query): Query<ListDeadLettersQuery>,
) -> Result<Json<WebhookDeadLetterListResponse>, ApiError> {
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let offset = query.offset.unwrap_or(0);
    let mut dead_letters = load_webhook_dead_letters(&state.config.webhooks_dead_letter_jsonl)
        .await
        .map_err(ApiError::Internal)?;
    dead_letters.sort_by_key(|dead_letter| dead_letter.failed_at);
    dead_letters.reverse();
    let total = dead_letters.len();
    let dead_letters = dead_letters
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();

    Ok(Json(WebhookDeadLetterListResponse {
        dead_letters,
        total,
        limit,
        offset,
    }))
}

async fn webhook_dead_letters_page(
    State(state): State<AppState>,
) -> Result<Html<String>, ApiError> {
    render_dead_letters_page(&state, None, None).await
}

async fn replay_webhook_dead_letter(
    State(state): State<AppState>,
    AxumPath(event_id): AxumPath<Uuid>,
) -> Result<Json<WebhookReplayResponse>, ApiError> {
    Ok(Json(
        replay_webhook_dead_letter_record(&state, event_id).await?,
    ))
}

async fn replay_webhook_dead_letter_form(
    State(state): State<AppState>,
    AxumPath(event_id): AxumPath<Uuid>,
) -> Result<Html<String>, ApiError> {
    match replay_webhook_dead_letter_record(&state, event_id).await {
        Ok(response) => {
            render_dead_letters_page(
                &state,
                Some(format!(
                    "Replayed webhook {} in {} attempt{}",
                    response.event_id,
                    response.attempts,
                    if response.attempts == 1 { "" } else { "s" }
                )),
                None,
            )
            .await
        }
        Err(err) => render_dead_letters_page(&state, None, Some(err.to_string())).await,
    }
}

async fn dismiss_webhook_dead_letter(
    State(state): State<AppState>,
    AxumPath(event_id): AxumPath<Uuid>,
) -> Result<Json<WebhookDismissResponse>, ApiError> {
    Ok(Json(
        dismiss_webhook_dead_letter_record(&state, event_id).await?,
    ))
}

async fn dismiss_webhook_dead_letter_form(
    State(state): State<AppState>,
    AxumPath(event_id): AxumPath<Uuid>,
) -> Result<Html<String>, ApiError> {
    match dismiss_webhook_dead_letter_record(&state, event_id).await {
        Ok(response) => {
            render_dead_letters_page(
                &state,
                Some(format!(
                    "Dismissed webhook dead letter {}",
                    response.event_id
                )),
                None,
            )
            .await
        }
        Err(err) => render_dead_letters_page(&state, None, Some(err.to_string())).await,
    }
}

async fn dismiss_webhook_dead_letter_record(
    state: &AppState,
    event_id: Uuid,
) -> Result<WebhookDismissResponse, ApiError> {
    let removed = remove_webhook_dead_letter(&state.config.webhooks_dead_letter_jsonl, event_id)
        .await
        .map_err(ApiError::Internal)?;
    if !removed {
        return Err(ApiError::NotFound);
    }

    Ok(WebhookDismissResponse { event_id, removed })
}

async fn replay_webhook_dead_letter_record(
    state: &AppState,
    event_id: Uuid,
) -> Result<WebhookReplayResponse, ApiError> {
    let dead_letters = load_webhook_dead_letters(&state.config.webhooks_dead_letter_jsonl)
        .await
        .map_err(ApiError::Internal)?;
    let dead_letter = dead_letters
        .into_iter()
        .find(|dead_letter| dead_letter.event.event_id == event_id)
        .ok_or(ApiError::NotFound)?;
    let webhooks = WebhookClient::from_config(&state.config).map_err(ApiError::Internal)?;
    let report = webhooks
        .replay_event(&dead_letter.event)
        .await
        .map_err(|err| ApiError::ServiceUnavailable(err.to_string()))?;
    let removed = remove_webhook_dead_letter(&state.config.webhooks_dead_letter_jsonl, event_id)
        .await
        .map_err(ApiError::Internal)?;

    Ok(WebhookReplayResponse {
        event_id: report.event_id,
        delivered: true,
        attempts: report.attempts,
        removed,
    })
}

async fn redeliver_job_webhook(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Json<WebhookReplayResponse>, ApiError> {
    Ok(Json(redeliver_job_webhook_record(&state, id).await?))
}

async fn redeliver_job_webhook_record(
    state: &AppState,
    id: Uuid,
) -> Result<WebhookReplayResponse, ApiError> {
    let record = {
        let jobs = state.jobs.read().await;
        jobs.get(&id).cloned().ok_or(ApiError::NotFound)?
    };
    if matches!(record.status, JobStatus::Queued | JobStatus::Running) {
        return Err(ApiError::Conflict(format!(
            "job {id} is not in a terminal state"
        )));
    }
    if matches!(record.status, JobStatus::Deleted) {
        return Err(ApiError::Conflict(format!("job {id} has been deleted")));
    }
    if record.webhook_url.is_none() {
        return Err(ApiError::Conflict(format!("job {id} has no webhook_url")));
    }

    let webhooks = WebhookClient::from_config(&state.config).map_err(ApiError::Internal)?;
    let report = webhooks
        .redeliver_job(&record)
        .await
        .map_err(|err| ApiError::ServiceUnavailable(err.to_string()))?;

    Ok(WebhookReplayResponse {
        event_id: report.event_id,
        delivered: true,
        attempts: report.attempts,
        removed: false,
    })
}

async fn retry_job(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Json<QueueResponse>, ApiError> {
    Ok(Json(retry_job_record(&state, id).await?))
}

async fn retry_jobs_batch(
    State(state): State<AppState>,
    Json(request): Json<BatchJobActionRequest>,
) -> Result<Json<BatchJobActionResponse<QueueResponse>>, ApiError> {
    Ok(Json(
        batch_job_action("retry", request.ids, |id| {
            let state = state.clone();
            async move { retry_job_record(&state, id).await }
        })
        .await?,
    ))
}

async fn cancel_job(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Json<JobRecord>, ApiError> {
    Ok(Json(cancel_job_record(&state, id).await?))
}

async fn cancel_jobs_batch(
    State(state): State<AppState>,
    Json(request): Json<BatchJobActionRequest>,
) -> Result<Json<BatchJobActionResponse<JobRecord>>, ApiError> {
    Ok(Json(
        batch_job_action("cancel", request.ids, |id| {
            let state = state.clone();
            async move { cancel_job_record(&state, id).await }
        })
        .await?,
    ))
}

async fn cancel_job_form(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Html<String>, ApiError> {
    match cancel_job_record(&state, id).await {
        Ok(_) => render_index(&state, None, Some(format!("Canceled job {id}")), None).await,
        Err(err) => render_index(&state, None, None, Some(err.to_string())).await,
    }
}

async fn cancel_job_record(state: &AppState, id: Uuid) -> Result<JobRecord, ApiError> {
    let mut record = {
        let jobs = state.jobs.read().await;
        jobs.get(&id).cloned().ok_or(ApiError::NotFound)?
    };
    if !matches!(record.status, JobStatus::Queued | JobStatus::Running) {
        return Err(ApiError::Conflict(format!(
            "job {id} is not queued or running"
        )));
    }

    state.cancellations.cancel(id);
    record.status = JobStatus::Canceled;
    record.updated_at = OffsetDateTime::now_utc();
    record.result = None;
    record.error_kind = Some("canceled".to_string());
    record.error = Some("job canceled by request".to_string());
    {
        let mut jobs = state.jobs.write().await;
        jobs.insert(id, record.clone());
    }
    append_jsonl(&state.config.results_jsonl, &record)
        .await
        .map_err(ApiError::Internal)?;

    Ok(record)
}

async fn retry_job_form(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Html<String>, ApiError> {
    match retry_job_record(&state, id).await {
        Ok(response) => {
            render_index(
                &state,
                Some(BatchQueueResponse {
                    jobs: vec![response],
                }),
                Some(format!("Queued retry for job {id}")),
                None,
            )
            .await
        }
        Err(err) => render_index(&state, None, None, Some(err.to_string())).await,
    }
}

async fn redeliver_job_webhook_form(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Html<String>, ApiError> {
    match redeliver_job_webhook_record(&state, id).await {
        Ok(response) => {
            render_job_detail_with_notice(
                &state,
                job_record(&state, id).await?,
                Some(format!(
                    "Redelivered webhook {} in {} attempt{}",
                    response.event_id,
                    response.attempts,
                    if response.attempts == 1 { "" } else { "s" }
                )),
                None,
            )
            .await
        }
        Err(err) => {
            render_job_detail_with_notice(
                &state,
                job_record(&state, id).await?,
                None,
                Some(err.to_string()),
            )
            .await
        }
    }
}

async fn retry_job_record(state: &AppState, id: Uuid) -> Result<QueueResponse, ApiError> {
    ensure_workers_ready(state)?;
    if state.queue_tx.is_full() {
        return Err(ApiError::ServiceUnavailable(
            "download queue is full".to_string(),
        ));
    }

    let original = {
        let jobs = state.jobs.read().await;
        jobs.get(&id).cloned().ok_or(ApiError::NotFound)?
    };
    if matches!(original.status, JobStatus::Queued | JobStatus::Running) {
        return Err(ApiError::Conflict(format!(
            "job {id} is not in a terminal state"
        )));
    }
    if matches!(original.status, JobStatus::Deleted) {
        return Err(ApiError::Conflict(format!("job {id} has been deleted")));
    }

    queue_url_job(
        state,
        original.url,
        original.webhook_url,
        original.format,
        original.cookie_profile,
    )
    .await
}

async fn get_job_media(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    serve_job_media(&state, id, headers, MediaDisposition::Attachment, true).await
}

async fn get_job_media_inline(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    serve_job_media(&state, id, headers, MediaDisposition::Inline, true).await
}

async fn head_job_media(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    serve_job_media(&state, id, headers, MediaDisposition::Attachment, false).await
}

async fn head_job_media_inline(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    serve_job_media(&state, id, headers, MediaDisposition::Inline, false).await
}

async fn get_job_artifacts(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Json<JobArtifactsResponse>, ApiError> {
    let record = job_record(&state, id).await?;
    let Some(result) = record.result else {
        return Err(ApiError::Conflict(format!(
            "job {id} has no downloaded artifacts"
        )));
    };
    ensure_download_path(&state.config, &result.media_path)?;
    ensure_download_path(&state.config, &result.info_json_path)?;
    let media_sha256 = download_artifact_sha256(&result.media_path).await?;
    let info_json_sha256 = download_artifact_sha256(&result.info_json_path).await?;
    let media_name = archive_entry_name(&result.media_path, "download.bin");
    let info_json_name = archive_entry_name(&result.info_json_path, "info.json");
    let archive_entries = [
        TarFileEntry {
            name: &media_name,
            path: &result.media_path,
        },
        TarFileEntry {
            name: &info_json_name,
            path: &result.info_json_path,
        },
    ];
    let archive_bytes = tar_archive_file_len(&archive_entries).await?;
    let archive_sha256 = archive_sha256(&archive_entries).await?;

    Ok(Json(JobArtifactsResponse {
        id,
        media_url: format!("/v1/jobs/{id}/media"),
        media_inline_url: format!("/v1/jobs/{id}/media-inline"),
        info_json_url: format!("/v1/jobs/{id}/info-json"),
        archive_url: format!("/v1/jobs/{id}/archive"),
        media_bytes: result.media_bytes,
        media_sha256,
        media_content_type: content_type_for_path(&result.media_path),
        info_json_sha256,
        archive_bytes,
        archive_sha256,
        extension: result.extension,
    }))
}

async fn serve_job_media(
    state: &AppState,
    id: Uuid,
    headers: HeaderMap,
    disposition: MediaDisposition,
    include_body: bool,
) -> Result<Response, ApiError> {
    let record = {
        let jobs = state.jobs.read().await;
        jobs.get(&id).cloned().ok_or(ApiError::NotFound)?
    };
    let Some(result) = record.result else {
        return Err(ApiError::Conflict(format!(
            "job {id} has no downloaded media"
        )));
    };
    ensure_download_path(&state.config, &result.media_path)?;
    let mut media = open_download_artifact(&result.media_path).await?;
    let media_len = media_len(&media).await?;
    let range = parse_range_header(&headers, media_len)?;
    let filename = result
        .media_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("download.bin");
    let content_type = content_type_for_path(&result.media_path);
    let content_disposition = disposition.header_value(filename);
    let etag = artifact_etag(&result.media_path).await?;
    if if_none_match_matches(&headers, &etag) {
        return Ok(not_modified_response(etag));
    }
    let Some(range) = range else {
        let body = if include_body {
            Body::from_stream(ReaderStream::new(media))
        } else {
            Body::empty()
        };
        return Ok((
            [
                (header::CONTENT_TYPE, content_type),
                (header::CONTENT_DISPOSITION, content_disposition),
                (header::ACCEPT_RANGES, "bytes".to_string()),
                (header::CONTENT_LENGTH, media_len.to_string()),
                (header::ETAG, etag),
            ],
            body,
        )
            .into_response());
    };

    let body = if include_body {
        media
            .seek(SeekFrom::Start(range.start))
            .await
            .map_err(|err| ApiError::Internal(err.into()))?;
        Body::from_stream(ReaderStream::new(media.take(range.len())))
    } else {
        Body::empty()
    };
    Ok((
        StatusCode::PARTIAL_CONTENT,
        [
            (header::CONTENT_TYPE, content_type),
            (header::CONTENT_DISPOSITION, content_disposition),
            (header::ACCEPT_RANGES, "bytes".to_string()),
            (header::CONTENT_RANGE, range.content_range(media_len)),
            (header::CONTENT_LENGTH, range.len().to_string()),
            (header::ETAG, etag),
        ],
        body,
    )
        .into_response())
}

async fn get_job_info_json(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let record = {
        let jobs = state.jobs.read().await;
        jobs.get(&id).cloned().ok_or(ApiError::NotFound)?
    };
    let Some(result) = record.result else {
        return Err(ApiError::Conflict(format!(
            "job {id} has no downloaded info JSON"
        )));
    };
    ensure_download_path(&state.config, &result.info_json_path)?;
    let filename = result
        .info_json_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("info.json");
    let etag = artifact_etag(&result.info_json_path).await?;
    if if_none_match_matches(&headers, &etag) {
        return Ok(not_modified_response(etag));
    }
    let bytes = read_download_artifact(&result.info_json_path).await?;

    Ok((
        [
            (header::CONTENT_TYPE, "application/json".to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
            (header::CONTENT_LENGTH, bytes.len().to_string()),
            (header::ETAG, etag),
        ],
        Body::from(bytes),
    )
        .into_response())
}

async fn head_job_info_json(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let record = {
        let jobs = state.jobs.read().await;
        jobs.get(&id).cloned().ok_or(ApiError::NotFound)?
    };
    let Some(result) = record.result else {
        return Err(ApiError::Conflict(format!(
            "job {id} has no downloaded info JSON"
        )));
    };
    ensure_download_path(&state.config, &result.info_json_path)?;
    let info_json_len = download_artifact_len(&result.info_json_path).await?;
    let filename = result
        .info_json_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("info.json");
    let etag = artifact_etag(&result.info_json_path).await?;
    if if_none_match_matches(&headers, &etag) {
        return Ok(not_modified_response(etag));
    }

    Ok((
        [
            (header::CONTENT_TYPE, "application/json".to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
            (header::CONTENT_LENGTH, info_json_len.to_string()),
            (header::ETAG, etag),
        ],
        Body::empty(),
    )
        .into_response())
}

async fn get_job_archive(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    serve_job_archive(&state, id, headers, true).await
}

async fn head_job_archive(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    serve_job_archive(&state, id, headers, false).await
}

async fn serve_job_archive(
    state: &AppState,
    id: Uuid,
    headers: HeaderMap,
    include_body: bool,
) -> Result<Response, ApiError> {
    let record = {
        let jobs = state.jobs.read().await;
        jobs.get(&id).cloned().ok_or(ApiError::NotFound)?
    };
    let Some(result) = record.result else {
        return Err(ApiError::Conflict(format!(
            "job {id} has no downloaded artifacts"
        )));
    };
    ensure_download_path(&state.config, &result.media_path)?;
    ensure_download_path(&state.config, &result.info_json_path)?;

    let media_name = archive_entry_name(&result.media_path, "download.bin");
    let info_json_name = archive_entry_name(&result.info_json_path, "info.json");
    let archive_files = [
        TarFileEntry {
            name: &media_name,
            path: &result.media_path,
        },
        TarFileEntry {
            name: &info_json_name,
            path: &result.info_json_path,
        },
    ];
    let etag = archive_etag(&archive_files).await?;
    if if_none_match_matches(&headers, &etag) {
        return Ok(not_modified_response(etag));
    }
    let archive_len = tar_archive_file_len(&archive_files).await?;
    let range = parse_range_header(&headers, archive_len)?;

    let Some(range) = range else {
        let body = if include_body {
            let media_bytes = read_download_artifact(&result.media_path).await?;
            let info_json_bytes = read_download_artifact(&result.info_json_path).await?;
            let archive = build_tar_archive(&[
                TarEntry {
                    name: &media_name,
                    bytes: &media_bytes,
                },
                TarEntry {
                    name: &info_json_name,
                    bytes: &info_json_bytes,
                },
            ])
            .map_err(ApiError::Internal)?;
            Body::from(archive)
        } else {
            Body::empty()
        };

        return Ok((
            [
                (header::CONTENT_TYPE, "application/x-tar".to_string()),
                (
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{id}.tar\""),
                ),
                (header::ACCEPT_RANGES, "bytes".to_string()),
                (header::CONTENT_LENGTH, archive_len.to_string()),
                (header::ETAG, etag),
            ],
            body,
        )
            .into_response());
    };

    let body = if include_body {
        let media_bytes = read_download_artifact(&result.media_path).await?;
        let info_json_bytes = read_download_artifact(&result.info_json_path).await?;
        let archive = build_tar_archive(&[
            TarEntry {
                name: &media_name,
                bytes: &media_bytes,
            },
            TarEntry {
                name: &info_json_name,
                bytes: &info_json_bytes,
            },
        ])
        .map_err(ApiError::Internal)?;
        Body::from(archive[range.start as usize..=range.end as usize].to_vec())
    } else {
        Body::empty()
    };
    Ok((
        StatusCode::PARTIAL_CONTENT,
        [
            (header::CONTENT_TYPE, "application/x-tar".to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{id}.tar\""),
            ),
            (header::ACCEPT_RANGES, "bytes".to_string()),
            (header::CONTENT_RANGE, range.content_range(archive_len)),
            (header::CONTENT_LENGTH, range.len().to_string()),
            (header::ETAG, etag),
        ],
        body,
    )
        .into_response())
}

async fn delete_job(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Json<DeleteJobResponse>, ApiError> {
    Ok(Json(delete_job_record(&state, id).await?))
}

async fn delete_jobs_batch(
    State(state): State<AppState>,
    Json(request): Json<BatchJobActionRequest>,
) -> Result<Json<BatchJobActionResponse<DeleteJobResponse>>, ApiError> {
    Ok(Json(
        batch_job_action("delete", request.ids, |id| {
            let state = state.clone();
            async move { delete_job_record(&state, id).await }
        })
        .await?,
    ))
}

async fn delete_job_form(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Html<String>, ApiError> {
    match delete_job_record(&state, id).await {
        Ok(response) => {
            render_index(
                &state,
                None,
                Some(format!("Deleted job {}", response.id)),
                None,
            )
            .await
        }
        Err(err) => render_index(&state, None, None, Some(err.to_string())).await,
    }
}

async fn delete_job_record(state: &AppState, id: Uuid) -> Result<DeleteJobResponse, ApiError> {
    let mut record = {
        let jobs = state.jobs.read().await;
        jobs.get(&id).cloned().ok_or(ApiError::NotFound)?
    };
    if matches!(record.status, JobStatus::Queued | JobStatus::Running) {
        return Err(ApiError::Conflict(format!(
            "job {id} is not in a terminal state"
        )));
    }

    let media_deleted = delete_job_artifacts(&state.config, &record).await?;
    record.status = JobStatus::Deleted;
    record.updated_at = OffsetDateTime::now_utc();
    record.result = None;
    record.error_kind = None;
    record.error = None;
    {
        let mut jobs = state.jobs.write().await;
        jobs.insert(id, record.clone());
    }
    append_jsonl(&state.config.results_jsonl, &record)
        .await
        .map_err(ApiError::Internal)?;

    Ok(DeleteJobResponse {
        id,
        deleted: true,
        media_deleted,
    })
}

async fn storage_cleanup_response(
    state: &AppState,
    max_bytes_override: Option<u64>,
    dry_run: bool,
) -> Result<StorageCleanupResponse, ApiError> {
    let max_bytes = max_bytes_override.or_else(|| {
        (state.config.max_download_storage_bytes > 0)
            .then_some(state.config.max_download_storage_bytes)
    });
    let (current_bytes, jobs_to_delete) = storage_cleanup_plan(state, max_bytes).await;
    let bytes_to_delete = jobs_to_delete
        .iter()
        .map(|candidate| candidate.media_bytes)
        .sum::<u64>();
    let planned_bytes_after = current_bytes.saturating_sub(bytes_to_delete);
    let candidate_responses = jobs_to_delete
        .iter()
        .map(storage_cleanup_candidate_response)
        .collect::<Vec<_>>();

    if dry_run {
        return Ok(StorageCleanupResponse {
            dry_run,
            max_bytes,
            current_bytes,
            bytes_to_delete,
            bytes_after: planned_bytes_after,
            jobs_to_delete: candidate_responses,
            deleted: Vec::new(),
            failed: Vec::new(),
        });
    }

    let mut deleted = Vec::new();
    let mut failed = Vec::new();
    let mut deleted_bytes = 0_u64;
    for candidate in jobs_to_delete {
        match delete_job_record(state, candidate.id).await {
            Ok(response) => {
                deleted_bytes = deleted_bytes.saturating_add(candidate.media_bytes);
                deleted.push(response);
            }
            Err(err) => {
                state.metrics.record_cleanup_failure();
                failed.push(BatchJobActionError {
                    id: candidate.id,
                    code: err.code().to_string(),
                    message: err.to_string(),
                });
            }
        }
    }

    Ok(StorageCleanupResponse {
        dry_run,
        max_bytes,
        current_bytes,
        bytes_to_delete: deleted_bytes,
        bytes_after: current_bytes.saturating_sub(deleted_bytes),
        jobs_to_delete: candidate_responses,
        deleted,
        failed,
    })
}

async fn storage_cleanup_plan(
    state: &AppState,
    max_bytes: Option<u64>,
) -> (u64, Vec<StorageCleanupCandidate>) {
    let mut candidates = {
        let jobs = state.jobs.read().await;
        jobs.values()
            .filter_map(|record| storage_cleanup_candidate(&state.config, record))
            .collect::<Vec<_>>()
    };
    let current_bytes = candidates
        .iter()
        .map(|candidate| candidate.media_bytes)
        .sum::<u64>();
    let Some(max_bytes) = max_bytes else {
        return (current_bytes, Vec::new());
    };
    if current_bytes <= max_bytes {
        return (current_bytes, Vec::new());
    }

    candidates.sort_by_key(|candidate| candidate.updated_at);
    let mut remaining = current_bytes;
    let mut selected = Vec::new();
    for candidate in candidates {
        if remaining <= max_bytes {
            break;
        }
        remaining = remaining.saturating_sub(candidate.media_bytes);
        selected.push(candidate);
    }
    (current_bytes, selected)
}

fn storage_cleanup_candidate(
    config: &Config,
    record: &JobRecord,
) -> Option<StorageCleanupCandidate> {
    if record.status != JobStatus::Succeeded {
        return None;
    }
    let result = record.result.as_ref()?;
    let directory = result.media_path.parent()?.to_path_buf();
    if !directory.starts_with(&config.downloads_dir) || directory == config.downloads_dir {
        return None;
    }
    Some(StorageCleanupCandidate {
        id: record.id,
        updated_at: record.updated_at,
        media_bytes: result.media_bytes,
        directory,
    })
}

fn storage_cleanup_candidate_response(
    candidate: &StorageCleanupCandidate,
) -> StorageCleanupCandidateResponse {
    StorageCleanupCandidateResponse {
        id: candidate.id,
        status_url: format!("/v1/jobs/{}", candidate.id),
        updated_at: candidate.updated_at,
        media_bytes: candidate.media_bytes,
        directory: candidate.directory.display().to_string(),
    }
}

async fn track_request_metrics(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let content_length = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok());
    let started = Instant::now();

    debug!(
        "http request started method={} uri={} content_length={:?}",
        method, uri, content_length
    );

    let response = next.run(request).await;
    let elapsed = started.elapsed();
    let failed = response.status().is_client_error() || response.status().is_server_error();
    state
        .metrics
        .record_http_request(elapsed.as_millis(), failed);
    info!(
        "http request finished method={} uri={} status={} elapsed_ms={}",
        method,
        uri,
        response.status(),
        elapsed.as_millis()
    );

    response
}

async fn rate_limit(State(state): State<AppState>, request: Request, next: Next) -> Response {
    if is_probe_path(request.uri().path()) {
        return next.run(request).await;
    }

    let bucket = rate_limit_bucket(&state, &request);
    match state.rate_limiter.check(&bucket) {
        RateLimitDecision::Allowed => next.run(request).await,
        RateLimitDecision::Limited { retry_after } => {
            let mut response = json_error_response(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "request rate limit exceeded",
            );
            if let Ok(value) = HeaderValue::from_str(&retry_after.to_string()) {
                response.headers_mut().insert(header::RETRY_AFTER, value);
            }
            response
        }
    }
}

fn rate_limit_bucket(state: &AppState, request: &Request) -> String {
    if state.config.api_keys.is_empty() {
        return "anonymous".to_string();
    }

    request_api_key(request).unwrap_or_else(|| "unauthenticated".to_string())
}

async fn require_api_key(State(state): State<AppState>, request: Request, next: Next) -> Response {
    if state.config.api_keys.is_empty() || is_probe_path(request.uri().path()) {
        return next.run(request).await;
    }

    let presented = request_api_key(&request);
    if presented
        .as_deref()
        .is_some_and(|key| api_key_allowed(key, &state.config.api_keys))
    {
        return next.run(request).await;
    }

    let mut response = json_error_response(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "valid API key is required",
    );
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"yt-dlp-server\", Bearer"),
    );
    response
}

fn is_probe_path(path: &str) -> bool {
    matches!(path, "/health" | "/ready")
}

fn request_api_key(request: &Request) -> Option<String> {
    request
        .headers()
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| bearer_token(request))
        .or_else(|| basic_auth_api_key(request))
}

fn bearer_token(request: &Request) -> Option<String> {
    request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn basic_auth_api_key(request: &Request) -> Option<String> {
    let credentials = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Basic "))
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let decoded = BASE64_STANDARD.decode(credentials).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (_username, password) = decoded.split_once(':')?;
    let password = password.trim();
    (!password.is_empty()).then(|| password.to_string())
}

fn api_key_allowed(presented: &str, allowed: &[String]) -> bool {
    allowed
        .iter()
        .any(|expected| constant_time_eq(presented.as_bytes(), expected.as_bytes()))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for index in 0..max_len {
        let left_byte = left.get(index).copied().unwrap_or(0);
        let right_byte = right.get(index).copied().unwrap_or(0);
        diff |= usize::from(left_byte ^ right_byte);
    }
    diff == 0
}

async fn add_security_headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        HeaderName::from_static("referrer-policy"),
        HeaderValue::from_static("no-referrer"),
    );
    response
}

fn cors_layer(origins: &[String]) -> CorsLayer {
    let origins = origins
        .iter()
        .filter_map(|origin| origin.parse::<HeaderValue>().ok())
        .collect::<Vec<_>>();

    CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([
            header::CONTENT_TYPE,
            header::AUTHORIZATION,
            HeaderName::from_static("x-api-key"),
        ])
}

fn json_error_response(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(ErrorResponse {
            code: code.to_string(),
            message: message.to_string(),
        }),
    )
        .into_response()
}

fn worker_health(state: &AppState) -> WorkerHealth {
    let snapshot = state.workers.snapshot();
    WorkerHealth {
        expected: snapshot.expected,
        ready: snapshot.ready,
        failed: snapshot.failed,
    }
}

fn worker_activity_response(state: &AppState) -> Vec<WorkerActivityResponse> {
    state
        .workers
        .active_snapshot()
        .into_iter()
        .map(|activity| WorkerActivityResponse {
            worker_id: activity.worker_id,
            job_id: activity.job_id,
            status_url: format!("/v1/jobs/{}", activity.job_id),
            url: activity.url,
            started_at: activity.started_at,
            elapsed_ms: activity.elapsed_ms,
        })
        .collect()
}

fn rfc3339(value: OffsetDateTime) -> String {
    value
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| value.to_string())
}

fn validate_download_urls(
    values: &[String],
    max_urls: usize,
    enabled_platforms: &[String],
) -> Result<Vec<String>, ApiError> {
    let candidates = values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Err(ApiError::BadRequest(
            "at least one URL is required".to_string(),
        ));
    }
    if candidates.len() > max_urls {
        return Err(ApiError::BadRequest(format!(
            "too many URLs: got {}, maximum is {}",
            candidates.len(),
            max_urls
        )));
    }

    let mut seen = HashSet::new();
    let mut urls = Vec::new();
    for value in candidates {
        let url = validate_download_url(value, enabled_platforms)?;
        if seen.insert(url.clone()) {
            urls.push(url);
        }
    }
    Ok(urls)
}

fn validate_download_request(
    config: &Config,
    values: &[String],
    webhook_url: Option<String>,
    format: Option<String>,
    cookie_profile: Option<String>,
) -> ValidateDownloadsResponse {
    let candidates = values
        .iter()
        .enumerate()
        .filter_map(|(index, value)| {
            let value = value.trim();
            (!value.is_empty()).then_some((index, value))
        })
        .collect::<Vec<_>>();

    let mut errors = Vec::new();
    if candidates.is_empty() {
        errors.push(ValidationErrorResponse {
            field: "urls",
            index: None,
            value: None,
            message: "at least one URL is required".to_string(),
        });
    }
    if candidates.len() > config.max_urls_per_request {
        errors.push(ValidationErrorResponse {
            field: "urls",
            index: None,
            value: None,
            message: format!(
                "too many URLs: got {}, maximum is {}",
                candidates.len(),
                config.max_urls_per_request
            ),
        });
    }

    let mut seen = HashSet::new();
    let mut urls = Vec::new();
    for (index, value) in candidates {
        match validate_download_url(value, &config.download_enabled_platforms) {
            Ok(url) if seen.insert(url.clone()) => urls.push(url),
            Ok(_) => {}
            Err(err) => errors.push(ValidationErrorResponse {
                field: "urls",
                index: Some(index),
                value: Some(value.to_string()),
                message: err.to_string(),
            }),
        }
    }

    let webhook_url = match validate_webhook_url(config, webhook_url) {
        Ok(webhook_url) => webhook_url,
        Err(err) => {
            errors.push(ValidationErrorResponse {
                field: "webhook_url",
                index: None,
                value: None,
                message: err.to_string(),
            });
            None
        }
    };
    let format = match validate_download_format(format) {
        Ok(format) => format,
        Err(err) => {
            errors.push(ValidationErrorResponse {
                field: "format",
                index: None,
                value: None,
                message: err.to_string(),
            });
            None
        }
    };
    let cookie_profile = match validate_cookie_profile(config, cookie_profile) {
        Ok(cookie_profile) => cookie_profile,
        Err(err) => {
            errors.push(ValidationErrorResponse {
                field: "cookie_profile",
                index: None,
                value: None,
                message: err.to_string(),
            });
            None
        }
    };

    ValidateDownloadsResponse {
        valid: errors.is_empty(),
        url_count: urls.len(),
        urls,
        max_urls_per_request: config.max_urls_per_request,
        webhook_url,
        format,
        cookie_profile,
        errors,
    }
}

fn parse_job_status(value: &str) -> Result<JobStatus, ApiError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "queued" => Ok(JobStatus::Queued),
        "running" => Ok(JobStatus::Running),
        "succeeded" => Ok(JobStatus::Succeeded),
        "failed" => Ok(JobStatus::Failed),
        "canceled" => Ok(JobStatus::Canceled),
        "deleted" => Ok(JobStatus::Deleted),
        other => Err(ApiError::BadRequest(format!(
            "unsupported job status `{other}`"
        ))),
    }
}

#[derive(Debug)]
struct JobRecordFilters {
    status: Option<JobStatus>,
    platform: Option<String>,
    query: Option<String>,
}

impl JobRecordFilters {
    fn parse(
        status: Option<String>,
        platform: Option<String>,
        query: Option<String>,
    ) -> Result<Self, ApiError> {
        let status = status.as_deref().map(parse_job_status).transpose()?;
        let platform = parse_platform_filter(platform)?;
        let query = parse_job_search_query(query)?;
        Ok(Self {
            status,
            platform,
            query,
        })
    }

    fn matches(&self, record: &JobRecord) -> bool {
        self.status
            .as_ref()
            .is_none_or(|status| record.status == *status)
            && self
                .platform
                .as_deref()
                .is_none_or(|platform| job_platform(record) == Some(platform))
            && self
                .query
                .as_deref()
                .is_none_or(|query| job_matches_query(record, query))
    }
}

fn parse_platform_filter(value: Option<String>) -> Result<Option<String>, ApiError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let platform = value.trim().to_ascii_lowercase();
    if platform.is_empty() {
        return Ok(None);
    }
    if !platforms::known_platforms()
        .iter()
        .any(|known| *known == platform)
    {
        return Err(ApiError::BadRequest(format!(
            "unsupported platform `{platform}`"
        )));
    }
    Ok(Some(platform))
}

fn parse_job_search_query(value: Option<String>) -> Result<Option<String>, ApiError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let query = value.trim();
    if query.is_empty() {
        return Ok(None);
    }
    if query.chars().any(char::is_control) {
        return Err(ApiError::BadRequest(
            "job search query must not contain control characters".to_string(),
        ));
    }
    if query.chars().count() > 200 {
        return Err(ApiError::BadRequest(
            "job search query must be at most 200 characters".to_string(),
        ));
    }
    Ok(Some(query.to_ascii_lowercase()))
}

fn job_platform(record: &JobRecord) -> Option<&'static str> {
    let url = Url::parse(&record.url).ok()?;
    let host = url.host_str()?;
    platforms::platform_for_host(host)
}

fn job_matches_query(record: &JobRecord, query: &str) -> bool {
    text_matches_query(&record.id.to_string(), query)
        || text_matches_query(&record.url, query)
        || record
            .format
            .as_deref()
            .is_some_and(|format| text_matches_query(format, query))
        || record
            .cookie_profile
            .as_deref()
            .is_some_and(|cookie_profile| text_matches_query(cookie_profile, query))
        || record
            .error_kind
            .as_deref()
            .is_some_and(|error_kind| text_matches_query(error_kind, query))
        || record
            .error
            .as_deref()
            .is_some_and(|error| text_matches_query(error, query))
        || record
            .result
            .as_ref()
            .is_some_and(|result| download_metadata_matches_query(result, query))
}

fn download_metadata_matches_query(metadata: &crate::types::DownloadMetadata, query: &str) -> bool {
    text_matches_query(&metadata.original_url, query)
        || metadata
            .webpage_url
            .as_deref()
            .is_some_and(|value| text_matches_query(value, query))
        || metadata
            .extractor
            .as_deref()
            .is_some_and(|value| text_matches_query(value, query))
        || metadata
            .title
            .as_deref()
            .is_some_and(|value| text_matches_query(value, query))
        || metadata
            .uploader
            .as_deref()
            .is_some_and(|value| text_matches_query(value, query))
}

fn text_matches_query(value: &str, query: &str) -> bool {
    value.to_ascii_lowercase().contains(query)
}

enum JobsExportFormat {
    Jsonl,
    Csv,
}

impl JobsExportFormat {
    fn parse(value: &str) -> Result<Self, ApiError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "jsonl" | "ndjson" => Ok(Self::Jsonl),
            "csv" => Ok(Self::Csv),
            other => Err(ApiError::BadRequest(format!(
                "unsupported export format `{other}`"
            ))),
        }
    }
}

fn export_jobs_jsonl(records: &[JobRecord]) -> Result<String, ApiError> {
    let mut output = String::new();
    for record in records {
        let line = serde_json::to_string(record).map_err(|err| ApiError::Internal(err.into()))?;
        output.push_str(&line);
        output.push('\n');
    }
    Ok(output)
}

fn export_jobs_csv(records: &[JobRecord]) -> String {
    const HEADERS: &[&str] = &[
        "id",
        "status",
        "created_at",
        "updated_at",
        "url",
        "platform",
        "webhook_url",
        "format",
        "cookie_profile",
        "attempts",
        "error_kind",
        "error",
        "extractor",
        "title",
        "uploader",
        "duration",
        "extension",
        "media_path",
        "media_bytes",
        "info_json_path",
        "yt_dlp_version",
        "elapsed_ms",
    ];
    let mut output = String::new();
    output.push_str(&csv_row(HEADERS.iter().copied()));
    output.push('\n');
    for record in records {
        let result = record.result.as_ref();
        let fields = [
            record.id.to_string(),
            record.status.to_string(),
            record.created_at.to_string(),
            record.updated_at.to_string(),
            record.url.clone(),
            job_platform(record).unwrap_or_default().to_string(),
            record.webhook_url.clone().unwrap_or_default(),
            record.format.clone().unwrap_or_default(),
            record.cookie_profile.clone().unwrap_or_default(),
            record.attempts.to_string(),
            record.error_kind.clone().unwrap_or_default(),
            record.error.clone().unwrap_or_default(),
            result
                .and_then(|result| result.extractor.clone())
                .unwrap_or_default(),
            result
                .and_then(|result| result.title.clone())
                .unwrap_or_default(),
            result
                .and_then(|result| result.uploader.clone())
                .unwrap_or_default(),
            result
                .and_then(|result| result.duration)
                .map(|duration| duration.to_string())
                .unwrap_or_default(),
            result
                .and_then(|result| result.extension.clone())
                .unwrap_or_default(),
            result
                .map(|result| result.media_path.display().to_string())
                .unwrap_or_default(),
            result
                .map(|result| result.media_bytes.to_string())
                .unwrap_or_default(),
            result
                .map(|result| result.info_json_path.display().to_string())
                .unwrap_or_default(),
            result
                .map(|result| result.yt_dlp_version.clone())
                .unwrap_or_default(),
            result
                .map(|result| result.elapsed_ms.to_string())
                .unwrap_or_default(),
        ];
        output.push_str(&csv_row(fields.iter().map(String::as_str)));
        output.push('\n');
    }
    output
}

fn csv_row<'a>(fields: impl IntoIterator<Item = &'a str>) -> String {
    fields
        .into_iter()
        .map(csv_field)
        .collect::<Vec<_>>()
        .join(",")
}

fn csv_field(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn validate_download_url(value: &str, enabled_platforms: &[String]) -> Result<String, ApiError> {
    let url = Url::parse(value)
        .map_err(|err| ApiError::BadRequest(format!("invalid URL `{value}`: {err}")))?;
    match url.scheme() {
        "http" | "https" => {}
        scheme => {
            return Err(ApiError::BadRequest(format!(
                "unsupported URL scheme `{scheme}`; expected http or https"
            )));
        }
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ApiError::BadRequest(
            "download URLs must not include credentials".to_string(),
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| ApiError::BadRequest("download URL must include a host".to_string()))?
        .to_ascii_lowercase();
    let Some(platform) = platforms::platform_for_host(&host) else {
        return Err(ApiError::BadRequest(format!(
            "unsupported URL host `{host}`; expected a supported social video platform"
        )));
    };
    if !platforms::is_platform_enabled(platform, enabled_platforms) {
        return Err(ApiError::BadRequest(format!(
            "platform `{platform}` is disabled by configuration"
        )));
    }
    if !platforms::is_supported_video_url(platform, &url, &host) {
        return Err(ApiError::BadRequest(format!(
            "unsupported URL path `{}` for host `{host}`; expected a short-form video URL",
            url.path()
        )));
    }
    Ok(url.to_string())
}

fn validate_download_format(format: Option<String>) -> Result<Option<String>, ApiError> {
    const MAX_FORMAT_BYTES: usize = 256;
    let Some(format) = format else {
        return Ok(None);
    };
    let format = format.trim();
    if format.is_empty() {
        return Ok(None);
    }
    if format.len() > MAX_FORMAT_BYTES {
        return Err(ApiError::BadRequest(format!(
            "download format is too long; maximum is {MAX_FORMAT_BYTES} bytes"
        )));
    }
    if format.chars().any(char::is_control) {
        return Err(ApiError::BadRequest(
            "download format must not contain control characters".to_string(),
        ));
    }

    Ok(Some(format.to_string()))
}

fn validate_cookie_profile(
    config: &Config,
    cookie_profile: Option<String>,
) -> Result<Option<String>, ApiError> {
    let Some(cookie_profile) = cookie_profile else {
        return Ok(None);
    };
    let cookie_profile = cookie_profile.trim();
    if cookie_profile.is_empty() {
        return Ok(None);
    }
    let cookie_profile = normalize_cookie_profile_name(cookie_profile)
        .map_err(|err| ApiError::BadRequest(err.to_string()))?;
    if !config.cookie_profiles.contains_key(&cookie_profile) {
        return Err(ApiError::BadRequest(format!(
            "unknown cookie profile `{cookie_profile}`"
        )));
    }
    Ok(Some(cookie_profile))
}

fn validate_webhook_url(
    config: &Config,
    webhook_url: Option<String>,
) -> Result<Option<String>, ApiError> {
    let Some(webhook_url) = webhook_url.map(|url| url.trim().to_string()) else {
        return Ok(None);
    };
    if webhook_url.is_empty() {
        return Ok(None);
    }

    let parsed = Url::parse(&webhook_url)
        .map_err(|err| ApiError::BadRequest(format!("invalid webhook_url: {err}")))?;
    match parsed.scheme() {
        "http" | "https" => {}
        scheme => {
            return Err(ApiError::BadRequest(format!(
                "unsupported webhook_url scheme `{scheme}`; expected http or https"
            )));
        }
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(ApiError::BadRequest(
            "webhook_url must not include credentials".to_string(),
        ));
    }
    if parsed.fragment().is_some() {
        return Err(ApiError::BadRequest(
            "webhook_url must not include a fragment".to_string(),
        ));
    }
    if !config.allow_private_webhook_urls
        && parsed
            .host_str()
            .and_then(|host| host.parse::<IpAddr>().ok())
            .is_some_and(is_private_or_local_ip)
    {
        return Err(ApiError::BadRequest(
            "webhook_url must not target a local or private IP address".to_string(),
        ));
    }

    Ok(Some(webhook_url))
}

fn is_private_or_local_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip == Ipv4Addr::UNSPECIFIED
                || ip == Ipv4Addr::BROADCAST
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || is_unique_local_ipv6(ip)
                || is_unicast_link_local_ipv6(ip)
        }
    }
}

fn is_unique_local_ipv6(ip: Ipv6Addr) -> bool {
    (ip.segments()[0] & 0xfe00) == 0xfc00
}

fn is_unicast_link_local_ipv6(ip: Ipv6Addr) -> bool {
    (ip.segments()[0] & 0xffc0) == 0xfe80
}

fn ensure_workers_ready(state: &AppState) -> Result<(), ApiError> {
    if state.workers.is_ready() {
        return Ok(());
    }

    Err(ApiError::ServiceUnavailable(
        "download workers are not ready".to_string(),
    ))
}

fn ensure_download_path(config: &Config, path: &Path) -> Result<(), ApiError> {
    if path.starts_with(&config.downloads_dir) && path != config.downloads_dir {
        return Ok(());
    }

    Err(ApiError::Internal(anyhow::anyhow!(
        "download path is outside configured download directory: {}",
        path.display()
    )))
}

async fn read_download_artifact(path: &Path) -> Result<Vec<u8>, ApiError> {
    fs::read(path).await.map_err(|err| match err.kind() {
        std::io::ErrorKind::NotFound => ApiError::NotFound,
        _ => ApiError::Internal(err.into()),
    })
}

async fn open_download_artifact(path: &Path) -> Result<fs::File, ApiError> {
    fs::File::open(path).await.map_err(|err| match err.kind() {
        std::io::ErrorKind::NotFound => ApiError::NotFound,
        _ => ApiError::Internal(err.into()),
    })
}

async fn download_artifact_sha256(path: &Path) -> Result<String, ApiError> {
    sha256_file_hex(path).await.map_err(|err| match err.kind() {
        std::io::ErrorKind::NotFound => ApiError::NotFound,
        _ => ApiError::Internal(err.into()),
    })
}

async fn artifact_etag(path: &Path) -> Result<String, ApiError> {
    Ok(format!(
        "\"sha256-{}\"",
        download_artifact_sha256(path).await?
    ))
}

fn if_none_match_matches(headers: &HeaderMap, etag: &str) -> bool {
    let Some(value) = headers.get(header::IF_NONE_MATCH) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };

    value.split(',').any(|candidate| {
        let candidate = candidate.trim();
        candidate == "*" || candidate == etag || candidate.strip_prefix("W/") == Some(etag)
    })
}

fn not_modified_response(etag: String) -> Response {
    (
        StatusCode::NOT_MODIFIED,
        [(header::ETAG, etag)],
        Body::empty(),
    )
        .into_response()
}

async fn download_artifact_len(path: &Path) -> Result<u64, ApiError> {
    fs::metadata(path)
        .await
        .map(|metadata| metadata.len())
        .map_err(|err| match err.kind() {
            std::io::ErrorKind::NotFound => ApiError::NotFound,
            _ => ApiError::Internal(err.into()),
        })
}

async fn media_len(media: &fs::File) -> Result<u64, ApiError> {
    media
        .metadata()
        .await
        .map(|metadata| metadata.len())
        .map_err(|err| ApiError::Internal(err.into()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ByteRange {
    start: u64,
    end: u64,
}

impl ByteRange {
    fn len(self) -> u64 {
        self.end - self.start + 1
    }

    fn content_range(self, total_len: u64) -> String {
        format!("bytes {}-{}/{}", self.start, self.end, total_len)
    }
}

fn parse_range_header(headers: &HeaderMap, media_len: u64) -> Result<Option<ByteRange>, ApiError> {
    let Some(range) = headers.get(header::RANGE) else {
        return Ok(None);
    };
    let range = range
        .to_str()
        .map_err(|_| ApiError::BadRequest("range header is not valid UTF-8".to_string()))?;
    parse_byte_range(range, media_len).map(Some)
}

fn parse_byte_range(value: &str, media_len: u64) -> Result<ByteRange, ApiError> {
    let Some(range) = value.trim().strip_prefix("bytes=") else {
        return Err(ApiError::BadRequest(
            "only bytes range requests are supported".to_string(),
        ));
    };
    if range.contains(',') {
        return Err(ApiError::BadRequest(
            "multiple byte ranges are not supported".to_string(),
        ));
    }
    if media_len == 0 {
        return Err(ApiError::RangeNotSatisfiable { length: media_len });
    }

    let Some((start, end)) = range.split_once('-') else {
        return Err(ApiError::BadRequest(
            "range header must use bytes=start-end syntax".to_string(),
        ));
    };
    let parsed = if start.is_empty() {
        suffix_byte_range(end, media_len)?
    } else {
        explicit_byte_range(start, end, media_len)?
    };
    if parsed.start > parsed.end || parsed.start >= media_len {
        return Err(ApiError::RangeNotSatisfiable { length: media_len });
    }

    Ok(ByteRange {
        start: parsed.start,
        end: parsed.end.min(media_len - 1),
    })
}

fn suffix_byte_range(value: &str, media_len: u64) -> Result<ByteRange, ApiError> {
    let suffix_len = parse_range_number(value)?;
    if suffix_len == 0 {
        return Err(ApiError::RangeNotSatisfiable { length: media_len });
    }
    let start = media_len.saturating_sub(suffix_len);
    Ok(ByteRange {
        start,
        end: media_len - 1,
    })
}

fn explicit_byte_range(start: &str, end: &str, media_len: u64) -> Result<ByteRange, ApiError> {
    let start = parse_range_number(start)?;
    let end = if end.is_empty() {
        media_len - 1
    } else {
        parse_range_number(end)?
    };
    Ok(ByteRange { start, end })
}

fn parse_range_number(value: &str) -> Result<u64, ApiError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ApiError::BadRequest(
            "range bounds must be non-negative integers".to_string(),
        ));
    }
    value
        .parse::<u64>()
        .map_err(|_| ApiError::BadRequest("range bound is too large".to_string()))
}

async fn delete_job_artifacts(config: &Config, record: &JobRecord) -> Result<bool, ApiError> {
    let Some(result) = &record.result else {
        return Ok(false);
    };
    ensure_download_path(config, &result.media_path)?;
    let Some(job_dir) = result.media_path.parent() else {
        return Ok(false);
    };
    ensure_download_path(config, job_dir)?;

    match fs::remove_dir_all(job_dir).await {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(ApiError::Internal(err.into())),
    }
}

fn content_type_for_path(path: &Path) -> String {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.to_ascii_lowercase())
        .as_deref()
    {
        Some("mp4") | Some("m4v") => "video/mp4",
        Some("webm") => "video/webm",
        Some("mov") => "video/quicktime",
        Some("mkv") => "video/x-matroska",
        _ => "application/octet-stream",
    }
    .to_string()
}

fn is_previewable_media_type(content_type: &str) -> bool {
    matches!(content_type, "video/mp4" | "video/webm" | "video/quicktime")
}

#[derive(Debug, Clone, Copy)]
enum MediaDisposition {
    Attachment,
    Inline,
}

impl MediaDisposition {
    fn header_value(self, filename: &str) -> String {
        match self {
            Self::Attachment => format!("attachment; filename=\"{filename}\""),
            Self::Inline => format!("inline; filename=\"{filename}\""),
        }
    }
}

struct TarEntry<'a> {
    name: &'a str,
    bytes: &'a [u8],
}

struct TarFileEntry<'a> {
    name: &'a str,
    path: &'a Path,
}

struct TarEntryMeta<'a> {
    name: &'a str,
    len: u64,
}

fn archive_entry_name(path: &Path, fallback: &str) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(fallback)
        .to_string()
}

fn build_tar_archive(entries: &[TarEntry<'_>]) -> anyhow::Result<Vec<u8>> {
    let mut archive = Vec::new();
    for entry in entries {
        append_tar_entry(&mut archive, entry)?;
    }
    archive.extend_from_slice(&[0; 1024]);
    Ok(archive)
}

fn validate_tar_entry_name(name: &str) -> anyhow::Result<()> {
    if name.is_empty() {
        anyhow::bail!("tar entry name must not be empty");
    }
    if name.len() > 100 {
        anyhow::bail!("tar entry name is too long: {name}");
    }

    Ok(())
}

fn tar_archive_len(entries: &[TarEntryMeta<'_>]) -> anyhow::Result<u64> {
    let mut len = 1024_u64;
    for entry in entries {
        validate_tar_entry_name(entry.name)?;
        len = len
            .checked_add(512)
            .and_then(|len| len.checked_add(tar_padded_len(entry.len)))
            .ok_or_else(|| anyhow::anyhow!("tar archive length overflow"))?;
    }
    Ok(len)
}

fn append_tar_entry(archive: &mut Vec<u8>, entry: &TarEntry<'_>) -> anyhow::Result<()> {
    let header = tar_header(entry.name, entry.bytes.len() as u64)?;
    archive.extend_from_slice(&header);
    archive.extend_from_slice(entry.bytes);
    archive.extend(std::iter::repeat_n(
        0,
        tar_padding_len(entry.bytes.len() as u64) as usize,
    ));
    Ok(())
}

fn tar_header(name: &str, len: u64) -> anyhow::Result<[u8; 512]> {
    validate_tar_entry_name(name)?;

    let mut header = [0_u8; 512];
    write_tar_field(&mut header[0..100], name.as_bytes())?;
    write_tar_field(&mut header[100..108], b"0000644\0")?;
    write_tar_field(&mut header[108..116], b"0000000\0")?;
    write_tar_field(&mut header[116..124], b"0000000\0")?;
    write_tar_field(&mut header[124..136], format!("{len:011o}\0").as_bytes())?;
    write_tar_field(&mut header[136..148], b"00000000000\0")?;
    header[148..156].fill(b' ');
    header[156] = b'0';
    write_tar_field(&mut header[257..263], b"ustar\0")?;
    write_tar_field(&mut header[263..265], b"00")?;

    let checksum = header.iter().map(|byte| u32::from(*byte)).sum::<u32>();
    write_tar_field(
        &mut header[148..156],
        format!("{checksum:06o}\0 ").as_bytes(),
    )?;

    Ok(header)
}

async fn tar_archive_file_len(entries: &[TarFileEntry<'_>]) -> Result<u64, ApiError> {
    let mut metadata = Vec::with_capacity(entries.len());
    for entry in entries {
        metadata.push(TarEntryMeta {
            name: entry.name,
            len: download_artifact_len(entry.path).await?,
        });
    }
    tar_archive_len(&metadata).map_err(ApiError::Internal)
}

async fn archive_sha256(entries: &[TarFileEntry<'_>]) -> Result<String, ApiError> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 16 * 1024];

    for entry in entries {
        let len = download_artifact_len(entry.path).await?;
        let header = tar_header(entry.name, len).map_err(ApiError::Internal)?;
        hasher.update(header);

        let mut file = open_download_artifact(entry.path).await?;
        loop {
            let read = file
                .read(&mut buffer)
                .await
                .map_err(|err| ApiError::Internal(err.into()))?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }

        let padding = tar_padding_len(len);
        if padding > 0 {
            let zeros = [0_u8; 512];
            hasher.update(&zeros[..padding as usize]);
        }
    }

    hasher.update([0_u8; 1024]);
    Ok(hex_lower(&hasher.finalize()))
}

async fn archive_etag(entries: &[TarFileEntry<'_>]) -> Result<String, ApiError> {
    Ok(format!("\"sha256-{}\"", archive_sha256(entries).await?))
}

fn tar_padding_len(len: u64) -> u64 {
    let remainder = len % 512;
    if remainder == 0 { 0 } else { 512 - remainder }
}

fn tar_padded_len(len: u64) -> u64 {
    len + tar_padding_len(len)
}

fn write_tar_field(field: &mut [u8], bytes: &[u8]) -> anyhow::Result<()> {
    if bytes.len() > field.len() {
        anyhow::bail!("tar header field is too small");
    }
    field[..bytes.len()].copy_from_slice(bytes);
    Ok(())
}

async fn render_index(
    state: &AppState,
    response: Option<BatchQueueResponse>,
    notice: Option<String>,
    error: Option<String>,
) -> Result<Html<String>, ApiError> {
    let recent_jobs = recent_jobs(state, 20).await;
    let summary = current_job_summary(state).await;
    let workers = worker_health(state);
    let active_workers = state.workers.active_snapshot().len();
    let queue_len = state.queue_tx.len();
    let template = IndexTemplate {
        queued_jobs: response.map(|response| response.jobs).unwrap_or_default(),
        notice: notice.unwrap_or_default(),
        has_active_jobs: recent_jobs
            .iter()
            .any(|job| matches!(job.status, JobStatus::Queued | JobStatus::Running)),
        recent_jobs,
        error: error.unwrap_or_default(),
        total_jobs: summary.total,
        queued_count: summary.queued,
        running_count: summary.running,
        succeeded_count: summary.succeeded,
        failed_count: summary.failed,
        canceled_count: summary.canceled,
        deleted_count: summary.deleted,
        queue_capacity: state.config.queue_size,
        queue_available_slots: state.config.queue_size.saturating_sub(queue_len),
        workers_ready: workers.ready,
        workers_expected: workers.expected,
        active_workers,
    };
    let html = template
        .render()
        .context("failed to render index template")?;
    Ok(Html(html))
}

async fn render_job_detail(state: &AppState, record: JobRecord) -> Result<Html<String>, ApiError> {
    render_job_detail_with_notice(state, record, None, None).await
}

async fn render_job_detail_with_notice(
    state: &AppState,
    record: JobRecord,
    notice: Option<String>,
    error: Option<String>,
) -> Result<Html<String>, ApiError> {
    let artifact_checksums = job_artifact_checksums(&state.config, &record).await;
    let template = JobDetailTemplate {
        job: job_detail_view(record, artifact_checksums),
        notice: notice.unwrap_or_default(),
        error: error.unwrap_or_default(),
    };
    let html = template
        .render()
        .context("failed to render job detail template")?;
    Ok(Html(html))
}

async fn render_dead_letters_page(
    state: &AppState,
    notice: Option<String>,
    error: Option<String>,
) -> Result<Html<String>, ApiError> {
    let mut dead_letters = load_webhook_dead_letters(&state.config.webhooks_dead_letter_jsonl)
        .await
        .map_err(ApiError::Internal)?;
    dead_letters.sort_by_key(|dead_letter| dead_letter.failed_at);
    dead_letters.reverse();
    dead_letters.truncate(100);

    let template = DeadLettersTemplate {
        dead_letters: dead_letters.iter().map(dead_letter_view).collect(),
        notice: notice.unwrap_or_default(),
        error: error.unwrap_or_default(),
    };
    let html = template
        .render()
        .context("failed to render webhook dead letters template")?;
    Ok(Html(html))
}

async fn render_job_list(state: &AppState, query: ListJobsQuery) -> Result<Html<String>, ApiError> {
    let selected_status = query.status.clone().unwrap_or_default();
    let selected_platform = query.platform.clone().unwrap_or_default();
    let selected_query = query.q.clone().unwrap_or_default();
    let response = list_job_records(state, query).await?;
    let shown_start = if response.jobs.is_empty() {
        0
    } else {
        response.offset + 1
    };
    let shown_end = response.offset + response.jobs.len();
    let previous_offset = response.offset.saturating_sub(response.limit);
    let next_offset = response.offset + response.limit;
    let template = JobListTemplate {
        jobs: response.jobs.iter().map(job_list_item_view).collect(),
        status: selected_status.clone(),
        platform: selected_platform.clone(),
        platform_options: platforms::known_platforms()
            .into_iter()
            .map(|platform| PlatformFilterOption {
                id: platform,
                selected: platform == selected_platform,
            })
            .collect(),
        query: selected_query.clone(),
        total: response.total,
        limit: response.limit,
        shown_start,
        shown_end,
        has_previous: response.offset > 0,
        previous_url: job_list_url(
            &selected_status,
            &selected_platform,
            &selected_query,
            response.limit,
            previous_offset,
        ),
        has_next: next_offset < response.total,
        next_url: job_list_url(
            &selected_status,
            &selected_platform,
            &selected_query,
            response.limit,
            next_offset,
        ),
    };
    let html = template
        .render()
        .context("failed to render job list template")?;
    Ok(Html(html))
}

fn job_detail_view(
    record: JobRecord,
    artifact_checksums: Option<JobArtifactChecksums>,
) -> JobDetailView {
    let result = record
        .result
        .as_ref()
        .map(|result| job_result_view_for_job(record.id, result, artifact_checksums));
    let has_webhook = record.webhook_url.is_some();
    let has_cookie_profile = record.cookie_profile.is_some();
    let can_redeliver_webhook = has_webhook
        && !matches!(
            record.status,
            JobStatus::Queued | JobStatus::Running | JobStatus::Deleted
        );
    JobDetailView {
        id: record.id.to_string(),
        status: record.status.to_string(),
        url: record.url,
        has_format: record.format.is_some(),
        format: record.format.unwrap_or_default(),
        has_cookie_profile,
        cookie_profile: record.cookie_profile.unwrap_or_default(),
        has_webhook,
        webhook_url: record.webhook_url.unwrap_or_default(),
        created_at: record.created_at.to_string(),
        updated_at: record.updated_at.to_string(),
        attempts: record.attempts,
        has_error_kind: record.error_kind.is_some(),
        error_kind: record.error_kind.unwrap_or_default(),
        has_error: record.error.is_some(),
        error: record.error.unwrap_or_default(),
        has_result: result.is_some(),
        result: result.unwrap_or_default(),
        attempt_errors: record
            .attempt_errors
            .into_iter()
            .map(|attempt| JobAttemptView {
                attempt: attempt.attempt,
                error: attempt.error,
                elapsed_ms: attempt.elapsed_ms,
                retry_backoff_ms: attempt
                    .retry_backoff_ms
                    .map(|backoff| format!("{backoff} ms"))
                    .unwrap_or_else(|| "none".to_string()),
            })
            .collect(),
        can_cancel: record.status.is_cancelable(),
        can_retry: record.status.is_retryable(),
        can_delete: record.status.is_deletable(),
        can_redeliver_webhook,
        is_active: matches!(record.status, JobStatus::Queued | JobStatus::Running),
    }
}

fn job_list_item_view(record: &JobRecord) -> JobListItemView {
    JobListItemView {
        id: record.id.to_string(),
        status: record.status.to_string(),
        platform: job_platform(record).unwrap_or("unknown").to_string(),
        url: record.url.clone(),
        updated_at: record.updated_at.to_string(),
        has_media: record.result.is_some(),
    }
}

fn job_list_url(status: &str, platform: &str, query: &str, limit: usize, offset: usize) -> String {
    let mut url = "/jobs".to_string();
    push_query_param_if_present(&mut url, "status", status);
    push_query_param_if_present(&mut url, "platform", platform);
    push_query_param_if_present(&mut url, "q", query);
    push_query_param(&mut url, "limit", &limit.to_string());
    push_query_param(&mut url, "offset", &offset.to_string());
    url
}

fn push_query_param_if_present(url: &mut String, key: &str, value: &str) {
    if !value.trim().is_empty() {
        push_query_param(url, key, value);
    }
}

fn push_query_param(url: &mut String, key: &str, value: &str) {
    url.push(if url.contains('?') { '&' } else { '?' });
    url.push_str(key);
    url.push('=');
    url.push_str(&query_encode(value));
}

fn query_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                vec![byte as char]
            }
            b' ' => vec!['+'],
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

fn dead_letter_view(dead_letter: &WebhookDeadLetter) -> DeadLetterView {
    DeadLetterView {
        event_id: dead_letter.event.event_id.to_string(),
        event_type: dead_letter.event.event_type.clone(),
        job_id: dead_letter.event.job.id.to_string(),
        job_status: dead_letter.event.job.status.to_string(),
        webhook_url: dead_letter
            .event
            .job
            .webhook_url
            .clone()
            .unwrap_or_default(),
        failed_at: dead_letter.failed_at.to_string(),
        attempts: dead_letter.attempts,
        error: dead_letter.error.clone(),
    }
}

struct JobArtifactChecksums {
    media_sha256: String,
    info_json_sha256: String,
    archive_bytes: u64,
    archive_sha256: String,
}

async fn job_artifact_checksums(
    config: &Config,
    record: &JobRecord,
) -> Option<JobArtifactChecksums> {
    let result = record.result.as_ref()?;
    ensure_download_path(config, &result.media_path).ok()?;
    ensure_download_path(config, &result.info_json_path).ok()?;
    let media_sha256 = download_artifact_sha256(&result.media_path).await.ok()?;
    let info_json_sha256 = download_artifact_sha256(&result.info_json_path)
        .await
        .ok()?;
    let media_name = archive_entry_name(&result.media_path, "download.bin");
    let info_json_name = archive_entry_name(&result.info_json_path, "info.json");
    let archive_entries = [
        TarFileEntry {
            name: &media_name,
            path: &result.media_path,
        },
        TarFileEntry {
            name: &info_json_name,
            path: &result.info_json_path,
        },
    ];
    let archive_bytes = tar_archive_file_len(&archive_entries).await.ok()?;
    let archive_sha256 = archive_sha256(&archive_entries).await.ok()?;
    Some(JobArtifactChecksums {
        media_sha256,
        info_json_sha256,
        archive_bytes,
        archive_sha256,
    })
}

fn job_result_view_for_job(
    id: Uuid,
    result: &crate::types::DownloadMetadata,
    artifact_checksums: Option<JobArtifactChecksums>,
) -> JobResultView {
    let media_content_type = content_type_for_path(&result.media_path);
    let (media_sha256, info_json_sha256, archive_bytes, archive_sha256) = artifact_checksums
        .map(|checksums| {
            (
                checksums.media_sha256,
                checksums.info_json_sha256,
                checksums.archive_bytes,
                checksums.archive_sha256,
            )
        })
        .unwrap_or_default();
    JobResultView {
        original_url: result.original_url.clone(),
        has_webpage_url: result.webpage_url.is_some(),
        webpage_url: result.webpage_url.clone().unwrap_or_default(),
        has_extractor: result.extractor.is_some(),
        extractor: result.extractor.clone().unwrap_or_default(),
        has_title: result.title.is_some(),
        title: result.title.clone().unwrap_or_default(),
        has_uploader: result.uploader.is_some(),
        uploader: result.uploader.clone().unwrap_or_default(),
        has_duration: result.duration.is_some(),
        duration: result
            .duration
            .map(|duration| format!("{duration:.3} s"))
            .unwrap_or_default(),
        has_extension: result.extension.is_some(),
        extension: result.extension.clone().unwrap_or_default(),
        can_preview: is_previewable_media_type(&media_content_type),
        preview_url: format!("/v1/jobs/{id}/media-inline"),
        media_content_type,
        media_path: result.media_path.display().to_string(),
        media_bytes: result.media_bytes,
        has_media_sha256: !media_sha256.is_empty(),
        media_sha256,
        info_json_path: result.info_json_path.display().to_string(),
        has_info_json_sha256: !info_json_sha256.is_empty(),
        info_json_sha256,
        has_archive_metadata: !archive_sha256.is_empty(),
        archive_bytes,
        archive_sha256,
        yt_dlp_version: result.yt_dlp_version.clone(),
        elapsed_ms: result.elapsed_ms,
    }
}

fn prometheus_metrics(metrics: &MetricsResponse) -> String {
    let mut output = String::new();
    push_metric(
        &mut output,
        "yt_dlp_server_workers_expected",
        "gauge",
        "Configured download worker count.",
        metrics.workers.expected,
    );
    push_metric(
        &mut output,
        "yt_dlp_server_workers_ready",
        "gauge",
        "Download workers currently ready.",
        metrics.workers.ready,
    );
    push_metric(
        &mut output,
        "yt_dlp_server_workers_failed",
        "gauge",
        "Download workers that stopped unexpectedly.",
        metrics.workers.failed,
    );
    push_metric(
        &mut output,
        "yt_dlp_server_queue_depth",
        "gauge",
        "Jobs currently waiting in the in-process queue.",
        metrics.queued,
    );
    push_metric(
        &mut output,
        "yt_dlp_server_http_requests_total",
        "counter",
        "HTTP requests handled by the server.",
        metrics.http_requests_total,
    );
    push_metric(
        &mut output,
        "yt_dlp_server_http_requests_failed_total",
        "counter",
        "HTTP requests that returned 4xx or 5xx.",
        metrics.http_requests_failed,
    );
    push_metric(
        &mut output,
        "yt_dlp_server_http_request_duration_ms_total",
        "counter",
        "Total HTTP request handling time in milliseconds.",
        metrics.total_request_ms,
    );
    push_metric(
        &mut output,
        "yt_dlp_server_jobs_started_total",
        "counter",
        "Download jobs started by workers.",
        metrics.jobs_started,
    );
    push_metric(
        &mut output,
        "yt_dlp_server_jobs_succeeded_total",
        "counter",
        "Download jobs that completed successfully.",
        metrics.jobs_succeeded,
    );
    push_metric(
        &mut output,
        "yt_dlp_server_jobs_failed_total",
        "counter",
        "Download jobs that failed or were canceled.",
        metrics.jobs_failed,
    );
    push_metric(
        &mut output,
        "yt_dlp_server_jobs_timed_out_total",
        "counter",
        "Download jobs that timed out.",
        metrics.jobs_timed_out,
    );
    push_metric(
        &mut output,
        "yt_dlp_server_download_duration_ms_total",
        "counter",
        "Total download job runtime in milliseconds.",
        metrics.total_download_ms,
    );
    push_metric(
        &mut output,
        "yt_dlp_server_webhook_failures_total",
        "counter",
        "Webhook delivery failures.",
        metrics.webhook_failures,
    );
    push_metric(
        &mut output,
        "yt_dlp_server_cleanup_failures_total",
        "counter",
        "Download cleanup failures.",
        metrics.cleanup_failures,
    );
    push_metric(
        &mut output,
        "yt_dlp_server_retained_jobs",
        "gauge",
        "Job records retained in memory.",
        metrics.retained_jobs,
    );
    if let Some(rss) = metrics.process_memory_rss_bytes {
        push_metric(
            &mut output,
            "yt_dlp_server_process_memory_rss_bytes",
            "gauge",
            "Resident set size in bytes, when available.",
            rss,
        );
    }
    output
}

fn push_metric<T: std::fmt::Display>(
    output: &mut String,
    name: &str,
    kind: &str,
    help: &str,
    value: T,
) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push(' ');
    output.push_str(kind);
    output.push('\n');
    output.push_str(name);
    output.push(' ');
    output.push_str(&value.to_string());
    output.push('\n');
}

async fn recent_jobs(state: &AppState, limit: usize) -> Vec<JobRecord> {
    let mut records = state
        .jobs
        .read()
        .await
        .values()
        .cloned()
        .collect::<Vec<_>>();
    records.sort_by_key(|record| record.updated_at);
    records.reverse();
    records.truncate(limit);
    records
}

fn openapi_document() -> Value {
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Social Video Download Server",
            "version": env!("CARGO_PKG_VERSION")
        },
        "paths": {
            "/health": {
                "get": {
                    "summary": "Liveness check",
                    "responses": {
                        "200": { "description": "Server is alive", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/HealthResponse" } } } }
                    }
                }
            },
            "/ready": {
                "get": {
                    "summary": "Readiness check",
                    "responses": {
                        "200": { "description": "Download workers are ready" },
                        "503": { "description": "No download worker is ready" }
                    }
                }
            },
            "/metrics": {
                "get": {
                    "summary": "Runtime metrics",
                    "responses": {
                        "200": { "description": "Metrics snapshot", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/MetricsResponse" } } } }
                    }
                }
            },
            "/metrics.prometheus": {
                "get": {
                    "summary": "Runtime metrics in Prometheus text format",
                    "responses": {
                        "200": { "description": "Prometheus metrics", "content": { "text/plain": { "schema": { "type": "string" } } } }
                    }
                }
            },
            "/v1/config": {
                "get": {
                    "summary": "Read sanitized effective runtime configuration",
                    "responses": {
                        "200": { "description": "Sanitized runtime configuration", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/RuntimeConfigResponse" } } } },
                        "401": { "description": "API key is required when authentication is enabled" }
                    }
                }
            },
            "/v1/cookie-profiles": {
                "get": {
                    "summary": "List configured server-side cookie profiles and retained-job health",
                    "responses": {
                        "200": { "description": "Cookie profile status list", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/CookieProfileListResponse" } } } },
                        "401": { "description": "API key is required when authentication is enabled" }
                    }
                }
            },
            "/v1/platforms": {
                "get": {
                    "summary": "List supported short-form platforms and enabled URL rules",
                    "responses": {
                        "200": { "description": "Platform capability list", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/PlatformListResponse" } } } }
                    }
                }
            },
            "/v1/queue": {
                "get": {
                    "summary": "Read queue capacity and worker readiness",
                    "responses": {
                        "200": { "description": "Queue status", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/QueueStatusResponse" } } } }
                    }
                }
            },
            "/v1/workers": {
                "get": {
                    "summary": "Read worker readiness and active download jobs",
                    "responses": {
                        "200": { "description": "Worker status", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/WorkerStatusResponse" } } } }
                    }
                }
            },
            "/v1/storage/cleanup": {
                "get": {
                    "summary": "Preview storage cleanup without deleting artifacts",
                    "parameters": [
                        { "name": "max_bytes", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 0 } }
                    ],
                    "responses": {
                        "200": { "description": "Storage cleanup preview", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/StorageCleanupResponse" } } } }
                    }
                },
                "post": {
                    "summary": "Run storage cleanup and delete oldest successful artifacts above the byte limit",
                    "parameters": [
                        { "name": "max_bytes", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 0 } }
                    ],
                    "responses": {
                        "200": { "description": "Storage cleanup result", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/StorageCleanupResponse" } } } }
                    }
                }
            },
            "/v1/downloads/validate": {
                "post": {
                    "summary": "Validate social video download URLs without enqueueing jobs",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/DownloadRequest" }
                            }
                        }
                    },
                    "responses": {
                        "200": { "description": "Validation result", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ValidateDownloadsResponse" } } } }
                    }
                }
            },
            "/v1/downloads": {
                "post": {
                    "summary": "Queue social video downloads",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/DownloadRequest" }
                            }
                        }
                    },
                    "responses": {
                        "200": { "description": "Jobs queued", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/BatchQueueResponse" } } } },
                        "400": { "description": "Invalid request" },
                        "503": { "description": "Download queue is full or unavailable" }
                    }
                }
            },
            "/v1/jobs": {
                "get": {
                    "summary": "List jobs",
                    "parameters": [
                        { "name": "status", "in": "query", "required": false, "schema": { "type": "string", "enum": ["queued", "running", "succeeded", "failed", "canceled", "deleted"] } },
                        { "name": "platform", "in": "query", "required": false, "schema": { "type": "string", "enum": ["tiktok", "instagram", "youtube", "facebook", "snapchat", "rutube", "douyin", "likee", "vk", "yappy"] } },
                        { "name": "q", "in": "query", "required": false, "schema": { "type": "string", "maxLength": 200 }, "description": "Case-insensitive search across URL, id, format, error, title, uploader, extractor, and final webpage URL." },
                        { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "maximum": 500 } },
                        { "name": "offset", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 0 } }
                    ],
                    "responses": {
                        "200": { "description": "Job list", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/JobListResponse" } } } },
                        "400": { "description": "Invalid filter" }
                    }
                }
            },
            "/v1/jobs/export": {
                "get": {
                    "summary": "Export retained jobs as JSONL or CSV",
                    "parameters": [
                        { "name": "format", "in": "query", "required": false, "schema": { "type": "string", "enum": ["jsonl", "csv"], "default": "jsonl" } },
                        { "name": "status", "in": "query", "required": false, "schema": { "type": "string", "enum": ["queued", "running", "succeeded", "failed", "canceled", "deleted"] } },
                        { "name": "platform", "in": "query", "required": false, "schema": { "type": "string", "enum": ["tiktok", "instagram", "youtube", "facebook", "snapchat", "rutube", "douyin", "likee", "vk", "yappy"] } },
                        { "name": "q", "in": "query", "required": false, "schema": { "type": "string", "maxLength": 200 }, "description": "Case-insensitive search across URL, id, format, error, title, uploader, extractor, and final webpage URL." }
                    ],
                    "responses": {
                        "200": {
                            "description": "Retained jobs export",
                            "content": {
                                "application/x-ndjson": { "schema": { "type": "string" } },
                                "text/csv": { "schema": { "type": "string" } }
                            }
                        },
                        "400": { "description": "Invalid export format or status filter" }
                    }
                }
            },
            "/v1/jobs/summary": {
                "get": {
                    "summary": "Read current job counts by status",
                    "responses": {
                        "200": { "description": "Job status summary", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/JobSummaryResponse" } } } }
                    }
                }
            },
            "/v1/jobs/batch/cancel": {
                "post": {
                    "summary": "Cancel multiple queued or running jobs",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/BatchJobActionRequest" }
                            }
                        }
                    },
                    "responses": {
                        "200": { "description": "Per-job cancel results", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/BatchCancelJobsResponse" } } } },
                        "400": { "description": "Invalid batch request" }
                    }
                }
            },
            "/v1/jobs/batch/retry": {
                "post": {
                    "summary": "Retry multiple terminal jobs as new queued jobs",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/BatchJobActionRequest" }
                            }
                        }
                    },
                    "responses": {
                        "200": { "description": "Per-job retry results", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/BatchRetryJobsResponse" } } } },
                        "400": { "description": "Invalid batch request" }
                    }
                }
            },
            "/v1/jobs/batch/delete": {
                "post": {
                    "summary": "Delete multiple terminal jobs and their artifacts",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/BatchJobActionRequest" }
                            }
                        }
                    },
                    "responses": {
                        "200": { "description": "Per-job delete results", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/BatchDeleteJobsResponse" } } } },
                        "400": { "description": "Invalid batch request" }
                    }
                }
            },
            "/v1/webhooks/dead-letters": {
                "get": {
                    "summary": "List failed webhook deliveries",
                    "parameters": [
                        { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "maximum": 500 } },
                        { "name": "offset", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 0 } }
                    ],
                    "responses": {
                        "200": { "description": "Webhook dead-letter list" }
                    }
                }
            },
            "/v1/webhooks/dead-letters/{event_id}/replay": {
                "post": {
                    "summary": "Replay a failed webhook delivery and remove it after success",
                    "parameters": [
                        { "name": "event_id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Webhook replay delivered" },
                        "404": { "description": "Dead-letter event not found" },
                        "503": { "description": "Webhook replay failed" }
                    }
                }
            },
            "/v1/webhooks/dead-letters/{event_id}": {
                "delete": {
                    "summary": "Dismiss a failed webhook delivery without replaying it",
                    "parameters": [
                        { "name": "event_id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Dead-letter event dismissed" },
                        "404": { "description": "Dead-letter event not found" }
                    }
                }
            },
            "/v1/jobs/{id}": {
                "get": {
                    "summary": "Read job status",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Job record", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/JobRecord" } } } },
                        "404": { "description": "Job not found" }
                    }
                },
                "delete": {
                    "summary": "Delete a terminal job and its media",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Job deleted", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/DeleteJobResponse" } } } },
                        "404": { "description": "Job not found" },
                        "409": { "description": "Job is not terminal" }
                    }
                }
            },
            "/v1/jobs/{id}/wait": {
                "get": {
                    "summary": "Wait for a job to become terminal or until the wait timeout expires",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } },
                        { "name": "timeout_seconds", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 0, "maximum": 55, "default": 30 } }
                    ],
                    "responses": {
                        "200": { "description": "Current or terminal job record", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/JobRecord" } } } },
                        "404": { "description": "Job not found" }
                    }
                }
            },
            "/v1/jobs/{id}/queue-position": {
                "get": {
                    "summary": "Read a queued job's estimated position",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Queue position", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/JobQueuePositionResponse" } } } },
                        "404": { "description": "Job not found" },
                        "409": { "description": "Job is not queued" }
                    }
                }
            },
            "/v1/jobs/{id}/cancel": {
                "post": {
                    "summary": "Cancel a queued or running job",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Job canceled", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/JobRecord" } } } },
                        "404": { "description": "Job not found" },
                        "409": { "description": "Job cannot be canceled" }
                    }
                }
            },
            "/v1/jobs/{id}/retry": {
                "post": {
                    "summary": "Retry a terminal job as a new queued job",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Retry job queued", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/QueueResponse" } } } },
                        "404": { "description": "Job not found" },
                        "409": { "description": "Job cannot be retried" }
                    }
                }
            },
            "/v1/jobs/{id}/webhook": {
                "post": {
                    "summary": "Redeliver a terminal job webhook",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Webhook delivered" },
                        "404": { "description": "Job not found" },
                        "409": { "description": "Job is not terminal or has no webhook URL" },
                        "503": { "description": "Webhook delivery failed" }
                    }
                }
            },
            "/v1/jobs/{id}/artifacts": {
                "get": {
                    "summary": "Discover stable artifact URLs for a completed job",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Artifact URLs and metadata", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/JobArtifactsResponse" } } } },
                        "404": { "description": "Job not found" },
                        "409": { "description": "Job has no artifacts" }
                    }
                }
            },
            "/v1/jobs/{id}/media": {
                "get": {
                    "summary": "Download completed job media",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Media file with SHA-256 based ETag header" },
                        "206": { "description": "Partial media file for a satisfiable Range request with SHA-256 based ETag header" },
                        "304": { "description": "Media file is unchanged for a matching If-None-Match request" },
                        "404": { "description": "Job or media not found" },
                        "409": { "description": "Job has no media" },
                        "416": { "description": "Requested byte range is not satisfiable" }
                    }
                },
                "head": {
                    "summary": "Read completed job media headers without downloading the body",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Media file headers including SHA-256 based ETag" },
                        "206": { "description": "Partial media file headers for a satisfiable Range request including SHA-256 based ETag" },
                        "304": { "description": "Media file is unchanged for a matching If-None-Match request" },
                        "404": { "description": "Job or media not found" },
                        "409": { "description": "Job has no media" },
                        "416": { "description": "Requested byte range is not satisfiable" }
                    }
                }
            },
            "/v1/jobs/{id}/media-inline": {
                "get": {
                    "summary": "Stream completed job media for inline browser preview",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Inline media stream with SHA-256 based ETag header" },
                        "206": { "description": "Partial inline media stream for a satisfiable Range request with SHA-256 based ETag header" },
                        "304": { "description": "Inline media is unchanged for a matching If-None-Match request" },
                        "404": { "description": "Job or media not found" },
                        "409": { "description": "Job has no media" },
                        "416": { "description": "Requested byte range is not satisfiable" }
                    }
                },
                "head": {
                    "summary": "Read inline media headers without downloading the body",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Inline media stream headers including SHA-256 based ETag" },
                        "206": { "description": "Partial inline media headers for a satisfiable Range request including SHA-256 based ETag" },
                        "304": { "description": "Inline media is unchanged for a matching If-None-Match request" },
                        "404": { "description": "Job or media not found" },
                        "409": { "description": "Job has no media" },
                        "416": { "description": "Requested byte range is not satisfiable" }
                    }
                }
            },
            "/v1/jobs/{id}/info-json": {
                "get": {
                    "summary": "Download completed job yt-dlp info JSON",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "yt-dlp info JSON with SHA-256 based ETag header" },
                        "304": { "description": "yt-dlp info JSON is unchanged for a matching If-None-Match request" },
                        "404": { "description": "Job or info JSON not found" },
                        "409": { "description": "Job has no info JSON" }
                    }
                },
                "head": {
                    "summary": "Read completed job yt-dlp info JSON headers without downloading the body",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "yt-dlp info JSON headers including SHA-256 based ETag" },
                        "304": { "description": "yt-dlp info JSON is unchanged for a matching If-None-Match request" },
                        "404": { "description": "Job or info JSON not found" },
                        "409": { "description": "Job has no info JSON" }
                    }
                }
            },
            "/v1/jobs/{id}/archive": {
                "get": {
                    "summary": "Download completed job media and metadata as a tar archive",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Tar archive including a Content-Length header" },
                        "206": { "description": "Partial tar archive for a satisfiable Range request" },
                        "304": { "description": "Tar archive is unchanged for a matching If-None-Match request" },
                        "404": { "description": "Job or artifact not found" },
                        "409": { "description": "Job has no artifacts" },
                        "416": { "description": "Requested byte range is not satisfiable" }
                    }
                },
                "head": {
                    "summary": "Read completed job archive headers without downloading the body",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Tar archive headers including Content-Length" },
                        "206": { "description": "Partial tar archive headers for a satisfiable Range request" },
                        "304": { "description": "Tar archive is unchanged for a matching If-None-Match request" },
                        "404": { "description": "Job or artifact not found" },
                        "409": { "description": "Job has no artifacts" },
                        "416": { "description": "Requested byte range is not satisfiable" }
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "DownloadRequest": {
                    "type": "object",
                    "required": ["urls"],
                    "properties": {
                        "urls": { "type": "array", "items": { "type": "string", "format": "uri" } },
                        "webhook_url": { "type": ["string", "null"], "format": "uri" },
                        "format": {
                            "type": ["string", "null"],
                            "description": "Optional yt-dlp format selector applied to every URL in this request."
                        },
                        "cookie_profile": {
                            "type": ["string", "null"],
                            "description": "Optional configured server-side cookie profile name. The request never includes raw cookies."
                        },
                        "force": {
                            "type": "boolean",
                            "default": false,
                            "description": "Queue a fresh download even when a successful retained job already exists for the same normalized URL, format, and cookie profile."
                        }
                    }
                },
                "BatchQueueResponse": {
                    "type": "object",
                    "required": ["jobs"],
                    "properties": {
                        "jobs": { "type": "array", "items": { "$ref": "#/components/schemas/QueueResponse" } }
                    }
                },
                "BatchJobActionRequest": {
                    "type": "object",
                    "required": ["ids"],
                    "properties": {
                        "ids": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 500,
                            "items": { "type": "string", "format": "uuid" }
                        }
                    }
                },
                "BatchJobActionError": {
                    "type": "object",
                    "required": ["id", "code", "message"],
                    "properties": {
                        "id": { "type": "string", "format": "uuid" },
                        "code": { "type": "string" },
                        "message": { "type": "string" }
                    }
                },
                "BatchCancelJobsResponse": {
                    "type": "object",
                    "required": ["action", "succeeded", "failed"],
                    "properties": {
                        "action": { "type": "string", "enum": ["cancel"] },
                        "succeeded": { "type": "array", "items": { "$ref": "#/components/schemas/JobRecord" } },
                        "failed": { "type": "array", "items": { "$ref": "#/components/schemas/BatchJobActionError" } }
                    }
                },
                "BatchRetryJobsResponse": {
                    "type": "object",
                    "required": ["action", "succeeded", "failed"],
                    "properties": {
                        "action": { "type": "string", "enum": ["retry"] },
                        "succeeded": { "type": "array", "items": { "$ref": "#/components/schemas/QueueResponse" } },
                        "failed": { "type": "array", "items": { "$ref": "#/components/schemas/BatchJobActionError" } }
                    }
                },
                "BatchDeleteJobsResponse": {
                    "type": "object",
                    "required": ["action", "succeeded", "failed"],
                    "properties": {
                        "action": { "type": "string", "enum": ["delete"] },
                        "succeeded": { "type": "array", "items": { "$ref": "#/components/schemas/DeleteJobResponse" } },
                        "failed": { "type": "array", "items": { "$ref": "#/components/schemas/BatchJobActionError" } }
                    }
                },
                "QueueResponse": {
                    "type": "object",
                    "required": ["id", "status", "status_url", "existing"],
                    "properties": {
                        "id": { "type": "string", "format": "uuid" },
                        "status": { "type": "string", "enum": ["queued", "running", "succeeded", "failed", "canceled", "deleted"] },
                        "status_url": { "type": "string" },
                        "existing": {
                            "type": "boolean",
                            "description": "True when the response points at a retained successful job instead of a newly queued job."
                        }
                    }
                },
                "JobListResponse": { "type": "object" },
                "DeleteJobResponse": { "type": "object" },
                "RuntimeConfigResponse": {
                    "type": "object",
                    "required": ["server", "queue", "download", "webhooks", "retention"],
                    "properties": {
                        "server": { "$ref": "#/components/schemas/RuntimeServerConfig" },
                        "queue": { "$ref": "#/components/schemas/RuntimeQueueConfig" },
                        "download": { "$ref": "#/components/schemas/RuntimeDownloadConfig" },
                        "webhooks": { "$ref": "#/components/schemas/RuntimeWebhookConfig" },
                        "retention": { "$ref": "#/components/schemas/RuntimeRetentionConfig" }
                    }
                },
                "RuntimeServerConfig": {
                    "type": "object",
                    "required": ["bind_addr", "cors_allowed_origins", "api_key_auth_enabled", "rate_limit_requests_per_minute"],
                    "properties": {
                        "bind_addr": { "type": "string" },
                        "cors_allowed_origins": { "type": "array", "items": { "type": "string" } },
                        "api_key_auth_enabled": { "type": "boolean" },
                        "rate_limit_requests_per_minute": { "type": "integer", "minimum": 0 }
                    }
                },
                "RuntimeQueueConfig": {
                    "type": "object",
                    "required": ["queue_size", "body_limit_bytes", "request_timeout_seconds"],
                    "properties": {
                        "queue_size": { "type": "integer", "minimum": 0 },
                        "body_limit_bytes": { "type": "integer", "minimum": 0 },
                        "request_timeout_seconds": { "type": "integer", "minimum": 0 }
                    }
                },
                "RuntimeDownloadConfig": {
                    "type": "object",
                    "required": ["workers", "output_dir", "yt_dlp_command", "cookies_configured", "cookie_profiles", "format_configured", "proxy_configured", "enabled_platforms", "platform_policies", "max_urls_per_request", "job_timeout_seconds", "max_attempts", "initial_backoff_ms", "max_storage_bytes", "min_free_disk_bytes", "post_processing_enabled", "post_processing_command_count", "object_storage_backend", "object_storage_configured", "object_storage_public_urls"],
                    "properties": {
                        "workers": { "type": "integer", "minimum": 1 },
                        "output_dir": { "type": "string" },
                        "yt_dlp_command": { "type": "string" },
                        "cookies_configured": { "type": "boolean" },
                        "cookie_profiles": { "type": "array", "items": { "type": "string" } },
                        "format_configured": { "type": "boolean" },
                        "proxy_configured": { "type": "boolean" },
                        "enabled_platforms": { "type": "array", "items": { "type": "string" } },
                        "platform_policies": { "type": "array", "items": { "$ref": "#/components/schemas/RuntimePlatformDownloadPolicy" } },
                        "max_urls_per_request": { "type": "integer", "minimum": 1 },
                        "job_timeout_seconds": { "type": "integer", "minimum": 0 },
                        "max_attempts": { "type": "integer", "minimum": 1 },
                        "initial_backoff_ms": { "type": "integer", "minimum": 0 },
                        "max_storage_bytes": { "type": "integer", "minimum": 0 },
                        "min_free_disk_bytes": { "type": "integer", "minimum": 0 },
                        "post_processing_enabled": { "type": "boolean" },
                        "post_processing_command_count": { "type": "integer", "minimum": 0 },
                        "object_storage_backend": { "type": "string", "enum": ["local", "s3"] },
                        "object_storage_configured": { "type": "boolean" },
                        "object_storage_public_urls": { "type": "boolean" }
                    }
                },
                "RuntimePlatformDownloadPolicy": {
                    "type": "object",
                    "required": ["platform", "cookies_configured", "format_configured", "proxy_configured"],
                    "properties": {
                        "platform": { "type": "string" },
                        "cookies_configured": { "type": "boolean" },
                        "format_configured": { "type": "boolean" },
                        "proxy_configured": { "type": "boolean" },
                        "job_timeout_seconds": { "type": ["integer", "null"], "minimum": 0 },
                        "max_attempts": { "type": ["integer", "null"], "minimum": 1 },
                        "initial_backoff_ms": { "type": ["integer", "null"], "minimum": 0 },
                        "max_concurrent": { "type": ["integer", "null"], "minimum": 1 }
                    }
                },
                "RuntimeWebhookConfig": {
                    "type": "object",
                    "required": ["timeout_seconds", "connect_timeout_seconds", "max_attempts", "initial_backoff_ms", "signing_enabled", "allow_private_webhook_urls"],
                    "properties": {
                        "timeout_seconds": { "type": "integer", "minimum": 0 },
                        "connect_timeout_seconds": { "type": "integer", "minimum": 0 },
                        "max_attempts": { "type": "integer", "minimum": 1 },
                        "initial_backoff_ms": { "type": "integer", "minimum": 0 },
                        "signing_enabled": { "type": "boolean" },
                        "allow_private_webhook_urls": { "type": "boolean" }
                    }
                },
                "RuntimeRetentionConfig": {
                    "type": "object",
                    "required": ["job_retention_limit", "metadata_retention_limit"],
                    "properties": {
                        "job_retention_limit": { "type": "integer", "minimum": 0 },
                        "metadata_retention_limit": { "type": "integer", "minimum": 0 }
                    }
                },
                "CookieProfileListResponse": {
                    "type": "object",
                    "required": ["profiles"],
                    "properties": {
                        "profiles": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/CookieProfileResponse" }
                        }
                    }
                },
                "CookieProfileResponse": {
                    "type": "object",
                    "required": ["name", "configured"],
                    "properties": {
                        "name": { "type": "string" },
                        "configured": { "type": "boolean" },
                        "last_job_id": { "type": ["string", "null"], "format": "uuid" },
                        "last_status": { "type": ["string", "null"], "enum": ["queued", "running", "succeeded", "failed", "canceled", "deleted", null] },
                        "last_status_url": { "type": ["string", "null"] },
                        "last_used_at": { "type": ["string", "null"], "format": "date-time" },
                        "last_success_job_id": { "type": ["string", "null"], "format": "uuid" },
                        "last_success_at": { "type": ["string", "null"], "format": "date-time" },
                        "last_failure_job_id": { "type": ["string", "null"], "format": "uuid" },
                        "last_failure_at": { "type": ["string", "null"], "format": "date-time" },
                        "last_error_kind": { "type": ["string", "null"] },
                        "last_error": { "type": ["string", "null"] }
                    }
                },
                "PlatformListResponse": {
                    "type": "object",
                    "required": ["platforms"],
                    "properties": {
                        "platforms": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/PlatformResponse" }
                        }
                    }
                },
                "PlatformResponse": {
                    "type": "object",
                    "required": ["id", "enabled", "hosts", "path_examples"],
                    "properties": {
                        "id": { "type": "string" },
                        "enabled": { "type": "boolean" },
                        "hosts": { "type": "array", "items": { "type": "string" } },
                        "path_examples": { "type": "array", "items": { "type": "string" } }
                    }
                },
                "QueueStatusResponse": {
                    "type": "object",
                    "required": ["queued", "capacity", "available_slots", "max_urls_per_request", "workers"],
                    "properties": {
                        "queued": { "type": "integer", "minimum": 0 },
                        "capacity": { "type": "integer", "minimum": 0 },
                        "available_slots": { "type": "integer", "minimum": 0 },
                        "max_urls_per_request": { "type": "integer", "minimum": 1 },
                        "workers": { "$ref": "#/components/schemas/WorkerHealth" }
                    }
                },
                "WorkerStatusResponse": {
                    "type": "object",
                    "required": ["workers", "active"],
                    "properties": {
                        "workers": { "$ref": "#/components/schemas/WorkerHealth" },
                        "active": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/WorkerActivity" }
                        }
                    }
                },
                "WorkerActivity": {
                    "type": "object",
                    "required": ["worker_id", "job_id", "status_url", "url", "started_at", "elapsed_ms"],
                    "properties": {
                        "worker_id": { "type": "integer", "minimum": 0 },
                        "job_id": { "type": "string", "format": "uuid" },
                        "status_url": { "type": "string" },
                        "url": { "type": "string", "format": "uri" },
                        "started_at": { "type": "string", "format": "date-time" },
                        "elapsed_ms": { "type": "integer", "minimum": 0 }
                    }
                },
                "StorageCleanupResponse": {
                    "type": "object",
                    "required": ["dry_run", "max_bytes", "current_bytes", "bytes_to_delete", "bytes_after", "jobs_to_delete", "deleted", "failed"],
                    "properties": {
                        "dry_run": { "type": "boolean" },
                        "max_bytes": { "type": ["integer", "null"], "minimum": 0 },
                        "current_bytes": { "type": "integer", "minimum": 0 },
                        "bytes_to_delete": { "type": "integer", "minimum": 0 },
                        "bytes_after": { "type": "integer", "minimum": 0 },
                        "jobs_to_delete": { "type": "array", "items": { "$ref": "#/components/schemas/StorageCleanupCandidate" } },
                        "deleted": { "type": "array", "items": { "$ref": "#/components/schemas/DeleteJobResponse" } },
                        "failed": { "type": "array", "items": { "$ref": "#/components/schemas/BatchJobActionError" } }
                    }
                },
                "StorageCleanupCandidate": {
                    "type": "object",
                    "required": ["id", "status_url", "updated_at", "media_bytes", "directory"],
                    "properties": {
                        "id": { "type": "string", "format": "uuid" },
                        "status_url": { "type": "string" },
                        "updated_at": { "type": "string", "format": "date-time" },
                        "media_bytes": { "type": "integer", "minimum": 0 },
                        "directory": { "type": "string" }
                    }
                },
                "ValidateDownloadsResponse": {
                    "type": "object",
                    "required": ["valid", "urls", "url_count", "max_urls_per_request", "errors"],
                    "properties": {
                        "valid": { "type": "boolean" },
                        "urls": { "type": "array", "items": { "type": "string", "format": "uri" } },
                        "url_count": { "type": "integer", "minimum": 0 },
                        "max_urls_per_request": { "type": "integer", "minimum": 1 },
                        "webhook_url": { "type": ["string", "null"], "format": "uri" },
                        "format": { "type": ["string", "null"] },
                        "cookie_profile": { "type": ["string", "null"] },
                        "errors": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/ValidationErrorResponse" }
                        }
                    }
                },
                "ValidationErrorResponse": {
                    "type": "object",
                    "required": ["field", "message"],
                    "properties": {
                        "field": { "type": "string" },
                        "index": { "type": ["integer", "null"], "minimum": 0 },
                        "value": { "type": ["string", "null"] },
                        "message": { "type": "string" }
                    }
                },
                "JobArtifactsResponse": {
                    "type": "object",
                    "required": [
                        "id",
                        "media_url",
                        "media_inline_url",
                        "info_json_url",
                        "archive_url",
                        "media_bytes",
                        "media_sha256",
                        "media_content_type",
                        "info_json_sha256",
                        "archive_bytes",
                        "archive_sha256"
                    ],
                    "properties": {
                        "id": { "type": "string", "format": "uuid" },
                        "media_url": { "type": "string" },
                        "media_inline_url": { "type": "string" },
                        "info_json_url": { "type": "string" },
                        "archive_url": { "type": "string" },
                        "media_bytes": { "type": "integer", "minimum": 0 },
                        "media_sha256": { "type": "string" },
                        "media_content_type": { "type": "string" },
                        "info_json_sha256": { "type": "string" },
                        "archive_bytes": { "type": "integer", "minimum": 0 },
                        "archive_sha256": { "type": "string" },
                        "extension": { "type": ["string", "null"] }
                    }
                },
                "JobRecord": { "type": "object" },
                "JobSummaryResponse": {
                    "type": "object",
                    "required": ["total", "queued", "running", "succeeded", "failed", "canceled", "deleted"],
                    "properties": {
                        "total": { "type": "integer", "minimum": 0 },
                        "queued": { "type": "integer", "minimum": 0 },
                        "running": { "type": "integer", "minimum": 0 },
                        "succeeded": { "type": "integer", "minimum": 0 },
                        "failed": { "type": "integer", "minimum": 0 },
                        "canceled": { "type": "integer", "minimum": 0 },
                        "deleted": { "type": "integer", "minimum": 0 }
                    }
                },
                "JobQueuePositionResponse": {
                    "type": "object",
                    "required": ["id", "status", "position", "queued_count"],
                    "properties": {
                        "id": { "type": "string", "format": "uuid" },
                        "status": { "type": "string", "enum": ["queued"] },
                        "position": { "type": "integer", "minimum": 1 },
                        "queued_count": { "type": "integer", "minimum": 1 }
                    }
                },
                "HealthResponse": { "type": "object" },
                "WorkerHealth": { "type": "object" },
                "MetricsResponse": { "type": "object" },
                "ErrorResponse": {
                    "type": "object",
                    "required": ["code", "message"],
                    "properties": {
                        "code": { "type": "string" },
                        "message": { "type": "string" }
                    }
                }
            }
        }
    })
}

#[cfg(target_os = "linux")]
fn process_memory_rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(pages * 4096)
}

#[cfg(not(target_os = "linux"))]
fn process_memory_rss_bytes() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};

    use async_channel::{Receiver, bounded};
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::RwLock;
    use tower::ServiceExt;

    use super::*;
    use crate::{
        config::{Config, PlatformDownloadPolicy},
        jobs::{WebhookDeadLetter, WebhookEvent},
        state::{AppMetrics, RateLimiter, WorkerPoolState},
        types::{DownloadAttempt, DownloadMetadata},
    };

    const VIDEO_ETAG: &str =
        "\"sha256-0cab1c9617404faf2b24e221e189ca5945813e14d3f766345b09ca13bbe28ffc\"";
    const EXAMPLE_INFO_JSON_ETAG: &str =
        "\"sha256-d8a17f1503bfdb6feb42dfaa2c5da2d2fea770ac28c5f685bb1105e5a75b2f43\"";

    #[test]
    fn validates_and_deduplicates_supported_urls() {
        let enabled_platforms = platforms::default_enabled_platforms();
        let urls = validate_download_urls(
            &[
                " https://www.tiktok.com/@user/video/123 ".to_string(),
                "https://www.tiktok.com/@user/video/123".to_string(),
                "https://www.instagram.com/reel/abc/".to_string(),
                "https://www.youtube.com/shorts/abc".to_string(),
                "https://youtu.be/abcdefghijk".to_string(),
                "https://www.facebook.com/reel/123".to_string(),
                "https://fb.watch/abc/".to_string(),
                "https://www.snapchat.com/spotlight/abc".to_string(),
                "https://rutube.ru/shorts/abc/".to_string(),
                "https://www.douyin.com/video/6961737553342991651".to_string(),
                "https://www.likee.video/@user/video/123".to_string(),
                "https://vk.com/clip-123_456".to_string(),
                "https://vkvideo.ru/video-123_456".to_string(),
                "https://yappy.media/video/abc".to_string(),
            ],
            20,
            &enabled_platforms,
        )
        .unwrap();

        assert_eq!(urls.len(), 13);
        assert_eq!(urls[0], "https://www.tiktok.com/@user/video/123");
        assert_eq!(urls[1], "https://www.instagram.com/reel/abc/");
        assert_eq!(urls[2], "https://www.youtube.com/shorts/abc");
        assert_eq!(urls[3], "https://youtu.be/abcdefghijk");
        assert_eq!(urls[4], "https://www.facebook.com/reel/123");
        assert_eq!(urls[5], "https://fb.watch/abc/");
        assert_eq!(urls[6], "https://www.snapchat.com/spotlight/abc");
        assert_eq!(urls[7], "https://rutube.ru/shorts/abc/");
        assert_eq!(urls[8], "https://www.douyin.com/video/6961737553342991651");
        assert_eq!(urls[9], "https://www.likee.video/@user/video/123");
        assert_eq!(urls[10], "https://vk.com/clip-123_456");
        assert_eq!(urls[11], "https://vkvideo.ru/video-123_456");
        assert_eq!(urls[12], "https://yappy.media/video/abc");
    }

    #[test]
    fn rejects_unsupported_url_hosts() {
        let enabled_platforms = platforms::default_enabled_platforms();
        let err = validate_download_urls(
            &["https://example.com/video".to_string()],
            10,
            &enabled_platforms,
        )
        .unwrap_err();

        assert!(err.to_string().contains("unsupported URL host"));
    }

    #[test]
    fn rejects_unsupported_paths_on_supported_hosts() {
        let enabled_platforms = platforms::default_enabled_platforms();
        for url in [
            "https://www.youtube.com/watch?v=abc123",
            "https://www.instagram.com/accounts/login/",
            "https://www.tiktok.com/@user",
            "https://rutube.ru/video/0123456789abcdef0123456789abcdef/",
            "https://vk.com/feed",
        ] {
            let err =
                validate_download_urls(&[url.to_string()], 10, &enabled_platforms).unwrap_err();

            assert!(
                err.to_string().contains("unsupported URL path"),
                "expected {url} to be rejected by path, got {err}"
            );
        }
    }

    #[test]
    fn rejects_edge_or_broad_video_hosts_by_default() {
        let enabled_platforms = platforms::default_enabled_platforms();
        for url in [
            "https://www.xiaohongshu.com/explore/abc123",
            "https://www.bilibili.com/video/BV123",
            "https://www.ixigua.com/123",
            "https://www.pinterest.com/pin/123",
            "https://x.com/user/status/123",
            "https://twitter.com/user/status/123",
        ] {
            let err =
                validate_download_urls(&[url.to_string()], 10, &enabled_platforms).unwrap_err();

            assert!(
                err.to_string().contains("unsupported URL host"),
                "expected {url} to be rejected by host, got {err}"
            );
        }
    }

    #[test]
    fn rejects_config_disabled_platforms() {
        let enabled_platforms = vec!["instagram".to_string()];
        let err = validate_download_urls(
            &["https://www.youtube.com/shorts/abcdefghijk".to_string()],
            10,
            &enabled_platforms,
        )
        .unwrap_err();

        assert!(err.to_string().contains("platform `youtube` is disabled"));
    }

    #[test]
    fn rejects_too_many_urls() {
        let enabled_platforms = platforms::default_enabled_platforms();
        let err = validate_download_urls(
            &[
                "https://www.tiktok.com/@a/video/1".to_string(),
                "https://www.instagram.com/reel/b/".to_string(),
            ],
            1,
            &enabled_platforms,
        )
        .unwrap_err();

        assert!(err.to_string().contains("too many URLs"));
    }

    #[test]
    fn validates_download_format() {
        assert_eq!(
            validate_download_format(Some(" bv*+ba/b ".to_string())).unwrap(),
            Some("bv*+ba/b".to_string())
        );
        assert_eq!(
            validate_download_format(Some(" ".to_string())).unwrap(),
            None
        );
        assert!(validate_download_format(Some("best\nworst".to_string())).is_err());
        assert!(validate_download_format(Some("x".repeat(257))).is_err());
    }

    #[tokio::test]
    async fn json_submission_queues_one_job_per_url() {
        let (state, _queue_rx) = test_state(10);
        state.workers.mark_ready();
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/downloads")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "urls": [
                        "https://www.tiktok.com/@user/video/123",
                        "https://www.instagram.com/reel/abc/"
                    ]
                })
                .to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = serde_json::from_slice::<BatchQueueResponse>(&body).unwrap();

        assert_eq!(body.jobs.len(), 2);
    }

    #[tokio::test]
    async fn json_submission_stores_format_on_job_and_queue_request() {
        let (state, queue_rx) = test_state(10);
        state.workers.mark_ready();
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/downloads")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "urls": ["https://www.instagram.com/reel/abc/"],
                    "format": " mp4/best "
                })
                .to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = serde_json::from_slice::<BatchQueueResponse>(&body).unwrap();
        let id = body.jobs[0].id;
        let queued_request = queue_rx.recv().await.unwrap();
        let stored = state.jobs.read().await.get(&id).cloned().unwrap();

        assert_eq!(stored.format.as_deref(), Some("mp4/best"));
        assert_eq!(queued_request.id, id);
        assert_eq!(queued_request.format.as_deref(), Some("mp4/best"));
    }

    #[tokio::test]
    async fn json_submission_stores_cookie_profile_on_job_and_queue_request() {
        let (mut state, queue_rx) = test_state(10);
        Arc::get_mut(&mut state.config)
            .unwrap()
            .cookie_profiles
            .insert(
                "account_a".to_string(),
                PathBuf::from("account-a-cookies.txt"),
            );
        state.workers.mark_ready();
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/downloads")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "urls": ["https://www.instagram.com/reel/abc/"],
                    "cookie_profile": " Account_A "
                })
                .to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = serde_json::from_slice::<BatchQueueResponse>(&body).unwrap();
        let id = body.jobs[0].id;
        let queued_request = queue_rx.recv().await.unwrap();
        let stored = state.jobs.read().await.get(&id).cloned().unwrap();

        assert_eq!(stored.cookie_profile.as_deref(), Some("account_a"));
        assert_eq!(queued_request.id, id);
        assert_eq!(queued_request.cookie_profile.as_deref(), Some("account_a"));
    }

    #[tokio::test]
    async fn json_submission_reuses_successful_duplicate_without_enqueueing() {
        let (state, queue_rx) = test_state(10);
        state.workers.mark_ready();
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let mut existing = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        existing.result = Some(test_download_metadata(&existing));
        let existing_id = existing.id;
        state.jobs.write().await.insert(existing_id, existing);
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/downloads")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "urls": [" https://www.instagram.com/reel/abc/ "] }).to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = serde_json::from_slice::<BatchQueueResponse>(&body).unwrap();
        assert_eq!(body.jobs.len(), 1);
        assert_eq!(body.jobs[0].id, existing_id);
        assert_eq!(body.jobs[0].status, JobStatus::Succeeded);
        assert!(body.jobs[0].existing);
        assert!(queue_rx.is_empty());
    }

    #[tokio::test]
    async fn json_submission_force_queues_duplicate_again() {
        let (state, queue_rx) = test_state(10);
        state.workers.mark_ready();
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let mut existing = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        existing.result = Some(test_download_metadata(&existing));
        let existing_id = existing.id;
        state.jobs.write().await.insert(existing_id, existing);
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/downloads")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "urls": ["https://www.instagram.com/reel/abc/"],
                    "force": true
                })
                .to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = serde_json::from_slice::<BatchQueueResponse>(&body).unwrap();
        assert_eq!(body.jobs.len(), 1);
        assert_ne!(body.jobs[0].id, existing_id);
        assert_eq!(body.jobs[0].status, JobStatus::Queued);
        assert!(!body.jobs[0].existing);
        assert_eq!(
            queue_rx.recv().await.unwrap().url,
            "https://www.instagram.com/reel/abc/"
        );
    }

    #[tokio::test]
    async fn duplicate_hits_do_not_consume_queue_capacity() {
        let (state, _queue_rx) = test_state(1);
        state.workers.mark_ready();
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let mut existing = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        existing.result = Some(test_download_metadata(&existing));
        state.jobs.write().await.insert(existing.id, existing);
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/downloads")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "urls": [
                        "https://www.tiktok.com/@user/video/123",
                        "https://www.instagram.com/reel/abc/"
                    ]
                })
                .to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = serde_json::from_slice::<BatchQueueResponse>(&body).unwrap();
        assert_eq!(body.jobs.len(), 2);
        assert!(!body.jobs[0].existing);
        assert!(body.jobs[1].existing);
    }

    #[tokio::test]
    async fn validate_downloads_returns_normalized_deduped_urls() {
        let (mut state, _queue_rx) = test_state(10);
        Arc::get_mut(&mut state.config)
            .unwrap()
            .cookie_profiles
            .insert(
                "account_a".to_string(),
                PathBuf::from("account-a-cookies.txt"),
            );
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/downloads/validate")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "urls": [
                        " https://www.youtube.com/shorts/abc ",
                        "https://www.youtube.com/shorts/abc",
                        "https://www.instagram.com/reel/def/"
                    ],
                    "webhook_url": " https://example.com/hook ",
                    "format": " mp4/best ",
                    "cookie_profile": " Account_A "
                })
                .to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(body["valid"], json!(true));
        assert_eq!(body["urls"].as_array().unwrap().len(), 2);
        assert_eq!(body["url_count"], json!(2));
        assert_eq!(body["webhook_url"], json!("https://example.com/hook"));
        assert_eq!(body["format"], json!("mp4/best"));
        assert_eq!(body["cookie_profile"], json!("account_a"));
        assert_eq!(body["errors"].as_array().unwrap().len(), 0);
        assert_eq!(state.jobs.read().await.len(), 0);
    }

    #[tokio::test]
    async fn validate_downloads_reports_all_errors_without_enqueueing() {
        let (mut state, _queue_rx) = test_state(10);
        Arc::get_mut(&mut state.config)
            .unwrap()
            .max_urls_per_request = 1;
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/downloads/validate")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "urls": [
                        "https://www.youtube.com/shorts/abc",
                        "https://example.com/video"
                    ],
                    "webhook_url": "ftp://example.com/hook",
                    "format": "best\nworst",
                    "cookie_profile": "missing"
                })
                .to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let errors = body["errors"].as_array().unwrap();

        assert_eq!(body["valid"], json!(false));
        assert_eq!(body["urls"], json!(["https://www.youtube.com/shorts/abc"]));
        assert_eq!(body["url_count"], json!(1));
        assert_eq!(body["max_urls_per_request"], json!(1));
        assert!(errors.iter().any(|error| {
            error["field"] == "urls" && error["message"].as_str().unwrap().contains("too many")
        }));
        assert!(errors.iter().any(|error| {
            error["field"] == "urls"
                && error["index"] == json!(1)
                && error["message"]
                    .as_str()
                    .unwrap()
                    .contains("unsupported URL host")
        }));
        assert!(errors.iter().any(|error| {
            error["field"] == "webhook_url"
                && error["message"]
                    .as_str()
                    .unwrap()
                    .contains("unsupported webhook_url scheme")
        }));
        assert!(errors.iter().any(|error| {
            error["field"] == "format"
                && error["message"]
                    .as_str()
                    .unwrap()
                    .contains("control characters")
        }));
        assert!(errors.iter().any(|error| {
            error["field"] == "cookie_profile"
                && error["message"]
                    .as_str()
                    .unwrap()
                    .contains("unknown cookie profile")
        }));
        assert_eq!(state.jobs.read().await.len(), 0);
    }

    #[tokio::test]
    async fn invalid_url_rejects_before_enqueueing() {
        let (state, _queue_rx) = test_state(10);
        state.workers.mark_ready();
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/downloads")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "urls": ["https://example.com"] }).to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(state.jobs.read().await.len(), 0);
    }

    #[tokio::test]
    async fn invalid_format_rejects_before_enqueueing() {
        let (state, _queue_rx) = test_state(10);
        state.workers.mark_ready();
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/downloads")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "urls": ["https://www.instagram.com/reel/abc/"],
                    "format": "best\nworst"
                })
                .to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(state.jobs.read().await.len(), 0);
    }

    #[tokio::test]
    async fn unknown_cookie_profile_rejects_before_enqueueing() {
        let (state, _queue_rx) = test_state(10);
        state.workers.mark_ready();
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/downloads")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "urls": ["https://www.instagram.com/reel/abc/"],
                    "cookie_profile": "missing"
                })
                .to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(state.jobs.read().await.len(), 0);
    }

    #[tokio::test]
    async fn queue_full_returns_service_unavailable() {
        let (state, _queue_rx) = test_state(1);
        state.workers.mark_ready();
        state
            .queue_tx
            .try_send(crate::jobs::JobRequest {
                id: Uuid::new_v4(),
                url: "https://www.tiktok.com/@busy/video/1".to_string(),
                format: None,
                cookie_profile: None,
            })
            .unwrap();
        let app = router(state);
        let request = Request::builder()
            .method("POST")
            .uri("/v1/downloads")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "urls": ["https://www.instagram.com/reel/abc/"] }).to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn get_job_returns_record() {
        let (state, _queue_rx) = test_state(10);
        let record = JobRecord {
            id: Uuid::new_v4(),
            status: JobStatus::Queued,
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            url: "https://www.tiktok.com/@user/video/123".to_string(),
            url_sha256: Some(download_url_sha256(
                "https://www.tiktok.com/@user/video/123",
            )),
            webhook_url: None,
            format: None,
            cookie_profile: None,
            result: None,
            attempts: 0,
            attempt_errors: Vec::new(),
            error_kind: None,
            error: None,
        };
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state);
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let got = serde_json::from_slice::<JobRecord>(&body).unwrap();

        assert_eq!(got.id, record.id);
        assert_eq!(got.url, record.url);
    }

    #[tokio::test]
    async fn wait_for_job_returns_terminal_job_immediately() {
        let (state, _queue_rx) = test_state(10);
        let record = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state);
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/wait?timeout_seconds=30", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let got = serde_json::from_slice::<JobRecord>(&body).unwrap();

        assert_eq!(got.id, record.id);
        assert_eq!(got.status, JobStatus::Succeeded);
    }

    #[tokio::test]
    async fn wait_for_job_returns_current_active_job_after_timeout() {
        let (state, _queue_rx) = test_state(10);
        let record = test_job(JobStatus::Queued, "https://www.instagram.com/reel/abc/");
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state);
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/wait?timeout_seconds=0", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let got = serde_json::from_slice::<JobRecord>(&body).unwrap();

        assert_eq!(got.id, record.id);
        assert_eq!(got.status, JobStatus::Queued);
    }

    #[tokio::test]
    async fn export_jobs_jsonl_returns_filtered_records() {
        let (state, _queue_rx) = test_state(10);
        let failed = test_job(JobStatus::Failed, "https://www.instagram.com/reel/failed/");
        let queued = test_job(JobStatus::Queued, "https://www.instagram.com/reel/queued/");
        state.jobs.write().await.insert(failed.id, failed.clone());
        state.jobs.write().await.insert(queued.id, queued.clone());
        let app = router(state);
        let request = Request::builder()
            .uri("/v1/jobs/export?status=failed&platform=instagram&q=failed&format=jsonl")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/x-ndjson"
        );
        assert_eq!(
            response.headers().get(header::CONTENT_DISPOSITION).unwrap(),
            "attachment; filename=\"jobs.jsonl\""
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let lines = String::from_utf8(body.to_vec()).unwrap();
        let records = lines
            .lines()
            .map(|line| serde_json::from_str::<JobRecord>(line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, failed.id);
    }

    #[tokio::test]
    async fn export_jobs_csv_escapes_flat_job_fields() {
        let (state, _queue_rx) = test_state(10);
        let mut record = test_job(
            JobStatus::Failed,
            "https://www.instagram.com/reel/abc/?caption=hello,world",
        );
        record.error = Some("quoted \"error\", with comma".to_string());
        record.result = Some(DownloadMetadata {
            original_url: record.url.clone(),
            webpage_url: None,
            extractor: Some("Instagram".to_string()),
            title: Some("hello, \"csv\"".to_string()),
            uploader: Some("creator".to_string()),
            duration: Some(12.5),
            extension: Some("mp4".to_string()),
            media_path: PathBuf::from("data/downloads/job/video.mp4"),
            media_bytes: 5,
            info_json_path: PathBuf::from("data/downloads/job/info.json"),
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 123,
            post_processing: Vec::new(),
            storage: None,
        });
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state);
        let request = Request::builder()
            .uri("/v1/jobs/export?format=csv")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/csv; charset=utf-8"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(body.starts_with("id,status,created_at,updated_at,url,platform,"));
        assert!(body.contains("\"https://www.instagram.com/reel/abc/?caption=hello,world\""));
        assert!(body.contains("\"quoted \"\"error\"\", with comma\""));
        assert!(body.contains("\"hello, \"\"csv\"\"\""));
    }

    #[tokio::test]
    async fn export_jobs_rejects_unknown_format() {
        let (state, _queue_rx) = test_state(10);
        let app = router(state);
        let request = Request::builder()
            .uri("/v1/jobs/export?format=xlsx")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn job_detail_page_renders_job_metadata_and_actions() {
        let (state, _queue_rx) = test_state(10);
        let mut record = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        record.webhook_url = Some("http://127.0.0.1:1/webhook".to_string());
        let media_path = state
            .config
            .downloads_dir
            .join(record.id.to_string())
            .join(format!("{}.mp4", record.id));
        let info_json_path = state
            .config
            .downloads_dir
            .join(record.id.to_string())
            .join(format!("{}.info.json", record.id));
        tokio::fs::create_dir_all(media_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&media_path, b"video").await.unwrap();
        tokio::fs::write(&info_json_path, b"{}").await.unwrap();
        let media_name = archive_entry_name(&media_path, "download.bin");
        let info_json_name = archive_entry_name(&info_json_path, "info.json");
        let expected_archive_sha256 = archive_sha256(&[
            TarFileEntry {
                name: &media_name,
                path: &media_path,
            },
            TarFileEntry {
                name: &info_json_name,
                path: &info_json_path,
            },
        ])
        .await
        .unwrap();
        record.attempts = 2;
        record.format = Some("mp4/best".to_string());
        record.cookie_profile = Some("account_a".to_string());
        record.attempt_errors.push(DownloadAttempt {
            attempt: 1,
            error: "temporary rate limit".to_string(),
            elapsed_ms: 42,
            retry_backoff_ms: Some(100),
        });
        record.result = Some(DownloadMetadata {
            original_url: record.url.clone(),
            webpage_url: Some("https://www.instagram.com/reel/final/".to_string()),
            extractor: Some("Instagram".to_string()),
            title: Some("test reel".to_string()),
            uploader: Some("creator".to_string()),
            duration: Some(12.3456),
            extension: Some("mp4".to_string()),
            media_path,
            media_bytes: 5,
            info_json_path,
            yt_dlp_version: "2026.01.01".to_string(),
            elapsed_ms: 1234,
            post_processing: Vec::new(),
            storage: None,
        });
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state);
        let request = Request::builder()
            .uri(format!("/jobs/{}", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(body.contains(&record.id.to_string()));
        assert!(body.contains("test reel"));
        assert!(body.contains("mp4/best"));
        assert!(body.contains("account_a"));
        assert!(body.contains("Instagram"));
        assert!(body.contains("12.346 s"));
        assert!(body.contains("0cab1c9617404faf2b24e221e189ca5945813e14d3f766345b09ca13bbe28ffc"));
        assert!(body.contains("44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"));
        assert!(body.contains("Archive bytes"));
        assert!(body.contains(">3072<"));
        assert!(body.contains(&expected_archive_sha256));
        assert!(body.contains("temporary rate limit"));
        assert!(body.contains("<video"));
        assert!(body.contains(&format!("/v1/jobs/{}/media-inline", record.id)));
        assert!(body.contains(&format!("/v1/jobs/{}/media", record.id)));
        assert!(body.contains(&format!("/v1/jobs/{}/info-json", record.id)));
        assert!(body.contains(&format!("/v1/jobs/{}/archive", record.id)));
        assert!(body.contains(&format!("/jobs-form/{}/webhook", record.id)));
        assert!(body.contains(&format!("/jobs-form/{}/retry", record.id)));
        assert!(body.contains(&format!("/jobs-form/{}/delete", record.id)));
    }

    #[tokio::test]
    async fn prometheus_metrics_endpoint_returns_text_metrics() {
        let (state, _queue_rx) = test_state(10);
        state.workers.mark_ready();
        state.metrics.record_job_started();
        let app = router(state);
        let request = Request::builder()
            .uri("/metrics.prometheus")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; version=0.0.4"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(body.contains("# TYPE yt_dlp_server_jobs_started_total counter"));
        assert!(body.contains("yt_dlp_server_jobs_started_total 1"));
        assert!(body.contains("yt_dlp_server_workers_ready 1"));
    }

    #[tokio::test]
    async fn runtime_config_reports_effective_settings_without_secret_values() {
        let (mut state, _queue_rx) = test_state(10);
        let config = Arc::get_mut(&mut state.config).unwrap();
        config.api_keys = vec!["super-secret-api-key".to_string()];
        config.cookies_path = Some(PathBuf::from("/private/cookies.txt"));
        config.cookie_profiles.insert(
            "account_a".to_string(),
            PathBuf::from("/private/account-a-cookies.txt"),
        );
        config.format = Some("mp4/best".to_string());
        config.proxy = Some("http://user:password@127.0.0.1:8080".to_string());
        config.webhook_signing_secret = Some("webhook-secret".to_string());
        config.download_enabled_platforms = vec!["youtube".to_string(), "instagram".to_string()];
        config.platform_policies.insert(
            "instagram".to_string(),
            PlatformDownloadPolicy {
                cookies_path: Some(PathBuf::from("/private/ig-auth.txt")),
                format: Some("mp4/best".to_string()),
                proxy: Some("http://instagram-proxy".to_string()),
                job_timeout_seconds: Some(90),
                download_max_attempts: Some(6),
                download_initial_backoff_ms: Some(750),
                max_concurrent: Some(1),
            },
        );
        config.download_max_attempts = 4;
        config.download_initial_backoff_ms = 250;
        let app = router(state);
        let request = Request::builder()
            .uri("/v1/config")
            .header("x-api-key", "super-secret-api-key")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_text = String::from_utf8(body.to_vec()).unwrap();
        let body: serde_json::Value = serde_json::from_str(&body_text).unwrap();

        assert_eq!(body["server"]["api_key_auth_enabled"], json!(true));
        assert_eq!(body["download"]["cookies_configured"], json!(true));
        assert_eq!(body["download"]["cookie_profiles"], json!(["account_a"]));
        assert_eq!(body["download"]["format_configured"], json!(true));
        assert_eq!(body["download"]["proxy_configured"], json!(true));
        assert_eq!(body["webhooks"]["signing_enabled"], json!(true));
        assert_eq!(
            body["download"]["enabled_platforms"],
            json!(["youtube", "instagram"])
        );
        assert_eq!(body["download"]["max_attempts"], json!(4));
        assert_eq!(body["download"]["initial_backoff_ms"], json!(250));
        assert_eq!(
            body["download"]["platform_policies"],
            json!([
                {
                    "platform": "instagram",
                    "cookies_configured": true,
                    "format_configured": true,
                    "proxy_configured": true,
                    "job_timeout_seconds": 90,
                    "max_attempts": 6,
                    "initial_backoff_ms": 750,
                    "max_concurrent": 1
                }
            ])
        );
        assert!(!body_text.contains("super-secret-api-key"));
        assert!(!body_text.contains("cookies.txt"));
        assert!(!body_text.contains("account-a-cookies.txt"));
        assert!(!body_text.contains("ig-auth.txt"));
        assert!(!body_text.contains("password"));
        assert!(!body_text.contains("instagram-proxy"));
        assert!(!body_text.contains("webhook-secret"));
    }

    #[tokio::test]
    async fn cookie_profiles_report_retained_job_health_without_paths() {
        let (mut state, _queue_rx) = test_state(10);
        Arc::get_mut(&mut state.config)
            .unwrap()
            .cookie_profiles
            .extend([
                (
                    "account_a".to_string(),
                    PathBuf::from("/private/account-a-cookies.txt"),
                ),
                (
                    "account_b".to_string(),
                    PathBuf::from("/private/account-b-cookies.txt"),
                ),
            ]);

        let now = OffsetDateTime::now_utc();
        let mut success = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        success.cookie_profile = Some("account_a".to_string());
        success.updated_at = now - time::Duration::minutes(10);
        let mut failure = test_job(JobStatus::Failed, "https://www.instagram.com/reel/def/");
        failure.cookie_profile = Some("account_a".to_string());
        failure.updated_at = now;
        failure.error_kind = Some("authentication_required".to_string());
        failure.error = Some("please sign in with cookies".to_string());
        state
            .jobs
            .write()
            .await
            .extend([(success.id, success.clone()), (failure.id, failure.clone())]);

        let app = router(state);
        let request = Request::builder()
            .uri("/v1/cookie-profiles")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_text = String::from_utf8(body.to_vec()).unwrap();
        let body: serde_json::Value = serde_json::from_str(&body_text).unwrap();
        let profiles = body["profiles"].as_array().unwrap();
        let account_a = profiles
            .iter()
            .find(|profile| profile["name"] == "account_a")
            .unwrap();
        let account_b = profiles
            .iter()
            .find(|profile| profile["name"] == "account_b")
            .unwrap();

        assert_eq!(profiles.len(), 2);
        assert_eq!(account_a["configured"], json!(true));
        assert_eq!(account_a["last_job_id"], json!(failure.id));
        assert_eq!(account_a["last_status"], json!("failed"));
        assert_eq!(
            account_a["last_status_url"],
            json!(format!("/v1/jobs/{}", failure.id))
        );
        assert_eq!(account_a["last_success_job_id"], json!(success.id));
        assert_eq!(account_a["last_failure_job_id"], json!(failure.id));
        assert_eq!(
            account_a["last_error_kind"],
            json!("authentication_required")
        );
        assert_eq!(
            account_a["last_error"],
            json!("please sign in with cookies")
        );
        assert!(account_a["last_used_at"].as_str().unwrap().ends_with('Z'));
        assert_eq!(account_b["last_job_id"], json!(null));
        assert_eq!(account_b["last_status"], json!(null));
        assert!(!body_text.contains("account-a-cookies.txt"));
        assert!(!body_text.contains("account-b-cookies.txt"));
    }

    #[tokio::test]
    async fn list_platforms_reports_enabled_platforms_and_url_shapes() {
        let (mut state, _queue_rx) = test_state(10);
        Arc::get_mut(&mut state.config)
            .unwrap()
            .download_enabled_platforms = vec!["youtube".to_string()];
        let app = router(state);
        let request = Request::builder()
            .uri("/v1/platforms")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let platforms = body["platforms"].as_array().unwrap();
        let youtube = platforms
            .iter()
            .find(|platform| platform["id"] == "youtube")
            .unwrap();
        let instagram = platforms
            .iter()
            .find(|platform| platform["id"] == "instagram")
            .unwrap();

        assert_eq!(platforms.len(), platforms::known_platforms().len());
        assert_eq!(youtube["enabled"], json!(true));
        assert_eq!(instagram["enabled"], json!(false));
        assert!(
            youtube["hosts"]
                .as_array()
                .unwrap()
                .contains(&json!("youtu.be"))
        );
        assert!(
            youtube["path_examples"]
                .as_array()
                .unwrap()
                .contains(&json!("/shorts/<id>"))
        );
    }

    #[tokio::test]
    async fn queue_status_reports_capacity_slots_and_worker_health() {
        let (state, _queue_rx) = test_state(3);
        state.workers.mark_ready();
        state
            .queue_tx
            .try_send(crate::jobs::JobRequest {
                id: Uuid::new_v4(),
                url: "https://www.youtube.com/shorts/abc".to_string(),
                format: None,
                cookie_profile: None,
            })
            .unwrap();
        let app = router(state);
        let request = Request::builder()
            .uri("/v1/queue")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(body["queued"], json!(1));
        assert_eq!(body["capacity"], json!(3));
        assert_eq!(body["available_slots"], json!(2));
        assert_eq!(body["max_urls_per_request"], json!(100));
        assert_eq!(body["workers"]["expected"], json!(1));
        assert_eq!(body["workers"]["ready"], json!(1));
        assert_eq!(body["workers"]["failed"], json!(0));
    }

    #[tokio::test]
    async fn worker_status_reports_active_jobs() {
        let (state, _queue_rx) = test_state(3);
        state.workers.mark_ready();
        let job_id = Uuid::new_v4();
        state
            .workers
            .mark_active(0, job_id, "https://www.instagram.com/reel/abc/".to_string());
        let app = router(state);
        let request = Request::builder()
            .uri("/v1/workers")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(body["workers"]["expected"], json!(1));
        assert_eq!(body["workers"]["ready"], json!(1));
        assert_eq!(body["active"].as_array().unwrap().len(), 1);
        assert_eq!(body["active"][0]["worker_id"], json!(0));
        assert_eq!(body["active"][0]["job_id"], json!(job_id));
        assert_eq!(
            body["active"][0]["status_url"],
            json!(format!("/v1/jobs/{job_id}"))
        );
        assert_eq!(
            body["active"][0]["url"],
            json!("https://www.instagram.com/reel/abc/")
        );
        assert!(body["active"][0]["elapsed_ms"].as_u64().is_some());
        assert!(body["active"][0]["started_at"].as_str().is_some());
    }

    #[tokio::test]
    async fn storage_cleanup_preview_reports_oldest_candidates_without_deleting() {
        let (state, _queue_rx) = test_state(10);
        let older = insert_succeeded_download(&state, b"old1", br#"{"id":"old"}"#).await;
        let newer = insert_succeeded_download(&state, b"new1", br#"{"id":"new"}"#).await;
        {
            let mut jobs = state.jobs.write().await;
            jobs.get_mut(&older.id).unwrap().updated_at -= time::Duration::seconds(60);
        }
        let older_dir = older
            .result
            .as_ref()
            .unwrap()
            .media_path
            .parent()
            .unwrap()
            .to_path_buf();
        let app = router(state.clone());
        let request = Request::builder()
            .uri("/v1/storage/cleanup?max_bytes=4")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["dry_run"], json!(true));
        assert_eq!(body["max_bytes"], json!(4));
        assert_eq!(body["current_bytes"], json!(8));
        assert_eq!(body["bytes_to_delete"], json!(4));
        assert_eq!(body["bytes_after"], json!(4));
        assert_eq!(body["jobs_to_delete"].as_array().unwrap().len(), 1);
        assert_eq!(body["jobs_to_delete"][0]["id"], json!(older.id));
        assert_eq!(body["deleted"].as_array().unwrap().len(), 0);
        assert!(older_dir.exists());
        assert_eq!(
            state.jobs.read().await.get(&newer.id).unwrap().status,
            JobStatus::Succeeded
        );
    }

    #[tokio::test]
    async fn storage_cleanup_execute_deletes_oldest_candidates() {
        let (state, _queue_rx) = test_state(10);
        let older = insert_succeeded_download(&state, b"old1", br#"{"id":"old"}"#).await;
        let newer = insert_succeeded_download(&state, b"new1", br#"{"id":"new"}"#).await;
        {
            let mut jobs = state.jobs.write().await;
            jobs.get_mut(&older.id).unwrap().updated_at -= time::Duration::seconds(60);
        }
        let older_dir = older
            .result
            .as_ref()
            .unwrap()
            .media_path
            .parent()
            .unwrap()
            .to_path_buf();
        let newer_dir = newer
            .result
            .as_ref()
            .unwrap()
            .media_path
            .parent()
            .unwrap()
            .to_path_buf();
        let app = router(state.clone());
        let request = Request::builder()
            .method(Method::POST)
            .uri("/v1/storage/cleanup?max_bytes=4")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["dry_run"], json!(false));
        assert_eq!(body["bytes_to_delete"], json!(4));
        assert_eq!(body["bytes_after"], json!(4));
        assert_eq!(body["deleted"].as_array().unwrap().len(), 1);
        assert_eq!(body["deleted"][0]["id"], json!(older.id));
        assert_eq!(body["failed"].as_array().unwrap().len(), 0);
        assert!(!older_dir.exists());
        assert!(newer_dir.exists());
        let jobs = state.jobs.read().await;
        assert_eq!(jobs.get(&older.id).unwrap().status, JobStatus::Deleted);
        assert_eq!(jobs.get(&newer.id).unwrap().status, JobStatus::Succeeded);
    }

    #[tokio::test]
    async fn job_queue_position_returns_one_based_position() {
        let (state, _queue_rx) = test_state(10);
        let mut first = test_job(JobStatus::Queued, "https://www.tiktok.com/@user/video/1");
        let mut second = test_job(JobStatus::Queued, "https://www.tiktok.com/@user/video/2");
        let mut running = test_job(JobStatus::Running, "https://www.tiktok.com/@user/video/3");
        let now = OffsetDateTime::now_utc();
        first.created_at = now;
        second.created_at = now + time::Duration::seconds(1);
        running.created_at = first.created_at;
        {
            let mut jobs = state.jobs.write().await;
            jobs.insert(second.id, second.clone());
            jobs.insert(running.id, running);
            jobs.insert(first.id, first);
        }
        let app = router(state);
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/queue-position", second.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(body["id"], json!(second.id));
        assert_eq!(body["status"], json!("queued"));
        assert_eq!(body["position"], json!(2));
        assert_eq!(body["queued_count"], json!(2));
    }

    #[tokio::test]
    async fn job_queue_position_rejects_non_queued_job() {
        let (state, _queue_rx) = test_state(10);
        let succeeded = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        state
            .jobs
            .write()
            .await
            .insert(succeeded.id, succeeded.clone());
        let app = router(state);
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/queue-position", succeeded.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], json!("conflict"));
    }

    #[tokio::test]
    async fn basic_auth_password_is_accepted_as_api_key() {
        let (state, _queue_rx) =
            test_state_with_api_keys(10, vec!["replace-with-a-long-random-secret".to_string()]);
        let credentials = BASE64_STANDARD.encode("browser:replace-with-a-long-random-secret");
        let app = router(state);
        let request = Request::builder()
            .uri("/metrics")
            .header(header::AUTHORIZATION, format!("Basic {credentials}"))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn missing_api_key_returns_basic_auth_challenge() {
        let (state, _queue_rx) = test_state_with_api_keys(10, vec!["secret".to_string()]);
        let app = router(state);
        let request = Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            "Basic realm=\"yt-dlp-server\", Bearer"
        );
    }

    #[tokio::test]
    async fn list_jobs_filters_by_status() {
        let (state, _queue_rx) = test_state(10);
        let queued = test_job(JobStatus::Queued, "https://www.tiktok.com/@user/video/123");
        let failed = test_job(JobStatus::Failed, "https://www.instagram.com/reel/abc/");
        state.jobs.write().await.insert(queued.id, queued);
        state.jobs.write().await.insert(failed.id, failed.clone());
        let app = router(state);
        let request = Request::builder()
            .uri("/v1/jobs?status=failed")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let got = serde_json::from_slice::<JobListResponse>(&body).unwrap();

        assert_eq!(got.total, 1);
        assert_eq!(got.jobs[0].id, failed.id);
    }

    #[tokio::test]
    async fn list_jobs_filters_by_platform_and_query() {
        let (state, _queue_rx) = test_state(10);
        let mut instagram = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        instagram.result = Some(DownloadMetadata {
            original_url: instagram.url.clone(),
            webpage_url: Some("https://www.instagram.com/reel/abc/".to_string()),
            extractor: Some("Instagram".to_string()),
            title: Some("Creator Launch Clip".to_string()),
            uploader: Some("alice".to_string()),
            duration: Some(10.0),
            extension: Some("mp4".to_string()),
            media_path: PathBuf::from("data/downloads/job/video.mp4"),
            media_bytes: 5,
            info_json_path: PathBuf::from("data/downloads/job/info.json"),
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 123,
            post_processing: Vec::new(),
            storage: None,
        });
        let tiktok = test_job(
            JobStatus::Succeeded,
            "https://www.tiktok.com/@user/video/123",
        );
        state
            .jobs
            .write()
            .await
            .insert(instagram.id, instagram.clone());
        state.jobs.write().await.insert(tiktok.id, tiktok);
        let app = router(state);
        let request = Request::builder()
            .uri("/v1/jobs?platform=instagram&q=launch")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let got = serde_json::from_slice::<JobListResponse>(&body).unwrap();

        assert_eq!(got.total, 1);
        assert_eq!(got.jobs[0].id, instagram.id);
    }

    #[tokio::test]
    async fn list_jobs_rejects_unknown_platform_filter() {
        let (state, _queue_rx) = test_state(10);
        let app = router(state);
        let request = Request::builder()
            .uri("/v1/jobs?platform=unknown")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn job_summary_returns_current_status_counts() {
        let (state, _queue_rx) = test_state(10);
        for status in [
            JobStatus::Queued,
            JobStatus::Running,
            JobStatus::Succeeded,
            JobStatus::Failed,
            JobStatus::Canceled,
            JobStatus::Deleted,
        ] {
            let record = test_job(status, "https://www.instagram.com/reel/abc/");
            state.jobs.write().await.insert(record.id, record);
        }
        let app = router(state);
        let request = Request::builder()
            .uri("/v1/jobs/summary")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(body["total"], json!(6));
        assert_eq!(body["queued"], json!(1));
        assert_eq!(body["running"], json!(1));
        assert_eq!(body["succeeded"], json!(1));
        assert_eq!(body["failed"], json!(1));
        assert_eq!(body["canceled"], json!(1));
        assert_eq!(body["deleted"], json!(1));
    }

    #[tokio::test]
    async fn index_page_renders_job_and_queue_summary() {
        let (state, _queue_rx) = test_state(10);
        state.workers.mark_ready();
        for status in [
            JobStatus::Queued,
            JobStatus::Running,
            JobStatus::Succeeded,
            JobStatus::Failed,
            JobStatus::Canceled,
            JobStatus::Deleted,
        ] {
            let record = test_job(status, "https://www.instagram.com/reel/abc/");
            state.jobs.write().await.insert(record.id, record);
        }
        state
            .queue_tx
            .try_send(crate::jobs::JobRequest {
                id: Uuid::new_v4(),
                url: "https://www.youtube.com/shorts/abc".to_string(),
                format: None,
                cookie_profile: None,
            })
            .unwrap();
        let app = router(state);
        let request = Request::builder().uri("/").body(Body::empty()).unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(body.contains("Current server summary"));
        assert!(body.contains("<strong>6</strong><span>Total</span>"));
        assert!(body.contains("<strong>1</strong><span>Queued</span>"));
        assert!(body.contains("<strong>9/10</strong><span>Queue slots</span>"));
        assert!(body.contains("<strong>1/1</strong><span>Workers</span>"));
    }

    #[tokio::test]
    async fn jobs_page_filters_and_paginates_records() {
        let (state, _queue_rx) = test_state(10);
        let failed_older = test_job(JobStatus::Failed, "https://www.instagram.com/reel/old/");
        let mut failed_newer = test_job(JobStatus::Failed, "https://www.instagram.com/reel/new/");
        failed_newer.updated_at += time::Duration::seconds(60);
        let succeeded = test_job(JobStatus::Succeeded, "https://www.youtube.com/shorts/abc");
        state
            .jobs
            .write()
            .await
            .insert(failed_older.id, failed_older.clone());
        state
            .jobs
            .write()
            .await
            .insert(failed_newer.id, failed_newer.clone());
        state.jobs.write().await.insert(succeeded.id, succeeded);
        let app = router(state);
        let request = Request::builder()
            .uri("/jobs?status=failed&limit=1&offset=0")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(body.contains("Job History"));
        assert!(body.contains(&failed_newer.id.to_string()));
        assert!(!body.contains(&failed_older.id.to_string()));
        assert!(!body.contains("https://www.youtube.com/shorts/abc"));
        assert!(body.contains("Next"));
        assert!(body.contains("offset=1"));
        assert!(body.contains(&format!("/jobs/{}", failed_newer.id)));
    }

    #[tokio::test]
    async fn list_webhook_dead_letters_returns_recent_failures() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let older_id = Uuid::new_v4();
        let newer_id = Uuid::new_v4();
        append_jsonl(
            &state.config.webhooks_dead_letter_jsonl,
            &test_dead_letter(older_id, "http://127.0.0.1:1/older", -60),
        )
        .await
        .unwrap();
        append_jsonl(
            &state.config.webhooks_dead_letter_jsonl,
            &test_dead_letter(newer_id, "http://127.0.0.1:1/newer", 0),
        )
        .await
        .unwrap();
        let app = router(state);
        let request = Request::builder()
            .uri("/v1/webhooks/dead-letters?limit=1")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = serde_json::from_slice::<serde_json::Value>(&body).unwrap();

        assert_eq!(body["total"], 2);
        assert_eq!(body["dead_letters"].as_array().unwrap().len(), 1);
        assert_eq!(
            body["dead_letters"][0]["event"]["event_id"],
            newer_id.to_string()
        );
    }

    #[tokio::test]
    async fn replay_webhook_dead_letter_delivers_and_removes_entry() {
        let (webhook_url, received_request) = one_shot_webhook_server().await;
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let event_id = Uuid::new_v4();
        append_jsonl(
            &state.config.webhooks_dead_letter_jsonl,
            &test_dead_letter(event_id, &webhook_url, 0),
        )
        .await
        .unwrap();
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri(format!("/v1/webhooks/dead-letters/{event_id}/replay"))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        let request_text = received_request.await.unwrap();
        let remaining = load_webhook_dead_letters(&state.config.webhooks_dead_letter_jsonl)
            .await
            .unwrap();

        assert_eq!(body["event_id"], event_id.to_string());
        assert_eq!(body["delivered"], true);
        assert_eq!(body["removed"], true);
        assert!(request_text.contains(&format!("x-download-event-id: {event_id}")));
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn dismiss_webhook_dead_letter_removes_entry_without_delivery() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let event_id = Uuid::new_v4();
        append_jsonl(
            &state.config.webhooks_dead_letter_jsonl,
            &test_dead_letter(event_id, "http://127.0.0.1:1/webhook", 0),
        )
        .await
        .unwrap();
        let app = router(state.clone());
        let request = Request::builder()
            .method("DELETE")
            .uri(format!("/v1/webhooks/dead-letters/{event_id}"))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        let remaining = load_webhook_dead_letters(&state.config.webhooks_dead_letter_jsonl)
            .await
            .unwrap();

        assert_eq!(body["event_id"], event_id.to_string());
        assert_eq!(body["removed"], true);
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn dismiss_webhook_dead_letter_returns_not_found_for_missing_event() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let app = router(state);
        let request = Request::builder()
            .method("DELETE")
            .uri(format!("/v1/webhooks/dead-letters/{}", Uuid::new_v4()))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn redeliver_job_webhook_sends_terminal_job_event() {
        let (webhook_url, received_request) = one_shot_webhook_server().await;
        let (state, _queue_rx) = test_state(10);
        let mut record = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        record.webhook_url = Some(webhook_url);
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state);
        let request = Request::builder()
            .method("POST")
            .uri(format!("/v1/jobs/{}/webhook", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        let request_text = received_request.await.unwrap();

        assert_eq!(body["delivered"], true);
        assert_eq!(body["removed"], false);
        assert!(request_text.contains("x-download-event-type: job.completed"));
        assert!(request_text.contains(&record.id.to_string()));
    }

    #[tokio::test]
    async fn redeliver_job_webhook_rejects_non_terminal_job() {
        let (state, _queue_rx) = test_state(10);
        let mut record = test_job(JobStatus::Queued, "https://www.instagram.com/reel/abc/");
        record.webhook_url = Some("http://127.0.0.1:1/webhook".to_string());
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state);
        let request = Request::builder()
            .method("POST")
            .uri(format!("/v1/jobs/{}/webhook", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn redeliver_job_webhook_form_delivers_and_renders_notice() {
        let (webhook_url, received_request) = one_shot_webhook_server().await;
        let (state, _queue_rx) = test_state(10);
        let mut record = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        record.webhook_url = Some(webhook_url);
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state);
        let request = Request::builder()
            .method("POST")
            .uri(format!("/jobs-form/{}/webhook", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        let request_text = received_request.await.unwrap();

        assert!(body.contains("Redelivered webhook"));
        assert!(request_text.contains(&record.id.to_string()));
    }

    #[tokio::test]
    async fn webhook_dead_letters_page_renders_failed_deliveries() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let event_id = Uuid::new_v4();
        append_jsonl(
            &state.config.webhooks_dead_letter_jsonl,
            &test_dead_letter(event_id, "http://127.0.0.1:1/webhook", 0),
        )
        .await
        .unwrap();
        let app = router(state);
        let request = Request::builder()
            .uri("/webhooks/dead-letters")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(body.contains("Webhook Dead Letters"));
        assert!(body.contains(&event_id.to_string()));
        assert!(body.contains("/webhooks/dead-letters/"));
        assert!(body.contains("Replay"));
        assert!(body.contains("Dismiss"));
    }

    #[tokio::test]
    async fn webhook_dead_letter_dismiss_form_removes_entry_and_renders_notice() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let event_id = Uuid::new_v4();
        append_jsonl(
            &state.config.webhooks_dead_letter_jsonl,
            &test_dead_letter(event_id, "http://127.0.0.1:1/webhook", 0),
        )
        .await
        .unwrap();
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri(format!("/webhooks/dead-letters/{event_id}/dismiss"))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        let remaining = load_webhook_dead_letters(&state.config.webhooks_dead_letter_jsonl)
            .await
            .unwrap();

        assert!(body.contains("Dismissed webhook dead letter"));
        assert!(body.contains("No webhook dead letters."));
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn webhook_dead_letter_replay_form_delivers_and_renders_notice() {
        let (webhook_url, received_request) = one_shot_webhook_server().await;
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let event_id = Uuid::new_v4();
        append_jsonl(
            &state.config.webhooks_dead_letter_jsonl,
            &test_dead_letter(event_id, &webhook_url, 0),
        )
        .await
        .unwrap();
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri(format!("/webhooks/dead-letters/{event_id}/replay"))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        let request_text = received_request.await.unwrap();
        let remaining = load_webhook_dead_letters(&state.config.webhooks_dead_letter_jsonl)
            .await
            .unwrap();

        assert!(body.contains("Replayed webhook"));
        assert!(body.contains("No webhook dead letters."));
        assert!(request_text.contains(&format!("x-download-event-id: {event_id}")));
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn retry_job_queues_new_job_from_failed_record() {
        let (state, _queue_rx) = test_state(10);
        state.workers.mark_ready();
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let failed = test_job(JobStatus::Failed, "https://www.instagram.com/reel/abc/");
        state.jobs.write().await.insert(failed.id, failed.clone());
        let app = router(state);
        let request = Request::builder()
            .method("POST")
            .uri(format!("/v1/jobs/{}/retry", failed.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let got = serde_json::from_slice::<QueueResponse>(&body).unwrap();

        assert_ne!(got.id, failed.id);
        assert_eq!(got.status, JobStatus::Queued);
    }

    #[tokio::test]
    async fn batch_retry_jobs_returns_per_id_success_and_errors() {
        let (state, _queue_rx) = test_state(10);
        state.workers.mark_ready();
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let failed = test_job(JobStatus::Failed, "https://www.instagram.com/reel/abc/");
        let queued = test_job(JobStatus::Queued, "https://www.instagram.com/reel/queued/");
        state.jobs.write().await.insert(failed.id, failed.clone());
        state.jobs.write().await.insert(queued.id, queued.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .method(Method::POST)
            .uri("/v1/jobs/batch/retry")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({ "ids": [failed.id, failed.id, queued.id] }).to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(body["action"], json!("retry"));
        assert_eq!(body["succeeded"].as_array().unwrap().len(), 1);
        assert_ne!(body["succeeded"][0]["id"], json!(failed.id));
        assert_eq!(body["succeeded"][0]["status"], json!("queued"));
        assert_eq!(body["failed"].as_array().unwrap().len(), 1);
        assert_eq!(body["failed"][0]["id"], json!(queued.id));
        assert_eq!(body["failed"][0]["code"], json!("conflict"));
        assert_eq!(state.jobs.read().await.len(), 3);
    }

    #[tokio::test]
    async fn cancel_job_marks_queued_job_canceled() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let queued = test_job(JobStatus::Queued, "https://www.instagram.com/reel/abc/");
        state.jobs.write().await.insert(queued.id, queued.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri(format!("/v1/jobs/{}/cancel", queued.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let got = serde_json::from_slice::<JobRecord>(&body).unwrap();
        let stored = state.jobs.read().await.get(&queued.id).cloned().unwrap();

        assert_eq!(got.status, JobStatus::Canceled);
        assert_eq!(stored.status, JobStatus::Canceled);
        assert_eq!(stored.error_kind.as_deref(), Some("canceled"));
        assert!(
            state
                .cancellations
                .flag_for(queued.id)
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    #[tokio::test]
    async fn batch_cancel_jobs_deduplicates_ids_and_reports_failures() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let queued = test_job(JobStatus::Queued, "https://www.instagram.com/reel/abc/");
        let succeeded = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/done/");
        let missing = Uuid::new_v4();
        state.jobs.write().await.insert(queued.id, queued.clone());
        state
            .jobs
            .write()
            .await
            .insert(succeeded.id, succeeded.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .method(Method::POST)
            .uri("/v1/jobs/batch/cancel")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({ "ids": [queued.id, queued.id, succeeded.id, missing] }).to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let stored = state.jobs.read().await.get(&queued.id).cloned().unwrap();

        assert_eq!(body["action"], json!("cancel"));
        assert_eq!(body["succeeded"].as_array().unwrap().len(), 1);
        assert_eq!(body["succeeded"][0]["id"], json!(queued.id));
        assert_eq!(body["failed"].as_array().unwrap().len(), 2);
        assert_eq!(body["failed"][0]["id"], json!(succeeded.id));
        assert_eq!(body["failed"][0]["code"], json!("conflict"));
        assert_eq!(body["failed"][1]["id"], json!(missing));
        assert_eq!(body["failed"][1]["code"], json!("not_found"));
        assert_eq!(stored.status, JobStatus::Canceled);
    }

    #[tokio::test]
    async fn batch_cancel_jobs_rejects_empty_id_list() {
        let (state, _queue_rx) = test_state(10);
        let app = router(state);
        let request = Request::builder()
            .method(Method::POST)
            .uri("/v1/jobs/batch/cancel")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(json!({ "ids": [] }).to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn cancel_job_marks_running_job_canceled() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let running = test_job(JobStatus::Running, "https://www.instagram.com/reel/abc/");
        state.jobs.write().await.insert(running.id, running.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri(format!("/v1/jobs/{}/cancel", running.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let stored = state.jobs.read().await.get(&running.id).cloned().unwrap();

        assert_eq!(stored.status, JobStatus::Canceled);
        assert!(
            state
                .cancellations
                .flag_for(running.id)
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    #[tokio::test]
    async fn cancel_job_form_marks_job_canceled_and_renders_index() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let queued = test_job(JobStatus::Queued, "https://www.instagram.com/reel/abc/");
        state.jobs.write().await.insert(queued.id, queued.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri(format!("/jobs-form/{}/cancel", queued.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        let stored = state.jobs.read().await.get(&queued.id).cloned().unwrap();

        assert!(body.contains("Canceled job"));
        assert_eq!(stored.status, JobStatus::Canceled);
    }

    #[tokio::test]
    async fn retry_job_form_queues_new_job_and_renders_index() {
        let (state, _queue_rx) = test_state(10);
        state.workers.mark_ready();
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let failed = test_job(JobStatus::Failed, "https://www.instagram.com/reel/abc/");
        state.jobs.write().await.insert(failed.id, failed.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri(format!("/jobs-form/{}/retry", failed.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(body.contains("Queued retry for job"));
        assert_eq!(state.jobs.read().await.len(), 2);
    }

    #[tokio::test]
    async fn get_job_artifacts_returns_stable_urls_and_metadata() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.downloads_dir)
            .await
            .unwrap();
        let mut record = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        let job_dir = state.config.downloads_dir.join(record.id.to_string());
        tokio::fs::create_dir_all(&job_dir).await.unwrap();
        let media_path = job_dir.join(format!("{}.mp4", record.id));
        let info_json_path = job_dir.join(format!("{}.info.json", record.id));
        tokio::fs::write(&media_path, b"video").await.unwrap();
        tokio::fs::write(&info_json_path, br#"{"id":"info"}"#)
            .await
            .unwrap();
        record.result = Some(DownloadMetadata {
            original_url: record.url.clone(),
            webpage_url: None,
            extractor: None,
            title: None,
            uploader: None,
            duration: None,
            extension: Some("mp4".to_string()),
            media_path,
            media_bytes: 12345,
            info_json_path,
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 1,
            post_processing: Vec::new(),
            storage: None,
        });
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/artifacts", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["id"], json!(record.id));
        assert_eq!(
            body["media_url"],
            json!(format!("/v1/jobs/{}/media", record.id))
        );
        assert_eq!(
            body["media_inline_url"],
            json!(format!("/v1/jobs/{}/media-inline", record.id))
        );
        assert_eq!(
            body["info_json_url"],
            json!(format!("/v1/jobs/{}/info-json", record.id))
        );
        assert_eq!(
            body["archive_url"],
            json!(format!("/v1/jobs/{}/archive", record.id))
        );
        assert_eq!(body["media_bytes"], json!(12345));
        assert_eq!(body["media_content_type"], json!("video/mp4"));
        assert_eq!(
            body["media_sha256"],
            json!("0cab1c9617404faf2b24e221e189ca5945813e14d3f766345b09ca13bbe28ffc")
        );
        assert_eq!(
            body["info_json_sha256"],
            json!("fe91e06aba4c6d4f1ddc66995d8a85f893602458ad420c64a66f7df0c7e6b098")
        );
        assert_eq!(body["archive_bytes"], json!(3072));
        let archive_sha256 = body["archive_sha256"].as_str().unwrap();
        assert_eq!(archive_sha256.len(), 64);
        assert!(archive_sha256.chars().all(|ch| ch.is_ascii_hexdigit()));
        assert_eq!(body["extension"], json!("mp4"));
    }

    #[tokio::test]
    async fn get_job_artifacts_rejects_jobs_without_result() {
        let (state, _queue_rx) = test_state(10);
        let record = test_job(JobStatus::Queued, "https://www.instagram.com/reel/abc/");
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/artifacts", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], json!("conflict"));
    }

    #[tokio::test]
    async fn get_job_media_returns_downloaded_file() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.downloads_dir)
            .await
            .unwrap();
        let mut record = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        let job_dir = state.config.downloads_dir.join(record.id.to_string());
        tokio::fs::create_dir_all(&job_dir).await.unwrap();
        let media_path = job_dir.join(format!("{}.mp4", record.id));
        let info_json_path = job_dir.join(format!("{}.info.json", record.id));
        tokio::fs::write(&media_path, b"video").await.unwrap();
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
            media_bytes: 5,
            info_json_path,
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 1,
            post_processing: Vec::new(),
            storage: None,
        });
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/media", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "video/mp4"
        );
        assert_eq!(response.headers().get(header::CONTENT_LENGTH).unwrap(), "5");
        assert_eq!(
            response.headers().get(header::ACCEPT_RANGES).unwrap(),
            "bytes"
        );
        assert_eq!(response.headers().get(header::ETAG).unwrap(), VIDEO_ETAG);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"video");
    }

    #[tokio::test]
    async fn head_job_media_returns_headers_without_body() {
        let (state, _queue_rx) = test_state(10);
        let record = insert_succeeded_download(&state, b"video", b"{}").await;
        let app = router(state.clone());
        let request = Request::builder()
            .method(Method::HEAD)
            .uri(format!("/v1/jobs/{}/media", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "video/mp4"
        );
        assert_eq!(response.headers().get(header::CONTENT_LENGTH).unwrap(), "5");
        assert_eq!(
            response.headers().get(header::ACCEPT_RANGES).unwrap(),
            "bytes"
        );
        assert_eq!(
            response.headers().get(header::CONTENT_DISPOSITION).unwrap(),
            &HeaderValue::from_str(&format!("attachment; filename=\"{}.mp4\"", record.id)).unwrap()
        );
        assert_eq!(response.headers().get(header::ETAG).unwrap(), VIDEO_ETAG);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn get_job_media_returns_not_modified_for_matching_etag() {
        let (state, _queue_rx) = test_state(10);
        let record = insert_succeeded_download(&state, b"video", b"{}").await;
        let app = router(state.clone());
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/media", record.id))
            .header(header::IF_NONE_MATCH, format!("\"stale\", W/{VIDEO_ETAG}"))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers().get(header::ETAG).unwrap(), VIDEO_ETAG);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn get_job_media_ignores_non_matching_etag() {
        let (state, _queue_rx) = test_state(10);
        let record = insert_succeeded_download(&state, b"video", b"{}").await;
        let app = router(state.clone());
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/media", record.id))
            .header(header::IF_NONE_MATCH, "\"sha256-not-the-file\"")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(header::ETAG).unwrap(), VIDEO_ETAG);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"video");
    }

    #[tokio::test]
    async fn head_job_media_respects_byte_range_without_body() {
        let (state, _queue_rx) = test_state(10);
        let record = insert_succeeded_download(&state, b"video", b"{}").await;
        let app = router(state.clone());
        let request = Request::builder()
            .method(Method::HEAD)
            .uri(format!("/v1/jobs/{}/media", record.id))
            .header(header::RANGE, "bytes=1-3")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers().get(header::CONTENT_LENGTH).unwrap(), "3");
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes 1-3/5"
        );
        assert_eq!(response.headers().get(header::ETAG).unwrap(), VIDEO_ETAG);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn head_job_media_inline_uses_inline_disposition_without_body() {
        let (state, _queue_rx) = test_state(10);
        let record = insert_succeeded_download(&state, b"video", b"{}").await;
        let app = router(state.clone());
        let request = Request::builder()
            .method(Method::HEAD)
            .uri(format!("/v1/jobs/{}/media-inline", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_DISPOSITION).unwrap(),
            &HeaderValue::from_str(&format!("inline; filename=\"{}.mp4\"", record.id)).unwrap()
        );
        assert_eq!(response.headers().get(header::ETAG).unwrap(), VIDEO_ETAG);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn get_job_media_inline_streams_with_inline_disposition() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.downloads_dir)
            .await
            .unwrap();
        let mut record = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        let job_dir = state.config.downloads_dir.join(record.id.to_string());
        tokio::fs::create_dir_all(&job_dir).await.unwrap();
        let media_path = job_dir.join(format!("{}.mp4", record.id));
        let info_json_path = job_dir.join(format!("{}.info.json", record.id));
        tokio::fs::write(&media_path, b"video").await.unwrap();
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
            media_bytes: 5,
            info_json_path,
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 1,
            post_processing: Vec::new(),
            storage: None,
        });
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/media-inline", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_DISPOSITION).unwrap(),
            &HeaderValue::from_str(&format!("inline; filename=\"{}.mp4\"", record.id)).unwrap()
        );
        assert_eq!(response.headers().get(header::ETAG).unwrap(), VIDEO_ETAG);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"video");
    }

    #[tokio::test]
    async fn get_job_media_returns_requested_byte_range() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.downloads_dir)
            .await
            .unwrap();
        let mut record = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        let job_dir = state.config.downloads_dir.join(record.id.to_string());
        tokio::fs::create_dir_all(&job_dir).await.unwrap();
        let media_path = job_dir.join(format!("{}.mp4", record.id));
        let info_json_path = job_dir.join(format!("{}.info.json", record.id));
        tokio::fs::write(&media_path, b"video").await.unwrap();
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
            media_bytes: 5,
            info_json_path,
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 1,
            post_processing: Vec::new(),
            storage: None,
        });
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/media", record.id))
            .header(header::RANGE, "bytes=1-3")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers().get(header::CONTENT_LENGTH).unwrap(), "3");
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes 1-3/5"
        );
        assert_eq!(response.headers().get(header::ETAG).unwrap(), VIDEO_ETAG);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"ide");
    }

    #[tokio::test]
    async fn get_job_media_supports_suffix_byte_range() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.downloads_dir)
            .await
            .unwrap();
        let mut record = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        let job_dir = state.config.downloads_dir.join(record.id.to_string());
        tokio::fs::create_dir_all(&job_dir).await.unwrap();
        let media_path = job_dir.join(format!("{}.mp4", record.id));
        let info_json_path = job_dir.join(format!("{}.info.json", record.id));
        tokio::fs::write(&media_path, b"video").await.unwrap();
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
            media_bytes: 5,
            info_json_path,
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 1,
            post_processing: Vec::new(),
            storage: None,
        });
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/media", record.id))
            .header(header::RANGE, "bytes=-2")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes 3-4/5"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"eo");
    }

    #[tokio::test]
    async fn get_job_media_rejects_unsatisfiable_byte_range() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.downloads_dir)
            .await
            .unwrap();
        let mut record = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        let job_dir = state.config.downloads_dir.join(record.id.to_string());
        tokio::fs::create_dir_all(&job_dir).await.unwrap();
        let media_path = job_dir.join(format!("{}.mp4", record.id));
        let info_json_path = job_dir.join(format!("{}.info.json", record.id));
        tokio::fs::write(&media_path, b"video").await.unwrap();
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
            media_bytes: 5,
            info_json_path,
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 1,
            post_processing: Vec::new(),
            storage: None,
        });
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/media", record.id))
            .header(header::RANGE, "bytes=99-100")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes */5"
        );
    }

    #[tokio::test]
    async fn get_job_info_json_returns_download_metadata_file() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.downloads_dir)
            .await
            .unwrap();
        let mut record = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        let job_dir = state.config.downloads_dir.join(record.id.to_string());
        tokio::fs::create_dir_all(&job_dir).await.unwrap();
        let media_path = job_dir.join(format!("{}.mp4", record.id));
        let info_json_path = job_dir.join(format!("{}.info.json", record.id));
        tokio::fs::write(&media_path, b"video").await.unwrap();
        tokio::fs::write(&info_json_path, br#"{"title":"example"}"#)
            .await
            .unwrap();
        record.result = Some(DownloadMetadata {
            original_url: record.url.clone(),
            webpage_url: None,
            extractor: None,
            title: None,
            uploader: None,
            duration: None,
            extension: Some("mp4".to_string()),
            media_path,
            media_bytes: 5,
            info_json_path,
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 1,
            post_processing: Vec::new(),
            storage: None,
        });
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/info-json", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        assert_eq!(
            response.headers().get(header::CONTENT_DISPOSITION).unwrap(),
            &HeaderValue::from_str(&format!("attachment; filename=\"{}.info.json\"", record.id))
                .unwrap()
        );
        assert_eq!(
            response.headers().get(header::ETAG).unwrap(),
            EXAMPLE_INFO_JSON_ETAG
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], br#"{"title":"example"}"#);
    }

    #[tokio::test]
    async fn head_job_info_json_returns_headers_without_body() {
        let (state, _queue_rx) = test_state(10);
        let record = insert_succeeded_download(&state, b"video", br#"{"title":"example"}"#).await;
        let app = router(state.clone());
        let request = Request::builder()
            .method(Method::HEAD)
            .uri(format!("/v1/jobs/{}/info-json", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        assert_eq!(
            response.headers().get(header::CONTENT_DISPOSITION).unwrap(),
            &HeaderValue::from_str(&format!("attachment; filename=\"{}.info.json\"", record.id))
                .unwrap()
        );
        assert_eq!(
            response.headers().get(header::CONTENT_LENGTH).unwrap(),
            "19"
        );
        assert_eq!(
            response.headers().get(header::ETAG).unwrap(),
            EXAMPLE_INFO_JSON_ETAG
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn head_job_info_json_returns_not_modified_for_matching_etag() {
        let (state, _queue_rx) = test_state(10);
        let record = insert_succeeded_download(&state, b"video", br#"{"title":"example"}"#).await;
        let app = router(state.clone());
        let request = Request::builder()
            .method(Method::HEAD)
            .uri(format!("/v1/jobs/{}/info-json", record.id))
            .header(header::IF_NONE_MATCH, EXAMPLE_INFO_JSON_ETAG)
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(
            response.headers().get(header::ETAG).unwrap(),
            EXAMPLE_INFO_JSON_ETAG
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn get_job_archive_returns_media_and_info_json() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.downloads_dir)
            .await
            .unwrap();
        let mut record = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        let job_dir = state.config.downloads_dir.join(record.id.to_string());
        tokio::fs::create_dir_all(&job_dir).await.unwrap();
        let media_name = format!("{}.mp4", record.id);
        let info_json_name = format!("{}.info.json", record.id);
        let media_path = job_dir.join(&media_name);
        let info_json_path = job_dir.join(&info_json_name);
        tokio::fs::write(&media_path, b"video").await.unwrap();
        tokio::fs::write(&info_json_path, br#"{"id":"test"}"#)
            .await
            .unwrap();
        record.result = Some(DownloadMetadata {
            original_url: record.url.clone(),
            webpage_url: None,
            extractor: None,
            title: None,
            uploader: None,
            duration: None,
            extension: Some("mp4".to_string()),
            media_path,
            media_bytes: 5,
            info_json_path,
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 1,
            post_processing: Vec::new(),
            storage: None,
        });
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/archive", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/x-tar"
        );
        assert_eq!(
            response.headers().get(header::CONTENT_DISPOSITION).unwrap(),
            &HeaderValue::from_str(&format!("attachment; filename=\"{}.tar\"", record.id)).unwrap()
        );
        let content_length = response
            .headers()
            .get(header::CONTENT_LENGTH)
            .cloned()
            .unwrap();
        let accept_ranges = response
            .headers()
            .get(header::ACCEPT_RANGES)
            .cloned()
            .unwrap();
        let etag = response.headers().get(header::ETAG).cloned().unwrap();
        assert!(etag.to_str().unwrap().starts_with("\"sha256-"));
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            content_length,
            &HeaderValue::from_str(&body.len().to_string()).unwrap()
        );
        assert_eq!(accept_ranges, "bytes");
        assert_eq!(body.len() % 512, 0);
        assert_eq!(&body[..media_name.len()], media_name.as_bytes());
        assert!(contains_bytes(&body, info_json_name.as_bytes()));
        assert!(contains_bytes(&body, b"video"));
        assert!(contains_bytes(&body, br#"{"id":"test"}"#));
    }

    #[tokio::test]
    async fn head_job_archive_returns_headers_without_body() {
        let (state, _queue_rx) = test_state(10);
        let record = insert_succeeded_download(&state, b"video", br#"{"id":"test"}"#).await;
        let app = router(state.clone());
        let request = Request::builder()
            .method(Method::HEAD)
            .uri(format!("/v1/jobs/{}/archive", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/x-tar"
        );
        assert_eq!(
            response.headers().get(header::CONTENT_DISPOSITION).unwrap(),
            &HeaderValue::from_str(&format!("attachment; filename=\"{}.tar\"", record.id)).unwrap()
        );
        assert_eq!(
            response.headers().get(header::CONTENT_LENGTH).unwrap(),
            "3072"
        );
        assert_eq!(
            response.headers().get(header::ACCEPT_RANGES).unwrap(),
            "bytes"
        );
        assert!(
            response
                .headers()
                .get(header::ETAG)
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("\"sha256-")
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn get_job_archive_returns_requested_byte_range() {
        let (state, _queue_rx) = test_state(10);
        let record = insert_succeeded_download(&state, b"video", br#"{"id":"test"}"#).await;
        let app = router(state.clone());
        let full_request = Request::builder()
            .uri(format!("/v1/jobs/{}/archive", record.id))
            .body(Body::empty())
            .unwrap();
        let full_response = app.clone().oneshot(full_request).await.unwrap();
        let full_body = to_bytes(full_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/archive", record.id))
            .header(header::RANGE, "bytes=1-7")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes 1-7/3072"
        );
        assert_eq!(response.headers().get(header::CONTENT_LENGTH).unwrap(), "7");
        assert_eq!(
            response.headers().get(header::ACCEPT_RANGES).unwrap(),
            "bytes"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], &full_body[1..8]);
    }

    #[tokio::test]
    async fn head_job_archive_respects_byte_range_without_body() {
        let (state, _queue_rx) = test_state(10);
        let record = insert_succeeded_download(&state, b"video", br#"{"id":"test"}"#).await;
        let app = router(state.clone());
        let request = Request::builder()
            .method(Method::HEAD)
            .uri(format!("/v1/jobs/{}/archive", record.id))
            .header(header::RANGE, "bytes=1-7")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes 1-7/3072"
        );
        assert_eq!(response.headers().get(header::CONTENT_LENGTH).unwrap(), "7");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn get_job_archive_rejects_unsatisfiable_byte_range() {
        let (state, _queue_rx) = test_state(10);
        let record = insert_succeeded_download(&state, b"video", br#"{"id":"test"}"#).await;
        let app = router(state.clone());
        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/archive", record.id))
            .header(header::RANGE, "bytes=9999-10000")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes */3072"
        );
    }

    #[tokio::test]
    async fn get_job_archive_returns_not_modified_for_matching_etag() {
        let (state, _queue_rx) = test_state(10);
        let record = insert_succeeded_download(&state, b"video", br#"{"id":"test"}"#).await;
        let app = router(state.clone());
        let first_request = Request::builder()
            .uri(format!("/v1/jobs/{}/archive", record.id))
            .body(Body::empty())
            .unwrap();
        let first_response = app.clone().oneshot(first_request).await.unwrap();
        assert_eq!(first_response.status(), StatusCode::OK);
        let etag = first_response.headers().get(header::ETAG).cloned().unwrap();

        let request = Request::builder()
            .uri(format!("/v1/jobs/{}/archive", record.id))
            .header(
                header::IF_NONE_MATCH,
                format!("\"stale\", W/{}", etag.to_str().unwrap()),
            )
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers().get(header::ETAG).unwrap(), &etag);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn head_job_archive_returns_not_modified_for_matching_etag() {
        let (state, _queue_rx) = test_state(10);
        let record = insert_succeeded_download(&state, b"video", br#"{"id":"test"}"#).await;
        let app = router(state.clone());
        let first_request = Request::builder()
            .method(Method::HEAD)
            .uri(format!("/v1/jobs/{}/archive", record.id))
            .body(Body::empty())
            .unwrap();
        let first_response = app.clone().oneshot(first_request).await.unwrap();
        assert_eq!(first_response.status(), StatusCode::OK);
        let etag = first_response.headers().get(header::ETAG).cloned().unwrap();

        let request = Request::builder()
            .method(Method::HEAD)
            .uri(format!("/v1/jobs/{}/archive", record.id))
            .header(header::IF_NONE_MATCH, etag.clone())
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers().get(header::ETAG).unwrap(), &etag);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn delete_job_removes_media_and_marks_deleted() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let mut record = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        let job_dir = state.config.downloads_dir.join(record.id.to_string());
        tokio::fs::create_dir_all(&job_dir).await.unwrap();
        let media_path = job_dir.join(format!("{}.mp4", record.id));
        let info_json_path = job_dir.join(format!("{}.info.json", record.id));
        tokio::fs::write(&media_path, b"video").await.unwrap();
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
            media_bytes: 5,
            info_json_path,
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 1,
            post_processing: Vec::new(),
            storage: None,
        });
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .method("DELETE")
            .uri(format!("/v1/jobs/{}", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!job_dir.exists());
        let stored = state.jobs.read().await.get(&record.id).cloned().unwrap();
        assert_eq!(stored.status, JobStatus::Deleted);
        assert!(stored.result.is_none());
    }

    #[tokio::test]
    async fn batch_delete_jobs_removes_artifacts_and_reports_failures() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let succeeded = insert_succeeded_download(&state, b"video", b"{}").await;
        let job_dir = succeeded
            .result
            .as_ref()
            .unwrap()
            .media_path
            .parent()
            .unwrap()
            .to_path_buf();
        let queued = test_job(JobStatus::Queued, "https://www.instagram.com/reel/queued/");
        state.jobs.write().await.insert(queued.id, queued.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .method(Method::POST)
            .uri("/v1/jobs/batch/delete")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({ "ids": [succeeded.id, queued.id] }).to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let stored = state.jobs.read().await.get(&succeeded.id).cloned().unwrap();

        assert_eq!(body["action"], json!("delete"));
        assert_eq!(body["succeeded"].as_array().unwrap().len(), 1);
        assert_eq!(body["succeeded"][0]["id"], json!(succeeded.id));
        assert_eq!(body["succeeded"][0]["media_deleted"], json!(true));
        assert_eq!(body["failed"].as_array().unwrap().len(), 1);
        assert_eq!(body["failed"][0]["id"], json!(queued.id));
        assert_eq!(body["failed"][0]["code"], json!("conflict"));
        assert_eq!(stored.status, JobStatus::Deleted);
        assert!(!job_dir.exists());
    }

    #[tokio::test]
    async fn delete_job_form_removes_media_and_renders_index() {
        let (state, _queue_rx) = test_state(10);
        tokio::fs::create_dir_all(&state.config.metadata_dir)
            .await
            .unwrap();
        let mut record = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        let job_dir = state.config.downloads_dir.join(record.id.to_string());
        tokio::fs::create_dir_all(&job_dir).await.unwrap();
        let media_path = job_dir.join(format!("{}.mp4", record.id));
        let info_json_path = job_dir.join(format!("{}.info.json", record.id));
        tokio::fs::write(&media_path, b"video").await.unwrap();
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
            media_bytes: 5,
            info_json_path,
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 1,
            post_processing: Vec::new(),
            storage: None,
        });
        state.jobs.write().await.insert(record.id, record.clone());
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri(format!("/jobs-form/{}/delete", record.id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        let stored = state.jobs.read().await.get(&record.id).cloned().unwrap();

        assert!(body.contains("Deleted job"));
        assert!(!job_dir.exists());
        assert_eq!(stored.status, JobStatus::Deleted);
        assert!(stored.result.is_none());
    }

    fn test_state(queue_size: usize) -> (AppState, Receiver<crate::jobs::JobRequest>) {
        test_state_with_api_keys(queue_size, Vec::new())
    }

    fn test_state_with_api_keys(
        queue_size: usize,
        api_keys: Vec<String>,
    ) -> (AppState, Receiver<crate::jobs::JobRequest>) {
        let root = temp_dir("state");
        let metadata_dir = root.join("metadata");
        let config = Arc::new(Config {
            addr: SocketAddr::from(([127, 0, 0, 1], 3000)),
            data_dir: root.clone(),
            downloads_dir: root.join("downloads"),
            metadata_dir: metadata_dir.clone(),
            submissions_jsonl: metadata_dir.join("download_submissions.jsonl"),
            results_jsonl: metadata_dir.join("download_results.jsonl"),
            cors_allowed_origins: Vec::new(),
            api_keys,
            rate_limit_requests_per_minute: 0,
            job_retention_limit: 1000,
            metadata_retention_limit: 10000,
            workers: 1,
            queue_size,
            body_limit_bytes: 1024 * 1024,
            request_timeout_seconds: 60,
            rust_log: "info".to_string(),
            yt_dlp_command: "uv".to_string(),
            cookies_path: None,
            cookie_profiles: Default::default(),
            format: None,
            proxy: None,
            platform_policies: Default::default(),
            download_enabled_platforms: platforms::default_enabled_platforms(),
            max_urls_per_request: 100,
            job_timeout_seconds: 300,
            download_max_attempts: 1,
            download_initial_backoff_ms: 0,
            max_download_storage_bytes: 0,
            min_free_disk_bytes: 0,
            post_processing: crate::config::PostProcessingConfig {
                enabled: false,
                fail_job_on_error: true,
                commands: Vec::new(),
            },
            object_storage: crate::config::ObjectStorageConfig {
                backend: crate::config::ObjectStorageBackend::Local,
                endpoint_url: None,
                bucket: None,
                region: "us-east-1".to_string(),
                access_key_id: None,
                secret_access_key: None,
                session_token: None,
                prefix: String::new(),
                force_path_style: true,
                public_base_url: None,
            },
            webhook_timeout_seconds: 10,
            webhook_connect_timeout_seconds: 5,
            webhook_max_attempts: 1,
            webhook_initial_backoff_ms: 500,
            webhook_signing_secret: None,
            webhooks_dead_letter_jsonl: metadata_dir.join("webhooks_dead_letter.jsonl"),
            allow_private_webhook_urls: false,
        });
        let (queue_tx, queue_rx) = bounded(queue_size);
        let state = AppState {
            config,
            queue_tx,
            jobs: Arc::new(RwLock::new(HashMap::new())),
            workers: Arc::new(WorkerPoolState::new(1)),
            metrics: Arc::new(AppMetrics::default()),
            rate_limiter: Arc::new(RateLimiter::new(0)),
            cancellations: Arc::new(crate::state::CancellationRegistry::default()),
        };
        (state, queue_rx)
    }

    fn test_job(status: JobStatus, url: &str) -> JobRecord {
        JobRecord {
            id: Uuid::new_v4(),
            status,
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            url: url.to_string(),
            url_sha256: Some(download_url_sha256(url)),
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

    fn test_download_metadata(record: &JobRecord) -> DownloadMetadata {
        let job_dir = PathBuf::from("data/downloads").join(record.id.to_string());
        DownloadMetadata {
            original_url: record.url.clone(),
            webpage_url: Some(record.url.clone()),
            extractor: Some("Instagram".to_string()),
            title: Some("test video".to_string()),
            uploader: Some("tester".to_string()),
            duration: Some(1.0),
            extension: Some("mp4".to_string()),
            media_path: job_dir.join(format!("{}.mp4", record.id)),
            media_bytes: 1,
            info_json_path: job_dir.join(format!("{}.info.json", record.id)),
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 1,
            post_processing: Vec::new(),
            storage: None,
        }
    }

    async fn insert_succeeded_download(
        state: &AppState,
        media_bytes: &[u8],
        info_json_bytes: &[u8],
    ) -> JobRecord {
        tokio::fs::create_dir_all(&state.config.downloads_dir)
            .await
            .unwrap();
        let mut record = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        let job_dir = state.config.downloads_dir.join(record.id.to_string());
        tokio::fs::create_dir_all(&job_dir).await.unwrap();
        let media_path = job_dir.join(format!("{}.mp4", record.id));
        let info_json_path = job_dir.join(format!("{}.info.json", record.id));
        tokio::fs::write(&media_path, media_bytes).await.unwrap();
        tokio::fs::write(&info_json_path, info_json_bytes)
            .await
            .unwrap();
        record.result = Some(DownloadMetadata {
            original_url: record.url.clone(),
            webpage_url: None,
            extractor: None,
            title: None,
            uploader: None,
            duration: None,
            extension: Some("mp4".to_string()),
            media_path,
            media_bytes: media_bytes.len() as u64,
            info_json_path,
            yt_dlp_version: "test".to_string(),
            elapsed_ms: 1,
            post_processing: Vec::new(),
            storage: None,
        });
        state.jobs.write().await.insert(record.id, record.clone());
        record
    }

    fn test_dead_letter(
        event_id: Uuid,
        webhook_url: &str,
        failed_seconds_offset: i64,
    ) -> WebhookDeadLetter {
        let mut job = test_job(JobStatus::Succeeded, "https://www.instagram.com/reel/abc/");
        job.webhook_url = Some(webhook_url.to_string());
        WebhookDeadLetter {
            event: WebhookEvent {
                event_id,
                event_type: "job.completed".to_string(),
                created_at: OffsetDateTime::now_utc(),
                job,
                artifact_urls: None,
            },
            attempts: 1,
            failed_at: OffsetDateTime::now_utc() + time::Duration::seconds(failed_seconds_offset),
            error: "webhook endpoint returned HTTP 500".to_string(),
        }
    }

    async fn one_shot_webhook_server() -> (String, tokio::sync::oneshot::Receiver<String>) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0_u8; 8192];
            let read = stream.read(&mut buffer).await.unwrap();
            let request = String::from_utf8_lossy(&buffer[..read]).into_owned();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
            let _ = tx.send(request);
        });

        (format!("http://{addr}/webhook"), rx)
    }

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("yt-dlp-server-{nanos}-{name}"))
    }

    fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }
}
