use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    time::{Duration, Instant},
};

use anyhow::Context;
use askama::Template;
use axum::{
    Form, Json, Router,
    extract::{DefaultBodyLimit, Path as AxumPath, Request, State},
    http::{HeaderName, HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use log::{debug, info};
use reqwest::Url;
use serde::Deserialize;
use serde_json::{Value, json};
use time::OffsetDateTime;
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    timeout::TimeoutLayer,
};
use uuid::Uuid;

use crate::{
    config::Config,
    jobs::{EnqueueError, enqueue_record},
    state::{AppState, RateLimitDecision},
    templates::IndexTemplate,
    types::{
        BatchQueueResponse, ErrorResponse, HealthResponse, JobRecord, JobStatus, MetricsResponse,
        ReadinessResponse, WorkerHealth,
    },
};

pub fn router(state: AppState) -> Router {
    let cors_allowed_origins = state.config.cors_allowed_origins.clone();
    let request_timeout_seconds = state.config.request_timeout_seconds;
    let state_for_middleware = state.clone();
    let mut router = Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route("/ready", get(readiness))
        .route("/metrics", get(metrics))
        .route("/openapi.json", get(openapi))
        .route("/v1/downloads", post(submit_downloads))
        .route("/downloads-form", post(submit_downloads_form))
        .route("/v1/jobs/{id}", get(get_job))
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
    ServiceUnavailable(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::NotFound => StatusCode::NOT_FOUND,
            ApiError::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = Json(ErrorResponse {
            code: self.code().to_string(),
            message: self.to_string(),
        });
        (status, body).into_response()
    }
}

impl ApiError {
    fn code(&self) -> &'static str {
        match self {
            ApiError::BadRequest(_) => "bad_request",
            ApiError::NotFound => "not_found",
            ApiError::ServiceUnavailable(_) => "service_unavailable",
            ApiError::Internal(_) => "internal_error",
        }
    }
}

#[derive(Debug, Deserialize)]
struct DownloadRequest {
    urls: Vec<String>,
    webhook_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DownloadForm {
    urls: String,
    webhook_url: Option<String>,
}

async fn index() -> Result<Html<String>, ApiError> {
    render_index(None, None)
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
    let ready = workers.ready > 0;
    let response = ReadinessResponse {
        status: if ready { "ready" } else { "not_ready" },
        ready,
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
    let workers = worker_health(&state);
    let metrics = state.metrics.snapshot();
    let completed = metrics.jobs_succeeded + metrics.jobs_failed;
    let average_download_ms =
        (completed > 0).then_some(metrics.total_download_ms as f64 / completed as f64);
    let average_request_ms = (metrics.http_requests_total > 0)
        .then_some(metrics.total_request_ms as f64 / metrics.http_requests_total as f64);

    Json(MetricsResponse {
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
    })
}

async fn openapi() -> Json<Value> {
    Json(openapi_document())
}

async fn submit_downloads(
    State(state): State<AppState>,
    Json(request): Json<DownloadRequest>,
) -> Result<Json<BatchQueueResponse>, ApiError> {
    Ok(Json(
        submit_download_jobs(&state, request.urls, request.webhook_url).await?,
    ))
}

async fn submit_downloads_form(
    State(state): State<AppState>,
    Form(form): Form<DownloadForm>,
) -> Result<Html<String>, ApiError> {
    let urls = form.urls.lines().map(str::to_string).collect::<Vec<_>>();
    match submit_download_jobs(&state, urls, form.webhook_url).await {
        Ok(response) => render_index(Some(response), None),
        Err(err) => render_index(None, Some(err.to_string())),
    }
}

async fn submit_download_jobs(
    state: &AppState,
    urls: Vec<String>,
    webhook_url: Option<String>,
) -> Result<BatchQueueResponse, ApiError> {
    ensure_workers_ready(state)?;
    let urls = validate_download_urls(&urls, state.config.max_urls_per_request)?;
    let webhook_url = validate_webhook_url(&state.config, webhook_url)?;

    let available_slots = state.config.queue_size.saturating_sub(state.queue_tx.len());
    if urls.len() > available_slots {
        return Err(ApiError::ServiceUnavailable(
            "download queue is full".to_string(),
        ));
    }

    let mut responses = Vec::with_capacity(urls.len());
    for url in urls {
        let id = Uuid::new_v4();
        let now = OffsetDateTime::now_utc();
        let record = JobRecord {
            id,
            status: JobStatus::Queued,
            created_at: now,
            updated_at: now,
            url,
            webhook_url: webhook_url.clone(),
            result: None,
            error: None,
        };
        responses.push(enqueue_record(state, record).await?);
    }

    Ok(BatchQueueResponse { jobs: responses })
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

    match state.rate_limiter.check() {
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

    json_error_response(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "valid API key is required",
    )
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

fn validate_download_urls(values: &[String], max_urls: usize) -> Result<Vec<String>, ApiError> {
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
        let url = validate_download_url(value)?;
        if seen.insert(url.clone()) {
            urls.push(url);
        }
    }
    Ok(urls)
}

fn validate_download_url(value: &str) -> Result<String, ApiError> {
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
    if !is_supported_social_host(&host) {
        return Err(ApiError::BadRequest(format!(
            "unsupported URL host `{host}`; expected TikTok or Instagram"
        )));
    }
    Ok(url.to_string())
}

fn is_supported_social_host(host: &str) -> bool {
    host == "tiktok.com"
        || host.ends_with(".tiktok.com")
        || host == "instagram.com"
        || host.ends_with(".instagram.com")
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

fn render_index(
    response: Option<BatchQueueResponse>,
    error: Option<String>,
) -> Result<Html<String>, ApiError> {
    let template = IndexTemplate {
        queued_jobs: response.map(|response| response.jobs).unwrap_or_default(),
        error: error.unwrap_or_default(),
    };
    let html = template
        .render()
        .context("failed to render index template")?;
    Ok(Html(html))
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
            "/v1/downloads": {
                "post": {
                    "summary": "Queue TikTok or Instagram video downloads",
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
                        "webhook_url": { "type": ["string", "null"], "format": "uri" }
                    }
                },
                "BatchQueueResponse": {
                    "type": "object",
                    "required": ["jobs"],
                    "properties": {
                        "jobs": { "type": "array", "items": { "$ref": "#/components/schemas/QueueResponse" } }
                    }
                },
                "QueueResponse": {
                    "type": "object",
                    "required": ["id", "status", "status_url"],
                    "properties": {
                        "id": { "type": "string", "format": "uuid" },
                        "status": { "type": "string", "enum": ["queued", "running", "succeeded", "failed"] },
                        "status_url": { "type": "string" }
                    }
                },
                "JobRecord": { "type": "object" },
                "HealthResponse": { "type": "object" },
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
    use tokio::sync::RwLock;
    use tower::ServiceExt;

    use super::*;
    use crate::{
        config::Config,
        state::{AppMetrics, RateLimiter, WorkerPoolState},
    };

    #[test]
    fn validates_and_deduplicates_supported_urls() {
        let urls = validate_download_urls(
            &[
                " https://www.tiktok.com/@user/video/123 ".to_string(),
                "https://www.tiktok.com/@user/video/123".to_string(),
                "https://www.instagram.com/reel/abc/".to_string(),
            ],
            10,
        )
        .unwrap();

        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0], "https://www.tiktok.com/@user/video/123");
        assert_eq!(urls[1], "https://www.instagram.com/reel/abc/");
    }

    #[test]
    fn rejects_unsupported_url_hosts() {
        let err =
            validate_download_urls(&["https://example.com/video".to_string()], 10).unwrap_err();

        assert!(err.to_string().contains("unsupported URL host"));
    }

    #[test]
    fn rejects_too_many_urls() {
        let err = validate_download_urls(
            &[
                "https://www.tiktok.com/@a/video/1".to_string(),
                "https://www.instagram.com/reel/b/".to_string(),
            ],
            1,
        )
        .unwrap_err();

        assert!(err.to_string().contains("too many URLs"));
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
    async fn queue_full_returns_service_unavailable() {
        let (state, _queue_rx) = test_state(1);
        state.workers.mark_ready();
        state
            .queue_tx
            .try_send(crate::jobs::JobRequest {
                id: Uuid::new_v4(),
                url: "https://www.tiktok.com/@busy/video/1".to_string(),
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
            webhook_url: None,
            result: None,
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

    fn test_state(queue_size: usize) -> (AppState, Receiver<crate::jobs::JobRequest>) {
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
            api_keys: Vec::new(),
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
            max_urls_per_request: 100,
            job_timeout_seconds: 300,
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
        };
        (state, queue_rx)
    }

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("yt-dlp-server-{nanos}-{name}"))
    }
}
