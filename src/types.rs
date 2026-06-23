use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: Uuid,
    pub status: JobStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    pub url: String,
    pub webhook_url: Option<String>,
    pub result: Option<DownloadMetadata>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadMetadata {
    pub original_url: String,
    pub webpage_url: Option<String>,
    pub extractor: Option<String>,
    pub title: Option<String>,
    pub uploader: Option<String>,
    pub duration: Option<f64>,
    pub extension: Option<String>,
    pub media_path: PathBuf,
    pub media_bytes: u64,
    pub info_json_path: PathBuf,
    pub yt_dlp_version: String,
    pub elapsed_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueResponse {
    pub id: Uuid,
    pub status: JobStatus,
    pub status_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchQueueResponse {
    pub jobs: Vec<QueueResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub ready: bool,
    pub workers: WorkerHealth,
    pub queued: usize,
    pub output_dir: PathBuf,
    pub yt_dlp_command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessResponse {
    pub status: &'static str,
    pub ready: bool,
    pub workers: WorkerHealth,
    pub queued: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerHealth {
    pub expected: usize,
    pub ready: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsResponse {
    pub workers: WorkerHealth,
    pub queued: usize,
    pub http_requests_total: usize,
    pub http_requests_failed: usize,
    pub total_request_ms: u128,
    pub average_request_ms: Option<f64>,
    pub jobs_started: usize,
    pub jobs_succeeded: usize,
    pub jobs_failed: usize,
    pub jobs_timed_out: usize,
    pub worker_restarts: usize,
    pub total_download_ms: u128,
    pub average_download_ms: Option<f64>,
    pub webhook_failures: usize,
    pub cleanup_failures: usize,
    pub retained_jobs: usize,
    pub process_memory_rss_bytes: Option<u64>,
}
