use std::{fmt, path::PathBuf};

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
    Canceled,
    Deleted,
}

impl fmt::Display for JobStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::Deleted => "deleted",
        })
    }
}

impl JobStatus {
    pub fn is_cancelable(&self) -> bool {
        matches!(self, Self::Queued | Self::Running)
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }

    pub fn is_deletable(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Canceled | Self::Deleted
        )
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url_sha256: Option<String>,
    pub webhook_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cookie_profile: Option<String>,
    pub result: Option<DownloadMetadata>,
    #[serde(default)]
    pub attempts: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attempt_errors: Vec<DownloadAttempt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadAttempt {
    pub attempt: usize,
    pub error: String,
    pub elapsed_ms: u128,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_backoff_ms: Option<u128>,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub post_processing: Vec<PostProcessingStepResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<StoredArtifacts>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostProcessingStepResult {
    pub command_index: usize,
    pub program: String,
    pub success: bool,
    pub status_code: Option<i32>,
    pub elapsed_ms: u128,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout_tail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_tail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredArtifacts {
    pub backend: String,
    pub bucket: Option<String>,
    pub media: StoredObject,
    pub info_json: StoredObject,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredObject {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueResponse {
    pub id: Uuid,
    pub status: JobStatus,
    pub status_url: String,
    #[serde(default)]
    pub existing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchQueueResponse {
    pub jobs: Vec<QueueResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobListResponse {
    pub jobs: Vec<JobRecord>,
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteJobResponse {
    pub id: Uuid,
    pub deleted: bool,
    pub media_deleted: bool,
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
    pub downloader_ready: bool,
    pub downloader_error: Option<String>,
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
