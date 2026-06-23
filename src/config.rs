use std::{collections::BTreeMap, env, fs as std_fs, net::SocketAddr, path::PathBuf};

use anyhow::{Context, anyhow};
use log::debug;
use reqwest::Url;
use serde::Deserialize;
use tokio::fs;

use crate::platforms;

const DEFAULT_ADDR: &str = "127.0.0.1:3000";
const DEFAULT_CONFIG_PATH: &str = "config.toml";
const DEFAULT_DATA_DIR: &str = "data";
const DEFAULT_QUEUE_SIZE: usize = 128;
const DEFAULT_BODY_LIMIT_BYTES: usize = 128 * 1024;
const DEFAULT_RUST_LOG: &str = "info";
const DEFAULT_JOB_RETENTION_LIMIT: usize = 1_000;
const DEFAULT_METADATA_RETENTION_LIMIT: usize = 10_000;
const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 60;
const DEFAULT_WEBHOOK_TIMEOUT_SECONDS: u64 = 10;
const DEFAULT_WEBHOOK_CONNECT_TIMEOUT_SECONDS: u64 = 5;
const DEFAULT_WEBHOOK_MAX_ATTEMPTS: usize = 1;
const DEFAULT_WEBHOOK_INITIAL_BACKOFF_MS: u64 = 500;
const DEFAULT_ALLOW_PRIVATE_WEBHOOK_URLS: bool = false;
const DEFAULT_DOWNLOAD_WORKERS: usize = 1;
const DEFAULT_DOWNLOAD_OUTPUT_DIR: &str = "data/downloads";
const DEFAULT_YT_DLP_COMMAND: &str = "uv";
const DEFAULT_MAX_URLS_PER_REQUEST: usize = 100;
const DEFAULT_JOB_TIMEOUT_SECONDS: u64 = 1_800;
const DEFAULT_DOWNLOAD_MAX_ATTEMPTS: usize = 3;
const DEFAULT_DOWNLOAD_INITIAL_BACKOFF_MS: u64 = 1_000;
const DEFAULT_MAX_DOWNLOAD_STORAGE_BYTES: u64 = 0;
const DEFAULT_MIN_FREE_DISK_BYTES: u64 = 0;
const DEFAULT_POST_PROCESSING_ENABLED: bool = false;
const DEFAULT_POST_PROCESSING_FAIL_JOB_ON_ERROR: bool = true;
const DEFAULT_OBJECT_STORAGE_BACKEND: &str = "local";
const DEFAULT_OBJECT_STORAGE_REGION: &str = "us-east-1";
const DEFAULT_OBJECT_STORAGE_FORCE_PATH_STYLE: bool = true;

pub struct Config {
    pub addr: SocketAddr,
    pub data_dir: PathBuf,
    pub downloads_dir: PathBuf,
    pub metadata_dir: PathBuf,
    pub submissions_jsonl: PathBuf,
    pub results_jsonl: PathBuf,
    pub cors_allowed_origins: Vec<String>,
    pub api_keys: Vec<String>,
    pub rate_limit_requests_per_minute: u64,
    pub job_retention_limit: usize,
    pub metadata_retention_limit: usize,
    pub workers: usize,
    pub queue_size: usize,
    pub body_limit_bytes: usize,
    pub request_timeout_seconds: u64,
    pub rust_log: String,
    pub yt_dlp_command: String,
    pub cookies_path: Option<PathBuf>,
    pub cookie_profiles: BTreeMap<String, PathBuf>,
    pub format: Option<String>,
    pub proxy: Option<String>,
    pub platform_policies: BTreeMap<String, PlatformDownloadPolicy>,
    pub download_enabled_platforms: Vec<String>,
    pub max_urls_per_request: usize,
    pub job_timeout_seconds: u64,
    pub download_max_attempts: usize,
    pub download_initial_backoff_ms: u64,
    pub max_download_storage_bytes: u64,
    pub min_free_disk_bytes: u64,
    pub post_processing: PostProcessingConfig,
    pub object_storage: ObjectStorageConfig,
    pub webhook_timeout_seconds: u64,
    pub webhook_connect_timeout_seconds: u64,
    pub webhook_max_attempts: usize,
    pub webhook_initial_backoff_ms: u64,
    pub webhook_signing_secret: Option<String>,
    pub webhooks_dead_letter_jsonl: PathBuf,
    pub allow_private_webhook_urls: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlatformDownloadPolicy {
    pub cookies_path: Option<PathBuf>,
    pub format: Option<String>,
    pub proxy: Option<String>,
    pub job_timeout_seconds: Option<u64>,
    pub download_max_attempts: Option<usize>,
    pub download_initial_backoff_ms: Option<u64>,
    pub max_concurrent: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveDownloadPolicy {
    pub cookies_path: Option<PathBuf>,
    pub format: Option<String>,
    pub proxy: Option<String>,
    pub job_timeout_seconds: u64,
    pub download_max_attempts: usize,
    pub download_initial_backoff_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostProcessingConfig {
    pub enabled: bool,
    pub fail_job_on_error: bool,
    pub commands: Vec<PostProcessingCommand>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostProcessingCommand {
    pub program: String,
    pub args: Vec<String>,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStorageConfig {
    pub backend: ObjectStorageBackend,
    pub endpoint_url: Option<String>,
    pub bucket: Option<String>,
    pub region: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub prefix: String,
    pub force_path_style: bool,
    pub public_base_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStorageBackend {
    Local,
    S3,
}

impl ObjectStorageBackend {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::S3 => "s3",
        }
    }
}

impl Config {
    pub fn load(config_path: Option<PathBuf>) -> anyhow::Result<Self> {
        let file_config = FileConfig::load(config_path)?;
        let server = file_config.server.unwrap_or_default();
        let queue = file_config.queue.unwrap_or_default();
        let download = file_config.download.unwrap_or_default();
        let post_processing = file_config.post_processing.unwrap_or_default();
        let storage = file_config.storage.unwrap_or_default();
        let webhooks = file_config.webhooks.unwrap_or_default();
        let logging = file_config.logging.unwrap_or_default();
        let retention = file_config.retention.unwrap_or_default();

        let data_dir = path_setting("DATA_DIR", server.data_dir, DEFAULT_DATA_DIR);
        let metadata_dir = data_dir.join("metadata");
        let webhooks_dead_letter_jsonl = metadata_dir.join("webhooks_dead_letter.jsonl");
        let addr = string_setting("BIND_ADDR", server.bind_addr, DEFAULT_ADDR)
            .parse()
            .context("BIND_ADDR must be a socket address, for example 127.0.0.1:3000")?;
        let downloads_dir = env_path("DOWNLOAD_OUTPUT_DIR")
            .or(download.output_dir)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DOWNLOAD_OUTPUT_DIR));

        let platform_policies = platform_policies_setting(download.platforms)?;

        Ok(Self {
            addr,
            downloads_dir,
            submissions_jsonl: metadata_dir.join("download_submissions.jsonl"),
            results_jsonl: metadata_dir.join("download_results.jsonl"),
            cors_allowed_origins: string_list_setting(
                "CORS_ALLOWED_ORIGINS",
                server.cors_allowed_origins,
            ),
            api_keys: string_list_setting("API_KEYS", server.api_keys),
            rate_limit_requests_per_minute: u64_setting(
                "RATE_LIMIT_REQUESTS_PER_MINUTE",
                server.rate_limit_requests_per_minute,
                0,
            )?,
            job_retention_limit: usize_setting(
                "JOB_RETENTION_LIMIT",
                retention.job_retention_limit,
                DEFAULT_JOB_RETENTION_LIMIT,
            )?,
            metadata_retention_limit: usize_setting(
                "METADATA_RETENTION_LIMIT",
                retention.metadata_retention_limit,
                DEFAULT_METADATA_RETENTION_LIMIT,
            )?,
            workers: usize_setting(
                "DOWNLOAD_WORKERS",
                download.workers,
                DEFAULT_DOWNLOAD_WORKERS,
            )?
            .max(1),
            queue_size: usize_setting("QUEUE_SIZE", queue.queue_size, DEFAULT_QUEUE_SIZE)?,
            body_limit_bytes: usize_setting(
                "BODY_LIMIT_BYTES",
                queue.body_limit_bytes,
                DEFAULT_BODY_LIMIT_BYTES,
            )?,
            request_timeout_seconds: u64_setting(
                "REQUEST_TIMEOUT_SECONDS",
                queue.request_timeout_seconds,
                DEFAULT_REQUEST_TIMEOUT_SECONDS,
            )?,
            rust_log: string_setting("RUST_LOG", logging.rust_log, DEFAULT_RUST_LOG),
            yt_dlp_command: string_setting(
                "YT_DLP_COMMAND",
                download.yt_dlp_command,
                DEFAULT_YT_DLP_COMMAND,
            ),
            cookies_path: optional_path_setting("YT_DLP_COOKIES_PATH", download.cookies_path),
            cookie_profiles: cookie_profiles_setting(download.cookie_profiles)?,
            format: secret_setting("YT_DLP_FORMAT", download.format),
            proxy: secret_setting("YT_DLP_PROXY", download.proxy),
            platform_policies,
            download_enabled_platforms: platform_list_setting(
                "DOWNLOAD_ENABLED_PLATFORMS",
                download.enabled_platforms,
                platforms::default_enabled_platforms(),
            )?,
            max_urls_per_request: usize_setting(
                "MAX_URLS_PER_REQUEST",
                download.max_urls_per_request,
                DEFAULT_MAX_URLS_PER_REQUEST,
            )?
            .max(1),
            job_timeout_seconds: u64_setting(
                "JOB_TIMEOUT_SECONDS",
                download.job_timeout_seconds,
                DEFAULT_JOB_TIMEOUT_SECONDS,
            )?,
            download_max_attempts: usize_setting(
                "DOWNLOAD_MAX_ATTEMPTS",
                download.download_max_attempts,
                DEFAULT_DOWNLOAD_MAX_ATTEMPTS,
            )?
            .max(1),
            download_initial_backoff_ms: u64_setting(
                "DOWNLOAD_INITIAL_BACKOFF_MS",
                download.download_initial_backoff_ms,
                DEFAULT_DOWNLOAD_INITIAL_BACKOFF_MS,
            )?,
            max_download_storage_bytes: u64_setting(
                "MAX_DOWNLOAD_STORAGE_BYTES",
                download.max_download_storage_bytes,
                DEFAULT_MAX_DOWNLOAD_STORAGE_BYTES,
            )?,
            min_free_disk_bytes: u64_setting(
                "MIN_FREE_DISK_BYTES",
                download.min_free_disk_bytes,
                DEFAULT_MIN_FREE_DISK_BYTES,
            )?,
            post_processing: post_processing_setting(post_processing)?,
            object_storage: object_storage_setting(storage)?,
            webhook_timeout_seconds: u64_setting(
                "WEBHOOK_TIMEOUT_SECONDS",
                webhooks.webhook_timeout_seconds,
                DEFAULT_WEBHOOK_TIMEOUT_SECONDS,
            )?,
            webhook_connect_timeout_seconds: u64_setting(
                "WEBHOOK_CONNECT_TIMEOUT_SECONDS",
                webhooks.webhook_connect_timeout_seconds,
                DEFAULT_WEBHOOK_CONNECT_TIMEOUT_SECONDS,
            )?,
            webhook_max_attempts: usize_setting(
                "WEBHOOK_MAX_ATTEMPTS",
                webhooks.webhook_max_attempts,
                DEFAULT_WEBHOOK_MAX_ATTEMPTS,
            )?
            .max(1),
            webhook_initial_backoff_ms: u64_setting(
                "WEBHOOK_INITIAL_BACKOFF_MS",
                webhooks.webhook_initial_backoff_ms,
                DEFAULT_WEBHOOK_INITIAL_BACKOFF_MS,
            )?,
            webhook_signing_secret: secret_setting(
                "WEBHOOK_SIGNING_SECRET",
                webhooks.webhook_signing_secret,
            ),
            allow_private_webhook_urls: bool_setting(
                "ALLOW_PRIVATE_WEBHOOK_URLS",
                webhooks.allow_private_webhook_urls,
                DEFAULT_ALLOW_PRIVATE_WEBHOOK_URLS,
            )?,
            metadata_dir,
            data_dir,
            webhooks_dead_letter_jsonl,
        })
    }

    pub async fn ensure_dirs(&self) -> anyhow::Result<()> {
        debug!(
            "ensuring data directories downloads_dir={} metadata_dir={}",
            self.downloads_dir.display(),
            self.metadata_dir.display()
        );
        fs::create_dir_all(&self.downloads_dir).await?;
        fs::create_dir_all(&self.metadata_dir).await?;
        Ok(())
    }

    pub fn effective_download_policy(
        &self,
        url: &str,
        cookie_profile: Option<&str>,
    ) -> EffectiveDownloadPolicy {
        let platform_policy =
            platform_for_url(url).and_then(|platform| self.platform_policies.get(platform));
        EffectiveDownloadPolicy {
            cookies_path: cookie_profile
                .and_then(|profile| self.cookie_profiles.get(profile).cloned())
                .or_else(|| platform_policy.and_then(|policy| policy.cookies_path.clone()))
                .or_else(|| self.cookies_path.clone()),
            format: platform_policy
                .and_then(|policy| policy.format.clone())
                .or_else(|| self.format.clone()),
            proxy: platform_policy
                .and_then(|policy| policy.proxy.clone())
                .or_else(|| self.proxy.clone()),
            job_timeout_seconds: platform_policy
                .and_then(|policy| policy.job_timeout_seconds)
                .unwrap_or(self.job_timeout_seconds),
            download_max_attempts: platform_policy
                .and_then(|policy| policy.download_max_attempts)
                .unwrap_or(self.download_max_attempts)
                .max(1),
            download_initial_backoff_ms: platform_policy
                .and_then(|policy| policy.download_initial_backoff_ms)
                .unwrap_or(self.download_initial_backoff_ms),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FileConfig {
    server: Option<ServerConfig>,
    queue: Option<QueueConfig>,
    download: Option<DownloadConfig>,
    post_processing: Option<PostProcessingFileConfig>,
    storage: Option<ObjectStorageFileConfig>,
    webhooks: Option<WebhookConfig>,
    logging: Option<LoggingConfig>,
    retention: Option<RetentionConfig>,
}

impl FileConfig {
    fn load(config_path: Option<PathBuf>) -> anyhow::Result<Self> {
        let has_cli_path = config_path.is_some();
        let has_env_path = env::var_os("CONFIG_PATH").is_some();
        let config_path = config_path
            .or_else(|| env_path("CONFIG_PATH"))
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
        let has_explicit_path = has_cli_path || has_env_path;

        let contents = match std_fs::read_to_string(&config_path) {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound && !has_explicit_path => {
                return Ok(Self::default());
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(anyhow!(
                    "config file does not exist: {}",
                    config_path.display()
                ));
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to read config file {}", config_path.display())
                });
            }
        };
        toml::from_str(&contents)
            .with_context(|| format!("failed to parse TOML config {}", config_path.display()))
    }
}

#[derive(Debug, Default, Deserialize)]
struct ServerConfig {
    bind_addr: Option<String>,
    data_dir: Option<PathBuf>,
    cors_allowed_origins: Option<Vec<String>>,
    api_keys: Option<Vec<String>>,
    rate_limit_requests_per_minute: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct QueueConfig {
    queue_size: Option<usize>,
    body_limit_bytes: Option<usize>,
    request_timeout_seconds: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct DownloadConfig {
    workers: Option<usize>,
    output_dir: Option<PathBuf>,
    yt_dlp_command: Option<String>,
    cookies_path: Option<PathBuf>,
    cookie_profiles: Option<BTreeMap<String, PathBuf>>,
    format: Option<String>,
    proxy: Option<String>,
    platforms: Option<BTreeMap<String, PlatformDownloadConfig>>,
    enabled_platforms: Option<Vec<String>>,
    max_urls_per_request: Option<usize>,
    job_timeout_seconds: Option<u64>,
    download_max_attempts: Option<usize>,
    download_initial_backoff_ms: Option<u64>,
    max_download_storage_bytes: Option<u64>,
    min_free_disk_bytes: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct PlatformDownloadConfig {
    cookies_path: Option<PathBuf>,
    format: Option<String>,
    proxy: Option<String>,
    job_timeout_seconds: Option<u64>,
    download_max_attempts: Option<usize>,
    download_initial_backoff_ms: Option<u64>,
    max_concurrent: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct PostProcessingFileConfig {
    enabled: Option<bool>,
    fail_job_on_error: Option<bool>,
    commands: Option<Vec<PostProcessingCommandConfig>>,
}

#[derive(Debug, Default, Deserialize)]
struct PostProcessingCommandConfig {
    program: Option<String>,
    args: Option<Vec<String>>,
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct ObjectStorageFileConfig {
    backend: Option<String>,
    endpoint_url: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    prefix: Option<String>,
    force_path_style: Option<bool>,
    public_base_url: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct WebhookConfig {
    webhook_timeout_seconds: Option<u64>,
    webhook_connect_timeout_seconds: Option<u64>,
    webhook_max_attempts: Option<usize>,
    webhook_initial_backoff_ms: Option<u64>,
    webhook_signing_secret: Option<String>,
    allow_private_webhook_urls: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct LoggingConfig {
    rust_log: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct RetentionConfig {
    job_retention_limit: Option<usize>,
    metadata_retention_limit: Option<usize>,
}

fn env_path(key: &str) -> Option<PathBuf> {
    env::var_os(key).map(PathBuf::from)
}

fn optional_path_setting(key: &str, file_value: Option<PathBuf>) -> Option<PathBuf> {
    env_path(key)
        .or(file_value)
        .filter(|path| !path.as_os_str().is_empty())
}

fn path_setting(key: &str, file_value: Option<PathBuf>, default: &str) -> PathBuf {
    env_path(key)
        .or(file_value)
        .unwrap_or_else(|| PathBuf::from(default))
}

fn string_setting(key: &str, file_value: Option<String>, default: &str) -> String {
    env::var(key)
        .ok()
        .or(file_value)
        .unwrap_or_else(|| default.into())
}

fn usize_setting(key: &str, file_value: Option<usize>, default: usize) -> anyhow::Result<usize> {
    match env::var(key) {
        Ok(value) => value
            .parse()
            .map_err(|err| anyhow!("{key} has invalid value `{value}`: {err}")),
        Err(_) => Ok(file_value.unwrap_or(default)),
    }
}

fn u64_setting(key: &str, file_value: Option<u64>, default: u64) -> anyhow::Result<u64> {
    match env::var(key) {
        Ok(value) => value
            .parse()
            .map_err(|err| anyhow!("{key} has invalid value `{value}`: {err}")),
        Err(_) => Ok(file_value.unwrap_or(default)),
    }
}

fn bool_setting(key: &str, file_value: Option<bool>, default: bool) -> anyhow::Result<bool> {
    match env::var(key) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(anyhow!(
                "{key} has invalid value `{value}`; expected true or false"
            )),
        },
        Err(_) => Ok(file_value.unwrap_or(default)),
    }
}

fn string_list_setting(key: &str, file_value: Option<Vec<String>>) -> Vec<String> {
    env::var(key)
        .ok()
        .map(|value| split_string_list(&value))
        .or(file_value)
        .unwrap_or_default()
}

fn platform_list_setting(
    key: &str,
    file_value: Option<Vec<String>>,
    default: Vec<String>,
) -> anyhow::Result<Vec<String>> {
    let values = env::var(key)
        .ok()
        .map(|value| split_string_list(&value))
        .or(file_value)
        .unwrap_or(default);
    platforms::validate_enabled_platforms(values)
}

fn platform_policies_setting(
    file_value: Option<BTreeMap<String, PlatformDownloadConfig>>,
) -> anyhow::Result<BTreeMap<String, PlatformDownloadPolicy>> {
    let mut policies = BTreeMap::new();
    for (key, value) in file_value.unwrap_or_default() {
        let platform = key.trim().to_ascii_lowercase();
        if platform.is_empty() {
            continue;
        }
        if !platforms::known_platforms()
            .iter()
            .any(|known| *known == platform)
        {
            return Err(anyhow!(
                "unsupported download platform policy `{platform}`; supported values are {}",
                platforms::known_platforms().join(", ")
            ));
        }
        policies.insert(
            platform,
            PlatformDownloadPolicy {
                cookies_path: value
                    .cookies_path
                    .filter(|path| !path.as_os_str().is_empty()),
                format: optional_trimmed_string(value.format),
                proxy: optional_trimmed_string(value.proxy),
                job_timeout_seconds: value.job_timeout_seconds,
                download_max_attempts: value.download_max_attempts.map(|attempts| attempts.max(1)),
                download_initial_backoff_ms: value.download_initial_backoff_ms,
                max_concurrent: value.max_concurrent.map(|limit| limit.max(1)),
            },
        );
    }
    Ok(policies)
}

fn post_processing_setting(
    file_value: PostProcessingFileConfig,
) -> anyhow::Result<PostProcessingConfig> {
    let enabled = bool_setting(
        "POST_PROCESSING_ENABLED",
        file_value.enabled,
        DEFAULT_POST_PROCESSING_ENABLED,
    )?;
    let fail_job_on_error = bool_setting(
        "POST_PROCESSING_FAIL_JOB_ON_ERROR",
        file_value.fail_job_on_error,
        DEFAULT_POST_PROCESSING_FAIL_JOB_ON_ERROR,
    )?;
    let mut commands = Vec::new();
    for (index, command) in file_value
        .commands
        .unwrap_or_default()
        .into_iter()
        .enumerate()
    {
        let program = optional_trimmed_string(command.program)
            .ok_or_else(|| anyhow!("post_processing.commands[{index}].program is required"))?;
        commands.push(PostProcessingCommand {
            program,
            args: command.args.unwrap_or_default(),
            timeout_seconds: command.timeout_seconds.unwrap_or(0),
        });
    }
    Ok(PostProcessingConfig {
        enabled,
        fail_job_on_error,
        commands,
    })
}

fn object_storage_setting(
    file_value: ObjectStorageFileConfig,
) -> anyhow::Result<ObjectStorageConfig> {
    let backend = object_storage_backend(&string_setting(
        "OBJECT_STORAGE_BACKEND",
        file_value.backend,
        DEFAULT_OBJECT_STORAGE_BACKEND,
    ))?;
    let endpoint_url = secret_setting("OBJECT_STORAGE_ENDPOINT_URL", file_value.endpoint_url);
    let bucket = secret_setting("OBJECT_STORAGE_BUCKET", file_value.bucket);
    let region = string_setting(
        "OBJECT_STORAGE_REGION",
        file_value.region,
        DEFAULT_OBJECT_STORAGE_REGION,
    );
    let access_key_id = secret_setting("OBJECT_STORAGE_ACCESS_KEY_ID", file_value.access_key_id);
    let secret_access_key = secret_setting(
        "OBJECT_STORAGE_SECRET_ACCESS_KEY",
        file_value.secret_access_key,
    );
    let session_token = secret_setting("OBJECT_STORAGE_SESSION_TOKEN", file_value.session_token);
    let prefix = secret_setting("OBJECT_STORAGE_PREFIX", file_value.prefix).unwrap_or_default();
    let force_path_style = bool_setting(
        "OBJECT_STORAGE_FORCE_PATH_STYLE",
        file_value.force_path_style,
        DEFAULT_OBJECT_STORAGE_FORCE_PATH_STYLE,
    )?;
    let public_base_url =
        secret_setting("OBJECT_STORAGE_PUBLIC_BASE_URL", file_value.public_base_url);

    if backend == ObjectStorageBackend::S3 {
        if endpoint_url.is_none() {
            return Err(anyhow!(
                "OBJECT_STORAGE_ENDPOINT_URL is required when storage.backend is s3"
            ));
        }
        if bucket.is_none() {
            return Err(anyhow!(
                "OBJECT_STORAGE_BUCKET is required when storage.backend is s3"
            ));
        }
        if access_key_id.is_none() {
            return Err(anyhow!(
                "OBJECT_STORAGE_ACCESS_KEY_ID is required when storage.backend is s3"
            ));
        }
        if secret_access_key.is_none() {
            return Err(anyhow!(
                "OBJECT_STORAGE_SECRET_ACCESS_KEY is required when storage.backend is s3"
            ));
        }
    }

    Ok(ObjectStorageConfig {
        backend,
        endpoint_url,
        bucket,
        region,
        access_key_id,
        secret_access_key,
        session_token,
        prefix: normalize_object_prefix(&prefix),
        force_path_style,
        public_base_url,
    })
}

fn object_storage_backend(value: &str) -> anyhow::Result<ObjectStorageBackend> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "local" => Ok(ObjectStorageBackend::Local),
        "s3" => Ok(ObjectStorageBackend::S3),
        other => Err(anyhow!(
            "unsupported object storage backend `{other}`; supported values are local, s3"
        )),
    }
}

fn normalize_object_prefix(prefix: &str) -> String {
    prefix.trim().trim_matches('/').to_string()
}

fn cookie_profiles_setting(
    file_value: Option<BTreeMap<String, PathBuf>>,
) -> anyhow::Result<BTreeMap<String, PathBuf>> {
    let mut profiles = BTreeMap::new();
    for (name, path) in file_value.unwrap_or_default() {
        let name = normalize_cookie_profile_name(&name)?;
        if path.as_os_str().is_empty() {
            continue;
        }
        profiles.insert(name, path);
    }
    Ok(profiles)
}

pub fn normalize_cookie_profile_name(name: &str) -> anyhow::Result<String> {
    let name = name.trim().to_ascii_lowercase();
    if name.is_empty() {
        return Err(anyhow!("cookie profile name must not be empty"));
    }
    if name.len() > 64
        || !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(anyhow!(
            "cookie profile name `{name}` must use only ASCII letters, numbers, dash, or underscore and be at most 64 characters"
        ));
    }
    Ok(name)
}

fn split_string_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect()
}

fn secret_setting(key: &str, file_value: Option<String>) -> Option<String> {
    env::var(key)
        .ok()
        .or(file_value)
        .and_then(|value| optional_trimmed_string(Some(value)))
}

fn optional_trimmed_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn platform_for_url(url: &str) -> Option<&'static str> {
    let url = Url::parse(url).ok()?;
    let host = url.host_str()?;
    platforms::platform_for_host(host)
}

#[cfg(test)]
mod tests {
    use std::{env, fs, sync::Mutex};

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn defaults_to_download_config() {
        let _guard = ENV_LOCK.lock().unwrap();
        let config = Config::load(None).unwrap();

        assert_eq!(config.workers, 1);
        assert_eq!(config.downloads_dir, PathBuf::from("data/downloads"));
        assert_eq!(config.yt_dlp_command, "uv");
        assert_eq!(config.cookies_path, None);
        assert!(config.cookie_profiles.is_empty());
        assert_eq!(config.format, None);
        assert_eq!(config.proxy, None);
        assert!(config.platform_policies.is_empty());
        assert_eq!(config.download_enabled_platforms.len(), 10);
        assert!(
            config
                .download_enabled_platforms
                .contains(&"youtube".to_string())
        );
        assert!(
            config
                .download_enabled_platforms
                .contains(&"tiktok".to_string())
        );
        assert_eq!(config.max_urls_per_request, 100);
        assert_eq!(config.job_timeout_seconds, 1_800);
        assert_eq!(config.download_max_attempts, 3);
        assert_eq!(config.download_initial_backoff_ms, 1_000);
        assert_eq!(config.max_download_storage_bytes, 0);
        assert_eq!(config.min_free_disk_bytes, 0);
        assert!(!config.post_processing.enabled);
        assert!(config.post_processing.fail_job_on_error);
        assert!(config.post_processing.commands.is_empty());
        assert_eq!(config.object_storage.backend, ObjectStorageBackend::Local);
        assert_eq!(
            config.submissions_jsonl,
            PathBuf::from("data/metadata/download_submissions.jsonl")
        );
    }

    #[test]
    fn parses_toml_download_config() {
        let _guard = ENV_LOCK.lock().unwrap();
        let path = temp_path("download-config.toml");
        fs::write(
            &path,
            r#"
[server]
bind_addr = "127.0.0.1:4000"
data_dir = "custom-data"

[queue]
queue_size = 8
body_limit_bytes = 4096
request_timeout_seconds = 20

[download]
workers = 2
output_dir = "custom-downloads"
yt_dlp_command = "/usr/bin/uv"
cookies_path = "cookies.txt"
cookie_profiles.account_a = "account-a-cookies.txt"
format = "bv*+ba/b"
proxy = "socks5://127.0.0.1:1080"
enabled_platforms = ["youtube", "instagram", "vk"]
max_urls_per_request = 12
job_timeout_seconds = 45
download_max_attempts = 4
download_initial_backoff_ms = 250
max_download_storage_bytes = 1048576
min_free_disk_bytes = 524288

[download.platforms.instagram]
cookies_path = "instagram-cookies.txt"
format = "mp4/best"
proxy = "http://127.0.0.1:8080"
job_timeout_seconds = 90
download_max_attempts = 6
download_initial_backoff_ms = 750
max_concurrent = 1

[post_processing]
enabled = true
fail_job_on_error = false

[[post_processing.commands]]
program = "ffmpeg"
args = ["-i", "{media_path}", "{job_dir}/{job_id}.processed.mp4"]
timeout_seconds = 600

[storage]
backend = "s3"
endpoint_url = "http://localhost:9000"
bucket = "downloads"
region = "us-east-1"
access_key_id = "access"
secret_access_key = "secret"
prefix = "videos"
force_path_style = true
public_base_url = "https://cdn.example.com/videos"
"#,
        )
        .unwrap();

        let config = Config::load(Some(path.clone())).unwrap();

        assert_eq!(config.addr.to_string(), "127.0.0.1:4000");
        assert_eq!(config.data_dir, PathBuf::from("custom-data"));
        assert_eq!(config.downloads_dir, PathBuf::from("custom-downloads"));
        assert_eq!(config.queue_size, 8);
        assert_eq!(config.body_limit_bytes, 4096);
        assert_eq!(config.request_timeout_seconds, 20);
        assert_eq!(config.workers, 2);
        assert_eq!(config.yt_dlp_command, "/usr/bin/uv");
        assert_eq!(config.cookies_path, Some(PathBuf::from("cookies.txt")));
        assert_eq!(
            config.cookie_profiles.get("account_a"),
            Some(&PathBuf::from("account-a-cookies.txt"))
        );
        assert_eq!(config.format.as_deref(), Some("bv*+ba/b"));
        assert_eq!(config.proxy.as_deref(), Some("socks5://127.0.0.1:1080"));
        assert_eq!(
            config.download_enabled_platforms,
            vec!["youtube", "instagram", "vk"]
        );
        assert_eq!(config.max_urls_per_request, 12);
        assert_eq!(config.job_timeout_seconds, 45);
        assert_eq!(config.download_max_attempts, 4);
        assert_eq!(config.download_initial_backoff_ms, 250);
        assert_eq!(config.max_download_storage_bytes, 1048576);
        assert_eq!(config.min_free_disk_bytes, 524288);
        let instagram = config.platform_policies.get("instagram").unwrap();
        assert_eq!(
            instagram.cookies_path,
            Some(PathBuf::from("instagram-cookies.txt"))
        );
        assert_eq!(instagram.format.as_deref(), Some("mp4/best"));
        assert_eq!(instagram.proxy.as_deref(), Some("http://127.0.0.1:8080"));
        assert_eq!(instagram.job_timeout_seconds, Some(90));
        assert_eq!(instagram.download_max_attempts, Some(6));
        assert_eq!(instagram.download_initial_backoff_ms, Some(750));
        assert_eq!(instagram.max_concurrent, Some(1));
        assert!(config.post_processing.enabled);
        assert!(!config.post_processing.fail_job_on_error);
        assert_eq!(config.post_processing.commands.len(), 1);
        assert_eq!(config.post_processing.commands[0].program, "ffmpeg");
        assert_eq!(config.post_processing.commands[0].timeout_seconds, 600);
        assert_eq!(config.object_storage.backend, ObjectStorageBackend::S3);
        assert_eq!(
            config.object_storage.endpoint_url.as_deref(),
            Some("http://localhost:9000")
        );
        assert_eq!(config.object_storage.bucket.as_deref(), Some("downloads"));
        assert_eq!(config.object_storage.prefix, "videos");
        assert!(config.object_storage.force_path_style);
        assert_eq!(
            config.object_storage.public_base_url.as_deref(),
            Some("https://cdn.example.com/videos")
        );

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn resolves_platform_download_policy_from_url() {
        let _guard = ENV_LOCK.lock().unwrap();
        let path = temp_path("platform-policy.toml");
        fs::write(
            &path,
            r#"
[download]
cookies_path = "global-cookies.txt"
format = "global"
proxy = "http://global-proxy"
job_timeout_seconds = 45
download_max_attempts = 4
download_initial_backoff_ms = 250

[download.platforms.instagram]
cookies_path = "instagram-cookies.txt"
format = "instagram"
proxy = "http://instagram-proxy"
job_timeout_seconds = 90
download_max_attempts = 6
download_initial_backoff_ms = 750
"#,
        )
        .unwrap();

        let config = Config::load(Some(path.clone())).unwrap();
        let instagram =
            config.effective_download_policy("https://www.instagram.com/reel/abc/", None);
        let tiktok =
            config.effective_download_policy("https://www.tiktok.com/@user/video/123", None);

        assert_eq!(
            instagram.cookies_path,
            Some(PathBuf::from("instagram-cookies.txt"))
        );
        assert_eq!(instagram.format.as_deref(), Some("instagram"));
        assert_eq!(instagram.proxy.as_deref(), Some("http://instagram-proxy"));
        assert_eq!(instagram.job_timeout_seconds, 90);
        assert_eq!(instagram.download_max_attempts, 6);
        assert_eq!(instagram.download_initial_backoff_ms, 750);
        assert_eq!(
            tiktok.cookies_path,
            Some(PathBuf::from("global-cookies.txt"))
        );
        assert_eq!(tiktok.format.as_deref(), Some("global"));
        assert_eq!(tiktok.proxy.as_deref(), Some("http://global-proxy"));
        assert_eq!(tiktok.job_timeout_seconds, 45);
        assert_eq!(tiktok.download_max_attempts, 4);
        assert_eq!(tiktok.download_initial_backoff_ms, 250);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn cookie_profile_overrides_global_and_platform_cookie_path() {
        let _guard = ENV_LOCK.lock().unwrap();
        let path = temp_path("cookie-profile.toml");
        fs::write(
            &path,
            r#"
[download]
cookies_path = "global-cookies.txt"
cookie_profiles.account_a = "account-a-cookies.txt"

[download.platforms.instagram]
cookies_path = "instagram-cookies.txt"
"#,
        )
        .unwrap();

        let config = Config::load(Some(path.clone())).unwrap();
        let policy = config
            .effective_download_policy("https://www.instagram.com/reel/abc/", Some("account_a"));

        assert_eq!(
            policy.cookies_path,
            Some(PathBuf::from("account-a-cookies.txt"))
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn env_overrides_download_config() {
        let _guard = ENV_LOCK.lock().unwrap();
        with_envs(
            &[
                ("DOWNLOAD_WORKERS", "3"),
                ("DOWNLOAD_OUTPUT_DIR", "env-downloads"),
                ("YT_DLP_COMMAND", "uvx"),
                ("YT_DLP_COOKIES_PATH", "env-cookies.txt"),
                ("YT_DLP_FORMAT", "mp4"),
                ("YT_DLP_PROXY", "http://127.0.0.1:8080"),
                ("DOWNLOAD_ENABLED_PLATFORMS", "youtube,tiktok"),
                ("MAX_URLS_PER_REQUEST", "7"),
                ("JOB_TIMEOUT_SECONDS", "9"),
                ("DOWNLOAD_MAX_ATTEMPTS", "5"),
                ("DOWNLOAD_INITIAL_BACKOFF_MS", "17"),
                ("MAX_DOWNLOAD_STORAGE_BYTES", "11"),
                ("MIN_FREE_DISK_BYTES", "13"),
                ("POST_PROCESSING_ENABLED", "true"),
                ("POST_PROCESSING_FAIL_JOB_ON_ERROR", "false"),
                ("OBJECT_STORAGE_BACKEND", "s3"),
                ("OBJECT_STORAGE_ENDPOINT_URL", "http://localhost:9000"),
                ("OBJECT_STORAGE_BUCKET", "downloads"),
                ("OBJECT_STORAGE_REGION", "eu-central-1"),
                ("OBJECT_STORAGE_ACCESS_KEY_ID", "access"),
                ("OBJECT_STORAGE_SECRET_ACCESS_KEY", "secret"),
                ("OBJECT_STORAGE_PREFIX", "/env-prefix/"),
            ],
            || {
                let config = Config::load(None).unwrap();

                assert_eq!(config.workers, 3);
                assert_eq!(config.downloads_dir, PathBuf::from("env-downloads"));
                assert_eq!(config.yt_dlp_command, "uvx");
                assert_eq!(config.cookies_path, Some(PathBuf::from("env-cookies.txt")));
                assert_eq!(config.format.as_deref(), Some("mp4"));
                assert_eq!(config.proxy.as_deref(), Some("http://127.0.0.1:8080"));
                assert_eq!(config.download_enabled_platforms, vec!["youtube", "tiktok"]);
                assert_eq!(config.max_urls_per_request, 7);
                assert_eq!(config.job_timeout_seconds, 9);
                assert_eq!(config.download_max_attempts, 5);
                assert_eq!(config.download_initial_backoff_ms, 17);
                assert_eq!(config.max_download_storage_bytes, 11);
                assert_eq!(config.min_free_disk_bytes, 13);
                assert!(config.post_processing.enabled);
                assert!(!config.post_processing.fail_job_on_error);
                assert_eq!(config.object_storage.backend, ObjectStorageBackend::S3);
                assert_eq!(config.object_storage.region, "eu-central-1");
                assert_eq!(config.object_storage.prefix, "env-prefix");
            },
        );
    }

    #[test]
    fn rejects_unknown_enabled_platform() {
        let _guard = ENV_LOCK.lock().unwrap();
        with_envs(&[("DOWNLOAD_ENABLED_PLATFORMS", "youtube,unknown")], || {
            let err = Config::load(None)
                .err()
                .expect("unknown platform should fail config loading");

            assert!(err.to_string().contains("unsupported platform `unknown`"));
        });
    }

    #[test]
    fn rejects_unknown_platform_policy() {
        let _guard = ENV_LOCK.lock().unwrap();
        let path = temp_path("unknown-platform-policy.toml");
        fs::write(
            &path,
            r#"
[download.platforms.unknown]
format = "best"
"#,
        )
        .unwrap();

        let err = Config::load(Some(path.clone()))
            .err()
            .expect("unknown platform policy should fail config loading");

        assert!(
            err.to_string()
                .contains("unsupported download platform policy `unknown`")
        );
        fs::remove_file(path).unwrap();
    }

    fn with_envs(envs: &[(&str, &str)], test: impl FnOnce()) {
        let originals = envs
            .iter()
            .map(|(key, _)| (*key, env::var_os(key)))
            .collect::<Vec<_>>();

        for (key, value) in envs {
            // SAFETY: tests that mutate process environment hold ENV_LOCK, so no other
            // test in this module concurrently reads or writes these variables.
            unsafe {
                env::set_var(key, value);
            }
        }

        test();

        for (key, original) in originals {
            // SAFETY: tests that mutate process environment hold ENV_LOCK, so restoring
            // variables is serialized with every environment-dependent config test.
            unsafe {
                match original {
                    Some(value) => env::set_var(key, value),
                    None => env::remove_var(key),
                }
            }
        }
    }

    fn temp_path(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        env::temp_dir().join(format!("yt-dlp-server-{nanos}-{name}"))
    }
}
