use std::{env, fs as std_fs, net::SocketAddr, path::PathBuf};

use anyhow::{Context, anyhow};
use log::debug;
use serde::Deserialize;
use tokio::fs;

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
const DEFAULT_MAX_DOWNLOAD_STORAGE_BYTES: u64 = 0;
const DEFAULT_MIN_FREE_DISK_BYTES: u64 = 0;

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
    pub format: Option<String>,
    pub proxy: Option<String>,
    pub max_urls_per_request: usize,
    pub job_timeout_seconds: u64,
    pub max_download_storage_bytes: u64,
    pub min_free_disk_bytes: u64,
    pub webhook_timeout_seconds: u64,
    pub webhook_connect_timeout_seconds: u64,
    pub webhook_max_attempts: usize,
    pub webhook_initial_backoff_ms: u64,
    pub webhook_signing_secret: Option<String>,
    pub webhooks_dead_letter_jsonl: PathBuf,
    pub allow_private_webhook_urls: bool,
}

impl Config {
    pub fn load(config_path: Option<PathBuf>) -> anyhow::Result<Self> {
        let file_config = FileConfig::load(config_path)?;
        let server = file_config.server.unwrap_or_default();
        let queue = file_config.queue.unwrap_or_default();
        let download = file_config.download.unwrap_or_default();
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
            format: secret_setting("YT_DLP_FORMAT", download.format),
            proxy: secret_setting("YT_DLP_PROXY", download.proxy),
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
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FileConfig {
    server: Option<ServerConfig>,
    queue: Option<QueueConfig>,
    download: Option<DownloadConfig>,
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

        if !config_path.exists() {
            if has_explicit_path {
                return Err(anyhow!(
                    "config file does not exist: {}",
                    config_path.display()
                ));
            }
            return Ok(Self::default());
        }

        let contents = std_fs::read_to_string(&config_path)
            .with_context(|| format!("failed to read config file {}", config_path.display()))?;
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
    format: Option<String>,
    proxy: Option<String>,
    max_urls_per_request: Option<usize>,
    job_timeout_seconds: Option<u64>,
    max_download_storage_bytes: Option<u64>,
    min_free_disk_bytes: Option<u64>,
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
        .map(|value| {
            value
                .split(',')
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
        })
        .or(file_value)
        .unwrap_or_default()
}

fn secret_setting(key: &str, file_value: Option<String>) -> Option<String> {
    env::var(key)
        .ok()
        .or(file_value)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
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
        assert_eq!(config.format, None);
        assert_eq!(config.proxy, None);
        assert_eq!(config.max_urls_per_request, 100);
        assert_eq!(config.job_timeout_seconds, 1_800);
        assert_eq!(config.max_download_storage_bytes, 0);
        assert_eq!(config.min_free_disk_bytes, 0);
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
format = "bv*+ba/b"
proxy = "socks5://127.0.0.1:1080"
max_urls_per_request = 12
job_timeout_seconds = 45
max_download_storage_bytes = 1048576
min_free_disk_bytes = 524288
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
        assert_eq!(config.format.as_deref(), Some("bv*+ba/b"));
        assert_eq!(config.proxy.as_deref(), Some("socks5://127.0.0.1:1080"));
        assert_eq!(config.max_urls_per_request, 12);
        assert_eq!(config.job_timeout_seconds, 45);
        assert_eq!(config.max_download_storage_bytes, 1048576);
        assert_eq!(config.min_free_disk_bytes, 524288);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn env_overrides_download_config() {
        let _guard = ENV_LOCK.lock().unwrap();
        with_env("DOWNLOAD_WORKERS", "3", || {
            with_env("DOWNLOAD_OUTPUT_DIR", "env-downloads", || {
                with_env("YT_DLP_COMMAND", "uvx", || {
                    with_env("YT_DLP_COOKIES_PATH", "env-cookies.txt", || {
                        with_env("YT_DLP_FORMAT", "mp4", || {
                            with_env("YT_DLP_PROXY", "http://127.0.0.1:8080", || {
                                with_env("MAX_URLS_PER_REQUEST", "7", || {
                                    with_env("JOB_TIMEOUT_SECONDS", "9", || {
                                        with_env("MAX_DOWNLOAD_STORAGE_BYTES", "11", || {
                                            with_env("MIN_FREE_DISK_BYTES", "13", || {
                                                let config = Config::load(None).unwrap();

                                                assert_eq!(config.workers, 3);
                                                assert_eq!(
                                                    config.downloads_dir,
                                                    PathBuf::from("env-downloads")
                                                );
                                                assert_eq!(config.yt_dlp_command, "uvx");
                                                assert_eq!(
                                                    config.cookies_path,
                                                    Some(PathBuf::from("env-cookies.txt"))
                                                );
                                                assert_eq!(config.format.as_deref(), Some("mp4"));
                                                assert_eq!(
                                                    config.proxy.as_deref(),
                                                    Some("http://127.0.0.1:8080")
                                                );
                                                assert_eq!(config.max_urls_per_request, 7);
                                                assert_eq!(config.job_timeout_seconds, 9);
                                                assert_eq!(config.max_download_storage_bytes, 11);
                                                assert_eq!(config.min_free_disk_bytes, 13);
                                            });
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    }

    fn with_env(key: &str, value: &str, test: impl FnOnce()) {
        let original = env::var_os(key);
        unsafe {
            env::set_var(key, value);
        }
        test();
        unsafe {
            match original {
                Some(value) => env::set_var(key, value),
                None => env::remove_var(key),
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
