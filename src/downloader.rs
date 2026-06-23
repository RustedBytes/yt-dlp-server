use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use log::{debug, warn};
use serde_json::Value;
use tokio::{fs, io::AsyncReadExt, process::Command, task::JoinHandle};
use uuid::Uuid;

use crate::{
    config::{Config, EffectiveDownloadPolicy, PostProcessingCommand},
    storage::store_download_artifacts,
    types::{DownloadAttempt, DownloadMetadata, PostProcessingStepResult},
};

#[derive(Debug)]
pub struct DownloadReport {
    pub metadata: DownloadMetadata,
    pub attempts: usize,
    pub attempt_errors: Vec<DownloadAttempt>,
}

#[derive(Debug, thiserror::Error)]
#[error("{source}")]
pub struct DownloadError {
    pub attempts: usize,
    pub attempt_errors: Vec<DownloadAttempt>,
    #[source]
    source: anyhow::Error,
}

pub type DownloadResult = Result<DownloadReport, DownloadError>;

pub async fn check_downloader(config: &Config) -> anyhow::Result<String> {
    yt_dlp_version(config).await
}

pub async fn download_url(
    config: &Config,
    id: uuid::Uuid,
    url: &str,
    format: Option<&str>,
    cookie_profile: Option<&str>,
    cancel_flag: Arc<AtomicBool>,
) -> DownloadResult {
    let started = Instant::now();
    let job_dir = config.downloads_dir.join(id.to_string());
    if let Err(err) = prepare_download_dir(&job_dir).await {
        return Err(download_error(err, 0, Vec::new()));
    }
    if let Err(err) = ensure_min_free_disk_space(config).await {
        cleanup_partial_download(&job_dir).await;
        return Err(download_error(err, 0, Vec::new()));
    }

    let version = yt_dlp_version(config).await.unwrap_or_else(|err| {
        warn!(
            "failed to read yt-dlp version before download job_id={} error={}",
            id, err
        );
        "unknown".to_string()
    });

    let policy = config.effective_download_policy(url, cookie_profile);
    let attempts = policy.download_max_attempts.max(1);
    let mut last_error = None;
    let mut attempt_errors = Vec::new();
    for attempt in 1..=attempts {
        if let Err(err) = prepare_download_dir(&job_dir).await {
            return Err(download_error(err, attempt - 1, attempt_errors));
        }
        let attempt_started = Instant::now();
        if cancel_flag.load(Ordering::Relaxed) {
            return Err(download_error(
                anyhow!("job canceled before download attempt"),
                attempt - 1,
                attempt_errors,
            ));
        }
        let outcome = match run_yt_dlp(
            config,
            id,
            &job_dir,
            url,
            format,
            cookie_profile,
            Arc::clone(&cancel_flag),
        )
        .await
        {
            Ok(()) => {
                finalize_download_metadata(
                    config,
                    id,
                    &job_dir,
                    url,
                    version.clone(),
                    started.elapsed().as_millis(),
                    Arc::clone(&cancel_flag),
                )
                .await
            }
            Err(err) => Err(err),
        };

        match outcome {
            Ok(metadata) => {
                return Ok(DownloadReport {
                    metadata,
                    attempts: attempt,
                    attempt_errors,
                });
            }
            Err(err) => {
                cleanup_partial_download(&job_dir).await;
                let error = err.to_string();
                last_error = Some(err);
                let retry_backoff = (attempt < attempts)
                    .then(|| retry_backoff(policy.download_initial_backoff_ms, attempt));
                attempt_errors.push(DownloadAttempt {
                    attempt,
                    error: error.clone(),
                    elapsed_ms: attempt_started.elapsed().as_millis(),
                    retry_backoff_ms: retry_backoff.map(|delay| delay.as_millis()),
                });
                let Some(delay) = retry_backoff else {
                    break;
                };
                warn!(
                    "download attempt failed job_id={} attempt={} max_attempts={} retry_backoff_ms={} error={}",
                    id,
                    attempt,
                    attempts,
                    delay.as_millis(),
                    error
                );
                if !delay.is_zero() && sleep_or_cancel(delay, &cancel_flag).await {
                    return Err(download_error(
                        anyhow!("job canceled during retry backoff"),
                        attempt,
                        attempt_errors,
                    ));
                }
            }
        }
    }

    Err(download_error(
        last_error.unwrap_or_else(|| anyhow!("download failed without running an attempt")),
        attempts,
        attempt_errors,
    ))
}

async fn finalize_download_metadata(
    config: &Config,
    id: Uuid,
    job_dir: &Path,
    url: &str,
    yt_dlp_version: String,
    elapsed_ms: u128,
    cancel_flag: Arc<AtomicBool>,
) -> anyhow::Result<DownloadMetadata> {
    let mut metadata =
        metadata_from_download_dir(job_dir, url, yt_dlp_version.clone(), elapsed_ms).await?;
    metadata.post_processing =
        run_post_processing(config, id, job_dir, &metadata, Arc::clone(&cancel_flag)).await?;
    if !metadata.post_processing.is_empty() {
        let post_processing = metadata.post_processing;
        metadata = metadata_from_download_dir(job_dir, url, yt_dlp_version, elapsed_ms).await?;
        metadata.post_processing = post_processing;
    }
    metadata.storage = store_download_artifacts(config, id, &metadata).await?;
    Ok(metadata)
}

fn download_error(
    source: anyhow::Error,
    attempts: usize,
    attempt_errors: Vec<DownloadAttempt>,
) -> DownloadError {
    DownloadError {
        attempts,
        attempt_errors,
        source,
    }
}

async fn prepare_download_dir(job_dir: &Path) -> anyhow::Result<()> {
    match fs::remove_dir_all(job_dir).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "failed to clear existing download dir {}",
                    job_dir.display()
                )
            });
        }
    }
    fs::create_dir_all(job_dir)
        .await
        .with_context(|| format!("failed to create download dir {}", job_dir.display()))
}

fn retry_backoff(initial_backoff_ms: u64, failed_attempt: usize) -> Duration {
    if initial_backoff_ms == 0 {
        return Duration::ZERO;
    }
    let exponent = failed_attempt.saturating_sub(1).min(20);
    let multiplier = 1_u64.checked_shl(exponent as u32).unwrap_or(u64::MAX);
    Duration::from_millis(initial_backoff_ms.saturating_mul(multiplier))
}

async fn sleep_or_cancel(delay: Duration, cancel_flag: &AtomicBool) -> bool {
    let started = Instant::now();
    while started.elapsed() < delay {
        if cancel_flag.load(Ordering::Relaxed) {
            return true;
        }
        let remaining = delay.saturating_sub(started.elapsed());
        tokio::time::sleep(remaining.min(Duration::from_millis(100))).await;
    }
    cancel_flag.load(Ordering::Relaxed)
}

pub fn download_command_args(
    config: &Config,
    id: Uuid,
    job_dir: &Path,
    url: &str,
    format: Option<&str>,
    cookie_profile: Option<&str>,
) -> Vec<OsString> {
    let policy = config.effective_download_policy(url, cookie_profile);
    download_command_args_with_policy(config, &policy, id, job_dir, url, format)
}

fn download_command_args_with_policy(
    config: &Config,
    policy: &EffectiveDownloadPolicy,
    id: Uuid,
    job_dir: &Path,
    url: &str,
    format: Option<&str>,
) -> Vec<OsString> {
    let mut args = yt_dlp_prefix_args(config);
    args.extend([
        OsString::from("--no-config"),
        OsString::from("--no-progress"),
        OsString::from("--no-playlist"),
        OsString::from("--write-info-json"),
        OsString::from("--paths"),
        OsString::from(format!("home:{}", job_dir.display())),
        OsString::from("-o"),
        OsString::from(format!("{id}.%(ext)s")),
    ]);
    if let Some(cookies_path) = &policy.cookies_path {
        args.push(OsString::from("--cookies"));
        args.push(cookies_path.as_os_str().to_os_string());
    }
    if let Some(format) = format.or(policy.format.as_deref()) {
        args.push(OsString::from("--format"));
        args.push(OsString::from(format));
    }
    if let Some(proxy) = &policy.proxy {
        args.push(OsString::from("--proxy"));
        args.push(OsString::from(proxy));
    }
    args.push(OsString::from(url));
    args
}

pub fn version_command_args(config: &Config) -> Vec<OsString> {
    let mut args = yt_dlp_prefix_args(config);
    args.push(OsString::from("--version"));
    args
}

async fn run_yt_dlp(
    config: &Config,
    id: Uuid,
    job_dir: &Path,
    url: &str,
    format: Option<&str>,
    cookie_profile: Option<&str>,
    cancel_flag: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let policy = config.effective_download_policy(url, cookie_profile);
    let output = run_command_with_timeout(
        &config.yt_dlp_command,
        download_command_args(config, id, job_dir, url, format, cookie_profile),
        policy.job_timeout_seconds,
        cancel_flag,
    )
    .await?;

    if !output.status.success() {
        return Err(anyhow!(
            "yt-dlp failed with status {}: {}",
            output.status,
            process_output_summary(&output.stderr, &output.stdout)
        ));
    }

    debug!(
        "yt-dlp download finished url={} stdout_bytes={} stderr_bytes={}",
        url,
        output.stdout.len(),
        output.stderr.len()
    );
    Ok(())
}

async fn run_post_processing(
    config: &Config,
    id: Uuid,
    job_dir: &Path,
    metadata: &DownloadMetadata,
    cancel_flag: Arc<AtomicBool>,
) -> anyhow::Result<Vec<PostProcessingStepResult>> {
    if !config.post_processing.enabled || config.post_processing.commands.is_empty() {
        return Ok(Vec::new());
    }

    let mut results = Vec::new();
    for (index, command) in config.post_processing.commands.iter().enumerate() {
        let result = run_post_processing_command(
            index,
            command,
            id,
            job_dir,
            metadata,
            Arc::clone(&cancel_flag),
        )
        .await;
        let step = match result {
            Ok(step) => step,
            Err(err) => PostProcessingStepResult {
                command_index: index,
                program: command.program.clone(),
                success: false,
                status_code: None,
                elapsed_ms: 0,
                stdout_tail: None,
                stderr_tail: None,
                error: Some(err.to_string()),
            },
        };
        let failed = !step.success;
        let error = step.error.clone();
        results.push(step);
        if failed {
            if config.post_processing.fail_job_on_error {
                return Err(anyhow!(
                    "post-processing command {} failed: {}",
                    index,
                    error.unwrap_or_else(|| "command returned non-zero status".to_string())
                ));
            }
            break;
        }
    }
    Ok(results)
}

async fn run_post_processing_command(
    index: usize,
    command: &PostProcessingCommand,
    id: Uuid,
    job_dir: &Path,
    metadata: &DownloadMetadata,
    cancel_flag: Arc<AtomicBool>,
) -> anyhow::Result<PostProcessingStepResult> {
    let started = Instant::now();
    let args = command
        .args
        .iter()
        .map(|arg| expand_post_processing_arg(arg, id, job_dir, metadata))
        .map(OsString::from)
        .collect::<Vec<_>>();
    let output =
        run_command_with_timeout(&command.program, args, command.timeout_seconds, cancel_flag)
            .await?;
    let stdout_tail = output_tail(&output.stdout);
    let stderr_tail = output_tail(&output.stderr);
    let success = output.status.success();
    let error = (!success).then(|| {
        format!(
            "post-processing command exited with status {}: {}",
            output.status,
            process_output_summary(&output.stderr, &output.stdout)
        )
    });

    debug!(
        "post-processing command finished index={} program={} success={} stdout_bytes={} stderr_bytes={}",
        index,
        command.program,
        success,
        output.stdout.len(),
        output.stderr.len()
    );

    Ok(PostProcessingStepResult {
        command_index: index,
        program: command.program.clone(),
        success,
        status_code: output.status.code(),
        elapsed_ms: started.elapsed().as_millis(),
        stdout_tail,
        stderr_tail,
        error,
    })
}

fn expand_post_processing_arg(
    value: &str,
    id: Uuid,
    job_dir: &Path,
    metadata: &DownloadMetadata,
) -> String {
    value
        .replace("{job_id}", &id.to_string())
        .replace("{job_dir}", &job_dir.display().to_string())
        .replace("{media_path}", &metadata.media_path.display().to_string())
        .replace(
            "{info_json_path}",
            &metadata.info_json_path.display().to_string(),
        )
}

async fn yt_dlp_version(config: &Config) -> anyhow::Result<String> {
    let output = run_command_with_timeout(
        &config.yt_dlp_command,
        version_command_args(config),
        30,
        Arc::new(AtomicBool::new(false)),
    )
    .await?;
    if !output.status.success() {
        return Err(anyhow!(
            "yt-dlp --version failed with status {}: {}",
            output.status,
            process_output_summary(&output.stderr, &output.stdout)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

async fn run_command_with_timeout(
    command: &str,
    args: Vec<OsString>,
    timeout_seconds: u64,
    cancel_flag: Arc<AtomicBool>,
) -> anyhow::Result<std::process::Output> {
    let mut child = Command::new(command)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to spawn `{command}`"))?;

    let stdout = child.stdout.take().map(read_pipe);
    let stderr = child.stderr.take().map(read_pipe);
    let started = Instant::now();

    let status = loop {
        if let Some(status) = child
            .try_wait()
            .with_context(|| format!("failed to poll `{command}`"))?
        {
            break status;
        }
        if timeout_seconds > 0 && started.elapsed() >= Duration::from_secs(timeout_seconds) {
            kill_child(&mut child).await;
            drain_pipe(stdout).await;
            drain_pipe(stderr).await;
            return Err(anyhow!("job timed out after {timeout_seconds} seconds"));
        }
        if cancel_flag.load(Ordering::Relaxed) {
            kill_child(&mut child).await;
            drain_pipe(stdout).await;
            drain_pipe(stderr).await;
            return Err(anyhow!("job canceled by request"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    Ok(std::process::Output {
        status,
        stdout: drain_pipe(stdout).await,
        stderr: drain_pipe(stderr).await,
    })
}

fn read_pipe<T>(mut pipe: T) -> JoinHandle<Vec<u8>>
where
    T: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut bytes = Vec::new();
        if let Err(err) = pipe.read_to_end(&mut bytes).await {
            warn!("failed to read process output pipe error={}", err);
        }
        bytes
    })
}

async fn drain_pipe(handle: Option<JoinHandle<Vec<u8>>>) -> Vec<u8> {
    match handle {
        Some(handle) => match handle.await {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!("process output reader task failed error={}", err);
                Vec::new()
            }
        },
        None => Vec::new(),
    }
}

async fn kill_child(child: &mut tokio::process::Child) {
    if let Err(err) = child.kill().await {
        debug!("failed to kill child process error={}", err);
    }
}

async fn ensure_min_free_disk_space(config: &Config) -> anyhow::Result<()> {
    if config.min_free_disk_bytes == 0 {
        return Ok(());
    }

    let available = available_disk_bytes(&config.downloads_dir).await?;
    if available < config.min_free_disk_bytes {
        return Err(anyhow!(
            "insufficient free disk space in {}: available {} bytes, required {} bytes",
            config.downloads_dir.display(),
            available,
            config.min_free_disk_bytes
        ));
    }

    Ok(())
}

#[cfg(unix)]
async fn available_disk_bytes(path: &Path) -> anyhow::Result<u64> {
    let output = Command::new("df")
        .args(["-Pk"])
        .arg(path)
        .stdin(Stdio::null())
        .output()
        .await
        .context("failed to run df for free disk check")?;
    if !output.status.success() {
        return Err(anyhow!(
            "df failed for free disk check: {}",
            process_output_summary(&output.stderr, &output.stdout)
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .nth(1)
        .ok_or_else(|| anyhow!("df output did not include a data row"))?;
    let available_kib = line
        .split_whitespace()
        .nth(3)
        .ok_or_else(|| anyhow!("df output did not include available space"))?
        .parse::<u64>()
        .context("failed to parse available disk space from df")?;
    available_kib
        .checked_mul(1024)
        .ok_or_else(|| anyhow!("available disk space value overflowed"))
}

#[cfg(not(unix))]
async fn available_disk_bytes(_path: &Path) -> anyhow::Result<u64> {
    Ok(u64::MAX)
}

async fn metadata_from_download_dir(
    job_dir: &Path,
    original_url: &str,
    yt_dlp_version: String,
    elapsed_ms: u128,
) -> anyhow::Result<DownloadMetadata> {
    let info_json_path = find_info_json(job_dir).await?;
    let info = fs::read_to_string(&info_json_path)
        .await
        .with_context(|| format!("failed to read {}", info_json_path.display()))?;
    let info = serde_json::from_str::<Value>(&info)
        .with_context(|| format!("failed to parse {}", info_json_path.display()))?;
    let media_path = find_media_file(job_dir).await?;
    let media_bytes = fs::metadata(&media_path)
        .await
        .with_context(|| format!("failed to inspect {}", media_path.display()))?
        .len();

    Ok(DownloadMetadata {
        original_url: original_url.to_string(),
        webpage_url: optional_string(&info, "webpage_url"),
        extractor: optional_string(&info, "extractor"),
        title: optional_string(&info, "title"),
        uploader: optional_string(&info, "uploader"),
        duration: optional_f64(&info, "duration"),
        extension: optional_string(&info, "ext").or_else(|| {
            media_path
                .extension()
                .and_then(|extension| extension.to_str())
                .map(str::to_string)
        }),
        media_path,
        media_bytes,
        info_json_path,
        yt_dlp_version,
        elapsed_ms,
        post_processing: Vec::new(),
        storage: None,
    })
}

async fn find_info_json(job_dir: &Path) -> anyhow::Result<PathBuf> {
    let mut entries = fs::read_dir(job_dir)
        .await
        .with_context(|| format!("failed to read {}", job_dir.display()))?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".info.json"))
        {
            return Ok(path);
        }
    }
    Err(anyhow!(
        "yt-dlp did not produce an info JSON file in {}",
        job_dir.display()
    ))
}

async fn find_media_file(job_dir: &Path) -> anyhow::Result<PathBuf> {
    let mut entries = fs::read_dir(job_dir)
        .await
        .with_context(|| format!("failed to read {}", job_dir.display()))?;
    let mut candidates = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let metadata = entry.metadata().await?;
        if !metadata.is_file() {
            continue;
        }
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.ends_with(".info.json") || name.ends_with(".part") || name.ends_with(".ytdl") {
            continue;
        }
        candidates.push((metadata.len(), path));
    }

    candidates
        .into_iter()
        .max_by_key(|(len, _)| *len)
        .map(|(_, path)| path)
        .ok_or_else(|| {
            anyhow!(
                "yt-dlp did not produce a media file in {}",
                job_dir.display()
            )
        })
}

async fn cleanup_partial_download(job_dir: &Path) {
    match fs::remove_dir_all(job_dir).await {
        Ok(()) => debug!("partial download cleaned up path={}", job_dir.display()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => warn!(
            "failed to clean up partial download path={} error={}",
            job_dir.display(),
            err
        ),
    }
}

fn yt_dlp_prefix_args(config: &Config) -> Vec<OsString> {
    if Path::new(&config.yt_dlp_command)
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "uv")
    {
        return vec![
            OsString::from("run"),
            OsString::from("--frozen"),
            OsString::from("yt-dlp"),
        ];
    }

    Vec::new()
}

fn optional_string(info: &Value, key: &str) -> Option<String> {
    info.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn optional_f64(info: &Value, key: &str) -> Option<f64> {
    info.get(key).and_then(Value::as_f64)
}

fn process_output_summary(stderr: &[u8], stdout: &[u8]) -> String {
    let text = if stderr.is_empty() { stdout } else { stderr };
    let text = String::from_utf8_lossy(text);
    let text = text.trim();
    if text.is_empty() {
        return "<no process output>".to_string();
    }
    text.chars().take(2_000).collect()
}

fn output_tail(bytes: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(bytes);
    let text = text.trim();
    if text.is_empty() {
        None
    } else {
        let mut chars = text.chars().rev().take(2_000).collect::<Vec<_>>();
        chars.reverse();
        Some(chars.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;

    #[test]
    fn builds_uv_download_command_with_cookie_file() {
        let mut config = test_config();
        config.cookies_path = Some(PathBuf::from("cookies.txt"));
        config.format = Some("bv*+ba/b".to_string());
        config.proxy = Some("socks5://127.0.0.1:1080".to_string());
        let id = Uuid::parse_str("10af7128-4b98-4e19-a494-17a3d5597e2c").unwrap();

        let args = download_command_args(
            &config,
            id,
            Path::new("data/downloads/job"),
            "https://www.tiktok.com/@user/video/123",
            None,
            None,
        )
        .into_iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

        assert_eq!(&args[0..3], ["run", "--frozen", "yt-dlp"]);
        assert!(args.contains(&format!("{id}.%(ext)s")));
        assert!(args.contains(&"--cookies".to_string()));
        assert!(args.contains(&"cookies.txt".to_string()));
        assert!(args.contains(&"--format".to_string()));
        assert!(args.contains(&"bv*+ba/b".to_string()));
        assert!(args.contains(&"--proxy".to_string()));
        assert!(args.contains(&"socks5://127.0.0.1:1080".to_string()));
        assert_eq!(
            args.last().unwrap(),
            "https://www.tiktok.com/@user/video/123"
        );
    }

    #[test]
    fn builds_direct_download_command_without_uv_prefix() {
        let mut config = test_config();
        config.yt_dlp_command = "/tmp/fake-yt-dlp".to_string();
        let id = Uuid::parse_str("10af7128-4b98-4e19-a494-17a3d5597e2c").unwrap();

        let args = download_command_args(
            &config,
            id,
            Path::new("data/downloads/job"),
            "https://www.instagram.com/reel/abc/",
            None,
            None,
        )
        .into_iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

        assert_eq!(args.first().unwrap(), "--no-config");
        assert!(args.contains(&format!("{id}.%(ext)s")));
        assert!(!args.contains(&"--cookies".to_string()));
    }

    #[test]
    fn download_command_uses_job_format_before_config_format() {
        let mut config = test_config();
        config.format = Some("bestvideo+bestaudio/best".to_string());
        let id = Uuid::parse_str("10af7128-4b98-4e19-a494-17a3d5597e2c").unwrap();

        let args = download_command_args(
            &config,
            id,
            Path::new("data/downloads/job"),
            "https://www.youtube.com/shorts/abcdefghijk",
            Some("mp4/best"),
            None,
        )
        .into_iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

        let format_index = args
            .iter()
            .position(|arg| arg == "--format")
            .expect("format flag should be present");
        assert_eq!(
            args.get(format_index + 1).map(String::as_str),
            Some("mp4/best")
        );
    }

    #[test]
    fn download_command_uses_platform_policy_before_global_config() {
        let mut config = test_config();
        config.cookies_path = Some(PathBuf::from("global-cookies.txt"));
        config.format = Some("global-format".to_string());
        config.proxy = Some("http://global-proxy".to_string());
        config.platform_policies.insert(
            "instagram".to_string(),
            crate::config::PlatformDownloadPolicy {
                cookies_path: Some(PathBuf::from("instagram-cookies.txt")),
                format: Some("instagram-format".to_string()),
                proxy: Some("http://instagram-proxy".to_string()),
                job_timeout_seconds: Some(90),
                download_max_attempts: Some(6),
                download_initial_backoff_ms: Some(750),
                max_concurrent: None,
            },
        );
        let id = Uuid::parse_str("10af7128-4b98-4e19-a494-17a3d5597e2c").unwrap();

        let args = download_command_args(
            &config,
            id,
            Path::new("data/downloads/job"),
            "https://www.instagram.com/reel/abc/",
            Some("request-format"),
            None,
        )
        .into_iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

        assert!(args.contains(&"instagram-cookies.txt".to_string()));
        assert!(!args.contains(&"global-cookies.txt".to_string()));
        assert!(args.contains(&"request-format".to_string()));
        assert!(!args.contains(&"instagram-format".to_string()));
        assert!(args.contains(&"http://instagram-proxy".to_string()));
        assert!(!args.contains(&"http://global-proxy".to_string()));
    }

    #[test]
    fn download_command_uses_cookie_profile_before_platform_policy() {
        let mut config = test_config();
        config.cookies_path = Some(PathBuf::from("global-cookies.txt"));
        config.cookie_profiles.insert(
            "account_a".to_string(),
            PathBuf::from("account-a-cookies.txt"),
        );
        config.platform_policies.insert(
            "instagram".to_string(),
            crate::config::PlatformDownloadPolicy {
                cookies_path: Some(PathBuf::from("instagram-cookies.txt")),
                format: None,
                proxy: None,
                job_timeout_seconds: None,
                download_max_attempts: None,
                download_initial_backoff_ms: None,
                max_concurrent: None,
            },
        );
        let id = Uuid::parse_str("10af7128-4b98-4e19-a494-17a3d5597e2c").unwrap();

        let args = download_command_args(
            &config,
            id,
            Path::new("data/downloads/job"),
            "https://www.instagram.com/reel/abc/",
            None,
            Some("account_a"),
        )
        .into_iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

        assert!(args.contains(&"account-a-cookies.txt".to_string()));
        assert!(!args.contains(&"instagram-cookies.txt".to_string()));
        assert!(!args.contains(&"global-cookies.txt".to_string()));
    }

    #[tokio::test]
    async fn retry_backoff_sleep_returns_when_canceled() {
        let cancel_flag = AtomicBool::new(true);

        assert!(sleep_or_cancel(Duration::from_secs(60), &cancel_flag).await);
    }

    #[tokio::test]
    async fn extracts_download_metadata_from_info_json_and_media_file() {
        let dir = temp_dir("metadata");
        fs::create_dir_all(&dir).await.unwrap();
        let id = Uuid::parse_str("10af7128-4b98-4e19-a494-17a3d5597e2c").unwrap();
        let info_path = dir.join(format!("{id}.info.json"));
        let media_path = dir.join(format!("{id}.mp4"));
        fs::write(
            &info_path,
            r#"{
                "webpage_url": "https://www.instagram.com/reel/abc/",
                "extractor": "Instagram",
                "title": "Clip title",
                "uploader": "creator",
                "duration": 12.5,
                "ext": "mp4"
            }"#,
        )
        .await
        .unwrap();
        fs::write(&media_path, b"video").await.unwrap();

        let metadata =
            metadata_from_download_dir(&dir, "https://www.instagram.com/reel/abc/", "x".into(), 7)
                .await
                .unwrap();

        assert_eq!(metadata.title.as_deref(), Some("Clip title"));
        assert_eq!(metadata.uploader.as_deref(), Some("creator"));
        assert_eq!(metadata.duration, Some(12.5));
        assert_eq!(metadata.extension.as_deref(), Some("mp4"));
        assert_eq!(metadata.media_path, media_path);
        assert_eq!(metadata.media_bytes, 5);

        fs::remove_dir_all(dir).await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn download_url_uses_fake_yt_dlp_successfully() {
        let root = temp_dir("fake-success");
        fs::create_dir_all(&root).await.unwrap();
        let command = fake_yt_dlp(
            &root,
            r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo fake-yt-dlp
  exit 0
fi
dir=""
template=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --paths) shift; dir="${1#home:}" ;;
    -o) shift; template="$1" ;;
  esac
  shift
done
base="${template%%.*}"
mkdir -p "$dir"
printf 'video' > "$dir/$base.mp4"
cat > "$dir/$base.info.json" <<JSON
{"webpage_url":"https://www.youtube.com/shorts/abc","extractor":"Fake","title":"Fake title","ext":"mp4"}
JSON
"#,
        )
        .await;
        let mut config = test_config();
        config.yt_dlp_command = command.to_string_lossy().into_owned();
        config.downloads_dir = root.join("downloads");
        let id = Uuid::parse_str("10af7128-4b98-4e19-a494-17a3d5597e2c").unwrap();

        let report = download_url(
            &config,
            id,
            "https://www.youtube.com/shorts/abc",
            None,
            None,
            cancel_flag(),
        )
        .await
        .unwrap();
        let metadata = report.metadata;

        assert_eq!(report.attempts, 1);
        assert!(report.attempt_errors.is_empty());
        assert_eq!(metadata.yt_dlp_version, "fake-yt-dlp");
        assert_eq!(
            metadata.media_path,
            config
                .downloads_dir
                .join(id.to_string())
                .join(format!("{id}.mp4"))
        );
        assert_eq!(
            metadata.info_json_path,
            config
                .downloads_dir
                .join(id.to_string())
                .join(format!("{id}.info.json"))
        );
        assert_eq!(metadata.media_bytes, 5);

        fs::remove_dir_all(root).await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn download_url_runs_post_processing_commands_before_returning_metadata() {
        let root = temp_dir("fake-post-processing");
        fs::create_dir_all(&root).await.unwrap();
        let command = fake_yt_dlp(
            &root,
            r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo fake-yt-dlp
  exit 0
fi
dir=""
template=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --paths) shift; dir="${1#home:}" ;;
    -o) shift; template="$1" ;;
  esac
  shift
done
base="${template%%.*}"
mkdir -p "$dir"
printf 'video' > "$dir/$base.mp4"
cat > "$dir/$base.info.json" <<JSON
{"webpage_url":"https://www.youtube.com/shorts/abc","extractor":"Fake","title":"Fake title","ext":"mp4"}
JSON
"#,
        )
        .await;
        let postprocess = fake_executable(
            &root,
            "fake-postprocess",
            r#"#!/bin/sh
printf processed >> "$1"
echo postprocessed
"#,
        )
        .await;
        let mut config = test_config();
        config.yt_dlp_command = command.to_string_lossy().into_owned();
        config.downloads_dir = root.join("downloads");
        config.post_processing.enabled = true;
        config
            .post_processing
            .commands
            .push(crate::config::PostProcessingCommand {
                program: postprocess.to_string_lossy().into_owned(),
                args: vec!["{media_path}".to_string()],
                timeout_seconds: 5,
            });
        let id = Uuid::new_v4();

        let report = download_url(
            &config,
            id,
            "https://www.youtube.com/shorts/abc",
            None,
            None,
            cancel_flag(),
        )
        .await
        .unwrap();
        let metadata = report.metadata;

        assert_eq!(metadata.media_bytes, "videoprocessed".len() as u64);
        assert_eq!(metadata.post_processing.len(), 1);
        assert!(metadata.post_processing[0].success);
        assert_eq!(
            metadata.post_processing[0].stdout_tail.as_deref(),
            Some("postprocessed")
        );

        fs::remove_dir_all(root).await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn download_url_retries_failed_attempt_with_backoff() {
        let root = temp_dir("fake-retry");
        fs::create_dir_all(&root).await.unwrap();
        let counter = root.join("attempts");
        let command = fake_yt_dlp(
            &root,
            &format!(
                r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo fake-yt-dlp
  exit 0
fi
dir=""
template=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --paths) shift; dir="${{1#home:}}" ;;
    -o) shift; template="$1" ;;
  esac
  shift
done
base="${{template%%.*}}"
attempts_file="{}"
attempts=0
if [ -f "$attempts_file" ]; then
  attempts="$(cat "$attempts_file")"
fi
attempts=$((attempts + 1))
printf '%s' "$attempts" > "$attempts_file"
mkdir -p "$dir"
if [ "$attempts" -eq 1 ]; then
  printf partial > "$dir/partial.part"
  echo transient failure >&2
  exit 7
fi
printf 'video' > "$dir/$base.mp4"
cat > "$dir/$base.info.json" <<JSON
{{"webpage_url":"https://www.youtube.com/shorts/abc","extractor":"Fake","title":"Fake title","ext":"mp4"}}
JSON
"#,
                counter.display()
            ),
        )
        .await;
        let mut config = test_config();
        config.yt_dlp_command = command.to_string_lossy().into_owned();
        config.downloads_dir = root.join("downloads");
        config.download_max_attempts = 2;
        config.download_initial_backoff_ms = 0;
        let id = Uuid::new_v4();

        let report = download_url(
            &config,
            id,
            "https://www.youtube.com/shorts/abc",
            None,
            None,
            cancel_flag(),
        )
        .await
        .unwrap();
        let metadata = report.metadata;

        assert_eq!(report.attempts, 2);
        assert_eq!(report.attempt_errors.len(), 1);
        assert_eq!(report.attempt_errors[0].attempt, 1);
        assert_eq!(report.attempt_errors[0].retry_backoff_ms, Some(0));
        assert_eq!(fs::read_to_string(counter).await.unwrap(), "2");
        assert_eq!(metadata.media_bytes, 5);
        assert!(!metadata.media_path.with_file_name("partial.part").exists());

        fs::remove_dir_all(root).await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn download_url_cleans_partial_directory_on_failure() {
        let root = temp_dir("fake-failure");
        fs::create_dir_all(&root).await.unwrap();
        let command = fake_yt_dlp(
            &root,
            r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo fake-yt-dlp
  exit 0
fi
dir=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--paths" ]; then
    shift
    dir="${1#home:}"
  fi
  shift
done
mkdir -p "$dir"
printf partial > "$dir/partial.part"
echo failed >&2
exit 7
"#,
        )
        .await;
        let mut config = test_config();
        config.yt_dlp_command = command.to_string_lossy().into_owned();
        config.downloads_dir = root.join("downloads");
        let id = Uuid::new_v4();

        let err = download_url(
            &config,
            id,
            "https://www.youtube.com/shorts/abc",
            None,
            None,
            cancel_flag(),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("yt-dlp failed"));
        assert!(!config.downloads_dir.join(id.to_string()).exists());

        fs::remove_dir_all(root).await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn download_url_cleans_partial_directory_on_timeout() {
        let root = temp_dir("fake-timeout");
        fs::create_dir_all(&root).await.unwrap();
        let command = fake_yt_dlp(
            &root,
            r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo fake-yt-dlp
  exit 0
fi
dir=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--paths" ]; then
    shift
    dir="${1#home:}"
  fi
  shift
done
mkdir -p "$dir"
printf partial > "$dir/partial.part"
sleep 5
"#,
        )
        .await;
        let mut config = test_config();
        config.yt_dlp_command = command.to_string_lossy().into_owned();
        config.downloads_dir = root.join("downloads");
        config.job_timeout_seconds = 1;
        let id = Uuid::new_v4();

        let err = download_url(
            &config,
            id,
            "https://www.youtube.com/shorts/abc",
            None,
            None,
            cancel_flag(),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("timed out"));
        assert!(!config.downloads_dir.join(id.to_string()).exists());

        fs::remove_dir_all(root).await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn download_url_kills_child_process_on_cancel() {
        let root = temp_dir("fake-cancel");
        fs::create_dir_all(&root).await.unwrap();
        let command = fake_yt_dlp(
            &root,
            r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo fake-yt-dlp
  exit 0
fi
dir=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--paths" ]; then
    shift
    dir="${1#home:}"
  fi
  shift
done
mkdir -p "$dir"
printf partial > "$dir/partial.part"
while :; do :; done
"#,
        )
        .await;
        let mut config = test_config();
        config.yt_dlp_command = command.to_string_lossy().into_owned();
        config.downloads_dir = root.join("downloads");
        config.job_timeout_seconds = 30;
        let id = Uuid::new_v4();
        let cancel_flag = cancel_flag();
        let cancel_task_flag = Arc::clone(&cancel_flag);

        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(250)).await;
            cancel_task_flag.store(true, Ordering::Relaxed);
        });
        let err = download_url(
            &config,
            id,
            "https://www.youtube.com/shorts/abc",
            None,
            None,
            cancel_flag,
        )
        .await
        .unwrap_err();
        cancel_task.await.unwrap();

        assert!(err.to_string().contains("job canceled"));

        fs::remove_dir_all(root).await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn download_url_rejects_when_min_free_disk_space_is_not_met() {
        let root = temp_dir("disk-space");
        fs::create_dir_all(&root).await.unwrap();
        let command = fake_yt_dlp(
            &root,
            r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo fake-yt-dlp
  exit 0
fi
exit 0
"#,
        )
        .await;
        let mut config = test_config();
        config.yt_dlp_command = command.to_string_lossy().into_owned();
        config.downloads_dir = root.join("downloads");
        config.min_free_disk_bytes = u64::MAX;
        let id = Uuid::new_v4();

        let err = download_url(
            &config,
            id,
            "https://www.youtube.com/shorts/abc",
            None,
            None,
            cancel_flag(),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("insufficient free disk space"));
        assert!(!config.downloads_dir.join(id.to_string()).exists());

        fs::remove_dir_all(root).await.unwrap();
    }

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
            queue_size: 128,
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
            webhooks_dead_letter_jsonl: PathBuf::from("data/metadata/webhooks_dead_letter.jsonl"),
            allow_private_webhook_urls: false,
        }
    }

    fn cancel_flag() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("yt-dlp-server-{nanos}-{name}"))
    }

    #[cfg(unix)]
    async fn fake_yt_dlp(root: &Path, script: &str) -> PathBuf {
        fake_executable(root, "fake-yt-dlp", script).await
    }

    async fn fake_executable(root: &Path, name: &str, script: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let path = root.join(name);
        fs::write(&path, script).await.unwrap();
        let mut permissions = fs::metadata(&path).await.unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions).await.unwrap();
        path
    }
}
