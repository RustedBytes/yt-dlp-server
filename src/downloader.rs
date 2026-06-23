use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use log::{debug, warn};
use serde_json::Value;
use tokio::{fs, process::Command};
use uuid::Uuid;

use crate::{config::Config, types::DownloadMetadata};

pub async fn check_downloader(config: &Config) -> anyhow::Result<String> {
    yt_dlp_version(config).await
}

pub async fn download_url(
    config: &Config,
    id: uuid::Uuid,
    url: &str,
) -> anyhow::Result<DownloadMetadata> {
    let started = Instant::now();
    let job_dir = config.downloads_dir.join(id.to_string());
    prepare_download_dir(&job_dir).await?;
    if let Err(err) = ensure_min_free_disk_space(config).await {
        cleanup_partial_download(&job_dir).await;
        return Err(err);
    }

    let version = yt_dlp_version(config).await.unwrap_or_else(|err| {
        warn!(
            "failed to read yt-dlp version before download job_id={} error={}",
            id, err
        );
        "unknown".to_string()
    });

    let attempts = config.download_max_attempts.max(1);
    let mut last_error = None;
    for attempt in 1..=attempts {
        prepare_download_dir(&job_dir).await?;
        let outcome = match run_yt_dlp(config, id, &job_dir, url).await {
            Ok(()) => {
                metadata_from_download_dir(
                    &job_dir,
                    url,
                    version.clone(),
                    started.elapsed().as_millis(),
                )
                .await
            }
            Err(err) => Err(err),
        };

        match outcome {
            Ok(metadata) => return Ok(metadata),
            Err(err) => {
                cleanup_partial_download(&job_dir).await;
                let error = err.to_string();
                last_error = Some(err);
                if attempt == attempts {
                    break;
                }
                let delay = retry_backoff(config.download_initial_backoff_ms, attempt);
                warn!(
                    "download attempt failed job_id={} attempt={} max_attempts={} retry_backoff_ms={} error={}",
                    id,
                    attempt,
                    attempts,
                    delay.as_millis(),
                    error
                );
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("download failed without running an attempt")))
}

async fn prepare_download_dir(job_dir: &Path) -> anyhow::Result<()> {
    if job_dir.exists() {
        fs::remove_dir_all(job_dir).await.with_context(|| {
            format!(
                "failed to clear existing download dir {}",
                job_dir.display()
            )
        })?;
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

pub fn download_command_args(
    config: &Config,
    id: Uuid,
    job_dir: &Path,
    url: &str,
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
    if let Some(cookies_path) = &config.cookies_path {
        args.push(OsString::from("--cookies"));
        args.push(cookies_path.as_os_str().to_os_string());
    }
    if let Some(format) = &config.format {
        args.push(OsString::from("--format"));
        args.push(OsString::from(format));
    }
    if let Some(proxy) = &config.proxy {
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

async fn run_yt_dlp(config: &Config, id: Uuid, job_dir: &Path, url: &str) -> anyhow::Result<()> {
    let output = run_command_with_timeout(
        &config.yt_dlp_command,
        download_command_args(config, id, job_dir, url),
        config.job_timeout_seconds,
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

async fn yt_dlp_version(config: &Config) -> anyhow::Result<String> {
    let output =
        run_command_with_timeout(&config.yt_dlp_command, version_command_args(config), 30).await?;
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
) -> anyhow::Result<std::process::Output> {
    let child = Command::new(command)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to spawn `{command}`"))?;

    if timeout_seconds == 0 {
        return child
            .wait_with_output()
            .await
            .with_context(|| format!("failed to wait for `{command}`"));
    }

    match tokio::time::timeout(
        Duration::from_secs(timeout_seconds),
        child.wait_with_output(),
    )
    .await
    {
        Ok(output) => output.with_context(|| format!("failed to wait for `{command}`")),
        Err(_) => Err(anyhow!("job timed out after {timeout_seconds} seconds")),
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
        )
        .into_iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

        assert_eq!(args.first().unwrap(), "--no-config");
        assert!(args.contains(&format!("{id}.%(ext)s")));
        assert!(!args.contains(&"--cookies".to_string()));
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

        let metadata = download_url(&config, id, "https://www.youtube.com/shorts/abc")
            .await
            .unwrap();

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

        let metadata = download_url(&config, id, "https://www.youtube.com/shorts/abc")
            .await
            .unwrap();

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

        let err = download_url(&config, id, "https://www.youtube.com/shorts/abc")
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

        let err = download_url(&config, id, "https://www.youtube.com/shorts/abc")
            .await
            .unwrap_err();

        assert!(err.to_string().contains("timed out"));
        assert!(!config.downloads_dir.join(id.to_string()).exists());

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

        let err = download_url(&config, id, "https://www.youtube.com/shorts/abc")
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
            format: None,
            proxy: None,
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

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("yt-dlp-server-{nanos}-{name}"))
    }

    #[cfg(unix)]
    async fn fake_yt_dlp(root: &Path, script: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let path = root.join("fake-yt-dlp");
        fs::write(&path, script).await.unwrap();
        let mut permissions = fs::metadata(&path).await.unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions).await.unwrap();
        path
    }
}
