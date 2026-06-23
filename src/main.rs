#![recursion_limit = "256"]

mod api;
mod config;
mod downloader;
mod jobs;
mod platforms;
mod state;
mod storage;
mod templates;
mod types;
mod util;

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, anyhow};
use clap::Parser;
use log::{debug, info};
use state::{AppMetrics, AppState, CancellationRegistry, RateLimiter, WorkerPoolState};
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;

use crate::{
    config::Config,
    jobs::{
        JobRequest, WebhookClient, WorkerPoolDependencies, WorkerRuntime, load_jobs, start_workers,
    },
};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
    /// TOML config file path. Overrides CONFIG_PATH when set.
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,
}

struct ServerRuntime {
    state: AppState,
    worker_runtime: WorkerRuntime,
    cancellations: Arc<CancellationRegistry>,
    queue_shutdown: async_channel::Sender<JobRequest>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = Arc::new(Config::load(cli.config)?);

    init_tracing(&config.rust_log)?;
    config.ensure_dirs().await?;
    log_loaded_config(&config);

    let runtime = build_runtime(Arc::clone(&config)).await?;
    let app = api::router(runtime.state.clone());
    let listener = tokio::net::TcpListener::bind(config.addr)
        .await
        .context("failed to bind TCP listener")?;
    log_server_listening(&config);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    stop_workers(runtime).await;

    Ok(())
}

async fn build_runtime(config: Arc<Config>) -> anyhow::Result<ServerRuntime> {
    let (queue_tx, queue_rx) = async_channel::bounded(config.queue_size);
    let jobs = load_jobs(&config).await?;
    let workers = Arc::new(WorkerPoolState::new(config.workers));
    let metrics = Arc::new(AppMetrics::default());
    let webhooks = Arc::new(WebhookClient::from_config(&config)?);
    let cancellations = Arc::new(CancellationRegistry::default());
    let state = AppState {
        config: Arc::clone(&config),
        queue_tx,
        jobs: Arc::new(RwLock::new(jobs)),
        workers: Arc::clone(&workers),
        metrics: Arc::clone(&metrics),
        rate_limiter: Arc::new(RateLimiter::new(config.rate_limit_requests_per_minute)),
        cancellations: Arc::clone(&cancellations),
    };

    let queue_shutdown = state.queue_tx.clone();
    let worker_runtime = start_workers(
        WorkerPoolDependencies {
            config,
            jobs: Arc::clone(&state.jobs),
            workers,
            metrics,
            webhooks,
            cancellations: Arc::clone(&cancellations),
        },
        queue_rx,
    );

    Ok(ServerRuntime {
        state,
        worker_runtime,
        cancellations,
        queue_shutdown,
    })
}

fn log_loaded_config(config: &Config) {
    debug!(
        "config loaded addr={} data_dir={} downloads_dir={} metadata_dir={} workers={} queue_size={} body_limit_bytes={} request_timeout_seconds={} api_key_auth_enabled={} rate_limit_requests_per_minute={} yt_dlp_command={} cookies_configured={} format_configured={} proxy_configured={} enabled_platforms={} max_urls_per_request={} job_timeout_seconds={} download_max_attempts={} download_initial_backoff_ms={} max_download_storage_bytes={} webhook_timeout_seconds={} webhook_connect_timeout_seconds={} webhook_max_attempts={} webhook_initial_backoff_ms={} webhook_signing_enabled={} allow_private_webhook_urls={} rust_log={}",
        config.addr,
        config.data_dir.display(),
        config.downloads_dir.display(),
        config.metadata_dir.display(),
        config.workers,
        config.queue_size,
        config.body_limit_bytes,
        config.request_timeout_seconds,
        !config.api_keys.is_empty(),
        config.rate_limit_requests_per_minute,
        config.yt_dlp_command,
        config.cookies_path.is_some(),
        config.format.is_some(),
        config.proxy.is_some(),
        config.download_enabled_platforms.join(","),
        config.max_urls_per_request,
        config.job_timeout_seconds,
        config.download_max_attempts,
        config.download_initial_backoff_ms,
        config.max_download_storage_bytes,
        config.webhook_timeout_seconds,
        config.webhook_connect_timeout_seconds,
        config.webhook_max_attempts,
        config.webhook_initial_backoff_ms,
        config.webhook_signing_secret.is_some(),
        config.allow_private_webhook_urls,
        config.rust_log
    );
}

fn log_server_listening(config: &Config) {
    info!(
        "server listening addr={} workers={} data_dir={} downloads_dir={} queue_size={} body_limit_bytes={} request_timeout_seconds={} api_key_auth_enabled={} rate_limit_requests_per_minute={} yt_dlp_command={} cookies_configured={} format_configured={} proxy_configured={} enabled_platforms={} max_urls_per_request={} job_timeout_seconds={} download_max_attempts={} download_initial_backoff_ms={} max_download_storage_bytes={} webhook_timeout_seconds={} webhook_connect_timeout_seconds={} webhook_max_attempts={} webhook_initial_backoff_ms={} webhook_signing_enabled={} allow_private_webhook_urls={}",
        config.addr,
        config.workers,
        config.data_dir.display(),
        config.downloads_dir.display(),
        config.queue_size,
        config.body_limit_bytes,
        config.request_timeout_seconds,
        !config.api_keys.is_empty(),
        config.rate_limit_requests_per_minute,
        config.yt_dlp_command,
        config.cookies_path.is_some(),
        config.format.is_some(),
        config.proxy.is_some(),
        config.download_enabled_platforms.join(","),
        config.max_urls_per_request,
        config.job_timeout_seconds,
        config.download_max_attempts,
        config.download_initial_backoff_ms,
        config.max_download_storage_bytes,
        config.webhook_timeout_seconds,
        config.webhook_connect_timeout_seconds,
        config.webhook_max_attempts,
        config.webhook_initial_backoff_ms,
        config.webhook_signing_secret.is_some(),
        config.allow_private_webhook_urls
    );
}

async fn stop_workers(runtime: ServerRuntime) {
    info!("server shutdown requested; stopping download workers");
    runtime.worker_runtime.request_shutdown();
    runtime.cancellations.cancel_all();
    runtime.queue_shutdown.close();
    runtime.worker_runtime.wait().await;
    info!("download workers stopped");
}

async fn shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        log::error!("failed to install ctrl-c handler error={}", err);
    }
}

fn init_tracing(rust_log: &str) -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new(rust_log))?;
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .compact()
        .try_init()
        .map_err(|err| anyhow!("failed to initialize tracing subscriber: {err}"))?;
    Ok(())
}
