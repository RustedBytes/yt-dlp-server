# Configuration

Copy the sample TOML config:

```bash
cp config.example.toml config.toml
```

The server reads `config.toml` by default. Use another file with:

```bash
cargo run -- --config /path/to/config.toml
```

`CONFIG_PATH=/path/to/config.toml cargo run` is also supported when no `--config` argument is provided.

Important settings:

```toml
[server]
bind_addr = "127.0.0.1:3000"
data_dir = "data"
cors_allowed_origins = []
api_keys = []
rate_limit_requests_per_minute = 0

[queue]
queue_size = 128
body_limit_bytes = 131072
request_timeout_seconds = 60

[download]
workers = 1
output_dir = "data/downloads"
yt_dlp_command = "uv"
cookies_path = ""
format = ""
proxy = ""
max_urls_per_request = 100
job_timeout_seconds = 1800
max_download_storage_bytes = 0
min_free_disk_bytes = 0

[webhooks]
webhook_timeout_seconds = 10
webhook_connect_timeout_seconds = 5
webhook_max_attempts = 1
webhook_initial_backoff_ms = 500
webhook_signing_secret = ""
allow_private_webhook_urls = false

[logging]
rust_log = "info"
```

Environment variables override TOML values when set:

- `BIND_ADDR`: bind address, default `127.0.0.1:3000`
- `DATA_DIR`: metadata directory base, default `data`
- `CORS_ALLOWED_ORIGINS`: comma-separated origins allowed by browser CORS checks
- `API_KEYS`: comma-separated accepted API keys; empty disables API key authentication
- `RATE_LIMIT_REQUESTS_PER_MINUTE`: per-key request limit when API keys are configured, or shared anonymous limit otherwise; set to `0` to disable rate limiting
- `QUEUE_SIZE`: queued job capacity
- `BODY_LIMIT_BYTES`: JSON/form body limit
- `REQUEST_TIMEOUT_SECONDS`: whole HTTP request timeout; set to `0` to disable timeout enforcement
- `DOWNLOAD_WORKERS`: number of concurrent download workers
- `DOWNLOAD_OUTPUT_DIR`: directory where per-job download folders are written
- `YT_DLP_COMMAND`: command to execute; `uv` expands to `uv run --frozen yt-dlp`
- `YT_DLP_COOKIES_PATH`: optional server-side cookie file passed as `--cookies`
- `YT_DLP_FORMAT`: optional yt-dlp format selector passed as `--format`
- `YT_DLP_PROXY`: optional proxy URL passed as `--proxy`
- `MAX_URLS_PER_REQUEST`: maximum non-empty URLs accepted in one submission
- `JOB_TIMEOUT_SECONDS`: per-download timeout; set to `0` to disable timeout enforcement
- `MAX_DOWNLOAD_STORAGE_BYTES`: maximum retained downloaded media bytes; set to `0` to disable automatic cleanup
- `MIN_FREE_DISK_BYTES`: minimum free bytes required in the download directory before starting a job; set to `0` to disable the preflight check
- `JOB_RETENTION_LIMIT`: maximum in-memory job records kept queryable through `/v1/jobs/{id}`
- `METADATA_RETENTION_LIMIT`: maximum latest metadata records kept when JSONL files are compacted at startup; set to `0` to disable compaction
- `WEBHOOK_TIMEOUT_SECONDS`: total outbound webhook request timeout; set to `0` to disable
- `WEBHOOK_CONNECT_TIMEOUT_SECONDS`: outbound webhook connection timeout; set to `0` to disable
- `WEBHOOK_MAX_ATTEMPTS`: maximum webhook delivery attempts, including the first attempt
- `WEBHOOK_INITIAL_BACKOFF_MS`: initial webhook retry backoff; later retries double this delay
- `WEBHOOK_SIGNING_SECRET`: HMAC-SHA256 signing secret for webhook bodies; empty disables signatures
- `ALLOW_PRIVATE_WEBHOOK_URLS`: set to `true` only in trusted deployments that must call local or private webhook targets
- `RUST_LOG`: logging level, for example `debug`
- `CONFIG_PATH`: explicit TOML config path when `--config` is not set

Cookie files are server-side configuration only. API requests cannot submit cookies or credentials.
