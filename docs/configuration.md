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
cookie_profiles.account_a = "account-a-cookies.txt"
format = ""
proxy = ""
enabled_platforms = ["tiktok", "instagram", "youtube", "facebook", "snapchat", "rutube", "douyin", "likee", "vk", "yappy"]
max_urls_per_request = 100
job_timeout_seconds = 1800
download_max_attempts = 3
download_initial_backoff_ms = 1000
max_download_storage_bytes = 0
min_free_disk_bytes = 0

[download.platforms.instagram]
cookies_path = "instagram-cookies.txt"
format = "mp4/best"
proxy = ""
job_timeout_seconds = 1800
download_max_attempts = 3
download_initial_backoff_ms = 1000
max_concurrent = 1

[post_processing]
enabled = false
fail_job_on_error = true

[[post_processing.commands]]
program = "ffmpeg"
args = ["-y", "-i", "{media_path}", "-map_metadata", "-1", "{job_dir}/{job_id}.processed.mp4"]
timeout_seconds = 600

[storage]
backend = "local"
endpoint_url = ""
bucket = ""
region = "us-east-1"
access_key_id = ""
secret_access_key = ""
session_token = ""
prefix = ""
force_path_style = true
public_base_url = ""

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
- `API_KEYS`: comma-separated accepted API keys; empty disables API key authentication. Clients may send `x-api-key`, Bearer auth, or Basic auth with the API key as the password.
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
- `DOWNLOAD_ENABLED_PLATFORMS`: comma-separated platform IDs accepted by URL validation; default is `tiktok,instagram,youtube,facebook,snapchat,rutube,douyin,likee,vk,yappy`; an empty value disables all platforms
- `MAX_URLS_PER_REQUEST`: maximum non-empty URLs accepted in one submission
- `JOB_TIMEOUT_SECONDS`: per-download timeout; set to `0` to disable timeout enforcement
- `DOWNLOAD_MAX_ATTEMPTS`: maximum yt-dlp attempts per job, including the first attempt; minimum effective value is `1`
- `DOWNLOAD_INITIAL_BACKOFF_MS`: initial retry backoff in milliseconds; later retries double this delay; set to `0` for immediate retries
- `MAX_DOWNLOAD_STORAGE_BYTES`: maximum retained downloaded media bytes; set to `0` to disable automatic cleanup
- `MIN_FREE_DISK_BYTES`: minimum free bytes required in the download directory before starting a job; set to `0` to disable the preflight check
- `POST_PROCESSING_ENABLED`: enable configured post-processing commands
- `POST_PROCESSING_FAIL_JOB_ON_ERROR`: fail the job when a post-processing command fails; defaults to `true`
- `OBJECT_STORAGE_BACKEND`: `local` or `s3`; `local` is the default
- `OBJECT_STORAGE_ENDPOINT_URL`: S3-compatible endpoint URL when `OBJECT_STORAGE_BACKEND=s3`
- `OBJECT_STORAGE_BUCKET`: S3/R2/MinIO bucket name
- `OBJECT_STORAGE_REGION`: SigV4 region, default `us-east-1`
- `OBJECT_STORAGE_ACCESS_KEY_ID`: S3-compatible access key
- `OBJECT_STORAGE_SECRET_ACCESS_KEY`: S3-compatible secret key
- `OBJECT_STORAGE_SESSION_TOKEN`: optional temporary credential token
- `OBJECT_STORAGE_PREFIX`: optional object key prefix
- `OBJECT_STORAGE_FORCE_PATH_STYLE`: defaults to `true`, useful for MinIO/R2-style endpoints
- `OBJECT_STORAGE_PUBLIC_BASE_URL`: optional CDN/public base URL used in stored object metadata
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

`download.cookie_profiles.<name>` entries define named server-side cookie files that API clients may select by name with `cookie_profile`. Profile names may contain ASCII letters, numbers, dashes, and underscores. The API never accepts raw cookie values.

Per-platform tables under `[download.platforms.<platform-id>]` override the global cookies, format, proxy, timeout, max attempts, and retry backoff for that platform. They may also set `max_concurrent` to cap how many downloads for that platform run at once across all workers. Omit a field to inherit `[download]`; omitting `max_concurrent` leaves that platform limited only by `download.workers`. API request `format` still has highest precedence for that job. API request `cookie_profile` has highest precedence for cookies.

Cookie files are server-side configuration only. API requests cannot submit cookies or credentials.

Post-processing commands run after a successful `yt-dlp` download and before object-storage upload. Commands are executed directly with argument arrays, not through shell interpolation. Supported placeholders are `{job_id}`, `{job_dir}`, `{media_path}`, and `{info_json_path}`. Step results are stored in each job's `result.post_processing`.

When `[storage] backend = "s3"`, the server uploads the completed media file and `info.json` to an S3-compatible service after post-processing. Local files remain the staging and serving copy; object keys, byte sizes, SHA-256 hashes, and optional public URLs are stored in `result.storage`. Secret storage values are not returned by `/v1/config`.
