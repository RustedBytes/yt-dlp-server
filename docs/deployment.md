# Deployment

## Production Config

Start from `config.example.toml` and set at least:

```toml
[server]
bind_addr = "0.0.0.0:3000"
data_dir = "/var/lib/yt-dlp-server"
api_keys = ["replace-with-a-long-random-secret"]
rate_limit_requests_per_minute = 120

[queue]
queue_size = 256
request_timeout_seconds = 30

[download]
workers = 2
output_dir = "/var/lib/yt-dlp-server/downloads"
cookies_path = "/etc/yt-dlp-server/cookies.txt"
format = "bv*+ba/b"
job_timeout_seconds = 1800
download_max_attempts = 3
download_initial_backoff_ms = 1000
max_download_storage_bytes = 107374182400
min_free_disk_bytes = 1073741824

[webhooks]
webhook_max_attempts = 3
webhook_initial_backoff_ms = 500
webhook_signing_secret = "replace-with-a-long-random-secret"
```

Keep cookie files and API keys outside the repository. Mount cookie files read-only in containers.

## systemd

Example unit:

```ini
[Unit]
Description=yt-dlp-server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=yt-dlp-server
Group=yt-dlp-server
WorkingDirectory=/opt/yt-dlp-server
Environment=CONFIG_PATH=/etc/yt-dlp-server/config.toml
ExecStart=/usr/local/bin/yt-dlp-server
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=/var/lib/yt-dlp-server

[Install]
WantedBy=multi-user.target
```

## Compose With Cookies

```yaml
services:
  downloader:
    image: yt-dlp-server:local
    ports:
      - "3000:3000"
    environment:
      BIND_ADDR: 0.0.0.0:3000
      DATA_DIR: /app/data
      DOWNLOAD_OUTPUT_DIR: /app/data/downloads
      YT_DLP_COOKIES_PATH: /app/secrets/cookies.txt
    volumes:
      - downloader-data:/app/data
      - ./cookies.txt:/app/secrets/cookies.txt:ro

volumes:
  downloader-data:
```
