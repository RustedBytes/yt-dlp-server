# HTTP API

Health:

```bash
curl http://127.0.0.1:3000/health
```

Readiness:

```bash
curl http://127.0.0.1:3000/ready
```

`/health` is a liveness endpoint. `/ready` returns `200` when at least one download worker is ready and the configured `yt-dlp` command passes a version check. When the downloader runtime is broken, the readiness response includes `downloader_ready: false` and `downloader_error`.

When `server.api_keys` is configured, all endpoints except `/health` and `/ready` require an API key:

```bash
curl -H 'x-api-key: replace-with-a-long-random-secret' http://127.0.0.1:3000/metrics
```

`Authorization: Bearer <key>` is also accepted. For browser use, HTTP Basic auth is accepted with any username and the API key as the password:

```bash
curl -u 'browser:replace-with-a-long-random-secret' http://127.0.0.1:3000/
```

Metrics snapshot:

```bash
curl http://127.0.0.1:3000/metrics
```

The metrics response includes HTTP request counts and latency, queue depth, download job counts and latency, webhook failures, cleanup failures, retained jobs, and Linux process RSS memory when available.

Prometheus text metrics:

```bash
curl http://127.0.0.1:3000/metrics.prometheus
```

OpenAPI document:

```bash
curl http://127.0.0.1:3000/openapi.json
```

Read the effective runtime configuration without secret values:

```bash
curl http://127.0.0.1:3000/v1/config
```

The response includes operational limits and feature flags, including per-platform policy flags and `max_concurrent` limits. API keys, cookie file paths, proxy values, and webhook signing secrets are not returned; fields such as `api_key_auth_enabled`, `cookies_configured`, `proxy_configured`, and `signing_enabled` report whether those settings are configured.

Check configured cookie profile names and their retained-job health without exposing cookie file paths:

```bash
curl http://127.0.0.1:3000/v1/cookie-profiles
```

Each profile includes its most recent retained job, most recent success, and most recent failure when available. Empty profiles remain listed with null status fields.

Discover supported short-form platforms and which ones are enabled in this deployment:

```bash
curl http://127.0.0.1:3000/v1/platforms
```

Each platform entry includes its stable platform ID, whether URL submissions are currently accepted for it, accepted base hosts, and example short-form path shapes.

Check queue capacity before submitting a large URL list:

```bash
curl http://127.0.0.1:3000/v1/queue
```

The response includes current queue depth, configured queue capacity, available slots, max URLs per request, and worker readiness.

Inspect active workers and the jobs they are currently processing:

```bash
curl http://127.0.0.1:3000/v1/workers
```

The response includes worker readiness plus an `active` array with `worker_id`, `job_id`, `status_url`, URL, start time, and elapsed milliseconds for each worker currently running a download.

Preview storage cleanup without deleting artifacts:

```bash
curl 'http://127.0.0.1:3000/v1/storage/cleanup?max_bytes=1073741824'
```

Run cleanup using the same oldest-successful-job policy:

```bash
curl -s -X POST 'http://127.0.0.1:3000/v1/storage/cleanup?max_bytes=1073741824'
```

When `max_bytes` is omitted, cleanup uses `download.max_download_storage_bytes`. If that config value is `0`, preview reports current retained bytes and no deletion candidates. Cleanup marks deleted jobs as `deleted` and removes their artifact directories.

Validate a pasted URL list without enqueueing jobs:

```bash
curl -s -X POST http://127.0.0.1:3000/v1/downloads/validate \
  -H 'content-type: application/json' \
  -d '{
    "urls": [
      "https://www.tiktok.com/@user/video/123",
      "https://example.com/not-supported"
    ],
    "format": "mp4/best",
    "cookie_profile": "account_a"
  }'
```

The validation response includes normalized unique URLs, accepted webhook, format, and cookie profile values when valid, and a list of all detected errors. It never creates jobs.

Queue downloads:

```bash
curl -s -X POST http://127.0.0.1:3000/v1/downloads \
  -H 'content-type: application/json' \
  -d '{
    "urls": [
      "https://www.tiktok.com/@user/video/123",
      "https://www.instagram.com/reel/ABC/",
      "https://www.youtube.com/shorts/XYZ"
    ],
    "webhook_url": "https://example.com/download-webhook",
    "format": "mp4/best",
    "cookie_profile": "account_a",
    "force": false
  }'
```

The response contains one job reference per accepted URL. By default, if a normalized URL, explicit `format`, and `cookie_profile` already have a successful retained job, the existing job is returned instead of queueing another download:

```json
{
  "jobs": [
    {
      "id": "f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5",
      "status": "queued",
      "status_url": "/v1/jobs/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5",
      "existing": false
    }
  ]
}
```

`format` is optional and uses yt-dlp's format selector syntax. When provided, it applies to every URL in the request and overrides the server's `download.format` setting for those jobs.

`cookie_profile` is optional and names a configured server-side cookie profile. The API accepts only the configured profile name, never raw cookies or cookie file paths.

Set `force` to `true` to queue a fresh download even when a successful retained job already exists. Existing successful job hits do not consume queue capacity and do not send a new webhook event.

`webhook_url` is optional. When set, it must use `http` or `https`, must not include credentials or fragments, and rejects local/private literal IP addresses by default. After each job reaches a terminal state such as `succeeded`, `failed`, or `canceled`, the server sends a `POST` request to that URL with a webhook event envelope:

```json
{
  "event_id": "8d0381b0-5df5-41ee-8f08-b0cd150e9b38",
  "event_type": "job.completed",
  "created_at": "2026-06-23T12:00:00Z",
  "job": {
    "id": "f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5",
    "status": "succeeded",
    "url": "https://www.instagram.com/reel/ABC/",
    "result": {
      "media_path": "data/downloads/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5.mp4",
      "media_bytes": 1234567,
      "info_json_path": "data/downloads/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5.info.json",
      "post_processing": [
        {
          "command_index": 0,
          "program": "ffmpeg",
          "success": true,
          "status_code": 0,
          "elapsed_ms": 512
        }
      ],
      "storage": {
        "backend": "s3",
        "bucket": "downloads",
        "media": {
          "key": "videos/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5.mp4",
          "url": "https://cdn.example.com/videos/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5.mp4",
          "bytes": 1234567,
          "sha256": "..."
        },
        "info_json": {
          "key": "videos/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5.info.json",
          "bytes": 2345,
          "sha256": "..."
        }
      }
    },
    "attempts": 2,
    "attempt_errors": [
      {
        "attempt": 1,
        "error": "yt-dlp failed with status exit status: 1",
        "elapsed_ms": 742,
        "retry_backoff_ms": 1000
      }
    ],
    "error": null
  },
  "artifact_urls": {
    "artifacts_url": "/v1/jobs/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5/artifacts",
    "media_url": "/v1/jobs/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5/media",
    "media_inline_url": "/v1/jobs/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5/media-inline",
    "info_json_url": "/v1/jobs/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5/info-json",
    "archive_url": "/v1/jobs/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5/archive"
  }
}
```

`artifact_urls` is present when the job has downloaded artifacts. Use these stable HTTP paths instead of reading filesystem paths from `job.result`.

Webhook requests include:

- `x-download-event-id`: stable for all attempts for the same event
- `x-download-event-type`: currently `job.completed`
- `x-download-delivery-attempt`: one-based attempt number
- `x-download-signature`: `sha256=<hex-hmac>` when `webhooks.webhook_signing_secret` is configured

Webhook receivers should treat `x-download-event-id` as the idempotency key and ignore duplicate events that were already processed. Failed callbacks are retried according to `webhooks.webhook_max_attempts`; final failures are appended to `data/metadata/webhooks_dead_letter.jsonl`. Webhook delivery does not change the job result.

List failed webhook deliveries:

```bash
curl 'http://127.0.0.1:3000/v1/webhooks/dead-letters?limit=50&offset=0'
```

Open failed webhook deliveries in the browser:

```bash
open http://127.0.0.1:3000/webhooks/dead-letters
```

Replay a failed webhook delivery by event id:

```bash
curl -s -X POST http://127.0.0.1:3000/v1/webhooks/dead-letters/<event-id>/replay
```

A successful replay reuses the original `x-download-event-id` and removes that event from `webhooks_dead_letter.jsonl`.

Dismiss a failed webhook delivery without replaying it:

```bash
curl -s -X DELETE http://127.0.0.1:3000/v1/webhooks/dead-letters/<event-id>
```

Redeliver a terminal job webhook as a new event:

```bash
curl -s -X POST http://127.0.0.1:3000/v1/jobs/<job-id>/webhook
```

Manual redelivery uses the job's stored `webhook_url` and the configured retry policy. Failed manual redeliveries return an error response and are not appended to `webhooks_dead_letter.jsonl`.

Open the browser job detail page:

```bash
open http://127.0.0.1:3000/jobs/<job-id>
```

Check job as JSON:

```bash
curl http://127.0.0.1:3000/v1/jobs/<job-id>
```

Job records include `attempts` and, when retries failed before the final result, `attempt_errors`. Failed and canceled jobs also include a human-readable `error` and, when classified, a stable `error_kind` such as `canceled`, `timeout`, `rate_limited`, `authentication_required`, `unsupported_url`, `downloader_failed`, or `download_failed`.

Check a queued job's estimated position:

```bash
curl http://127.0.0.1:3000/v1/jobs/<job-id>/queue-position
```

Positions are 1-based among jobs still marked `queued`, ordered by creation time. Running or terminal jobs return `409`.

Wait for a job to become terminal, or return the current record when the wait timeout expires:

```bash
curl 'http://127.0.0.1:3000/v1/jobs/<job-id>/wait?timeout_seconds=30'
```

`timeout_seconds` defaults to `30` and is capped at `55` so it remains below the request timeout in normal configurations.

List recent jobs:

```bash
curl 'http://127.0.0.1:3000/v1/jobs?status=succeeded&limit=50&offset=0'
curl 'http://127.0.0.1:3000/v1/jobs?platform=instagram&q=creator&limit=50'
```

Job listing supports `status`, `platform`, and `q` filters. The text query is case-insensitive and searches URL, job id, requested format, error text, title, uploader, extractor, and final webpage URL.

Export retained job history as JSONL or CSV:

```bash
curl -OJ 'http://127.0.0.1:3000/v1/jobs/export?format=jsonl'
curl -OJ 'http://127.0.0.1:3000/v1/jobs/export?format=csv&status=failed&platform=tiktok&q=rate'
```

Export uses the same `status`, `platform`, and `q` filters as job listing and includes all retained matches rather than a paginated page. CSV exports include a derived `platform` column.

Read current job counts by status:

```bash
curl http://127.0.0.1:3000/v1/jobs/summary
```

Open job history in the browser:

```bash
open 'http://127.0.0.1:3000/jobs?status=failed&limit=50&offset=0'
```

Discover stable artifact URLs for a completed job:

```bash
curl http://127.0.0.1:3000/v1/jobs/<job-id>/artifacts
```

The response contains media, inline media, info JSON, and archive URLs plus media byte size, content type, and extension. Use this endpoint instead of deriving download URLs from stored filesystem paths.

Artifact discovery also includes `media_sha256`, `info_json_sha256`, `archive_bytes`, and `archive_sha256` so clients can verify downloaded files and decide whether to fetch the combined tar.

Media, info JSON, and archive download responses also include a strong `ETag` header derived from the served bytes. Send `If-None-Match` with that value to receive `304 Not Modified` when the artifact has not changed.

Download completed media:

```bash
curl -OJ http://127.0.0.1:3000/v1/jobs/<job-id>/media
```

The media endpoint supports single HTTP byte ranges for seeking or resuming downloads:

```bash
curl -H 'Range: bytes=0-1048575' http://127.0.0.1:3000/v1/jobs/<job-id>/media -o part.mp4
```

Use `HEAD` to check artifact size, type, disposition, and range metadata without downloading the body:

```bash
curl -I http://127.0.0.1:3000/v1/jobs/<job-id>/media
curl -I http://127.0.0.1:3000/v1/jobs/<job-id>/info-json
```

Stream media inline for browser preview:

```bash
open http://127.0.0.1:3000/v1/jobs/<job-id>/media-inline
```

Download yt-dlp metadata only:

```bash
curl -OJ http://127.0.0.1:3000/v1/jobs/<job-id>/info-json
```

Download completed media and yt-dlp metadata together:

```bash
curl -OJ http://127.0.0.1:3000/v1/jobs/<job-id>/archive
```

Archive responses include `Content-Length` and `ETag`. Use `HEAD` to inspect the tar size, cache validator, and disposition without downloading the archive body:

```bash
curl -I http://127.0.0.1:3000/v1/jobs/<job-id>/archive
```

The archive endpoint also supports single HTTP byte ranges for resuming interrupted archive downloads:

```bash
curl -H 'Range: bytes=0-1048575' http://127.0.0.1:3000/v1/jobs/<job-id>/archive -o archive.part.tar
```

Cancel a queued or running job:

```bash
curl -s -X POST http://127.0.0.1:3000/v1/jobs/<job-id>/cancel
```

Retry a terminal failed, canceled, or succeeded job as a new queued job:

```bash
curl -s -X POST http://127.0.0.1:3000/v1/jobs/<job-id>/retry
```

Delete a terminal job's downloaded files and mark the job as deleted:

```bash
curl -s -X DELETE http://127.0.0.1:3000/v1/jobs/<job-id>
```

Run job actions in batches:

```bash
curl -s -X POST http://127.0.0.1:3000/v1/jobs/batch/cancel \
  -H 'content-type: application/json' \
  -d '{"ids":["<job-id-1>","<job-id-2>"]}'

curl -s -X POST http://127.0.0.1:3000/v1/jobs/batch/retry \
  -H 'content-type: application/json' \
  -d '{"ids":["<job-id-1>","<job-id-2>"]}'

curl -s -X POST http://127.0.0.1:3000/v1/jobs/batch/delete \
  -H 'content-type: application/json' \
  -d '{"ids":["<job-id-1>","<job-id-2>"]}'
```

Batch actions deduplicate repeated IDs in request order and return `succeeded` plus `failed` arrays. A conflict or missing job for one ID does not stop the rest of the batch.

Errors use a stable JSON shape:

```json
{
  "code": "bad_request",
  "message": "at least one URL is required"
}
```

Known error codes are `bad_request`, `not_found`, `conflict`, `unauthorized`, `rate_limited`, `service_unavailable`, and `internal_error`.

Requests return `408` if they exceed the configured `queue.request_timeout_seconds` limit, `401` if API key authentication is enabled and the key is missing or invalid, and `429` if rate limiting is enabled and the process-wide limit is exceeded. Download submissions return `503` when no worker is ready, when the queue is full, or when the queue has closed.
