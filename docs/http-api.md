# HTTP API

Health:

```bash
curl http://127.0.0.1:3000/health
```

Readiness:

```bash
curl http://127.0.0.1:3000/ready
```

`/health` is a liveness endpoint. `/ready` returns `200` when at least one download worker is ready.

When `server.api_keys` is configured, all endpoints except `/health` and `/ready` require an API key:

```bash
curl -H 'x-api-key: replace-with-a-long-random-secret' http://127.0.0.1:3000/metrics
```

`Authorization: Bearer <key>` is also accepted.

Metrics snapshot:

```bash
curl http://127.0.0.1:3000/metrics
```

The metrics response includes HTTP request counts and latency, queue depth, download job counts and latency, webhook failures, cleanup failures, retained jobs, and Linux process RSS memory when available.

OpenAPI document:

```bash
curl http://127.0.0.1:3000/openapi.json
```

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
    "webhook_url": "https://example.com/download-webhook"
  }'
```

The response contains one queued job per accepted URL:

```json
{
  "jobs": [
    {
      "id": "f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5",
      "status": "queued",
      "status_url": "/v1/jobs/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5"
    }
  ]
}
```

`webhook_url` is optional. When set, it must use `http` or `https`, must not include credentials or fragments, and rejects local/private literal IP addresses by default. After each job reaches `succeeded` or `failed`, the server sends a `POST` request to that URL with a webhook event envelope:

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
      "info_json_path": "data/downloads/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5/f0eb3aca-77c4-49a6-b7e5-7e44d0325bd5.info.json"
    },
    "error": null
  }
}
```

Webhook requests include:

- `x-download-event-id`: stable for all attempts for the same event
- `x-download-event-type`: currently `job.completed`
- `x-download-delivery-attempt`: one-based attempt number
- `x-download-signature`: `sha256=<hex-hmac>` when `webhooks.webhook_signing_secret` is configured

Webhook receivers should treat `x-download-event-id` as the idempotency key and ignore duplicate events that were already processed. Failed callbacks are retried according to `webhooks.webhook_max_attempts`; final failures are appended to `data/metadata/webhooks_dead_letter.jsonl`. Webhook delivery does not change the job result.

Check job:

```bash
curl http://127.0.0.1:3000/v1/jobs/<job-id>
```

Errors use a stable JSON shape:

```json
{
  "code": "bad_request",
  "message": "at least one URL is required"
}
```

Known error codes are `bad_request`, `not_found`, `unauthorized`, `rate_limited`, `service_unavailable`, and `internal_error`.

Requests return `408` if they exceed the configured `queue.request_timeout_seconds` limit, `401` if API key authentication is enabled and the key is missing or invalid, and `429` if rate limiting is enabled and the process-wide limit is exceeded. Download submissions return `503` when no worker is ready, when the queue is full, or when the queue has closed.
