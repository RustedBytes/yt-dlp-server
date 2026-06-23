# Metadata and Cleanup

Runtime metadata is stored as JSON Lines under `data/metadata/`:

- `download_submissions.jsonl`: accepted jobs when they are queued
- `download_results.jsonl`: terminal job states after success or failure
- `webhooks_dead_letter.jsonl`: webhook events that could not be delivered

On startup, the server reloads these JSONL files so completed job records remain available through `/v1/jobs/{id}` after a restart. Any job that was still `queued` or `running` when the previous process exited is recovered as `failed` and appended to `download_results.jsonl`.

Successful downloads are retained under `data/downloads/<job-id>/`. Each directory contains the downloaded media file and the `*.info.json` file produced by `yt-dlp`.

Failed or timed-out downloads remove their partial `data/downloads/<job-id>/` directory on a best-effort basis.

The in-memory job map is bounded by `retention.job_retention_limit`. Metadata compaction at startup is bounded by `retention.metadata_retention_limit`.
