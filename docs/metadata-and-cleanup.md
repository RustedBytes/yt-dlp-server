# Metadata and Cleanup

Runtime metadata is stored as JSON Lines under `data/metadata/`:

- `download_submissions.jsonl`: accepted jobs when they are queued
- `download_results.jsonl`: terminal job states after success or failure
- `webhooks_dead_letter.jsonl`: webhook events that could not be delivered

On startup, the server reloads these JSONL files so completed job records remain available through `/v1/jobs/{id}` after a restart. Any job that was still `queued` or `running` when the previous process exited is recovered as `failed` and appended to `download_results.jsonl`.

Successful downloads are retained under `data/downloads/<job-id>/`. Each directory contains the downloaded media file named `<job-id>.<ext>` and the metadata file named `<job-id>.info.json`.

Failed or timed-out downloads remove their partial `data/downloads/<job-id>/` directory on a best-effort basis.

The in-memory job map is bounded by `retention.job_retention_limit`. Metadata compaction at startup is bounded by `retention.metadata_retention_limit`.

When `download.max_download_storage_bytes` is greater than `0`, the worker prunes the oldest succeeded download directories after jobs complete until retained media is under the configured byte limit. Pruned jobs are marked `deleted` and a tombstone record is appended to `download_results.jsonl`.

When `download.min_free_disk_bytes` is greater than `0`, each job checks available space in the download directory before running `yt-dlp` and fails before download if the threshold is not met.
