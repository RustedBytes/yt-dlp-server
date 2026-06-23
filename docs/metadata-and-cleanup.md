# Metadata and Cleanup

Runtime metadata is stored as JSON Lines under `data/metadata/`:

- `download_submissions.jsonl`: accepted jobs when they are queued
- `download_results.jsonl`: terminal job states after success or failure
- `webhooks_dead_letter.jsonl`: webhook events that could not be delivered

On startup, the server reloads these JSONL files so completed job records remain available through `/v1/jobs/{id}` after a restart. Any job that was still `queued` or `running` when the previous process exited is recovered as `failed` and appended to `download_results.jsonl`.

On graceful shutdown, the HTTP server stops accepting requests, the in-process queue is closed, active download attempts are signaled for cancellation, and the process waits for download workers to exit. Any non-terminal job that does not reach a terminal state before process exit is still recovered as `failed` on the next startup.

Successful downloads are retained under `data/downloads/<job-id>/`. Each directory contains the downloaded media file named `<job-id>.<ext>` and the metadata file named `<job-id>.info.json`.

If post-processing is enabled, configured commands run after `yt-dlp` succeeds. Step results are persisted in `result.post_processing`. If object storage is enabled, the post-processed media file and `info.json` are uploaded after post-processing, and object metadata is persisted in `result.storage`.

Failed or timed-out downloads remove their partial `data/downloads/<job-id>/` directory on a best-effort basis.

When `download.download_max_attempts` is greater than `1`, failed `yt-dlp` attempts are retried with exponential backoff starting at `download.download_initial_backoff_ms`. Each retry starts from a clean `data/downloads/<job-id>/` directory, so partial files from previous attempts are not retained. Final job records persist `attempts` and `attempt_errors` for API reads and webhook payloads.

The in-memory job map is bounded by `retention.job_retention_limit`. Metadata compaction at startup is bounded by `retention.metadata_retention_limit`.

When `download.max_download_storage_bytes` is greater than `0`, the worker prunes the oldest succeeded download directories after jobs complete until retained media is under the configured byte limit. Pruned jobs are marked `deleted` and a tombstone record is appended to `download_results.jsonl`.

When `download.min_free_disk_bytes` is greater than `0`, each job checks available space in the download directory before running `yt-dlp` and fails before download if the threshold is not met.
