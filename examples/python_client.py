#!/usr/bin/env python3
"""Small urllib3 client for the social video download server."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse

try:
    import urllib3
    from urllib3.exceptions import HTTPError
except ImportError:  # pragma: no cover
    urllib3 = None
    HTTPError = Exception


class ClientError(RuntimeError):
    pass


class DownloadClient:
    def __init__(self, base_url: str, api_key: str | None = None) -> None:
        if urllib3 is None:
            raise ClientError("urllib3 is not installed; run `python3 -m pip install urllib3`")
        self.base_url = base_url.rstrip("/") + "/"
        self.http = urllib3.PoolManager()
        self.api_key = api_key

    def submit(
        self,
        urls: list[str],
        webhook_url: str | None = None,
        format_selector: str | None = None,
        cookie_profile: str | None = None,
        force: bool = False,
        wait: bool = False,
        wait_timeout: float = 30.0,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"urls": urls}
        if webhook_url:
            body["webhook_url"] = webhook_url
        if format_selector:
            body["format"] = format_selector
        if cookie_profile:
            body["cookie_profile"] = cookie_profile
        if force:
            body["force"] = True
        response = self._request_json("POST", "v1/downloads", json_body=body)
        if wait:
            response["jobs"] = [
                self.wait_for_job(job["id"], wait_timeout) for job in response["jobs"]
            ]
        return response

    def validate(
        self,
        urls: list[str],
        webhook_url: str | None = None,
        format_selector: str | None = None,
        cookie_profile: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"urls": urls}
        if webhook_url:
            body["webhook_url"] = webhook_url
        if format_selector:
            body["format"] = format_selector
        if cookie_profile:
            body["cookie_profile"] = cookie_profile
        return self._request_json("POST", "v1/downloads/validate", json_body=body)

    def platforms(self) -> dict[str, Any]:
        return self._request_json("GET", "v1/platforms")

    def config(self) -> dict[str, Any]:
        return self._request_json("GET", "v1/config")

    def cookie_profiles(self) -> dict[str, Any]:
        return self._request_json("GET", "v1/cookie-profiles")

    def queue(self) -> dict[str, Any]:
        return self._request_json("GET", "v1/queue")

    def workers(self) -> dict[str, Any]:
        return self._request_json("GET", "v1/workers")

    def storage_cleanup(self, max_bytes: int | None = None, execute: bool = False) -> dict[str, Any]:
        query = {}
        if max_bytes is not None:
            query["max_bytes"] = max_bytes
        path = "v1/storage/cleanup"
        if query:
            path = f"{path}?{urlencode(query)}"
        method = "POST" if execute else "GET"
        return self._request_json(method, path)

    def get_job(self, job_id: str) -> dict[str, Any]:
        return self._request_json("GET", f"v1/jobs/{job_id}")

    def summary(self) -> dict[str, Any]:
        return self._request_json("GET", "v1/jobs/summary")

    def queue_position(self, job_id: str) -> dict[str, Any]:
        return self._request_json("GET", f"v1/jobs/{job_id}/queue-position")

    def artifacts(self, job_id: str) -> dict[str, Any]:
        return self._request_json("GET", f"v1/jobs/{job_id}/artifacts")

    def cancel_job(self, job_id: str) -> dict[str, Any]:
        return self._request_json("POST", f"v1/jobs/{job_id}/cancel")

    def cancel_jobs(self, job_ids: list[str]) -> dict[str, Any]:
        return self._request_json("POST", "v1/jobs/batch/cancel", json_body={"ids": job_ids})

    def retry_job(self, job_id: str) -> dict[str, Any]:
        return self._request_json("POST", f"v1/jobs/{job_id}/retry")

    def retry_jobs(self, job_ids: list[str]) -> dict[str, Any]:
        return self._request_json("POST", "v1/jobs/batch/retry", json_body={"ids": job_ids})

    def redeliver_webhook(self, job_id: str) -> dict[str, Any]:
        return self._request_json("POST", f"v1/jobs/{job_id}/webhook")

    def delete_job(self, job_id: str) -> dict[str, Any]:
        return self._request_json("DELETE", f"v1/jobs/{job_id}")

    def delete_jobs(self, job_ids: list[str]) -> dict[str, Any]:
        return self._request_json("POST", "v1/jobs/batch/delete", json_body={"ids": job_ids})

    def export_jobs(
        self,
        export_format: str = "jsonl",
        status: str | None = None,
        platform: str | None = None,
        query_text: str | None = None,
        output: Path | None = None,
    ) -> Path:
        query = {"format": export_format}
        if status:
            query["status"] = status
        if platform:
            query["platform"] = platform
        if query_text:
            query["q"] = query_text
        response = self._request("GET", f"v1/jobs/export?{urlencode(query)}")
        filename = filename_from_response(response, fallback=f"jobs.{export_format}")
        destination = output or Path(filename)
        if destination.is_dir():
            destination = destination / filename
        destination.write_bytes(response.data)
        return destination

    def list_jobs(
        self,
        status: str | None = None,
        platform: str | None = None,
        query_text: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        query: dict[str, Any] = {"limit": limit, "offset": offset}
        if status:
            query["status"] = status
        if platform:
            query["platform"] = platform
        if query_text:
            query["q"] = query_text
        return self._request_json("GET", f"v1/jobs?{urlencode(query)}")

    def download_artifact(
        self,
        job_id: str,
        artifact: str,
        output: Path | None = None,
        verify: bool = True,
        resume: bool = False,
    ) -> Path:
        artifact_paths = {
            "media": f"v1/jobs/{job_id}/media",
            "info-json": f"v1/jobs/{job_id}/info-json",
            "archive": f"v1/jobs/{job_id}/archive",
        }
        path = artifact_paths[artifact]
        verification = self._artifact_verification(job_id, artifact) if verify else None
        destination_source = self._request("HEAD", path) if resume else None
        filename = (
            filename_from_response(destination_source, fallback=f"{job_id}-{artifact}")
            if destination_source
            else f"{job_id}-{artifact}"
        )
        destination = output or Path(filename)
        if destination.is_dir():
            destination = destination / filename

        request_headers = {}
        append = False
        expected_size = None
        if resume and destination.exists():
            current_size = destination.stat().st_size
            expected_size = (
                verification.get("bytes")
                if verification
                else response_content_length(destination_source)
            )
            if current_size > 0 and artifact not in {"media", "archive"}:
                raise ClientError(f"{artifact} downloads do not support HTTP range resume")
            if expected_size is None and current_size > 0:
                raise ClientError(f"{artifact} does not expose byte size metadata for resume")
            if expected_size is not None:
                if current_size == expected_size:
                    if verification:
                        verify_artifact_file(destination, artifact, verification)
                    return destination
                if current_size > expected_size:
                    raise ClientError(
                        f"{artifact} existing file is larger than expected: "
                        f"{current_size} > {expected_size}"
                    )
            if current_size > 0:
                request_headers["Range"] = f"bytes={current_size}-"
                append = True

        response = self._request_stream("GET", path, extra_headers=request_headers)
        try:
            if append and response.status != 206:
                raise ClientError(f"resume expected HTTP 206 but got HTTP {response.status}")

            if not append:
                if verification is None:
                    expected_size = response_content_length(response)
                filename = filename_from_response(response, fallback=filename)
                if output is None:
                    destination = Path(filename)
                elif output.is_dir():
                    destination = output / filename

            write_destination = resume_path(destination) if append else partial_path(destination)
            try:
                if append:
                    copy_file(destination, write_destination)
                write_artifact_stream(
                    response,
                    write_destination,
                    artifact,
                    verification=verification,
                    append=append,
                    expected_size=expected_size,
                )
                write_destination.replace(destination)
            except Exception:
                write_destination.unlink(missing_ok=True)
                raise
        finally:
            response.release_conn()
        return destination

    def _artifact_verification(self, job_id: str, artifact: str) -> dict[str, Any]:
        artifacts = self.artifacts(job_id)
        checksum_fields = {
            "media": "media_sha256",
            "info-json": "info_json_sha256",
            "archive": "archive_sha256",
        }
        size_fields = {
            "media": "media_bytes",
            "archive": "archive_bytes",
        }
        verification: dict[str, Any] = {"sha256": artifacts[checksum_fields[artifact]]}
        size_field = size_fields.get(artifact)
        if size_field:
            verification["bytes"] = artifacts[size_field]
        return verification

    def wait_for_job(self, job_id: str, timeout_seconds: float = 30.0) -> dict[str, Any]:
        return self._request_json(
            "GET",
            f"v1/jobs/{job_id}/wait?{urlencode({'timeout_seconds': timeout_seconds})}",
        )

    def _request_json(
        self,
        method: str,
        path: str,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = self._request(method, path, json_body=json_body)
        text = response.data.decode("utf-8", errors="replace")
        try:
            return json.loads(text)
        except json.JSONDecodeError as err:
            raise ClientError(f"response was not JSON: {text}") from err

    def _request(
        self,
        method: str,
        path: str,
        json_body: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> Any:
        headers = {"accept": "application/json"}
        body = None
        if self.api_key:
            headers["x-api-key"] = self.api_key
        if json_body is not None:
            headers["content-type"] = "application/json"
            body = json.dumps(json_body).encode()
        if extra_headers:
            headers.update(extra_headers)

        try:
            response = self.http.request(
                method,
                urljoin(self.base_url, path),
                body=body,
                headers=headers,
            )
        except HTTPError as err:
            raise ClientError(f"request failed: {err}") from err

        if response.status >= 400:
            text = response.data.decode("utf-8", errors="replace")
            raise ClientError(f"HTTP {response.status}: {text}")
        return response

    def _request_stream(
        self,
        method: str,
        path: str,
        extra_headers: dict[str, str] | None = None,
    ) -> Any:
        headers = {"accept": "application/octet-stream"}
        if self.api_key:
            headers["x-api-key"] = self.api_key
        if extra_headers:
            headers.update(extra_headers)

        try:
            response = self.http.request(
                method,
                urljoin(self.base_url, path),
                headers=headers,
                preload_content=False,
            )
        except HTTPError as err:
            raise ClientError(f"request failed: {err}") from err

        if response.status >= 400:
            text = response.read().decode("utf-8", errors="replace")
            response.release_conn()
            raise ClientError(f"HTTP {response.status}: {text}")
        return response


def filename_from_response(response: Any, fallback: str) -> str:
    disposition = response.headers.get("content-disposition", "")
    for part in disposition.split(";"):
        part = part.strip()
        if part.startswith("filename="):
            filename = part.removeprefix("filename=").strip('"')
            if filename:
                return filename
    geturl = getattr(response, "geturl", None)
    path = urlparse(geturl()).path if callable(geturl) else ""
    filename = Path(path).name
    return filename or fallback


def response_content_length(response: Any) -> int | None:
    if response is None:
        return None
    value = response.headers.get("content-length")
    if not value or not value.isdigit():
        return None
    return int(value)


def partial_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.part")


def resume_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.resume")


def copy_file(source: Path, destination: Path) -> None:
    with source.open("rb") as src, destination.open("wb") as dst:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            dst.write(chunk)


def collect_values(values: list[str], file: Path | None) -> list[str]:
    items = list(values)
    if file:
        items.extend(line.strip() for line in file.read_text().splitlines() if line.strip())
    return items


def verify_artifact_bytes(data: bytes, artifact: str, verification: dict[str, Any]) -> None:
    verify_artifact_digest(
        artifact,
        verification,
        actual_size=len(data),
        actual_sha256=hashlib.sha256(data).hexdigest(),
    )


def verify_artifact_digest(
    artifact: str,
    verification: dict[str, Any],
    actual_size: int,
    actual_sha256: str,
) -> None:
    expected_size = verification.get("bytes")
    if expected_size is not None and actual_size != expected_size:
        raise ClientError(
            f"{artifact} size mismatch: expected {expected_size} bytes, got {actual_size}"
        )

    expected_sha256 = verification.get("sha256")
    if actual_sha256 != expected_sha256:
        raise ClientError(
            f"{artifact} SHA-256 mismatch: expected {expected_sha256}, got {actual_sha256}"
        )


def verify_artifact_file(path: Path, artifact: str, verification: dict[str, Any]) -> None:
    expected_size = verification.get("bytes")
    if expected_size is not None:
        actual_size = path.stat().st_size
        if actual_size != expected_size:
            raise ClientError(
                f"{artifact} size mismatch: expected {expected_size} bytes, got {actual_size}"
            )

    expected_sha256 = verification.get("sha256")
    hasher = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            hasher.update(chunk)
    actual_sha256 = hasher.hexdigest()
    if actual_sha256 != expected_sha256:
        raise ClientError(
            f"{artifact} SHA-256 mismatch: expected {expected_sha256}, got {actual_sha256}"
        )


def write_artifact_stream(
    response: Any,
    destination: Path,
    artifact: str,
    verification: dict[str, Any] | None,
    append: bool,
    expected_size: int | None,
) -> None:
    hasher = hashlib.sha256() if verification and not append else None
    bytes_written = 0
    mode = "ab" if append else "wb"
    with destination.open(mode) as file:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            file.write(chunk)
            bytes_written += len(chunk)
            if hasher:
                hasher.update(chunk)

    if append:
        if verification:
            verify_artifact_file(destination, artifact, verification)
        elif expected_size is not None and destination.stat().st_size != expected_size:
            raise ClientError(
                f"{artifact} size mismatch: expected {expected_size} bytes, "
                f"got {destination.stat().st_size}"
            )
    elif verification and hasher:
        verify_artifact_digest(
            artifact,
            verification,
            actual_size=bytes_written,
            actual_sha256=hasher.hexdigest(),
        )
    elif expected_size is not None and bytes_written != expected_size:
        raise ClientError(
            f"{artifact} size mismatch: expected {expected_size} bytes, got {bytes_written}"
        )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:3000")
    parser.add_argument("--api-key")
    subparsers = parser.add_subparsers(dest="command", required=True)

    submit = subparsers.add_parser("submit")
    submit.add_argument("urls", nargs="*")
    submit.add_argument("--file", type=Path, help="text file with one URL per line")
    submit.add_argument("--webhook-url")
    submit.add_argument("--format", dest="format_selector", help="yt-dlp format selector")
    submit.add_argument("--cookie-profile", help="configured server-side cookie profile name")
    submit.add_argument(
        "--force",
        action="store_true",
        help="redownload URLs with existing successful jobs",
    )
    submit.add_argument("--wait", action="store_true")
    submit.add_argument("--wait-timeout", type=float, default=30.0)

    validate = subparsers.add_parser("validate")
    validate.add_argument("urls", nargs="*")
    validate.add_argument("--file", type=Path, help="text file with one URL per line")
    validate.add_argument("--webhook-url")
    validate.add_argument("--format", dest="format_selector", help="yt-dlp format selector")
    validate.add_argument("--cookie-profile", help="configured server-side cookie profile name")

    subparsers.add_parser("platforms")
    subparsers.add_parser("config")
    subparsers.add_parser("cookie-profiles")
    subparsers.add_parser("queue")
    subparsers.add_parser("workers")
    subparsers.add_parser("summary")

    cleanup = subparsers.add_parser("cleanup")
    cleanup.add_argument("--max-bytes", type=int)
    cleanup.add_argument("--execute", action="store_true")

    job = subparsers.add_parser("job")
    job.add_argument("job_id")

    position = subparsers.add_parser("position")
    position.add_argument("job_id")

    wait = subparsers.add_parser("wait")
    wait.add_argument("job_id")
    wait.add_argument("--timeout", type=float, default=30.0)

    artifacts = subparsers.add_parser("artifacts")
    artifacts.add_argument("job_id")

    cancel = subparsers.add_parser("cancel")
    cancel.add_argument("job_id")

    batch_cancel = subparsers.add_parser("batch-cancel")
    batch_cancel.add_argument("job_ids", nargs="*")
    batch_cancel.add_argument("--file", type=Path, help="text file with one job id per line")

    retry = subparsers.add_parser("retry")
    retry.add_argument("job_id")

    batch_retry = subparsers.add_parser("batch-retry")
    batch_retry.add_argument("job_ids", nargs="*")
    batch_retry.add_argument("--file", type=Path, help="text file with one job id per line")

    webhook = subparsers.add_parser("webhook")
    webhook.add_argument("job_id")

    delete = subparsers.add_parser("delete")
    delete.add_argument("job_id")

    batch_delete = subparsers.add_parser("batch-delete")
    batch_delete.add_argument("job_ids", nargs="*")
    batch_delete.add_argument("--file", type=Path, help="text file with one job id per line")

    export = subparsers.add_parser("export")
    export.add_argument("--format", choices=["jsonl", "csv"], default="jsonl")
    export.add_argument(
        "--status",
        choices=["queued", "running", "succeeded", "failed", "canceled", "deleted"],
    )
    export.add_argument("--platform")
    export.add_argument("--query", dest="query_text")
    export.add_argument("-o", "--output", type=Path)

    list_command = subparsers.add_parser("list")
    list_command.add_argument(
        "--status",
        choices=["queued", "running", "succeeded", "failed", "canceled", "deleted"],
    )
    list_command.add_argument("--platform")
    list_command.add_argument("--query", dest="query_text")
    list_command.add_argument("--limit", type=int, default=100)
    list_command.add_argument("--offset", type=int, default=0)

    download = subparsers.add_parser("download")
    download.add_argument("job_id")
    download.add_argument(
        "--artifact",
        choices=["media", "info-json", "archive"],
        default="media",
    )
    download.add_argument("-o", "--output", type=Path)
    download.add_argument(
        "--no-verify",
        action="store_true",
        help="skip checksum and size verification from the artifacts endpoint",
    )
    download.add_argument(
        "--resume",
        action="store_true",
        help="resume an existing partial media or archive file with an HTTP range request",
    )

    args = parser.parse_args(argv)
    client = DownloadClient(args.base_url, args.api_key)

    if args.command == "submit":
        urls = collect_values(args.urls, args.file)
        response = client.submit(
            urls,
            webhook_url=args.webhook_url,
            format_selector=args.format_selector,
            cookie_profile=args.cookie_profile,
            force=args.force,
            wait=args.wait,
            wait_timeout=args.wait_timeout,
        )
    elif args.command == "validate":
        urls = collect_values(args.urls, args.file)
        response = client.validate(
            urls,
            webhook_url=args.webhook_url,
            format_selector=args.format_selector,
            cookie_profile=args.cookie_profile,
        )
    elif args.command == "platforms":
        response = client.platforms()
    elif args.command == "config":
        response = client.config()
    elif args.command == "cookie-profiles":
        response = client.cookie_profiles()
    elif args.command == "queue":
        response = client.queue()
    elif args.command == "workers":
        response = client.workers()
    elif args.command == "cleanup":
        response = client.storage_cleanup(max_bytes=args.max_bytes, execute=args.execute)
    elif args.command == "summary":
        response = client.summary()
    elif args.command == "job":
        response = client.get_job(args.job_id)
    elif args.command == "position":
        response = client.queue_position(args.job_id)
    elif args.command == "wait":
        response = client.wait_for_job(args.job_id, args.timeout)
    elif args.command == "artifacts":
        response = client.artifacts(args.job_id)
    elif args.command == "cancel":
        response = client.cancel_job(args.job_id)
    elif args.command == "batch-cancel":
        response = client.cancel_jobs(collect_values(args.job_ids, args.file))
    elif args.command == "retry":
        response = client.retry_job(args.job_id)
    elif args.command == "batch-retry":
        response = client.retry_jobs(collect_values(args.job_ids, args.file))
    elif args.command == "webhook":
        response = client.redeliver_webhook(args.job_id)
    elif args.command == "delete":
        response = client.delete_job(args.job_id)
    elif args.command == "batch-delete":
        response = client.delete_jobs(collect_values(args.job_ids, args.file))
    elif args.command == "export":
        destination = client.export_jobs(
            export_format=args.format,
            status=args.status,
            platform=args.platform,
            query_text=args.query_text,
            output=args.output,
        )
        print(destination)
        return 0
    elif args.command == "list":
        response = client.list_jobs(
            status=args.status,
            platform=args.platform,
            query_text=args.query_text,
            limit=args.limit,
            offset=args.offset,
        )
    else:
        destination = client.download_artifact(
            args.job_id,
            artifact=args.artifact,
            output=args.output,
            verify=not args.no_verify,
            resume=args.resume,
        )
        print(destination)
        return 0

    print(json.dumps(response, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except ClientError as err:
        print(err, file=sys.stderr)
        raise SystemExit(1)
