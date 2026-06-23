#!/usr/bin/env python3
"""Small urllib3 client for the social video download server."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

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
        wait: bool = False,
        poll_interval: float = 2.0,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"urls": urls}
        if webhook_url:
            body["webhook_url"] = webhook_url
        response = self._request_json("POST", "v1/downloads", json_body=body)
        if wait:
            response["jobs"] = [
                self.wait_for_job(job["id"], poll_interval) for job in response["jobs"]
            ]
        return response

    def get_job(self, job_id: str) -> dict[str, Any]:
        return self._request_json("GET", f"v1/jobs/{job_id}")

    def wait_for_job(self, job_id: str, poll_interval: float) -> dict[str, Any]:
        while True:
            job = self.get_job(job_id)
            if job["status"] in {"succeeded", "failed"}:
                return job
            time.sleep(poll_interval)

    def _request_json(
        self,
        method: str,
        path: str,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        headers = {"accept": "application/json"}
        body = None
        if self.api_key:
            headers["x-api-key"] = self.api_key
        if json_body is not None:
            headers["content-type"] = "application/json"
            body = json.dumps(json_body).encode()

        try:
            response = self.http.request(
                method,
                urljoin(self.base_url, path),
                body=body,
                headers=headers,
            )
        except HTTPError as err:
            raise ClientError(f"request failed: {err}") from err

        text = response.data.decode("utf-8", errors="replace")
        if response.status >= 400:
            raise ClientError(f"HTTP {response.status}: {text}")
        try:
            return json.loads(text)
        except json.JSONDecodeError as err:
            raise ClientError(f"response was not JSON: {text}") from err


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:3000")
    parser.add_argument("--api-key")
    subparsers = parser.add_subparsers(dest="command", required=True)

    submit = subparsers.add_parser("submit")
    submit.add_argument("urls", nargs="*")
    submit.add_argument("--file", type=Path, help="text file with one URL per line")
    submit.add_argument("--webhook-url")
    submit.add_argument("--wait", action="store_true")
    submit.add_argument("--poll-interval", type=float, default=2.0)

    job = subparsers.add_parser("job")
    job.add_argument("job_id")

    args = parser.parse_args(argv)
    client = DownloadClient(args.base_url, args.api_key)

    if args.command == "submit":
        urls = list(args.urls)
        if args.file:
            urls.extend(
                line.strip()
                for line in args.file.read_text().splitlines()
                if line.strip()
            )
        response = client.submit(
            urls,
            webhook_url=args.webhook_url,
            wait=args.wait,
            poll_interval=args.poll_interval,
        )
    else:
        response = client.get_job(args.job_id)

    print(json.dumps(response, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except ClientError as err:
        print(err, file=sys.stderr)
        raise SystemExit(1)
