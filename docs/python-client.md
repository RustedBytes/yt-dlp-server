# Python Client

A small `urllib3` client is available at `examples/python_client.py`.

Install the Python dependency:

```bash
python3 -m pip install urllib3
```

Queue downloads:

```bash
python3 examples/python_client.py submit \
  https://www.tiktok.com/@user/video/123 \
  https://www.instagram.com/reel/ABC/ \
  https://www.youtube.com/shorts/XYZ
```

Queue downloads with a yt-dlp format selector and wait for terminal results:

```bash
python3 examples/python_client.py submit --format 'mp4/best' --wait \
  https://www.youtube.com/shorts/XYZ
```

Queue downloads with a configured server-side cookie profile:

```bash
python3 examples/python_client.py submit --cookie-profile account_a \
  https://www.instagram.com/reel/ABC/
```

Queue from a text file with one URL per line:

```bash
python3 examples/python_client.py submit --file urls.txt
```

Redownload URLs that already have successful retained jobs:

```bash
python3 examples/python_client.py submit --force --file urls.txt
```

Validate URLs without enqueueing jobs:

```bash
python3 examples/python_client.py validate --file urls.txt --format 'mp4/best'
```

Inspect server capabilities before submitting:

```bash
python3 examples/python_client.py config
python3 examples/python_client.py cookie-profiles
python3 examples/python_client.py platforms
python3 examples/python_client.py queue
python3 examples/python_client.py workers
```

Preview or run storage cleanup:

```bash
python3 examples/python_client.py cleanup --max-bytes 1073741824
python3 examples/python_client.py cleanup --max-bytes 1073741824 --execute
```

Read current job counts by status:

```bash
python3 examples/python_client.py summary
```

List retained jobs with archive filters:

```bash
python3 examples/python_client.py list --status succeeded --platform instagram --query creator
```

Export retained job history:

```bash
python3 examples/python_client.py export --format jsonl
python3 examples/python_client.py export --format csv --status failed --platform tiktok --query rate -o failed-jobs.csv
```

Check a job:

```bash
python3 examples/python_client.py job <job-id>
```

Check a queued job's estimated position:

```bash
python3 examples/python_client.py position <job-id>
```

Wait for a terminal job status or until the wait timeout expires:

```bash
python3 examples/python_client.py wait <job-id> --timeout 30
```

Discover and download completed job artifacts:

```bash
python3 examples/python_client.py artifacts <job-id>
python3 examples/python_client.py download <job-id> --artifact media -o video.mp4
python3 examples/python_client.py download <job-id> --artifact info-json
python3 examples/python_client.py download <job-id> --artifact archive
```

Downloads are streamed to a `.part` file, verified, and then moved into place. Verification uses the server's artifact SHA-256 metadata by default; media/archive downloads also verify the expected byte size. Use `--no-verify` to skip that preflight check; streamed downloads still validate `Content-Length` when the response includes it.

Use `--resume` with media or archive downloads to continue an existing partial output file with an HTTP range request. Resume writes into a temporary copy first, so the existing partial file is only replaced after the completed file passes verification:

```bash
python3 examples/python_client.py download <job-id> --artifact archive -o video.tar --resume
```

Manage terminal or active jobs:

```bash
python3 examples/python_client.py cancel <job-id>
python3 examples/python_client.py retry <job-id>
python3 examples/python_client.py webhook <job-id>
python3 examples/python_client.py delete <job-id>
```

Run job actions in batches:

```bash
python3 examples/python_client.py batch-cancel <job-id-1> <job-id-2>
python3 examples/python_client.py batch-retry --file job-ids.txt
python3 examples/python_client.py batch-delete <job-id-1> <job-id-2>
```
