# Running the Server

Install/sync the Python dependency:

```bash
uv sync --frozen
```

Start the server:

```bash
cargo run
```

Default bind address is `127.0.0.1:3000`.

Open the browser UI:

```bash
open http://127.0.0.1:3000/
```

The browser UI template is embedded into the binary.

## Podman

Build the container image:

```bash
podman build -f Containerfile -t yt-dlp-server .
```

Run it with a persistent data volume:

```bash
podman volume create downloader-data
podman run --rm \
  -p 3000:3000 \
  -v downloader-data:/app/data:U \
  yt-dlp-server
```

To use a custom config file:

```bash
podman run --rm \
  -p 3000:3000 \
  -v "$PWD/config.toml:/app/config.toml:ro" \
  -v downloader-data:/app/data:U \
  yt-dlp-server
```

To use a server-side cookie file:

```bash
podman run --rm \
  -p 3000:3000 \
  -e YT_DLP_COOKIES_PATH=/app/cookies/cookies.txt \
  -v "$PWD/cookies.txt:/app/cookies/cookies.txt:ro" \
  -v downloader-data:/app/data:U \
  yt-dlp-server
```

## Compose

Build and run with Compose:

```bash
podman compose -f compose.yml up --build
```

Docker Compose works with the same file:

```bash
docker compose -f compose.yml up --build
```

The Compose file stores runtime data in the `downloader-data` named volume.
