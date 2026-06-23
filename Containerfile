FROM docker.io/library/rust:1-trixie AS builder

ARG APP_NAME=yt-dlp-server

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked \
    && mkdir -p /out/bin \
    && cp "target/release/${APP_NAME}" /out/bin/

FROM docker.io/library/debian:trixie-slim AS runtime

ARG APP_NAME=yt-dlp-server

ENV UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libssl3 \
        python3 \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh \
    && useradd --system --uid 10001 --home-dir /app --create-home --shell /usr/sbin/nologin downloader

WORKDIR /app

COPY --from=builder /out/bin/${APP_NAME} /usr/local/bin/${APP_NAME}
COPY pyproject.toml uv.lock .python-version /app/
COPY config.example.toml /app/config.example.toml

RUN uv sync --frozen --no-dev \
    && mkdir -p /app/data \
    && chown -R downloader:downloader /app

ENV BIND_ADDR=0.0.0.0:3000 \
    DATA_DIR=/app/data \
    DOWNLOAD_OUTPUT_DIR=/app/data/downloads \
    RUST_LOG=info

USER downloader

EXPOSE 3000
VOLUME ["/app/data"]
STOPSIGNAL SIGTERM

ENTRYPOINT ["yt-dlp-server"]
