# Meta-Stremio Docker Image
# Standalone Stremio addon with HLS transcoding and KV storage integration
#
# Build standalone:
#   docker build -t meta-stremio .
#
# Build with custom meta-core (for development):
#   docker build --build-arg META_CORE_IMAGE=meta-core:local -t meta-stremio .

# Stage 0: Get meta-core binary from published image
ARG META_CORE_IMAGE=ghcr.io/worph/meta-core:latest
FROM ${META_CORE_IMAGE} AS meta-core

# Stage 1: Runtime
FROM python:3.11-slim

# Container registry metadata
LABEL org.opencontainers.image.source=https://github.com/worph/meta-stremio
LABEL org.opencontainers.image.description="MetaMesh Stremio addon with HLS transcoding"
LABEL org.opencontainers.image.licenses=MIT

# Install FFmpeg, curl (for healthcheck), Redis, and other dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    redis-server \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source files
COPY src/ ./src/
COPY www/ ./www/

# Copy meta-core sidecar binary
COPY --from=meta-core /usr/local/bin/meta-core /usr/local/bin/meta-core
RUN chmod +x /usr/local/bin/meta-core

# Copy startup script
COPY docker/start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Create directories for cache (media comes from volume)
RUN mkdir -p /data/cache

# Environment variables with defaults
# Storage modes: 'leader' (discover via lock file), 'redis' (direct URL)
ENV PORT=7000 \
    MEDIA_DIR=/files \
    CACHE_DIR=/data/cache \
    STORAGE_MODE=leader \
    META_CORE_PATH=/meta-core \
    FILES_PATH=/files \
    REDIS_URL=redis://localhost:6379 \
    REDIS_PREFIX= \
    SEGMENT_DURATION=4 \
    PREFETCH_SEGMENTS=4 \
    SCHEME=auto \
    SERVICE_NAME=meta-stremio \
    SERVICE_VERSION=1.0.0 \
    META_CORE_HTTP_PORT=9000

# Expose port
EXPOSE 7000

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:7000/health || exit 1

# Run the server via startup script (starts meta-core + Python server)
CMD ["/app/start.sh"]
