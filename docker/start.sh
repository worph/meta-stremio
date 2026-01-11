#!/bin/bash
# Meta-Stremio startup script
# Starts meta-core sidecar before the Python server (when using leader discovery)

set -e

# Only start meta-core when using leader discovery mode
if [ "${STORAGE_MODE}" = "leader" ]; then
    echo "[start.sh] Starting meta-core sidecar (leader discovery mode)..."

    # Start meta-core in background
    /usr/local/bin/meta-core &
    META_CORE_PID=$!

    # Wait for meta-core to be ready (health endpoint)
    echo "[start.sh] Waiting for meta-core to be ready..."
    for i in {1..30}; do
        if curl -sf http://localhost:9000/health > /dev/null 2>&1; then
            echo "[start.sh] meta-core is ready"
            break
        fi
        if [ $i -eq 30 ]; then
            echo "[start.sh] Warning: meta-core health check timeout, continuing anyway..."
        fi
        sleep 1
    done
else
    echo "[start.sh] Using direct Redis connection (STORAGE_MODE=${STORAGE_MODE}), skipping meta-core..."
fi

# Start the Python server (foreground)
echo "[start.sh] Starting meta-stremio server..."

# Use dev source if available (for development with hot reload)
if [ -d "/app/src-dev" ] && [ -f "/app/src-dev/server.py" ]; then
    echo "[start.sh] Using development source (/app/src-dev)"
    cd /app/src-dev
else
    echo "[start.sh] Using production source (/app/src)"
    cd /app/src
fi

exec python server.py
