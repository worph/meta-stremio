"""
Meta Events Consumer for meta-stremio.

Consumes the meta:events Redis stream published by meta-core.
Provides real-time notifications when metadata changes.
"""
from __future__ import annotations

import threading
import logging
from typing import Callable, List, Optional

try:
    from redis import Redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    Redis = None

logger = logging.getLogger(__name__)


class MetaConsumer:
    """
    Consumes meta:events stream for real-time metadata updates.

    Uses Redis consumer groups for reliable delivery.
    Filters events to only notify about interesting field changes.
    """

    STREAM = "meta:events"
    GROUP = "stremio-consumer"
    CONSUMER = "stremio-1"

    # Fields that trigger cache invalidation
    INTERESTING_FIELDS = [
        "tmdb", "tmdbId", "title", "poster", "backdrop",
        "imdbId", "imdbid", "year", "type", "rating",
        "description", "plot", "genres", "fileType"
    ]

    def __init__(self, redis_client: Redis, prefix: str = ""):
        if not REDIS_AVAILABLE:
            raise ImportError("redis package not installed")

        self._client = redis_client
        self._prefix = prefix
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._callbacks: List[Callable[[str, str], None]] = []

    def on_change(self, callback: Callable[[str, str], None]) -> 'MetaConsumer':
        """
        Register callback for metadata changes.

        callback(key, event_type): Called when a file's metadata changes.
            - key: The Redis key that changed (e.g., "file:abc123/tmdb")
            - event_type: The operation type ("set", "del", etc.)
        """
        self._callbacks.append(callback)
        return self

    def start(self) -> None:
        """Start consuming events."""
        if self._running:
            return

        # Create consumer group if not exists
        try:
            self._client.xgroup_create(
                self.STREAM, self.GROUP, id="$", mkstream=True
            )
            logger.info(f"[MetaConsumer] Created consumer group '{self.GROUP}'")
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                logger.warning(f"[MetaConsumer] Group creation error: {e}")

        self._running = True
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()
        logger.info("[MetaConsumer] Started consuming meta:events stream")

    def stop(self) -> None:
        """Stop consuming events."""
        if not self._running:
            return

        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        logger.info("[MetaConsumer] Stopped")

    def is_running(self) -> bool:
        """Check if consumer is running."""
        return self._running

    def _consume_loop(self) -> None:
        """Main consumer loop."""
        while self._running:
            try:
                # Read from stream with 2s block timeout
                entries = self._client.xreadgroup(
                    self.GROUP,
                    self.CONSUMER,
                    {self.STREAM: ">"},
                    count=100,
                    block=2000
                )

                if not entries:
                    continue

                for stream_name, messages in entries:
                    for msg_id, fields in messages:
                        self._process_event(msg_id, fields)
                        # Acknowledge
                        self._client.xack(self.STREAM, self.GROUP, msg_id)

            except Exception as e:
                if self._running:
                    logger.error(f"[MetaConsumer] Error consuming: {e}")

    def _process_event(self, msg_id: str, fields: dict) -> None:
        """Process a single event from the stream."""
        # Handle both bytes and string keys
        key = fields.get(b"key") or fields.get("key")
        event_type = fields.get(b"type") or fields.get("type")

        if key is None or event_type is None:
            return

        # Decode bytes if needed
        key = key.decode() if isinstance(key, bytes) else key
        event_type = event_type.decode() if isinstance(event_type, bytes) else event_type

        # Filter: only care about interesting field changes
        if not self._is_interesting_field(key):
            return

        # Notify callbacks
        for callback in self._callbacks:
            try:
                callback(key, event_type)
            except Exception as e:
                logger.error(f"[MetaConsumer] Callback error: {e}")

    def _is_interesting_field(self, key: str) -> bool:
        """Check if the key represents an interesting field."""
        # Key format: file:{hashId}/{field}
        for field in self.INTERESTING_FIELDS:
            if key.endswith(f"/{field}"):
                return True
        return False
