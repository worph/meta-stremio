"""
Meta Events Consumer for meta-stremio.

Consumes the meta:events stream published by meta-core. Originally used a
Redis Streams XREADGROUP loop; migrated to meta-core's HTTP SSE endpoint at
/api/events/meta as part of the api-mediated-access lockdown (see
meta-core docs/api-mediated-access.md, PR C).

The callback signature is unchanged so leader_storage.py needs no changes:
each event is delivered as (key, event_type) where key has the form
"file:<hashId>/<field>" and event_type is "set" / "del" / "expire".
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Callable, List, Optional
from urllib.parse import urlparse

import urllib.request
import urllib.error

logger = logging.getLogger(__name__)


class MetaConsumer:
    """
    Streams meta:events from meta-core over SSE and dispatches a filtered
    subset of changes to registered callbacks.

    Cursor durability: the last-seen event id is persisted to a file (default
    /meta-core/cursors/meta-stremio-meta.cursor) so the consumer resumes
    cleanly after a restart. On a gap event the cursor is reset to the
    server-supplied resumeFrom value.
    """

    STREAM_PATH = "/api/events/meta"

    # Fields whose changes invalidate cached Stremio responses.
    INTERESTING_FIELDS = [
        "tmdb", "tmdbId", "title", "poster", "backdrop",
        "imdbId", "imdbid", "year", "type", "rating",
        "description", "plot", "genres", "fileType",
    ]

    def __init__(
        self,
        api_url: str,
        cursor_path: str = "/meta-core/cursors/meta-stremio-meta.cursor",
        request_timeout: int = 35,
    ) -> None:
        self._api_url = api_url.rstrip("/")
        self._cursor_path = cursor_path
        self._request_timeout = request_timeout
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._callbacks: List[Callable[[str, str], None]] = []
        self._cursor: str = self._load_cursor()
        self._stop_event = threading.Event()

    def on_change(self, callback: Callable[[str, str], None]) -> "MetaConsumer":
        """Register a callback. See module docstring for the contract."""
        self._callbacks.append(callback)
        return self

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()
        logger.info(
            "[MetaConsumer] Started SSE consumer for %s (cursor=%s)",
            f"{self._api_url}{self.STREAM_PATH}",
            self._cursor or "$",
        )

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        # Best-effort flush of any unsaved cursor advance.
        self._save_cursor()
        logger.info("[MetaConsumer] Stopped")

    def is_running(self) -> bool:
        return self._running

    # ---------- internals -----------------------------------------------

    def _load_cursor(self) -> str:
        try:
            with open(self._cursor_path, "r", encoding="utf-8") as f:
                value = f.read().strip()
                if value:
                    logger.info("[MetaConsumer] Resuming from cursor %s", value)
                return value
        except FileNotFoundError:
            return ""
        except Exception as e:
            logger.warning(
                "[MetaConsumer] Could not read cursor %s: %s", self._cursor_path, e
            )
            return ""

    def _save_cursor(self) -> None:
        if not self._cursor:
            return
        try:
            Path(self._cursor_path).parent.mkdir(parents=True, exist_ok=True)
            with open(self._cursor_path, "w", encoding="utf-8") as f:
                f.write(self._cursor)
        except Exception as e:
            logger.warning(
                "[MetaConsumer] Could not write cursor %s: %s", self._cursor_path, e
            )

    def _consume_loop(self) -> None:
        backoff = 0.5
        max_backoff = 30.0
        while self._running:
            try:
                self._stream_once()
                backoff = 0.5  # successful read → reset backoff
            except Exception as e:
                if not self._running:
                    return
                logger.warning(
                    "[MetaConsumer] SSE error, reconnecting in %.1fs: %s",
                    backoff, e,
                )
                # interruptible sleep
                self._stop_event.wait(timeout=backoff)
                backoff = min(backoff * 2, max_backoff)

    def _stream_once(self) -> None:
        url = f"{self._api_url}{self.STREAM_PATH}"
        req = urllib.request.Request(url, method="GET")
        req.add_header("Accept", "text/event-stream")
        req.add_header("Cache-Control", "no-cache")
        if self._cursor:
            req.add_header("Last-Event-ID", self._cursor)

        # urllib.request doesn't natively grok SSE; we open the socket and
        # read line-by-line until the connection drops or the thread is
        # asked to stop.
        with urllib.request.urlopen(req, timeout=self._request_timeout) as resp:
            buffer = ""
            since_save = time.time()
            for chunk in resp:
                if not self._running:
                    return
                buffer += chunk.decode("utf-8", errors="replace")
                # Process complete events (separated by blank lines).
                while "\n\n" in buffer:
                    event_chunk, buffer = buffer.split("\n\n", 1)
                    self._handle_chunk(event_chunk)
                # Flush cursor periodically.
                if time.time() - since_save > 2.0:
                    self._save_cursor()
                    since_save = time.time()

    def _handle_chunk(self, chunk: str) -> None:
        event_id = ""
        event_type = ""
        data_lines: List[str] = []
        for line in chunk.split("\n"):
            if not line or line.startswith(":"):
                continue  # comment / heartbeat
            colon = line.find(":")
            if colon == -1:
                continue
            field = line[:colon]
            value = line[colon + 1:]
            if value.startswith(" "):
                value = value[1:]
            if field == "id":
                event_id = value
            elif field == "event":
                event_type = value
            elif field == "data":
                data_lines.append(value)

        if not data_lines:
            return

        try:
            data = json.loads("\n".join(data_lines))
        except json.JSONDecodeError as e:
            logger.warning(
                "[MetaConsumer] Skipping unparseable event %s: %s",
                event_id or "(no id)", e,
            )
            return

        # `gap` is a synthetic event signalling that the server trimmed the
        # cursor out of retention. Reset to the resumeFrom value and
        # continue. Stremio doesn't bootstrap from the stream so loss is
        # acceptable — it just means the cache may be stale for a moment.
        if event_type == "gap":
            resume = data.get("resumeFrom")
            if isinstance(resume, str) and resume:
                self._cursor = resume
            logger.warning(
                "[MetaConsumer] Stream cursor trimmed; resuming from %s",
                resume,
            )
            return

        key = data.get("key")
        if not key or not event_type:
            return
        if not self._is_interesting_field(key):
            # Still advance the cursor so we don't replay this event after
            # a restart.
            if event_id:
                self._cursor = event_id
            return

        for callback in self._callbacks:
            try:
                callback(key, event_type)
            except Exception as e:
                logger.error("[MetaConsumer] Callback error: %s", e)

        if event_id:
            self._cursor = event_id

    def _is_interesting_field(self, key: str) -> bool:
        for field in self.INTERESTING_FIELDS:
            if key.endswith(f"/{field}"):
                return True
        return False
