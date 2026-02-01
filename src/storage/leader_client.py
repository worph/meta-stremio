"""
LeaderClient - Client for reading leader info from meta-core

This replaces the old LeaderDiscovery.py by delegating leader election
to meta-core (Go sidecar) and just reading the results.

Features:
- Reads kv-leader.info file for leader URLs
- Calls meta-core /urls API for current leader URLs
- Watches for leader changes via file system watcher
"""
from __future__ import annotations

import os
import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Callable
from urllib.request import urlopen, Request
from urllib.error import URLError

# Try to import watchdog for file watching (optional)
try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False
    Observer = None
    FileSystemEventHandler = object


@dataclass
class LeaderLockInfo:
    """Leader lock file content format (kv-leader.info).
    Written by meta-core leader, read by other services.
    """
    hostname: str
    base_url: str
    api_url: str      # meta-core API URL (port 9000)
    redis_url: str
    webdav_url: str
    timestamp: int
    pid: int


@dataclass
class URLsResponse:
    """Response from meta-core /urls API."""
    hostname: str
    base_url: str
    api_url: str
    redis_url: str
    webdav_url: str
    is_leader: bool


class LockFileHandler(FileSystemEventHandler):
    """File system event handler for lock file changes."""

    def __init__(self, callback: Callable[[], None], lock_filename: str):
        self.callback = callback
        self.lock_filename = lock_filename

    def on_modified(self, event):
        if event.src_path.endswith(self.lock_filename):
            self.callback()

    def on_created(self, event):
        if event.src_path.endswith(self.lock_filename):
            self.callback()

    def on_deleted(self, event):
        if event.src_path.endswith(self.lock_filename):
            self.callback()


class LeaderClient:
    """
    LeaderClient for meta-stremio.

    Reads leader info from kv-leader.info file written by meta-core.
    """

    def __init__(
        self,
        meta_core_path: str,
        meta_core_url: Optional[str] = None
    ):
        self.meta_core_path = meta_core_path
        self.meta_core_url = meta_core_url

        self.info_file_path = os.path.join(meta_core_path, 'locks', 'kv-leader.info')
        self.leader_info: Optional[LeaderLockInfo] = None

        self._file_observer = None
        self._on_change_callbacks: list[Callable[[], None]] = []
        self._stop_event = threading.Event()

        # URL caching
        self._cached_urls: Optional[URLsResponse] = None
        self._urls_cache_time: float = 0
        self._urls_cache_ttl: float = 5.0  # 5 seconds

    def _get_api_url_from_file(self) -> Optional[str]:
        """Read API URL from file (plain text format)."""
        try:
            with open(self.info_file_path, 'r') as f:
                content = f.read()
            return content.strip() or None
        except FileNotFoundError:
            print(f"[LeaderClient] Leader info file not found: {self.info_file_path}")
            return None
        except Exception as e:
            print(f"[LeaderClient] Error reading API URL from file: {e}")
            return None

    def _fetch_urls(self, api_url: str) -> Optional[URLsResponse]:
        """Fetch URLs from meta-core /urls API with caching."""
        import time

        # Check cache
        now = time.time()
        if self._cached_urls and (now - self._urls_cache_time) < self._urls_cache_ttl:
            return self._cached_urls

        try:
            url = f"{api_url}/urls"
            req = Request(url, headers={'Accept': 'application/json'})
            with urlopen(req, timeout=5) as response:
                if response.status != 200:
                    print(f"[LeaderClient] Failed to fetch URLs: {response.status}")
                    return None

                data = json.loads(response.read().decode())

                self._cached_urls = URLsResponse(
                    hostname=data.get('hostname', ''),
                    base_url=data.get('baseUrl', ''),
                    api_url=data.get('apiUrl', ''),
                    redis_url=data.get('redisUrl', ''),
                    webdav_url=data.get('webdavUrl', ''),
                    is_leader=data.get('isLeader', False)
                )
                self._urls_cache_time = now
                return self._cached_urls

        except URLError as e:
            print(f"[LeaderClient] Error calling /urls API: {e}")
            return None
        except Exception as e:
            print(f"[LeaderClient] Error fetching URLs: {e}")
            return None

    def get_leader_info(self) -> Optional[LeaderLockInfo]:
        """Read leader info from file and /urls API."""
        try:
            # Read API URL from file (plain text)
            api_url = self._get_api_url_from_file()
            if not api_url:
                return None

            # Fetch full info from /urls API
            urls = self._fetch_urls(api_url)
            if not urls:
                return None

            # Convert URLsResponse to LeaderLockInfo
            import time
            self.leader_info = LeaderLockInfo(
                hostname=urls.hostname,
                base_url=urls.base_url,
                api_url=urls.api_url,
                redis_url=urls.redis_url,
                webdav_url=urls.webdav_url,
                timestamp=int(time.time() * 1000),
                pid=0  # Unknown for remote leader
            )

            return self.leader_info

        except Exception as e:
            print(f"[LeaderClient] Error reading leader info: {e}")
            return None

    def get_redis_url(self) -> Optional[str]:
        """Get Redis URL from leader info."""
        info = self.get_leader_info()
        return info.redis_url if info else None

    def get_webdav_url(self) -> Optional[str]:
        """Get WebDAV URL from leader info."""
        info = self.get_leader_info()
        return info.webdav_url if info else None

    def get_api_url(self) -> Optional[str]:
        """Get meta-core API URL from leader info."""
        info = self.get_leader_info()
        return info.api_url if info else None

    def get_urls(self) -> Optional[URLsResponse]:
        """Call meta-core /urls API to get current URLs."""
        # First try using configured meta_core_url
        api_url = self.meta_core_url

        # Fall back to reading from file
        if not api_url:
            api_url = self._get_api_url_from_file()

        if not api_url:
            print("[LeaderClient] No meta-core API URL available")
            return None

        return self._fetch_urls(api_url)

    def wait_for_leader(self, timeout_ms: int = 30000) -> LeaderLockInfo:
        """Wait for leader info to be available."""
        import time
        start_time = time.time()
        poll_interval = 0.5  # 500ms

        while (time.time() - start_time) * 1000 < timeout_ms:
            info = self.get_leader_info()
            if info:
                print(f"[LeaderClient] Leader found: {info.hostname} at {info.redis_url}")
                return info

            print("[LeaderClient] Waiting for leader...")
            time.sleep(poll_interval)

        raise TimeoutError(f"No leader found within {timeout_ms}ms")

    def start_watching(self) -> None:
        """Start watching for leader changes."""
        if not WATCHDOG_AVAILABLE:
            print("[LeaderClient] watchdog not available, file watching disabled")
            return

        try:
            lock_dir = os.path.dirname(self.info_file_path)

            handler = LockFileHandler(
                callback=self._on_file_change,
                lock_filename='kv-leader.info'
            )

            self._file_observer = Observer()
            self._file_observer.schedule(handler, lock_dir, recursive=False)
            self._file_observer.start()
            print(f"[LeaderClient] Watching for leader changes in {lock_dir}")

        except Exception as e:
            print(f"[LeaderClient] Could not start file watcher: {e}")

    def stop_watching(self) -> None:
        """Stop watching for leader changes."""
        if self._file_observer:
            self._file_observer.stop()
            self._file_observer.join(timeout=2.0)
            self._file_observer = None
            print("[LeaderClient] Stopped watching for leader changes")

    def _on_file_change(self) -> None:
        """Handle file change event."""
        print("[LeaderClient] Leader info changed, invalidating cache...")
        # Invalidate cache to force fresh API call
        self._cached_urls = None
        self._urls_cache_time = 0

        self.get_leader_info()
        self._notify_change()

    def on_change(self, callback: Callable[[], None]) -> 'LeaderClient':
        """Register callback for leader changes."""
        self._on_change_callbacks.append(callback)
        return self

    def _notify_change(self) -> None:
        """Notify all change callbacks."""
        for callback in self._on_change_callbacks:
            try:
                callback()
            except Exception as e:
                print(f"[LeaderClient] Error in change callback: {e}")

    def get_cached_leader_info(self) -> Optional[LeaderLockInfo]:
        """Get cached leader info (without re-reading file)."""
        return self.leader_info

    def close(self) -> None:
        """Clean up resources."""
        self._stop_event.set()
        self.stop_watching()
        self._on_change_callbacks = []
