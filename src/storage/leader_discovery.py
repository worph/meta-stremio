"""
Leader Discovery for meta-stremio (FOLLOWER-only)

This is a Python implementation of the leader discovery pattern used in meta-fuse.
meta-stremio never spawns Redis - it only connects to the leader's Redis instance.

Architecture:
- Reads info file at META_CORE_PATH/locks/kv-leader.info
- The leader holds flock on kv-leader.lock, writes metadata to kv-leader.info
- Parses leader info (Redis URL, HTTP URL)
- Watches for leader changes via polling and file watching
- Triggers reconnection callbacks on leader failure/change
"""
from __future__ import annotations

import os
import json
import time
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Callable
from urllib.request import urlopen
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
    """Leader lock file content format."""
    host: str
    api: str  # Redis connection URL (e.g., redis://10.0.1.50:6379)
    http: str  # HTTP API URL for the leader service
    timestamp: int
    pid: int


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


class LeaderDiscovery:
    """
    Leader Discovery for meta-stremio.

    Discovers and monitors the KV leader via a lock file in the shared filesystem.
    """

    def __init__(
        self,
        meta_core_path: str,
        health_check_interval: float = 5.0,
        max_failures: int = 3
    ):
        self.meta_core_path = meta_core_path
        self.health_check_interval = health_check_interval
        self.max_failures = max_failures

        self.lock_file_path = os.path.join(meta_core_path, 'locks', 'kv-leader.info')
        self.leader_info: Optional[LeaderLockInfo] = None

        self._is_started = False
        self._stop_event = threading.Event()
        self._health_check_thread: Optional[threading.Thread] = None
        self._file_observer = None
        self._consecutive_failures = 0

        # Event callbacks
        self._on_leader_found_callback: Optional[Callable[[LeaderLockInfo], None]] = None
        self._on_leader_lost_callback: Optional[Callable[[], None]] = None

    def start(self) -> None:
        """Start watching for leader."""
        if self._is_started:
            print("[LeaderDiscovery] Already started")
            return

        print(f"[LeaderDiscovery] Starting, lock file: {self.lock_file_path}")

        # Ensure lock directory exists
        self._ensure_lock_dir()

        # Try to read existing leader info
        self._check_for_leader()

        # Start file watcher (if watchdog is available)
        self._start_file_watcher()

        # Start health check thread
        self._start_health_check()

        self._is_started = True

    def stop(self) -> None:
        """Stop watching."""
        if not self._is_started:
            return

        print("[LeaderDiscovery] Stopping...")

        self._stop_event.set()

        # Stop health check thread
        if self._health_check_thread and self._health_check_thread.is_alive():
            self._health_check_thread.join(timeout=2.0)

        # Stop file observer
        if self._file_observer:
            self._file_observer.stop()
            self._file_observer.join(timeout=2.0)
            self._file_observer = None

        self._is_started = False

    def _ensure_lock_dir(self) -> None:
        """Ensure lock directory exists."""
        lock_dir = os.path.dirname(self.lock_file_path)
        try:
            os.makedirs(lock_dir, exist_ok=True)
        except OSError as e:
            print(f"[LeaderDiscovery] Could not create lock directory: {e}")

    def _check_for_leader(self) -> None:
        """Check for leader by reading lock file."""
        try:
            with open(self.lock_file_path, 'r') as f:
                content = f.read()

            data = json.loads(content)

            # Validate leader info
            if data and data.get('api') and data.get('http'):
                info = LeaderLockInfo(
                    host=data.get('host', 'unknown'),
                    api=data['api'],
                    http=data['http'],
                    timestamp=data.get('timestamp', 0),
                    pid=data.get('pid', 0)
                )

                is_new = (
                    self.leader_info is None or
                    self.leader_info.api != info.api
                )

                self.leader_info = info
                self._consecutive_failures = 0

                if is_new:
                    print(f"[LeaderDiscovery] Leader found: {info.host} at {info.api}")
                    if self._on_leader_found_callback:
                        self._on_leader_found_callback(info)

        except FileNotFoundError:
            # Lock file doesn't exist - no leader yet
            if self.leader_info:
                print("[LeaderDiscovery] Lock file removed, leader lost")
                self._handle_leader_lost()

        except json.JSONDecodeError as e:
            print(f"[LeaderDiscovery] Error parsing lock file: {e}")

        except Exception as e:
            print(f"[LeaderDiscovery] Error reading lock file: {e}")

    def _is_leader_healthy(self) -> bool:
        """Check if current leader is still healthy."""
        if not self.leader_info:
            return False

        try:
            health_url = f"{self.leader_info.http}/health"
            with urlopen(health_url, timeout=5) as response:
                return response.status == 200
        except (URLError, TimeoutError, Exception):
            return False

    def _handle_leader_lost(self) -> None:
        """Handle leader being lost."""
        self.leader_info = None
        if self._on_leader_lost_callback:
            self._on_leader_lost_callback()

    def _start_file_watcher(self) -> None:
        """Start watching lock file for changes."""
        if not WATCHDOG_AVAILABLE:
            print("[LeaderDiscovery] watchdog not available, using polling only")
            return

        try:
            lock_dir = os.path.dirname(self.lock_file_path)
            handler = LockFileHandler(
                callback=self._check_for_leader,
                lock_filename='kv-leader.info'
            )

            self._file_observer = Observer()
            self._file_observer.schedule(handler, lock_dir, recursive=False)
            self._file_observer.start()
            print(f"[LeaderDiscovery] File watcher started for {lock_dir}")

        except Exception as e:
            print(f"[LeaderDiscovery] Could not start file watcher: {e}")

    def _start_health_check(self) -> None:
        """Start health check loop."""
        def health_check_loop():
            while not self._stop_event.is_set():
                try:
                    if not self.leader_info:
                        # No leader known, try to find one
                        self._check_for_leader()
                    else:
                        # Check if current leader is healthy
                        healthy = self._is_leader_healthy()

                        if healthy:
                            self._consecutive_failures = 0
                        else:
                            self._consecutive_failures += 1
                            print(f"[LeaderDiscovery] Leader health check failed ({self._consecutive_failures}/{self.max_failures})")

                            if self._consecutive_failures >= self.max_failures:
                                print("[LeaderDiscovery] Leader appears dead, marking as lost")
                                self._handle_leader_lost()

                                # Try to find new leader
                                self._check_for_leader()

                except Exception as e:
                    print(f"[LeaderDiscovery] Health check error: {e}")

                self._stop_event.wait(self.health_check_interval)

        self._health_check_thread = threading.Thread(
            target=health_check_loop,
            daemon=True,
            name="leader-health-check"
        )
        self._health_check_thread.start()

    # ========================================================================
    # Event Registration
    # ========================================================================

    def on_leader_found(self, callback: Callable[[LeaderLockInfo], None]) -> 'LeaderDiscovery':
        """Register callback for when leader is found/changed."""
        self._on_leader_found_callback = callback

        # If we already have leader info, call immediately
        if self.leader_info:
            callback(self.leader_info)

        return self

    def on_leader_lost(self, callback: Callable[[], None]) -> 'LeaderDiscovery':
        """Register callback for when leader is lost."""
        self._on_leader_lost_callback = callback
        return self

    # ========================================================================
    # Getters
    # ========================================================================

    def get_leader_info(self) -> Optional[LeaderLockInfo]:
        """Get current leader info."""
        return self.leader_info

    def get_redis_url(self) -> Optional[str]:
        """Get Redis URL of current leader."""
        return self.leader_info.api if self.leader_info else None

    def has_leader(self) -> bool:
        """Check if a leader is known."""
        return self.leader_info is not None
