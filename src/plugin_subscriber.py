"""
Plugin Completion Subscriber for meta-stremio.

Subscribes to Redis pub/sub channel 'meta-sort:plugin:complete' to be notified
when specific plugins finish processing files. This allows Stremio to:
1. Log metadata updates for debugging
2. Invalidate any local caches (if implemented)
3. Track which files have complete metadata

Plugins of interest to Stremio:
- filename-parser: Provides season/episode info for TV vs Movie categorization
- tmdb: Provides rich metadata (title, year, poster, description, etc.)
"""
from __future__ import annotations

import os
import json
import threading
from typing import Callable, Optional, List, Set

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None


# Redis configuration
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')

# Plugins that Stremio cares about
WATCHED_PLUGINS: Set[str] = {'filename-parser', 'tmdb'}

# Channel for plugin completion events
PLUGIN_COMPLETE_CHANNEL = 'meta-sort:plugin:complete'


class PluginSubscriber:
    """
    Subscribes to plugin completion events from meta-sort.

    When filename-parser or tmdb plugins complete, the subscriber
    receives notifications that can be used to update Stremio's view
    of the metadata.
    """

    def __init__(
        self,
        url: str = None,
        watched_plugins: Set[str] = None,
        on_plugin_complete: Callable[[str, str, str], None] = None
    ):
        """
        Initialize the plugin subscriber.

        Args:
            url: Redis connection URL
            watched_plugins: Set of plugin IDs to watch (default: filename-parser, tmdb)
            on_plugin_complete: Callback when a watched plugin completes
                                Signature: (file_hash, plugin_id, file_path) -> None
        """
        if not REDIS_AVAILABLE:
            raise ImportError("redis package not installed. Run: pip install redis")

        self._url = url or REDIS_URL
        self._watched_plugins = watched_plugins or WATCHED_PLUGINS
        self._on_plugin_complete = on_plugin_complete
        self._pubsub: Optional[redis.client.PubSub] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._client: Optional[redis.Redis] = None

        # Track files with complete metadata (all watched plugins finished)
        self._file_plugin_status: dict[str, Set[str]] = {}

    def start(self) -> bool:
        """Start the subscriber in a background thread."""
        if self._running:
            return True

        try:
            self._client = redis.from_url(self._url, decode_responses=True)
            self._client.ping()  # Test connection

            self._pubsub = self._client.pubsub()
            self._pubsub.subscribe(PLUGIN_COMPLETE_CHANNEL)

            self._running = True
            self._thread = threading.Thread(
                target=self._listen_loop,
                daemon=True,
                name='PluginSubscriber'
            )
            self._thread.start()

            print(f"[PluginSubscriber] Started, watching plugins: {', '.join(self._watched_plugins)}")
            return True

        except Exception as e:
            print(f"[PluginSubscriber] Failed to start: {e}")
            self._running = False
            return False

    def stop(self) -> None:
        """Stop the subscriber."""
        self._running = False

        if self._pubsub:
            try:
                self._pubsub.unsubscribe()
                self._pubsub.close()
            except Exception:
                pass
            self._pubsub = None

        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

        print("[PluginSubscriber] Stopped")

    def _listen_loop(self) -> None:
        """Background loop that listens for plugin completion events."""
        while self._running and self._pubsub:
            try:
                message = self._pubsub.get_message(timeout=1.0)
                if message and message['type'] == 'message':
                    self._handle_message(message['data'])
            except redis.ConnectionError:
                print("[PluginSubscriber] Connection lost, attempting reconnect...")
                self._reconnect()
            except Exception as e:
                if self._running:
                    print(f"[PluginSubscriber] Error in listen loop: {e}")

    def _reconnect(self) -> None:
        """Attempt to reconnect to Redis."""
        import time

        for attempt in range(5):
            if not self._running:
                return

            try:
                time.sleep(2 ** attempt)  # Exponential backoff

                if self._pubsub:
                    self._pubsub.close()
                if self._client:
                    self._client.close()

                self._client = redis.from_url(self._url, decode_responses=True)
                self._client.ping()

                self._pubsub = self._client.pubsub()
                self._pubsub.subscribe(PLUGIN_COMPLETE_CHANNEL)

                print("[PluginSubscriber] Reconnected successfully")
                return

            except Exception as e:
                print(f"[PluginSubscriber] Reconnect attempt {attempt + 1} failed: {e}")

        print("[PluginSubscriber] Failed to reconnect after 5 attempts")
        self._running = False

    def _handle_message(self, data: str) -> None:
        """Handle a plugin completion message."""
        try:
            payload = json.loads(data)
            file_hash = payload.get('fileHash', '')
            plugin_id = payload.get('pluginId', '')
            file_path = payload.get('filePath', '')

            # Only process plugins we're watching
            if plugin_id not in self._watched_plugins:
                return

            # Track plugin completion for this file
            if file_hash not in self._file_plugin_status:
                self._file_plugin_status[file_hash] = set()
            self._file_plugin_status[file_hash].add(plugin_id)

            # Log the completion
            print(f"[PluginSubscriber] Plugin '{plugin_id}' completed for {file_hash[:12]}...")

            # Check if all watched plugins have completed for this file
            completed_plugins = self._file_plugin_status[file_hash]
            if self._watched_plugins.issubset(completed_plugins):
                print(f"[PluginSubscriber] All watched plugins complete for {file_hash[:12]}...")
                # Could trigger a cache refresh here if we had local caching

            # Call the callback if provided
            if self._on_plugin_complete:
                try:
                    self._on_plugin_complete(file_hash, plugin_id, file_path)
                except Exception as e:
                    print(f"[PluginSubscriber] Callback error: {e}")

        except json.JSONDecodeError:
            print(f"[PluginSubscriber] Invalid JSON message: {data[:100]}")
        except Exception as e:
            print(f"[PluginSubscriber] Error handling message: {e}")

    def is_running(self) -> bool:
        """Check if the subscriber is running."""
        return self._running

    def get_status(self) -> dict:
        """Get subscriber status for debugging."""
        return {
            'running': self._running,
            'watched_plugins': list(self._watched_plugins),
            'tracked_files': len(self._file_plugin_status),
        }


# Global subscriber instance
_subscriber: Optional[PluginSubscriber] = None


def init_subscriber(
    on_plugin_complete: Callable[[str, str, str], None] = None
) -> Optional[PluginSubscriber]:
    """
    Initialize and start the global plugin subscriber.

    Args:
        on_plugin_complete: Optional callback when watched plugins complete

    Returns:
        The subscriber instance, or None if Redis is unavailable
    """
    global _subscriber

    if not REDIS_AVAILABLE:
        print("[PluginSubscriber] Redis not available, skipping subscriber init")
        return None

    if _subscriber and _subscriber.is_running():
        return _subscriber

    _subscriber = PluginSubscriber(on_plugin_complete=on_plugin_complete)
    if _subscriber.start():
        return _subscriber

    _subscriber = None
    return None


def get_subscriber() -> Optional[PluginSubscriber]:
    """Get the global subscriber instance."""
    return _subscriber


def stop_subscriber() -> None:
    """Stop the global subscriber."""
    global _subscriber
    if _subscriber:
        _subscriber.stop()
        _subscriber = None
