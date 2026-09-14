"""LeaderClient — locates meta-core and exposes its URLs.

Since meta-discovery v1 this is a thin adapter over ``MetaCoreLocator``
(``meshdisco.py``): meta-core is found by UDP announce instead of by reading
``/meta-core/locks/kv-leader.info``, so meta-stremio no longer needs the
``/meta-core`` volume mounted at all.

The public surface is deliberately unchanged so ``LeaderStorage`` did not have
to move: ``get_leader_info``, ``get_urls``, ``get_api_url``,
``get_webdav_url*``, ``wait_for_leader``, ``start_watching`` / ``on_change``
(the watchdog on the lock directory became "an announce arrived carrying a
different apiUrl", feeding the same callback).

Precedence is the pin: when ``meta_core_url`` (``META_CORE_URL``) is set it
always wins and the wire is never consulted for core selection. See
docs/project-architecture/service-discovery.md.

The name "leader" is legacy: meta-core's flock election is vestigial, and
``redis_url`` has been empty since the api-mediated-access lockdown — every
read goes over HTTP.
"""

import json
import os
import time
import urllib.request
from dataclasses import dataclass
from typing import Callable, List, Optional

from .meshdisco import MetaCoreLocator


@dataclass
class LeaderLockInfo:
    """meta-core's identity and URLs.

    Historically the parsed kv-leader.info + /urls pair; now assembled from the
    announce (or from /urls when meta-core is pinned).
    """
    hostname: str
    base_url: str
    api_url: str      # meta-core API URL (port 9000)
    redis_url: str
    webdav_url: str           # External WebDAV URL (via nginx/HTTPS)
    webdav_url_internal: str  # Internal WebDAV URL (direct to port 9000)
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
    webdav_url_internal: str
    is_leader: bool


class LeaderClient:
    """Locates meta-core over UDP and resolves its URL set."""

    def __init__(
        self,
        meta_core_path: Optional[str] = None,
        meta_core_url: Optional[str] = None,
        service_name: str = "meta-stremio",
        base_url: Optional[str] = None,
        version: str = "",
    ) -> None:
        # meta_core_path is accepted and ignored — kept so existing callers
        # construct unchanged. Nothing reads the volume any more.
        self._meta_core_path = meta_core_path
        self._meta_core_url = (meta_core_url or "").strip() or None
        self._leader_info: Optional[LeaderLockInfo] = None
        self._on_change_callbacks: List[Callable[[], None]] = []
        self._started = False

        # Only used on the pinned path, where the URLs still come from an HTTP
        # GET /urls. A discovered core carries them in the announce itself.
        self._cached_urls: Optional[URLsResponse] = None
        self._urls_cache_time: float = 0.0
        self._urls_cache_ttl: float = 5.0

        self._locator = MetaCoreLocator(
            service_name=service_name,
            base_url=base_url,
            version=version,
            meta_core_url=self._meta_core_url,
        )
        self._locator.on_change(self._on_locator_change)

    # -- lifecycle ---------------------------------------------------------

    def _ensure_started(self) -> None:
        if self._started:
            return
        self._started = True
        self._locator.start()

    def _on_locator_change(self) -> None:
        self._cached_urls = None
        self._urls_cache_time = 0.0
        self._notify_change()

    # -- url resolution ----------------------------------------------------

    def _assert_no_redis_url(self, redis_url: Optional[str]) -> None:
        """Guard against a rollback reintroducing direct Redis exposure.

        api-mediated-access PR D removed redis_url; ALLOW_LEGACY_REDIS_URL=1
        downgrades this to a warning during a deliberate temporary rollback.
        """
        if not redis_url:
            return
        msg = (
            "meta-core still publishes redisUrl; direct Redis access was retired "
            "by the api-mediated-access lockdown. Verify meta-core version."
        )
        if os.environ.get("ALLOW_LEGACY_REDIS_URL") == "1":
            print(f"[LeaderClient] WARNING: {msg}")
        else:
            raise RuntimeError(msg)

    def _fetch_urls(self, api_url: str) -> Optional[URLsResponse]:
        now = time.time()
        if self._cached_urls and (now - self._urls_cache_time) < self._urls_cache_ttl:
            return self._cached_urls
        try:
            req = urllib.request.Request(
                f"{api_url.rstrip('/')}/urls", headers={"Accept": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            self._assert_no_redis_url(data.get("redisUrl"))
            self._cached_urls = URLsResponse(
                hostname=data.get("hostname", ""),
                base_url=data.get("baseUrl", ""),
                api_url=data.get("apiUrl", ""),
                redis_url=data.get("redisUrl", "") or "",
                webdav_url=data.get("webdavUrl", ""),
                webdav_url_internal=data.get("webdavUrlInternal", ""),
                is_leader=bool(data.get("isLeader", True)),
            )
            self._urls_cache_time = now
            return self._cached_urls
        except Exception as e:
            print(f"[LeaderClient] Error calling /urls API: {e}")
            return None

    def get_urls(self) -> Optional[URLsResponse]:
        """Resolve meta-core's URLs.

        Straight from the announce when discovered (no HTTP hop needed); over
        HTTP when meta-core is pinned.
        """
        self._ensure_started()

        wire = self._locator.get_urls()
        if wire and not self._meta_core_url:
            return URLsResponse(
                hostname=wire.get("hostname", ""),
                base_url=wire.get("baseUrl", ""),
                api_url=wire.get("apiUrl", ""),
                redis_url="",
                webdav_url=wire.get("webdavUrl", ""),
                webdav_url_internal=wire.get("webdavUrlInternal", ""),
                is_leader=True,
            )

        api_url = self._locator.get_api_url()
        if not api_url:
            return None
        return self._fetch_urls(api_url)

    def get_leader_info(self) -> Optional[LeaderLockInfo]:
        try:
            urls = self.get_urls()
            if not urls:
                return None
            self._leader_info = LeaderLockInfo(
                hostname=urls.hostname,
                base_url=urls.base_url,
                api_url=urls.api_url,
                redis_url=urls.redis_url,
                webdav_url=urls.webdav_url,
                webdav_url_internal=urls.webdav_url_internal,
                timestamp=int(time.time() * 1000),
                pid=0,
            )
            return self._leader_info
        except Exception as e:
            print(f"[LeaderClient] Failed to read leader info: {e}")
            return None

    def get_redis_url(self) -> Optional[str]:
        info = self.get_leader_info()
        return info.redis_url if info else None

    def get_webdav_url(self) -> Optional[str]:
        info = self.get_leader_info()
        return info.webdav_url if info else None

    def get_webdav_url_internal(self) -> Optional[str]:
        """Internal WebDAV URL — use for container-to-container access."""
        info = self.get_leader_info()
        return info.webdav_url_internal if info else None

    def get_api_url(self) -> Optional[str]:
        info = self.get_leader_info()
        return info.api_url if info else None

    def wait_for_leader(self, timeout_ms: int = 30000) -> LeaderLockInfo:
        """Block until meta-core is reachable.

        Probes immediately then every 500ms, matching the previous
        file-polling loop's timing.
        """
        self._ensure_started()
        timeout_s = None if timeout_ms <= 0 else timeout_ms / 1000.0
        deadline = None if timeout_s is None else time.time() + timeout_s

        if self._locator.wait_for_core(timeout_s) is None:
            raise TimeoutError(f"No meta-core found within {timeout_ms}ms")

        while deadline is None or time.time() < deadline:
            info = self.get_leader_info()
            if info:
                print(f"[LeaderClient] meta-core found: {info.hostname} at {info.api_url}")
                return info
            time.sleep(0.5)
        raise TimeoutError(f"No meta-core found within {timeout_ms}ms")

    # -- change notification ----------------------------------------------

    def start_watching(self) -> None:
        """Previously a watchdog on the lock directory.

        Discovery is always listening, so this only has to make sure the node
        is running.
        """
        self._ensure_started()

    def stop_watching(self) -> None:
        """No-op: the locator keeps listening until close()."""

    def on_change(self, callback: Callable[[], None]) -> "LeaderClient":
        self._on_change_callbacks.append(callback)
        return self

    def _notify_change(self) -> None:
        print("[LeaderClient] meta-core changed, invalidating cache...")
        for cb in list(self._on_change_callbacks):
            try:
                cb()
            except Exception as e:
                print(f"[LeaderClient] Error in change callback: {e}")

    def get_cached_leader_info(self) -> Optional[LeaderLockInfo]:
        return self._leader_info

    # -- nav menu ----------------------------------------------------------

    def neighbors(self) -> List[dict]:
        """Neighbours for the nav menu (one row per service name)."""
        self._ensure_started()
        return self._locator.neighbors()

    def self_announce(self) -> dict:
        """This service's own announce, so the menu can show itself."""
        self._ensure_started()
        return self._locator.self_announce()

    def close(self) -> None:
        self._on_change_callbacks = []
        self._locator.stop()
        self._started = False


_leader_client: Optional[LeaderClient] = None


def get_leader_client() -> Optional[LeaderClient]:
    """Process-wide singleton, so one UDP socket serves every caller."""
    global _leader_client
    if _leader_client is None:
        _leader_client = LeaderClient(
            meta_core_path=os.environ.get("META_CORE_PATH"),
            meta_core_url=os.environ.get("META_CORE_URL"),
            # PUBLIC_URL (a browser-reachable debug-direct port) wins over
            # BASE_URL (the Caddy URL), same ladder as meta-core. Announcing a
            # Caddy URL on a stack with no Caddy puts a dead link in every menu.
            base_url=os.environ.get("PUBLIC_URL") or os.environ.get("BASE_URL") or None,
        )
    return _leader_client
