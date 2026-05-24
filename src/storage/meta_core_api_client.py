"""
MetaCoreApiClient — HTTP read client against meta-core.

Mirrors the subset of meta-stremio's storage-read surface that previously
spoke ioredis directly. Backs the LeaderStorage when meta-core's leader info
omits redis_url (post api-mediated-access PR D), so the addon keeps working
without direct Redis access.

Endpoints used:
  - GET /meta                  → enumerate hashIds
  - GET /meta/{hash}           → flat metadata map for one file
  - GET /meta/{hash}/{key}     → single property (text/plain)
  - GET /api/file/{cid}/info   → resolve CID to file path
  - GET /health                → connectivity probe
"""
from __future__ import annotations

import json
import logging
from typing import Optional, List, Dict
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)


class MetaCoreApiClient:
    """HTTP client for meta-core's /meta/* read surface."""

    def __init__(self, api_url: str, timeout: int = 30):
        self._api_url = api_url.rstrip('/')
        self._timeout = timeout

    # ----- enumeration ---------------------------------------------------

    def list_hash_ids(self) -> List[str]:
        """GET /meta — returns every hashId in file:__index__."""
        body = self._get_json('/meta')
        if not body:
            return []
        ids = body.get('hashIds')
        return ids if isinstance(ids, list) else []

    def count_hash_ids(self) -> int:
        body = self._get_json('/meta')
        if not body:
            return 0
        return int(body.get('count', 0))

    # ----- per-file -----------------------------------------------------

    def get_metadata_flat(self, hash_id: str) -> Optional[Dict[str, str]]:
        """GET /meta/{hash}. Returns the flat property map or None on 404."""
        body = self._get_json(f'/meta/{quote(hash_id, safe="")}', allow_404=True)
        if not body:
            return None
        meta = body.get('metadata')
        return meta if isinstance(meta, dict) else None

    def get_property(self, hash_id: str, key: str) -> Optional[str]:
        """GET /meta/{hash}/{key}. meta-core returns text/plain (or 404)."""
        path = f'/meta/{quote(hash_id, safe="")}/{quote(key, safe="/")}'
        url = f"{self._api_url}{path}"
        try:
            req = Request(url, headers={'Accept': 'text/plain'})
            with urlopen(req, timeout=self._timeout) as resp:
                text = resp.read().decode('utf-8', errors='replace')
                return text if text != '' else None
        except HTTPError as e:
            if e.code == 404:
                return None
            logger.warning("[MetaCoreApiClient] GET %s → %s", path, e)
            return None
        except URLError as e:
            logger.warning("[MetaCoreApiClient] GET %s failed: %s", path, e)
            return None

    # ----- CID resolution -----------------------------------------------

    def resolve_file_path(self, cid: str) -> Optional[str]:
        """GET /api/file/{cid}/info → relative path (or None on miss)."""
        path = f'/api/file/{quote(cid, safe="")}/info'
        body = self._get_json(path)
        if not body or not body.get('exists'):
            return None
        return body.get('filePath') or None

    # ----- health -------------------------------------------------------

    def health(self) -> bool:
        try:
            req = Request(f"{self._api_url}/health", headers={'Accept': 'application/json'})
            with urlopen(req, timeout=min(self._timeout, 5)) as resp:
                return resp.status == 200
        except Exception:
            return False

    # ----- internals ----------------------------------------------------

    def _get_json(self, path: str, allow_404: bool = False) -> Optional[dict]:
        url = f"{self._api_url}{path}"
        try:
            req = Request(url, headers={'Accept': 'application/json'})
            with urlopen(req, timeout=self._timeout) as resp:
                raw = resp.read().decode('utf-8', errors='replace')
                if not raw:
                    return None
                return json.loads(raw)
        except HTTPError as e:
            if allow_404 and e.code == 404:
                return None
            logger.warning("[MetaCoreApiClient] GET %s → %s", path, e)
            return None
        except URLError as e:
            logger.warning("[MetaCoreApiClient] GET %s failed: %s", path, e)
            return None
        except json.JSONDecodeError as e:
            logger.warning("[MetaCoreApiClient] GET %s — invalid JSON: %s", path, e)
            return None
