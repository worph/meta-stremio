"""
WebDAV Client for Meta-Stremio

Provides HTTP-based file access to meta-sort's WebDAV server.
This allows meta-stremio to access files without direct volume mounts.

Environment Variables:
- META_SORT_WEBDAV_URL: Base URL for WebDAV access (e.g., http://meta-sort/webdav)
- FILES_PATH: Local files path prefix to strip when building WebDAV URLs (default: /files)
"""
from __future__ import annotations

import os
import re
from typing import Optional, Generator
from urllib.parse import quote

import requests

# Configuration
WEBDAV_URL = os.environ.get('META_SORT_WEBDAV_URL', '').rstrip('/')
FILES_PATH = os.environ.get('FILES_PATH', '/files')

# Check if WebDAV is enabled
WEBDAV_ENABLED = bool(WEBDAV_URL)

if WEBDAV_ENABLED:
    print(f"[WebDAV] Using meta-sort WebDAV at {WEBDAV_URL}")
else:
    print("[WebDAV] Not configured, using direct filesystem access")


def to_webdav_url(file_path: str) -> Optional[str]:
    """
    Convert a local file path to a WebDAV URL.

    Args:
        file_path: Local file path (e.g., /files/watch/movie.mp4)

    Returns:
        WebDAV URL or None if WebDAV is not configured
    """
    if not WEBDAV_ENABLED:
        return None

    # Strip the FILES_PATH prefix to get relative path
    relative_path = file_path
    if file_path.startswith(FILES_PATH + '/'):
        relative_path = file_path[len(FILES_PATH):]
    elif file_path.startswith(FILES_PATH):
        relative_path = file_path[len(FILES_PATH):]

    # Ensure path starts with /
    if not relative_path.startswith('/'):
        relative_path = '/' + relative_path

    # URL-encode path segments (but not slashes)
    encoded_path = '/'.join(quote(segment, safe='') for segment in relative_path.split('/'))

    return WEBDAV_URL + encoded_path


def get_file_size(file_path: str) -> Optional[int]:
    """
    Get file size via HTTP HEAD request to WebDAV.

    Args:
        file_path: Local file path

    Returns:
        File size in bytes, or None on error
    """
    url = to_webdav_url(file_path)
    if not url:
        # Fall back to local filesystem
        try:
            return os.path.getsize(file_path)
        except OSError:
            return None

    try:
        response = requests.head(url, timeout=30)
        if response.status_code == 200:
            return int(response.headers.get('Content-Length', 0))
        return None
    except Exception as e:
        print(f"[WebDAV] HEAD error for {url}: {e}")
        return None


def file_exists(file_path: str) -> bool:
    """
    Check if file exists via HTTP HEAD request to WebDAV.

    Args:
        file_path: Local file path

    Returns:
        True if file exists
    """
    url = to_webdav_url(file_path)
    if not url:
        # Fall back to local filesystem
        return os.path.exists(file_path)

    try:
        response = requests.head(url, timeout=30)
        return response.status_code == 200
    except Exception:
        return False


def read_file(file_path: str) -> Optional[bytes]:
    """
    Read entire file from WebDAV.

    Args:
        file_path: Local file path

    Returns:
        File contents as bytes, or None on error
    """
    url = to_webdav_url(file_path)
    if not url:
        # Fall back to local filesystem
        try:
            with open(file_path, 'rb') as f:
                return f.read()
        except OSError:
            return None

    try:
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            return response.content
        return None
    except Exception as e:
        print(f"[WebDAV] GET error for {url}: {e}")
        return None


def read_range(file_path: str, start: int, end: int) -> Optional[bytes]:
    """
    Read a byte range from a file via WebDAV.

    Args:
        file_path: Local file path
        start: Start byte offset (inclusive)
        end: End byte offset (inclusive)

    Returns:
        Bytes in the specified range, or None on error
    """
    url = to_webdav_url(file_path)
    if not url:
        # Fall back to local filesystem
        try:
            with open(file_path, 'rb') as f:
                f.seek(start)
                return f.read(end - start + 1)
        except OSError:
            return None

    try:
        headers = {'Range': f'bytes={start}-{end}'}
        response = requests.get(url, headers=headers, timeout=60)
        if response.status_code in (200, 206):
            return response.content
        return None
    except Exception as e:
        print(f"[WebDAV] Range GET error for {url}: {e}")
        return None


def stream_file(file_path: str, chunk_size: int = 64 * 1024) -> Generator[bytes, None, None]:
    """
    Stream file content from WebDAV.

    Args:
        file_path: Local file path
        chunk_size: Size of chunks to yield

    Yields:
        File content in chunks
    """
    url = to_webdav_url(file_path)
    if not url:
        # Fall back to local filesystem
        try:
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk
        except OSError:
            return
    else:
        try:
            response = requests.get(url, stream=True, timeout=300)
            if response.status_code == 200:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        yield chunk
        except Exception as e:
            print(f"[WebDAV] Stream error for {url}: {e}")


def stream_range(file_path: str, start: int, end: int, file_size: int,
                 chunk_size: int = 64 * 1024) -> Generator[bytes, None, None]:
    """
    Stream a byte range from a file via WebDAV.

    Args:
        file_path: Local file path
        start: Start byte offset (inclusive)
        end: End byte offset (inclusive)
        file_size: Total file size (for local fallback)
        chunk_size: Size of chunks to yield

    Yields:
        File content in chunks
    """
    url = to_webdav_url(file_path)
    if not url:
        # Fall back to local filesystem
        try:
            with open(file_path, 'rb') as f:
                f.seek(start)
                remaining = end - start + 1
                while remaining > 0:
                    chunk = f.read(min(chunk_size, remaining))
                    if not chunk:
                        break
                    yield chunk
                    remaining -= len(chunk)
        except OSError:
            return
    else:
        try:
            headers = {'Range': f'bytes={start}-{end}'}
            response = requests.get(url, headers=headers, stream=True, timeout=300)
            if response.status_code in (200, 206):
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        yield chunk
        except Exception as e:
            print(f"[WebDAV] Stream range error for {url}: {e}")
