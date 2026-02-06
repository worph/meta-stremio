"""
WebDAV Client for Meta-Stremio

Provides HTTP-based file access to meta-core's WebDAV server.
Files are accessed exclusively via WebDAV - no local filesystem access.

The WebDAV URL is discovered automatically from the leader via LeaderStorage.
"""
from __future__ import annotations

import os
from typing import Optional, Generator
from urllib.parse import quote

import requests

# Configuration - set dynamically by LeaderStorage after leader discovery
WEBDAV_URL: str = ''
FILES_PATH = os.environ.get('FILES_PATH', '/files')


def configure(webdav_url: str) -> None:
    """
    Configure the WebDAV client with a URL discovered from leader.

    Called by LeaderStorage after connecting to the leader.

    Args:
        webdav_url: The WebDAV base URL (e.g., http://meta-core/webdav)
    """
    global WEBDAV_URL

    if not webdav_url:
        return

    WEBDAV_URL = webdav_url.rstrip('/')
    print(f"[webdav-client] Configured: {WEBDAV_URL}")


def is_configured() -> bool:
    """Check if WebDAV client is configured."""
    return bool(WEBDAV_URL)


def to_webdav_url(file_path: str) -> Optional[str]:
    """
    Convert a local file path to a WebDAV URL.

    Args:
        file_path: Local file path (e.g., /files/watch/movie.mp4)

    Returns:
        WebDAV URL or None if not configured
    """
    if not WEBDAV_URL:
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
        file_path: File path

    Returns:
        File size in bytes, or None on error
    """
    url = to_webdav_url(file_path)
    if not url:
        return None

    try:
        response = requests.head(url, timeout=30)
        if response.status_code == 200:
            return int(response.headers.get('Content-Length', 0))
        return None
    except Exception as e:
        print(f"[webdav-client] HEAD error for {url}: {e}")
        return None


def file_exists(file_path: str) -> bool:
    """
    Check if file exists via HTTP HEAD request to WebDAV.

    Args:
        file_path: File path

    Returns:
        True if file exists
    """
    url = to_webdav_url(file_path)
    if not url:
        return False

    try:
        response = requests.head(url, timeout=30)
        return response.status_code == 200
    except Exception:
        return False


def read_file(file_path: str) -> Optional[bytes]:
    """
    Read entire file from WebDAV.

    Args:
        file_path: File path

    Returns:
        File contents as bytes, or None on error
    """
    url = to_webdav_url(file_path)
    if not url:
        return None

    try:
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            return response.content
        return None
    except Exception as e:
        print(f"[webdav-client] GET error for {url}: {e}")
        return None


def read_range(file_path: str, start: int, end: int) -> Optional[bytes]:
    """
    Read a byte range from a file via WebDAV.

    Args:
        file_path: File path
        start: Start byte offset (inclusive)
        end: End byte offset (inclusive)

    Returns:
        Bytes in the specified range, or None on error
    """
    url = to_webdav_url(file_path)
    if not url:
        return None

    try:
        headers = {'Range': f'bytes={start}-{end}'}
        response = requests.get(url, headers=headers, timeout=60)
        if response.status_code in (200, 206):
            return response.content
        return None
    except Exception as e:
        print(f"[webdav-client] Range GET error for {url}: {e}")
        return None


def stream_file(file_path: str, chunk_size: int = 64 * 1024) -> Generator[bytes, None, None]:
    """
    Stream file content from WebDAV.

    Args:
        file_path: File path
        chunk_size: Size of chunks to yield

    Yields:
        File content in chunks
    """
    url = to_webdav_url(file_path)
    if not url:
        return

    try:
        response = requests.get(url, stream=True, timeout=300)
        if response.status_code == 200:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    yield chunk
    except Exception as e:
        print(f"[webdav-client] Stream error for {url}: {e}")


def stream_range(file_path: str, start: int, end: int, file_size: int,
                 chunk_size: int = 64 * 1024) -> Generator[bytes, None, None]:
    """
    Stream a byte range from a file via WebDAV.

    Args:
        file_path: File path
        start: Start byte offset (inclusive)
        end: End byte offset (inclusive)
        file_size: Total file size (unused, kept for API compatibility)
        chunk_size: Size of chunks to yield

    Yields:
        File content in chunks
    """
    url = to_webdav_url(file_path)
    if not url:
        return

    try:
        headers = {'Range': f'bytes={start}-{end}'}
        response = requests.get(url, headers=headers, stream=True, timeout=300)
        if response.status_code in (200, 206):
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    yield chunk
    except Exception as e:
        print(f"[webdav-client] Stream range error for {url}: {e}")
