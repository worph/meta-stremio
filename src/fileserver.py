"""
File Server API for Meta-Stremio

Serves files by CID by looking up the path from metadata.
This is a generic file serving endpoint that can serve any file
whose CID and path are stored in the metadata.

Endpoints:
- GET /file/{cid} - Serve file by CID
- GET /file/{cid}/w{width} - Serve image resized to width (images only)
"""
from __future__ import annotations

import os
import io
from typing import Optional, Tuple

import webdav_client

# Optional PIL for image resizing
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    Image = None

# Configuration
FILES_PATH = os.environ.get('FILES_PATH', '/files')

# Storage reference (set by init)
_storage = None


def init(storage) -> None:
    """Initialize with storage reference."""
    global _storage
    _storage = storage


def lookup_path_by_cid(cid: str) -> Optional[str]:
    """
    Look up a file path by its CID from Redis.

    Files (including poster images) are stored in Redis as file:{cid} entries.
    The file path is stored in the 'path' field.

    Returns the relative path if found, None otherwise.
    """
    if not _storage:
        return None

    try:
        # Look up directly by CID - poster files are processed like any other file
        file_path = _storage.get_file_path_by_cid(cid)
        if file_path:
            return file_path
        return None
    except Exception as e:
        print(f"[FileServer] Error looking up CID {cid}: {e}")
        return None


def get_file_path(cid: str) -> Optional[str]:
    """Get the absolute file path for a CID."""
    rel_path = lookup_path_by_cid(cid)
    if rel_path:
        return os.path.join(FILES_PATH, rel_path)
    return None


def resize_image(image_data: bytes, width: int) -> bytes:
    """Resize an image to the specified width, maintaining aspect ratio."""
    if not PIL_AVAILABLE:
        raise RuntimeError("PIL not available for image resizing")

    img = Image.open(io.BytesIO(image_data))

    # Calculate new height maintaining aspect ratio
    ratio = width / img.width
    height = int(img.height * ratio)

    # Use high-quality resampling
    resized = img.resize((width, height), Image.Resampling.LANCZOS)

    # Save to bytes
    output = io.BytesIO()
    # Preserve format, default to JPEG
    fmt = img.format or 'JPEG'
    if fmt.upper() == 'PNG' and img.mode == 'RGBA':
        resized.save(output, format='PNG', optimize=True)
    else:
        # Convert to RGB for JPEG
        if resized.mode in ('RGBA', 'P'):
            resized = resized.convert('RGB')
        resized.save(output, format='JPEG', quality=85, optimize=True)

    return output.getvalue()


def serve_file(cid: str, width: Optional[int] = None) -> Tuple[Optional[bytes], str, int]:
    """
    Serve a file by CID.

    Args:
        cid: The CID of the file
        width: Optional width for resizing (images only)

    Returns:
        Tuple of (file_data, content_type, status_code)
    """
    # Get file path from CID
    file_path = get_file_path(cid)

    if not file_path:
        return None, 'text/plain', 404

    # Check if file exists (works with both local and WebDAV)
    if not webdav_client.file_exists(file_path):
        print(f"[FileServer] File not found: {file_path}")
        return None, 'text/plain', 404

    # Determine content type from extension
    ext = os.path.splitext(file_path)[1].lower()
    content_types = {
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
        '.webp': 'image/webp',
        '.gif': 'image/gif',
        '.mp4': 'video/mp4',
        '.mkv': 'video/x-matroska',
        '.avi': 'video/x-msvideo',
        '.webm': 'video/webm',
    }
    content_type = content_types.get(ext, 'application/octet-stream')

    try:
        # Read file (works with both local and WebDAV)
        file_data = webdav_client.read_file(file_path)
        if file_data is None:
            print(f"[FileServer] Failed to read file: {file_path}")
            return None, 'text/plain', 500

        # Resize if width is specified and it's an image
        if width and PIL_AVAILABLE and content_type.startswith('image/'):
            try:
                file_data = resize_image(file_data, width)
                # After resize, it's always JPEG (unless PNG with alpha)
                if ext != '.png':
                    content_type = 'image/jpeg'
            except Exception as e:
                print(f"[FileServer] Resize error: {e}")
                # Fall through to serve original

        return file_data, content_type, 200

    except Exception as e:
        print(f"[FileServer] Error reading {file_path}: {e}")
        return None, 'text/plain', 500


def get_file_url(cid: str, base_url: str, width: Optional[int] = None) -> str:
    """
    Generate a file URL for a CID.

    Args:
        cid: The CID of the file
        base_url: Base URL of the server
        width: Optional width for resizing (images only)

    Returns:
        Full URL to the file
    """
    if not cid:
        return ""

    if width:
        return f"{base_url}/file/{cid}/w{width}"
    return f"{base_url}/file/{cid}"
