"""
Poster API for Meta-Stremio

Serves poster and backdrop images by CID with optional resizing.
Builds a CID -> file path index from video metadata.

Endpoints:
- GET /poster/{cid} - Serve original image
- GET /poster/{cid}/w{width} - Serve resized image (width specified)
"""
from __future__ import annotations

import os
import io
from typing import Optional, Dict, Tuple
from threading import Lock

# Optional PIL for image resizing
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    Image = None

# Configuration
FILES_PATH = os.environ.get('FILES_PATH', '/files')

# CID -> file path index
_cid_index: Dict[str, str] = {}
_index_lock = Lock()
_index_built = False


def build_cid_index(storage) -> None:
    """Build CID to file path index from storage metadata."""
    global _cid_index, _index_built

    with _index_lock:
        _cid_index.clear()

        try:
            videos = storage.get_all_videos()
            for video in videos:
                # Index poster
                if video.poster and video.poster_path:
                    _cid_index[video.poster] = video.poster_path
                # Index backdrop
                if video.backdrop and video.backdrop_path:
                    _cid_index[video.backdrop] = video.backdrop_path

            _index_built = True
            print(f"[Poster] Built CID index: {len(_cid_index)} images")
        except Exception as e:
            print(f"[Poster] Error building CID index: {e}")


def get_image_path(cid: str) -> Optional[str]:
    """Get the file path for a CID."""
    with _index_lock:
        rel_path = _cid_index.get(cid)
        if rel_path:
            return os.path.join(FILES_PATH, rel_path)
    return None


def is_index_built() -> bool:
    """Check if the CID index has been built."""
    return _index_built


def get_index_size() -> int:
    """Get the number of images in the index."""
    with _index_lock:
        return len(_cid_index)


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


def serve_poster(cid: str, width: Optional[int] = None) -> Tuple[Optional[bytes], str, int]:
    """
    Serve a poster image by CID.

    Args:
        cid: The CID of the image
        width: Optional width for resizing

    Returns:
        Tuple of (image_data, content_type, status_code)
    """
    # Get file path from CID
    file_path = get_image_path(cid)

    if not file_path:
        return None, 'text/plain', 404

    if not os.path.exists(file_path):
        print(f"[Poster] File not found: {file_path}")
        return None, 'text/plain', 404

    # Determine content type from extension
    ext = os.path.splitext(file_path)[1].lower()
    content_types = {
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
        '.webp': 'image/webp',
        '.gif': 'image/gif',
    }
    content_type = content_types.get(ext, 'image/jpeg')

    try:
        with open(file_path, 'rb') as f:
            image_data = f.read()

        # Resize if width is specified
        if width and PIL_AVAILABLE:
            try:
                image_data = resize_image(image_data, width)
                # After resize, it's always JPEG (unless PNG with alpha)
                if not (ext == '.png'):
                    content_type = 'image/jpeg'
            except Exception as e:
                print(f"[Poster] Resize error: {e}")
                # Fall through to serve original

        return image_data, content_type, 200

    except Exception as e:
        print(f"[Poster] Error reading {file_path}: {e}")
        return None, 'text/plain', 500


def get_poster_url(cid: str, base_url: str, width: Optional[int] = None) -> str:
    """
    Generate a poster URL for a CID.

    Args:
        cid: The CID of the image
        base_url: Base URL of the server
        width: Optional width for resizing

    Returns:
        Full URL to the poster image
    """
    if not cid:
        return ""

    if width:
        return f"{base_url}/poster/{cid}/w{width}"
    return f"{base_url}/poster/{cid}"
