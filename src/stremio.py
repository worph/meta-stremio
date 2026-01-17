"""
Stremio Addon for Meta-Stremio

Provides a Stremio-compatible addon that reads video metadata from storage (KV/Redis).
Unlike the original segment-stremio-addon, this does NOT scan files - it only reads
metadata written by meta-sort.

Endpoints (Stremio protocol):
- GET /manifest.json - Addon manifest
- GET /catalog/:type/:id.json - List of videos
- GET /meta/:type/:id.json - Video metadata
- GET /stream/:type/:id.json - Stream URLs
"""
from __future__ import annotations

import os
import json
import hashlib
import subprocess
import re
from urllib.parse import quote
from typing import Optional, List, Dict
from collections import defaultdict

from storage import StorageProvider, VideoMetadata, RedisStorage, LeaderStorage

# Configuration
MEDIA_DIR = os.environ.get('MEDIA_DIR', '/files/watch')
STORAGE_MODE = os.environ.get('STORAGE_MODE', 'redis').lower()  # 'leader' or 'redis'

# Poster dimensions for Stremio
POSTER_WIDTH = 342  # Stremio standard poster width
BACKDROP_WIDTH = 1280  # Stremio standard backdrop width

# Global storage provider (initialized by server.py)
_storage: Optional[StorageProvider] = None

# Base URL for poster URLs (set by server.py)
_base_url: str = ""


def set_base_url(url: str) -> None:
    """Set the base URL for poster/backdrop URLs."""
    global _base_url
    _base_url = url.rstrip('/')


def get_poster_url(cid: Optional[str], width: Optional[int] = None) -> str:
    """Convert a CID to a file URL for serving posters/images.

    If the value is already a URL (http/https), returns it as-is.
    If no base_url is set, returns empty string.
    """
    if not cid:
        return ""

    # If it's already a URL, return it as-is
    if cid.startswith('http://') or cid.startswith('https://'):
        return cid

    # If no base URL is set, return empty string
    if not _base_url:
        return ""

    if width:
        return f"{_base_url}/file/{cid}/w{width}"
    return f"{_base_url}/file/{cid}"


def init_storage() -> StorageProvider:
    """Initialize the storage provider based on configuration.

    Storage modes:
    - 'leader': Use leader discovery to find the KV store (recommended)
    - 'redis': Direct Redis connection using REDIS_URL
    """
    global _storage

    if STORAGE_MODE == 'redis':
        _storage = RedisStorage()
        _storage.connect()
        print(f"[Stremio] Using Redis storage")
    else:
        # Default to leader discovery
        _storage = LeaderStorage()
        _storage.connect()
        print(f"[Stremio] Using leader discovery storage")

    return _storage


def get_storage() -> StorageProvider:
    """Get the current storage provider."""
    global _storage
    if _storage is None:
        return init_storage()
    return _storage


def set_storage(storage: StorageProvider) -> None:
    """Set the storage provider (for testing or custom setup)."""
    global _storage
    _storage = storage


def get_localized_metadata(video: VideoMetadata, config: dict = None) -> dict:
    """
    Get metadata from stored video, using config to select preferred fields.

    The language configuration affects which stored fields are preferred.
    All localized data comes from meta-sort's TMDB plugin at indexing time.

    Args:
        video: VideoMetadata from storage
        config: User config with displayLanguage

    Returns:
        Dict with title, description, tagline from stored metadata.
    """
    return {
        'title': video.title,
        'description': video.description,
        'tagline': video.tagline,
    }


def get_localized_episode_metadata(
    video: VideoMetadata,
    tmdb_id: str,
    season: int,
    episode: int,
    config: dict = None
) -> dict:
    """Get episode metadata from stored video."""
    return {
        'title': video.episode_title or video.title,
        'description': video.description,
    }


# Supported display languages
SUPPORTED_LANGUAGES = [
    ("en", "English"),
    ("ja", "Japanese"),
    ("ko", "Korean"),
    ("zh", "Chinese"),
    ("fr", "French"),
    ("de", "German"),
    ("es", "Spanish"),
    ("pt", "Portuguese"),
    ("it", "Italian"),
    ("ru", "Russian"),
    ("ar", "Arabic"),
    ("hi", "Hindi"),
    ("th", "Thai"),
    ("vi", "Vietnamese"),
    ("id", "Indonesian"),
    ("pl", "Polish"),
    ("nl", "Dutch"),
    ("tr", "Turkish"),
    ("sv", "Swedish"),
    ("no", "Norwegian"),
]

# Base manifest definition (name fields are set dynamically based on host)
BASE_MANIFEST = {
    "id": "com.metastremio.addon",
    "version": "1.0.0",
    "name": "Meta-Stremio",
    "description": "Stream your media library with on-the-fly HLS transcoding (powered by meta-sort)",
    "logo": "https://raw.githubusercontent.com/user/meta-stremio/main/logo.png",
    "resources": [
        "catalog",
        {
            "name": "meta",
            "types": ["movie", "series"],
            "idPrefixes": ["ms_"]
        },
        {
            "name": "stream",
            "types": ["movie", "series"],
            "idPrefixes": ["ms_", "tt"]
        }
    ],
    "types": ["movie", "series"],
    "catalogs": [
        {
            "type": "movie",
            "id": "ms_movies",
            "name": "Movies",
            "extra": [
                {"name": "search", "isRequired": False}
            ]
        },
        {
            "type": "series",
            "id": "ms_series",
            "name": "Series",
            "extra": [
                {"name": "search", "isRequired": False}
            ]
        }
    ],
    "idPrefixes": ["ms_"],
    "behaviorHints": {
        "configurable": True,
        "configurationRequired": False
    },
    "config": [
        {
            "key": "displayLanguage",
            "type": "select",
            "title": "Display Language",
            "options": [code for code, name in SUPPORTED_LANGUAGES],
            "default": "en"
        }
    ]
}


def get_manifest(host: str) -> dict:
    """Generate manifest with host in the addon ID and name."""
    manifest = json.loads(json.dumps(BASE_MANIFEST))  # Deep copy

    # Build reverse-DNS style ID
    if ':' in host:
        domain, port = host.rsplit(':', 1)
    else:
        domain, port = host, None

    domain_parts = domain.split('.')
    reversed_domain = '.'.join(reversed(domain_parts))

    addon_id = f"{reversed_domain}.metastremio"
    if port:
        addon_id += f".{port}"

    manifest["id"] = addon_id
    manifest["name"] = f"Meta-Stremio @ {host}"
    manifest["catalogs"][0]["name"] = f"Movies @ {host}"
    manifest["catalogs"][1]["name"] = f"Series @ {host}"
    return manifest


def get_stremio_id(hash_id: str) -> str:
    """Convert storage hash ID to Stremio ID format."""
    if hash_id.startswith('ms_'):
        return hash_id
    return f"ms_{hash_id}"


def get_hash_from_stremio_id(stremio_id: str) -> str:
    """Extract storage hash ID from Stremio ID."""
    if stremio_id.startswith('ms_'):
        return stremio_id[3:]
    return stremio_id


def get_relative_path(video: VideoMetadata) -> str:
    """Get relative path from video metadata for URL encoding."""
    # Remove MEDIA_DIR prefix if present
    path = video.file_path
    if path.startswith(MEDIA_DIR):
        path = path[len(MEDIA_DIR):].lstrip('/')
    elif path.startswith('/'):
        # Try to make it relative to any common prefix
        path = path.lstrip('/')
    return path


def format_size(size: int) -> str:
    """Format file size to human readable string."""
    if not size:
        return "Unknown"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def format_duration(seconds: float) -> str:
    """Format duration to human readable string."""
    if not seconds:
        return "Unknown"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def get_video_info(filepath: str) -> Optional[dict]:
    """Get video metadata using ffprobe."""
    full_path = os.path.join(MEDIA_DIR, filepath) if not filepath.startswith('/') else filepath
    if not os.path.exists(full_path):
        return None

    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams', full_path],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
    except Exception:
        pass
    return None


# Codecs that are HLS-compatible
HLS_NATIVE_VIDEO_CODECS = {'h264', 'avc1'}
HLS_NATIVE_AUDIO_CODECS = {'aac', 'mp4a'}


def needs_transcoding(video: VideoMetadata) -> tuple[bool, str]:
    """Check if video needs transcoding based on codec compatibility."""
    reasons = []

    if video.video_codec:
        codec = video.video_codec.lower()
        if codec not in HLS_NATIVE_VIDEO_CODECS:
            reasons.append(f"video:{codec}")

    if video.audio_codec:
        codec = video.audio_codec.lower()
        if codec not in HLS_NATIVE_AUDIO_CODECS:
            reasons.append(f"audio:{codec}")

    if reasons:
        return True, ", ".join(reasons)
    return False, ""


def is_series_content(video: VideoMetadata) -> bool:
    """
    Determine if a video is series content based on metadata.

    A video is considered series if:
    - It has season AND/OR episode information
    - OR its video_type is 'series', 'anime', or 'tvshow'
    """
    # Check for explicit episode/season info
    has_episode_info = video.season is not None or video.episode is not None

    # Check video type
    is_series_type = video.video_type in ('series', 'anime', 'tvshow')

    return has_episode_info or is_series_type


def get_series_title(video: VideoMetadata) -> str:
    """
    Extract the base series title from a video.

    Removes episode/season specific parts from the title to get the series name.
    Falls back to filename-based extraction if title is empty.
    """
    title = video.title

    # If title is empty, try to extract from filename
    if not title:
        filename = os.path.basename(video.file_path)
        # Remove extension
        title = os.path.splitext(filename)[0]
        # Clean up common patterns
        title = re.sub(r'\[.*?\]', '', title)  # Remove [brackets]
        title = re.sub(r'\(.*?\)', '', title)  # Remove (parentheses)
        title = re.sub(r'[._]', ' ', title)    # Replace dots/underscores with spaces

    # Remove common episode patterns from title
    # e.g., "Show Name S01E01", "Show Name - Episode 1", "Show Name 1x01"
    patterns = [
        r'\s*[Ss]\d+[Ee]\d+.*$',          # S01E01 and anything after
        r'\s*-?\s*[Ss]eason\s*\d+.*$',    # Season X and anything after
        r'\s*-?\s*[Ee]pisode\s*\d+.*$',   # Episode X and anything after
        r'\s*\d+[xX]\d+.*$',              # 1x01 format
        r'\s*-?\s*[Ee]p\.?\s*\d+.*$',     # Ep 1, Ep. 1
        r'\s*-?\s*E\d+.*$',               # E01
        r'\s*#\d+.*$',                     # #1
    ]

    for pattern in patterns:
        title = re.sub(pattern, '', title, flags=re.IGNORECASE)

    # Clean up trailing dashes, underscores, and extra spaces
    result = re.sub(r'[\s\-_]+$', '', title).strip()

    # If still empty after all processing, use a hash of the file path as fallback
    if not result:
        # Last resort: use parent directory name as title
        parent_dir = os.path.basename(os.path.dirname(video.file_path))
        if parent_dir and parent_dir not in ('Season 01', 'Season 02', 'Season 1', 'Season 2'):
            result = parent_dir
        else:
            # Use grandparent directory
            grandparent = os.path.basename(os.path.dirname(os.path.dirname(video.file_path)))
            result = grandparent if grandparent else "Unknown Series"

    return result


def get_series_id(video: VideoMetadata) -> str:
    """
    Generate a unique series ID from the series title.

    Uses a hash of the normalized series title to create a stable ID.
    """
    series_title = get_series_title(video).lower()
    # Create a short hash for the series ID
    title_hash = hashlib.md5(series_title.encode()).hexdigest()[:12]
    return f"ms_series_{title_hash}"


def group_videos_by_series(videos: List[VideoMetadata]) -> Dict[str, Dict]:
    """
    Group video episodes by their parent series.

    Returns a dict mapping series_id to series info with episodes.
    """
    series_map: Dict[str, Dict] = {}

    for video in videos:
        if not is_series_content(video):
            continue

        series_id = get_series_id(video)
        series_title = get_series_title(video)

        if series_id not in series_map:
            series_map[series_id] = {
                'id': series_id,
                'title': series_title,
                'episodes': [],
                'poster': video.poster,
                'backdrop': video.backdrop,
                'description': video.description,
                'year': video.year,
                'genres': video.genres or [],
                'imdb_id': video.imdb_id,
                'tmdb_id': video.tmdb_id,
                'video_type': video.video_type,
                'rating': video.rating,
                'tagline': video.tagline,
                'studios': video.studios or [],
            }

        # Add episode to series
        series_map[series_id]['episodes'].append(video)

        # Update series metadata with better values if available
        series_info = series_map[series_id]
        if not series_info['poster'] and video.poster:
            series_info['poster'] = video.poster
        if not series_info['backdrop'] and video.backdrop:
            series_info['backdrop'] = video.backdrop
        if not series_info['description'] and video.description:
            series_info['description'] = video.description
        if not series_info['year'] and video.year:
            series_info['year'] = video.year
        if not series_info['genres'] and video.genres:
            series_info['genres'] = video.genres
        if not series_info['imdb_id'] and video.imdb_id:
            series_info['imdb_id'] = video.imdb_id
        if not series_info['tmdb_id'] and video.tmdb_id:
            series_info['tmdb_id'] = video.tmdb_id
        if not series_info['rating'] and video.rating:
            series_info['rating'] = video.rating
        if not series_info['tagline'] and video.tagline:
            series_info['tagline'] = video.tagline
        if not series_info['studios'] and video.studios:
            series_info['studios'] = video.studios

    # Sort episodes by season and episode number
    for series_id, series_info in series_map.items():
        series_info['episodes'].sort(
            key=lambda v: (v.season or 0, v.episode or 0)
        )

    return series_map


def extract_episode_title_from_filename(filename: str, series_title: str) -> Optional[str]:
    """
    Extract episode title from filename.

    For example:
    - "Sintel.S03E03.The.Warrior.480p.mp4" -> "The Warrior"
    - "[SubsPlease] Show Name - 01 (1080p).mkv" -> None (no episode title)
    - "Show.S01E05.Episode.Title.Here.720p.mkv" -> "Episode Title Here"
    """
    import re

    # Remove file extension
    name = os.path.splitext(filename)[0]

    # Common patterns to find where episode info ends and title begins
    # Pattern: S01E01.Episode.Title or S01E01 - Episode Title
    ep_pattern = r'[Ss]\d+[Ee]\d+[\.\s\-_]+'

    match = re.search(ep_pattern, name)
    if match:
        # Get everything after the episode pattern
        after_ep = name[match.end():]

        # Remove common quality/release tags
        quality_patterns = [
            r'\b\d{3,4}p\b.*$',  # 480p, 720p, 1080p and everything after
            r'\bHDTV\b.*$',
            r'\bWEB[-\.]?(?:DL|Rip)?\b.*$',
            r'\bBluRay\b.*$',
            r'\bBDRip\b.*$',
            r'\bDVDRip\b.*$',
            r'\b[xXhH]\.?26[45]\b.*$',
            r'\bHEVC\b.*$',
            r'\bAV1\b.*$',
            r'\bAAC\b.*$',
            r'\bAC3\b.*$',
            r'\bDTS\b.*$',
            r'\b10bit\b.*$',
            r'\[.*\].*$',  # Remove [tags] at the end
        ]

        for pattern in quality_patterns:
            after_ep = re.sub(pattern, '', after_ep, flags=re.IGNORECASE)

        # Clean up the result
        after_ep = after_ep.strip(' .-_')

        # Replace dots and underscores with spaces
        after_ep = re.sub(r'[._]+', ' ', after_ep)
        after_ep = re.sub(r'\s+', ' ', after_ep).strip()

        # Don't return if it's empty or just the series title
        if after_ep and after_ep.lower() != series_title.lower():
            return after_ep

    return None


def create_episode_id(series_id: str, season: int, episode: int) -> str:
    """Create a Stremio-compatible episode ID."""
    return f"{series_id}:{season or 1}:{episode or 1}"


def parse_episode_id(episode_id: str) -> tuple[str, int, int]:
    """Parse an episode ID into series_id, season, episode."""
    parts = episode_id.split(':')
    if len(parts) == 3:
        return parts[0], int(parts[1]), int(parts[2])
    return episode_id, 1, 1


def create_catalog_response(catalog_type: str, search: Optional[str] = None, config: dict = None) -> dict:
    """Create catalog response from storage."""
    storage = get_storage()
    all_videos = storage.get_all_videos()

    metas = []

    if catalog_type == 'movie':
        # Movies: videos that are NOT series content
        movies = [v for v in all_videos if not is_series_content(v)]

        # Apply search filter
        if search:
            search_lower = search.lower()
            movies = [v for v in movies if search_lower in v.title.lower()]

        for video in movies:
            meta = create_movie_meta(video, config)
            metas.append(meta)

    elif catalog_type == 'series':
        # Series: videos that ARE series content, grouped by series
        series_videos = [v for v in all_videos if is_series_content(v)]
        series_map = group_videos_by_series(series_videos)

        # Apply search filter to series
        if search:
            search_lower = search.lower()
            series_map = {
                sid: info for sid, info in series_map.items()
                if search_lower in info['title'].lower()
            }

        # Create catalog entry for each series (not individual episodes)
        for series_id, series_info in sorted(series_map.items(), key=lambda x: x[1]['title'].lower()):
            meta = create_series_catalog_meta(series_info, config)
            metas.append(meta)

    else:
        # All content - mix of movies and series
        # Movies
        movies = [v for v in all_videos if not is_series_content(v)]
        for video in movies:
            metas.append(create_movie_meta(video, config))

        # Series (grouped)
        series_videos = [v for v in all_videos if is_series_content(v)]
        series_map = group_videos_by_series(series_videos)
        for series_id, series_info in sorted(series_map.items(), key=lambda x: x[1]['title'].lower()):
            metas.append(create_series_catalog_meta(series_info, config))

        # Sort combined list by name
        metas.sort(key=lambda m: m.get('name', '').lower())

    return {"metas": metas}


def create_movie_meta(video: VideoMetadata, config: dict = None) -> dict:
    """Create a Stremio catalog meta object for a movie."""
    # Get localized metadata if config is provided
    localized = get_localized_metadata(video, config)

    meta = {
        "id": get_stremio_id(video.hash_id),
        "type": "movie",
        "name": localized.get('title', video.title),
        "poster": get_poster_url(video.poster, POSTER_WIDTH),
        "description": localized.get('description') or "",
    }

    # Release info with rating if available
    release_parts = []
    if video.year:
        release_parts.append(str(video.year))
    if video.rating:
        release_parts.append(f"★ {video.rating:.1f}")
    if release_parts:
        meta["releaseInfo"] = " | ".join(release_parts)

    # Genres
    if video.genres:
        meta["genres"] = video.genres

    # Runtime
    if video.duration:
        meta["runtime"] = format_duration(video.duration)

    # Background/backdrop
    if video.backdrop:
        meta["background"] = get_poster_url(video.backdrop, BACKDROP_WIDTH)

    # Links (IMDB)
    if video.imdb_id:
        meta["links"] = [{"name": "IMDB", "category": "imdb", "url": f"https://www.imdb.com/title/{video.imdb_id}"}]

    # Technical info in description if no plot
    if not localized.get('description'):
        desc_parts = [f"File: {os.path.basename(video.file_path)}"]
        if video.width and video.height:
            desc_parts.append(f"{video.width}x{video.height}")
        if video.video_codec:
            desc_parts.append(video.video_codec.upper())
        if video.file_size:
            desc_parts.append(format_size(video.file_size))
        meta["description"] = " | ".join(desc_parts)

    return meta


def create_series_catalog_meta(series_info: dict, config: dict = None) -> dict:
    """Create a Stremio catalog meta object for a series."""
    episodes = series_info['episodes']
    episode_count = len(episodes)

    # Get unique seasons
    seasons = sorted(set(ep.season or 1 for ep in episodes))
    season_count = len(seasons)

    meta = {
        "id": series_info['id'],
        "type": "series",
        "name": series_info['title'],
        "poster": get_poster_url(series_info['poster'], POSTER_WIDTH),
        "description": series_info['description'] or "",
    }

    # Get rating from any episode (they should share the series rating)
    rating = series_info.get('rating')
    if not rating:
        for ep in episodes:
            if ep.rating:
                rating = ep.rating
                break

    # Release info with season/episode count and rating
    release_parts = []
    if series_info['year']:
        release_parts.append(str(series_info['year']))
    if rating:
        release_parts.append(f"★ {rating:.1f}")
    release_parts.append(f"{season_count} Season{'s' if season_count > 1 else ''}")
    release_parts.append(f"{episode_count} Episode{'s' if episode_count > 1 else ''}")
    meta["releaseInfo"] = " | ".join(release_parts)

    # Genres
    if series_info['genres']:
        meta["genres"] = series_info['genres']

    # Background/backdrop
    if series_info['backdrop']:
        meta["background"] = get_poster_url(series_info['backdrop'], BACKDROP_WIDTH)

    # Links (IMDB)
    if series_info['imdb_id']:
        meta["links"] = [{"name": "IMDB", "category": "imdb", "url": f"https://www.imdb.com/title/{series_info['imdb_id']}"}]

    # Video type indicator (anime vs regular series)
    if series_info['video_type'] == 'anime':
        if not meta["description"]:
            meta["description"] = "Anime Series"
        if "Anime" not in (meta.get("genres") or []):
            meta["genres"] = ["Anime"] + (meta.get("genres") or [])

    return meta


def create_meta_response(meta_id: str, config: dict = None) -> Optional[dict]:
    """Create detailed metadata response for a video or series."""
    storage = get_storage()

    # Check if this is a series ID (ms_series_*)
    if meta_id.startswith('ms_series_'):
        return create_series_meta_response(meta_id, config)

    # Otherwise it's a movie or direct episode access
    hash_id = get_hash_from_stremio_id(meta_id)

    # Check if it's an episode ID (ms_series_xxx:season:episode)
    if ':' in meta_id:
        series_id, season, episode = parse_episode_id(meta_id)
        return create_episode_meta_response(series_id, season, episode, config)

    video = storage.get_video_by_hash(hash_id)
    if not video:
        return None

    # If it's a series/anime episode accessed directly, redirect to series format
    if is_series_content(video):
        series_id = get_series_id(video)
        return create_series_meta_response(series_id, config)

    # Movie meta response
    return create_movie_meta_response(video, config)


def create_movie_meta_response(video: VideoMetadata, config: dict = None) -> dict:
    """Create detailed metadata response for a movie."""
    # Get localized metadata if config is provided
    localized = get_localized_metadata(video, config)

    # Build technical info
    tech_parts = []
    if video.width and video.height:
        resolution = f"{video.height}p" if video.height in [480, 720, 1080, 2160, 4320] else f"{video.width}x{video.height}"
        tech_parts.append(resolution)
    if video.video_codec:
        tech_parts.append(video.video_codec.upper())
    if video.audio_codec:
        tech_parts.append(video.audio_codec.upper())
    if video.file_size:
        tech_parts.append(format_size(video.file_size))

    # Build description
    desc_parts = []
    tagline = localized.get('tagline') or video.tagline
    description = localized.get('description') or video.description
    if tagline:
        desc_parts.append(f"_{tagline}_\n\n")
    if description:
        desc_parts.append(description)
    if tech_parts:
        desc_parts.append(f"\n\n📺 {' | '.join(tech_parts)}")
    desc_parts.append(f"\n📁 {os.path.basename(video.file_path)}")

    meta = {
        "id": get_stremio_id(video.hash_id),
        "type": "movie",
        "name": localized.get('title', video.title),
        "poster": get_poster_url(video.poster, POSTER_WIDTH),
        "description": "".join(desc_parts),
    }

    # Release info with rating
    release_parts = []
    if video.year:
        release_parts.append(str(video.year))
        meta["year"] = video.year
    if video.rating:
        release_parts.append(f"★ {video.rating:.1f}")
    if release_parts:
        meta["releaseInfo"] = " | ".join(release_parts)

    # Runtime
    if video.duration:
        meta["runtime"] = format_duration(video.duration)

    # Genres
    if video.genres:
        meta["genres"] = video.genres

    # Background/Backdrop
    if video.backdrop:
        meta["background"] = get_poster_url(video.backdrop, BACKDROP_WIDTH)

    # IMDB
    if video.imdb_id:
        meta["imdb_id"] = video.imdb_id
        meta["links"] = [
            {"name": "IMDB", "category": "imdb", "url": f"https://www.imdb.com/title/{video.imdb_id}"}
        ]

    # TMDB
    if video.tmdb_id:
        meta["tmdb_id"] = video.tmdb_id

    # Audio/subtitle info
    if video.audio_tracks:
        langs = list(dict.fromkeys(t.get('language', t.get('lang', 'und')) for t in video.audio_tracks))
        if langs:
            meta["audioLanguages"] = langs

    if video.subtitles:
        sub_langs = list(dict.fromkeys(s.get('language', s.get('lang', 'und')) for s in video.subtitles))
        if sub_langs:
            meta["subtitleLanguages"] = sub_langs

    return {"meta": meta}


def create_series_meta_response(series_id: str, config: dict = None) -> Optional[dict]:
    """Create detailed metadata response for a series with videos array."""
    storage = get_storage()
    all_videos = storage.get_all_videos()

    # Get all series episodes
    series_videos = [v for v in all_videos if is_series_content(v)]
    series_map = group_videos_by_series(series_videos)

    if series_id not in series_map:
        return None

    series_info = series_map[series_id]
    episodes = series_info['episodes']

    # Get unique seasons
    seasons = sorted(set(ep.season or 1 for ep in episodes))

    # Get rating from any episode
    rating = series_info.get('rating')
    if not rating:
        for ep in episodes:
            if ep.rating:
                rating = ep.rating
                break

    # Use stored metadata (localized by meta-sort's TMDB plugin at indexing time)
    series_title = series_info['title']
    series_description = series_info['description'] or ""
    series_tagline = series_info.get('tagline', '')

    # Build videos array (Stremio series format)
    videos = []
    for ep in episodes:
        season = ep.season or 1
        episode_num = ep.episode or 1
        episode_id = create_episode_id(series_id, season, episode_num)

        # Build episode title - prefer episode_title, then extract from filename
        ep_title = ep.episode_title

        if not ep_title:
            # Try to extract from filename (e.g., "Show.S01E01.Episode.Title.480p.mp4")
            filename = os.path.basename(ep.file_path)
            ep_title = extract_episode_title_from_filename(filename, series_info['title'])

        if not ep_title:
            # Try to extract from title by removing series title prefix
            full_title = ep.title
            if full_title.lower().startswith(series_info['title'].lower()):
                ep_title = full_title[len(series_info['title']):].strip(' -:')
            else:
                ep_title = full_title

        # If still empty or same as series title, use generic name
        if not ep_title or ep_title.lower() == series_info['title'].lower():
            ep_title = f"Episode {episode_num}"

        # Build episode overview with description and technical info
        overview_parts = []

        # Add episode description if available
        if ep.description:
            overview_parts.append(ep.description)

        # Technical details as a separate line
        tech_info = []
        if ep.width and ep.height:
            resolution = f"{ep.height}p" if ep.height in [480, 720, 1080, 2160, 4320] else f"{ep.width}x{ep.height}"
            tech_info.append(resolution)
        if ep.video_codec:
            tech_info.append(ep.video_codec.upper())
        if ep.file_size:
            tech_info.append(format_size(ep.file_size))
        if ep.duration:
            tech_info.append(format_duration(ep.duration))

        # Combine overview
        if overview_parts and tech_info:
            overview = overview_parts[0] + f"\n\n📺 {' | '.join(tech_info)}"
        elif tech_info:
            overview = f"📺 {' | '.join(tech_info)}"
        elif overview_parts:
            overview = overview_parts[0]
        else:
            overview = f"S{season:02d}E{episode_num:02d}"

        # Build released date - prefer release_date, then year
        released_date = ep.release_date
        if not released_date:
            year = ep.year or series_info['year'] or 2000
            released_date = f"{year}-01-01"

        video_entry = {
            "id": episode_id,
            "title": ep_title,
            "season": season,
            "episode": episode_num,
            "released": f"{released_date}T00:00:00.000Z",
            "overview": overview,
        }

        # Add thumbnail if available (use smaller size for episode thumbnails)
        if ep.poster:
            video_entry["thumbnail"] = get_poster_url(ep.poster, 185)

        videos.append(video_entry)

    # Build series description
    desc_parts = []
    if series_tagline:
        desc_parts.append(f"_{series_tagline}_\n\n")
    if series_description:
        desc_parts.append(series_description)

    desc_parts.append(f"\n\n📺 {len(seasons)} Season{'s' if len(seasons) > 1 else ''} | {len(episodes)} Episode{'s' if len(episodes) > 1 else ''}")

    # Get typical quality from first episode
    if episodes:
        first_ep = episodes[0]
        if first_ep.width and first_ep.height:
            resolution = f"{first_ep.height}p" if first_ep.height in [480, 720, 1080, 2160, 4320] else f"{first_ep.width}x{first_ep.height}"
            if first_ep.video_codec:
                desc_parts.append(f"\nQuality: {resolution} {first_ep.video_codec.upper()}")
            else:
                desc_parts.append(f"\nQuality: {resolution}")

    meta = {
        "id": series_id,
        "type": "series",
        "name": series_title,
        "poster": get_poster_url(series_info['poster'], POSTER_WIDTH),
        "description": "".join(desc_parts),
        "videos": videos,
    }

    # Release info with rating
    release_parts = []
    if series_info['year']:
        release_parts.append(str(series_info['year']))
        meta["year"] = series_info['year']
    if rating:
        release_parts.append(f"★ {rating:.1f}")
    if release_parts:
        meta["releaseInfo"] = " | ".join(release_parts)

    # Genres
    if series_info['genres']:
        meta["genres"] = series_info['genres']
    elif series_info['video_type'] == 'anime':
        meta["genres"] = ["Anime"]

    # Background/Backdrop
    if series_info['backdrop']:
        meta["background"] = get_poster_url(series_info['backdrop'], BACKDROP_WIDTH)

    # IMDB
    if series_info['imdb_id']:
        meta["imdb_id"] = series_info['imdb_id']
        meta["links"] = [
            {"name": "IMDB", "category": "imdb", "url": f"https://www.imdb.com/title/{series_info['imdb_id']}"}
        ]

    return {"meta": meta}


def create_episode_meta_response(series_id: str, season: int, episode: int, config: dict = None) -> Optional[dict]:
    """Create metadata response for accessing a specific episode."""
    # For episode-level meta, return the full series meta
    # Stremio will use the videos array to find the specific episode
    return create_series_meta_response(series_id, config)


def find_episode_video(series_id: str, season: int, episode: int) -> Optional[VideoMetadata]:
    """Find the video for a specific episode in a series."""
    storage = get_storage()
    all_videos = storage.get_all_videos()

    # Find videos that match this series
    series_videos = [v for v in all_videos if is_series_content(v)]
    series_map = group_videos_by_series(series_videos)

    if series_id not in series_map:
        return None

    # Find the episode
    for ep in series_map[series_id]['episodes']:
        if (ep.season or 1) == season and (ep.episode or 1) == episode:
            return ep

    return None


def create_stream_response(stream_id: str, base_url: str) -> Optional[dict]:
    """Create stream response with multiple stream options."""
    storage = get_storage()
    video = None

    # Check if this is an IMDB ID (tt followed by digits)
    if stream_id.startswith('tt') and stream_id[2:].isdigit():
        video = storage.get_video_by_imdb_id(stream_id)
    # Check if this is an episode ID (ms_series_xxx:season:episode or tt123:1:2)
    elif ':' in stream_id:
        series_id, season, episode = parse_episode_id(stream_id)
        # Check if series_id is an IMDB ID for series episode lookup
        if series_id.startswith('tt') and series_id[2:].isdigit():
            # TODO: Implement series episode lookup by IMDB ID
            video = None
        else:
            video = find_episode_video(series_id, season, episode)
    else:
        # Regular movie or direct video access
        hash_id = get_hash_from_stremio_id(stream_id)
        video = storage.get_video_by_hash(hash_id)

    if not video:
        # Return empty streams for unknown IDs (Stremio protocol)
        return {"streams": []}

    # Get relative path for URL encoding
    rel_path = get_relative_path(video)
    encoded_path = '/'.join(quote(part, safe='') for part in rel_path.split('/'))
    filename = os.path.basename(video.file_path)

    # Get additional info via ffprobe if available
    info = get_video_info(video.file_path)

    streams = []
    subtitles = []
    audio_tracks = video.audio_tracks or []

    # Video metadata - use ffprobe to fill in missing info
    video_codec = video.video_codec
    audio_codec = video.audio_codec
    video_width = video.width
    video_height = video.height

    if info:
        # Get video stream info from ffprobe
        video_streams = [s for s in info.get('streams', []) if s.get('codec_type') == 'video']
        if video_streams:
            vs = video_streams[0]  # Use first video stream
            if not video_codec:
                video_codec = vs.get('codec_name', 'unknown')
            if not video_width:
                video_width = vs.get('width', 0)
            if not video_height:
                video_height = vs.get('height', 0)

        # Get subtitle info from ffprobe
        subtitle_streams = [s for s in info.get('streams', []) if s.get('codec_type') == 'subtitle']
        for i, sub in enumerate(subtitle_streams):
            lang = sub.get('tags', {}).get('language', 'und')
            subtitles.append({
                "id": f"{stream_id}-sub-{i}",
                "url": f"{base_url}/transcode/{encoded_path}/subtitle_{i}.vtt",
                "lang": lang,
            })

        # Get audio tracks from ffprobe for more accurate info
        audio_streams = [s for s in info.get('streams', []) if s.get('codec_type') == 'audio']
        if audio_streams:
            audio_tracks = []
            for i, aud in enumerate(audio_streams):
                lang = aud.get('tags', {}).get('language', 'und')
                title = aud.get('tags', {}).get('title', '')
                codec = aud.get('codec_name', 'unknown').upper()
                channels = aud.get('channels', 2)
                audio_tracks.append({
                    'index': i,
                    'lang': lang,
                    'name': title or (lang.upper() if lang != 'und' else f"Audio {i+1}"),
                    'codec': codec,
                    'channels': channels,
                })
            # Update audio_codec from first audio stream if missing
            if not audio_codec and audio_streams:
                audio_codec = audio_streams[0].get('codec_name', 'unknown')

    # Default audio track if none detected
    if not audio_tracks:
        audio_tracks = [{'index': 0, 'lang': 'und', 'name': 'Default', 'codec': audio_codec or 'Unknown', 'channels': 2}]

    # Check transcoding requirements (use ffprobe-enriched values)
    transcode_needed, transcode_reason = needs_transcoding(video)

    # Format codec strings for display
    video_codec_display = (video_codec or 'unknown').upper()
    audio_codec_display = (audio_codec or 'unknown').upper()
    video_height_display = video_height or 0

    # Build audio info string
    if len(audio_tracks) > 1:
        audio_langs = list(dict.fromkeys(t.get('lang', 'und') for t in audio_tracks))
        audio_info = f" | {'/'.join(audio_langs)}"
    else:
        audio_info = f" | {audio_tracks[0].get('lang', 'und')}"

    # 1. Direct File
    direct_stream = {
        "url": f"{base_url}/direct/{encoded_path}",
        "title": f"Direct File ({video_codec_display}/{audio_codec_display}){audio_info}",
        "name": "MS Direct",
        "behaviorHints": {
            "notWebReady": False,
            "filename": filename,
        }
    }
    if subtitles:
        direct_stream["subtitles"] = subtitles
    streams.append(direct_stream)

    # 2. HLS Original - One stream per audio track
    transcode_note = f" [transcode: {transcode_reason}]" if transcode_needed else ""
    for track in audio_tracks:
        audio_suffix = f" ({track.get('name', 'Audio')})" if len(audio_tracks) > 1 else ""
        hls_stream = {
            "url": f"{base_url}/transcode/{encoded_path}/master_original_a{track.get('index', 0)}.m3u8",
            "title": f"HLS {video_height_display}p{audio_suffix}{transcode_note}",
            "name": f"MS {video_height_display}p" + (f" {track.get('name', '')}" if len(audio_tracks) > 1 else ""),
            "behaviorHints": {"notWebReady": False}
        }
        if subtitles:
            hls_stream["subtitles"] = subtitles
        streams.append(hls_stream)

    # 3. HLS ABR - One stream per audio track
    for track in audio_tracks:
        audio_suffix = f" ({track.get('name', 'Audio')})" if len(audio_tracks) > 1 else ""
        hls_stream = {
            "url": f"{base_url}/transcode/{encoded_path}/master_a{track.get('index', 0)}.m3u8",
            "title": f"HLS ABR (up to {video_height_display}p){audio_suffix}",
            "name": f"MS ABR" + (f" {track.get('name', '')}" if len(audio_tracks) > 1 else ""),
            "behaviorHints": {"notWebReady": False}
        }
        if subtitles:
            hls_stream["subtitles"] = subtitles
        streams.append(hls_stream)

    return {"streams": streams}


def get_library_stats() -> dict:
    """Get library statistics for dashboard."""
    storage = get_storage()
    all_videos = storage.get_all_videos()

    # Categorize using the new detection logic
    movies = [v for v in all_videos if not is_series_content(v)]
    series_videos = [v for v in all_videos if is_series_content(v)]

    # Group series to count unique series
    series_map = group_videos_by_series(series_videos)

    return {
        'total': len(all_videos),
        'movies': len(movies),
        'series': len(series_map),  # Count of unique series
        'episodes': len(series_videos),  # Total episode count
        'storage_status': storage.get_status(),
    }


class StremioHandler:
    """Handler for Stremio addon requests."""

    def __init__(self):
        pass

    def handle_manifest(self, host: str = "localhost", config: dict = None) -> tuple[bytes, str]:
        """Return addon manifest with host in the name."""
        manifest = get_manifest(host)
        return json.dumps(manifest).encode(), 'application/json'

    def handle_catalog(self, catalog_type: str, catalog_id: str, extra: dict = None, config: dict = None) -> tuple[bytes, str]:
        """Return catalog of videos from storage."""
        search = extra.get('search') if extra else None
        response = create_catalog_response(catalog_type, search, config)
        return json.dumps(response).encode(), 'application/json'

    def handle_meta(self, meta_type: str, meta_id: str, config: dict = None) -> tuple[Optional[bytes], str]:
        """Return video metadata."""
        response = create_meta_response(meta_id, config)
        if response:
            return json.dumps(response).encode(), 'application/json'
        return None, 'application/json'

    def handle_stream(self, stream_type: str, stream_id: str, base_url: str, config: dict = None) -> tuple[bytes, str]:
        """Return stream URLs."""
        response = create_stream_response(stream_id, base_url)
        # Always return a valid response (empty streams for unknown IDs)
        return json.dumps(response).encode(), 'application/json'


def get_supported_languages() -> list:
    """Return list of supported languages with codes and names."""
    return SUPPORTED_LANGUAGES
