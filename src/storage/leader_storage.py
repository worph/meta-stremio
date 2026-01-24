"""
Leader-aware Storage Provider for meta-stremio.

Uses leader discovery to automatically connect to the KV leader.
Handles reconnection when the leader changes or becomes unavailable.
"""
from __future__ import annotations

import os
import json
import threading
from typing import List, Optional, Callable

from .provider import StorageProvider, VideoMetadata
from .leader_discovery import LeaderDiscovery, LeaderLockInfo

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None


# Configuration from environment
META_CORE_PATH = os.environ.get('META_CORE_PATH', '/meta-core')
FILES_PATH = os.environ.get('FILES_PATH', '/files')
REDIS_PREFIX = os.environ.get('REDIS_PREFIX', 'meta-sort:')


class LeaderStorage(StorageProvider):
    """
    Storage provider that discovers and connects to the KV leader.

    Uses leader discovery to automatically find the Redis instance
    managed by meta-sort. Handles reconnection on leader failure.
    """

    def __init__(
        self,
        meta_core_path: str = None,
        files_path: str = None,
        prefix: str = None,
        redis_url: str = None  # Override - skip leader discovery
    ):
        if not REDIS_AVAILABLE:
            raise ImportError("redis package not installed. Run: pip install redis")

        self._meta_core_path = meta_core_path or META_CORE_PATH
        self._files_path = files_path or FILES_PATH
        self._prefix = prefix or REDIS_PREFIX
        self._override_url = redis_url  # Direct Redis URL (skip leader discovery)

        self._leader_discovery: Optional[LeaderDiscovery] = None
        self._client: Optional[redis.Redis] = None
        self._connected = False
        self._lock = threading.Lock()

        # Callbacks
        self._on_ready_callbacks: List[Callable[[], None]] = []
        self._on_disconnect_callbacks: List[Callable[[], None]] = []

    def connect(self) -> None:
        """Connect to the storage backend via leader discovery."""
        # If direct URL provided, skip leader discovery
        if self._override_url:
            print(f"[LeaderStorage] Using direct Redis URL: {self._override_url}")
            self._connect_to_redis(self._override_url)
            return

        # Use leader discovery
        self._leader_discovery = LeaderDiscovery(
            meta_core_path=self._meta_core_path
        )

        # Set up callbacks
        self._leader_discovery.on_leader_found(self._on_leader_found)
        self._leader_discovery.on_leader_lost(self._on_leader_lost)

        # Start discovery
        self._leader_discovery.start()

    def disconnect(self) -> None:
        """Disconnect from the storage backend."""
        # Stop leader discovery
        if self._leader_discovery:
            self._leader_discovery.stop()
            self._leader_discovery = None

        # Disconnect Redis
        self._disconnect_redis()

    def is_connected(self) -> bool:
        """Check if connected to the storage backend."""
        with self._lock:
            if not self._connected or not self._client:
                return False
            try:
                self._client.ping()
                return True
            except Exception:
                self._connected = False
                return False

    def _on_leader_found(self, info: LeaderLockInfo) -> None:
        """Handle leader found event."""
        print(f"[LeaderStorage] Connecting to leader at {info.api}...")
        self._connect_to_redis(info.api)

    def _on_leader_lost(self) -> None:
        """Handle leader lost event."""
        print("[LeaderStorage] Leader lost, disconnecting...")
        self._disconnect_redis()
        self._notify_disconnect()

    def _connect_to_redis(self, url: str) -> None:
        """Connect to Redis."""
        with self._lock:
            try:
                # Disconnect existing client
                self._disconnect_redis_unlocked()

                # Create new client
                self._client = redis.from_url(url, decode_responses=True)
                self._client.ping()  # Test connection
                self._connected = True

                print(f"[LeaderStorage] Connected to Redis at {url}")

                # Notify ready
                self._notify_ready()

            except Exception as e:
                print(f"[LeaderStorage] Failed to connect to Redis: {e}")
                self._client = None
                self._connected = False

    def _disconnect_redis(self) -> None:
        """Disconnect from Redis."""
        with self._lock:
            self._disconnect_redis_unlocked()

    def _disconnect_redis_unlocked(self) -> None:
        """Disconnect from Redis (without lock)."""
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
        self._connected = False

    def _notify_ready(self) -> None:
        """Notify ready callbacks."""
        for callback in self._on_ready_callbacks:
            try:
                callback()
            except Exception as e:
                print(f"[LeaderStorage] Error in ready callback: {e}")

    def _notify_disconnect(self) -> None:
        """Notify disconnect callbacks."""
        for callback in self._on_disconnect_callbacks:
            try:
                callback()
            except Exception as e:
                print(f"[LeaderStorage] Error in disconnect callback: {e}")

    # ========================================================================
    # Event Registration
    # ========================================================================

    def on_ready(self, callback: Callable[[], None]) -> 'LeaderStorage':
        """Register callback for when storage is ready."""
        self._on_ready_callbacks.append(callback)
        if self._connected:
            callback()
        return self

    def on_disconnect(self, callback: Callable[[], None]) -> 'LeaderStorage':
        """Register callback for disconnection."""
        self._on_disconnect_callbacks.append(callback)
        return self

    # ========================================================================
    # Key Schema Helpers
    # ========================================================================

    def _get_file_key(self, hash_id: str) -> str:
        """Get the Redis key for a file hash."""
        return f"{self._prefix}file:{hash_id}"

    def _resolve_path(self, path: str) -> str:
        """Resolve path by prepending FILES_PATH if needed."""
        if not path:
            return path
        if path.startswith(self._files_path):
            return path
        if path.startswith('/'):
            return f"{self._files_path}{path}"
        return f"{self._files_path}/{path}"

    def _parse_nested_array(self, data: dict, prefix: str) -> list:
        """Parse nested array format (prefix/0, prefix/1, etc.) into a list."""
        items = []
        index = 0
        while True:
            key = f'{prefix}/{index}'
            if key in data:
                items.append(data[key])
                index += 1
            else:
                break
        return items

    def _parse_video(self, hash_id: str, data: dict) -> Optional[VideoMetadata]:
        """Parse Redis hash data into VideoMetadata.

        Handles both flat keys and nested keys (e.g., 'plot/eng' for description).
        meta-sort stores data using a flattened nested key format.
        """
        if not data:
            return None

        import json as jsonlib

        # Parse JSON fields
        audio_tracks = []
        subtitles = []
        genres = []
        studios = []

        if 'audioTracks' in data:
            try:
                audio_tracks = jsonlib.loads(data['audioTracks'])
            except (jsonlib.JSONDecodeError, TypeError):
                pass

        if 'subtitles' in data:
            try:
                subtitles = jsonlib.loads(data['subtitles'])
            except (jsonlib.JSONDecodeError, TypeError):
                pass

        # Parse genres - can be JSON array or nested keys (genres/0, genres/1, etc.)
        if 'genres' in data:
            try:
                genres = jsonlib.loads(data['genres'])
            except (jsonlib.JSONDecodeError, TypeError):
                if isinstance(data['genres'], str):
                    genres = [g.strip() for g in data['genres'].split(',') if g.strip()]
        else:
            # Try nested key format (genres/0, genres/1, etc.)
            genres = self._parse_nested_array(data, 'genres')

        # Parse studios - nested keys (studio/0, studio/1, etc.)
        studios = self._parse_nested_array(data, 'studio')

        # Parse numeric fields
        def parse_int(val):
            try:
                return int(val) if val else None
            except (ValueError, TypeError):
                return None

        def parse_float(val):
            try:
                return float(val) if val else None
            except (ValueError, TypeError):
                return None

        # Resolve file path with FILES_PATH
        file_path = data.get('filePath', data.get('sourcePath', ''))
        resolved_path = self._resolve_path(file_path)

        # Normalize video type: 'tvshow' -> 'series' for Stremio compatibility
        raw_type = data.get('type', data.get('videoType', 'movie'))
        video_type = 'series' if raw_type == 'tvshow' else raw_type

        # Get description from nested plot/eng or flat description/plot
        description = data.get('description')
        if not description:
            description = data.get('plot')
        if not description:
            # Try nested format: plot/eng, plot/en, etc.
            for lang in ['eng', 'en', 'english', 'und']:
                description = data.get(f'plot/{lang}')
                if description:
                    break

        # Parse rating (stored as string by TMDBProcessor)
        rating = parse_float(data.get('rating'))

        # Parse release date
        release_date = data.get('releasedate', data.get('releaseDate'))

        # Parse episode title - try episodeTitle or extract from titles/eng
        episode_title = data.get('episodeTitle', data.get('episodeName'))
        if not episode_title:
            episode_title = data.get('titles/eng', data.get('titles/en'))

        # Parse tagline
        tagline = data.get('tagline')

        # Get video/audio codec and resolution from various field formats
        # New format: stream/0, stream/1, etc. as JSON strings
        # Old format: fileinfo/streamdetails/video/0/... as flat keys
        video_codec = data.get('videoCodec')
        audio_codec = data.get('audioCodec')
        width = parse_int(data.get('width'))
        height = parse_int(data.get('height'))

        # Try new stream/* JSON format first
        if not video_codec or not width or not height or not audio_codec:
            for i in range(20):  # Check up to 20 streams
                stream_key = f'stream/{i}'
                stream_json = data.get(stream_key)
                if not stream_json:
                    break
                try:
                    stream = json.loads(stream_json) if isinstance(stream_json, str) else stream_json
                    stream_type = stream.get('type', '')
                    if stream_type == 'video' and not video_codec:
                        video_codec = stream.get('codec')
                        if not width:
                            width = parse_int(stream.get('width'))
                        if not height:
                            height = parse_int(stream.get('height'))
                    elif stream_type == 'audio' and not audio_codec:
                        audio_codec = stream.get('codec')
                except (json.JSONDecodeError, TypeError):
                    pass

        # Fall back to old flat key format
        if not video_codec:
            video_codec = data.get('fileinfo/streamdetails/video/0/codec')
        if not audio_codec:
            audio_codec = data.get('fileinfo/streamdetails/audio/0/codec')
        if not width:
            width = parse_int(data.get('fileinfo/streamdetails/video/0/width'))
        if not height:
            height = parse_int(data.get('fileinfo/streamdetails/video/0/height'))

        return VideoMetadata(
            hash_id=hash_id,
            file_path=resolved_path,
            title=data.get('title', data.get('originalTitle', '')),
            video_type=video_type,
            year=parse_int(data.get('year', data.get('movieYear'))),
            season=parse_int(data.get('season')),
            episode=parse_int(data.get('episode')),
            duration=parse_float(data.get('duration', data.get('fileinfo/duration'))),
            width=width,
            height=height,
            video_codec=video_codec,
            audio_codec=audio_codec,
            container=data.get('container'),
            file_size=parse_int(data.get('fileSize', data.get('sizeByte'))),
            audio_tracks=audio_tracks,
            subtitles=subtitles,
            imdb_id=data.get('imdbId', data.get('imdbid')),
            tmdb_id=data.get('tmdbId', data.get('tmdbid')),
            poster=data.get('poster') or data.get('posterUrl'),  # Fallback to posterUrl for backward compat
            backdrop=data.get('backdrop') or data.get('backdropUrl'),  # Fallback to backdropUrl
            poster_path=data.get('posterPath'),
            backdrop_path=data.get('backdropPath'),
            description=description,
            genres=genres,
            episode_title=episode_title,
            rating=rating,
            release_date=release_date,
            tagline=tagline,
            studios=studios,
        )

    # ========================================================================
    # StorageProvider Interface
    # ========================================================================

    def get_all_videos(self) -> List[VideoMetadata]:
        """Get all videos from storage."""
        if not self.is_connected():
            return []

        videos = []
        pattern = f"{self._prefix}file:*"

        try:
            for key in self._client.scan_iter(pattern, count=100):
                # Skip index key
                if '__index__' in key:
                    continue

                # Check file type - only include video files
                file_type = self._client.hget(key, 'fileType')
                if file_type != 'video':
                    continue

                hash_id = key.replace(f"{self._prefix}file:", "")
                data = self._client.hgetall(key)
                video = self._parse_video(hash_id, data)
                if video and video.file_path:
                    videos.append(video)

        except Exception as e:
            print(f"[LeaderStorage] Error getting all videos: {e}")

        # Sort by title
        videos.sort(key=lambda v: v.title.lower())
        return videos

    def get_video_by_hash(self, hash_id: str) -> Optional[VideoMetadata]:
        """Get a video by its hash ID."""
        if not self.is_connected():
            return None

        try:
            key = self._get_file_key(hash_id)
            data = self._client.hgetall(key)
            return self._parse_video(hash_id, data)
        except Exception as e:
            print(f"[LeaderStorage] Error getting video {hash_id}: {e}")
            return None

    def get_videos_by_type(self, video_type: str) -> List[VideoMetadata]:
        """Get all videos of a specific type."""
        videos = self.get_all_videos()
        return [v for v in videos if v.video_type == video_type]

    def search_videos(self, query: str) -> List[VideoMetadata]:
        """Search videos by title."""
        if not query:
            return self.get_all_videos()

        query_lower = query.lower()
        videos = self.get_all_videos()
        return [v for v in videos if query_lower in v.title.lower()]

    def get_video_by_imdb_id(self, imdb_id: str) -> Optional[VideoMetadata]:
        """Get a video by its IMDB ID (e.g., 'tt1727587')."""
        if not self.is_connected() or not imdb_id:
            return None

        # Normalize IMDB ID format
        imdb_id = imdb_id.lower()
        if not imdb_id.startswith('tt'):
            imdb_id = f"tt{imdb_id}"

        try:
            # Scan all videos looking for matching IMDB ID
            pattern = f"{self._prefix}file:*"
            for key in self._client.scan_iter(pattern, count=100):
                if '__index__' in key:
                    continue

                # Check imdbid/imdbId field
                stored_imdb = self._client.hget(key, 'imdbid') or self._client.hget(key, 'imdbId')
                if stored_imdb and stored_imdb.lower() == imdb_id:
                    hash_id = key.replace(f"{self._prefix}file:", "")
                    data = self._client.hgetall(key)
                    return self._parse_video(hash_id, data)

        except Exception as e:
            print(f"[LeaderStorage] Error finding video by IMDB ID {imdb_id}: {e}")

        return None

    def get_file_path_by_cid(self, cid: str) -> Optional[str]:
        """
        Get the file path for a file by its CID.

        Looks up file:{cid} in Redis and returns the path.
        Tries 'path' field first, then falls back to 'filePath'.
        This works for any file including poster images.

        Returns the relative path if found, None otherwise.
        """
        if not self.is_connected():
            return None

        try:
            key = self._get_file_key(cid)
            # Try 'path' field first (relative path)
            path = self._client.hget(key, 'path')
            if path:
                return path
            # Fall back to 'filePath' (full path from meta-sort)
            file_path = self._client.hget(key, 'filePath')
            if file_path:
                # Convert absolute path to relative by removing /files/ prefix
                if file_path.startswith('/files/'):
                    return file_path[7:]  # Remove '/files/'
                return file_path
            return None
        except Exception as e:
            print(f"[LeaderStorage] Error getting path for CID {cid}: {e}")
            return None

    def get_video_count(self) -> int:
        """Get total number of videos."""
        if not self.is_connected():
            return 0

        try:
            # Try index first
            index_key = f"{self._prefix}file:__index__"
            count = self._client.scard(index_key)
            if count > 0:
                return count

            # Fallback to scanning
            pattern = f"{self._prefix}file:*"
            count = 0
            for key in self._client.scan_iter(pattern, count=100):
                if '__index__' not in key:
                    count += 1
            return count

        except Exception as e:
            print(f"[LeaderStorage] Error counting videos: {e}")
            return 0

    def get_status(self) -> dict:
        """Get storage status for dashboard."""
        leader_info = None
        if self._leader_discovery:
            info = self._leader_discovery.get_leader_info()
            if info:
                leader_info = {
                    'host': info.host,
                    'api': info.api,
                    'http': info.http,
                }

        status = {
            'type': 'leader',
            'connected': self.is_connected(),
            'meta_core_path': self._meta_core_path,
            'files_path': self._files_path,
            'prefix': self._prefix,
            'video_count': 0,
            'leader': leader_info,
        }

        if status['connected']:
            try:
                status['video_count'] = self.get_video_count()
                info = self._client.info('memory')
                status['memory_used'] = info.get('used_memory_human', 'N/A')
            except Exception:
                pass

        return status

    # ========================================================================
    # Pub/Sub (Future enhancement)
    # ========================================================================

    def on_video_added(self, callback: Callable[[VideoMetadata], None]) -> None:
        """Subscribe to video added events."""
        # TODO: Implement pub/sub for real-time updates
        pass

    def on_video_removed(self, callback: Callable[[str], None]) -> None:
        """Subscribe to video removed events."""
        # TODO: Implement pub/sub for real-time updates
        pass
