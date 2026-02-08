"""
Redis storage provider for meta-stremio.

Reads video metadata from Redis (written by meta-sort).
Uses the same key schema as meta-sort for compatibility.
"""
from __future__ import annotations

import os
import json
from typing import List, Optional

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None

from .provider import StorageProvider, VideoMetadata


# Redis configuration
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')
REDIS_PREFIX = os.environ.get('REDIS_PREFIX', '')


class RedisStorage(StorageProvider):
    """
    Redis storage provider.

    Reads video metadata from Redis using flat key schema:
    - file:{hashId}/{field} -> String value for each metadata property
    """

    def __init__(self, url: str = None, prefix: str = None):
        if not REDIS_AVAILABLE:
            raise ImportError("redis package not installed. Run: pip install redis")

        self._url = url or REDIS_URL
        self._prefix = prefix or REDIS_PREFIX
        self._client: Optional[redis.Redis] = None
        self._connected = False

    def connect(self) -> None:
        """Connect to Redis."""
        try:
            self._client = redis.from_url(self._url, decode_responses=True)
            # Test connection
            self._client.ping()
            self._connected = True
            print(f"[RedisStorage] Connected to {self._url}")
        except Exception as e:
            self._connected = False
            print(f"[RedisStorage] Failed to connect: {e}")
            raise

    def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self._client:
            self._client.close()
            self._client = None
        self._connected = False
        print("[RedisStorage] Disconnected")

    def is_connected(self) -> bool:
        """Check if connected to Redis."""
        if not self._connected or not self._client:
            return False
        try:
            self._client.ping()
            return True
        except Exception:
            self._connected = False
            return False

    def get_file_path_by_cid(self, cid: str) -> Optional[str]:
        """
        Get the file path for a file by its CID.

        Looks up file:{cid}/* in Redis and returns the path.
        Tries 'path' field first, then falls back to 'filePath'.
        This works for any file including poster images.

        Returns the relative path if found, None otherwise.
        """
        if not self.is_connected():
            return None

        try:
            prefix = self._get_file_key_prefix(cid)
            # Try 'path' field first (relative path)
            path = self._client.get(f"{prefix}path")
            if path:
                return path if isinstance(path, str) else path.decode()
            # Fall back to 'filePath' (full path from meta-sort)
            file_path = self._client.get(f"{prefix}filePath")
            if file_path:
                file_path_str = file_path if isinstance(file_path, str) else file_path.decode()
                # Convert absolute path to relative by removing /files/ prefix
                if file_path_str.startswith('/files/'):
                    return file_path_str[7:]  # Remove '/files/'
                return file_path_str
            return None
        except Exception as e:
            print(f"[RedisStorage] Error getting path for CID {cid}: {e}")
            return None

    def _get_file_key_prefix(self, hash_id: str) -> str:
        """Get the Redis key prefix for a file's flat keys."""
        return f"{self._prefix}file:{hash_id}/"

    def _get_file_metadata(self, hash_id: str) -> dict:
        """Get all metadata for a file using flat key scan."""
        prefix = self._get_file_key_prefix(hash_id)
        data = {}

        for key in self._client.scan_iter(f"{prefix}*", count=100):
            key_str = key if isinstance(key, str) else key.decode()
            field = key_str[len(prefix):]
            value = self._client.get(key)
            if value:
                data[field] = value if isinstance(value, str) else value.decode()

        return data

    def _parse_video(self, hash_id: str, data: dict) -> Optional[VideoMetadata]:
        """Parse Redis hash data into VideoMetadata.

        Handles both flat keys and nested keys (e.g., 'plot/eng' for description).
        meta-sort stores data using a flattened nested key format.
        """
        if not data:
            return None

        # Parse JSON fields
        audio_tracks = []
        subtitles = []
        genres = []
        studios = []

        if 'audioTracks' in data:
            try:
                audio_tracks = json.loads(data['audioTracks'])
            except (json.JSONDecodeError, TypeError):
                pass

        if 'subtitles' in data:
            try:
                subtitles = json.loads(data['subtitles'])
            except (json.JSONDecodeError, TypeError):
                pass

        # Parse genres - can be JSON array or nested keys (genres/0, genres/1, etc.)
        if 'genres' in data:
            try:
                genres = json.loads(data['genres'])
            except (json.JSONDecodeError, TypeError):
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

        # Parse episode title (for series episodes)
        episode_title = data.get('episodeTitle', data.get('episodeName'))

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

        duration = parse_float(data.get('duration'))
        if not duration:
            duration = parse_float(data.get('fileinfo/duration'))

        return VideoMetadata(
            hash_id=hash_id,
            file_path=data.get('filePath', ''),
            title=data.get('title', data.get('originalTitle', '')),
            video_type=data.get('type', data.get('videoType', 'movie')),
            year=parse_int(data.get('year', data.get('movieYear'))),
            season=parse_int(data.get('season')),
            episode=parse_int(data.get('episode')),
            duration=duration,
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

    def get_all_videos(self) -> List[VideoMetadata]:
        """Get all videos from Redis."""
        if not self.is_connected():
            return []

        videos = []

        try:
            # Get all hash IDs from index
            index_key = f"{self._prefix}file:__index__"
            hash_ids = self._client.smembers(index_key)

            for hash_id in hash_ids:
                hash_id_str = hash_id if isinstance(hash_id, str) else hash_id.decode()

                # Get all metadata for this file
                data = self._get_file_metadata(hash_id_str)

                # Check file type - only include video files
                if not data or data.get('fileType') != 'video':
                    continue

                video = self._parse_video(hash_id_str, data)
                if video and video.file_path:
                    videos.append(video)

        except Exception as e:
            print(f"[RedisStorage] Error getting all videos: {e}")

        # Sort by title
        videos.sort(key=lambda v: v.title.lower())
        return videos

    def get_video_by_hash(self, hash_id: str) -> Optional[VideoMetadata]:
        """Get a video by its hash ID."""
        if not self.is_connected():
            return None

        try:
            data = self._get_file_metadata(hash_id)
            return self._parse_video(hash_id, data) if data else None
        except Exception as e:
            print(f"[RedisStorage] Error getting video {hash_id}: {e}")
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
            # Get from index and check each file's imdbid
            index_key = f"{self._prefix}file:__index__"
            for hash_id in self._client.smembers(index_key):
                hash_id_str = hash_id if isinstance(hash_id, str) else hash_id.decode()
                prefix = self._get_file_key_prefix(hash_id_str)

                # Check imdbid field directly
                stored_imdb = self._client.get(f"{prefix}imdbid") or self._client.get(f"{prefix}imdbId")
                if stored_imdb:
                    stored_imdb_str = stored_imdb if isinstance(stored_imdb, str) else stored_imdb.decode()
                    if stored_imdb_str.lower() == imdb_id:
                        data = self._get_file_metadata(hash_id_str)
                        return self._parse_video(hash_id_str, data)

        except Exception as e:
            print(f"[RedisStorage] Error finding video by IMDB ID {imdb_id}: {e}")

        return None

    def get_video_count(self) -> int:
        """Get total number of videos."""
        if not self.is_connected():
            return 0

        try:
            # Use index set for accurate count
            index_key = f"{self._prefix}file:__index__"
            return self._client.scard(index_key)
        except Exception as e:
            print(f"[RedisStorage] Error counting videos: {e}")
            return 0

    def get_status(self) -> dict:
        """Get storage status for dashboard."""
        status = {
            'type': 'redis',
            'connected': self.is_connected(),
            'url': self._url,
            'prefix': self._prefix,
            'video_count': 0,
        }

        if status['connected']:
            try:
                status['video_count'] = self.get_video_count()
                info = self._client.info('memory')
                status['memory_used'] = info.get('used_memory_human', 'N/A')
            except Exception:
                pass

        return status
