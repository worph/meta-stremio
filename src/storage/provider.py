"""
Abstract StorageProvider interface for video metadata.

meta-stremio is a read-only consumer - it does NOT write metadata.
All writes are handled by meta-sort.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Callable


@dataclass
class VideoMetadata:
    """Video metadata stored in KV by meta-sort, read by meta-stremio."""
    hash_id: str
    file_path: str
    title: str
    video_type: str = 'movie'  # 'movie' | 'series' | 'anime'
    year: Optional[int] = None
    season: Optional[int] = None
    episode: Optional[int] = None
    duration: Optional[float] = None
    width: Optional[int] = None
    height: Optional[int] = None
    video_codec: Optional[str] = None
    audio_codec: Optional[str] = None
    container: Optional[str] = None
    file_size: Optional[int] = None
    audio_tracks: List[dict] = field(default_factory=list)
    subtitles: List[dict] = field(default_factory=list)
    imdb_id: Optional[str] = None
    tmdb_id: Optional[str] = None
    poster: Optional[str] = None
    backdrop: Optional[str] = None
    poster_path: Optional[str] = None
    backdrop_path: Optional[str] = None
    description: Optional[str] = None
    genres: List[str] = field(default_factory=list)
    # New fields for improved listings
    episode_title: Optional[str] = None  # Title specific to this episode
    rating: Optional[float] = None  # TMDB/IMDB rating (0-10)
    release_date: Optional[str] = None  # Full release date (YYYY-MM-DD)
    tagline: Optional[str] = None  # Movie/series tagline
    studios: List[str] = field(default_factory=list)  # Production studios

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'hash_id': self.hash_id,
            'file_path': self.file_path,
            'title': self.title,
            'video_type': self.video_type,
            'year': self.year,
            'season': self.season,
            'episode': self.episode,
            'duration': self.duration,
            'width': self.width,
            'height': self.height,
            'video_codec': self.video_codec,
            'audio_codec': self.audio_codec,
            'container': self.container,
            'file_size': self.file_size,
            'audio_tracks': self.audio_tracks,
            'subtitles': self.subtitles,
            'imdb_id': self.imdb_id,
            'tmdb_id': self.tmdb_id,
            'poster': self.poster,
            'backdrop': self.backdrop,
            'poster_path': self.poster_path,
            'backdrop_path': self.backdrop_path,
            'description': self.description,
            'genres': self.genres,
            'episode_title': self.episode_title,
            'rating': self.rating,
            'release_date': self.release_date,
            'tagline': self.tagline,
            'studios': self.studios,
        }


class StorageProvider(ABC):
    """
    Abstract interface for video metadata storage.

    meta-stremio only reads - all writes are handled by meta-sort.
    """

    @abstractmethod
    def connect(self) -> None:
        """Connect to the storage backend."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Disconnect from the storage backend."""
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if connected to the storage backend."""
        pass

    @abstractmethod
    def get_all_videos(self) -> List[VideoMetadata]:
        """Get all videos from storage."""
        pass

    @abstractmethod
    def get_video_by_hash(self, hash_id: str) -> Optional[VideoMetadata]:
        """Get a video by its hash ID."""
        pass

    @abstractmethod
    def get_videos_by_type(self, video_type: str) -> List[VideoMetadata]:
        """Get all videos of a specific type (movie, series, anime)."""
        pass

    @abstractmethod
    def search_videos(self, query: str) -> List[VideoMetadata]:
        """Search videos by title."""
        pass

    @abstractmethod
    def get_video_by_imdb_id(self, imdb_id: str) -> Optional[VideoMetadata]:
        """Get a video by its IMDB ID (e.g., 'tt1727587')."""
        pass

    @abstractmethod
    def get_video_count(self) -> int:
        """Get total number of videos in storage."""
        pass

    def on_video_added(self, callback: Callable[[VideoMetadata], None]) -> None:
        """Subscribe to video added events (optional)."""
        pass

    def on_video_removed(self, callback: Callable[[str], None]) -> None:
        """Subscribe to video removed events (optional)."""
        pass

    def get_status(self) -> dict:
        """Get storage status for dashboard."""
        return {
            'connected': self.is_connected(),
            'video_count': self.get_video_count() if self.is_connected() else 0,
        }
