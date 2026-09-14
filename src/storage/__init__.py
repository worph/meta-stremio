"""
Storage abstraction layer for meta-stremio.

Provides a unified interface for reading video metadata from different backends:
- LeaderStorage: Discovers and connects to the KV leader (recommended)
- RedisStorage: Reads from Redis with direct URL (legacy)

Also provides service registration for inter-service communication.
Service discovery is centralized in meta-core.
"""

from .provider import StorageProvider, VideoMetadata
from .redis_storage import RedisStorage
from .leader_storage import LeaderStorage
from .leader_client import LeaderClient, LeaderLockInfo, get_leader_client

__all__ = [
    'StorageProvider',
    'VideoMetadata',
    'RedisStorage',
    'LeaderStorage',
    'LeaderClient',
    'LeaderLockInfo',
    'get_leader_client',
]
