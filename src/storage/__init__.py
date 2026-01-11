"""
Storage abstraction layer for meta-stremio.

Provides a unified interface for reading video metadata from different backends:
- LeaderStorage: Discovers and connects to the KV leader (recommended)
- RedisStorage: Reads from Redis with direct URL (legacy)

Also provides service discovery for inter-service communication.
"""

from .provider import StorageProvider, VideoMetadata
from .redis_storage import RedisStorage
from .leader_storage import LeaderStorage
from .leader_discovery import LeaderDiscovery, LeaderLockInfo
from .service_discovery import (
    ServiceDiscovery,
    ServiceInfo,
    get_service_discovery,
    init_service_discovery
)

__all__ = [
    'StorageProvider',
    'VideoMetadata',
    'RedisStorage',
    'LeaderStorage',
    'LeaderDiscovery',
    'LeaderLockInfo',
    'ServiceDiscovery',
    'ServiceInfo',
    'get_service_discovery',
    'init_service_discovery',
]
