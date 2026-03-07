"""
Service Registration for meta-stremio.

Python implementation of the service registration pattern used in meta-sort/meta-fuse.
Each service registers itself in /meta-core/services/{service-name}-{hostname}.json.

Service discovery is centralized in meta-core - this module only handles registration.
For service discovery, use the /api/services endpoint (fetches from meta-core).

Features:
- Service registration with heartbeat
- Automatic heartbeat loop
"""
from __future__ import annotations

import os
import json
import socket
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Callable
from urllib.request import urlopen
from urllib.error import URLError


# Configuration
META_CORE_PATH = os.environ.get('META_CORE_PATH', '/meta-core')
SERVICE_NAME = os.environ.get('SERVICE_NAME', 'meta-stremio')
SERVICE_VERSION = os.environ.get('SERVICE_VERSION', '1.0.0')
BASE_URL = os.environ.get('BASE_URL', '')


@dataclass
class ServiceInfo:
    """
    Service registration info stored in JSON file.

    Simplified format matching TypeScript ServiceInfo interface:
    - name: Service name (e.g., 'meta-stremio')
    - hostname: Container/host hostname
    - baseUrl: Base URL for the service (e.g., 'http://localhost:8182')
    - status: Current status ('running' | 'stale' | 'stopped')
    - lastHeartbeat: ISO timestamp of last heartbeat
    - role: Optional role ('leader' | 'follower') - only used by meta-core
    """
    name: str
    hostname: str
    baseUrl: str
    status: str = 'running'  # running | stale | stopped
    lastHeartbeat: str = ''
    role: Optional[str] = None  # "leader", "follower", or None (for non-meta-core services)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization.
        Excludes None values to avoid polluting JSON with null fields.
        """
        result = asdict(self)
        # Remove None values (e.g., role should only appear for meta-core)
        return {k: v for k, v in result.items() if v is not None}


class ServiceRegistration:
    """
    Service Discovery using shared filesystem.

    Each service registers itself in /meta-core/services/{service-name}-{hostname}.json
    and can discover other registered services.
    """

    def __init__(
        self,
        meta_core_path: str = None,
        service_name: str = None,
        version: str = None,
        api_url: str = None,
        base_url: str = None,
        capabilities: List[str] = None,
        endpoints: Dict[str, str] = None,
        heartbeat_interval: float = 30.0,
        stale_threshold: float = 60.0
    ):
        self.meta_core_path = meta_core_path or META_CORE_PATH
        self.service_name = service_name or SERVICE_NAME
        # Use base_url if provided, then api_url, then env BASE_URL, then auto-detect
        self.base_url = base_url or api_url or BASE_URL or self._get_default_api_url()
        self.heartbeat_interval = heartbeat_interval
        self.stale_threshold = stale_threshold

        # Keep for backwards compatibility but not used in simplified format
        self._version = version or SERVICE_VERSION
        self._capabilities = capabilities or []
        self._endpoints = endpoints or {}

        self.current_hostname = socket.gethostname()
        self.services_dir = os.path.join(self.meta_core_path, 'services')
        # Use hostname-based file naming like TypeScript services
        self.service_file_path = os.path.join(self.services_dir, f'{self.service_name}-{self.current_hostname}.json')

        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._is_started = False

    def _get_default_api_url(self) -> str:
        """Get default API URL using local IP and PORT."""
        port = os.environ.get('PORT', '7000')
        ip = self._get_local_ip()
        return f'http://{ip}:{port}'

    def _get_local_ip(self) -> str:
        """Get the local machine's IP address."""
        try:
            # Create a socket to determine the outbound IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return socket.gethostname() or 'localhost'

    def _ensure_services_dir(self) -> None:
        """Ensure services directory exists."""
        Path(self.services_dir).mkdir(parents=True, exist_ok=True)

    def _build_service_info(self, status: str = 'running') -> ServiceInfo:
        """Build service info for this service (simplified format)."""
        return ServiceInfo(
            name=self.service_name,
            hostname=self.current_hostname,
            baseUrl=self.base_url,
            status=status,
            lastHeartbeat=datetime.utcnow().isoformat() + 'Z'
        )

    def register(self) -> None:
        """Register this service."""
        self._ensure_services_dir()

        info = self._build_service_info('starting')

        with open(self.service_file_path, 'w') as f:
            json.dump(info.to_dict(), f, indent=2)

        print(f'[ServiceRegistration] Registered {self.service_name}')

    def update_status(self, status: str) -> None:
        """Update service status."""
        try:
            with open(self.service_file_path, 'r') as f:
                info = json.load(f)

            info['status'] = status
            info['lastHeartbeat'] = datetime.utcnow().isoformat() + 'Z'

            with open(self.service_file_path, 'w') as f:
                json.dump(info, f, indent=2)

        except Exception as e:
            print(f'[ServiceRegistration] Failed to update status: {e}')

    def heartbeat(self) -> None:
        """Send heartbeat (update lastHeartbeat timestamp)."""
        try:
            with open(self.service_file_path, 'r') as f:
                info = json.load(f)

            info['lastHeartbeat'] = datetime.utcnow().isoformat() + 'Z'

            with open(self.service_file_path, 'w') as f:
                json.dump(info, f, indent=2)

        except FileNotFoundError:
            # If file was deleted, re-register
            self.register()
            self.update_status('running')
        except Exception as e:
            print(f'[ServiceRegistration] Heartbeat failed: {e}')

    def _heartbeat_loop(self) -> None:
        """Background heartbeat loop."""
        while not self._stop_event.is_set():
            try:
                self.heartbeat()
            except Exception as e:
                print(f'[ServiceRegistration] Heartbeat error: {e}')

            self._stop_event.wait(self.heartbeat_interval)

    def start_heartbeat(self) -> None:
        """Start heartbeat loop in background thread."""
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return

        self._stop_event.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name='service-heartbeat'
        )
        self._heartbeat_thread.start()

    def stop_heartbeat(self) -> None:
        """Stop heartbeat loop."""
        self._stop_event.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=2.0)

    def unregister(self) -> None:
        """Unregister this service (on shutdown)."""
        self.stop_heartbeat()

        try:
            self.update_status('stopped')
            print(f'[ServiceRegistration] Unregistered {self.service_name}')
        except Exception as e:
            print(f'[ServiceRegistration] Failed to unregister: {e}')

    # ========================================================================
    # Lifecycle
    # ========================================================================

    def start(self) -> None:
        """Full startup sequence."""
        if self._is_started:
            return

        # Try to register, but don't fail if filesystem is read-only
        # This allows discovery-only mode when we can't write
        can_register = os.access(self.services_dir, os.W_OK) if os.path.exists(self.services_dir) else False

        if can_register:
            try:
                self.register()
                self.update_status('running')
                self.start_heartbeat()
                print(f'[ServiceRegistration] Started {self.service_name}-{self.current_hostname} at {self.base_url}')
            except Exception as e:
                print(f'[ServiceRegistration] Registration failed (read-only?): {e}')
                print(f'[ServiceRegistration] Running in discovery-only mode')
        else:
            print(f'[ServiceRegistration] Services directory not writable, running in discovery-only mode')

        self._is_started = True

    def stop(self) -> None:
        """Full shutdown sequence."""
        if not self._is_started:
            return

        self.unregister()
        self._is_started = False

    # ========================================================================
    # Getters
    # ========================================================================

    def get_service_file_path(self) -> str:
        return self.service_file_path

    def get_services_dir(self) -> str:
        return self.services_dir

    def is_started(self) -> bool:
        return self._is_started


# Singleton instance
_service_registration: Optional[ServiceRegistration] = None


def get_service_discovery() -> ServiceRegistration:
    """Get or create the service registration singleton.

    Note: Function name kept as 'get_service_discovery' for backward compatibility.
    """
    global _service_registration
    if _service_registration is None:
        _service_registration = ServiceRegistration()
    return _service_registration


def init_service_discovery(
    api_url: str = None,
    base_url: str = None,
    **kwargs
) -> ServiceRegistration:
    """Initialize and start the service registration singleton.

    Note: Function name kept as 'init_service_discovery' for backward compatibility.
    """
    global _service_registration
    _service_registration = ServiceRegistration(api_url=api_url, base_url=base_url, **kwargs)
    _service_registration.start()
    return _service_registration
