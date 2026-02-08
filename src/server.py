#!/usr/bin/env python3
"""
Meta-Stremio Server

A standalone Stremio addon that:
1. Reads video metadata from KV storage (written by meta-sort)
2. Transcodes video on-the-fly using FFmpeg with adaptive quality
3. Serves HLS streams with muxed video+audio segments
4. Provides a dashboard for monitoring and setup
5. Registers with service discovery for inter-service navigation

Environment Variables:
- MEDIA_DIR: Directory containing video files (default: /files/watch)
- CACHE_DIR: Directory for transcoded segments (default: /data/cache)
- PORT: HTTP server port (default: 7000)
- META_CORE_PATH: Path to meta-core shared volume (default: /meta-core)
- REDIS_URL: Redis connection URL (default: redis://localhost:6379)
- REDIS_PREFIX: Redis key prefix (default: empty)
- SCHEME: URL scheme for generated URLs - http/https/auto (default: auto)
- SEGMENT_DURATION: HLS segment length in seconds (default: 4)
- PREFETCH_SEGMENTS: How many segments to prefetch ahead (default: 4)
"""
from __future__ import annotations

import os
import re
import json
import signal
import sys
import base64
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, unquote
import threading

import stremio
import transcoder
from storage import init_service_discovery, get_service_discovery
import fileserver
import webdav_client

# Configuration
PORT = int(os.environ.get('PORT', '7000'))
SCHEME = os.environ.get('SCHEME', 'auto').lower().strip()
META_CORE_PATH = os.environ.get('META_CORE_PATH', '/meta-core')
BASE_URL = os.environ.get('BASE_URL', '')

# Initialize storage
storage = stremio.init_storage()

# Initialize service discovery (only if META_CORE_PATH exists)
service_discovery = None
if os.path.exists(META_CORE_PATH):
    try:
        service_discovery = init_service_discovery(base_url=BASE_URL if BASE_URL else None)
    except Exception as e:
        print(f"[Server] Service discovery init failed: {e}")

# Global Stremio handler
stremio_handler = stremio.StremioHandler()


def parse_config_from_path(path: str) -> tuple[str, dict]:
    """
    Parse Stremio config from URL path.

    Stremio passes config as URL-safe base64-encoded JSON in the path:
    /{config}/manifest.json -> config dict
    /manifest.json -> empty dict

    Returns:
        (remaining_path, config_dict)
    """
    # Check if path starts with a potential config segment
    # Config paths look like: /eyJsYW5n.../manifest.json
    parts = path.strip('/').split('/', 1)

    if len(parts) >= 1 and parts[0]:
        potential_config = parts[0]

        # Skip known non-config paths
        if potential_config in ('manifest.json', 'catalog', 'meta', 'stream', 'stremio',
                                 'health', 'api', 'transcode', 'direct', 'poster', 'configure'):
            return path, {}

        # Try to decode as base64 config
        try:
            # Add padding if needed
            padded = potential_config + '=' * (4 - len(potential_config) % 4)
            decoded = base64.urlsafe_b64decode(padded).decode('utf-8')
            config = json.loads(decoded)

            # Return remaining path with leading slash
            remaining = '/' + parts[1] if len(parts) > 1 else '/'
            return remaining, config
        except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
            # Not a valid config, return original path
            pass

    return path, {}


def encode_config(config: dict) -> str:
    """Encode config dict to URL-safe base64 string."""
    json_str = json.dumps(config, separators=(',', ':'))
    encoded = base64.urlsafe_b64encode(json_str.encode('utf-8')).decode('utf-8')
    return encoded.rstrip('=')


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{self.address_string()}] {fmt % args}")

    def send_json(self, data: dict, status: int = 200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def send_data(self, data: bytes, content_type: str):
        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', len(data))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, HEAD, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Range')
        self.end_headers()

    def do_HEAD(self):
        parsed = urlparse(self.path)
        path = unquote(parsed.path)

        if path.startswith('/stremio/') or path == '/' or path.startswith('/manifest.json'):
            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            if path.endswith('.json'):
                self.send_header('Content-Type', 'application/json')
            else:
                self.send_header('Content-Type', 'text/html')
            self.end_headers()
        elif path.startswith('/transcode/') or path.startswith('/direct/'):
            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            if path.endswith('.m3u8'):
                self.send_header('Content-Type', 'application/vnd.apple.mpegurl')
            elif path.endswith('.ts'):
                self.send_header('Content-Type', 'video/mp2t')
            elif path.endswith('.vtt'):
                self.send_header('Content-Type', 'text/vtt')
            else:
                self.send_header('Content-Type', 'application/octet-stream')
            self.end_headers()
        elif path.startswith('/file/') or path.startswith('/poster/'):
            # Generic file serving by CID - /file/{cid} or /poster/{cid} (backward compat)
            m = re.match(r'^/(?:file|poster)/([a-zA-Z0-9]+)(?:/w(\d+))?$', path)
            if m:
                cid = m.group(1)
                # Check if file exists by looking up CID in metadata
                file_path = fileserver.get_file_path(cid)
                if file_path and webdav_client.file_exists(file_path):
                    self.send_response(200)
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.send_header('Content-Type', 'image/jpeg')
                    self.send_header('Cache-Control', 'public, max-age=604800')
                    self.end_headers()
                else:
                    self.send_error(404, f"File not found: {cid}")
            else:
                self.send_error(404, "Invalid file path")
        else:
            self.send_error(404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = unquote(parsed.path)

        # Reset metrics
        if path == '/transcode/reset-metrics':
            transcoder.reset_metrics()
            return self.send_json({'status': 'ok'})

        self.send_error(404)

    def do_GET(self):
        parsed = urlparse(self.path)
        raw_path = unquote(parsed.path)

        # Parse config from path (e.g., /eyJsYW5n.../manifest.json)
        path, config = parse_config_from_path(raw_path)

        # Root - serve setup page
        if path == '/' or path == '/index.html':
            return self.serve_setup_page()

        # Configure page
        if path == '/configure':
            return self.serve_configure_page()

        # API: Get supported languages
        if path == '/api/languages':
            return self.send_json({
                'languages': [{'code': code, 'name': name} for code, name in stremio.get_supported_languages()],
            })

        # Health check
        if path == '/health':
            storage = stremio.get_storage()
            return self.send_json({
                'status': 'ok',
                'storage': storage.get_status() if storage else {'connected': False},
            })

        # === Dashboard API ===

        # Library stats
        if path == '/api/stats':
            stats = stremio.get_library_stats()
            return self.send_json(stats)

        # Library list
        if path == '/api/library':
            storage = stremio.get_storage()
            videos = storage.get_all_videos() if storage else []
            return self.send_json({
                'videos': [v.to_dict() for v in videos],
                'count': len(videos),
            })

        # Discovered services (for inter-service navigation)
        if path == '/api/services':
            return self.handle_services_api()

        # === File API ===
        # /file/{cid} or /poster/{cid} (backward compat) - serve file by CID
        # /file/{cid}/w{width} - serve resized image
        m = re.match(r'^/(?:file|poster)/([a-zA-Z0-9]+)(?:/w(\d+))?$', path)
        if m:
            return self.handle_file(m.group(1), int(m.group(2)) if m.group(2) else None)

        # === Stremio manifest ===
        if path == '/manifest.json' or path == '/stremio/manifest.json':
            return self.handle_stremio_manifest(config)

        # Stremio catalog
        m = re.match(r'^(?:/stremio)?/catalog/(\w+)/([^/]+)(?:/([^.]+))?\.json$', path)
        if m:
            return self.handle_stremio_catalog(m.group(1), m.group(2), m.group(3), config)

        # Stremio meta
        m = re.match(r'^(?:/stremio)?/meta/(\w+)/([^/]+)\.json$', path)
        if m:
            return self.handle_stremio_meta(m.group(1), m.group(2), config)

        # Stremio stream
        m = re.match(r'^(?:/stremio)?/stream/(\w+)/([^/]+)\.json$', path)
        if m:
            return self.handle_stremio_stream(m.group(1), m.group(2), config)

        # === Transcoder endpoints ===

        # Metrics
        if path == '/transcode/metrics':
            metrics = transcoder.get_metrics()
            metrics['total_files'] = stremio.get_storage().get_video_count()
            return self.send_json(metrics)

        # Direct file serving
        m = re.match(r'^/direct/(.+)$', path)
        if m:
            return self.handle_direct_file(m.group(1))

        # Master playlist variants
        m = re.match(r'^/transcode/(.+?)/master_(\w+)_a(\d+)\.m3u8$', path)
        if m:
            return self.handle_master_playlist(m.group(1), m.group(2), int(m.group(3)))

        m = re.match(r'^/transcode/(.+?)/master_a(\d+)\.m3u8$', path)
        if m:
            return self.handle_master_playlist(m.group(1), None, int(m.group(2)))

        m = re.match(r'^/transcode/(.+?)/master\.m3u8$', path)
        if m:
            return self.handle_master_playlist(m.group(1))

        m = re.match(r'^/transcode/(.+?)/master_(\w+)\.m3u8$', path)
        if m:
            return self.handle_master_playlist(m.group(1), m.group(2))

        # Stream playlist
        m = re.match(r'^/transcode/(.+?)/stream_a(\d+)_(\w+)\.m3u8$', path)
        if m:
            return self.handle_stream_playlist(m.group(1), int(m.group(2)), m.group(3))

        # Segment
        m = re.match(r'^/transcode/(.+?)/seg_a(\d+)_(\w+)_(\d+)\.ts$', path)
        if m:
            return self.handle_segment(m.group(1), int(m.group(2)), m.group(3), int(m.group(4)))

        # Subtitle playlist
        m = re.match(r'^/transcode/(.+?)/subtitle_(\d+)\.m3u8$', path)
        if m:
            return self.handle_subtitle_playlist(m.group(1), int(m.group(2)))

        # Subtitle VTT
        m = re.match(r'^/transcode/(.+?)/subtitle_(\d+)\.vtt$', path)
        if m:
            return self.handle_subtitle_vtt(m.group(1), int(m.group(2)))

        self.send_error(404)

    def serve_setup_page(self):
        """Serve the setup/dashboard page."""
        html_path = os.path.join(os.path.dirname(__file__), 'www', 'index.html')
        if not os.path.exists(html_path):
            # Try parent directory
            html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'www', 'index.html')

        if os.path.exists(html_path):
            with open(html_path, 'r', encoding='utf-8') as f:
                content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.send_header('Content-Length', len(content.encode('utf-8')))
            self.end_headers()
            self.wfile.write(content.encode('utf-8'))
        else:
            # Fallback minimal page
            content = f"""<!DOCTYPE html>
<html>
<head><title>Meta-Stremio</title></head>
<body style="font-family: sans-serif; background: #1a1a2e; color: #fff; padding: 2rem;">
<h1>Meta-Stremio</h1>
<p>Install URL: <code>{self.get_base_url()}/manifest.json</code></p>
<p><a href="stremio://{self.headers.get('Host', 'localhost')}/manifest.json" style="color: #4dabf7;">Install in Stremio</a></p>
</body>
</html>"""
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(content.encode('utf-8'))

    def serve_configure_page(self):
        """Serve the configuration page for language settings."""
        html_path = os.path.join(os.path.dirname(__file__), 'www', 'configure.html')
        if not os.path.exists(html_path):
            html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'www', 'configure.html')

        if os.path.exists(html_path):
            with open(html_path, 'r', encoding='utf-8') as f:
                content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.send_header('Content-Length', len(content.encode('utf-8')))
            self.end_headers()
            self.wfile.write(content.encode('utf-8'))
        else:
            # Inline configure page (fallback)
            base_url = self.get_base_url()
            host = self.headers.get('Host', 'localhost')
            languages = stremio.get_supported_languages()

            lang_options = '\n'.join(
                f'<option value="{code}">{name}</option>'
                for code, name in languages
            )

            content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Configure Meta-Stremio</title>
    <style>
        :root {{
            --bg-primary: #0a0a0f;
            --bg-secondary: #12121a;
            --bg-tertiary: #1a1a2e;
            --text-primary: #e0e0e0;
            --text-secondary: #a0a0a0;
            --accent-primary: #4ecdc4;
            --border-color: #2a2a3e;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            padding: 2rem;
            display: flex;
            justify-content: center;
            align-items: center;
        }}
        .container {{
            max-width: 500px;
            width: 100%;
        }}
        h1 {{
            font-size: 1.8rem;
            margin-bottom: 0.5rem;
            background: linear-gradient(135deg, var(--accent-primary), #ff6b6b);
            -webkit-background-clip: text;
            background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        .subtitle {{ color: var(--text-secondary); margin-bottom: 2rem; }}
        .card {{
            background: var(--bg-secondary);
            border-radius: 12px;
            padding: 1.5rem;
            border: 1px solid var(--border-color);
            margin-bottom: 1.5rem;
        }}
        .form-group {{ margin-bottom: 1.5rem; }}
        .form-group:last-child {{ margin-bottom: 0; }}
        label {{
            display: block;
            margin-bottom: 0.5rem;
            font-weight: 500;
        }}
        .hint {{ font-size: 0.85rem; color: var(--text-secondary); margin-top: 0.25rem; }}
        select {{
            width: 100%;
            padding: 0.75rem;
            border-radius: 8px;
            border: 1px solid var(--border-color);
            background: var(--bg-tertiary);
            color: var(--text-primary);
            font-size: 1rem;
        }}
        .url-box {{
            background: var(--bg-tertiary);
            border-radius: 8px;
            padding: 1rem;
            font-family: monospace;
            font-size: 0.85rem;
            word-break: break-all;
            margin-bottom: 1rem;
            border: 1px solid var(--border-color);
            color: var(--accent-primary);
        }}
        .btn {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 0.5rem;
            padding: 0.75rem 1.5rem;
            border-radius: 8px;
            text-decoration: none;
            font-weight: 600;
            font-size: 0.9rem;
            border: none;
            cursor: pointer;
            width: 100%;
            margin-bottom: 0.75rem;
        }}
        .btn-primary {{
            background: var(--accent-primary);
            color: var(--bg-primary);
        }}
        .btn-secondary {{
            background: var(--bg-tertiary);
            color: var(--text-primary);
            border: 1px solid var(--border-color);
        }}
        .warning {{
            background: rgba(255, 217, 61, 0.15);
            color: #ffd93d;
            padding: 0.75rem;
            border-radius: 8px;
            font-size: 0.85rem;
            margin-bottom: 1rem;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Configure Meta-Stremio</h1>
        <p class="subtitle">Set your language preferences for metadata display</p>


        <div class="card">
            <div class="form-group">
                <label for="displayLang">Display Language</label>
                <select id="displayLang">
                    {lang_options}
                </select>
                <p class="hint">Language preference for titles (uses metadata from meta-sort's TMDB plugin)</p>
            </div>
        </div>

        <div class="card">
            <p style="margin-bottom: 0.5rem; font-weight: 500;">Install URL:</p>
            <div class="url-box" id="installUrl">{base_url}/manifest.json</div>

            <a href="#" id="installBtn" class="btn btn-primary">Install in Stremio</a>
            <button onclick="copyUrl()" class="btn btn-secondary">Copy URL</button>
        </div>
    </div>

    <script>
        const baseUrl = '{base_url}';
        const host = '{host}';

        function encodeConfig(config) {{
            const json = JSON.stringify(config);
            // URL-safe base64 encode
            const encoded = btoa(json).replace(/\\+/g, '-').replace(/\\//g, '_').replace(/=+$/, '');
            return encoded;
        }}

        function updateUrl() {{
            const displayLang = document.getElementById('displayLang').value;

            let url, stremioUrl;
            if (displayLang === 'en') {{
                // No config needed for default
                url = baseUrl + '/manifest.json';
                stremioUrl = 'stremio://' + host + '/manifest.json';
            }} else {{
                const config = {{ displayLanguage: displayLang }};
                const encoded = encodeConfig(config);
                url = baseUrl + '/' + encoded + '/manifest.json';
                stremioUrl = 'stremio://' + host + '/' + encoded + '/manifest.json';
            }}

            document.getElementById('installUrl').textContent = url;
            document.getElementById('installBtn').href = stremioUrl;
        }}

        function copyUrl() {{
            const url = document.getElementById('installUrl').textContent;
            navigator.clipboard.writeText(url).then(() => {{
                const btn = event.target;
                const original = btn.textContent;
                btn.textContent = 'Copied!';
                setTimeout(() => btn.textContent = original, 2000);
            }});
        }}

        // Update URL when language changes
        document.getElementById('displayLang').addEventListener('change', updateUrl);

        // Try to detect browser language
        const browserLang = navigator.language.split('-')[0];
        const langSelect = document.getElementById('displayLang');
        for (const opt of langSelect.options) {{
            if (opt.value === browserLang) {{
                opt.selected = true;
                break;
            }}
        }}

        // Initial URL update
        updateUrl();
    </script>
</body>
</html>"""
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(content.encode('utf-8'))

    def handle_services_api(self):
        """Return list of discovered services for dashboard navigation."""
        services = []
        leader_info = None

        try:
            sd = get_service_discovery()
            if sd and sd.is_started():
                all_services = sd.discover_all_services()

                for svc in all_services:
                    # Filter out follower meta-core instances (only show leader)
                    svc_name = svc.get('name', '')
                    svc_role = svc.get('role', '')
                    if svc_name == 'meta-core' and svc_role != 'leader':
                        continue

                    # Build dashboard URL from base URL (supports both 'baseUrl' and legacy 'api')
                    api_url = svc.get('baseUrl', '') or svc.get('api', '')
                    dashboard_path = svc.get('endpoints', {}).get('dashboard', '/')

                    services.append({
                        'name': svc_name or 'Unknown',
                        'url': api_url + dashboard_path if api_url else '',
                        'api': api_url,
                        'status': svc.get('status', 'unknown'),
                        'capabilities': svc.get('capabilities', []),
                        'version': svc.get('version', ''),
                        'role': svc_role or None,
                    })
        except Exception as e:
            print(f"[Server] Error discovering services: {e}")

        # Get leader info from leader storage
        try:
            storage = stremio.get_storage()
            if storage and hasattr(storage, '_leader_discovery'):
                discovery = storage._leader_discovery
                if discovery:
                    info = discovery.get_leader_info()
                    if info:
                        leader_info = {
                            'host': info.hostname if hasattr(info, 'hostname') else info.get('hostname', ''),
                            'api': info.api_url if hasattr(info, 'api_url') else info.get('apiUrl', ''),
                            'http': info.base_url if hasattr(info, 'base_url') else info.get('baseUrl', ''),
                        }
        except Exception as e:
            print(f"[Server] Error getting leader info: {e}")

        return self.send_json({
            'services': services,
            'current': 'meta-stremio',
            'leader': leader_info,
            'isLeader': False,  # meta-stremio is never the leader
        })

    def handle_file(self, cid: str, width: int = None):
        """Serve a file by CID with optional resizing (for images)."""
        file_data, content_type, status = fileserver.serve_file(cid, width)

        if status == 200 and file_data:
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', len(file_data))
            self.send_header('Cache-Control', 'public, max-age=604800')  # 7 days
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(file_data)
        elif status == 404:
            self.send_error(404, f"File not found: {cid}")
        else:
            self.send_error(status, "Error serving file")

    def handle_stremio_manifest(self, config: dict = None):
        host = self.get_host()
        # Set base URL for poster URLs in responses
        stremio.set_base_url(self.get_base_url())
        data, content_type = stremio_handler.handle_manifest(host, config)
        self.send_data(data, content_type)

    def get_host(self) -> str:
        host = self.headers.get('X-Forwarded-Host') or self.headers.get('Host', 'localhost')
        if ':' in host and not host.startswith('['):
            host = host.rsplit(':', 1)[0]
        return host

    def handle_stremio_catalog(self, catalog_type: str, catalog_id: str, extra: str = None, config: dict = None):
        extra_dict = {}
        if extra:
            for param in extra.split('&'):
                if '=' in param:
                    key, value = param.split('=', 1)
                    extra_dict[key] = unquote(value)

        # Set base URL for poster URLs in responses
        stremio.set_base_url(self.get_base_url())
        data, content_type = stremio_handler.handle_catalog(catalog_type, catalog_id, extra_dict, config)
        self.send_data(data, content_type)

    def handle_stremio_meta(self, meta_type: str, meta_id: str, config: dict = None):
        # Set base URL for poster URLs in responses
        stremio.set_base_url(self.get_base_url())
        data, content_type = stremio_handler.handle_meta(meta_type, meta_id, config)
        if data:
            self.send_data(data, content_type)
        else:
            self.send_error(404, "Video not found")

    def handle_stremio_stream(self, stream_type: str, stream_id: str, config: dict = None):
        base_url = self.get_base_url()
        # Set base URL for poster URLs in responses
        stremio.set_base_url(base_url)
        data, content_type = stremio_handler.handle_stream(stream_type, stream_id, base_url, config)
        if data:
            self.send_data(data, content_type)
        else:
            self.send_error(404, "Video not found")

    # === Transcoder handlers ===

    def get_file_info(self, filepath: str):
        # Try multiple path resolutions:
        # 1. Relative to MEDIA_DIR (e.g., /files/watch/filename.mp4)
        # 2. Absolute path starting with /files/ (e.g., /files/test/filename.mp4)
        # 3. Prepend /files/ if path looks like files/... (from URL encoding)
        candidates = [
            os.path.join(transcoder.MEDIA_DIR, filepath),  # Relative to MEDIA_DIR
            '/' + filepath if not filepath.startswith('/') else filepath,  # Make absolute
        ]

        full_path = None
        for candidate in candidates:
            # Check if file exists (works with both local and WebDAV)
            if webdav_client.file_exists(candidate):
                full_path = candidate
                break

        if not full_path:
            self.send_error(404, f"File not found: {filepath}")
            return None, None, None

        info = transcoder.get_video_info(full_path)
        if not info:
            self.send_error(500, "Could not probe file")
            return None, None, None

        return full_path, transcoder.get_file_hash(filepath), info

    def handle_master_playlist(self, filepath: str, resolution: str = None, audio: int = None):
        full_path, file_hash, info = self.get_file_info(filepath)
        if not info:
            return

        playlist = transcoder.generate_master_playlist(info, resolution, audio)
        self.send_data(playlist.encode(), 'application/vnd.apple.mpegurl')

    def handle_stream_playlist(self, filepath: str, audio: int, resolution: str):
        full_path, file_hash, info = self.get_file_info(filepath)
        if not info:
            return

        playlist = transcoder.generate_stream_playlist(info, audio, resolution)
        self.send_data(playlist.encode(), 'application/vnd.apple.mpegurl')

    def handle_segment(self, filepath: str, audio: int, resolution: str, segment: int):
        full_path, file_hash, info = self.get_file_info(filepath)
        if not info:
            return

        video_codec, audio_codec = transcoder.extract_codecs(info)
        transcoder.segment_manager.set_codec_info(video_codec, audio_codec, full_path)

        data = transcoder.get_or_transcode_segment(full_path, file_hash, audio, resolution, segment, info)
        if data:
            self.send_data(data, 'video/mp2t')
        else:
            self.send_error(500, "Transcode failed")

    def handle_subtitle_playlist(self, filepath: str, sub_index: int):
        full_path, file_hash, info = self.get_file_info(filepath)
        if not info:
            return

        playlist = transcoder.generate_subtitle_playlist(info, sub_index)
        self.send_data(playlist.encode(), 'application/vnd.apple.mpegurl')

    def handle_subtitle_vtt(self, filepath: str, sub_index: int):
        full_path, file_hash, info = self.get_file_info(filepath)
        if not info:
            return

        key = f"{file_hash}:sub:{sub_index}"
        content, error = transcoder.subtitle_manager.get_subtitle(key, full_path, file_hash, sub_index, info)

        if content:
            self.send_data(content.encode('utf-8'), 'text/vtt')
        else:
            error_vtt = f"WEBVTT\n\nNOTE Subtitle extraction failed: {error or 'Unknown error'}\n"
            self.send_data(error_vtt.encode('utf-8'), 'text/vtt')

    def handle_direct_file(self, filepath: str):
        # Try multiple path resolutions (same logic as get_file_info)
        candidates = [
            os.path.join(transcoder.MEDIA_DIR, filepath),  # Relative to MEDIA_DIR
            '/' + filepath if not filepath.startswith('/') else filepath,  # Make absolute
        ]

        full_path = None
        for candidate in candidates:
            # Check if file exists (works with both local and WebDAV)
            if webdav_client.file_exists(candidate):
                full_path = candidate
                break

        if not full_path:
            self.send_error(404, f"File not found: {filepath}")
            return

        ext = os.path.splitext(filepath)[1].lower()
        content_types = {
            '.mp4': 'video/mp4',
            '.mkv': 'video/x-matroska',
            '.webm': 'video/webm',
            '.avi': 'video/x-msvideo',
            '.mov': 'video/quicktime',
            '.m4v': 'video/x-m4v',
            '.ts': 'video/mp2t',
            '.m2ts': 'video/mp2t',
        }
        content_type = content_types.get(ext, 'application/octet-stream')

        # Get file size (works with both local and WebDAV)
        file_size = webdav_client.get_file_size(full_path)
        if file_size is None:
            self.send_error(500, f"Could not get file size: {filepath}")
            return

        range_header = self.headers.get('Range')

        if range_header:
            range_match = re.match(r'bytes=(\d*)-(\d*)', range_header)
            if range_match:
                start = int(range_match.group(1)) if range_match.group(1) else 0
                end = int(range_match.group(2)) if range_match.group(2) else file_size - 1
                end = min(end, file_size - 1)
                length = end - start + 1

                self.send_response(206)
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Length', length)
                self.send_header('Content-Range', f'bytes {start}-{end}/{file_size}')
                self.send_header('Accept-Ranges', 'bytes')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()

                # Stream the range (works with both local and WebDAV)
                for chunk in webdav_client.stream_range(full_path, start, end, file_size):
                    self.wfile.write(chunk)
                return

        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', file_size)
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        # Stream the entire file (works with both local and WebDAV)
        for chunk in webdav_client.stream_file(full_path):
            self.wfile.write(chunk)

    def get_base_url(self) -> str:
        host = self.headers.get('X-Forwarded-Host') or self.headers.get('Host', 'localhost')

        # SCHEME can be 'http', 'https', 'auto', or empty
        # 'auto' or empty means auto-detect based on host
        if SCHEME and SCHEME not in ('auto', ''):
            proto = SCHEME
        else:
            is_localhost = 'localhost' in host or '127.0.0.1' in host or '::1' in host
            proto = 'http' if is_localhost else 'https'

        clean_host = host.replace(':80', '').replace(':443', '')
        return f"{proto}://{clean_host}"


class ThreadedServer(HTTPServer):
    def process_request(self, request, client_address):
        t = threading.Thread(target=self._handle, args=(request, client_address), daemon=True)
        t.start()

    def _handle(self, request, client_address):
        try:
            self.finish_request(request, client_address)
        except Exception:
            self.handle_error(request, client_address)
        finally:
            self.shutdown_request(request)


def shutdown_handler(signum, frame):
    """Handle graceful shutdown."""
    print("\n[Server] Shutting down...")

    # Stop service discovery
    try:
        sd = get_service_discovery()
        if sd:
            sd.stop()
    except Exception as e:
        print(f"[Server] Error stopping service discovery: {e}")

    sys.exit(0)


def main():
    # Register shutdown handlers
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    storage = stremio.get_storage()
    storage_type = storage.get_status().get('type', 'unknown')
    video_count = storage.get_video_count()

    # Initialize fileserver with storage reference
    fileserver.init(storage)

    print(f"Meta-Stremio starting on port {PORT}")
    print(f"Storage: {storage_type} | Videos: {video_count}")
    print(f"Media: {transcoder.MEDIA_DIR} | Cache: {transcoder.CACHE_DIR}")
    print(f"Segment: {transcoder.SEGMENT_DURATION}s | Prefetch: {transcoder.PREFETCH_SEGMENTS} segments")
    print(f"Manifest URL: http://localhost:{PORT}/manifest.json")
    print(f"Dashboard: http://localhost:{PORT}/")
    print("Adaptive quality: target 60-80% transcode ratio")

    if service_discovery:
        print(f"Service discovery: enabled (registered as meta-stremio)")
    else:
        print(f"Service discovery: disabled (META_CORE_PATH not found)")

    ThreadedServer(('0.0.0.0', PORT), Handler).serve_forever()


if __name__ == '__main__':
    main()
