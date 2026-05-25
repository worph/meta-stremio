# Meta-Stremio

Stremio addon for the MetaMesh stack. Reads file metadata from the shared KV store, reads files via WebDAV (or direct mount), and serves HLS with on-the-fly FFmpeg transcoding plus the standard Stremio addon protocol.

External port in the dev stack: **8182** (auth-gated, via Caddy + nginx-hash-lock). Internal container port is `7000`. Debug-direct port (bypasses the perimeter): **18182**. Container name: `metastremio-app` (backend) / `metastremio` (hash-lock proxy).

## Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        MetaMesh Ecosystem                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐          │
│  │  meta-sort   │    │ meta-stremio │    │  meta-fuse   │          │
│  │ [write meta] │    │ [read meta]  │    │ [read meta]  │          │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘          │
│         │                   │                   │                   │
│         ▼                   ▼                   ▼                   │
│  ┌─────────────────────────────────────────────────────┐           │
│  │              Shared KV Store (Redis)                │           │
│  │   meta-core is leader, exposes /urls + /meta APIs   │           │
│  └─────────────────────────────────────────────────────┘           │
│                             │                                       │
│                             ▼                                       │
│  ┌─────────────────────────────────────────────────────┐           │
│  │      File access via WebDAV (or direct mount)       │           │
│  └─────────────────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────────────┘
```

## Features

- **HLS transcoding** — adaptive preset+CRF targeting a 60–80% transcode ratio, prefetch of N segments ahead, persistent segment cache.
- **Stremio protocol** — manifest, catalog, meta, stream; one HLS stream per audio track; subtitles extracted to VTT.
- **Direct file serving** — range-request capable, for clients that can play the source directly.
- **Storage abstractions** — `LeaderStorage` (auto-discovers Redis via meta-core), `RedisStorage` (direct URL), `direct` mode (reads from meta-core's HTTP API + SSE meta-stream).
- **Path-token gate for addon URLs** — when `HASH_API_SEED` is set, addon paths must be prefixed with `/s/<token>`. Dashboard/browser paths are protected separately by the upstream hash-lock + OIDC sidecar (Stremio clients can't carry cookies, so they get the path-token instead).
- **Language-configurable manifest** — `displayLanguage` in the per-install config (base64-in-path) selects which `titles` translation meta-sort's TMDB plugin populated.

## Installation

### Docker (in the MetaMesh dev stack)

```bash
cd dev
./scripts/start.sh
```

This brings up `metastremio-app` on port 18182 (debug-direct) and the auth-gated entry on `https://metastremio-dev.localhost:8182`.

### Docker (standalone)

```bash
docker build -t meta-stremio packages/meta-stremio

# Leader mode (auto-discover Redis via meta-core)
docker run -d \
  -p 8182:7000 \
  -v /path/to/media:/files:ro \
  -v /path/to/meta-core:/meta-core \
  meta-stremio

# Direct Redis mode
docker run -d \
  -p 8182:7000 \
  -v /path/to/media:/files:ro \
  -e STORAGE_MODE=redis \
  -e REDIS_URL=redis://your-redis:6379 \
  meta-stremio
```

The container always listens on port `7000` internally — map it to whatever you want externally.

### Standalone (development)

```bash
# Requires Python 3.9+ and FFmpeg.
pip install -r requirements.txt   # redis, watchdog, Pillow, requests
cd src
python server.py
```

## Configuration

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `7000` | Internal HTTP listen port (the dev stack maps 18182 / 8182 to this). |
| `BASE_URL` | — | External URL emitted in manifests / poster URLs (e.g. `https://metastremio-dev.localhost:8182`). |
| `MEDIA_DIR` | `/files/watch` | Media directory (consumed by `transcoder.py`). |
| `CACHE_DIR` | `/data/cache` | Transcoded segment cache. |
| `STORAGE_MODE` | `leader` | `leader`, `redis`, or `direct`. |
| `META_CORE_PATH` | `/meta-core` | Shared meta-core volume; used by `LeaderClient` for `kv-leader.info`. |
| `META_CORE_URL` | — | meta-core HTTP API (e.g. `http://metacore-app:9000`); required for `STORAGE_MODE=direct` since there is no leader file to read. |
| `FILES_PATH` | `/files` | Files volume. |
| `REDIS_URL` | `redis://localhost:6379` | Redis URL for `STORAGE_MODE=redis`. |
| `REDIS_PREFIX` | `meta-sort:` | Redis key prefix. |
| `SCHEME` | `auto` | URL scheme for generated URLs: `http`, `https`, or `auto`. |
| `SEGMENT_DURATION` | `4` | HLS segment length (s). |
| `PREFETCH_SEGMENTS` | `4` | Segments to prefetch ahead. |
| `META_CORE_WEBDAV_URL` | — | When set, files are read over HTTP from meta-core's WebDAV instead of the filesystem (lets meta-stremio reach SMB / rclone mounts that only exist inside meta-core). |
| `HASH_API_SEED` | — | When set, derives a 16-char path-token; all addon routes must be prefixed with `/s/<token>`. Dashboard routes bypass. |

## API endpoints

All routes below are matched in `src/server.py`. When `HASH_API_SEED` is set, **every addon path is prefixed with `/s/<token>`**; the dashboard paths in the first table are *not* prefixed (they're protected by hash-lock/OIDC upstream).

### Dashboard / browser

| Endpoint | Method | Description |
|---|---|---|
| `/` , `/index.html` | GET | Setup dashboard |
| `/configure` | GET | Per-install language configuration page |
| `/health` | GET | Liveness + storage status |
| `/api/stats` | GET | Library statistics |
| `/api/library` | GET | Full library list (videos + count) |
| `/api/services` | GET | Discovered MetaMesh services (proxied from `meta-core /services`) |
| `/api/languages` | GET | Languages selectable from the `/configure` page |

### Stremio addon protocol

`/manifest.json` and `/stremio/manifest.json` are aliases; same for catalog/meta/stream. Stremio per-install configuration is encoded as URL-safe base64 JSON between the `/s/<token>` prefix and the addon path (e.g. `/s/<token>/<configB64>/manifest.json`).

| Endpoint | Method | Description |
|---|---|---|
| `/manifest.json` | GET, HEAD | Addon manifest |
| `/stremio/manifest.json` | GET, HEAD | Alias of `/manifest.json` |
| `/catalog/:type/:id.json` | GET | Catalog (with optional `…/:extra.json`) |
| `/stremio/catalog/:type/:id.json` | GET | Alias |
| `/meta/:type/:id.json` | GET | Video metadata |
| `/stremio/meta/:type/:id.json` | GET | Alias |
| `/stream/:type/:id.json` | GET | Stream URLs |
| `/stremio/stream/:type/:id.json` | GET | Alias |

### File serving

| Endpoint | Method | Description |
|---|---|---|
| `/file/{cid}` | GET, HEAD | Serve file by CID |
| `/file/{cid}/w{width}` | GET, HEAD | Serve resized image by CID (uses Pillow) |
| `/poster/{cid}` | GET, HEAD | Legacy alias of `/file/{cid}` |
| `/poster/{cid}/w{width}` | GET, HEAD | Legacy alias of `/file/{cid}/w{width}` |
| `/direct/{path}` | GET, HEAD | Direct file serving with HTTP `Range` support |

### Transcoder

| Endpoint | Method | Description |
|---|---|---|
| `/transcode/{path}/master.m3u8` | GET, HEAD | ABR master playlist |
| `/transcode/{path}/master_{resolution}.m3u8` | GET, HEAD | Master playlist for a single resolution |
| `/transcode/{path}/master_a{audio}.m3u8` | GET, HEAD | Master playlist for a single audio track |
| `/transcode/{path}/master_{resolution}_a{audio}.m3u8` | GET, HEAD | Combined variant |
| `/transcode/{path}/stream_a{audio}_{resolution}.m3u8` | GET, HEAD | Per-variant stream playlist |
| `/transcode/{path}/seg_a{audio}_{resolution}_{n}.ts` | GET, HEAD | Video segment |
| `/transcode/{path}/subtitle_{idx}.m3u8` | GET, HEAD | Subtitle playlist |
| `/transcode/{path}/subtitle_{idx}.vtt` | GET, HEAD | Subtitle VTT |
| `/transcode/metrics` | GET | Transcoder metrics (includes `total_files`) |
| `/transcode/reset-metrics` | POST | Reset transcoder metrics |

## Project structure

```
meta-stremio/
├── README.md
├── Dockerfile
├── docker-compose.yml
├── docker/                            # container entry scripts
├── requirements.txt                   # redis, watchdog, Pillow, requests
├── src/
│   ├── server.py                      # HTTP server + routing (entry point)
│   ├── stremio.py                     # Stremio handlers + library/language helpers
│   ├── transcoder.py                  # HLS transcoding engine + metrics + segment/subtitle managers
│   ├── fileserver.py                  # serve_file(cid, width) — file/poster endpoints
│   ├── poster.py                      # poster URL/CID helpers
│   ├── webdav_client.py               # WebDAV client (file_exists, stream_file, stream_range, get_file_size)
│   └── storage/
│       ├── __init__.py
│       ├── provider.py                # StorageProvider + VideoMetadata abstract base
│       ├── redis_storage.py           # Direct Redis backend
│       ├── leader_storage.py          # Leader-aware Redis backend
│       ├── leader_client.py           # Reads /meta-core/locks/kv-leader.info + calls meta-core /urls
│       ├── meta_consumer.py           # SSE consumer from meta-core /meta stream
│       ├── meta_core_api_client.py    # HTTP client for meta-core REST API
│       └── service_registration.py    # Registers meta-stremio with meta-core /services
└── www/
    ├── index.html                     # Dashboard
    └── configure.html                 # Language configuration page
```

## KV store schema

meta-stremio reads from the per-file Redis hash `/file/{cid}` populated by meta-sort and its plugins. Field semantics (`videoType`, `originalTitle`, `titles`, `season`, `episode`, `movieYear`, `cid_*`, etc.) are documented in the repo-root [`METADATA_KEYS.md`](../../METADATA_KEYS.md) — that is the single source of truth, and matches the `@metazla/meta-interface` types.

## Stream types

For each video, the addon advertises multiple streams:

1. **Direct file** — original bytes, served via `/direct/...` with `Range` support. Best quality, may not play on every device.
2. **HLS original** — transcoded at source resolution, H.264/AAC, one stream per audio track.
3. **HLS ABR** — adaptive ladder. Quality presets are baked into `transcoder.py`.

| Resolution | Video bitrate | Audio bitrate |
|---|---|---|
| 1080p | 5000 kbps | 192 kbps |
| 720p  | 3000 kbps | 128 kbps |
| 480p  | 1500 kbps | 128 kbps |
| 360p  |  800 kbps |  96 kbps |

## Adding to Stremio

1. Open the dashboard at `https://metastremio-dev.localhost:8182/` (or debug-direct `http://localhost:18182/`).
2. Pick your display language on `/configure` if you want non-English titles.
3. Use the **Install in Stremio** button (uses the `stremio://` protocol handler) or copy the manifest URL into Stremio: Settings → Addons → Add Addon.

## Debugging

```bash
# Container logs
docker compose -f dev/docker-compose.yml logs -f metastremio-app

# Live health
curl -k https://metastremio-dev.localhost:8182/health
curl    http://localhost:18182/health   # debug-direct (no auth, no TLS)

# Transcoder metrics
curl    http://localhost:18182/transcode/metrics
curl -X POST http://localhost:18182/transcode/reset-metrics
```

Container lifecycle: do **not** `docker restart metastremio-app` to apply config — use the dev-stack reload script (`./scripts/reload-stremio.sh`) so supervisord restarts the right process inside the container.

## Related projects

- **[meta-sort](../meta-sort)** — file indexer; writes the metadata meta-stremio reads.
- **[meta-fuse](../meta-fuse)** — virtual filesystem; reads the same KV store.
- **[meta-share](../meta-share)** — decentralised metadata sharing.

## License

MIT
