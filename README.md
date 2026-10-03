# Meta-Stremio

Stremio addon for the MetaMesh stack. Reads file metadata from meta-core's HTTP API (no direct Redis access), reads files via meta-core's WebDAV (or a direct mount), and serves HLS with on-the-fly FFmpeg transcoding plus the standard Stremio addon protocol.

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
│  │     meta-core (owns Redis; /urls + /meta APIs)      │           │
│  │   located over UDP multicast (meta-discovery v1)    │           │
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
- **meta-core over HTTP** — `LeaderStorage` locates meta-core via **meta-discovery v1** (UDP multicast `239.255.77.1:9399`; see [`service-discovery.md`](../../docs/project-architecture/service-discovery.md)) or a pinned `META_CORE_URL`, reads every record through meta-core's `/meta/*` API, and keeps its cache fresh from the SSE meta-stream. The WebDAV URL is taken from meta-core's announce/`/urls` — nothing is configured by hand and no `/meta-core` volume is mounted. (`RedisStorage` remains in `src/storage/` as a legacy, unused backend; meta-core no longer publishes a Redis URL.)
- **Neighbour nav** — this service announces itself on the discovery group and serves its own neighbour map at `/api/neighbors` for the dashboard's shared `<meta-service-menu>`.
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

# Against an existing meta-core (found over UDP on the same Docker network,
# or pinned with META_CORE_URL)
docker run -d \
  -p 8182:7000 \
  -v /path/to/media:/files:ro \
  -e STORAGE_MODE=direct \
  -e META_CORE_URL=http://metacore-app:9000 \
  meta-stremio
```

The container always listens on port `7000` internally — map it to whatever you want externally. The image bundles a `meta-core` binary (copied from `ghcr.io/worph/meta-core`, overridable with `--build-arg META_CORE_IMAGE=…`): with the image default `STORAGE_MODE=leader`, `docker/start.sh` starts it as an in-container sidecar first; any other value (the dev stack and the CasaOS app use `direct`) skips it and uses an external meta-core. Published images: `ghcr.io/worph/meta-stremio` (semver tags from `v*` git tags; in this repo `:latest` follows `main` — see `.github/workflows/docker-publish.yml`); the CasaOS app is `MetaStremio` in [`MetaAppStore`](../MetaAppStore/Apps/MetaStremio/docker-compose.yml).

### Standalone (development)

```bash
# Requires Python 3.9+ and FFmpeg.
pip install -r requirements.txt   # redis, watchdog, Pillow, requests
cd src
python server.py   # finds meta-core over UDP, or set META_CORE_URL=<meta-core API URL>
```

## Configuration

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `7000` | Internal HTTP listen port (the dev stack maps 18182 / 8182 to this). |
| `BASE_URL` | — | External URL emitted in manifests / poster URLs (e.g. `https://metastremio-dev.localhost:8182`). |
| `MEDIA_DIR` | `/files/watch` | Media directory (consumed by `transcoder.py`). |
| `CACHE_DIR` | `/data/cache` | Transcoded segment cache. |
| `PUBLIC_URL` | — | Browser-facing URL announced to neighbours (nav menu); wins over `BASE_URL` for the announce. |
| `STORAGE_MODE` | `leader` (image) | Read only by `docker/start.sh`: `leader` starts the bundled meta-core sidecar in-container, anything else (e.g. `direct`) skips it. The Python server always uses `LeaderStorage`. |
| `META_CORE_URL` | — | Pins meta-core's HTTP API (e.g. `http://metacore-app:9000`); when set, UDP discovery never overrides it. Unset → meta-core is located over UDP. |
| `ENABLE_UDP_DISCOVERY` | `true` | meta-discovery v1 announce/listen. |
| `LEADER_WAIT_TIMEOUT` | `0` | Seconds to wait for meta-core at startup (`0` = forever); on timeout, keeps retrying in the background. |
| `LEADER_RETRY_INTERVAL` | `5` | Seconds between meta-core lookup attempts. |
| `FILES_PATH` | `/files` | Files volume (local fallback when WebDAV is not configured). |
| `SCHEME` | `auto` | URL scheme for generated URLs: `http`, `https`, or `auto`. |
| `SEGMENT_DURATION` | `4` | HLS segment length (s). |
| `PREFETCH_SEGMENTS` | `4` | Segments to prefetch ahead. |
| `HASH_API_SEED` | — | When set, derives a 16-char path-token; all addon routes must be prefixed with `/s/<token>`. Dashboard routes bypass. |

`META_CORE_PATH`, `REDIS_URL` and `REDIS_PREFIX` are still set by the Dockerfile but are vestigial: meta-core is no longer found through the `/meta-core` volume, and reads go over meta-core's HTTP API rather than Redis.

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
| `/api/neighbors` | GET | This peer's UDP neighbour map (meta-discovery v1) for the nav menu |
| `/api/services` | GET | Alias of `/api/neighbors` (kept for older dashboards) |
| `/api/languages` | GET | Languages selectable from the `/configure` page |
| `/meta-service-menu.js` | GET | Shared `<meta-service-menu>` nav element loaded by the dashboard |
| `/transcode/metrics` | GET | Transcoder metrics (also listed below; exempt from the path-token) |

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
│       ├── leader_storage.py          # Storage backend in use: meta-core HTTP API + SSE cache invalidation
│       ├── leader_client.py           # Locates meta-core (UDP or META_CORE_URL pin), exposes its /urls set
│       ├── meshdisco.py               # meta-discovery v1 (Python port of the Go reference in meta-core)
│       ├── meta_consumer.py           # SSE consumer from meta-core /meta stream
│       ├── meta_core_api_client.py    # HTTP client for meta-core REST API
│       └── redis_storage.py           # Legacy direct-Redis backend (unused)
└── www/
    ├── index.html                     # Dashboard
    └── meta-service-menu.js           # Shared nav element
```

The `/configure` page is rendered inline by `server.py` (there is no `www/configure.html`).

## KV store schema

meta-stremio reads the per-file record `/file/{cid}` (via meta-core's API) populated by meta-sort and its plugins. Field semantics (`videoType`, `originalTitle`, `titles`, `season`, `episode`, `movieYear`, `cid_*`, etc.) are documented in the repo-root [`METADATA_KEYS.md`](../../METADATA_KEYS.md) — that is the single source of truth, and matches the `@metazla/meta-interface` types.

## Stream types

For each video, the addon advertises multiple streams:

1. **Direct file** — original bytes, served via `/direct/...` with `Range` support. Best quality, may not play on every device.
2. **HLS original** — transcoded at source resolution, H.264/AAC, one stream per audio track.
3. **HLS ABR** — adaptive ladder (rungs above the source height are skipped). Presets are baked into `transcoder.py`: video is x264 **CRF-based** (the adaptive controller shifts preset and CRF offset at runtime), audio is AAC 128 kbps stereo. `BANDWIDTH` is only the hint written to the master playlist.

| Rung | Base CRF | Master-playlist `BANDWIDTH` hint |
|---|---|---|
| original | 23 | — |
| 1080p | 23 | 4000 kbps |
| 720p  | 24 | 2500 kbps |
| 480p  | 25 | 1200 kbps |
| 360p  | 26 |  800 kbps |

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
- **[meta-core](../meta-core)** — owns Redis; the HTTP API + WebDAV meta-stremio reads from.
- **[meta-fuse](../meta-fuse)** — virtual filesystem; reads the same records.
- **[meta-watch](../meta-watch)** — browser video client for the meta-share network (server-side zero media work, unlike this addon's transcoder).

## License

MIT
