# Meta-Stremio

A standalone Stremio addon service for streaming personal media with on-the-fly HLS transcoding. Part of the MetaMesh decentralized media ecosystem.

## Overview

Meta-Stremio is a **process-read** service that:
- Reads file metadata from the shared KV store (Redis)
- Reads media files from the shared DATA volume
- Provides HLS streaming with adaptive real-time FFmpeg transcoding
- Serves as a Stremio addon for seamless playback

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
│  │   Leader election via flock on shared filesystem    │           │
│  │   DB stored in: DATA/Apps/meta-core/DB              │           │
│  └─────────────────────────────────────────────────────┘           │
│                             │                                       │
│                             ▼                                       │
│  ┌─────────────────────────────────────────────────────┐           │
│  │           Shared DATA Volume (media files)          │           │
│  │   meta-sort writes → meta-stremio/meta-fuse reads   │           │
│  └─────────────────────────────────────────────────────┘           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Features

### High Performance
- **Adaptive Quality** - Dynamic preset + CRF adjustment targeting 60-80% transcode ratio
- **Intelligent Prefetch** - 4 segments ahead for smooth playback
- **Single-threaded Encode** - 100% CPU per segment for fastest encoding
- **Muxed Segments** - Video+audio in one FFmpeg call
- **Segment Caching** - Persistent cache for repeated access

### Stremio Integration
- **Catalog Endpoint** - Browse media library by type (movies, series)
- **Meta Endpoint** - Detailed video information with codec details
- **Stream Endpoint** - Multiple stream options per video
- **Multi-Audio** - Separate HLS streams per audio track
- **Subtitles** - Extract and serve embedded subtitles as VTT
- **Language Configuration** - Configure display language (uses metadata stored by meta-sort's TMDB plugin)

### Storage Abstraction
- **Leader Storage** - Auto-discovers meta-sort's Redis via leader election (recommended)
- **Redis Storage** - Direct Redis connection via URL

## Project Structure

```
meta-stremio/
├── README.md
├── CLAUDE.md                   # Development guide
├── Dockerfile
├── docker-compose.yml
├── requirements.txt            # Just redis
├── src/
│   ├── server.py               # HTTP server + routing
│   ├── stremio.py              # Stremio handlers + storage integration
│   ├── transcoder.py           # HLS transcoding engine
│   └── storage/
│       ├── __init__.py
│       ├── provider.py         # Abstract StorageProvider
│       ├── redis_storage.py    # Direct Redis connection
│       ├── leader_storage.py   # Leader-aware Redis connection
│       └── leader_discovery.py # Leader election discovery
└── www/
    └── index.html              # Dashboard
```

## Installation

### Docker (Recommended)

```bash
# Build
docker build -t meta-stremio .

# Run with leader discovery (recommended - auto-discovers meta-sort's Redis)
docker run -d \
  -p 7000:7000 \
  -v /path/to/media:/files:ro \
  -v /path/to/meta-core:/meta-core \
  meta-stremio

# Run with direct Redis connection
docker run -d \
  -p 7000:7000 \
  -v /path/to/media:/files:ro \
  -e STORAGE_MODE=redis \
  -e REDIS_URL=redis://your-redis:6379 \
  meta-stremio
```

### Docker Compose

```bash
# Run as part of the MetaMesh stack (uses leader discovery)
cd dev
./start.sh
```

### Standalone (Development)

```bash
# Prerequisites: Python 3.9+, FFmpeg

# Install dependencies
pip install -r requirements.txt

# Run
cd src
python server.py
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `7000` | Server listen port |
| `MEDIA_DIR` | `/data/media` | Media files directory |
| `CACHE_DIR` | `/data/cache` | Transcoded segment cache |
| `STORAGE_MODE` | `leader` | `leader` or `redis` |
| `META_CORE_PATH` | `/meta-core` | Path to meta-core shared volume (for leader discovery) |
| `FILES_PATH` | `/files` | Path to files volume |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL (for `redis` mode) |
| `REDIS_PREFIX` | `meta-sort:` | Redis key prefix |
| `SEGMENT_DURATION` | `4` | HLS segment length (seconds) |
| `PREFETCH_SEGMENTS` | `4` | Segments to prefetch ahead |
| `SCHEME` | `auto` | URL scheme (`http`, `https`, `auto`) |

## API Endpoints

### Dashboard
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Setup dashboard |
| `/health` | GET | Health check with storage status |

### Dashboard API
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/stats` | GET | Library statistics |
| `/api/storage-status` | GET | KV connection status |
| `/api/library` | GET | Full library list |
| `/api/library/refresh` | POST | Refresh library from KV |

### Stremio Protocol
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/manifest.json` | GET | Addon manifest |
| `/catalog/:type/:id.json` | GET | Video catalog |
| `/meta/:type/:id.json` | GET | Video metadata |
| `/stream/:type/:id.json` | GET | Stream URLs |

### Transcoding
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/transcode/:path/master.m3u8` | GET | ABR master playlist |
| `/transcode/:path/master_original_a0.m3u8` | GET | Original quality playlist |
| `/transcode/:path/stream_a0_720p.m3u8` | GET | Quality stream playlist |
| `/transcode/:path/seg_a0_720p_0.ts` | GET | Video segment |
| `/transcode/:path/subtitle_0.vtt` | GET | Subtitle track |
| `/direct/:path` | GET | Direct file serving |
| `/transcode/metrics` | GET | Transcoding metrics |

## KV Store Schema

Meta-stremio reads from meta-sort's Redis using this key format:

```
meta-sort:file:{hashId}              → Hash containing all metadata
  - filePath                         → /media/movies/Movie.mkv
  - title                            → Movie Title
  - type                             → movie|series|anime
  - year                             → 2024
  - season                           → 1 (for series)
  - episode                          → 1 (for series)
  - duration                         → 7200 (seconds)
  - width                            → 1920
  - height                           → 1080
  - videoCodec                       → h264
  - audioCodec                       → aac
  - audioTracks                      → JSON string
  - subtitles                        → JSON string
  - imdbId                           → tt1234567
  - poster                           → URL
```

## Stream Types

Each video provides multiple stream options:

1. **Direct File** - Original file, no transcoding
   - Best quality, may not play on all devices

2. **HLS Original** - Transcoded at source resolution
   - H.264/AAC in HLS container
   - One stream per audio track

3. **HLS ABR** - Adaptive bitrate with quality ladder
   - Multiple resolutions (1080p, 720p, 480p, 360p)
   - Automatic quality switching

## Transcoding Details

### Adaptive Quality System

The transcoder automatically adjusts encoding settings to maintain a 60-80% transcode ratio (segment encode time / segment duration):

- **Too Fast (<60%)**: Increase quality (lower CRF, slower preset)
- **Too Slow (>80%)**: Decrease quality (higher CRF, faster preset)

### Quality Ladder

| Resolution | Video Bitrate | Audio Bitrate |
|------------|---------------|---------------|
| 1080p | 5000 kbps | 192 kbps |
| 720p | 3000 kbps | 128 kbps |
| 480p | 1500 kbps | 128 kbps |
| 360p | 800 kbps | 96 kbps |

## Adding to Stremio

1. Open the dashboard at `http://localhost:7000`
2. Click "Install in Stremio" or copy the manifest URL
3. In Stremio: Settings → Addons → Enter URL → Install

## Development

```bash
# Run in development mode (requires meta-sort running with Redis)
cd src
python server.py

# Or with direct Redis URL
STORAGE_MODE=redis REDIS_URL=redis://localhost:6379 python server.py

# View logs
tail -f /data/cache/*.log
```

## Related Projects

- **[meta-sort](../meta-sort)** - File indexer (writes metadata to Redis)
- **[meta-fuse](../meta-fuse)** - Virtual filesystem (reads metadata)
- **[meta-orbit](../meta-orbit)** - P2P metadata sharing

## License

MIT
