# nosy-seer

Lite public-camera still-capture viewer for Hawke's Bay & Tairāwhiti.
Companion to [nosy-neighbor](https://github.com/ebaikie/nosy-neighbor).

Repo: https://github.com/ebaikie/nosy-seer  
Runs on nosy-box at **http://192.168.4.127:5075**

## Stack

Single Python process — no Redis, no Postgres, no MinIO, no build step.

| Component | Technology |
|-----------|-----------|
| API + worker | FastAPI + asyncio background task |
| Database | SQLite (`./data/nosy_seer.db`) |
| Still storage | Local filesystem (`./data/stills/`) |
| Frontend | Plain HTML + vanilla JS + MapLibre GL (CDN) |

## Quick start

```bash
cd /home/user/Apps/nosy-seer
cp seed.json.example data/seed.json   # edit with your cameras
docker compose up -d --build
```

UI: http://192.168.4.127:5075  
Default login: `admin` / `nosy123` (change in Admin → Change Password)

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `DB_PATH` | `/app/data/nosy_seer.db` | SQLite database |
| `STILLS_DIR` | `/app/data/stills` | Captured JPEG directory |
| `SEED_FILE` | `/app/data/seed.json` | Camera seed list |
| `CAPTURE_INTERVAL_S` | `60` | Seconds between capture runs |
| `ADMIN_USERNAME` | `admin` | Login username (fallback if not in DB) |
| `ADMIN_PASSWORD` | `nosy123` | Login password (fallback if not in DB) |
| `SESSION_SECRET` | random | Set a fixed value for sessions to survive restarts |

## Adding cameras

**Via seed file** — edit `data/seed.json`, restart the container. New cameras are inserted; existing ones are untouched.

```json
[
  {
    "name": "SH2 Watchman Rd Roundabout",
    "description": "North along SH2 from Watchman Rd roundabout",
    "category": "traffic",
    "lat": -39.492,
    "lon": 176.871,
    "source_page": "https://www.journeys.nzta.govt.nz/traffic-cameras/hawkes-bay/1470",
    "snapshot_url": "https://www.trafficnz.info/camera/740.jpg",
    "capture_policy": "capture",
    "keep_last_n": 1
  }
]
```

**Via web UI** — Admin tab → Add Camera form. Click the map to set lat/lon.

### capture_policy

| Value | Behaviour |
|-------|-----------|
| `capture` | Worker fetches JPEG from `snapshot_url` every `CAPTURE_INTERVAL_S` seconds |
| `embed_only` | No capture — stream/source URL shown as a link |

### keep_last_n

Per-camera retention. `0` = keep all stills. `1` = latest only (default). Files are named by UTC timestamp and pruned after each capture run.

### Camera URL pattern (NZTA)

- Source page: `https://www.journeys.nzta.govt.nz/traffic-cameras/hawkes-bay/{id}`
- Image URL: `https://www.trafficnz.info/camera/{id}.jpg`
- The `?t=...` cache-buster in the browser URL is not needed — the worker adds its own `Cache-Control: no-cache` headers.

## How it works

1. On startup: DB tables created → `seed.json` loaded (additive, by name) → capture loop starts
2. Every `CAPTURE_INTERVAL_S`: fetch all active `capture` cameras, save `{ts}.jpg` + `latest.jpg`, prune old files, push SSE event to connected browsers
3. Browser receives SSE event → refreshes that camera's grid card in real time

## Key commands

```bash
docker compose up -d --build   # rebuild + start
docker compose logs -f         # follow logs
docker compose down            # stop
docker compose restart         # restart without rebuild
```

## Data directory layout

```
data/
  nosy_seer.db        # SQLite — cameras + settings tables
  seed.json           # camera seed list (your source of truth)
  stills/
    {camera_id}/
      latest.jpg      # always current
      20260613T120000.jpg   # timestamped archive (if keep_last_n > 1)
```

## Pending todos

- Click map pin → scroll detail panel into view
- Cluster pins when zoomed out (GeoJSON source + MapLibre cluster layer — requires refactor from individual Marker instances)

## Feature ideas

- Category filter bar (traffic / river / surf toggles)
- Camera health dot (marker colour by staleness: green / amber / red)
- Auto-refresh detail image on a timer
- Archive scrubber for cameras with keep_last_n > 1
- Per-camera capture interval column
- Map style switcher (street / satellite / topo)
- Export current camera list as seed.json
- Embed-only iframe viewer for stream_url cameras
- Stills diff / motion score in worker (flag activity on markers)
