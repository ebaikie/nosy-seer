import asyncio
import datetime as dt
import json
import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

DB_PATH = Path(os.getenv("DB_PATH", "nosy_seer.db"))
STILLS_DIR = Path(os.getenv("STILLS_DIR", "stills"))
CAPTURE_INTERVAL_S = int(os.getenv("CAPTURE_INTERVAL_S", "60"))

_subs: list[asyncio.Queue] = []


# ── DB ────────────────────────────────────────────────────────────────────────

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def _init():
    STILLS_DIR.mkdir(parents=True, exist_ok=True)
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS cameras (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                name           TEXT    NOT NULL,
                description    TEXT,
                category       TEXT    DEFAULT 'traffic',
                lat            REAL,
                lon            REAL,
                source_page    TEXT,
                snapshot_url   TEXT,
                stream_url     TEXT,
                capture_policy TEXT    DEFAULT 'capture',
                keep_last_n    INTEGER DEFAULT 1,
                active         INTEGER DEFAULT 1
            )
        """)
        c.commit()


def _row(r: sqlite3.Row) -> dict:
    d = dict(r)
    d["active"] = bool(d["active"])
    return d


# ── Capture loop ──────────────────────────────────────────────────────────────

async def _fetch(client: httpx.AsyncClient, url: str) -> bytes | None:
    try:
        r = await client.get(
            url, timeout=20,
            headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
        )
        r.raise_for_status()
        return r.content
    except Exception:
        return None


def _prune(cam_dir: Path, keep: int):
    if keep <= 0:
        return
    files = sorted(cam_dir.glob("[0-9]*.jpg"))
    for f in files[:-keep]:
        f.unlink(missing_ok=True)


async def capture_loop():
    await asyncio.sleep(3)
    while True:
        t0 = dt.datetime.utcnow()
        with _conn() as c:
            cams = [_row(r) for r in c.execute(
                "SELECT * FROM cameras "
                "WHERE active=1 AND capture_policy='capture' AND snapshot_url IS NOT NULL"
            )]
        if cams:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                for i, cam in enumerate(cams):
                    if i > 0:
                        await asyncio.sleep(0.5)
                    blob = await _fetch(client, cam["snapshot_url"])
                    if not blob:
                        continue
                    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
                    cam_dir = STILLS_DIR / str(cam["id"])
                    cam_dir.mkdir(parents=True, exist_ok=True)
                    (cam_dir / f"{ts}.jpg").write_bytes(blob)
                    (cam_dir / "latest.jpg").write_bytes(blob)
                    _prune(cam_dir, cam["keep_last_n"])
                    msg = json.dumps({"camera_id": cam["id"], "ts": ts})
                    dead = []
                    for q in list(_subs):
                        try:
                            q.put_nowait(msg)
                        except asyncio.QueueFull:
                            dead.append(q)
                    for q in dead:
                        if q in _subs:
                            _subs.remove(q)
        elapsed = (dt.datetime.utcnow() - t0).total_seconds()
        await asyncio.sleep(max(1, CAPTURE_INTERVAL_S - elapsed))


# ── App ───────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    _init()
    asyncio.create_task(capture_loop())
    yield


STILLS_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="nosy-seer", lifespan=lifespan)
app.mount("/stills", StaticFiles(directory=str(STILLS_DIR)), name="stills")


# ── Schemas ───────────────────────────────────────────────────────────────────

class CameraIn(BaseModel):
    name: str
    description: Optional[str] = None
    category: str = "traffic"
    lat: Optional[float] = None
    lon: Optional[float] = None
    source_page: Optional[str] = None
    snapshot_url: Optional[str] = None
    stream_url: Optional[str] = None
    capture_policy: str = "capture"
    keep_last_n: int = 1
    active: bool = True


class CameraUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    source_page: Optional[str] = None
    snapshot_url: Optional[str] = None
    stream_url: Optional[str] = None
    capture_policy: Optional[str] = None
    keep_last_n: Optional[int] = None
    active: Optional[bool] = None


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/api/cameras")
def list_cameras():
    with _conn() as c:
        return [_row(r) for r in c.execute(
            "SELECT * FROM cameras WHERE active=1 ORDER BY name"
        )]


@app.get("/api/cameras/all")
def list_all_cameras():
    with _conn() as c:
        return [_row(r) for r in c.execute("SELECT * FROM cameras ORDER BY name")]


@app.post("/api/cameras", status_code=201)
def add_camera(body: CameraIn):
    with _conn() as c:
        cur = c.execute(
            """INSERT INTO cameras
               (name,description,category,lat,lon,source_page,
                snapshot_url,stream_url,capture_policy,keep_last_n,active)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (body.name, body.description, body.category, body.lat, body.lon,
             body.source_page, body.snapshot_url, body.stream_url,
             body.capture_policy, body.keep_last_n, int(body.active)),
        )
        c.commit()
        return _row(c.execute("SELECT * FROM cameras WHERE id=?", (cur.lastrowid,)).fetchone())


_ALLOWED_COLS = frozenset(CameraUpdate.model_fields.keys())


@app.patch("/api/cameras/{cam_id}")
def update_camera(cam_id: int, body: CameraUpdate):
    data = {k: v for k, v in body.model_dump(exclude_unset=True).items() if k in _ALLOWED_COLS}
    if not data:
        raise HTTPException(400, "nothing to update")
    if "active" in data:
        data["active"] = int(data["active"])
    sets = ", ".join(f"{k}=?" for k in data)
    with _conn() as c:
        c.execute(f"UPDATE cameras SET {sets} WHERE id=?", (*data.values(), cam_id))
        c.commit()
        row = c.execute("SELECT * FROM cameras WHERE id=?", (cam_id,)).fetchone()
        if not row:
            raise HTTPException(404)
        return _row(row)


@app.delete("/api/cameras/{cam_id}", status_code=204)
def delete_camera(cam_id: int):
    with _conn() as c:
        c.execute("DELETE FROM cameras WHERE id=?", (cam_id,))
        c.commit()


@app.get("/api/events/stills")
async def sse_stills(request: Request):
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    _subs.append(q)

    async def gen():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"event: still\ndata: {msg}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            if q in _subs:
                _subs.remove(q)

    return StreamingResponse(gen(), media_type="text/event-stream")


# catch-all static (must be last)
app.mount("/", StaticFiles(directory="static", html=True), name="static")
