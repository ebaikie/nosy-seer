#!/usr/bin/env python3
"""
Purge nosy-seer cameras that have never captured a valid image.

Dry-run by default. Pass --delete to actually remove via API.

Categories reported:
  NEVER  — last_capture_ok IS NULL or 0, zero timestamped stills on disk
           → safe to delete (truly never worked)
  DEAD   — last_capture_ok=0 (NZTA placeholder), but has old stills on disk
           → once worked, now serving placeholder; skipped unless --include-dead
"""

import argparse
import os
import sqlite3
import sys
from pathlib import Path

import httpx

API_BASE = os.getenv("API_BASE", "http://localhost:5075")
API_USER = os.getenv("API_USER", "admin")
API_PASS = os.getenv("API_PASS", "Mousepad1")
DB_PATH  = Path(os.getenv("DB_PATH", "/mnt/data/nosy-seer/nosy_seer.db"))
STILLS   = Path(os.getenv("STILLS_DIR", "/mnt/data/nosy-seer/stills"))


def stills_count(cam_id: int) -> int:
    d = STILLS / str(cam_id)
    if not d.exists():
        return 0
    return sum(1 for f in d.glob("[0-9]*.jpg"))


def fetch_candidates() -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT id, name, category, snapshot_url, last_capture_ok
        FROM cameras
        WHERE active=1
          AND capture_policy='capture'
          AND snapshot_url IS NOT NULL
          AND (last_capture_ok IS NULL OR last_capture_ok=0)
        ORDER BY id
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def classify(cams: list[dict]) -> tuple[list[dict], list[dict]]:
    never, dead = [], []
    for cam in cams:
        n = stills_count(cam["id"])
        cam["stills_count"] = n
        if n == 0:
            never.append(cam)
        else:
            dead.append(cam)
    return never, dead


def login(client: httpx.Client) -> None:
    r = client.post(f"{API_BASE}/api/login",
                    json={"username": API_USER, "password": API_PASS})
    r.raise_for_status()


def delete_camera(client: httpx.Client, cam: dict) -> bool:
    r = client.delete(f"{API_BASE}/api/cameras/{cam['id']}")
    return r.status_code == 204


def print_table(cams: list[dict], label: str) -> None:
    if not cams:
        print(f"  (none)")
        return
    for c in cams:
        stills_note = f"{c['stills_count']} stills" if c["stills_count"] else "no stills"
        ok = "null" if c["last_capture_ok"] is None else c["last_capture_ok"]
        print(f"  [{c['id']:4d}] {c['name'][:55]:<55}  ok={ok}  {stills_note}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--delete",       action="store_true", help="Delete NEVER cameras via API")
    ap.add_argument("--include-dead", action="store_true", help="Also delete DEAD cameras (had stills)")
    args = ap.parse_args()

    print("Querying candidates…")
    cams = fetch_candidates()
    never, dead = classify(cams)

    print(f"\n── NEVER captured ({len(never)}) — no stills on disk ──")
    print_table(never, "NEVER")

    print(f"\n── DEAD / placeholder ({len(dead)}) — has old stills, now returning placeholder ──")
    print_table(dead, "DEAD")

    to_delete = never[:]
    if args.include_dead:
        to_delete += dead

    if not to_delete:
        print("\nNothing to delete.")
        return

    if not args.delete:
        print(f"\nDry run — would delete {len(to_delete)} camera(s). Pass --delete to confirm.")
        if args.include_dead or dead:
            print("Pass --include-dead to also delete DEAD cameras.")
        return

    print(f"\nDeleting {len(to_delete)} camera(s)…")
    with httpx.Client(follow_redirects=True) as client:
        login(client)
        ok_count = 0
        for cam in to_delete:
            if delete_camera(client, cam):
                print(f"  deleted [{cam['id']}] {cam['name']}")
                ok_count += 1
            else:
                print(f"  FAILED  [{cam['id']}] {cam['name']}", file=sys.stderr)

    print(f"\nDone — {ok_count}/{len(to_delete)} deleted.")


if __name__ == "__main__":
    main()
