from __future__ import annotations
import json, os, hashlib
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output"
OUT.mkdir(exist_ok=True)

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def stable_hash(value: str | None) -> str | None:
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:20]

def write_jsonl(name: str, rows):
    path = OUT / name
    n = 0
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            n += 1
    print(f"wrote {n} rows -> {path}")
    return path

def base_record(source, event_type, *, event_time=None, region=None, track_title=None,
                artist=None, track_key=None, user_id=None, metrics=None, tags=None, raw=None):
    return {
        "source": source,
        "event_type": event_type,
        "event_time": event_time or utc_now(),
        "region": region,
        "track_key": track_key,
        "track_title": track_title,
        "artist": artist,
        "user_id_hash": stable_hash(user_id),
        "metrics": metrics or {},
        "tags": tags or [],
        "raw": raw or {},
        "ingested_at": utc_now(),
    }
