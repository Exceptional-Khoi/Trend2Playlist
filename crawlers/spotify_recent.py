from __future__ import annotations
import argparse, os, requests
from dotenv import load_dotenv
from common import base_record, write_jsonl

API = "https://api.spotify.com/v1/me/player/recently-played"

def main():
    load_dotenv()
    ap = argparse.ArgumentParser(description="Fetch the authenticated user's recently played tracks.")
    ap.add_argument("--limit", type=int, default=50)
    args = ap.parse_args()
    token = os.environ.get("SPOTIFY_ACCESS_TOKEN")
    if not token:
        raise SystemExit("Missing SPOTIFY_ACCESS_TOKEN (scope: user-read-recently-played)")
    r = requests.get(API, params={"limit": min(args.limit, 50)},
                     headers={"Authorization": f"Bearer {token}"}, timeout=30)
    r.raise_for_status()
    data = r.json(); rows = []
    for item in data.get("items", []):
        t = item.get("track", {})
        artists = ", ".join(a.get("name", "") for a in t.get("artists", []))
        isrc = t.get("external_ids", {}).get("isrc")
        rows.append(base_record(
            "spotify", "recent_play", event_time=item.get("played_at"),
            track_title=t.get("name"), artist=artists,
            track_key=f"isrc:{isrc}" if isrc else f"spotify:{t.get('id')}",
            metrics={"duration_ms": t.get("duration_ms")}, raw=item
        ))
    write_jsonl("spotify_recent.jsonl", rows)

if __name__ == "__main__":
    main()
