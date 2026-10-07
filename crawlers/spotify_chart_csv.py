from __future__ import annotations
import argparse, csv
from pathlib import Path
from common import base_record, write_jsonl

def pick(row, *names):
    cleaned = {k.lower().strip().replace("_", " "): v for k, v in row.items() if k}
    for n in names:
        target = n.lower().strip().replace("_", " ")
        if target in cleaned:
            return cleaned[target]
    return None

def main():
    ap = argparse.ArgumentParser(description="Normalize a CSV exported manually from charts.spotify.com.")
    ap.add_argument("csv", nargs="?", default=None, help="Path to spotify CSV file (default: input/spotify_vn.csv)")
    ap.add_argument("--region", default="VN")
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    
    csv_path = Path(args.csv) if args.csv else Path(__file__).resolve().parents[1] / "input" / "spotify_vn.csv"
    if not csv_path.exists():
        raise SystemExit(f"Spotify chart CSV file not found at: {csv_path}")
    print(f"Reading Spotify chart CSV from {csv_path}")
    path = csv_path
    rows = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        # Spotify exports have changed format over time; skip non-header preamble if needed.
        lines = f.readlines()
    header_idx = 0
    for i, line in enumerate(lines[:8]):
        l = line.lower()
        if "rank" in l and ("track" in l or "uri" in l):
            header_idx = i; break
    reader = csv.DictReader(lines[header_idx:])
    for row in reader:
        rank = pick(row, "rank", "position")
        title = pick(row, "track name", "track", "name")
        artist = pick(row, "artist", "artist names", "artists")
        uri = pick(row, "uri", "url")
        streams = pick(row, "streams")
        metrics = {"rank": int(rank) if rank and rank.isdigit() else rank}
        if streams:
            try: metrics["streams"] = int(streams.replace(",", ""))
            except ValueError: metrics["streams"] = streams
        rows.append(base_record(
            "spotify_charts", "chart_rank", event_time=args.date, region=args.region,
            track_title=title, artist=artist, track_key=f"spotify_chart:{uri or title}",
            metrics=metrics, raw=row
        ))
    write_jsonl(f"spotify_chart_{args.region}.jsonl", rows)

if __name__ == "__main__":
    main()
