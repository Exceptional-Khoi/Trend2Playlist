from __future__ import annotations
import argparse, csv, json, re, requests
from pathlib import Path
from common import base_record, write_jsonl

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"
_NEXT = re.compile(r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>', re.S)

def fetch_live_spotify_embed(playlist_id="37i9dQZEVXbLdGSmz6xilI", region="VN"):
    """Fetch live Top 50 Vietnam directly from public Spotify Embed (no token required)."""
    url = f"https://open.spotify.com/embed/playlist/{playlist_id}"
    print(f"Fetching live Spotify charts from public embed: {url}")
    r = requests.get(url, headers={"User-Agent": UA}, timeout=20)
    r.raise_for_status()
    m = _NEXT.search(r.text)
    if not m:
        raise RuntimeError(f"Could not parse __NEXT_DATA__ from Spotify embed")
    entity = json.loads(m.group(1))["props"]["pageProps"]["state"]["data"]["entity"]
    tracks = entity.get("trackList", [])
    rows = []
    for rank, t in enumerate(tracks, start=1):
        tid = (t.get("uri") or "").split(":")[-1] or None
        subtitle = (t.get("subtitle") or "").replace("\xa0", " ")
        artists = [a.strip() for a in re.split(r",\s", subtitle) if a.strip()]
        artist_str = ", ".join(artists) if artists else subtitle
        duration_s = (t.get("duration") or 0) / 1000 or None
        rows.append(base_record(
            "spotify_charts", "chart_rank", region=region,
            track_title=t.get("title"), artist=artist_str,
            track_key=f"spotify_chart:spotify:track:{tid}" if tid else f"spotify_chart:{t.get('title')}",
            metrics={"rank": rank, "chart": "top50_vn_daily", "duration_s": duration_s},
            raw=t
        ))
    return rows

def fetch_from_csv(csv_path: Path, region="VN", date=None):
    def pick(row, *names):
        cleaned = {k.lower().strip().replace("_", " "): v for k, v in row.items() if k}
        for n in names:
            target = n.lower().strip().replace("_", " ")
            if target in cleaned:
                return cleaned[target]
        return None

    print(f"Reading Spotify chart from fallback CSV: {csv_path}")
    rows = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
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
            "spotify_charts", "chart_rank", event_time=date, region=region,
            track_title=title, artist=artist, track_key=f"spotify_chart:{uri or title}",
            metrics=metrics, raw=row
        ))
    return rows

def main():
    ap = argparse.ArgumentParser(description="Fetch live Spotify Top 50 VN without credentials, with CSV fallback.")
    ap.add_argument("csv", nargs="?", default=None, help="Optional path to spotify CSV fallback file")
    ap.add_argument("--region", default="VN")
    ap.add_argument("--date", default=None)
    ap.add_argument("--offline", action="store_true", help="Force using offline CSV only")
    args = ap.parse_args()
    
    csv_path = Path(args.csv) if args.csv else Path(__file__).resolve().parents[1] / "input" / "spotify_vn.csv"
    rows = []

    if not args.offline:
        try:
            rows = fetch_live_spotify_embed("37i9dQZEVXbLdGSmz6xilI", args.region)
            print(f"Successfully fetched {len(rows)} live tracks from Spotify!")
        except Exception as e:
            print(f"Live Spotify embed fetch failed ({e}). Falling back to CSV...")

    if not rows and csv_path.exists():
        rows = fetch_from_csv(csv_path, args.region, args.date)

    write_jsonl(f"spotify_chart_{args.region}.jsonl", rows)

if __name__ == "__main__":
    main()
