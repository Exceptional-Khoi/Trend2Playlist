from __future__ import annotations
import argparse, hashlib, hmac, json, os, time, requests
from pathlib import Path
from common import base_record, write_jsonl

BASE = "https://zingmp3.vn"
API_KEY = os.getenv("ZING_API_KEY", "88265e23d4284f25963e6eedac8fbfa3")
SECRET_KEY = os.getenv("ZING_SECRET_KEY", "2aa2d1c561e809b267f3638c4a307aab")
VERSION = os.getenv("ZING_VERSION", "1.6.34")

def fetch_live_zing_charts():
    """Fetch live #zingchart 100 realtime songs using Zing Web v2 signed API."""
    print("Attempting to fetch live #zingchart realtime from Zing MP3 API v2...")
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36",
        "Referer": "https://zingmp3.vn/zing-chart",
    })
    # Establish session cookie
    s.get(f"{BASE}/", timeout=15)

    path = "/api/v2/page/get/chart-home"
    ctime = str(int(time.time()))
    raw = f"ctime={ctime}version={VERSION}"
    sha = hashlib.sha256(raw.encode()).hexdigest()
    sig = hmac.new(SECRET_KEY.encode(), (path + sha).encode(), hashlib.sha512).hexdigest()

    params = {
        "ctime": ctime,
        "version": VERSION,
        "sig": sig,
        "apiKey": API_KEY,
    }
    r = s.get(f"{BASE}{path}", params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if data.get("err") != 0:
        raise RuntimeError(f"Zing API err={data.get('err')} msg={data.get('msg')}")
    
    items = data.get("data", {}).get("RTChart", {}).get("items", [])
    return items

def get_fallback_zing_chart(target_path: Path):
    if target_path.exists():
        print(f"Reading Zing chart snapshot from fallback file: {target_path}")
        try:
            obj = json.loads(target_path.read_text(encoding="utf-8"))
            if isinstance(obj, dict):
                return obj.get("data", {}).get("RTChart", {}).get("items", []) or obj.get("items", [])
            elif isinstance(obj, list):
                return obj
        except Exception:
            pass
    return []

def main():
    ap = argparse.ArgumentParser(description="Fetch live Zing MP3 chart with HMAC-SHA512 and offline fallback.")
    ap.add_argument("json_file", nargs="?", default=None, help="Path to offline snapshot zing_chart.json")
    ap.add_argument("--region", default="VN")
    ap.add_argument("--offline", action="store_true", help="Force offline snapshot")
    args = ap.parse_args()

    target_path = Path(args.json_file) if args.json_file else Path(__file__).resolve().parents[1] / "input" / "zing_chart.json"
    items = []

    if not args.offline:
        try:
            items = fetch_live_zing_charts()
            print(f"Successfully fetched {len(items)} live songs from Zing MP3!")
            # Cache live chart to input/zing_chart.json for offline resilience
            if items:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                cache_payload = {"data": {"RTChart": {"items": items}}}
                target_path.write_text(json.dumps(cache_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            print(f"Live Zing chart fetch failed ({e}). Falling back to snapshot...")

    if not items:
        items = get_fallback_zing_chart(target_path)

    rows = []
    for rank, item in enumerate(items, start=1):
        artists = item.get("artistsNames") or item.get("artist") or item.get("artists_names")
        total = item.get("total") or item.get("score")
        metrics = {"rank": rank}
        if total:
            metrics["score"] = total
        rows.append(base_record(
            "zing_mp3", "chart_rank", region=args.region,
            track_title=item.get("title") or item.get("name"), artist=artists,
            track_key=f"zing:{item.get('encodeId') or item.get('id')}",
            metrics=metrics, raw=item
        ))

    write_jsonl("zing_chart_vn.jsonl", rows)

if __name__ == "__main__":
    main()
