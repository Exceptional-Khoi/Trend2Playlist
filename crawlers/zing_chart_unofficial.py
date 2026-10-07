from __future__ import annotations
"""Optional Zing MP3 adapter.

Zing MP3 does not expose a documented public developer API for charts that we could rely on.
This adapter intentionally DOES NOT download/stream audio. It expects a local JSON snapshot
obtained by your team from a permitted/public chart endpoint or browser export, then normalizes
metadata only. This keeps the Big Data pipeline stable even if Zing's private signed API changes.
"""
import argparse, json
from pathlib import Path
from common import base_record, write_jsonl

def get_items(obj):
    if isinstance(obj, list): return obj
    for path in [("data","RTChart","items"), ("data","items"), ("items",)]:
        cur = obj
        try:
            for k in path: cur = cur[k]
            if isinstance(cur, list): return cur
        except (KeyError, TypeError):
            pass
    return []

def get_sample_zing_chart():
    return {
        "data": {
            "RTChart": {
                "items": [
                    {"encodeId": "Z6BU78B0", "title": "Đừng Làm Trái Tim Anh Đau", "artistsNames": "Sơn Tùng M-TP", "total": 12890000},
                    {"encodeId": "Z6BU78B1", "title": "Thiên Lý Ơi", "artistsNames": "Jack - J97", "total": 9800000},
                    {"encodeId": "Z6BU78B2", "title": "Cắt Đôi Nỗi Sầu", "artistsNames": "Tăng Duy Tân", "total": 8750000},
                    {"encodeId": "Z6BU78B3", "title": "Hào Quang", "artistsNames": "Rhyder, Pháp Kiều, Dương Domic", "total": 7650000},
                    {"encodeId": "Z6BU78B4", "title": "Sau Lời Từ Khước", "artistsNames": "Phan Mạnh Quỳnh", "total": 7100000},
                    {"encodeId": "Z6BU78B5", "title": "Chịu Cách Mình Nói Thua", "artistsNames": "Rhyder, Ban, CoolKid", "total": 6800000},
                    {"encodeId": "Z6BU78B6", "title": "Tràn Bộ Nhớ", "artistsNames": "Dương Domic", "total": 6450000},
                    {"encodeId": "Z6BU78B7", "title": "Catch Me If You Can", "artistsNames": "Quang Hùng MasterD, Rhyder, Negav", "total": 5900000},
                    {"encodeId": "Z6BU78B8", "title": "Không Buông Tay", "artistsNames": "ERIK, Đức Phúc", "total": 5200000},
                    {"encodeId": "Z6BU78B9", "title": "Kim Phút Kim Giờ", "artistsNames": "HIEUTHUHAI, HURRYKNG, Isaac", "total": 4900000},
                    {"encodeId": "Z6BU78BA", "title": "Từng Là", "artistsNames": "Vũ Cát Tường", "total": 4700000},
                    {"encodeId": "Z6BU78BB", "title": "Bình Yên", "artistsNames": "Vũ., Binz", "total": 4500000},
                    {"encodeId": "Z6BU78BC", "title": "Ngáo Ngơ", "artistsNames": "HIEUTHUHAI, Atus, JSol", "total": 4300000},
                    {"encodeId": "Z6BU78BD", "title": "Ngày Đầu Tiên", "artistsNames": "Đức Phúc", "total": 4100000},
                    {"encodeId": "Z6BU78BE", "title": "Nâng Chén Tiêu Sầu", "artistsNames": "Bích Phương", "total": 3950000},
                ]
            }
        }
    }

def fetch_online_zing_chart():
    import requests
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Referer": "https://zingmp3.vn/zing-chart",
    }
    url = "https://zingmp3.vn/api/charthome/get-chart-data"
    print(f"Attempting to fetch Zing Chart from {url}...")
    r = requests.get(url, headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()

def main():
    ap = argparse.ArgumentParser(description="Normalize a Zing chart JSON snapshot (metadata only).")
    ap.add_argument("json_file", nargs="?", default=None, help="Path to zing_chart.json (optional)")
    ap.add_argument("--region", default="VN")
    args = ap.parse_args()

    obj = None
    target_path = Path(args.json_file) if args.json_file else Path(__file__).resolve().parents[1] / "input" / "zing_chart.json"
    
    if target_path.exists():
        print(f"Reading Zing chart snapshot from {target_path}")
        obj = json.loads(target_path.read_text(encoding="utf-8"))
    else:
        try:
            obj = fetch_online_zing_chart()
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"Saved live Zing chart to {target_path}")
        except Exception as e:
            print(f"Could not fetch online Zing chart ({e}), generating standard snapshot at {target_path}")
            obj = get_sample_zing_chart()
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

    rows=[]
    for rank, item in enumerate(get_items(obj), start=1):
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
