from __future__ import annotations
import json, re, unicodedata
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output"

PROVINCES_DICT = {
    "Hà Nội": ["ha noi", "hanoi", "hn", "thủ đô"],
    "Hồ Chí Minh": ["ho chi minh", "hcm", "sai gon", "sài gòn", "tphcm"],
    "Đà Nẵng": ["da nang", "danang", "đà thành"],
    "Hải Phòng": ["hai phong", "haiphong", "đất cảng"],
    "Cần Thơ": ["can tho", "cantho", "tây đô"],
    "Bình Dương": ["binh duong", "binhduong"],
    "Đồng Nai": ["dong nai", "bien hoa"],
    "Nghệ An": ["nghe an", "vinh"],
    "Thanh Hóa": ["thanh hoa"],
    "Khánh Hòa": ["khanh hoa", "nha trang"],
    "Lâm Đồng": ["lam dong", "da lat", "đà lạt"],
    "Thừa Thiên Huế": ["hue", "thừa thiên"],
}

def remove_accents(input_str: str) -> str:
    if not input_str:
        return ""
    nfkd = unicodedata.normalize('NFKD', input_str)
    return "".join([c for c in nfkd if not unicodedata.combining(c)]).lower()

def detect_province_in_text(text: str) -> list[str]:
    found = []
    norm_text = remove_accents(text)
    for p_name, aliases in PROVINCES_DICT.items():
        for alias in aliases:
            if re.search(r"\b" + re.escape(alias) + r"\b", norm_text):
                found.append(p_name)
                break
    return found

def calculate_province_interest(a=0.50, b=0.15, c=0.35):
    """
    Computes province interest estimate per track according to:
    province_interest(track, p, t) = a*norm(Trends) + b*norm(comment_signal) + c*norm(events)
    """
    OUT.mkdir(exist_ok=True)
    canonical_file = OUT / "canonical_tracks.jsonl"
    trends_file = OUT / "google_trends_regions.jsonl"
    youtube_file = OUT / "youtube_VN.jsonl"
    events_file = OUT / "user_events.jsonl"

    tracks = {}
    if canonical_file.exists():
        for line in canonical_file.read_text(encoding="utf-8").splitlines():
            if line.strip():
                t = json.loads(line)
                tracks[t["canonical_track_id"]] = t

    # 1. Trends signal: track_title -> province -> interest
    trends_signal = defaultdict(lambda: defaultdict(float))
    if trends_file.exists():
        for line in trends_file.read_text(encoding="utf-8").splitlines():
            if line.strip():
                try:
                    rec = json.loads(line)
                    title = rec.get("track_title")
                    prov = rec.get("region")
                    interest = float(rec.get("metrics", {}).get("interest", 0))
                    # Map province name
                    for std_prov, aliases in PROVINCES_DICT.items():
                        if remove_accents(prov) in [remove_accents(std_prov)] + aliases:
                            trends_signal[title][std_prov] = interest
                            break
                except Exception:
                    pass

    # 2. Comments text province signal: video/track -> province -> count
    comments_signal = defaultdict(lambda: defaultdict(int))
    if youtube_file.exists():
        for line in youtube_file.read_text(encoding="utf-8").splitlines():
            if line.strip():
                try:
                    rec = json.loads(line)
                    if rec.get("event_type") == "comment":
                        title = rec.get("track_title")
                        text = rec.get("raw", {}).get("text", "")
                        matched_provinces = detect_province_in_text(text)
                        for p in matched_provinces:
                            comments_signal[title][p] += 1
                except Exception:
                    pass

    # 3. System events signal: track_id -> province -> count
    events_signal = defaultdict(lambda: defaultdict(int))
    if events_file.exists():
        for line in events_file.read_text(encoding="utf-8").splitlines():
            if line.strip():
                try:
                    rec = json.loads(line)
                    tid = rec.get("track_id")
                    prov = rec.get("province")
                    for std_prov, aliases in PROVINCES_DICT.items():
                        if remove_accents(prov) in [remove_accents(std_prov)] + aliases:
                            events_signal[tid][std_prov] += 1
                            break
                except Exception:
                    pass

    all_provinces = list(PROVINCES_DICT.keys())
    output_rows = []

    # Map tracks
    for cid, tinfo in tracks.items():
        title = tinfo["title"]
        aliases = tinfo.get("aliases", []) + [title]

        # Gather signals
        for p in all_provinces:
            # Trends
            t_score = 0.0
            for a_name in aliases:
                if a_name in trends_signal and p in trends_signal[a_name]:
                    t_score = max(t_score, trends_signal[a_name][p])

            # Comments
            c_count = 0
            for a_name in aliases:
                if a_name in comments_signal and p in comments_signal[a_name]:
                    c_count += comments_signal[a_name][p]

            # Events
            e_count = events_signal[cid][p]

            # Normalization (0-1)
            norm_trends = min(1.0, t_score / 100.0)
            norm_comments = min(1.0, c_count / 5.0)
            norm_events = min(1.0, e_count / 10.0)

            composite = a * norm_trends + b * norm_comments + c * norm_events
            final_interest = round(composite * 100, 2)

            # Confidence based on available components
            active_signals = sum([norm_trends > 0, norm_comments > 0, norm_events > 0])
            confidence = round(active_signals / 3.0, 2)

            output_rows.append({
                "canonical_track_id": cid,
                "track_title": title,
                "province": p,
                "estimated_interest": final_interest,
                "confidence": confidence,
                "components": {
                    "trends_normalized": round(norm_trends, 3),
                    "comments_signal_normalized": round(norm_comments, 3),
                    "events_normalized": round(norm_events, 3),
                },
                "weights": {"a_trends": a, "b_comments": b, "c_events": c}
            })

    # Sort descending by estimated interest
    output_rows.sort(key=lambda x: x["estimated_interest"], reverse=True)

    out_file = OUT / "province_interest.jsonl"
    with out_file.open("w", encoding="utf-8") as f:
        for row in output_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"Province Interest Calculation complete: {len(output_rows)} estimates written -> {out_file}")
    return output_rows

if __name__ == "__main__":
    calculate_province_interest()
