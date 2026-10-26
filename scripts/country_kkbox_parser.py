"""Parse saved KKBOX charts; no network requests or model training."""
from datetime import datetime, timezone
from html import unescape
import json
import re

CATEGORIES = {
    ("TW", 297): ("Mandarin", "mandarin"),
    ("SG", 297): ("Mandarin", "mandarin"),
    ("HK", 320): ("Local", "local"),
    ("JP", 733): ("Japanese/domestic", "domestic"),
}

def embedded(html, name):
    match = re.search(r"^\s*var\s+" + re.escape(name) + r"\s*=\s*(.*);\s*$", html, re.MULTILINE)
    if not match:
        raise ValueError("Missing embedded " + name)
    return json.loads(match.group(1))

def clean(value):
    if isinstance(value, list):
        return [clean(v) for v in value]
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items()}
    if isinstance(value, str):
        value = re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), value).replace(r"\/", "/")
        if any(0xD800 <= ord(c) <= 0xDFFF for c in value):
            value = value.encode("utf-16-le", "surrogatepass").decode("utf-16-le")
        return unescape(value)
    return value

def parse_kkbox(html, response_metadata):
    country = embedded(html, "terr").upper()
    if country != response_metadata.get("country_code"):
        raise ValueError("Returned territory differs from requested territory")
    category = embedded(html, "categoryId")
    chart_type = embedded(html, "chartType")
    if chart_type != "song":
        raise ValueError("Returned chart is not a song chart")
    chart_date = embedded(html, "chartDate")
    datetime.strptime(chart_date, "%Y-%m-%d")
    name, group = CATEGORIES.get((country, category), ("Unknown category", None))
    metadata = {
        "source": "kkbox", "platform": "kkbox", "country_code": country,
        "chart_scope": country, "chart_kind": "category",
        "category_id": category, "category_name": name, "category_group": group,
        "chart_type": chart_type, "source_period": chart_date, "period_type": "daily",
        "source_url": response_metadata["source_url"],
        "crawled_at": response_metadata.get("crawled_at") or response_metadata["requested_at"],
        "status": "parsed",
    }
    records = []
    for row in clean(embedded(html, "chart")):
        rankings = row["rankings"]
        record = dict(metadata)
        record.pop("status")
        record.update({
            "platform_id": row["song_id"], "title": row["song_name"],
            "artists": [row["artist_name"]], "native_artist_name": row["artist_name"],
            "artist_roles": row.get("artist_roles"), "album_name": row.get("album_name"),
            "rank": rankings["this_period"], "rank_previous": rankings.get("last_period"),
            "url": row.get("song_url"), "artist_url": row.get("artist_url"),
            "album_url": row.get("album_url"),
            "cover_url": row.get("cover_image", {}).get("normal"),
            "release_unix": row.get("release_date"), "streams": None,
        })
        records.append(record)
    records.sort(key=lambda r: r["rank"])
    metadata["record_count"] = len(records)
    return records, metadata
