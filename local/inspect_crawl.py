"""Tóm tắt các JSONL do crawler tạo mà không cần Kafka/Spark.

Chạy từ repo:
    python local/inspect_crawl.py out/validation

Output là JSON để dễ kiểm tra hoặc đưa vào báo cáo; không in nội dung bình luận
hay secret. Mỗi dòng input có dạng {topic, key, value} của JsonlSink.
"""
import argparse
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
from textnorm import find_province_mentions  # noqa: E402

if getattr(sys.stdout, "reconfigure", None):
    sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")


def read_values(path):
    if not os.path.isfile(path):
        return []
    values = []
    with open(path, encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            try:
                values.append(json.loads(line)["value"])
            except (KeyError, json.JSONDecodeError) as exc:
                raise ValueError(f"{path}:{line_no}: JSONL không hợp lệ") from exc
    return values


def chart_stats(rows):
    unique_key = lambda x: (x.get("record_type"), x.get("chart_id"), x.get("chart_scope"),
                            x.get("snapshot_id"), x.get("rank"))
    groups = Counter((x.get("source"), x.get("chart_id"), x.get("chart_scope")) for x in rows)
    return {
        "rows": len(rows),
        "unique_rows": len({unique_key(x) for x in rows}),
        "unique_tracks": len({x.get("track_key") for x in rows if x.get("track_key")}),
        "groups": [{"source": s, "chart_id": c, "scope": scope, "rows": n}
                   for (s, c, scope), n in sorted(groups.items())],
    }


def playlist_stats(rows):
    keys = {(x.get("playlist_id"), x.get("snapshot_id"), x.get("position")) for x in rows}
    sources = Counter(x.get("source") for x in rows)
    return {
        "rows": len(rows),
        "unique_rows": len(keys),
        "unique_playlists": len({x.get("playlist_id") for x in rows}),
        "unique_tracks": len({x.get("track_key") for x in rows if x.get("track_key")}),
        "by_source": dict(sorted(sources.items())),
    }


def comment_stats(rows):
    ids = {x.get("comment_id") for x in rows if x.get("comment_id")}
    kind_counts, mentioned_comments = Counter(), 0
    provinces = set()
    for x in rows:
        mentions = find_province_mentions(x.get("text") or "", x.get("title") or "")
        if mentions:
            mentioned_comments += 1
        for province, kind in mentions:
            provinces.add(province)
            kind_counts[kind] += 1
    return {
        "rows": len(rows),
        "unique_comments": len(ids),
        "duplicates": len(rows) - len(ids),
        "videos": len({x.get("video_id") for x in rows if x.get("video_id")}),
        "tracks": len({x.get("track_key") for x in rows if x.get("track_key")}),
        "comments_with_province_signal": mentioned_comments,
        "province_mentions": dict(sorted(kind_counts.items())),
        "provinces_covered": len(provinces),
    }


def trend_stats(rows):
    keys = {(x.get("snapshot_id"), x.get("group_id"), x.get("track_key"), x.get("geo_code")) for x in rows}
    return {
        "rows": len(rows),
        "unique_rows": len(keys),
        "tracks": len({x.get("track_key") for x in rows if x.get("track_key")}),
        "regions": len({x.get("geo_code") for x in rows if x.get("geo_code")}),
        "rows_with_data": sum(bool(x.get("has_data")) for x in rows),
        "anchor": next((x.get("anchor_query") for x in rows if x.get("anchor_query")), None),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("directory", nargs="?", default="out/validation")
    args = ap.parse_args()
    charts = read_values(os.path.join(args.directory, "music.charts.jsonl"))
    playlists = read_values(os.path.join(args.directory, "music.playlists.jsonl"))
    comments = read_values(os.path.join(args.directory, "music.comments.jsonl"))
    trends = read_values(os.path.join(args.directory, "music.trends.jsonl"))
    report = {
        "charts": chart_stats(charts),
        "playlists": playlist_stats(playlists),
        "comments": comment_stats(comments),
        "trends": trend_stats(trends),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
