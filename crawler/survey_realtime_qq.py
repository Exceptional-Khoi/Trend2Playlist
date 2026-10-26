"""Public QQ Music MV chart probe; polling cadence is not an event stream."""

import json
import re

from survey_western import _get


QQ_MV_URL = "https://y.qq.com/n/ryqq/toplist/201"


def _initial_data(source):
    match = re.search(r"window\.__INITIAL_DATA__\s*=\s*", source)
    if not match:
        raise ValueError("QQ Music public page has no initial chart data")
    value = source[match.end():]
    # Interpret only the public data literal. QQ may emit JS undefined for
    # optional fields; quoted strings must remain unchanged.
    value = re.sub(r'"(?:\\.|[^"\\])*"|\bundefined\b',
                   lambda token: "null" if token[0] == "undefined" else token[0], value)
    result, _end = json.JSONDecoder().raw_decode(value)
    if not isinstance(result, dict):
        raise ValueError("QQ initial chart data is not an object")
    return result


def collect_qq_mv(context, source_url=QQ_MV_URL):
    """Collect MV chart snapshots with the platform's explicit cadence hint."""
    def crawl():
        result = _initial_data(_get(context, source_url))
        detail = result.get("data") or {}
        if detail.get("topId") != 201:
            raise ValueError("Response did not select QQ Music MV chart 201")
        rows = result.get("rankList") or detail.get("song") or []
        rows = [item for item in rows if isinstance(item, dict) and item.get("vid")]
        if not rows:
            raise ValueError("QQ Music MV chart has no public MV entries")
        metadata_by_vid = {}
        for key in ("mvInfoList", "videoInfoList", "songInfoList"):
            for item in result.get(key, []) or []:
                if isinstance(item, dict) and item.get("vid"):
                    metadata_by_vid[item["vid"]] = item
        seen = set()
        for position, row in enumerate(rows[:100], 1):
            vid = row["vid"]
            if vid in seen:
                continue
            seen.add(vid)
            info = metadata_by_vid.get(vid, {})
            payload = {
                "vid": vid, "title": row.get("title") or info.get("title") or info.get("name"),
                "artist": row.get("singerName"), "artist_mid": row.get("singerMid"),
                "url": "https://y.qq.com/n/ryqq/mv/" + vid,
                "chart_id": 201, "chart_title": detail.get("title"),
                "chart_position": row.get("rank", position),
                "rank_type": row.get("rankType"), "rank_value": row.get("rankValue"),
                "cover": row.get("cover"), "album_mid": row.get("albumMid"),
                "chart_period": detail.get("period"), "chart_update_time": detail.get("updateTime"),
                "source_update_hint": detail.get("updateTips"),
                "chart_description": detail.get("intro"),
                "data_kind": "music_video", "collection_method": "public_chart_snapshot",
            }
            hint = detail.get("updateTips") or ""
            cadence = re.search(r"每\s*(\d+)\s*分钟更新", hint)
            if cadence:
                payload["source_update_interval_seconds"] = int(cadence[1]) * 60
            # Empty slots in QQ's chart object are not metadata observations.
            # Do not persist them as if views, album or update times were known.
            payload = {key: value for key, value in payload.items() if value is not None and value != ""}
            if info:
                # Store named public MV metadata fields without media delivery
                # URLs or download fields. Different QQ page versions vary.
                payload["mv_metadata"] = {key: info[key] for key in
                                          ("id", "vid", "name", "title", "desc", "duration",
                                           "interval", "playcnt", "playCount", "pubdate",
                                           "publish_date", "singer", "singers") if key in info}
            context.record("qq_music", "chart_mv", payload, source_url)

    context.attempt("qq_music", "mv_chart_201", source_url, crawl)


def collect(context):
    collect_qq_mv(context)
