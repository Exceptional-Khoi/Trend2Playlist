"""Bounded metadata collection from KKBOX's official hourly chart page."""

import json
import re
from datetime import datetime, timezone


HOURLY_URL = "https://kma.kkbox.com/charts/hourly?terr=tw&lang=tc"
_UNICODE_ESCAPE = re.compile(r"\\u([0-9a-fA-F]{4})")


def _decode_page_strings(value):
    """Decode the extra escaped Unicode layer used by KKBOX's inline JSON."""
    if isinstance(value, str):
        value = _UNICODE_ESCAPE.sub(lambda match: chr(int(match[1], 16)), value)
        if any(0xD800 <= ord(char) <= 0xDFFF for char in value):
            value = value.encode("utf-16", "surrogatepass").decode("utf-16")
        return value
    if isinstance(value, list):
        return [_decode_page_strings(item) for item in value]
    if isinstance(value, dict):
        return {key: _decode_page_strings(item) for key, item in value.items()}
    return value


def _inline_json(source, variable, default=None):
    match = re.search(r"\bvar\s+" + re.escape(variable) + r"\s*=\s*", source)
    if not match:
        return default
    try:
        value, _end = json.JSONDecoder().raw_decode(source, match.end())
    except ValueError:
        return default
    return _decode_page_strings(value)


def parse_hourly_page(source, limit=50):
    """Return chart rows with source timestamps, cadence and available rank history."""
    chart = _inline_json(source, "chart", {})
    history = _inline_json(source, "rankings", {})
    messages = _inline_json(source, "msg", {})
    if not isinstance(chart, dict) or not isinstance(chart.get("charts"), dict):
        raise ValueError("KKBOX hourly page contains no structured chart object")
    updated_at = chart.get("updated_at")
    note = messages.get("hourly_chart_note") if isinstance(messages, dict) else None
    common = {
        "territory": _inline_json(source, "terr", "tw"),
        "chart_type": "hourly",
    }
    if isinstance(updated_at, (int, float)):
        common["source_updated_at_unix"] = updated_at
        common["source_updated_at_utc"] = datetime.fromtimestamp(updated_at, timezone.utc).isoformat()
    if isinstance(messages, dict) and messages.get("hourly_chart_title"):
        common["chart_title"] = messages["hourly_chart_title"]
    if note:
        common["source_chart_method_note"] = note.replace("\\n", "\n")
        # The source expressly says it automatically sorts and updates each hour.
        if "\u6bcf\u5c0f\u6642" in note:
            common["source_update_interval_seconds"] = 3600
    records = []
    seen = set()
    for kind, items in chart["charts"].items():
        if not isinstance(items, list):
            continue
        history_items = history.get("charts", {}).get(kind, []) if isinstance(history, dict) else []
        # History omits track IDs, so only attach an unambiguous exact metadata match.
        history_index = {}
        duplicate_history_keys = set()
        for item in history_items:
            key = (item.get("song_name"), item.get("artist_roles"), item.get("album_name"))
            if key in history_index:
                duplicate_history_keys.add(key)
            history_index[key] = item
        for item in items:
            if not isinstance(item, dict) or not item.get("song_name") or not item.get("song_url"):
                continue
            identity = (kind, item.get("song_id") or item["song_url"])
            if identity in seen:
                continue
            seen.add(identity)
            payload = {**item, **common, "chart_category": kind}
            playlist = chart.get("playlist_id", {}).get(kind)
            if playlist:
                payload["chart_playlist_id"] = playlist
            key = (item.get("song_name"), item.get("artist_roles"), item.get("album_name"))
            history_item = history_index.get(key)
            if history_item and key not in duplicate_history_keys and history_item.get("rankings"):
                payload["hourly_rank_history"] = history_item["rankings"]
                if isinstance(history.get("updated_at"), (int, float)):
                    payload["rank_history_updated_at_unix"] = history["updated_at"]
            records.append(payload)
            if len(records) >= limit:
                return records
    if not records:
        raise ValueError("No song metadata in public KKBOX hourly chart")
    return records


def collect_kkbox_hourly(context):
    def collect_page():
        status, _headers, body = context.get(HOURLY_URL)
        if status < 200 or status >= 300:
            raise ValueError("HTTP %s" % status)
        source = body.decode("utf-8", errors="replace")
        for payload in parse_hourly_page(source):
            context.record("kkbox", "chart_track", payload, HOURLY_URL)

    context.attempt("kkbox", "hourly_song_chart", HOURLY_URL, collect_page)
