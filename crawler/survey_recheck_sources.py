"""Follow-up probes for public feeds that may change during the day."""

import json
import re
import xml.etree.ElementTree as ET
from html import unescape


def _get(context, url):
    status, _headers, body = context.get(url)
    if not 200 <= status < 300:
        raise ValueError("HTTP %s" % status)
    return body.decode("utf-8", errors="replace")


def _netease(context, url, chart_id, title):
    def run():
        source = _get(context, url)
        match = re.search(r'<textarea[^>]+id="song-list-pre-data"[^>]*>(.*?)</textarea>', source, re.S)
        if not match:
            raise ValueError("No public NetEase chart rows")
        rows = json.loads(unescape(match.group(1)))
        for position, item in enumerate(rows[:100], 1):
            artists = [a.get("name") for a in item.get("artists", []) if a.get("name")]
            context.record("NetEase Cloud Music", "realtime_chart_track", {
                "chart_id": chart_id, "chart_title": title, "chart_position": position,
                "id": item.get("id"), "title": item.get("name"), "artists": artists,
                "album": (item.get("album") or {}).get("name"),
                "duration_ms": item.get("duration"), "score": item.get("score"),
                "publish_time_ms": item.get("publishTime"),
                "chart_signal": "public chart page labelled realtime",
                "collection_method": "public HTML snapshot",
            }, url)
        if not rows:
            raise ValueError("Empty NetEase chart")
    context.attempt("NetEase Cloud Music", "realtime_chart", url, run)


def _jiosaavn(context):
    url = "https://www.jiosaavn.com/api.php?__call=content.getTrending&entity_type=song&entity_language=hindi&_format=json&_marker=0&ctx=web6.0"
    def run():
        value = json.loads(_get(context, url))
        count = 0
        for item in value if isinstance(value, list) else []:
            data = item.get("details") if isinstance(item, dict) else None
            if not isinstance(data, dict) or not data.get("id"):
                continue
            context.record("JioSaavn", "trending_snapshot", {
                "id": data.get("id"), "title": data.get("song") or data.get("title"),
                "artist": data.get("primary_artists") or (data.get("artist") or {}).get("name"),
                "album": data.get("album"), "release_date": data.get("release_date"),
                "play_count": data.get("play_count"), "weight": item.get("weight"),
                "collection_method": "public trending API snapshot",
            }, url)
            count += 1
        if not count:
            raise ValueError("No JioSaavn trending songs")
    context.attempt("JioSaavn", "trending_snapshot", url, run)


def _archive(context):
    url = "https://archive.org/advancedsearch.php?q=collection%3Aetree&rows=20&output=json&sort%5B%5D=publicdate+desc&fl%5B%5D=identifier&fl%5B%5D=title&fl%5B%5D=creator&fl%5B%5D=publicdate&fl%5B%5D=addeddate&fl%5B%5D=downloads&fl%5B%5D=num_reviews&fl%5B%5D=avg_rating"
    def run():
        value = json.loads(_get(context, url))
        docs = ((value.get("response") or {}).get("docs") or [])
        for item in docs:
            context.record("Internet Archive (music archive)", "recent_public_additions", item, url)
        if not docs:
            raise ValueError("No recent archive additions")
    context.attempt("Internet Archive (music archive)", "recent_public_additions", url, run)


def _qobuz(context):
    url = "https://www.qobuz.com/us-en/rss/new-releases/download-streaming-albums"
    def run():
        root = ET.fromstring(_get(context, url))
        items = root.findall(".//item")
        for item in items[:50]:
            def value(name):
                node = item.find(name)
                return node.text.strip() if node is not None and node.text else None
            context.record("Qobuz", "new_release_feed", {
                "title": value("title"), "url": value("link"),
                "published": value("pubDate"), "description": value("description"),
                "feed_signal": "public new-release RSS; publication time is exposed",
            }, url)
        if not items:
            raise ValueError("No Qobuz RSS releases")
    context.attempt("Qobuz", "new_release_feed", url, run)


def collect(context):
    _netease(context, "https://music.163.com/discover/toplist?id=8246775932", 8246775932, "实时热度榜")
    _netease(context, "https://music.163.com/discover/toplist?id=18176153161", 18176153161, "实时分享榜")
    _jiosaavn(context)
    _archive(context)
    _qobuz(context)
