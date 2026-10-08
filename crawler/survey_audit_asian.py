"""Bounded public metadata alternatives for the realtime source audit.

Networking and request evidence belong to the caller. These parsers retain only
song metadata and counters; no media links, lyrics, tokens, or write actions.
A counter snapshot and a chart labelled realtime do not establish latency.
"""

import hashlib
import json
import re
from html import unescape
from pathlib import Path
from urllib.parse import urljoin

from survey_asian import _DOM, _artists, _clean, _embedded, _fetch, _objects


NCT_URL = "https://www.nhaccuatui.com/song/cdDJjfkc8OFC"
NHACVN_URL = "https://nhac.vn/bai-hat/xin-dung-lang-im-soobin-hoang-son-soqXPxo"
JIO_DETAILS_URL = "https://www.jiosaavn.com/api.php?__call=song.getDetails&pids=YiVML4Zo&_format=json&_marker=0&ctx=web6.0"
JIO_TRENDING_URL = "https://www.jiosaavn.com/api.php?__call=content.getTrending&entity_type=song&entity_language=hindi&_format=json&_marker=0&ctx=web6.0"
KUGOU_URL = "https://www.kugou.com/yy/rank/home/1-6666.html?from=rank"
KUWO_URL = "https://www.kuwo.cn/api/www/bang/bang/musicList?bangId=93&pn=1&rn=30&httpsStatus=1"
JOOX_URL = "https://www.joox.com/th/single/8nhbykWZT2WJhX6mOHPSiA%3D%3D"
CHIA_URL = "https://chiasenhac.vn/"
NETEASE_CHARTS = (
    ("8246775932", "实时热度榜"),
    ("18176153161", "实时分享榜"),
)


def _attempt(context, platform, category, url, callback):
    # An offline cache miss is not another failed network request. The runner
    # retains the original network failure independently in its probe summary.
    directory = getattr(getattr(context, "store", None), "directory", None)
    if getattr(context, "offline", False) and directory is not None:
        identity = hashlib.sha256(("GET" + url).encode()).hexdigest()[:20]
        folder = Path(directory) / "http"
        if not (folder / (identity + ".json")).exists() or not (folder / (identity + ".body")).exists():
            return
    return context.attempt(platform, category, url, callback)


def _number(value):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and re.fullmatch(r"\d+", value):
        return int(value)
    return None


def _nct(context):
    def run():
        dom = _DOM(_fetch(context, NCT_URL))
        for state in _embedded(dom):
            for item in _objects(state):
                if item.get("key") != "cdDJjfkc8OFC" or not item.get("name") or "duration" not in item:
                    continue
                artists = _artists(item.get("artist") or item.get("artistName"))
                if not artists:
                    continue
                record = {
                    "id": item["key"], "title": _clean(item["name"]),
                    "artists": artists, "url": NCT_URL,
                    "duration_s": _number(item.get("duration")),
                    "artwork_url": item.get("image"), "genre": item.get("genreName"),
                    "release_timestamp_ms": _number(item.get("dateRelease")),
                    "collection_method": "public song page hydration snapshot",
                    "source_refresh_interval": None,
                }
                for target, field in (("likes", "totalLiked"), ("comments", "commentCnt"), ("shares", "shareCnt")):
                    value = _number(item.get(field))
                    if value is not None:
                        record[target] = value
                # viewed=0 in the current SSR data is not a verified play count.
                context.record("NhacCuaTui", "track_counter_snapshot", record, NCT_URL)
                return
        raise ValueError("Song page contains no matching structured song metadata")
    _attempt(context, "NhacCuaTui", "track_counter_snapshot", NCT_URL, run)


def _iso_duration(value):
    match = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?", value or "")
    if not match:
        return None
    total = int(match[1] or 0) * 3600 + int(match[2] or 0) * 60 + float(match[3] or 0)
    return int(total) if total.is_integer() else total


def _nhacvn(context):
    def run():
        dom = _DOM(_fetch(context, NHACVN_URL))
        for state in _embedded(dom):
            for item in _objects(state):
                if item.get("@type") != "MusicRecording" or not item.get("name"):
                    continue
                artists = _artists(item.get("byArtist"))
                if not artists:
                    continue
                producer = item.get("producer") or {}
                record = {
                    "id": "553660", "title": _clean(item["name"]),
                    "artists": artists, "url": item.get("url") or NHACVN_URL,
                    "duration_s": _iso_duration(item.get("duration")),
                    "artwork_url": item.get("image"), "genre": item.get("genre"),
                    "language": item.get("inLanguage"),
                    "producer": _clean(producer.get("name")) if isinstance(producer, dict) else None,
                    "collection_method": "public MusicRecording JSON-LD snapshot",
                    "source_refresh_interval": None,
                }
                # Facebook data-num-posts config is a display limit, not a count.
                context.record("Nhac.vn", "track_snapshot", record, NHACVN_URL)
                return
        raise ValueError("No genuine MusicRecording JSON-LD on this song page")
    _attempt(context, "Nhac.vn", "track_snapshot", NHACVN_URL, run)


def _jio_song(item):
    if not isinstance(item, dict) or not item.get("id") or not (item.get("song") or item.get("title")):
        return None
    record = {
        "id": item["id"], "title": _clean(item.get("song") or item.get("title")),
        "artists": _artists(item.get("primary_artists") or item.get("singers")),
        "album": _clean(item.get("album")), "duration_s": _number(item.get("duration")),
        "release_year": _number(item.get("year")), "release_date": item.get("release_date"),
        "language": item.get("language"), "label": item.get("label"),
        "artwork_url": item.get("image"), "url": item.get("perma_url"),
        "collection_method": "public web API metadata snapshot",
        "source_refresh_interval": None,
    }
    count = _number(item.get("play_count"))
    if count is not None:
        record["plays"] = count
    return record


def _jio(context):
    def details():
        data = json.loads(_fetch(context, JIO_DETAILS_URL))
        items = data.values() if isinstance(data, dict) else data if isinstance(data, list) else []
        count = 0
        for item in items:
            record = _jio_song(item)
            if record and record["id"] == "YiVML4Zo":
                context.record("JioSaavn", "track_counter_snapshot", record, JIO_DETAILS_URL)
                count += 1
        if not count:
            raise ValueError("No matching JioSaavn song details")

    def trending():
        data = json.loads(_fetch(context, JIO_TRENDING_URL))
        count = 0
        for position, item in enumerate(data[:100] if isinstance(data, list) else [], 1):
            # This feed mixes albums, playlists, and songs despite entity_type=song.
            if not isinstance(item, dict) or item.get("type") != "song":
                continue
            record = _jio_song(item.get("details"))
            if not record:
                continue
            record["source_list_position"] = position
            if isinstance(item.get("weight"), (int, float)):
                record["trending_weight"] = item["weight"]
            context.record("JioSaavn", "trending_track_snapshot", record, JIO_TRENDING_URL)
            count += 1
        if not count:
            raise ValueError("No type=song entries in JioSaavn trending response")
    _attempt(context, "JioSaavn", "track_counter_snapshot", JIO_DETAILS_URL, details)
    _attempt(context, "JioSaavn", "trending_track_snapshot", JIO_TRENDING_URL, trending)


def _netease(context, chart_id, expected_title):
    url = "https://music.163.com/discover/toplist?id=" + chart_id
    def run():
        dom = _DOM(_fetch(context, url))
        title = next((n.text() for n in dom.root.walk() if n.tag == "title"), "")
        if expected_title not in title:
            raise ValueError("Expected realtime chart title is absent from the returned page")
        songs_node = next((n for n in dom.root.walk() if n.tag == "textarea" and n.attrs.get("id") == "song-list-pre-data"), None)
        if songs_node is None:
            raise ValueError("No embedded public NetEase songs")
        songs = json.loads(unescape(songs_node.text()))
        if not isinstance(songs, list) or not songs:
            raise ValueError("Empty public NetEase songs")
        chart_header = next((n.text() for n in dom.root.walk() if n.attrs.get("class") == "cnt" and expected_title in n.text()), "")
        update = re.search(r"最近更新：\s*(\d+月\d+日)\s*（([^）]*)）", chart_header)
        for position, item in enumerate(songs[:100], 1):
            if not isinstance(item, dict) or not item.get("id") or not item.get("name"):
                continue
            album = item.get("album") or {}
            record = {
                "id": item["id"], "title": item["name"],
                "artists": _artists(item.get("artists")), "chart_position": position,
                "chart_id": chart_id, "chart_title": expected_title,
                "source_chart_label": "realtime", "source_refresh_interval": None,
                "duration_ms": _number(item.get("duration")), "score": item.get("score"),
                "release_timestamp_ms": item.get("publishTime"),
                "album": album.get("name"), "artwork_url": album.get("picUrl"),
                "url": "https://music.163.com/song?id=" + str(item["id"]),
                "collection_method": "public realtime chart HTML snapshot",
            }
            if update:
                record["source_update_date_label"] = update[1]
                record["source_update_status_label"] = update[2]
            context.record("NetEase Cloud Music", "realtime_chart_entry", record, url)
    _attempt(context, "NetEase Cloud Music", "realtime_chart_entry", url, run)


def _kugou(context):
    def run():
        dom = _DOM(_fetch(context, KUGOU_URL))
        title = next((n.text() for n in dom.root.walk() if n.tag == "title"), "")
        if "酷狗飙升榜" not in title:
            raise ValueError("Expected Kugou rising chart is absent")
        date = next((n.text() for n in dom.root.walk() if "rank_update" in n.attrs.get("class", "").split()), "")
        date_match = re.search(r"\d{4}-\d{2}-\d{2}", date)
        count, seen = 0, set()
        for row in dom.root.walk():
            if row.tag != "li":
                continue
            nodes = list(row.walk())
            link = next((n for n in nodes if n.tag == "a" and "pc_temp_songname" in n.attrs.get("class", "").split()), None)
            if link is None or not link.text() or link.text() in seen:
                continue
            display = re.sub(r"\s+", " ", link.text()).strip()
            seen.add(display)
            song, separator, artist = display.rpartition(" - ")
            if not separator:
                song, artist = display, None
            rank_node = next((n for n in nodes if "pc_temp_num" in n.attrs.get("class", "").split()), None)
            rank = re.search(r"\d+", rank_node.text()) if rank_node else None
            duration = re.search(r"(?<!\d)(\d{1,2}):(\d{2})(?!\d)", row.text())
            record = {
                "title": song, "artists": [artist] if artist else [],
                "chart_id": "6666", "chart_title": "酷狗飙升榜",
                "source_list_position": count + 1, "display_name": display,
                "url": urljoin(KUGOU_URL, link.attrs.get("href", "")),
                "source_refresh_interval": None,
                "collection_method": "public rising chart HTML snapshot",
            }
            if rank:
                record["chart_position"] = int(rank[0])
            if duration:
                record["duration_s"] = int(duration[1]) * 60 + int(duration[2])
            if date_match:
                record["chart_date"] = date_match[0]
            context.record("kugou", "chart_snapshot", record, KUGOU_URL)
            count += 1
            if count >= 100:
                break
        if not count:
            raise ValueError("No genuine Kugou rising chart song rows")
    _attempt(context, "kugou", "chart_snapshot", KUGOU_URL, run)


def _unavailable(context):
    def kuwo():
        value = json.loads(_fetch(context, KUWO_URL))
        if not isinstance(value, dict) or value.get("success") is False:
            message = value.get("message") if isinstance(value, dict) else "Non-object response"
            raise ValueError("Kuwo public API denied request: " + str(message))
        raise ValueError("No verified song-list schema in returned Kuwo response")
    _attempt(context, "Kuwo", "public_chart_API", KUWO_URL, kuwo)

    def joox():
        dom = _DOM(_fetch(context, JOOX_URL))
        for state in _embedded(dom):
            if isinstance(state, dict) and state.get("query", {}).get("region") == "intl":
                raise ValueError("Regional JOOX song URL returned the intl marketing page; no catalog data")
        raise ValueError("No verified regional song metadata returned by JOOX")
    _attempt(context, "JOOX", "public_song_page", JOOX_URL, joox)

    def chia():
        _fetch(context, CHIA_URL)
        raise ValueError("No verified ChiaSeNhac song metadata parser")
    _attempt(context, "ChiaSeNhac", "public_homepage", CHIA_URL, chia)

    for url in (
        "https://mobilecdnbj.kugou.com/api/v3/rank/info?rankid=8888",
        "https://mobilecdnbj.kugou.com/api/v3/rank/list?withsong=0&page=1&pagesize=100",
    ):
        def mobile(url=url):
            _fetch(context, url)
            raise ValueError("No verified Kugou mobile API song schema returned")
        _attempt(context, "kugou", "public_mobile_chart_API", url, mobile)


def collect(context):
    _nct(context)
    _nhacvn(context)
    _jio(context)
    for chart_id, title in NETEASE_CHARTS:
        _netease(context, chart_id, title)
    _kugou(context)
    _unavailable(context)
