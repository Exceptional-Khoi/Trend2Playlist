"""Small public chart probes for additional East Asian music services."""

import json
import re
from html.parser import HTMLParser
from urllib.parse import urljoin

from survey_western import _clean, _get, _walk


class _Node:
    def __init__(self, tag, attrs=None):
        self.tag = tag
        self.attrs = attrs or {}
        self.children = []

    def nodes(self):
        yield self
        for child in self.children:
            if isinstance(child, _Node):
                yield from child.nodes()

    def text(self):
        return _clean(" ".join(child.text() if isinstance(child, _Node) else child
                               for child in self.children))


class _Tree(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = _Node("root")
        self.stack = [self.root]

    def handle_starttag(self, tag, attrs):
        node = _Node(tag, dict(attrs))
        self.stack[-1].children.append(node)
        if tag not in {"area", "base", "br", "col", "embed", "hr", "img", "input",
                       "link", "meta", "param", "source", "track", "wbr"}:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.stack[-1].children.append(_Node(tag, dict(attrs)))

    def handle_endtag(self, tag):
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == tag:
                del self.stack[index:]
                break

    def handle_data(self, value):
        self.stack[-1].children.append(value)


def _tree(source):
    tree = _Tree()
    tree.feed(source)
    return tree.root


def _duration(value):
    match = re.search(r"(?<!\d)(\d{1,2}):(\d{2})(?!\d)", value)
    return int(match[1]) * 60 + int(match[2]) if match else None


def _class(node, value):
    return value in node.attrs.get("class", "").split()


def _kugou(context):
    url = "https://www.kugou.com/yy/rank/home/1-8888.html"

    def crawl():
        source = _get(context, url)
        root = _tree(source)
        seen = set()
        count = 0
        chart_date = re.search(r"(20\d{2}-\d{2}-\d{2})\s*(?:更新|<)", source)
        for row in root.nodes():
            if row.tag != "li":
                continue
            nodes = list(row.nodes())
            song_link = next((node for node in nodes if node.tag == "a" and
                              ("/song/" in node.attrs.get("href", "") or
                               "pc_temp_songname" in node.attrs.get("class", ""))), None)
            if not song_link:
                continue
            href = urljoin(url, song_link.attrs.get("href", ""))
            display = song_link.text()
            identity = href if re.search(r"/(?:song|mixsong)/", href) else display
            if not display or identity in seen:
                continue
            seen.add(identity)
            # Current Kugou chart anchors display "song - artist"; retain the
            # original display and title attribute as evidence for parsing.
            title, sep, artist = display.rpartition(" - ")
            if not sep:
                title, artist = display, None
            rank = next((node.text() for node in nodes if _class(node, "pc_temp_num")), "")
            match = re.search(r"\d+", rank)
            data = {"title": title, "artist": artist, "url": href,
                    "source_position": count + 1, "duration_seconds": _duration(row.text()),
                    "chart": "Kugou TOP500", "display_name": display,
                    "anchor_title": song_link.attrs.get("title")}
            if match:
                data["chart_position"] = int(match[0])
            if chart_date:
                data["chart_date"] = chart_date[1]
            for key in ("data-hash", "data-id"):
                if row.attrs.get(key):
                    data[key.removeprefix("data-")] = row.attrs[key]
            context.record("kugou", "chart_track", data, url)
            count += 1
            if count >= 100:
                break
        if not count:
            raise ValueError("No public Kugou chart song rows")

    context.attempt("kugou", "chart_tracks", url, crawl)


def _qqmusic(context):
    url = "https://y.qq.com/n/ryqq/toplist/4"

    def crawl():
        source = _get(context, url)
        initial = re.search(r"window\.__INITIAL_DATA__\s*=\s*", source)
        if initial:
            # QQ publishes a JSON-shaped object with occasional JS undefined
            # values. Replace only standalone tokens outside quoted strings,
            # then decode data; do not evaluate any JavaScript.
            value = source[initial.end():]
            value = re.sub(r'"(?:\\.|[^"\\])*"|\bundefined\b',
                           lambda match: "null" if match[0] == "undefined" else match[0], value)
            try:
                initial_data, _end = json.JSONDecoder().raw_decode(value)
            except ValueError:
                initial_data = {}
            details = initial_data.get("data", {})
            ranks = {item.get("songId"): item for item in initial_data.get("rankList", [])}
            songs = initial_data.get("songInfoList", [])
            if songs:
                for position, song in enumerate(songs[:100], 1):
                    rank = ranks.get(song.get("id"), {})
                    album_data = song.get("album", {})
                    payload = {"id": song.get("id"), "mid": song.get("mid"),
                               "title": song.get("title") or song.get("name"),
                               "subtitle": song.get("subtitle"),
                               "duration_seconds": song.get("interval"),
                               "url": "https://y.qq.com/n/ryqq/songDetail/" + str(song.get("mid")),
                               "artists": [{key: artist[key] for key in ("id", "mid", "name", "title")
                                            if key in artist} for artist in song.get("singer", [])],
                               "album": {key: album_data[key] for key in
                                         ("id", "mid", "name", "title", "time_public") if key in album_data},
                               "source_position": position, "chart_id": details.get("topId", 4),
                               "chart_title": details.get("title"), "chart_period": details.get("period"),
                               "chart_position": rank.get("rank"), "rank_type": rank.get("rankType"),
                               "rank_value": rank.get("rankValue"), "cover": rank.get("cover")}
                    context.record("qq_music", "chart_track", payload, url)
                return
        root = _tree(source)
        count = 0
        seen = set()
        for row in root.nodes():
            if row.tag != "li":
                continue
            nodes = list(row.nodes())
            song = next((node for node in nodes if node.tag == "a" and
                         "/songDetail/" in node.attrs.get("href", "")), None)
            if not song:
                continue
            href = urljoin(url, song.attrs["href"])
            if href in seen:
                continue
            title = song.attrs.get("title") or song.text()
            if not title:
                continue
            seen.add(href)
            artist_links = [node for node in nodes if node.tag == "a" and
                            re.search(r"/(?:singer|singerDetail)/", node.attrs.get("href", ""))]
            album = next((node for node in nodes if node.tag == "a" and
                          "/albumDetail/" in node.attrs.get("href", "")), None)
            artists = [{"name": node.attrs.get("title") or node.text(),
                        "url": urljoin(url, node.attrs.get("href", ""))} for node in artist_links]
            payload = {"title": title, "url": href, "artists": artists,
                       "duration_seconds": _duration(row.text()), "source_position": count + 1,
                       "chart_id": 4}
            if album:
                # A cover link's title describes the song, so only claim an
                # album title when there is actual visible album text.
                payload["album"] = {"title": album.text() or None,
                                    "url": urljoin(url, album.attrs["href"])}
            context.record("qq_music", "chart_track", payload, url)
            count += 1
            if count >= 100:
                break
        if not count:
            raise ValueError("No public QQ Music SSR song rows; page may require JavaScript or regional access")

    context.attempt("qq_music", "chart_tracks", url, crawl)


def _kkbox(context):
    url = "https://kma.kkbox.com/charts/daily/song?terr=tw"
    state = {"count": 0}

    def page():
        source = _get(context, url)
        chart = re.search(r"var\s+chart\s*=\s*", source)
        if chart:
            try:
                tracks, _end = json.JSONDecoder().raw_decode(source, chart.end())
            except ValueError:
                tracks = []
            chart_date = re.search(r'var\s+chartDate\s*=\s*"([^"]+)"', source)
            category = re.search(r"var\s+categoryId\s*=\s*(\d+)", source)
            for item in tracks[:50]:
                if not item.get("song_name") or not item.get("song_url"):
                    continue
                payload = {**item, "territory": "tw", "chart_type": "daily_song",
                           "chart_date": chart_date[1] if chart_date else None,
                           "category_id": int(category[1]) if category else None}
                context.record("kkbox", "chart_track", payload, url)
                state["count"] += 1
            if state["count"]:
                return
        root = _tree(source)
        seen = set()
        for node in root.nodes():
            if node.tag != "a":
                continue
            href = urljoin(url, node.attrs.get("href", ""))
            if not re.search(r"kkbox\.com/.*/(?:song|track)/", href) or href in seen:
                continue
            title = node.attrs.get("title") or node.text()
            if not title:
                continue
            seen.add(href)
            context.record("kkbox", "chart_track_link", {"title": title, "url": href,
                           "source_position": len(seen), "territory": "tw", "category_id": 297}, url)
            state["count"] += 1
            if state["count"] >= 50:
                break
        if not state["count"]:
            raise ValueError("No public KKBOX chart song links")

    context.attempt("kkbox", "chart_page", url, page)
    if state["count"]:
        return
    from datetime import datetime, timedelta, timezone
    date = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    api_url = ("https://kma.kkbox.com/charts/api/v1/daily?category=297&date=" + date +
               "&lang=tc&limit=50&terr=tw&type=song")

    def public_chart():
        result = json.loads(_get(context, api_url))
        seen = set()
        for item in _walk(result):
            if not item.get("song_name") or not item.get("song_url"):
                continue
            if item["song_url"] in seen:
                continue
            seen.add(item["song_url"])
            context.record("kkbox", "chart_track", item, api_url)
            state["count"] += 1
            if state["count"] >= 50:
                break
        if not state["count"]:
            raise ValueError("No tracks in public KKBOX chart JSON")

    context.attempt("kkbox", "daily_song_chart", api_url, public_chart)


def collect(context):
    from survey_realtime_qq import collect_qq_mv
    from survey_realtime_kkbox import collect_kkbox_hourly

    _kugou(context)
    _qqmusic(context)
    collect_qq_mv(context)
    _kkbox(context)
    collect_kkbox_hourly(context)
