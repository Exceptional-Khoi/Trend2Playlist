"""Bounded, unauthenticated metadata survey for Asian music services.

These adapters read public catalog pages or their public JSON responses. They
do not fetch audio, request login cookies, or generate signed API requests.
The caller owns networking, provenance, request limits, and error reporting.
"""

from html import unescape
from html.parser import HTMLParser
import json
import re
from urllib.parse import urljoin


class _Node:
    def __init__(self, tag="root", attrs=None, parent=None):
        self.tag = tag
        self.attrs = dict(attrs or [])
        self.parent = parent
        self.children = []

    def text(self):
        return " ".join(
            child.text() if isinstance(child, _Node) else child
            for child in self.children
        ).strip()

    def walk(self):
        yield self
        for child in self.children:
            if isinstance(child, _Node):
                yield from child.walk()


class _DOM(HTMLParser):
    VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}

    def __init__(self, content):
        super().__init__(convert_charrefs=True)
        self.root = _Node()
        self.current = self.root
        self.feed(content)

    def handle_starttag(self, tag, attrs):
        node = _Node(tag, attrs, self.current)
        self.current.children.append(node)
        if tag not in self.VOID:
            self.current = node

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in self.VOID:
            self.handle_endtag(tag)

    def handle_endtag(self, tag):
        node = self.current
        while node.parent is not None:
            if node.tag == tag:
                self.current = node.parent
                return
            node = node.parent

    def handle_data(self, data):
        self.current.children.append(data)


def _fetch(context, url):
    status, headers, body = context.get(url)
    if status >= 400:
        raise RuntimeError(f"HTTP {status}")
    if not body:
        raise ValueError("Empty response body")
    return body.decode("utf-8-sig", errors="replace")


def _objects(value, seen=None):
    if seen is None:
        seen = set()
    if isinstance(value, (dict, list)):
        identity = id(value)
        if identity in seen:
            return
        seen.add(identity)
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _objects(child, seen)
    elif isinstance(value, list):
        for child in value:
            yield from _objects(child, seen)


def _nuxt_state(values):
    """Resolve Nuxt/devalue's JSON reference table without evaluating JS."""
    memo = {}
    wrappers = {"Reactive", "ShallowReactive", "Ref", "ShallowRef", "EmptyRef", "EmptyShallowRef"}

    def resolve(index):
        if not isinstance(index, int) or index < 0 or index >= len(values):
            return None
        if index in memo:
            return memo[index]
        value = values[index]
        if isinstance(value, dict):
            result = {}
            memo[index] = result
            for key, reference in value.items():
                result[key] = resolve(reference)
            return result
        if isinstance(value, list):
            if value and isinstance(value[0], str) and value[0] in wrappers:
                memo[index] = None
                result = resolve(value[1]) if len(value) > 1 else None
                memo[index] = result
                return result
            if value and isinstance(value[0], str) and value[0] in {"Date", "BigInt"}:
                memo[index] = value[1] if len(value) > 1 else None
                return memo[index]
            result = []
            memo[index] = result
            result.extend(resolve(reference) for reference in value)
            return result
        memo[index] = value
        return value

    return resolve(0)


def _embedded(dom):
    for node in dom.root.walk():
        if node.tag not in {"script", "textarea"}:
            continue
        content = node.text()
        if not content:
            continue
        try:
            value = json.loads(content)
            if node.attrs.get("id") == "__NUXT_DATA__" and isinstance(value, list):
                value = _nuxt_state(value)
            yield value
            continue
        except (ValueError, TypeError):
            pass
        # Parse JSON serialization only; never evaluate website JavaScript.
        for marker in ("window.__INITIAL_STATE__=", "window.__INITIAL_STATE__ =", "window.__NUXT__="):
            if marker in content:
                tail = content.split(marker, 1)[1].lstrip()
                try:
                    yield json.JSONDecoder().raw_decode(tail)[0]
                except ValueError:
                    pass
        for match in re.finditer(r"self\.__next_f\.push\((\[.*?\])\)", content, re.S):
            try:
                chunk = json.loads(match.group(1))
            except ValueError:
                continue
            for part in chunk:
                if not isinstance(part, str):
                    continue
                for line in part.splitlines():
                    fragment = re.sub(r"^[a-zA-Z0-9]+:", "", line)
                    if fragment.startswith(("{", "[")):
                        try:
                            yield json.loads(fragment)
                        except ValueError:
                            pass


def _clean(value):
    return unescape(value).strip() if isinstance(value, str) else value


def _artists(value):
    if isinstance(value, str):
        return [_clean(name) for name in value.split(",") if name.strip()]
    if isinstance(value, dict):
        return [_clean(value.get("name") or value.get("title") or value.get("singername"))] if value else []
    if isinstance(value, list):
        names = []
        for artist in value:
            if isinstance(artist, dict):
                name = artist.get("name") or artist.get("title") or artist.get("singername")
            else:
                name = artist
            if isinstance(name, str) and name.strip():
                names.append(_clean(name))
        return names
    return []


def _scalar_fields(record, item, mapping):
    for target, keys in mapping.items():
        for key in keys:
            value = item.get(key)
            if value is not None and isinstance(value, (str, int, float, bool)):
                record[target] = _clean(value)
                break


def _generic_tracks(context, platform, content, url):
    """Extract structured track objects; keep an explicit 100-record ceiling."""
    dom = _DOM(content)
    seen = set()
    count = 0
    for state in _embedded(dom):
        for item in _objects(state):
            title = item.get("song") or item.get("songName") or item.get("songname") or item.get("name") or item.get("title")
            artists = _artists(item.get("artists") or item.get("ar") or item.get("singers") or item.get("artistList") or item.get("artist") or item.get("singer") or item.get("singerName"))
            identifier = item.get("songid") or item.get("songId") or item.get("song_id") or item.get("mid") or item.get("rid") or item.get("encodeId") or item.get("key") or item.get("id")
            is_track = any(k in item for k in ("songid", "songId", "song_id", "duration", "durationMs", "dt", "singers", "songName", "songname", "rid"))
            if not isinstance(title, str) or not artists or not is_track:
                continue
            identity = (str(identifier or ""), title, tuple(artists))
            if identity in seen:
                continue
            seen.add(identity)
            record = {"id": identifier, "title": _clean(title), "artists": artists}
            _scalar_fields(record, item, {
                "duration_s": ("duration", "durationSeconds"),
                "duration_ms": ("durationMs", "dt"),
                "artwork_url": ("thumbnail", "cover", "picUrl", "image", "imgurl"),
                "release_date": ("releaseDate", "publishTime", "pub_time"),
                "popularity": ("popularity", "pop"),
                "plays": ("playCount", "listenCount", "play_count"),
                "rank": ("rank", "rankIndex"),
            })
            album = item.get("album") or item.get("al")
            if isinstance(album, dict):
                record["album"] = _clean(album.get("name") or album.get("title"))
            elif isinstance(album, str):
                record["album"] = _clean(album)
            context.record(platform, "track", record, url)
            count += 1
            if count >= 100:
                return count
    return count


def _nhaccuatui(context):
    url = "https://www.nhaccuatui.com/"

    def run():
        content = _fetch(context, url)
        dom = _DOM(content)
        states = list(_embedded(dom))
        count = 0
        seen_tracks = set()

        def track(item):
            if not item.get("key") or not item.get("name") or "duration" not in item:
                return None
            artists = _artists(item.get("artist") or item.get("artistName"))
            if not artists:
                return None
            result = {"id": item["key"], "title": item["name"], "artists": artists}
            _scalar_fields(result, item, {"duration_s": ("duration",), "artwork_url": ("image",), "url": ("linkShare",), "release_timestamp_ms": ("dateRelease",), "genre": ("genreName",), "likes": ("totalLiked",), "comments": ("commentCnt",), "shares": ("shareCnt",)})
            return result

        for state in states:
            for section in _objects(state):
                if section.get("showType") != "chartResource":
                    continue
                for chart in section.get("child", []):
                    for item in chart.get("items", []):
                        record = track(item)
                        if not record:
                            continue
                        record.update({"chart_id": chart.get("key"), "chart_name": chart.get("title"), "chart_date": chart.get("pubDate")})
                        if isinstance(item.get("position"), int) and item["position"] > 0:
                            record["rank"] = item["position"]
                        if isinstance(item.get("lastPosition"), int) and item["lastPosition"] > 0:
                            record["previous_rank"] = item["lastPosition"]
                        context.record("NhacCuaTui", "chart_entry", record, url)
                        seen_tracks.add(record["id"])
                        count += 1
                        if count >= 100:
                            return
        for state in states:
            for item in _objects(state):
                record = track(item)
                if not record or record["id"] in seen_tracks:
                    continue
                seen_tracks.add(record["id"])
                context.record("NhacCuaTui", "track", record, url)
                count += 1
                if count >= 100:
                    return
        if not count:
            raise ValueError("Public homepage returned no parseable structured track objects")

    context.attempt("NhacCuaTui", "chart_entry and track", url, run)


def _jiosaavn(context):
    url = "https://www.jiosaavn.com/api.php?__call=search.getResults&q=arijit%20singh&_format=json&_marker=0&ctx=web6.0&n=50&p=1"

    def run():
        data = json.loads(_fetch(context, url))
        results = data.get("results") if isinstance(data, dict) else []
        if not isinstance(results, list) or not results:
            raise ValueError("Search response contains no song results")
        count = 0
        for item in results[:100]:
            info = item.get("more_info") or {}
            title = item.get("song") or item.get("title")
            if not title:
                continue
            record = {"id": item.get("id"), "title": _clean(title)}
            record["artists"] = _artists(item.get("singers") or info.get("singers") or item.get("primary_artists") or info.get("artistMap", {}).get("primary_artists"))
            _scalar_fields(record, item, {"album": ("album",), "duration_s": ("duration",), "language": ("language",), "release_year": ("year",), "url": ("perma_url",), "artwork_url": ("image",)})
            _scalar_fields(record, info, {"album": ("album",), "duration_s": ("duration",), "language": ("language",), "plays": ("play_count",), "label": ("label",), "explicit": ("explicit_content",)})
            for field in ("duration_s", "release_year", "plays"):
                if isinstance(record.get(field), str) and record[field].isdigit():
                    record[field] = int(record[field])
            context.record("JioSaavn", "track", record, url)
            count += 1
        if not count:
            raise ValueError("Search response contains no named songs")

    context.attempt("JioSaavn", "track", url, run)


def _netease(context):
    url = "https://music.163.com/discover/toplist?id=19723756"

    def run():
        content = _fetch(context, url)
        dom = _DOM(content)
        songs = None
        for node in dom.root.walk():
            if node.tag == "textarea" and node.attrs.get("id") == "song-list-pre-data":
                songs = json.loads(unescape(node.text()))
                break
        if not isinstance(songs, list) or not songs:
            raise ValueError("Chart page contains no public song-list-pre-data")
        for rank, item in enumerate(songs[:100], 1):
            artists = _artists(item.get("artists") or item.get("ar"))
            record = {"id": item.get("id"), "title": item.get("name"), "artists": artists, "rank": rank, "chart_id": "19723756"}
            _scalar_fields(record, item, {"duration_ms": ("duration", "dt"), "popularity": ("popularity", "pop"), "score": ("score",)})
            album = item.get("album") or item.get("al") or {}
            if isinstance(album, dict):
                record["album"] = album.get("name")
                record["artwork_url"] = album.get("picUrl")
            record["url"] = f"https://music.163.com/song?id={item.get('id')}"
            context.record("NetEase Cloud Music", "chart_entry", record, url)

    context.attempt("NetEase Cloud Music", "chart_entry", url, run)


def _kuwo(context):
    url = "https://www.kuwo.cn/rankList"

    def run():
        content = _fetch(context, url)
        if not _generic_tracks(context, "Kuwo", content, url):
            raise ValueError("Public chart page contains no parseable structured tracks")

    context.attempt("Kuwo", "track", url, run)


def _joox(context):
    url = "https://www.joox.com/th/chart/42"

    def run():
        content = _fetch(context, url)
        dom = _DOM(content)
        for state in _embedded(dom):
            if isinstance(state, dict) and state.get("query", {}).get("region") == "intl":
                raise ValueError("JOOX redirected this request to its /intl marketing page; no regional music catalog was returned")
        count = 0
        seen = set()
        for node in dom.root.walk():
            href = node.attrs.get("href", "")
            if node.tag != "a" or not re.search(r"/(?:song|single)/[^/?]+", href):
                continue
            title = node.text().strip()
            if not title or href in seen:
                continue
            seen.add(href)
            # Song and artist links share a compact row on the SSR chart page.
            row = node.parent
            artists = []
            for _ in range(5):
                artists = [a.text().strip() for a in row.walk() if a.tag == "a" and "/artist/" in a.attrs.get("href", "") and a.text().strip()]
                if artists or row.parent is None:
                    break
                row = row.parent
            if not artists:
                continue
            record = {"id": href.rsplit("/", 1)[-1], "title": title, "artists": list(dict.fromkeys(artists)), "url": urljoin(url, href), "chart_id": "42"}
            ranks = [n.text() for n in row.walk() if re.search(r"rank|order|index", n.attrs.get("class", ""), re.I) and n.text().isdigit()]
            if ranks:
                record["rank"] = int(ranks[0])
            else:
                record["source_list_position"] = count + 1
            context.record("JOOX", "chart_entry", record, url)
            count += 1
            if count >= 100:
                break
        if not count:
            count = _generic_tracks(context, "JOOX", content, url)
        if not count:
            raise ValueError("Chart page contains no parseable public song rows")

    context.attempt("JOOX", "chart_entry", url, run)


def _by_class(node, class_name):
    return [child for child in node.walk() if class_name in child.attrs.get("class", "").split()]


def _anchor_text(node):
    return [child.text().strip() for child in node.walk() if child.tag == "a" and child.text().strip()]


def _melon(context):
    url = "https://www.melon.com/chart/index.htm"

    def run():
        dom = _DOM(_fetch(context, url))
        count = 0
        for row in dom.root.walk():
            song_id = row.attrs.get("data-song-no")
            if row.tag != "tr" or not song_id:
                continue
            titles = _by_class(row, "rank01")
            artist_nodes = _by_class(row, "rank02")
            album_nodes = _by_class(row, "rank03")
            ranks = _by_class(row, "rank")
            if not titles or not artist_nodes:
                continue
            title = _anchor_text(titles[0])
            if not title:
                continue
            rank = next((int(n.text()) for n in ranks if n.text().isdigit()), count + 1)
            record = {"id": song_id, "title": title[0], "artists": list(dict.fromkeys(_anchor_text(artist_nodes[0]))), "rank": rank, "chart_id": "top100", "url": f"https://www.melon.com/song/detail.htm?songId={song_id}"}
            if album_nodes:
                albums = _anchor_text(album_nodes[0])
                if albums:
                    record["album"] = albums[0]
            images = [n.attrs.get("src") for n in row.walk() if n.tag == "img" and n.attrs.get("src")]
            if images:
                record["artwork_url"] = urljoin(url, images[0])
            context.record("Melon", "chart_entry", record, url)
            count += 1
            if count >= 100:
                break
        if not count:
            raise ValueError("TOP100 page contains no parseable public song rows")

    context.attempt("Melon", "chart_entry", url, run)


def _bugs(context):
    url = "https://music.bugs.co.kr/chart"

    def run():
        dom = _DOM(_fetch(context, url))
        count = 0
        for row in dom.root.walk():
            song_id = row.attrs.get("trackid")
            if row.tag != "tr" or not song_id:
                continue
            titles = _by_class(row, "title")
            artist_nodes = _by_class(row, "artist")
            if not titles or not artist_nodes:
                continue
            title = _anchor_text(titles[0])
            if not title:
                continue
            ranking = _by_class(row, "ranking")
            rank = count + 1
            if ranking:
                digits = next((n.text() for n in ranking[0].walk() if n.tag == "strong" and n.text().isdigit()), None)
                if digits:
                    rank = int(digits)
            record = {"id": song_id, "title": title[0], "artists": list(dict.fromkeys(_anchor_text(artist_nodes[0]))), "rank": rank, "chart_id": "realtime", "url": f"https://music.bugs.co.kr/track/{song_id}"}
            album_nodes = _by_class(row, "album")
            if album_nodes and album_nodes[0].text():
                record["album"] = album_nodes[0].text()
            images = [n.attrs.get("src") for n in row.walk() if n.tag == "img" and n.attrs.get("src")]
            if images:
                record["artwork_url"] = urljoin(url, images[0])
            context.record("Bugs", "chart_entry", record, url)
            count += 1
            if count >= 100:
                break
        if not count:
            raise ValueError("Public chart page contains no parseable trackid rows")

    context.attempt("Bugs", "chart_entry", url, run)


def _nhacvn(context):
    found = []

    def run(url, chart_requested):
        dom = _DOM(_fetch(context, url))
        is_chart = chart_requested and any("bảng xếp hạng bài hát" in node.text().casefold() for node in dom.root.walk() if node.tag == "title")
        count = 0
        seen = set()
        for node in dom.root.walk():
            href = node.attrs.get("href", "")
            match = re.search(r"/bai-hat/[^/?]+-(so[A-Za-z0-9]+)(?:[?#]|$)", href)
            if node.tag != "a" or not match or not node.text().strip():
                continue
            song_id = match.group(1)
            if song_id in seen:
                continue
            row = node.parent
            ancestor = node.parent
            for _ in range(6):
                if ancestor.tag == "li":
                    row = ancestor
                    break
                if ancestor.parent is None:
                    break
                ancestor = ancestor.parent
            artists = []
            for _ in range(6):
                artists = [child.text().strip() for child in row.walk() if child.tag == "a" and "/nghe-si/" in child.attrs.get("href", "") and child.text().strip()]
                if artists or row.parent is None:
                    break
                row = row.parent
            if not artists:
                continue
            seen.add(song_id)
            record = {"id": song_id, "title": node.text().strip(), "artists": list(dict.fromkeys(artists)), "url": urljoin(url, href)}
            if is_chart:
                record.update({"chart_id": "bxdE", "chart_name": "BXH bài hát Việt Nam"})
                ranks = [child.text().strip() for child in row.walk() if re.search(r"rank|order|position|number|(^|\s)num(\s|$)", child.attrs.get("class", ""), re.I) and child.text().strip().isdigit()]
                if ranks:
                    record["rank"] = int(ranks[0])
                else:
                    record["source_list_position"] = count + 1
            images = [child.attrs.get("src") or child.attrs.get("data-src") for child in row.walk() if child.tag == "img"]
            if images and images[0]:
                image_url = urljoin(url, images[0])
                record["artist_image_url" if "/artists/" in image_url else "artwork_url"] = image_url
            context.record("Nhac.vn", "chart_entry" if is_chart else "track", record, url)
            count += 1
            if count >= 100:
                break
        if not count:
            raise ValueError("Public Nhac.vn chart contains no identifiable song and artist rows")
        found.append(count)

    chart_url = "https://nhac.vn/chart"
    context.attempt("Nhac.vn", "chart_entry", chart_url, lambda: run(chart_url, True))
    if not found:
        homepage = "https://nhac.vn/"
        context.attempt("Nhac.vn", "track", homepage, lambda: run(homepage, False))


def _chiasenhac(context):
    url = "https://chiasenhac.vn/"

    def run():
        content = _fetch(context, url)
        if not _generic_tracks(context, "ChiaSeNhac", content, url):
            raise ValueError("Homepage contains no parseable structured song entities; generic site metadata is excluded")

    context.attempt("ChiaSeNhac", "track", url, run)


def collect(context):
    """Each platform attempt is isolated by the shared survey context."""
    for adapter in (_nhaccuatui, _jiosaavn, _netease, _kuwo, _joox, _melon, _bugs, _nhacvn, _chiasenhac):
        adapter(context)
