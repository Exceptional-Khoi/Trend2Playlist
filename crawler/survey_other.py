"""Bounded public metadata probes for additional full-song music services.

This module never fetches audio, video, login endpoints, or undocumented tokens.
The runner provides get(), record(), and attempt() and keeps the request evidence.
"""

import json
import re
from html.parser import HTMLParser
from urllib.parse import urlencode, urljoin, urlparse


SOURCES = {
    "TIDAL": "https://developer.tidal.com/documentation/embeds/embeds-overview",
    "Qobuz": "https://www.qobuz.com/gb-en/album/kind-of-blue-miles-davis/qe7yczkjg0zxc",
    "Amazon Music": "https://music.amazon.com/",
    "Anghami": "https://play.anghami.com/song/36078771",
    "Jamendo": "https://developer.jamendo.com/v3.0/authentication",
    "Internet Archive (music archive)": "https://archive.org/services/docs/api/",
    "Napster": "https://help.napster.com/hc/en-us/articles/45020535864461-Listening-to-Music-on-Napster",
}


class MusicPage(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.meta = {}
        self.scripts = []
        self.oembed = []
        self.links = []
        self.text = []
        self._script = None
        self._anchor = None
        self._title = False
        self.title = ""

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "meta":
            key = attrs.get("property") or attrs.get("name") or attrs.get("itemprop")
            if key and attrs.get("content"):
                self.meta[key] = attrs["content"]
        if tag == "link" and attrs.get("type") == "application/json+oembed":
            if attrs.get("href"):
                self.oembed.append(attrs["href"])
        if tag == "script":
            self._script = [attrs, []]
        if tag == "a" and attrs.get("href"):
            self._anchor = [attrs["href"], []]
        if tag == "img" and self._anchor and attrs.get("alt"):
            self._anchor[1].append(attrs["alt"])
        if tag == "title":
            self._title = True

    def handle_data(self, data):
        if self._script is not None:
            self._script[1].append(data)
            return
        if self._anchor:
            self._anchor[1].append(data)
        if self._title:
            self.title += data
        if data.strip():
            self.text.append(data.strip())

    def handle_endtag(self, tag):
        if tag == "script" and self._script is not None:
            self.scripts.append((self._script[0], "".join(self._script[1])))
            self._script = None
        if tag == "a" and self._anchor is not None:
            self.links.append((self._anchor[0], " ".join(self._anchor[1]).strip()))
            self._anchor = None
        if tag == "title":
            self._title = False


class QobuzTracks(HTMLParser):
    """Read the public album track rows without fetching their sample URLs."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.records = []
        self.current = None
        self.depth = 0
        self.capture = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if self.current is None and attrs.get("data-track-v2"):
            try:
                data = json.loads(attrs["data-track-v2"])
                album = json.loads(attrs.get("data-gtm", "{}")).get("product", {})
            except ValueError:
                return
            if not isinstance(data, dict) or not data.get("item_name"):
                return
            self.current = {
                "track_id": attrs.get("data-track"),
                "track_data": data,
                "album_product": album,
                "index": attrs.get("data-index"),
                "credits_display": [],
            }
            self.depth = 1
            return
        if self.current is not None:
            if tag == "div":
                self.depth += 1
            if "track__item--duration" in attrs.get("class", ""):
                self.capture = (tag, "duration_display", [])
            elif tag == "p" and "track__info" in attrs.get("class", ""):
                self.capture = (tag, "credits_display", [])

    def handle_data(self, data):
        if self.capture:
            self.capture[2].append(data)

    def handle_endtag(self, tag):
        if self.current is None:
            return
        if self.capture and tag == self.capture[0]:
            value = " ".join("".join(self.capture[2]).split())
            if self.capture[1] == "credits_display":
                self.current["credits_display"].append(value)
            else:
                self.current[self.capture[1]] = value
            self.capture = None
        if tag == "div":
            self.depth -= 1
            if self.depth == 0:
                self.records.append(self.current)
                self.current = None


class TidalEmbedRows(HTMLParser):
    """Extract the SSR list-item metadata in TIDAL's official embed player."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.records = []
        self.current = None
        self.capture = None
        self.album_title = None
        self.album_artist = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if "media-album" in attrs.get("class", "") and attrs.get("title", "").startswith("Album: "):
            self.album_title = attrs["title"][7:]
        if "media-artist" in attrs.get("class", "") and attrs.get("title", "").startswith("Artist: "):
            self.album_artist = attrs["title"][8:]
        if tag == "list-item" and attrs.get("product-type") == "track" and attrs.get("product-id"):
            self.current = {"track_id": attrs["product-id"], "explicit_badge": False}
        if self.current is not None:
            slots = {"title": "title", "artist": "artist_display", "index": "track_number_display", "duration": "duration_display"}
            if attrs.get("slot") in slots:
                self.capture = (tag, slots[attrs["slot"]], [])
            if "explicit" in attrs.get("class", "").split():
                self.current["explicit_badge"] = True

    def handle_data(self, data):
        if self.capture:
            self.capture[2].append(data)

    def handle_endtag(self, tag):
        if self.current is None:
            return
        if self.capture and tag == self.capture[0]:
            value = " ".join("".join(self.capture[2]).split())
            if value:
                self.current[self.capture[1]] = value
            self.capture = None
        if tag == "list-item":
            if self.current.get("title") and self.current.get("duration_display"):
                if self.album_title:
                    self.current["album_title"] = self.album_title
                if self.album_artist:
                    self.current["album_artist"] = self.album_artist
                self.records.append(self.current)
            self.current = None


def _fetch(context, url):
    status, headers, body = context.get(url)
    if status < 200 or status >= 300:
        raise ValueError("HTTP %s" % status)
    if not body:
        raise ValueError("Empty response")
    return body.decode("utf-8", errors="replace")


def _walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _schema_records(context, platform, page, url, maximum=60):
    count = 0
    seen = set()
    kinds = {
        "MusicRecording": "track_schema",
        "MusicAlbum": "album_schema",
        "MusicGroup": "artist_schema",
        "MusicPlaylist": "playlist_schema",
    }
    for attrs, text in page.scripts:
        if "ld+json" not in attrs.get("type", ""):
            continue
        try:
            document = json.loads(text)
        except ValueError:
            continue
        for node in _walk(document):
            types = node.get("@type", [])
            if isinstance(types, str):
                types = [types]
            category = next((kinds[k] for k in types if k in kinds), None)
            if not category or not (node.get("name") or node.get("url") or node.get("@id")):
                continue
            signature = json.dumps(node, sort_keys=True, ensure_ascii=False)
            if signature in seen:
                continue
            seen.add(signature)
            context.record(platform, category, node, url)
            count += 1
            if count >= maximum:
                return count
    return count


def _page_metadata(context, platform, url, expected, category="music_page_metadata"):
    text = _fetch(context, url)
    page = MusicPage()
    page.feed(text)
    title = page.meta.get("og:title") or page.meta.get("twitter:title") or page.title.strip()
    if re.search(r"captcha|access denied|just a moment|robot check|forbidden", title, re.I):
        raise ValueError("Site returned a block/challenge page")
    count = _schema_records(context, platform, page, url, maximum=40 if platform == "Amazon Music" else 60)
    if platform == "Qobuz":
        tracks = QobuzTracks()
        tracks.feed(text)
        for track in tracks.records[:max(0, 90 - count)]:
            context.record(platform, "album_track", track, url)
            count += 1
    if not count and title and re.search(expected, title, re.I):
        payload = {"title": title, "page_metadata": page.meta}
        if platform == "Jamendo":
            match = re.match(r"(.+?) - (.+?) \| Jamendo", title)
            if match:
                payload["artist_name"] = match.group(1)
                payload["track_name"] = match.group(2)
        if platform == "Anghami":
            # Keep the literal display counts. Do not convert rounded counts to exact numbers.
            visible = " ".join(page.text)
            for metric in ("Likes", "Plays", "Followers"):
                match = re.search(r"(\d[\d.,]*\s*[KMB]?)\s+" + metric + r"\b", visible, re.I)
                if match:
                    payload[metric.lower() + "_display"] = match.group(1)
        context.record(platform, category, payload, url)
        count = 1
    if platform == "TIDAL" and page.oembed:
        oembed_url = urljoin(url, page.oembed[0])
        if urlparse(oembed_url).hostname in {"tidal.com", "www.tidal.com", "embed.tidal.com"}:
            def oembed():
                payload = json.loads(_fetch(context, oembed_url))
                if not isinstance(payload, dict) or not (payload.get("title") or payload.get("html")):
                    raise ValueError("No oEmbed metadata")
                context.record(platform, "oembed_metadata", payload, oembed_url)
            context.attempt(platform, "oembed_metadata", oembed_url, oembed)
    if platform == "TIDAL":
        match = re.search(r"/album/(\d+)", url)
        if match:
            _tidal_embed(context, match.group(1))
    if not count:
        raise ValueError("No verified music entity metadata in public page")
    return page


def _tidal_embed(context, album_id):
    url = "https://embed.tidal.com/albums/" + album_id

    def tracks():
        text = _fetch(context, url)
        rows = TidalEmbedRows()
        rows.feed(text)
        if rows.records:
            for record in rows.records[:60]:
                context.record("TIDAL", "album_track", record, url)
            return
        page = MusicPage()
        page.feed(text)
        seen = set()
        count = 0
        for attrs, text in page.scripts:
            if attrs.get("type") != "application/json" and attrs.get("id") not in {"__NEXT_DATA__", "__NUXT_DATA__"}:
                continue
            try:
                payload = json.loads(text)
            except ValueError:
                continue
            for node in _walk(payload):
                if not (node.get("trackNumber") and node.get("title") and node.get("duration") is not None):
                    continue
                identity = str(node.get("id") or (node.get("volumeNumber"), node["trackNumber"], node["title"]))
                if identity in seen:
                    continue
                seen.add(identity)
                record = {key: node[key] for key in (
                    "id", "title", "duration", "trackNumber", "volumeNumber", "artist", "artists",
                    "album", "explicit", "url", "isrc", "audioQuality", "popularity",
                ) if key in node}
                context.record("TIDAL", "album_track", record, url)
                count += 1
                if count >= 60:
                    return
        if not count:
            raise ValueError("Public TIDAL embed contains no structured track metadata")

    context.attempt("TIDAL", "album_track", url, tracks)


def _amazon(context):
    homepage = "https://music.amazon.com/"
    state = {}

    def catalog():
        page = MusicPage()
        page.feed(_fetch(context, homepage))
        seen = set()
        for link, label in page.links:
            url = urljoin(homepage, link)
            if urlparse(url).hostname != "music.amazon.com":
                continue
            match = re.search(r"/(albums|artists|playlists)/([A-Za-z0-9]+)", urlparse(url).path)
            if not match or not label or url in seen:
                continue
            seen.add(url)
            context.record("Amazon Music", "catalog_link", {"display_name": label, "entity_type": match.group(1), "url": url}, homepage)
            if match.group(1) == "albums" and "album" not in state:
                state["album"] = (url, label)
            if len(seen) >= 50:
                break
        if not seen:
            raise ValueError("No public music catalog links found in homepage HTML")

    context.attempt("Amazon Music", "catalog_link", homepage, catalog)
    # This primary public album URL is independently discoverable on Amazon Music.
    # The homepage currently serves a JavaScript shell to ordinary HTTP clients.
    url, label = state.get("album", ("https://music.amazon.com/albums/B0013D6LEM", "Thriller"))
    context.attempt("Amazon Music", "album_page_metadata", url, lambda: _page_metadata(context, "Amazon Music", url, re.escape(label[:20]), "album_page_metadata"))


def _jamendo_catalog(context):
    # Official docs publish this ID specifically for testing their read API.
    # It is not a user credential; replace it with one's own client_id for production.
    params = {
        "client_id": "709fa152", "format": "json", "limit": "20",
        "include": "musicinfo+stats+licenses", "order": "popularity_total",
        "type": "both",
    }
    url = "https://api.jamendo.com/v3.0/tracks/?" + urlencode(params)

    def catalog():
        payload = json.loads(_fetch(context, url))
        if payload.get("headers", {}).get("status") != "success":
            raise ValueError("Jamendo API: " + str(payload.get("headers", {}).get("error_message", "unsuccessful response")))
        tracks = payload.get("results", [])
        if not tracks:
            raise ValueError("No tracks returned")
        for track in tracks[:20]:
            context.record("Jamendo", "track_catalog", track, url)

    context.attempt("Jamendo", "track_catalog", url, catalog)


def _archive(context):
    platform = "Internet Archive (music archive)"
    params = [
        ("q", "collection:etree AND (mediatype:audio OR mediatype:etree)"),
        ("rows", "20"), ("output", "json"), ("sort[]", "downloads desc"),
    ]
    for field in ("identifier", "title", "creator", "date", "subject", "downloads", "mediatype", "collection", "licenseurl"):
        params.append(("fl[]", field))
    url = "https://archive.org/advancedsearch.php?" + urlencode(params)
    state = {}

    def search():
        payload = json.loads(_fetch(context, url))
        records = payload.get("response", {}).get("docs", [])
        if not records:
            raise ValueError("No music archive search items returned")
        for record in records[:20]:
            context.record(platform, "music_archive_item", record, url)
        state["identifier"] = records[0].get("identifier")

    context.attempt(platform, "music_archive_item", url, search)
    if state.get("identifier"):
        detail_url = "https://archive.org/metadata/" + state["identifier"]

        def details():
            payload = json.loads(_fetch(context, detail_url))
            metadata = payload.get("metadata")
            if not metadata:
                raise ValueError("No archive item metadata")
            context.record(platform, "music_archive_details", metadata, detail_url)
            count = 0
            for file in payload.get("files", []):
                fmt = file.get("format", "").lower()
                name = file.get("name", "").lower()
                if any(part in fmt for part in ("flac", "mp3", "ogg vorbis", "wave")) or name.endswith((".mp3", ".flac", ".ogg", ".wav")):
                    context.record(platform, "audio_file_metadata", file, detail_url)
                    count += 1
                    if count >= 40:
                        break

        context.attempt(platform, "music_archive_details", detail_url, details)


def collect(context):
    probes = [
        ("TIDAL", "https://tidal.com/browse/album/134858516", r"after hours|weeknd"),
        ("Qobuz", "https://www.qobuz.com/gb-en/album/kind-of-blue-miles-davis/qe7yczkjg0zxc", r"kind of blue|miles davis"),
        ("Anghami", "https://play.anghami.com/song/36078771", r"habibi|tamino"),
        ("Jamendo", "https://www.jamendo.com/track/1281388/upbeat", r"upbeat|akashic records"),
    ]
    for platform, url, expected in probes:
        context.attempt(platform, "music_page_metadata", url, lambda p=platform, u=url, e=expected: _page_metadata(context, p, u, e))
    _jamendo_catalog(context)
    _amazon(context)
    _archive(context)
    # Current public Napster homepage is checked instead of assuming its old catalog API still works.
    url = "https://www.napster.com/"
    context.attempt("Napster", "music_catalog_probe", url, lambda: _page_metadata(context, "Napster", url, r"^$"))
