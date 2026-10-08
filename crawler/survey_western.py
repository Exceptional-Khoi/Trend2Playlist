"""Bounded public metadata probes for full-song music platforms.

The shared runner supplies get, record and attempt. No audio is downloaded,
private API credentials are extracted, or authentication is attempted.
"""

import html
import json
import re
from html.parser import HTMLParser
from urllib.parse import urlencode, urljoin


class _Page(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.meta = {}
        self.scripts = []
        self.attributes = []
        self.anchors = []
        self._script = None
        self._anchor = None
        self._heading = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        for key in ("data-tralbum", "data-band", "data-embed"):
            if attrs.get(key):
                self.attributes.append((key, attrs[key]))
        if tag == "meta":
            name = attrs.get("property") or attrs.get("name")
            if name and attrs.get("content"):
                self.meta[name] = attrs["content"]
        elif tag == "script":
            self._script = [attrs, []]
        elif tag == "a":
            self._anchor = {"href": attrs.get("href", ""), "text": [], "headings": []}
        elif tag in ("h1", "h2", "h3", "h4") and self._anchor:
            self._heading = []
        elif tag == "img" and self._anchor:
            self._anchor["image_alt"] = attrs.get("alt", "")
            self._anchor["image_url"] = attrs.get("src", "")

    def handle_endtag(self, tag):
        if tag == "script" and self._script:
            self.scripts.append((self._script[0], "".join(self._script[1])))
            self._script = None
        elif tag in ("h1", "h2", "h3", "h4") and self._heading is not None:
            if self._anchor:
                self._anchor["headings"].append(_clean(" ".join(self._heading)))
            self._heading = None
        elif tag == "a" and self._anchor:
            self._anchor["text"] = _clean(" ".join(self._anchor["text"]))
            self.anchors.append(self._anchor)
            self._anchor = None

    def handle_data(self, data):
        if self._script:
            self._script[1].append(data)
        if self._anchor:
            self._anchor["text"].append(data)
        if self._heading is not None:
            self._heading.append(data)


def _clean(value):
    return re.sub(r"\s+", " ", html.unescape(str(value or ""))).strip()


def _get(context, url):
    status, _headers, body = context.get(url)
    if not 200 <= status < 300:
        raise ValueError("HTTP %s" % status)
    if not body:
        raise ValueError("Empty response")
    return body.decode("utf-8", errors="replace")


def _json(context, url):
    result = json.loads(_get(context, url))
    if isinstance(result, dict) and result.get("error"):
        raise ValueError("API error: %s" % result["error"])
    return result


def _page(context, url):
    page = _Page()
    page.feed(_get(context, url))
    return page


def _walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _embedded_json(page):
    flight_chunks = []
    decoder = json.JSONDecoder()
    for attrs, text in page.scripts:
        if "json" in attrs.get("type", "") or attrs.get("id") == "__NEXT_DATA__":
            try:
                yield json.loads(text)
            except (TypeError, ValueError):
                continue
        elif "window.__sc_hydration" in text:
            match = re.search(r"window\.__sc_hydration\s*=\s*(\[.*?\]);", text, re.S)
            if match:
                try:
                    yield json.loads(match.group(1))
                except ValueError:
                    continue
        elif "self.__next_f.push(" in text:
            # Next.js serves public initial props as JSON string chunks inside
            # React Flight script calls. Decode JSON only; never execute script.
            for match in re.finditer(r"self\.__next_f\.push\(", text):
                try:
                    chunk, _end = decoder.raw_decode(text, match.end())
                    if len(chunk) > 1 and isinstance(chunk[1], str):
                        flight_chunks.append(chunk[1])
                except (TypeError, ValueError):
                    continue
    # Flight includes length-prefixed text records which may contain newlines,
    # so a line-based parser would lose the music props following those records.
    # Scan for valid JSON containers and consume each whole container once.
    flight = "".join(flight_chunks)
    cursor = 0
    while cursor < len(flight):
        match = re.search(r"[\[{]", flight[cursor:])
        if not match:
            break
        start = cursor + match.start()
        try:
            document, cursor = decoder.raw_decode(flight, start)
            yield document
        except ValueError:
            cursor = start + 1


def _without_audio(value):
    """Keep metadata while omitting direct media delivery fields."""
    excluded = {"file", "files", "media", "stream_url", "streaming_url", "audio_url",
                "download_url", "download_urls", "streaming_urls", "transcodings"}
    if isinstance(value, dict):
        return {key: _without_audio(child) for key, child in value.items()
                if key not in excluded}
    if isinstance(value, list):
        return [_without_audio(child) for child in value]
    return value


def _deezer(context):
    url = "https://api.deezer.com/chart/0?limit=15"

    def charts():
        result = _json(context, url)
        count = 0
        for group in ("tracks", "albums", "artists", "playlists"):
            for position, item in enumerate(result.get(group, {}).get("data", [])[:15], 1):
                context.record("deezer", "chart_" + group.rstrip("s"),
                               {"chart_position": position, "chart": "all_genres", "chart_id": 0,
                                **_without_audio(item)}, url)
                count += 1
        if not count:
            raise ValueError("No chart entities in Deezer response")

    context.attempt("deezer", "charts", url, charts)
    search_url = "https://api.deezer.com/search?" + urlencode({"q": "Son Tung M-TP", "limit": 25})

    def search():
        result = _json(context, search_url)
        items = result.get("data", [])
        if not items:
            raise ValueError("No catalog search results")
        for item in items[:25]:
            context.record("deezer", "catalog_track", _without_audio(item), search_url)

    context.attempt("deezer", "catalog_search", search_url, search)


def _soundcloud(context):
    track_url = "https://soundcloud.com/forss/flickermood"
    embed_url = "https://soundcloud.com/oembed?" + urlencode({"format": "json", "url": track_url})

    def embed():
        result = _json(context, embed_url)
        if not result.get("title"):
            raise ValueError("oEmbed response has no title")
        context.record("soundcloud", "track_embed_metadata", result, embed_url)

    context.attempt("soundcloud", "track_embed_metadata", embed_url, embed)

    def track():
        page = _page(context, track_url)
        count = 0
        seen = set()
        for document in _embedded_json(page):
            for item in _walk(document):
                if item.get("kind") != "track" or not item.get("title"):
                    continue
                identity = str(item.get("id") or item.get("permalink_url") or item["title"])
                if identity in seen:
                    continue
                seen.add(identity)
                keys = ("id", "title", "description", "duration", "full_duration", "genre",
                        "tag_list", "created_at", "release_date", "release_year", "permalink_url",
                        "artwork_url", "playback_count", "likes_count", "reposts_count",
                        "comment_count", "license", "label_name", "publisher_metadata")
                payload = {key: item[key] for key in keys if key in item}
                user = item.get("user") or {}
                if user:
                    payload["artist"] = {key: user[key] for key in
                                         ("id", "username", "permalink_url", "verified") if key in user}
                context.record("soundcloud", "track_metadata", payload, track_url)
                count += 1
                if count >= 20:
                    break
            if count >= 20:
                break
        if not count and page.meta.get("og:title"):
            context.record("soundcloud", "track_page_metadata", page.meta, track_url)
            count += 1
        if not count:
            raise ValueError("No public track metadata in page")

    context.attempt("soundcloud", "track_metadata", track_url, track)
    playlist_url = "https://soundcloud.com/oembed?" + urlencode(
        {"format": "json", "url": "https://soundcloud.com/forss/sets/soulhack"})

    def playlist():
        result = _json(context, playlist_url)
        if not result.get("title"):
            raise ValueError("Playlist oEmbed has no title")
        context.record("soundcloud", "playlist_embed_metadata", result, playlist_url)

    context.attempt("soundcloud", "playlist_embed_metadata", playlist_url, playlist)


def _bandcamp(context):
    url = "https://c418.bandcamp.com/album/minecraft-volume-alpha"

    def album():
        page = _page(context, url)
        album_data = None
        for key, value in page.attributes:
            if key == "data-tralbum":
                try:
                    album_data = json.loads(value)
                    break
                except ValueError:
                    continue
        count = 0
        if album_data:
            current = album_data.get("current") or {}
            payload = {"artist": album_data.get("artist"), "url": url,
                       "album_release_date": album_data.get("album_release_date"),
                       "track_count": len(album_data.get("trackinfo", [])),
                       "metadata": _without_audio(current), "page_metadata": page.meta}
            context.record("bandcamp", "album_metadata", payload, url)
            count += 1
            for item in album_data.get("trackinfo", [])[:80]:
                track = _without_audio(item)
                track["artist"] = album_data.get("artist")
                track["album_title"] = current.get("title")
                if track.get("title_link"):
                    track["url"] = urljoin(url, track["title_link"])
                context.record("bandcamp", "album_track", track, url)
                count += 1
        else:
            for document in _embedded_json(page):
                for item in _walk(document):
                    entity_type = item.get("@type", "")
                    if entity_type in ("MusicAlbum", "MusicRecording"):
                        context.record("bandcamp", "album_metadata" if entity_type == "MusicAlbum"
                                       else "album_track", _without_audio(item), url)
                        count += 1
                        if count >= 80:
                            break
        if not count:
            raise ValueError("No Bandcamp album/track metadata found")

    context.attempt("bandcamp", "album_and_tracks", url, album)


def _audiomack(context):
    url = "https://audiomack.com/trending-now/songs"

    def trending():
        page = _page(context, url)
        seen = set()
        count = 0
        for document in _embedded_json(page):
            for item in _walk(document):
                kind = item.get("type") or item.get("@type") or item.get("kind")
                title = item.get("title") or item.get("name")
                is_track = kind in ("song", "Song", "MusicRecording", "track")
                is_track = is_track or (title and "song_id" in item)
                if not is_track or not title:
                    continue
                identity = str(item.get("id") or item.get("song_id") or title)
                if identity in seen:
                    continue
                seen.add(identity)
                keys = ("id", "title", "artist", "duration", "genre", "explicit", "description",
                        "producer", "featuring", "album", "upc", "isrc", "uploaded", "released",
                        "original_release_date", "url_slug", "image_base", "image", "images",
                        "stats", "links", "usertags", "keywords", "geo_restricted")
                payload = {key: item[key] for key in keys if key in item}
                uploader = item.get("uploader") or {}
                if uploader:
                    payload["uploader"] = {key: uploader[key] for key in
                                           ("id", "name", "url_slug", "verified", "followers_count")
                                           if key in uploader}
                payload["source_position"] = count + 1
                payload["source"] = "trending-now/songs"
                context.record("audiomack", "trending_track", payload, url)
                count += 1
                if count >= 60:
                    return
        if count:
            return
        # Artist and title are separate anchors sharing a song URL in the SSR
        # markup. Group them before deduplicating, rather than keeping the first.
        groups = {}
        for anchor in page.anchors:
            href = urljoin(url, anchor["href"])
            if "/song/" in href:
                groups.setdefault(href, []).append(anchor)
        for href, anchors in groups.items():
            if href in seen:
                continue
            texts = [anchor["text"] for anchor in anchors if anchor["text"]]
            headings = [value for anchor in anchors for value in anchor["headings"] if value]
            title = headings[-1] if headings else (texts[-1] if texts else "")
            artist = headings[0] if len(headings) > 1 else (texts[0] if len(texts) > 1 else None)
            alt = next((anchor.get("image_alt", "") for anchor in anchors if anchor.get("image_alt")), "")
            if " by " in alt:
                alt_title, alt_artist = alt.rsplit(" by ", 1)
                title = title or alt_title
                artist = artist or alt_artist
            if not title:
                continue
            seen.add(href)
            context.record("audiomack", "trending_track", {
                "title": title, "artist": artist, "url": href,
                "source_position": count + 1,
                "image_url": next((anchor.get("image_url") for anchor in anchors
                                   if anchor.get("image_url")), None), "source": "trending-now/songs",
            }, url)
            count += 1
            if count >= 60:
                break
        if not count:
            raise ValueError("No public Audiomack song entities or links found")

    context.attempt("audiomack", "trending_tracks", url, trending)


def collect(context):
    """Run the probes; successful records and failures go to the shared runner."""
    _deezer(context)
    _soundcloud(context)
    _bandcamp(context)
    _audiomack(context)
