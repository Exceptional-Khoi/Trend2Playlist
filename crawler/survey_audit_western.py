"""Public western source audit: sales feed, counters, comments and releases.

These are snapshots. Counter changes do not measure source publication latency.
No audio, account credentials or buyer identity is requested or retained.
"""

import hashlib
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urljoin

try:
    from .survey_western import _Page, _embedded_json, _walk, _get, _json, _clean
except ImportError:
    from survey_western import _Page, _embedded_json, _walk, _get, _json, _clean


BANDCAMP_PAGE = "https://bandcamp.com/artists"
BANDCAMP_INITIAL = "https://bandcamp.com/api/salesfeed/1/get_initial"
SOUNDCLOUD_PAGE = "https://soundcloud.com/forss/flickermood"
AUDIOMACK_PAGE = "https://audiomack.com/mr-eazi/song/comfort"
QOBUZ_RSS = "https://www.qobuz.com/us-en/rss/new-releases/download-streaming-albums"


def _iso_epoch(value):
    if not isinstance(value, (int, float)):
        return None
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


class _BandcampInitial(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.feed_data = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if attrs.get("data-initial-feed"):
            self.feed_data = json.loads(attrs["data-initial-feed"])


def parse_bandcamp_feed(value, limit=100):
    """Return one record per public sold item, including explicit product type."""
    feed = value.get("feed_data", value)
    if not isinstance(feed, dict) or not isinstance(feed.get("events"), list):
        raise ValueError("No public sale events in response")
    records = []
    keys = ("artist_name", "item_type", "item_description", "album_title", "slug_type",
            "currency", "amount_paid", "item_price", "amount_paid_usd", "country",
            "country_code", "amount_paid_fmt", "amount_over_fmt", "releases", "art_url")
    for event in feed["events"]:
        if event.get("event_type") != "sale":
            continue
        for item in event.get("items", []):
            if not item.get("item_description") or not item.get("url"):
                continue
            payload = {key: item[key] for key in keys if item.get(key) is not None}
            payload.update({
                "url": urljoin(BANDCAMP_PAGE, item["url"]),
                "event_timestamp_seconds": item.get("utc_date", event.get("utc_date")),
                "event_utc": _iso_epoch(item.get("utc_date", event.get("utc_date"))),
                "feed_server_time_seconds": feed.get("server_time"),
                "feed_start_cursor": feed.get("start_date"),
                "feed_end_cursor": feed.get("end_date"),
                # The public client waits before displaying events. This is
                # not a guaranteed delay of the API's raw event publication.
                "public_ui_display_buffer_seconds": feed.get("data_delay_sec"),
                "signal_type": "sale",
                "collection_method": "public sales feed snapshot",
            })
            # The client declares t=track, a=album, b=discography. The p type
            # can be physical music or unrelated merchandise; do not guess.
            payload["product_type"] = {"t": "track", "a": "album", "b": "discography",
                                       "p": "physical_product"}.get(item.get("item_type"), "unknown")
            fingerprint = json.dumps({key: payload.get(key) for key in
                                      ("event_timestamp_seconds", "url", "artist_name",
                                       "item_type", "currency", "amount_paid")},
                                     sort_keys=True, ensure_ascii=False)
            payload["sale_fingerprint"] = hashlib.sha256(fingerprint.encode()).hexdigest()
            records.append(payload)
            if limit is not None and len(records) >= limit:
                return records
    if not records:
        raise ValueError("Public sales feed is empty")
    return records


def parse_bandcamp_html(source, limit=100):
    page = _BandcampInitial()
    page.feed(source)
    if page.feed_data is None:
        raise ValueError("No data-initial-feed; page may be a client challenge")
    return parse_bandcamp_feed(page.feed_data, limit=limit)


class _SoundcloudComments(HTMLParser):
    """Read the public SEO comments section; omit author profile identity."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.in_comments = False
        self.active_tag = None
        self.buffer = []
        self.current = None
        self.records = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "section" and "comments" in attrs.get("class", "").split():
            self.in_comments = True
        if not self.in_comments:
            return
        if tag == "h2":
            self.current = {}
        elif tag == "a" and self.current is not None:
            author = attrs.get("href", "")
            if author:
                self.current["author_hash"] = hashlib.sha256(author.encode()).hexdigest()[:20]
        elif tag in ("p", "time"):
            self.active_tag = tag
            self.buffer = []

    def handle_data(self, data):
        if self.in_comments and self.active_tag:
            self.buffer.append(data)

    def handle_endtag(self, tag):
        if tag == "section" and self.in_comments:
            self.in_comments = False
        if self.current is not None and tag == self.active_tag:
            text = _clean("".join(self.buffer))
            if tag == "p":
                self.current["text"] = text
            elif tag == "time":
                self.current["published_utc"] = text
                if self.current.get("text") and text:
                    fingerprint = "|".join((SOUNDCLOUD_PAGE, text, self.current["text"],
                                             self.current.get("author_hash", "")))
                    self.current["comment_fingerprint"] = hashlib.sha256(fingerprint.encode()).hexdigest()
                    self.records.append(self.current)
            self.active_tag = None
            self.buffer = []


def parse_soundcloud_html(source):
    page = _Page()
    page.feed(source)
    tracks = []
    seen = set()
    for document in _embedded_json(page):
        for item in _walk(document):
            if item.get("kind") != "track" or not item.get("title") or item.get("id") in seen:
                continue
            seen.add(item.get("id"))
            keys = ("id", "title", "playback_count", "likes_count", "reposts_count", "comment_count")
            payload = {key: item[key] for key in keys if key in item}
            payload.update({"url": item.get("permalink_url", SOUNDCLOUD_PAGE),
                            "signal_type": "cumulative_public_counter",
                            "collection_method": "public HTML hydration snapshot",
                            "source_counter_latency": "not documented for this HTML snapshot"})
            tracks.append(payload)
    parser = _SoundcloudComments()
    parser.feed(source)
    comments = [{**item, "track_url": SOUNDCLOUD_PAGE,
                 "collection_method": "public HTML comments snapshot"} for item in parser.records[:50]]
    if not tracks:
        raise ValueError("No track counters in public page")
    return {"counters": tracks[:20], "comments": comments}


def parse_audiomack_html(source):
    page = _Page()
    page.feed(source)
    records = []
    seen = set()
    for document in _embedded_json(page):
        for item in _walk(document):
            stats = item.get("stats")
            if (item.get("type") != "song" or not item.get("title")
                    or not isinstance(stats, dict) or "plays-raw" not in stats
                    or item.get("id") in seen):
                continue
            # Related songs on a song page are catalog suggestions, not this
            # endpoint's target counter. Keep only the actual requested song.
            if (item.get("links") or {}).get("self") != AUDIOMACK_PAGE:
                continue
            seen.add(item.get("id"))
            selected_stats = {key: stats[key] for key in ("plays-raw", "favorites-raw", "reposts-raw",
                                                         "playlists-raw", "comments", "cache-hit")
                              if key in stats}
            records.append({"id": item.get("id"), "title": item["title"], "artist": item.get("artist"),
                            "url": AUDIOMACK_PAGE, "stats": selected_stats,
                            "signal_type": "cumulative_public_counter",
                            "collection_method": "public HTML hydration snapshot",
                            "source_counter_latency": "not documented for this HTML snapshot"})
    if not records:
        raise ValueError("No requested Audiomack song counter found")
    return records


def parse_qobuz_rss(source):
    root = ET.fromstring(source)
    records = []
    for item in root.findall(".//item")[:50]:
        def value(name):
            node = item.find(name)
            return node.text.strip() if node is not None and node.text else None
        link = value("link")
        records.append({"title": value("title"), "url": urljoin(QOBUZ_RSS, link) if link else None,
                        "published": value("pubDate"), "genre": value("category"),
                        "guid": value("guid"), "signal_type": "catalog_release",
                        "collection_method": "public new-release RSS snapshot",
                        "source_refresh_cadence": "not declared in feed"})
    if not records:
        raise ValueError("No Qobuz RSS releases")
    return records


def collect(context):
    def bandcamp_initial():
        for payload in parse_bandcamp_feed(_json(context, BANDCAMP_INITIAL)):
            category = ("public_sale_music" if payload["item_type"] in ("t", "a", "b")
                        else "public_sale_physical_product")
            context.record("bandcamp", category, payload, BANDCAMP_INITIAL)
    context.attempt("bandcamp", "public_sales_api_initial", BANDCAMP_INITIAL, bandcamp_initial)

    def soundcloud():
        parsed = parse_soundcloud_html(_get(context, SOUNDCLOUD_PAGE))
        for payload in parsed["counters"]:
            context.record("soundcloud", "public_counter_snapshot", payload, SOUNDCLOUD_PAGE)
        for payload in parsed["comments"]:
            context.record("soundcloud", "public_comment", payload, SOUNDCLOUD_PAGE)
    context.attempt("soundcloud", "public_html_counter_comments", SOUNDCLOUD_PAGE, soundcloud)

    def audiomack():
        for payload in parse_audiomack_html(_get(context, AUDIOMACK_PAGE)):
            context.record("audiomack", "public_counter_snapshot", payload, AUDIOMACK_PAGE)
    context.attempt("audiomack", "public_html_counter", AUDIOMACK_PAGE, audiomack)

    def qobuz():
        for payload in parse_qobuz_rss(_get(context, QOBUZ_RSS)):
            context.record("Qobuz", "new_release_feed", payload, QOBUZ_RSS)
    context.attempt("Qobuz", "new_release_feed", QOBUZ_RSS, qobuz)

    for entity, entity_id in (("album", 302127), ("artist", 27)):
        url = "https://api.deezer.com/%s/%s/comments?limit=25" % (entity, entity_id)
        def deezer(u=url, kind=entity, identity=entity_id):
            result = _json(context, u)
            rows = result.get("data", [])
            if not rows:
                raise ValueError("No public Deezer %s comments" % kind)
            for row in rows[:25]:
                keys = ("id", "text", "date")
                payload = {key: row[key] for key in keys if key in row}
                payload.update({"entity_type": kind, "entity_id": identity,
                                "collection_method": "public comments API snapshot"})
                context.record("deezer", "public_comment", payload, u)
        context.attempt("deezer", entity + "_comments", url, deezer)
