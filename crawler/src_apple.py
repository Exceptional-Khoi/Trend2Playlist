"""Apple Music - BXH 'Most Played' Việt Nam qua Apple Marketing Tools RSS (JSON công khai)."""
import logging
import os

from common import TOPIC_CHARTS, chart_entry, track_fields

log = logging.getLogger("apple")
FEED = "https://rss.applemarketingtools.com/api/v2/{country}/music/most-played/{limit}/songs.json"


def crawl_charts(session, crawled_ms):
    country = os.getenv("APPLE_COUNTRY", "vn")
    r = session.get(FEED.format(country=country, limit=100), timeout=25)
    r.raise_for_status()
    results = r.json()["feed"]["results"]
    for rank, x in enumerate(results, start=1):
        genres = [g.get("name") for g in x.get("genres", []) if g.get("name") and g.get("name") != "Music"]
        tr = track_fields(
            title=x.get("name"), artists=x.get("artistName"), source="apple_music", platform_id=x.get("id"),
            url=x.get("url"), thumbnail=(x.get("artworkUrl100") or "").replace("100x100", "300x300") or None,
            genres=genres, release_date=x.get("releaseDate"),
        )
        yield TOPIC_CHARTS, tr["track_key"], chart_entry(f"apple_most_played_{country}", crawled_ms, rank, len(results), tr)
    log.info("apple most-played %s: %d", country, len(results))
