"""Apple Music - BXH 'Most Played' Việt Nam qua Apple Marketing Tools RSS (JSON công khai)."""
import logging
import os

from common import TOPIC_CHARTS, chart_entry, track_fields

log = logging.getLogger("apple")
FEED = "https://rss.applemarketingtools.com/api/v2/{country}/music/most-played/{limit}/songs.json"


def crawl_charts(session, crawled_ms):
    yield from market_chart(session, crawled_ms, os.getenv("APPLE_COUNTRY", "vn"))


def market_chart(session, crawled_ms, country):
    r = session.get(FEED.format(country=country, limit=100), timeout=25)
    r.raise_for_status()
    feed = r.json()["feed"]
    if feed.get("country") and feed["country"].lower() != country.lower():
        raise ValueError("Apple Music returned a different market")
    results = feed["results"]
    for rank, x in enumerate(results, start=1):
        genres = [g.get("name") for g in x.get("genres", []) if g.get("name") and g.get("name") != "Music"]
        tr = track_fields(
            title=x.get("name"), artists=x.get("artistName"), source="apple_music", platform_id=x.get("id"),
            url=x.get("url"), thumbnail=(x.get("artworkUrl100") or "").replace("100x100", "300x300") or None,
            genres=genres, release_date=x.get("releaseDate"),
        )
        yield TOPIC_CHARTS, tr["track_key"], chart_entry(f"apple_most_played_{country}", crawled_ms, rank, len(results), tr, scope=country.upper())
    log.info("apple most-played %s: %d", country, len(results))


def crawl_countries(session, crawled_ms):
    from common import crawl_markets
    yield from crawl_markets(session, crawled_ms, market_chart)
