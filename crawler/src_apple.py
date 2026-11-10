"""Apple Music - BXH 'Most Played' Việt Nam qua Apple Marketing Tools RSS (JSON công khai)."""
import logging
import os
from datetime import datetime
from email.utils import parsedate_to_datetime

from common import TOPIC_CHARTS, chart_entry, track_fields

log = logging.getLogger("apple")
FEED = "https://rss.marketingtools.apple.com/api/v2/{country}/music/most-played/{limit}/songs.json"


def crawl_charts(session, crawled_ms):
    yield from market_chart(session, crawled_ms, os.getenv("APPLE_COUNTRY", "vn"))


def market_chart(session, crawled_ms, country):
    limit = max(1, min(100, int(os.getenv('COUNTRY_CHART_LIMIT', '20'))))
    r = session.get(FEED.format(country=country, limit=limit), timeout=(5, 10))
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
        record = chart_entry(f"apple_most_played_{country}", crawled_ms, rank, len(results), tr, scope=country.upper())
        updated = feed.get('updated')
        period = None
        if updated:
            try:
                period = datetime.fromisoformat(updated.replace('Z', '+00:00')).date().isoformat()
            except ValueError:
                period = parsedate_to_datetime(updated).date().isoformat()
        record.update(data_provider='apple_music_rss', country_code=country.upper(), period_type='realtime',
                      source_period=period,
                      source_url=FEED.format(country=country, limit=limit))
        yield TOPIC_CHARTS, tr["track_key"], record
    log.info("apple most-played %s: %d", country, len(results))


def crawl_countries(session, crawled_ms):
    from common import crawl_markets
    yield from crawl_markets(session, crawled_ms, market_chart)
