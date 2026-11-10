"""Public country charts: iTunes purchases and YouTube charts published by Kworb.

These are different chart populations, not interchangeable global listening counts.
"""
import html
import os
import re
from datetime import datetime, timezone
from urllib.parse import quote, urlsplit

from common import TOPIC_CHARTS, chart_entry, crawl_markets, track_fields

MARKETS = 'vn,th,id,us,jp,kr,sg,my,ph,tw,hk,in,au,nz,gb,de,fr,it,es,nl,se,no,fi,dk,pl,ca,mx,br,ar,cl,co,pe,za,tr,at,be,ch,pt,ie,gr,cz,hu,ro,bg,ua,eg,sa,ae,il,ng,ke,ma,pk,lk,kh,kz,rs,hr,sk,lt,lv,ee,is,lu,mt,cy,do,ec,cr,pa,uy,gt,hn,sv,bo,py'


def chart_limit():
    return max(1, min(100, int(os.getenv('COUNTRY_CHART_LIMIT', '20'))))


def market_itunes(session, crawled_ms, country):
    limit = chart_limit()
    url = f'https://itunes.apple.com/{country}/rss/topsongs/limit={limit}/json'
    response = session.get(url, timeout=(5, 10));response.raise_for_status()
    final = urlsplit(getattr(response, 'url', '')).path.strip('/').split('/')[0]
    if re.fullmatch('[a-z]{2}', final) and final != country:
        raise ValueError('iTunes redirected to a different storefront')
    feed = response.json()['feed'];entries = feed.get('entry', [])
    if isinstance(entries, dict):entries = [entries]
    if not isinstance(entries, list) or not entries:raise ValueError('No iTunes chart for this storefront')
    period = None
    updated = (feed.get('updated') or {}).get('label')
    if updated:
        period = datetime.fromisoformat(updated.replace('Z', '+00:00')).date().isoformat()
    seen = set();records = []
    for entry in entries[:limit]:
        title = entry.get('im:name', {}).get('label');artist = entry.get('im:artist', {}).get('label')
        identifier = entry.get('id', {}).get('attributes', {}).get('im:id')
        if not title or not artist or not identifier or identifier in seen:
            raise ValueError('Malformed or duplicate iTunes song')
        seen.add(identifier)
        pictures = entry.get('im:image') or []
        track = track_fields(title, artist, 'itunes', platform_id=identifier,
                             url=entry.get('id', {}).get('label'),
                             thumbnail=pictures[-1].get('label') if pictures else None)
        records.append(track)
    for rank, track in enumerate(records, 1):
        record = chart_entry(f'itunes_top_songs_{country}_top{limit}', crawled_ms, rank, len(records), track, scope=country.upper())
        record.update(data_provider='itunes_rss', country_code=country.upper(), period_type='daily' if period else 'realtime',
                      source_period=period, source_url=url, chart_family=f'itunes_top_songs_top{limit}',
                      metric_description='iTunes Store song chart; not a stream count')
        yield TOPIC_CHARTS, track['track_key'], record


def market_youtube(session, crawled_ms, country):
    url = f'https://kworb.net/youtube/insights/{country}_daily.html'
    response = session.get(url, timeout=(5, 10));response.raise_for_status()
    # Kworb publishes UTF-8 HTML; Requests otherwise defaults text/html to
    # Latin-1 when the response header omits its charset.
    response.encoding = 'utf-8'
    page = response.text
    heading = re.search(r'YouTube[^<]*Daily Chart\s*-\s*(20\d\d/\d\d/\d\d)', page)
    if not heading:raise ValueError('YouTube country chart heading changed')
    period = datetime.strptime(heading.group(1), '%Y/%m/%d').date().isoformat()
    tbody = re.search(r'<tbody[^>]*>(.*?)</tbody>', page, re.S)
    if not tbody:raise ValueError('YouTube country chart table missing')
    rows = []
    for tr in re.findall(r'<tr[^>]*>(.*?)</tr>', tbody.group(1), re.S):
        cells = [html.unescape(re.sub('<[^>]+>', '', td)).strip() for td in re.findall(r'<td[^>]*>(.*?)</td>', tr, re.S)]
        if len(cells) < 4 or not cells[0].isdigit():continue
        artist, sep, title = cells[2].partition(' - ')
        if not sep or not title or not artist:raise ValueError('YouTube chart title/artist missing')
        rows.append((int(cells[0]), title, artist, int(cells[3].replace(',', ''))))
    rows = rows[:chart_limit()]
    if not rows or [r[0] for r in rows] != list(range(1, len(rows)+1)):
        raise ValueError('YouTube chart ranks incomplete')
    for rank, title, artist, streams in rows:
        if streams < 0:raise ValueError('Negative YouTube chart metric')
        track = track_fields(title, artist, 'youtube', url='https://www.youtube.com/results?search_query='+quote(artist+' '+title))
        record = chart_entry(f'youtube_daily_{country}_top{chart_limit()}', crawled_ms, rank, len(rows), track,
                             scope=country.upper(), metric_name='chart_streams', metric_value=streams)
        record.update(data_provider='youtube_kworb', country_code=country.upper(), source_period=period,
                      period_type='daily', source_url=url, chart_family=f'youtube_daily_top{chart_limit()}',
                      id_kind='normalized_title_artist', metric_description='YouTube country chart as published by Kworb')
        yield TOPIC_CHARTS, track['track_key'], record


def crawl_itunes(session, crawled_ms):
    yield from crawl_markets(session, crawled_ms, market_itunes)


def crawl_youtube(session, crawled_ms):
    yield from crawl_markets(session, crawled_ms, market_youtube)
