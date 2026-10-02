"""Spotify.

1) Playlist chính thức của Spotify cho Việt Nam, đọc qua trang embed công khai
   open.spotify.com/embed/playlist/<id> (JSON __NEXT_DATA__, không cần tài khoản):
     - Top 50 - Vietnam (BXH ngày), Top Songs - Vietnam (BXH tuần), Hot Hits Vietnam (editorial)
   Thêm playlist khác bằng biến SPOTIFY_EXTRA_PLAYLISTS="id1:Tên 1,id2:Tên 2".
2) Số lượt stream hằng ngày của BXH Spotify Việt Nam qua kworb.net (trang tổng hợp
   số liệu từ Spotify Charts) -> có metric streams để phân tích.
"""
import html
import json
import logging
import os
import re

from common import TOPIC_CHARTS, TOPIC_PLAYLISTS, chart_entry, playlist_item, track_fields

log = logging.getLogger("spotify")

CHART_PLAYLISTS = [
    ("37i9dQZEVXbLdGSmz6xilI", "spotify_top50_vn_daily"),
    ("37i9dQZEVXbKZyn1mKjmIl", "spotify_top_songs_vn_weekly"),
]
EDITORIAL_PLAYLISTS = [
    ("37i9dQZF1DX0F4i7Q9pshJ", "Hot Hits Vietnam"),
]
_NEXT = re.compile(r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>', re.S)


def _extra_playlists():
    out = []
    for part in os.getenv("SPOTIFY_EXTRA_PLAYLISTS", "").split(","):
        if ":" in part:
            pid, name = part.split(":", 1)
            out.append((pid.strip(), name.strip()))
    return out


def fetch_embed_playlist(session, playlist_id):
    r = session.get(f"https://open.spotify.com/embed/playlist/{playlist_id}", timeout=25)
    r.raise_for_status()
    m = _NEXT.search(r.text)
    if not m:
        raise RuntimeError(f"spotify embed {playlist_id}: no __NEXT_DATA__")
    entity = json.loads(m.group(1))["props"]["pageProps"]["state"]["data"]["entity"]
    return entity.get("name"), entity.get("trackList", [])


def _track(t):
    tid = (t.get("uri") or "").split(":")[-1] or None
    artists = [a.strip() for a in re.split(r",\s", (t.get("subtitle") or "").replace("\xa0", " ")) if a.strip()]
    return track_fields(
        title=t.get("title"), artists=artists, source="spotify", platform_id=tid,
        url=f"https://open.spotify.com/track/{tid}" if tid else None,
        duration_s=(t.get("duration") or 0) / 1000 or None,
    )


def crawl_charts(session, crawled_ms):
    for pid, chart_id in CHART_PLAYLISTS:
        try:
            name, tracks = fetch_embed_playlist(session, pid)
        except Exception as e:  # noqa: BLE001
            log.warning("spotify chart %s failed: %s", chart_id, e)
            continue
        for rank, t in enumerate(tracks, start=1):
            tr = _track(t)
            yield TOPIC_CHARTS, tr["track_key"], chart_entry(chart_id, crawled_ms, rank, len(tracks), tr)
        log.info("spotify %s (%s): %d tracks", chart_id, name, len(tracks))
    yield from crawl_kworb_daily(session, crawled_ms)


def crawl_playlists(session, crawled_ms):
    for pid, label in EDITORIAL_PLAYLISTS + _extra_playlists():
        try:
            name, tracks = fetch_embed_playlist(session, pid)
        except Exception as e:  # noqa: BLE001
            log.warning("spotify playlist %s failed: %s", pid, e)
            continue
        for pos, t in enumerate(tracks, start=1):
            tr = _track(t)
            yield TOPIC_PLAYLISTS, tr["track_key"], playlist_item(f"spotify:{pid}", name or label, None, crawled_ms, pos, tr)
        log.info("spotify playlist %s: %d tracks", name or label, len(tracks))


# ------------------------------------------------------------------ kworb (streams)
_ROW = re.compile(r"<tr>(.*?)</tr>", re.S)
_TD = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
_ARTIST = re.compile(r'href="\.\./artist/[^"]+">([^<]+)</a>')
_TRACK = re.compile(r'href="\.\./track/([A-Za-z0-9]+)\.html">([^<]+)</a>')


def _int(s):
    s = re.sub(r"[^\d\-]", "", s or "")
    return int(s) if s not in ("", "-") else None


def crawl_kworb_daily(session, crawled_ms, country=os.getenv("KWORB_COUNTRY", "vn")):
    try:
        r = session.get(f"https://kworb.net/spotify/country/{country}_daily.html", timeout=25)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        log.warning("kworb failed: %s", e)
        return
    r.encoding = "utf-8"
    body = r.text[r.text.find("<tbody"):]
    rows = []
    for row in _ROW.findall(body):
        tds = _TD.findall(row)
        m = _TRACK.search(row)
        if len(tds) < 7 or not m:
            continue
        rank = _int(tds[0])
        move = tds[1].strip()
        prev = rank - int(move) if re.fullmatch(r"[+-]\d+", move) else (rank if move == "=" else None)
        rows.append(dict(rank=rank, prev=prev, track_id=m.group(1), title=html.unescape(m.group(2)),
                         artists=[html.unescape(a) for a in _ARTIST.findall(row)], streams=_int(tds[6]),
                         total=_int(tds[10]) if len(tds) > 10 else None))
    for x in rows:
        tr = track_fields(x["title"], x["artists"], "spotify", platform_id=x["track_id"],
                          url=f"https://open.spotify.com/track/{x['track_id']}")
        yield TOPIC_CHARTS, tr["track_key"], chart_entry(
            f"spotify_daily_streams_{country}", crawled_ms, x["rank"], len(rows), tr, previous_rank=x["prev"],
            metric_name="streams", metric_value=x["streams"], total_plays=x["total"])
    log.info("kworb spotify daily %s: %d rows", country, len(rows))
