"""YouTube / YouTube Music.

1) BXH YouTube Charts Việt Nam (charts.youtube.com) - top bài hát / video / nghệ sĩ theo tuần,
   kèm lượt xem. Gọi endpoint youtubei mà chính trang charts.youtube.com dùng, không cần key.
2) Bình luận của các video đang thịnh hành -> tín hiệu THEO TỈNH (người Việt hay bình luận
   "ai ở Nghệ An điểm danh", "Sài Gòn mưa nghe bài này"...). Spark sẽ nhận diện tên tỉnh.
     - Có YOUTUBE_API_KEY  -> YouTube Data API v3 (commentThreads.list, 1 quota/lần gọi)
     - Không có key         -> thư viện youtube-comment-downloader (đọc như trình duyệt)
   Không lưu tên/ID người bình luận: chỉ lưu author_hash = SHA-256(muối bí mật + ID kênh), dùng để
   đếm mỗi người 1 lần cho mỗi bài (chống 1 người spam nhiều bình luận), không thể suy ngược ra người viết.
3) (Tuỳ chọn, cần key) Tìm video âm nhạc có gắn vị trí gần từng tỉnh (search.list location=...).
"""
import hashlib
import itertools
import logging
import os
import time
from datetime import datetime, timezone

from common import TOPIC_CHARTS, TOPIC_COMMENTS, chart_entry, iso, track_fields

log = logging.getLogger("youtube")
CHARTS_URL = "https://charts.youtube.com/youtubei/v1/browse?alt=json"
COUNTRY = os.getenv("YT_COUNTRY", "vn")
AUTHOR_SALT = os.getenv("AUTHOR_SALT", "vn-music-pulse")


def author_hash(channel_id):
    if not channel_id:
        return None
    return hashlib.sha256(f"{AUTHOR_SALT}:{channel_id}".encode()).hexdigest()[:16]


def _browse(session, chart_type, period="WEEKLY"):
    body = {
        "context": {"client": {"clientName": "WEB_MUSIC_ANALYTICS", "clientVersion": "2.0",
                               "hl": "vi", "gl": COUNTRY.upper(), "theme": "MUSIC"}},
        "browseId": "FEmusic_analytics_charts_home",
        "query": (f"perspective=CHART_DETAILS&chart_params_country_code={COUNTRY}"
                  f"&chart_params_chart_type={chart_type}&chart_params_period_type={period}"),
    }
    r = session.post(CHARTS_URL, json=body, timeout=30,
                     headers={"Origin": "https://charts.youtube.com", "Referer": "https://charts.youtube.com/"})
    r.raise_for_status()
    content = r.json()["contents"]["sectionListRenderer"]["contents"][0]["musicAnalyticsSectionRenderer"]["content"]
    if chart_type == "TRACKS":
        return content["trackTypes"][0].get("trackViews", [])
    if chart_type == "VIDEOS":
        return content["videos"][0].get("videoViews", [])
    return content["artists"][0].get("artistViews", [])


def _thumb(x):
    th = (x.get("thumbnail") or {}).get("thumbnails") or []
    return th[-1]["url"] if th else None


def _release(x):
    rd = x.get("releaseDate") or {}
    return f"{rd['year']:04d}-{rd.get('month', 1):02d}-{rd.get('day', 1):02d}" if rd.get("year") else None


def chart_videos(session):
    """Danh sách (video_id, track_fields, weekly_views, rank, prev) của BXH bài hát + video."""
    out = []
    for x in _browse(session, "TRACKS"):
        vid = x.get("encryptedVideoId")
        tr = track_fields(x.get("name"), [a["name"] for a in x.get("artists", [])], "youtube", platform_id=vid,
                          url=f"https://www.youtube.com/watch?v={vid}" if vid else None, thumbnail=_thumb(x),
                          release_date=_release(x))
        meta = x.get("chartEntryMetadata", {})
        out.append(("youtube_top_songs_vn_weekly", vid, tr, x.get("viewCount"), meta))
    from textnorm import clean_channel_name, clean_video_title

    by_video = {vid: tr for _, vid, tr, _, _ in out if vid}
    for x in _browse(session, "VIDEOS"):
        vid = x.get("id")
        known = by_video.get(vid)  # video cũng nằm trong BXH bài hát -> dùng tên bài/nghệ sĩ sạch của nó
        if known:
            title, artists = known["title"], known["artists"]
        else:  # tên video kiểu 'NGHỆ SĨ | TÊN BÀI | OFFICIAL MV', nghệ sĩ có thể là tên kênh/label
            artists = [clean_channel_name(a["name"]) for a in x.get("artists", [])]
            title = clean_video_title(x.get("title"), artists)
        tr = track_fields(title, artists, "youtube", platform_id=vid,
                          url=f"https://www.youtube.com/watch?v={vid}" if vid else None, thumbnail=_thumb(x),
                          duration_s=x.get("videoDuration"), release_date=_release(x))
        tr["raw_title"] = x.get("title")
        meta = x.get("chartEntryMetadata", {})
        out.append(("youtube_top_videos_vn_weekly", vid, tr, x.get("viewCount"), meta))
    return out


def crawl_charts(session, crawled_ms):
    items = chart_videos(session)
    sizes = {}
    for chart_id, _, _, _, _ in items:
        sizes[chart_id] = sizes.get(chart_id, 0) + 1
    for chart_id, _, tr, views, meta in items:
        yield TOPIC_CHARTS, tr["track_key"], chart_entry(
            chart_id, crawled_ms, meta.get("currentPosition"), sizes[chart_id], tr,
            previous_rank=meta.get("previousPosition"), metric_name="weekly_views",
            metric_value=int(views) if views else None, granularity_min=60)
    # BXH nghệ sĩ: record_type riêng, không có track_key
    artists = _browse(session, "ARTISTS")
    for x in artists:
        meta = x.get("chartEntryMetadata", {})
        rec = {"record_type": "artist_entry", "chart_id": "youtube_top_artists_vn_weekly", "chart_scope": "VN",
               "crawled_ms": crawled_ms, "rank": meta.get("currentPosition"), "chart_size": len(artists),
               "previous_rank": meta.get("previousPosition"), "metric_name": "weekly_views",
               "metric_value": float(x.get("viewCount") or 0), "artists": [x.get("name")], "source": "youtube",
               "title": x.get("name"), "track_key": None, "platform_id": x.get("externalChannelId")}
        yield TOPIC_CHARTS, None, rec
    log.info("youtube charts: %s + %d artists", sizes, len(artists))


# ------------------------------------------------------------------ comments
def _api_comments(session, key, video_id, max_n, since_ms):
    params = dict(part="snippet", videoId=video_id, maxResults=100, order="time", textFormat="plainText", key=key)
    got = 0
    while got < max_n:
        r = session.get("https://www.googleapis.com/youtube/v3/commentThreads", params=params, timeout=25)
        if r.status_code in (403, 404):  # tắt bình luận / hết quota
            log.warning("comments %s: HTTP %s %s", video_id, r.status_code, r.text[:120])
            return
        r.raise_for_status()
        data = r.json()
        for it in data.get("items", []):
            sn = it["snippet"]["topLevelComment"]["snippet"]
            pub = int(datetime.fromisoformat(sn["publishedAt"].replace("Z", "+00:00")).timestamp() * 1000)
            if since_ms and pub < since_ms:
                return  # order=time -> các bình luận sau còn cũ hơn
            yield dict(comment_id=it["id"], text=sn.get("textOriginal") or sn.get("textDisplay") or "",
                       like_count=int(sn.get("likeCount") or 0), published_ms=pub, is_reply=False,
                       author_hash=author_hash((sn.get("authorChannelId") or {}).get("value")))
            got += 1
            if got >= max_n:
                return
        if not data.get("nextPageToken"):
            return
        params["pageToken"] = data["nextPageToken"]


def _scrape_comments(video_id, max_n, since_ms, sort="recent"):
    from textnorm import parse_count
    from youtube_comment_downloader import SORT_BY_POPULAR, SORT_BY_RECENT, YoutubeCommentDownloader

    dl = YoutubeCommentDownloader()
    gen = dl.get_comments(video_id, sort_by=SORT_BY_RECENT if sort == "recent" else SORT_BY_POPULAR, language="en")
    old_streak = 0
    for c in itertools.islice(gen, max_n):
        pub = int(c["time_parsed"] * 1000) if c.get("time_parsed") else None
        if sort == "recent" and since_ms and pub and pub < since_ms:
            old_streak += 1
            if old_streak >= 20:  # thứ tự "mới nhất" không tuyệt đối -> dừng khi gặp 20 cái cũ liên tiếp
                return
            continue
        old_streak = 0
        yield dict(comment_id=c.get("cid"), text=c.get("text") or "", like_count=parse_count(c.get("votes")),
                   published_ms=pub, is_reply=bool(c.get("reply")), author_hash=author_hash(c.get("channel")))


def search_video(session, query):
    """Tìm video đầu tiên trên YouTube cho 1 bài hát (endpoint youtubei/search của web, không tốn quota)."""
    body = {"context": {"client": {"clientName": "WEB", "clientVersion": "2.20250101.00.00", "hl": "vi", "gl": "VN"}},
            "query": query, "params": "EgIQAQ%3D%3D"}  # filter: chỉ video
    r = session.post("https://www.youtube.com/youtubei/v1/search?prettyPrint=false", json=body, timeout=20)
    r.raise_for_status()
    stack = [r.json()]
    while stack:
        o = stack.pop(0)
        if isinstance(o, dict):
            if "videoRenderer" in o and o["videoRenderer"].get("videoId"):
                return o["videoRenderer"]["videoId"]
            stack.extend(o.values())
        elif isinstance(o, list):
            stack.extend(o)
    return None


# Playlist Zing có nhiều bình luận "quê tôi ..." -> tăng tín hiệu theo tỉnh
ZING_GENRE_KEYWORDS = ["Trữ Tình", "Quê Hương", "Cải Lương", "Rap Việt", "Nhạc Trẻ", "V-Pop", "EDM Việt",
                       "Nhạc Trịnh", "Dance Việt", "Rock Việt"]


def _zing_candidates(session):
    """Bài hot trên Zing (BXH realtime + đầu các playlist thể loại) chưa có video id -> cần search."""
    import src_zing

    z = src_zing.ZingClient(session)
    out = []
    try:
        for it in z.chart_home()["RTChart"]["items"][: int(os.getenv("YT_ZING_RT_TOP", "30"))]:
            out.append(src_zing._track(it))
        per_pl = int(os.getenv("YT_ZING_PER_PLAYLIST", "6"))
        seen = set()
        for sec in z.top100():
            for pl in sec.get("items", []):
                name = pl.get("title", "")
                if pl.get("encodeId") in seen or not any(k.lower() in name.lower() for k in ZING_GENRE_KEYWORDS):
                    continue
                seen.add(pl.get("encodeId"))
                songs = (z.playlist(pl["encodeId"]).get("song") or {}).get("items", [])
                out.extend(src_zing._track(it) for it in songs[:per_pl])
    except Exception as e:  # noqa: BLE001
        log.warning("zing candidates failed: %s", e)
    return out


def crawl_comments(session, crawled_ms, shard_index=0, shard_count=1, since_hours=None):
    top_n = int(os.getenv("YT_COMMENT_VIDEOS", "60"))
    recent_n = int(os.getenv("YT_COMMENTS_PER_VIDEO", "200"))
    popular_n = int(os.getenv("YT_POPULAR_PER_VIDEO", "100"))
    key = os.getenv("YOUTUBE_API_KEY", "").strip()
    since_ms = crawled_ms - int(float(since_hours) * 3_600_000) if since_hours else None

    # 1) ứng viên: video trên BXH YouTube (đã có id) + bài hot trên Zing (cần search id)
    cands, seen = [], set()
    for _, vid, tr, _, meta in sorted(chart_videos(session), key=lambda x: x[4].get("currentPosition") or 999):
        if vid and tr["track_key"] not in seen and len(cands) < top_n:
            seen.add(tr["track_key"])
            cands.append((vid, tr))
    if os.getenv("YT_COMMENT_ZING_EXTRA", "true").lower() == "true":
        for tr in _zing_candidates(session):
            if tr["track_key"] not in seen:
                seen.add(tr["track_key"])
                cands.append((None, tr))
    # 2) chia shard: pod thứ i xử lý phần tử i, i+n, i+2n...
    mine = cands[shard_index::shard_count]
    log.info("comments: shard %d/%d -> %d/%d videos, mode=%s, since=%s", shard_index, shard_count, len(mine),
             len(cands), "api" if key else "scrape", iso(since_ms) if since_ms else "all")

    for vid, tr in mine:
        if not vid:
            try:
                vid = search_video(session, f"{tr['title']} {' '.join(tr['artists'][:1])}")
            except Exception as e:  # noqa: BLE001
                log.warning("search %s failed: %s", tr["track_key"], e)
            if not vid:
                continue
        n = 0
        if key:
            sources = [("recent", lambda: _api_comments(session, key, vid, recent_n, since_ms))]
        else:
            sources = [("popular", lambda: _scrape_comments(vid, popular_n, None, "popular")),
                       ("recent", lambda: _scrape_comments(vid, recent_n, since_ms, "recent"))]
        for label, make in sources:
            # chế độ scrape đôi khi trả về rỗng tạm thời -> thử lại 1 lần
            for attempt in range(2):
                got = 0
                try:
                    for c in make():
                        if not c["comment_id"] or not c["text"].strip():
                            continue
                        rec = {"record_type": "comment", "source": "youtube", "video_id": vid, "crawled_ms": crawled_ms,
                               "track_key": tr["track_key"], "title": tr["title"], "artists": tr["artists"],
                               "comment_sort": label, **c}
                        yield TOPIC_COMMENTS, tr["track_key"], rec
                        got += 1
                except Exception as e:  # noqa: BLE001 - 1 video lỗi không dừng cả job
                    log.warning("comments %s/%s failed after %d: %s", vid, label, got, e)
                n += got
                if got > 0 or key or (label == "recent" and since_ms):
                    break
                time.sleep(3)
        log.info("video %s (%s): %d comments", vid, tr["title"], n)
        time.sleep(0.5)


# ------------------------------------------------------------------ geo search (tuỳ chọn)
def crawl_geo(session, crawled_ms):
    from provinces import PROVINCES

    key = os.getenv("YOUTUBE_API_KEY", "").strip()
    if not key:
        log.warning("YOUTUBE_API_KEY chưa đặt -> bỏ qua geo search")
        return
    after = datetime.fromtimestamp(crawled_ms / 1000 - 30 * 86400, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for p in PROVINCES:
        params = dict(part="snippet", type="video", videoCategoryId="10", location=f"{p['lat']},{p['lon']}",
                      locationRadius=os.getenv("YT_GEO_RADIUS", "60km"), order="viewCount", publishedAfter=after,
                      maxResults=25, regionCode="VN", key=key)
        r = session.get("https://www.googleapis.com/youtube/v3/search", params=params, timeout=25)
        if r.status_code != 200:
            log.warning("geo %s: HTTP %s %s", p["code"], r.status_code, r.text[:150])
            if r.status_code == 403:
                return  # hết quota
            continue
        items = r.json().get("items", [])
        for rank, it in enumerate(items, start=1):
            sn = it["snippet"]
            title, artist = sn.get("title", ""), sn.get("channelTitle", "")
            if " - " in title:  # quy ước "Nghệ sĩ - Tên bài"
                artist, title = [s.strip() for s in title.split(" - ", 1)]
            vid = it["id"]["videoId"]
            tr = track_fields(title, artist, "youtube", platform_id=vid, url=f"https://www.youtube.com/watch?v={vid}",
                              thumbnail=((sn.get("thumbnails") or {}).get("high") or {}).get("url"))
            yield TOPIC_CHARTS, tr["track_key"], chart_entry("youtube_geo_search", crawled_ms, rank, len(items), tr,
                                                             scope=p["code"], granularity_min=720)
        log.info("geo %s: %d videos", p["code"], len(items))
        time.sleep(0.2)
