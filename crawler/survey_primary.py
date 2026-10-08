"""Trial the project's existing Zing, Spotify, Apple and YouTube collectors."""

import os
import itertools
import json
import re
import time
from urllib.parse import urlencode


def collect(context):
    from common import setup_logging
    from src_apple import crawl_charts as apple_charts
    from src_spotify import crawl_charts as spotify_charts, crawl_playlists as spotify_playlists
    import src_youtube
    import src_zing
    from __main__ import SessionProxy

    setup_logging()
    os.environ.setdefault("ZING_SONG_INFO_LIMIT", "8")
    src_zing.TOP100_LIMIT = 5
    session = SessionProxy(context)
    crawled_ms = int(time.time() * 1000)

    jobs = [
        ("zing", "charts", "https://zingmp3.vn/api/v2/page/get/chart-home", src_zing.crawl_charts),
        ("zing", "playlists", "https://zingmp3.vn/api/v2/page/get/top-100", src_zing.crawl_playlists),
        ("spotify", "charts", "https://open.spotify.com/embed/playlist/37i9dQZEVXbLdGSmz6xilI", spotify_charts),
        ("spotify", "playlists", "https://open.spotify.com/embed/playlist/37i9dQZF1DX0F4i7Q9pshJ", spotify_playlists),
        ("apple_music", "charts", "https://rss.applemarketingtools.com/api/v2/vn/music/most-played/100/songs.json", apple_charts),
        ("youtube", "charts", src_youtube.CHARTS_URL, src_youtube.crawl_charts),
    ]
    for platform, category, url, function in jobs:
        def run(fn=function, p=platform, u=url):
            for topic, key, value in fn(session, crawled_ms):
                value = dict(value)
                value["topic"] = topic
                value["collection_key"] = key
                # Kworb is a secondary source; preserve it separately from Spotify itself.
                source_url = ("https://kworb.net/spotify/country/vn_daily.html"
                              if str(value.get("chart_id", "")).startswith("spotify_daily_streams_") else u)
                if value.get("chart_id") == "spotify_top_songs_vn_weekly":
                    source_url = "https://open.spotify.com/embed/playlist/37i9dQZEVXbKZyn1mKjmIl"
                context.record(p, value.get("record_type", category), value, source_url)
        context.attempt(platform, category, url, run)

    candidates = []
    for item in context.store.records.get("youtube", []):
        data = item["data"]
        if data.get("record_type") == "chart_entry" and data.get("platform_id"):
            if data["platform_id"] not in [vid for vid, _ in candidates]:
                candidates.append((data["platform_id"], data))
    if not candidates:
        # Official full music video, verified from the artist's public channel.
        video_id = "Llw9Q6akRo4"
        url = "https://www.youtube.com/watch?v=" + video_id
        embed_url = "https://www.youtube.com/oembed?" + urlencode({"format": "json", "url": url})
        def embed():
            response = session.get(embed_url, timeout=20)
            response.raise_for_status()
            data = response.json()
            if not data.get("title"):
                raise ValueError("No music video title in oEmbed response")
            context.record("youtube", "video_embed_metadata", {**data, "platform_id": video_id, "url": url}, embed_url)
        context.attempt("youtube", "video_embed_metadata", embed_url, embed)

        def video():
            response = session.get(url, timeout=20)
            response.raise_for_status()
            match = re.search(r'(?:var\s+)?ytInitialPlayerResponse\s*=\s*', response.text)
            if not match:
                raise ValueError("No public video player metadata in watch page")
            player, _ = json.JSONDecoder().raw_decode(response.text[match.end():])
            details = player.get("videoDetails") or {}
            if not details.get("title"):
                raise ValueError("No videoDetails in public watch page")
            microformat = player.get("microformat", {}).get("playerMicroformatRenderer", {})
            payload = {key: details[key] for key in ("videoId", "title", "lengthSeconds", "channelId", "shortDescription", "viewCount", "author", "keywords", "thumbnail", "isLiveContent") if key in details}
            for key in ("publishDate", "uploadDate", "category"):
                if microformat.get(key):
                    payload[key] = microformat[key]
            payload["url"] = url
            context.record("youtube", "music_video_metadata", payload, url)
        context.attempt("youtube", "music_video_metadata", url, video)
        candidates = [(video_id, {"title": "LẠC TRÔI | OFFICIAL MUSIC VIDEO | SƠN TÙNG M-TP"})]

    for video_id, track in candidates[:2]:
        url = "https://www.youtube.com/watch?v=" + video_id
        def comments(vid=video_id, tr=track, u=url):
            from textnorm import parse_count
            from youtube_comment_downloader import YoutubeCommentDownloader, SORT_BY_RECENT
            class BoundedDownloader(YoutubeCommentDownloader):
                def ajax_request(self, endpoint, ytcfg, **kwargs):
                    return super().ajax_request(endpoint, ytcfg, retries=1, sleep=0, timeout=15)
            downloader = BoundedDownloader()
            downloader.session = session
            count = 0
            for raw in itertools.islice(downloader.get_comments(vid, sort_by=SORT_BY_RECENT, language="en"), 20):
                comment = {"comment_id": raw.get("cid"), "text": raw.get("text") or "",
                           "like_count": parse_count(raw.get("votes")),
                           "published_ms": int(raw["time_parsed"] * 1000) if raw.get("time_parsed") else None,
                           "is_reply": bool(raw.get("reply")), "author_hash": src_youtube.author_hash(raw.get("channel"))}
                context.record("youtube", "comment", {
                    **comment, "video_id": vid, "track_title": tr.get("title"),
                    "record_type": "comment", "source": "youtube", "crawled_ms": crawled_ms,
                }, u)
                count += 1
            if not count:
                raise ValueError("No comments returned by this public watch page")
        context.attempt("youtube", "comments", url, comments)
