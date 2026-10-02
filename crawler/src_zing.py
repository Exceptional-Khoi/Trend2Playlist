"""Zing MP3 (zingmp3.vn) - nền tảng nghe nhạc lớn nhất VN.

Dùng API nội bộ mà chính web zingmp3.vn gọi. Mỗi request cần chữ ký:
    sig = HMAC_SHA512(secret, path + SHA256("ctime=..version=.."))
apiKey/secret/version là giá trị công khai trong JS của web; khi Zing đổi,
cập nhật qua biến môi trường ZING_API_KEY / ZING_SECRET_KEY / ZING_VERSION.
"""
import hashlib
import hmac
import logging
import os
import time
from datetime import datetime, timezone

from common import TOPIC_CHARTS, TOPIC_PLAYLISTS, chart_entry, playlist_item, track_fields

log = logging.getLogger("zing")
BASE = "https://zingmp3.vn"
SIGNED_PARAMS = ("count", "ctime", "id", "page", "type", "version")

# Playlist "Top 100" dùng để lấy thể loại + dữ liệu đồng xuất hiện (co-occurrence) cho gợi ý.
# Để trống -> lấy toàn bộ danh sách từ trang top-100.
TOP100_LIMIT = int(os.getenv("ZING_TOP100_LIMIT", "40"))


class ZingClient:
    def __init__(self, session):
        self.s = session
        self.version = os.getenv("ZING_VERSION", "1.6.34")
        self.api_key = os.getenv("ZING_API_KEY", "88265e23d4284f25963e6eedac8fbfa3")
        self.secret = os.getenv("ZING_SECRET_KEY", "2aa2d1c561e809b267f3638c4a307aab")
        self._cookie_ready = False

    def _sig(self, path, params):
        raw = "".join(f"{k}={params[k]}" for k in sorted(params) if k in SIGNED_PARAMS)
        sha = hashlib.sha256(raw.encode()).hexdigest()
        return hmac.new(self.secret.encode(), (path + sha).encode(), hashlib.sha512).hexdigest()

    def get(self, path, **params):
        if not self._cookie_ready:  # Zing yêu cầu cookie phiên (zmp3_rqid)
            self.s.get(BASE + "/", timeout=20)
            self._cookie_ready = True
        params = {**params, "ctime": str(int(time.time())), "version": self.version}
        params["sig"] = self._sig(path, params)
        params["apiKey"] = self.api_key
        r = self.s.get(BASE + path, params=params, timeout=25)
        r.raise_for_status()
        data = r.json()
        if data.get("err") != 0:
            raise RuntimeError(f"Zing {path} err={data.get('err')} msg={data.get('msg')}")
        return data["data"]

    def chart_home(self):
        return self.get("/api/v2/page/get/chart-home")

    def song_info(self, song_id):
        return self.get("/api/v2/song/get/info", id=song_id)

    def top100(self):
        return self.get("/api/v2/page/get/top-100")

    def playlist(self, playlist_id):
        return self.get("/api/v2/page/get/playlist", id=playlist_id)


def _track(item, genres=None):
    artists = [a.get("name") for a in (item.get("artists") or []) if a.get("name")] or item.get("artistsNames")
    rd = item.get("releaseDate")
    release = datetime.fromtimestamp(rd, tz=timezone.utc).strftime("%Y-%m-%d") if isinstance(rd, int) and rd > 0 else None
    return track_fields(
        title=item.get("title"), artists=artists, source="zing", platform_id=item.get("encodeId"),
        url=BASE + item["link"] if item.get("link") else None,
        thumbnail=item.get("thumbnailM") or item.get("thumbnail"), duration_s=item.get("duration"),
        genres=genres, release_date=release, album=(item.get("album") or {}).get("title"),
    )


def crawl_charts(session, crawled_ms):
    """BXH #zingchart realtime (100), BXH tuần Việt Nam (40), BXH nhạc mới."""
    z = ZingClient(session)
    home = z.chart_home()
    lists = [
        ("zing_realtime", home["RTChart"]["items"], "score"),
        ("zing_week_vn", home.get("weekChart", {}).get("vn", {}).get("items", []), None),
        ("zing_new_release", (home.get("newRelease") or {}).get("items", []) if isinstance(home.get("newRelease"), dict)
         else (home.get("newRelease") or []), None),
    ]
    # Lấy thể loại + lượt nghe/thích cho từng bài (song-info), có cache trong lần chạy
    info_cache = {}
    max_info = int(os.getenv("ZING_SONG_INFO_LIMIT", "160"))
    for _, items, _ in lists:
        for it in items:
            sid = it.get("encodeId")
            if sid and sid not in info_cache and len(info_cache) < max_info:
                try:
                    info_cache[sid] = z.song_info(sid)
                    time.sleep(0.15)
                except Exception as e:  # noqa: BLE001 - 1 bài lỗi không làm hỏng cả lượt crawl
                    log.warning("song-info %s failed: %s", sid, e)
                    info_cache[sid] = {}

    for chart_id, items, metric in lists:
        n = len(items)
        for rank, it in enumerate(items, start=1):
            info = info_cache.get(it.get("encodeId"), {})
            genres = [g.get("name") for g in info.get("genres", []) if g.get("name")]
            moved = it.get("rakingStatus")  # >0: tăng hạng, <0: giảm hạng
            prev = rank + moved if isinstance(moved, int) and moved != 0 else (rank if moved == 0 else None)
            t = _track(it, genres)
            yield TOPIC_CHARTS, t["track_key"], chart_entry(
                chart_id, crawled_ms, rank, n, t, previous_rank=prev,
                metric_name=metric, metric_value=it.get("score") if metric else None,
                total_plays=info.get("listen"), total_likes=info.get("like"))
    log.info("zing charts: %s", {c: len(i) for c, i, _ in lists})


def crawl_playlists(session, crawled_ms):
    """Các playlist 'Top 100' theo thể loại -> thể loại bài hát + đồng xuất hiện."""
    from textnorm import genre_hint_from_playlist

    z = ZingClient(session)
    seen = []
    for section in z.top100():
        for pl in section.get("items", []):
            if pl.get("encodeId") and pl["encodeId"] not in [p for p, _ in seen]:
                seen.append((pl["encodeId"], pl.get("title", "")))
    for pid, name in seen[:TOP100_LIMIT]:
        try:
            data = z.playlist(pid)
        except Exception as e:  # noqa: BLE001
            log.warning("playlist %s failed: %s", pid, e)
            continue
        hint = genre_hint_from_playlist(name)
        genres = [g.get("name") for g in data.get("genres", []) if g.get("name")]
        songs = (data.get("song") or {}).get("items", [])
        for pos, it in enumerate(songs, start=1):
            t = _track(it, genres)
            yield TOPIC_PLAYLISTS, t["track_key"], playlist_item(f"zing:{pid}", name, hint, crawled_ms, pos, t)
        time.sleep(0.3)
    log.info("zing playlists: %d", len(seen[:TOP100_LIMIT]))
