"""Bộ giả lập người nghe -> Kafka topic music.events.

VÌ SAO CẦN: không nền tảng nào công khai lịch sử nghe của người dùng khác, nên để demo
"gợi ý playlist theo thời gian thực" ta sinh sự kiện nghe cho N người dùng ảo.
Bài hát lấy từ CATALOG THẬT đã crawl (ES index music-tracks, hoặc đọc lại Kafka nếu ES
chưa có), mức phổ biến lấy từ BXH thật và mức ưa thích theo tỉnh từ batch view thật.
Chỉ các tham số hành vi (thể loại ưa thích theo vùng, xác suất skip...) là giả định.

Mỗi replica (StatefulSet) sở hữu 1 nhóm người dùng riêng: sim<ordinal>-<i>.
  python simulator.py                                  # chạy liên tục
  python simulator.py --backfill-days 7 --backfill-events 150000   # sinh lịch sử quá khứ rồi thoát
"""
import argparse
import json
import logging
import math
import os
import random
import socket
import sys
import time
import uuid

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shared"))

from common import TOPIC_CHARTS, TOPIC_EVENTS, TOPIC_PLAYLISTS, make_sink, now_ms, setup_logging  # noqa: E402
from provinces import PROVINCES, REGION_GENRE_PRIOR  # noqa: E402
from textnorm import GENRE_BUCKETS, genre_bucket  # noqa: E402

log = logging.getLogger("simulator")
ES_URL = os.getenv("ES_URL", "http://localhost:9200")

# Hệ số hoạt động theo giờ trong ngày (giờ VN) - cao điểm 20h-23h
HOURLY = [0.25, 0.15, 0.1, 0.08, 0.08, 0.15, 0.35, 0.55, 0.65, 0.6, 0.6, 0.7,
          0.8, 0.7, 0.6, 0.6, 0.65, 0.75, 0.9, 1.0, 1.15, 1.2, 1.0, 0.6]


# ------------------------------------------------------------------ catalog
class Catalog:
    def __init__(self):
        self.tracks = {}          # track_key -> dict(title, artists, genre, pop, duration_s)
        self.by_genre = {}        # genre -> [track_key]
        self.by_artist = {}       # artist -> [track_key]
        self.province_top = {}    # province -> [(track_key, weight)]
        self.loaded_at = 0

    def _index(self):
        self.by_genre, self.by_artist = {}, {}
        for k, t in self.tracks.items():
            self.by_genre.setdefault(t["genre"], []).append(k)
            if t["artists"]:
                self.by_artist.setdefault(t["artists"][0], []).append(k)

    def load_from_es(self):
        r = requests.post(f"{ES_URL}/music-tracks/_search", timeout=15, json={
            "size": 3000, "sort": [{"national_score": "desc"}],
            "_source": ["track_key", "title", "artists", "genre", "national_score", "duration_s"]})
        if r.status_code != 200:
            return False
        hits = r.json()["hits"]["hits"]
        if len(hits) < 20:
            return False
        self.tracks = {h["_source"]["track_key"]: dict(
            title=h["_source"].get("title"), artists=h["_source"].get("artists") or [],
            genre=h["_source"].get("genre") or "Khác",
            pop=0.05 + float(h["_source"].get("national_score") or 0),
            duration_s=h["_source"].get("duration_s") or 210) for h in hits}
        # mức ưa thích theo tỉnh từ batch view thật (nếu đã có)
        try:
            r = requests.post(f"{ES_URL}/music-trending-batch/_search", timeout=15, json={
                "size": 34 * 30, "query": {"range": {"rank": {"lte": 30}}},
                "_source": ["province_code", "track_key", "score"]})
            self.province_top = {}
            for h in r.json()["hits"]["hits"]:
                s = h["_source"]
                if s["track_key"] in self.tracks:
                    self.province_top.setdefault(s["province_code"], []).append((s["track_key"], float(s["score"])))
        except Exception:  # noqa: BLE001
            self.province_top = {}
        self._index()
        return True

    def load_from_records(self, values):
        """Dựng catalog từ bản ghi crawl thô (chart_entry / playlist_item)."""
        best = {}
        for v in values:
            k = v.get("track_key")
            if not k or v.get("record_type") not in ("chart_entry", "playlist_item"):
                continue
            pos = v.get("rank") or v.get("position") or 100
            pts = (1.0 - min(pos, 100) / 101) * (1.0 if v["record_type"] == "chart_entry" else 0.3)
            t = best.setdefault(k, dict(title=v.get("title"), artists=v.get("artists") or [], genres=set(), pop=0.0,
                                        duration_s=v.get("duration_s") or 210))
            t["genres"].update(v.get("genres") or [])
            t["pop"] = max(t["pop"], pts)
        if len(best) < 20:
            return False
        self.tracks = {k: dict(title=t["title"], artists=t["artists"], pop=0.05 + t["pop"], duration_s=t["duration_s"],
                               genre=genre_bucket(sorted(t["genres"]), t["title"], t["artists"]))
                       for k, t in best.items()}
        self.province_top = {}
        self._index()
        return True

    def load_from_kafka(self, bootstrap, seconds=20):
        """Chưa có batch view -> đọc thẳng dữ liệu crawl trong Kafka để dựng catalog."""
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(TOPIC_CHARTS, TOPIC_PLAYLISTS, bootstrap_servers=bootstrap.split(","),
                                 auto_offset_reset="earliest", enable_auto_commit=False, group_id=None,
                                 consumer_timeout_ms=seconds * 1000,
                                 value_deserializer=lambda v: json.loads(v.decode("utf-8")))
        try:
            return self.load_from_records(m.value for m in consumer)
        finally:
            consumer.close()

    def refresh(self, bootstrap, force=False):
        if not force and time.time() - self.loaded_at < 600:
            return
        ok = False
        try:
            ok = self.load_from_es()
        except Exception as e:  # noqa: BLE001
            log.info("ES catalog not ready: %s", e)
        if not ok and bootstrap:
            ok = self.load_from_kafka(bootstrap)
        if ok:
            self.loaded_at = time.time()
            log.info("catalog: %d tracks, %d genres, province views=%d", len(self.tracks), len(self.by_genre),
                     len(self.province_top))
        elif not self.tracks:
            raise RuntimeError("catalog empty - chạy crawler trước")


# ------------------------------------------------------------------ users
class User:
    def __init__(self, uid, rng):
        self.id = uid
        pops = [p["population"] for p in PROVINCES]
        self.province = rng.choices(PROVINCES, weights=pops)[0]
        prior = REGION_GENRE_PRIOR[self.province["region"]]
        # khẩu vị cá nhân = ưu tiên theo vùng x nhiễu log-normal
        self.taste = {g: prior.get(g, 0.3) * math.exp(rng.gauss(0, 0.6)) for g in GENRE_BUCKETS}
        self.activity = math.exp(rng.gauss(0, 0.8))
        self.recent = []
        self.fav_artists = []

    def top_taste(self):
        return max(self.taste.values())


def pick_track(user, cat, rng):
    last = user.recent[-1] if user.recent else None
    r = rng.random()
    pool = None
    if last and r < 0.35:  # nghe tiếp bài tương tự (cùng nghệ sĩ / cùng thể loại)
        lt = cat.tracks.get(last)
        if lt:
            same_artist = cat.by_artist.get(lt["artists"][0], []) if lt["artists"] else []
            pool = same_artist if (same_artist and rng.random() < 0.4) else cat.by_genre.get(lt["genre"])
    elif r < 0.70:  # nghe theo xu hướng: ưu tiên BXH của tỉnh mình (batch view), không có thì toàn quốc
        local = cat.province_top.get(user.province["code"])
        if local:
            keys, w = zip(*local)
            return rng.choices(keys, weights=w)[0]
    if not pool:  # nghe theo khẩu vị thể loại
        genres = [g for g in cat.by_genre]
        g = rng.choices(genres, weights=[user.taste.get(x, 0.3) * len(cat.by_genre[x]) ** 0.5 for x in genres])[0]
        pool = cat.by_genre[g]
    pool = [k for k in pool if k not in user.recent[-5:]] or pool
    return rng.choices(pool, weights=[cat.tracks[k]["pop"] for k in pool])[0]


def make_events(user, cat, rng, ts_ms, source="simulator"):
    key = pick_track(user, cat, rng)
    t = cat.tracks[key]
    match = user.taste.get(t["genre"], 0.3) / user.top_taste()
    dur_ms = int((t.get("duration_s") or 210) * 1000)
    skip = rng.random() < (0.45 - 0.35 * match)
    listen = int(rng.uniform(3, 28) * 1000) if skip else int(dur_ms * rng.uniform(0.8, 1.0))
    base = {"user_id": user.id, "province_code": user.province["code"], "track_key": key, "title": t["title"],
            "artists": t["artists"], "genre": t["genre"], "duration_ms": dur_ms, "source": source}
    events = [dict(base, event_id=str(uuid.uuid4()), action="skip" if skip else "play", listen_ms=listen, ts_ms=ts_ms)]
    if not skip and rng.random() < 0.05 + 0.15 * match:
        events.append(dict(base, event_id=str(uuid.uuid4()), action="like", listen_ms=0, ts_ms=ts_ms + 1000))
    user.recent = (user.recent + [key])[-20:]
    return events


def ordinal():
    host = os.getenv("HOSTNAME", socket.gethostname())
    tail = host.rsplit("-", 1)[-1]
    return int(tail) if tail.isdigit() else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sink", default=os.getenv("SINK", "kafka"), choices=["kafka", "stdout", "jsonl"])
    ap.add_argument("--jsonl-dir", default=os.getenv("JSONL_DIR", "./out"))
    ap.add_argument("--users", type=int, default=int(os.getenv("SIM_USERS", "300")))
    ap.add_argument("--eps", type=float, default=float(os.getenv("SIM_EVENTS_PER_SEC", "5")),
                    help="số lượt nghe mỗi giây của replica này")
    ap.add_argument("--backfill-days", type=float, default=0)
    ap.add_argument("--backfill-events", type=int, default=0)
    ap.add_argument("--catalog-jsonl", default="", help="(test cục bộ) dựng catalog từ file jsonl của crawler")
    args = ap.parse_args()
    setup_logging()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "")
    idx = ordinal()
    rng = random.Random(1000 + idx)
    cat = Catalog()
    if args.catalog_jsonl:
        load_catalog_from_jsonl(cat, args.catalog_jsonl)
    else:
        while True:
            try:
                cat.refresh(bootstrap, force=True)
                break
            except Exception as e:  # noqa: BLE001
                log.warning("waiting for catalog: %s", e)
                time.sleep(30)
    users = [User(f"sim{idx}-{i:04d}", random.Random(f"{idx}-{i}")) for i in range(args.users)]
    weights = [u.activity for u in users]
    sink = make_sink(args.sink, bootstrap=bootstrap, directory=args.jsonl_dir)
    log.info("replica %d: %d users, %.1f events/s", idx, len(users), args.eps)

    if args.backfill_events:
        end = now_ms()
        start = end - int(args.backfill_days * 86400_000)
        for i in range(args.backfill_events):
            ts = rng.randint(start, end)
            hour = int((ts / 3600_000 + 7) % 24)  # giờ VN
            if rng.random() > HOURLY[hour] / max(HOURLY):
                continue
            u = rng.choices(users, weights=weights)[0]
            for ev in make_events(u, cat, rng, ts):
                sink.send(TOPIC_EVENTS, ev["user_id"], ev)
            if i % 20000 == 0:
                log.info("backfill %d/%d", i, args.backfill_events)
        sink.close()
        log.info("backfill done: %d events", sink.count)
        return

    last_log = time.time()
    while True:
        tick = time.time()
        hour = int((tick / 3600 + 7) % 24)
        factor = HOURLY[hour] if os.getenv("SIM_DIURNAL", "false").lower() == "true" else 1.0
        n = max(1, int(round(args.eps * factor)))
        for u in rng.choices(users, weights=weights, k=n):
            for ev in make_events(u, cat, rng, now_ms()):
                sink.send(TOPIC_EVENTS, ev["user_id"], ev)
        if time.time() - last_log > 60:
            log.info("sent %d events total", sink.count)
            last_log = time.time()
            if not args.catalog_jsonl:
                try:
                    cat.refresh(bootstrap)
                except Exception as e:  # noqa: BLE001
                    log.warning("catalog refresh failed: %s", e)
        time.sleep(max(0.0, 1.0 - (time.time() - tick)))


def load_catalog_from_jsonl(cat, path):
    def values():
        for fn in path.split(","):
            with open(fn, encoding="utf-8") as f:
                for line in f:
                    yield json.loads(line)["value"]
    cat.load_from_records(values())
    log.info("catalog from jsonl: %d tracks", len(cat.tracks))


if __name__ == "__main__":
    main()
