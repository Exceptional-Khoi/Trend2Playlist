"""Tiện ích chung cho crawler & simulator: HTTP session, sink Kafka, thời gian, logging.

Luồng dữ liệu: nền tảng (HTTP) -> crawler (pod k8s) -> Kafka topic.
Không ghi file trung gian ra đĩa; sink "stdout"/"jsonl" chỉ để debug trên máy dev.
"""
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Kafka topics (tạo sẵn bởi Job kafka-init-topics)
TOPIC_CHARTS = os.getenv("TOPIC_CHARTS", "music.charts")
TOPIC_PLAYLISTS = os.getenv("TOPIC_PLAYLISTS", "music.playlists")
TOPIC_COMMENTS = os.getenv("TOPIC_COMMENTS", "music.comments")
TOPIC_EVENTS = os.getenv("TOPIC_EVENTS", "music.events")
TOPIC_TRENDS = os.getenv("TOPIC_TRENDS", "music.trends")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/128.0 Safari/537.36")

log = logging.getLogger("music")


def setup_logging():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)


def http_session():
    s = requests.Session()
    retry = Retry(total=4, backoff_factor=1.5, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=frozenset(["GET", "POST"]))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": UA, "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8"})
    return s


def now_ms():
    return int(time.time() * 1000)


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def snapshot_id(chart_id, crawled_ms, granularity_min=30):
    """Gom các lần crawl gần nhau vào cùng 1 snapshot (mặc định 30 phút)."""
    bucket = crawled_ms // (granularity_min * 60_000) * (granularity_min * 60_000)
    return f"{chart_id}@{iso(bucket)}"


# ------------------------------------------------------------------ sinks
class KafkaSink:
    def __init__(self, bootstrap):
        from kafka import KafkaProducer  # import muộn để chạy sink stdout không cần kafka-python
        self.producer = KafkaProducer(
            bootstrap_servers=[b.strip() for b in bootstrap.split(",") if b.strip()],
            acks="all",                      # chờ đủ ISR -> không mất dữ liệu khi 1 broker chết
            retries=10,
            linger_ms=50,
            compression_type="gzip",
            max_in_flight_requests_per_connection=1,
            key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )
        self.count = 0
        self.errors = 0

    def _on_error(self, exc):
        self.errors += 1
        log.error("Kafka send failed: %s", exc)

    def send(self, topic, key, value):
        self.producer.send(topic, key=key, value=value).add_errback(self._on_error)
        self.count += 1

    def close(self):
        self.producer.flush(timeout=60)
        self.producer.close(timeout=30)


class StdoutSink:
    def __init__(self, limit_per_topic=3):
        self.count = 0
        self.errors = 0
        self.by_topic = {}
        self.limit = limit_per_topic

    def send(self, topic, key, value):
        self.count += 1
        n = self.by_topic.get(topic, 0)
        self.by_topic[topic] = n + 1
        if n < self.limit:
            print(topic, key, json.dumps(value, ensure_ascii=False)[:600])

    def close(self):
        print("SUMMARY", json.dumps(self.by_topic))


class JsonlSink:
    """Chỉ dùng khi test cục bộ (ví dụ đổ dữ liệu mẫu cho Spark local)."""

    def __init__(self, directory):
        os.makedirs(directory, exist_ok=True)
        self.dir = directory
        self.files = {}
        self.count = 0
        self.errors = 0

    def send(self, topic, key, value):
        f = self.files.get(topic)
        if f is None:
            f = self.files[topic] = open(os.path.join(self.dir, topic + ".jsonl"), "a", encoding="utf-8")
        f.write(json.dumps({"topic": topic, "key": key, "value": value}, ensure_ascii=False) + "\n")
        self.count += 1

    def close(self):
        for f in self.files.values():
            f.close()


def make_sink(kind, bootstrap=None, directory=None):
    if kind == "kafka":
        return KafkaSink(bootstrap or os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    if kind == "jsonl":
        return JsonlSink(directory or os.getenv("JSONL_DIR", "./out"))
    return StdoutSink()


# ------------------------------------------------------------------ record builders
def _textnorm():
    try:
        import textnorm  # trong container: shared/*.py được mount phẳng cùng thư mục /app
    except ImportError:  # chạy từ repo: thêm ../shared vào sys.path
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shared"))
        import textnorm
    return textnorm


def track_fields(title, artists, source, platform_id=None, url=None, thumbnail=None, duration_s=None,
                 genres=None, release_date=None, album=None):
    """Các trường mô tả bài hát, dùng chung cho mọi topic."""
    tn = _textnorm()
    arts = tn.split_artists(artists)
    return {
        "track_key": tn.track_key(title, arts),
        "title": tn.clean_title(title),
        "raw_title": title,
        "artists": arts,
        "source": source,
        "platform_id": platform_id,
        "url": url,
        "thumbnail": thumbnail,
        "duration_s": int(duration_s) if duration_s else None,
        "genres": [g for g in (genres or []) if g],
        "release_date": release_date,
        "album": album,
    }


def chart_entry(chart_id, crawled_ms, rank, chart_size, track, previous_rank=None, metric_name=None,
                metric_value=None, total_plays=None, total_likes=None, scope="VN", granularity_min=30):
    rec = {
        "record_type": "chart_entry",
        "chart_id": chart_id,
        "chart_scope": scope,
        "snapshot_id": snapshot_id(chart_id, crawled_ms, granularity_min),
        "crawled_ms": crawled_ms,
        "rank": int(rank),
        "chart_size": int(chart_size),
        "previous_rank": int(previous_rank) if previous_rank else None,
        "metric_name": metric_name,
        "metric_value": float(metric_value) if metric_value is not None else None,
        "total_plays": int(total_plays) if total_plays is not None else None,
        "total_likes": int(total_likes) if total_likes is not None else None,
    }
    rec.update(track)
    return rec


def playlist_item(playlist_id, playlist_name, genre_hint, crawled_ms, position, track):
    rec = {
        "record_type": "playlist_item",
        "playlist_id": playlist_id,
        "playlist_name": playlist_name,
        "genre_hint": genre_hint,
        "snapshot_id": snapshot_id(playlist_id, crawled_ms, 360),
        "crawled_ms": crawled_ms,
        "position": int(position),
    }
    rec.update(track)
    if genre_hint and genre_hint not in rec["genres"]:
        rec["genres"] = rec["genres"] + [genre_hint]
    return rec
