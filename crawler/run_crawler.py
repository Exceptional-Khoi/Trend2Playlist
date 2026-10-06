"""Điểm vào của crawler (chạy trong pod CronJob của k8s).

Ví dụ:
  python run_crawler.py --job charts                  # BXH mọi nền tảng -> Kafka
  python run_crawler.py --job playlists
  python run_crawler.py --job comments --shard-index 0 --shard-count 3 --since-hours 3
  python run_crawler.py --job trends                  # Google Trends theo tỉnh (chậm, ~1 nhóm/45 giây)
  python run_crawler.py --job charts --sink stdout    # debug, in ra màn hình
"""
import argparse
import logging
import os
import sys
import time


def _load_local_env():
    """Nạp .env khi chạy local, không ghi đè biến đã được k8s/CI cung cấp.

    Không dùng python-dotenv để giữ image crawler gọn và tránh thêm dependency chỉ
    cho vài cặp KEY=VALUE. Secret không bao giờ được log tại đây.
    """
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8-sig") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if not key or not key.replace("_", "").isalnum():
                continue
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            os.environ.setdefault(key, value)


_load_local_env()
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shared"))

import src_apple  # noqa: E402
import src_spotify  # noqa: E402
import src_trends  # noqa: E402
import src_youtube  # noqa: E402
import src_zing  # noqa: E402
from common import http_session, make_sink, now_ms, setup_logging  # noqa: E402

log = logging.getLogger("crawler")

JOBS = {
    "charts": [("zing", src_zing.crawl_charts), ("spotify", src_spotify.crawl_charts),
               ("apple_music", src_apple.crawl_charts), ("youtube", src_youtube.crawl_charts)],
    "playlists": [("zing", src_zing.crawl_playlists), ("spotify", src_spotify.crawl_playlists)],
    "comments": [("youtube", src_youtube.crawl_comments)],
    "geo": [("youtube", src_youtube.crawl_geo)],
    "trends": [("google_trends", src_trends.crawl_trends)],
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", choices=list(JOBS) + ["all"], required=True)
    ap.add_argument("--sink", choices=["kafka", "stdout", "jsonl"], default=os.getenv("SINK", "kafka"))
    ap.add_argument("--jsonl-dir", default=os.getenv("JSONL_DIR", "./out"))
    ap.add_argument("--sources", default="", help="lọc nguồn, ví dụ: zing,youtube")
    ap.add_argument("--shard-index", type=int, default=int(os.getenv("JOB_COMPLETION_INDEX", "0")))
    ap.add_argument("--shard-count", type=int, default=int(os.getenv("SHARD_COUNT", "1")))
    ap.add_argument("--since-hours", type=float, default=float(os.getenv("SINCE_HOURS", "0")) or None)
    args = ap.parse_args()
    setup_logging()

    jobs = list(JOBS) if args.job == "all" else [args.job]
    only = {s.strip() for s in args.sources.split(",") if s.strip()}
    sink = make_sink(args.sink, directory=args.jsonl_dir)
    session = http_session()
    crawled_ms = now_ms()
    ok, failed = [], []
    t0 = time.time()

    for job in jobs:
        for name, fn in JOBS[job]:
            if only and name not in only:
                continue
            kwargs = {}
            if job == "comments":
                kwargs = dict(shard_index=args.shard_index, shard_count=args.shard_count, since_hours=args.since_hours)
            n = 0
            try:
                for topic, key, value in fn(session, crawled_ms, **kwargs):
                    sink.send(topic, key, value)
                    n += 1
                ok.append(f"{job}/{name}={n}")
            except Exception as e:  # noqa: BLE001 - 1 nền tảng lỗi không làm hỏng các nền tảng khác
                log.exception("%s/%s failed after %d records: %s", job, name, n, e)
                failed.append(f"{job}/{name}")
    sink.close()
    log.info("DONE in %.1fs: sent=%d errors=%d ok=%s failed=%s", time.time() - t0, sink.count, sink.errors, ok, failed)
    # Chịu lỗi theo nguồn: một endpoint web bị chặn không được làm hỏng dữ liệu các nền tảng còn lại
    # hoặc chặn bootstrap. Vẫn có thể bật chế độ nghiêm ngặt để CI/monitoring bắt partial failure.
    fail_on_partial = os.getenv("FAIL_ON_PARTIAL", "false").lower() in ("1", "true", "yes")
    if failed and not fail_on_partial and sink.count > 0 and sink.errors == 0:
        log.warning("PARTIAL SUCCESS: đã giữ %d records; nguồn lỗi=%s", sink.count, failed)
    if sink.count == 0 or sink.errors > 0 or (failed and fail_on_partial):
        sys.exit(1)


if __name__ == "__main__":
    main()
