"""Điểm vào của crawler (chạy trong pod CronJob của k8s).

Ví dụ:
  python run_crawler.py --job charts                  # BXH mọi nền tảng -> Kafka
  python run_crawler.py --job playlists
  python run_crawler.py --job comments --shard-index 0 --shard-count 3 --since-hours 3
  python run_crawler.py --job trends                  # Google Trends theo tỉnh (chậm, ~1 nhóm/45 giây)
  python run_crawler.py --job charts --sink stdout    # debug, in ra màn hình
"""
import argparse
import json
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shared"))

import src_apple  # noqa: E402
import src_spotify  # noqa: E402
import src_trends  # noqa: E402
import src_youtube  # noqa: E402
import src_zing  # noqa: E402
from common import capture_issues, http_session, make_sink, now_ms, setup_logging  # noqa: E402

log = logging.getLogger("crawler")

JOBS = {
    "charts": [("zing", src_zing.crawl_charts), ("spotify", src_spotify.crawl_charts),
               ("apple_music", src_apple.crawl_charts), ("youtube", src_youtube.crawl_charts)],
    "playlists": [("zing", src_zing.crawl_playlists), ("spotify", src_spotify.crawl_playlists)],
    "comments": [("youtube", src_youtube.crawl_comments)],
    "geo": [("youtube", src_youtube.crawl_geo)],
    "trends": [("google_trends", src_trends.crawl_trends)],
}


def run_source(job, name, fn, session, sink, crawled_ms, **kwargs):
    n = 0
    delivery_failed = False
    with capture_issues() as issues:
        try:
            for topic, key, value in fn(session, crawled_ms, **kwargs):
                try:
                    sink.send(topic, key, value)
                except Exception:
                    delivery_failed = True
                    raise
                n += 1
        except Exception as exc:  # Keep healthy sources running; omit URLs/secrets.
            issues.append({"component": "sink" if delivery_failed else name, "reason": type(exc).__name__})
    status = ("partial" if n else "failed") if issues else ("success" if n else "empty")
    result = {"job": job, "source": name, "status": status, "records": n, "issues": issues,
              "delivery_failed": delivery_failed}
    log.info("SOURCE_RESULT %s", json.dumps(result, ensure_ascii=False))
    return result


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", choices=list(JOBS) + ["all"], required=True)
    ap.add_argument("--sink", choices=["kafka", "stdout", "jsonl"], default=os.getenv("SINK", "kafka"))
    ap.add_argument("--jsonl-dir", default=os.getenv("JSONL_DIR", "./out"))
    ap.add_argument("--sources", default="", help="lọc nguồn, ví dụ: zing,youtube")
    ap.add_argument("--shard-index", type=int, default=int(os.getenv("JOB_COMPLETION_INDEX", "0")))
    ap.add_argument("--shard-count", type=int, default=int(os.getenv("SHARD_COUNT", "1")))
    ap.add_argument("--since-hours", type=float, default=float(os.getenv("SINCE_HOURS", "0")) or None)
    ap.add_argument("--allow-partial", action="store_true", help="exit 0 for partial collection; still log every failure")
    args = ap.parse_args(argv)
    if args.shard_count < 1 or not 0 <= args.shard_index < args.shard_count:
        ap.error("shard-index must be in [0, shard-count), shard-count must be positive")
    if args.since_hours is not None and args.since_hours <= 0:
        ap.error("since-hours must be positive")
    setup_logging()

    jobs = list(JOBS) if args.job == "all" else [args.job]
    only = {s.strip() for s in args.sources.split(",") if s.strip()}
    available = {name for job in jobs for name, _ in JOBS[job]}
    if only - available:
        ap.error("unknown sources for selected job: " + ", ".join(sorted(only - available)))
    sink = make_sink(args.sink, directory=args.jsonl_dir)
    session = http_session()
    crawled_ms = now_ms()
    results = []
    t0 = time.time()

    for job in jobs:
        for name, fn in JOBS[job]:
            if only and name not in only:
                continue
            kwargs = {}
            if job == "comments":
                kwargs = dict(shard_index=args.shard_index, shard_count=args.shard_count, since_hours=args.since_hours)
            results.append(run_source(job, name, fn, session, sink, crawled_ms, **kwargs))
    close_failed = False
    try:
        sink.close()
    except Exception as exc:
        close_failed = True
        log.error("sink close failed: %s", type(exc).__name__)
    finally:
        session.close()
    incomplete = any(r["status"] in ("partial", "failed") or
                     (r["status"] == "empty" and r["job"] != "comments") for r in results)
    # An empty incremental comments window is valid, but is explicitly labelled empty.
    valid_empty = bool(results) and all(r["job"] == "comments" and r["status"] == "empty" for r in results)
    failed = (close_failed or sink.errors > 0 or any(r["delivery_failed"] for r in results)
              or (sink.count == 0 and not valid_empty))
    failed = failed or (incomplete and not args.allow_partial)
    log.info("DONE in %.1fs: sent=%d errors=%d incomplete=%s exit=%d", time.time() - t0,
             sink.count, sink.errors, incomplete, int(failed))
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
