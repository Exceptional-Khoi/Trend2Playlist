"""(Chỉ để TEST CỤC BỘ) Phát lại file JSONL đã crawl (crawler --sink jsonl) vào Kafka, giữ nguyên topic + key.
Dùng khi muốn đẩy lại dữ liệu thật đã thu (ví dụ bình luận mất 15 phút mới crawl xong) vào pipeline.

  python local/replay_jsonl.py out/music.comments.jsonl out/music.trends.jsonl --bootstrap localhost:9092 [--rate 500]
"""
import argparse
import json
import sys
import time

from kafka import KafkaProducer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--rate", type=float, default=0, help="bản ghi/giây (0 = nhanh nhất có thể)")
    a = ap.parse_args()
    p = KafkaProducer(bootstrap_servers=a.bootstrap.split(","), acks="all", linger_ms=20, compression_type="gzip",
                      key_serializer=lambda k: k.encode() if k else None,
                      value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode())
    n, t0 = 0, time.time()
    for fn in a.files:
        with open(fn, encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue  # dòng cuối bị cắt dở
                p.send(rec["topic"], key=rec.get("key"), value=rec["value"])
                n += 1
                if a.rate and n % max(1, int(a.rate)) == 0:
                    time.sleep(max(0.0, n / a.rate - (time.time() - t0)))
    p.flush()
    print(f"replayed {n} records in {time.time() - t0:.1f}s", file=sys.stderr)


if __name__ == "__main__":
    main()
