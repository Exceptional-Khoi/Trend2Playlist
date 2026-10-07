from __future__ import annotations
import argparse, json, random, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

def get_real_track_ids():
    from common import OUT
    canon = OUT / "canonical_tracks.jsonl"
    if canon.exists():
        tids = []
        for line in canon.read_text(encoding="utf-8").splitlines():
            try:
                tids.append(json.loads(line)["canonical_track_id"])
            except Exception:
                pass
        if tids:
            return tids
    return None

def main():
    ap = argparse.ArgumentParser(description="Generate play/like/skip events for the system's own realtime layer.")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="music.user-events.v1")
    ap.add_argument("--rate", type=float, default=50.0, help="events/sec")
    ap.add_argument("--users", type=int, default=100)
    ap.add_argument("--tracks", type=int, default=50)
    ap.add_argument("--max-events", type=int, default=200, help="Max events to generate (0 for infinite stream)")
    ap.add_argument("--out-file", default=None, help="Save events to JSONL file (e.g. output/user_events.jsonl)")
    args = ap.parse_args()

    track_pool = get_real_track_ids() or [f"trk_{i:04d}" for i in range(args.tracks)]
    actions = ["play", "play", "play", "like", "skip"]
    provinces = ["Hà Nội", "Hồ Chí Minh", "Đà Nẵng", "Hải Phòng", "Cần Thơ", "Bình Dương", "Huế", "Nha Trang"]

    producer = None
    file_handle = None

    if args.out_file:
        from pathlib import Path
        out_p = Path(args.out_file)
        if not out_p.is_absolute():
            from common import OUT
            out_p = OUT / args.out_file if not args.out_file.startswith("output/") else Path(__file__).resolve().parents[1] / args.out_file
        out_p.parent.mkdir(parents=True, exist_ok=True)
        file_handle = out_p.open("w", encoding="utf-8")
        print(f"Producing user events directly to file: {out_p}")
    else:
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
                request_timeout_ms=3000
            )
            print(f"Connected to Kafka broker at {args.bootstrap}, streaming to topic: {args.topic}")
        except Exception as e:
            from common import OUT
            fallback_file = OUT / "user_events.jsonl"
            print(f"Kafka unavailable ({e}). Fallback: writing events to {fallback_file}")
            file_handle = fallback_file.open("w", encoding="utf-8")

    count = 0
    try:
        while True:
            ev = {
                "event_id": str(uuid.uuid4()),
                "event_time": datetime.now(timezone.utc).isoformat(),
                "user_id": f"u{random.randrange(args.users):05d}",
                "track_id": random.choice(track_pool),
                "action": random.choice(actions),
                "province": random.choice(provinces),
                "session_id": f"s{random.randrange(max(1, args.users // 3)):05d}",
            }
            if file_handle:
                file_handle.write(json.dumps(ev, ensure_ascii=False) + "\n")
            if producer:
                producer.send(args.topic, ev)

            count += 1
            if args.max_events > 0 and count >= args.max_events:
                print(f"Successfully generated {count} user events.")
                break

            if args.rate > 0:
                time.sleep(1.0 / args.rate)
    finally:
        if file_handle:
            file_handle.close()
        if producer:
            producer.flush()
            producer.close()

if __name__ == "__main__":
    main()
