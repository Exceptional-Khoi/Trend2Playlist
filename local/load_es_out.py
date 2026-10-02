"""(Chỉ để TEST CỤC BỘ) Nạp output JSON của Spark chạy local (ES_NODES rỗng -> LOCAL_ES_OUT)
vào một Elasticsearch để thử API + dashboard mà không cần cụm k8s.

  python local/load_es_out.py <thư_mục_es_out> [--es http://localhost:9200] [--shift-now]

--shift-now: dời mốc thời gian của realtime view về "bây giờ" (để dashboard hiển thị dữ liệu 60 phút gần nhất).
"""
import argparse
import glob
import json
import os
import sys
import time

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "serving"))

ID_FIELD = {"music-recommendations": "user_id", "music-tracks": "track_key", "music-item-sim": "track_key",
            "music-an-rising": "track_key", "music-an-province-summary": "province_code"}
SHIFT_FIELDS = {"music-rt-plays": ["minute", "last_ts_ms"], "music-recommendations": ["updated_at"]}


def docs_of(index_dir):
    for fn in sorted(glob.glob(os.path.join(index_dir, "*.json"))):
        with open(fn, encoding="utf-8") as f:
            text = f.read().strip()
        if not text:
            continue
        if text.startswith("{") and "\n" not in text:  # file 1 document (es_put)
            yield os.path.splitext(os.path.basename(fn))[0], json.loads(text)
            continue
        for line in text.splitlines():
            if line.strip():
                yield None, json.loads(line)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("--es", default=os.getenv("ES_URL", "http://localhost:9200"))
    ap.add_argument("--shift-now", action="store_true")
    a = ap.parse_args()

    import es_setup
    es_setup.ensure(a.es, 20)
    for index_dir in sorted(glob.glob(os.path.join(a.src, "music-*"))):
        index = os.path.basename(index_dir)
        docs = list(docs_of(index_dir))
        shift = 0
        if a.shift_now and index in SHIFT_FIELDS:
            f0 = SHIFT_FIELDS[index][0]
            latest = max((d.get(f0) or 0) for _, d in docs) if docs else 0
            shift = (int(time.time() * 1000) // 60000 * 60000) - latest if latest else 0
        lines = []
        for fid, d in docs:
            for f in SHIFT_FIELDS.get(index, []) if shift else []:
                if d.get(f):
                    d[f] += shift
            _id = fid or d.get("doc_id") or d.get(ID_FIELD.get(index, ""))
            lines.append(json.dumps({"index": {"_index": index, **({"_id": _id} if _id else {})}}))
            lines.append(json.dumps(d, ensure_ascii=False))
        for i in range(0, len(lines), 4000):
            body = "\n".join(lines[i:i + 4000]) + "\n"
            r = requests.post(f"{a.es}/_bulk", data=body.encode("utf-8"),
                              headers={"Content-Type": "application/x-ndjson"}, timeout=120)
            r.raise_for_status()
            if r.json().get("errors"):
                bad = [x for x in r.json()["items"] if x["index"].get("error")][:2]
                print("  bulk errors:", json.dumps(bad, ensure_ascii=False)[:400])
        requests.post(f"{a.es}/{index}/_refresh", timeout=30)
        cnt = requests.get(f"{a.es}/{index}/_count", timeout=30).json().get("count")
        print(f"{index:32} {len(docs):6} docs -> {cnt} in ES" + (f" (shift {shift / 60000:.0f} min)" if shift else ""))


if __name__ == "__main__":
    main()
