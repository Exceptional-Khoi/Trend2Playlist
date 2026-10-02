"""Tạo index template cho mọi index music-* trên Elasticsearch (chạy 1 lần, idempotent).

- Chuỗi mặc định là keyword (để group/terms aggregation), trừ 'text' của bình luận.
- Các trường thời gian (epoch millis) map sang kiểu date để Kibana / date_histogram dùng được.
"""
import os
import sys
import time

import httpx

ES_URL = os.getenv("ES_URL", "http://localhost:9200")
REPLICAS = int(os.getenv("ES_REPLICAS", "0"))
SHARDS = int(os.getenv("ES_SHARDS", "1"))

DATE_FIELDS = ["minute", "updated_at", "run_ts", "crawled_ms", "published_ms", "last_ts_ms", "ts_ms",
               "first_seen_ms", "last_seen_ms", "last_crawl_ms", "produced_at"]

TEMPLATE = {
    "index_patterns": ["music-*"],
    "priority": 100,
    "template": {
        "settings": {"number_of_shards": SHARDS, "number_of_replicas": REPLICAS, "refresh_interval": "2s"},
        "mappings": {
            "dynamic_templates": [
                {"strings_as_keyword": {"match_mapping_type": "string",
                                        "mapping": {"type": "keyword", "ignore_above": 2048}}},
            ],
            "properties": {
                **{f: {"type": "date", "format": "epoch_millis||strict_date_optional_time"} for f in DATE_FIELDS},
                "text": {"type": "text"},
                "search_text": {"type": "keyword"},
                "score": {"type": "double"},
                "national_score": {"type": "double"},
            },
        },
    },
}


def ensure(es_url=ES_URL, retries=60):
    for i in range(retries):
        try:
            r = httpx.put(f"{es_url}/_index_template/music", json=TEMPLATE, timeout=10)
            r.raise_for_status()
            print("index template 'music' ok:", r.json(), flush=True)
            return True
        except Exception as e:  # noqa: BLE001 - ES chưa sẵn sàng
            print(f"waiting for Elasticsearch ({i + 1}/{retries}): {e}", flush=True)
            time.sleep(5)
    return False


if __name__ == "__main__":
    sys.exit(0 if ensure() else 1)
