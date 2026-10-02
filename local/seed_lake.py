"""(Chỉ dùng khi TEST CỤC BỘ, không dùng trên k8s) Nạp file JSONL do crawler --sink jsonl tạo ra
vào master dataset với đúng layout mà speed layer ghi từ Kafka (topic, key, value, kafka_ts, dt),
để chạy thử batch job mà không cần Kafka.

  spark-submit local/seed_lake.py <thư_mục_jsonl> <DATA_BASE, vd file:///tmp/music>

Lưu ý: KHÔNG nạp vào thư mục mà speed layer (streaming file sink) cũng ghi. Khi đã có
thư mục _spark_metadata, Spark chỉ đọc các file do streaming sink ghi nhận và bỏ qua file nạp tay.
"""
import glob
import json
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession

src, base = sys.argv[1], sys.argv[2]
spark = SparkSession.builder.appName("seed-lake").config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh").getOrCreate()
rows = []
for fn in glob.glob(os.path.join(src, "*.jsonl")):
    with open(fn, encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            v = rec["value"]
            ts = datetime.fromtimestamp((v.get("crawled_ms") or v.get("ts_ms")) / 1000, tz=timezone.utc)
            rows.append((rec["topic"], rec.get("key"), json.dumps(v, ensure_ascii=False), ts, ts.strftime("%Y-%m-%d")))
df = spark.createDataFrame(rows, "topic string, key string, value string, kafka_ts timestamp, dt string")
df.write.mode("append").partitionBy("topic", "dt").parquet(base + "/datalake/raw")
print("seeded", df.groupBy("topic").count().collect())
