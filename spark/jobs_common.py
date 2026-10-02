"""Cấu hình & tiện ích dùng chung cho các Spark job (speed layer + batch layer).

Đường dẫn dữ liệu (mặc định trên HDFS):
  {BASE}/datalake/raw/topic=<topic>/dt=<yyyy-MM-dd>/*.parquet   master dataset (bất biến, append-only)
  {BASE}/views/<view>/run_id=<id>/                               batch view (giữ 3 phiên bản gần nhất)
  {BASE}/views/_latest/<view>/                                   con trỏ tới phiên bản mới nhất
  {BASE}/checkpoints/<query>/                                    checkpoint của Structured Streaming
"""
import json
import os
import time
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
ES_NODES = os.getenv("ES_NODES", "")          # rỗng -> ghi JSON ra LOCAL_ES_OUT (chỉ để test cục bộ)
ES_PORT = os.getenv("ES_PORT", "9200")
BASE = os.getenv("DATA_BASE") or os.getenv("HDFS_URL") or "file:///tmp/music"
LAKE = BASE + "/datalake"
VIEWS = BASE + "/views"
CKPT = os.getenv("CHECKPOINT_PATH") or BASE + "/checkpoints"   # đổi khi nâng cấp query có trạng thái (schema/state store)
LOCAL_ES_OUT = os.getenv("LOCAL_ES_OUT", "/tmp/music/es_out")

TOPICS = {
    "charts": os.getenv("TOPIC_CHARTS", "music.charts"),
    "playlists": os.getenv("TOPIC_PLAYLISTS", "music.playlists"),
    "comments": os.getenv("TOPIC_COMMENTS", "music.comments"),
    "events": os.getenv("TOPIC_EVENTS", "music.events"),
    "trends": os.getenv("TOPIC_TRENDS", "music.trends"),
}

# ------------------------------------------------------------------ schemas (khớp crawler/common.py)
_S, _L, _I, _D, _B = T.StringType(), T.LongType(), T.IntegerType(), T.DoubleType(), T.BooleanType()
_ARR = T.ArrayType(T.StringType())
TRACK_FIELDS = [
    T.StructField("track_key", _S), T.StructField("title", _S), T.StructField("raw_title", _S),
    T.StructField("artists", _ARR), T.StructField("source", _S), T.StructField("platform_id", _S),
    T.StructField("url", _S), T.StructField("thumbnail", _S), T.StructField("duration_s", _I),
    T.StructField("genres", _ARR), T.StructField("release_date", _S), T.StructField("album", _S),
]
CHART_SCHEMA = T.StructType([
    T.StructField("record_type", _S), T.StructField("chart_id", _S), T.StructField("chart_scope", _S),
    T.StructField("snapshot_id", _S), T.StructField("crawled_ms", _L), T.StructField("rank", _I),
    T.StructField("chart_size", _I), T.StructField("previous_rank", _I), T.StructField("metric_name", _S),
    T.StructField("metric_value", _D), T.StructField("total_plays", _L), T.StructField("total_likes", _L),
] + TRACK_FIELDS)
PLAYLIST_SCHEMA = T.StructType([
    T.StructField("record_type", _S), T.StructField("playlist_id", _S), T.StructField("playlist_name", _S),
    T.StructField("genre_hint", _S), T.StructField("snapshot_id", _S), T.StructField("crawled_ms", _L),
    T.StructField("position", _I),
] + TRACK_FIELDS)
COMMENT_SCHEMA = T.StructType([
    T.StructField("record_type", _S), T.StructField("source", _S), T.StructField("video_id", _S),
    T.StructField("crawled_ms", _L), T.StructField("track_key", _S), T.StructField("title", _S),
    T.StructField("artists", _ARR), T.StructField("comment_sort", _S), T.StructField("comment_id", _S),
    T.StructField("text", _S), T.StructField("like_count", _L), T.StructField("published_ms", _L),
    T.StructField("is_reply", _B), T.StructField("author_hash", _S),
])
TRENDS_SCHEMA = T.StructType([
    T.StructField("record_type", _S), T.StructField("source", _S), T.StructField("snapshot_id", _S),
    T.StructField("crawled_ms", _L), T.StructField("timeframe", _S), T.StructField("property", _S),
    T.StructField("group_id", _I), T.StructField("anchor_key", _S), T.StructField("anchor_query", _S),
    T.StructField("track_key", _S), T.StructField("title", _S), T.StructField("artists", _ARR),
    T.StructField("query", _S), T.StructField("geo_code", _S), T.StructField("geo_name", _S),
    T.StructField("share", _I), T.StructField("has_data", _B), T.StructField("anchor_share", _I),
    T.StructField("anchor_has_data", _B), T.StructField("is_anchor", _B),
])
EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", _S), T.StructField("user_id", _S), T.StructField("province_code", _S),
    T.StructField("track_key", _S), T.StructField("title", _S), T.StructField("artists", _ARR),
    T.StructField("genre", _S), T.StructField("duration_ms", _L), T.StructField("source", _S),
    T.StructField("action", _S), T.StructField("listen_ms", _L), T.StructField("ts_ms", _L),
])
SCHEMAS = {"charts": CHART_SCHEMA, "playlists": PLAYLIST_SCHEMA, "comments": COMMENT_SCHEMA, "events": EVENT_SCHEMA,
           "trends": TRENDS_SCHEMA}


def get_spark(app_name):
    b = (SparkSession.builder.appName(app_name)
         .config("spark.sql.session.timeZone", os.getenv("TZ_NAME", "Asia/Ho_Chi_Minh")))
    if os.getenv("STATE_STORE", "").lower() == "rocksdb":
        # state streaming trong RocksDB (ngoài heap, tràn xuống đĩa được) thay vì giữ hết trên heap.
        # Bật trên k8s (Linux). Trên Windows bản JNI của RocksDB từng crash khi thiếu RAM -> mặc định tắt.
        b = b.config("spark.sql.streaming.stateStore.providerClass",
                     "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


# ------------------------------------------------------------------ UDF (textnorm được gửi kèm qua --py-files)
def province_udf():
    from textnorm import match_provinces
    return F.udf(lambda t: match_provinces(t) if t else [], T.ArrayType(T.StringType()))


MENTION_TYPE = T.ArrayType(T.StructType([T.StructField("province_code", T.StringType()),
                                         T.StructField("kind", T.StringType())]))


def mention_udf():
    """(text, title) -> [(tỉnh, 'self'|'mention')]; đã loại credit/lời bài hát và tỉnh nằm trong tên bài."""
    from textnorm import find_province_mentions
    return F.udf(lambda text, title: [{"province_code": c, "kind": k} for c, k in find_province_mentions(text, title or "")],
                 MENTION_TYPE)


def mention_weight_col(kind_col="kind"):
    from textnorm import MENTION_WEIGHT
    return F.when(F.col(kind_col) == "self", F.lit(MENTION_WEIGHT["self"])).otherwise(F.lit(MENTION_WEIGHT["mention"]))


def genre_udf():
    from textnorm import genre_bucket
    return F.udf(lambda g, t, a: genre_bucket(g or [], t or "", a or []), T.StringType())


def norm_udf():
    from textnorm import norm_text
    return F.udf(lambda t: norm_text(t) if t else "", T.StringType())


def slug_list_udf():
    from textnorm import slug
    return F.udf(lambda arr: [slug(a) for a in (arr or []) if a], T.ArrayType(T.StringType()))


def provinces_df(spark):
    from provinces import PROVINCES
    rows = [(p["code"], p["name"], p["region"], p["subregion"], float(p["lat"]), float(p["lon"]), float(p["population"]))
            for p in PROVINCES]
    return spark.createDataFrame(rows, "province_code string, province_name string, region string, subregion string, "
                                       "lat double, lon double, population double")


def old_provinces_df(spark):
    """63 tỉnh cũ (mã ISO của Google Trends) -> tỉnh mới + dân số, để gộp số liệu Trends."""
    from provinces import OLD_PROVINCES
    return spark.createDataFrame([(iso, name, new, float(pop)) for iso, name, new, pop in OLD_PROVINCES],
                                 "geo_code string, old_name string, province_code string, old_population double")


# ------------------------------------------------------------------ đọc master dataset
def read_raw(spark, topic_name):
    """Đọc 1 topic trong master dataset và parse JSON theo schema."""
    schema = SCHEMAS[topic_name]
    try:
        raw = spark.read.parquet(LAKE + "/raw").where(F.col("topic") == TOPICS[topic_name])
    except AnalysisException:
        empty = T.StructType(schema.fields + [T.StructField("kafka_ts", T.TimestampType()), T.StructField("dt", _S)])
        return spark.createDataFrame([], empty)
    return (raw.select(F.from_json("value", schema).alias("v"), "kafka_ts", "dt")
            .select("v.*", "kafka_ts", "dt"))


# ------------------------------------------------------------------ batch views (có phiên bản)
def _fs(spark, path):
    jvm = spark.sparkContext._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), p


def write_view(spark, df, name, run_id, keep=3):
    df.write.mode("overwrite").parquet(f"{VIEWS}/{name}/run_id={run_id}")
    spark.createDataFrame([(run_id,)], "run_id string").coalesce(1).write.mode("overwrite").json(f"{VIEWS}/_latest/{name}")
    fs, p = _fs(spark, f"{VIEWS}/{name}")
    runs = sorted(s.getPath().getName() for s in fs.listStatus(p) if s.getPath().getName().startswith("run_id="))
    for old in runs[:-keep]:
        fs.delete(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(f"{VIEWS}/{name}/{old}"), True)


def read_view(spark, name):
    try:
        rid = spark.read.json(f"{VIEWS}/_latest/{name}").first()["run_id"]
        return spark.read.parquet(f"{VIEWS}/{name}/run_id={rid}")
    except Exception:  # noqa: BLE001 - chưa có batch view
        return None


def path_size_bytes(spark, path):
    try:
        fs, p = _fs(spark, path)
        return int(fs.getContentSummary(p).getLength())
    except Exception:  # noqa: BLE001
        return 0


# ------------------------------------------------------------------ Elasticsearch (serving layer)
def es_write(df, index, id_col=None):
    if not ES_NODES:
        df.write.mode("append").json(f"{LOCAL_ES_OUT}/{index}")
        return
    w = (df.write.format("org.elasticsearch.spark.sql")
         .option("es.nodes", ES_NODES).option("es.port", ES_PORT)
         .option("es.nodes.wan.only", "true")          # chỉ đi qua Service k8s, không dò IP pod
         .option("es.batch.size.entries", "2000")
         .option("es.batch.write.retry.count", "6").option("es.batch.write.retry.wait", "5s")
         .option("es.resource", index))
    if id_col:
        w = w.option("es.mapping.id", id_col)        # ghi đè theo id -> idempotent (exactly-once hiệu quả)
    w.mode("append").save()


def _es_request(method, path, body=None):
    if not ES_NODES:
        return None
    req = urllib.request.Request(f"http://{ES_NODES}:{ES_PORT}{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode() or "{}")


def es_replace(df, index, id_col, run_id):
    """Ghi view mới rồi xoá document của các lần chạy cũ (view nhỏ, ghi đè toàn bộ)."""
    es_write(df, index, id_col)
    if ES_NODES:
        _es_request("POST", f"/{index}/_refresh")
        _es_request("POST", f"/{index}/_delete_by_query?conflicts=proceed&refresh=true",
                    {"query": {"bool": {"must_not": {"term": {"run_id": run_id}}}}})


def es_put(index, doc_id, doc):
    if not ES_NODES:
        os.makedirs(f"{LOCAL_ES_OUT}/{index}", exist_ok=True)
        with open(f"{LOCAL_ES_OUT}/{index}/{doc_id}.json", "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False)
        return
    _es_request("PUT", f"/{index}/_doc/{doc_id}?refresh=true", doc)


def now_ms():
    return int(time.time() * 1000)
