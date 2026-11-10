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
    "countries": "music.country-charts",
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
        memory_mb = int(os.getenv("ROCKSDB_MEMORY_MB", "64"))
        if memory_mb <= 0:
            raise ValueError("ROCKSDB_MEMORY_MB must be positive")
        b = (b.config("spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage", "true")
             .config("spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB", str(memory_mb)))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def batch_checkpoint_dir(spark, run_id):
    """Reliable, run-owned HDFS checkpoint; never share concurrent batch state."""
    import re
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("Invalid batch run ID")
    path = CKPT.rstrip("/") + "/batch-views/" + run_id
    spark.sparkContext.setCheckpointDir(path)
    return path


def cleanup_batch_checkpoint(spark, path):
    prefix = CKPT.rstrip("/") + "/batch-views/"
    suffix = path.removeprefix(prefix)
    import re
    if not path.startswith(prefix) or not re.fullmatch(r"[A-Za-z0-9_-]+", suffix):
        raise ValueError("Refusing checkpoint cleanup outside this batch run")
    fs, target = _fs(spark, path)
    fs.delete(target, True)


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
    except AnalysisException as exc:
        if getattr(exc, 'getErrorClass', lambda: None)() != 'PATH_NOT_FOUND':
            raise
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
    except Exception as exc:
        code = getattr(exc, 'getErrorClass', lambda: None)()
        if isinstance(exc, FileNotFoundError) or code in ('PATH_NOT_FOUND', 'TABLE_OR_VIEW_NOT_FOUND'):
            return None
        raise


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


def write_latest_partition(rows, endpoint, index, id_col, version_col):
    """Atomic latest-capture updates; a stale microbatch is a no-op, not a 409."""
    from urllib.error import HTTPError, URLError
    from urllib.parse import quote
    script = ("def old = ctx._source[params.field]; "
              "if (ctx.op == 'create' || old == null || params.doc[params.field] >= old) "
              "{ ctx._source = params.doc; } else { ctx.op = 'noop'; }")
    pending = []

    def flush(batch):
        lines = []
        for doc in batch:
            lines.extend([json.dumps({'update': {'_id': str(doc[id_col]), 'retry_on_conflict': 3}}),
                          json.dumps({'scripted_upsert': True, 'script': {'source': script,
                              'params': {'field': version_col, 'doc': doc}}, 'upsert': {}}, allow_nan=False)])
        req = urllib.request.Request(endpoint + '/' + quote(index, safe='') + '/_bulk', method='POST',
              data=('\n'.join(lines) + '\n').encode(), headers={'Content-Type': 'application/x-ndjson'})
        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=30) as response:
                    result = json.load(response)
                statuses = [item['update']['status'] for item in result.get('items', [])]
                if len(statuses) != len(batch):
                    raise RuntimeError('Incomplete Elasticsearch bulk response')
                if all(200 <= status < 300 for status in statuses):
                    return
                if not all(200 <= status < 300 or status in (429, 502, 503, 504) for status in statuses):
                    raise RuntimeError('Elasticsearch latest-capture bulk rejected document')
            except HTTPError as exc:
                if exc.code not in (429, 502, 503, 504):
                    raise
            except (URLError, TimeoutError):
                pass
            if attempt == 2:
                raise RuntimeError('Elasticsearch latest-capture bulk retries exhausted')
            time.sleep(.5 * (attempt + 1))

    for row in rows:
        doc = row.asDict(recursive=True) if hasattr(row, 'asDict') else dict(row)
        if doc.get(id_col) is None or type(doc.get(version_col)) is not int:
            raise ValueError('Latest-capture document requires ID and integer timestamp')
        pending.append(doc)
        if len(pending) == 200:
            flush(pending); pending = []
    if pending:
        flush(pending)


def es_write_latest(df, index, id_col, version_col='crawled_ms'):
    if not ES_NODES:
        return es_write(df, index, id_col)
    # Capture driver configuration: standalone executors need not inherit ES env.
    endpoint = f'http://{ES_NODES}:{ES_PORT}'
    df.foreachPartition(lambda rows: write_latest_partition(rows, endpoint, index, id_col, version_col))


def _es_request(method, path, body=None):
    if not ES_NODES:
        return None
    req = urllib.request.Request(f"http://{ES_NODES}:{ES_PORT}{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode() or "{}")


def es_replace(df, index, id_col, run_id):
    """Delete only generations observed BEFORE this write, never an unseen newer run.

    This is not an atomic view swap: readers may still observe an in-progress batch.
    A stale cleanup cannot delete a concurrently published generation, including a
    document overwritten between the delete query snapshot and its execution.
    """
    from urllib.error import HTTPError
    previous = []
    if ES_NODES:
        try:
            response = _es_request('POST', f'/{index}/_search', {
                'size': 0, 'aggs': {'runs': {'terms': {'field': 'run_id', 'size': 10000}}}})
            runs = response['aggregations']['runs']
            if runs.get('sum_other_doc_count', 0):
                raise ValueError('Too many generations for safe replacement cleanup')
            previous = [b['key'] for b in runs['buckets'] if b['key'] != run_id]
        except HTTPError as exc:
            if exc.code != 404:
                raise
    es_write(df, index, id_col)
    if ES_NODES:
        try:
            _es_request("POST", f"/{index}/_refresh")
        except HTTPError as exc:
            if exc.code != 404:
                raise
            # The Spark connector does not create an index for an empty frame.
            # A valid empty view must publish successfully on its first run too.
            try:
                _es_request("PUT", f"/{index}", {})
            except HTTPError as created:
                if created.code != 400:
                    raise
                try:
                    kind = json.loads(created.read()).get('error', {}).get('type')
                except (ValueError, AttributeError):
                    raise created
                if kind != 'resource_already_exists_exception':
                    raise
            _es_request("POST", f"/{index}/_refresh")
        if previous:
            _es_request("POST", f"/{index}/_delete_by_query?conflicts=proceed&refresh=true",
                        {'query': {'terms': {'run_id': previous}}})


def es_put(index, doc_id, doc):
    from pathlib import Path
    from urllib.parse import quote
    if not ES_NODES:
        if not isinstance(index, str) or not index or index in ('.', '..') or any(c in index for c in '/\\\x00'):
            raise ValueError('Invalid index directory')
        directory = Path(LOCAL_ES_OUT).resolve() / index
        if directory.resolve().parent != Path(LOCAL_ES_OUT).resolve():
            raise ValueError('Index directory escapes local output')
        if not isinstance(doc_id, str) or not doc_id or any(c in doc_id for c in '/\\\x00'):
            raise ValueError('Invalid local document ID')
        directory.mkdir(parents=True, exist_ok=True)
        with (directory / (quote(doc_id, safe='') + '.json')).open('w', encoding='utf-8') as f:
            json.dump(doc, f, ensure_ascii=False, allow_nan=False)
        return
    _es_request("PUT", f"/{index}/_doc/{quote(doc_id, safe='')}?refresh=true", doc)


def now_ms():
    return int(time.time() * 1000)
