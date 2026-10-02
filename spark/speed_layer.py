"""SPEED LAYER - Spark Structured Streaming đọc Kafka, chạy liên tục.

Các streaming query (chọn bằng SPEED_QUERIES, mặc định chạy tất cả trong 1 driver):
  ingest    : mọi topic -> HDFS master dataset (parquet, phân vùng topic/dt)   [cầu nối sang batch layer]
  plays     : lượt nghe -> đếm theo (tỉnh, bài, phút)  -> ES music-rt-plays      [xu hướng realtime theo tỉnh]
  mentions  : bình luận -> nhận diện tỉnh + loại (tự nhận/nhắc tên) -> ES music-rt-mentions
  charts    : BXH vừa crawl -> ES music-rt-charts (snapshot mới nhất)           [BXH realtime đa nền tảng]
  recommend : lịch sử nghe 30 phút gần nhất của từng user + item similarity
              (batch view) + xu hướng tỉnh -> playlist gợi ý -> ES music-recommendations

Chịu lỗi: mỗi query có checkpoint trên HDFS; pod driver chết -> k8s khởi động lại -> Spark đọc
checkpoint và tiếp tục từ offset Kafka cuối cùng. Ghi ES theo id cố định nên ghi lại không bị trùng.
"""
import os
import time

from pyspark.sql import Window
from pyspark.sql import functions as F

from jobs_common import (CKPT, COMMENT_SCHEMA, CHART_SCHEMA, EVENT_SCHEMA, KAFKA_BOOTSTRAP, LAKE, TOPICS,
                         es_put, es_write, get_spark, mention_udf, mention_weight_col, now_ms, read_view)

spark = get_spark("music-speed-layer")
QUERIES = [q.strip() for q in os.getenv("SPEED_QUERIES", "ingest,plays,mentions,charts,recommend").split(",") if q.strip()]
STARTING = os.getenv("STARTING_OFFSETS", "earliest")


def kafka_stream(topics, max_offsets=20000):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", ",".join(topics))
            .option("startingOffsets", STARTING)
            .option("maxOffsetsPerTrigger", max_offsets)
            .option("failOnDataLoss", "false")
            .load())


def parsed(topic_name, schema, alias="v"):
    return (kafka_stream([TOPICS[topic_name]])
            .select(F.from_json(F.col("value").cast("string"), schema).alias(alias)).select(f"{alias}.*"))


def events_stream():
    return (parsed("events", EVENT_SCHEMA)
            .where("user_id IS NOT NULL AND track_key IS NOT NULL AND ts_ms IS NOT NULL")
            .withColumn("ts", (F.col("ts_ms") / 1000).cast("timestamp")))


# ---------------------------------------------------------------- view cache (đọc lại batch view mỗi 5 phút)
_views = {}


def cached_view(name, ttl=int(os.getenv("VIEW_TTL_SEC", "300"))):
    df, loaded = _views.get(name, (None, 0))
    if time.time() - loaded < ttl:
        return df
    new = read_view(spark, name)
    if new is not None:
        new = new.cache()
        new.count()
    if df is not None:
        df.unpersist()
    _views[name] = (new, time.time())
    return new


def canonical(df):
    alias = cached_view("track_alias")
    if alias is None:
        return df
    return (df.join(F.broadcast(alias), df.track_key == alias.alias_key, "left")
            .withColumn("track_key", F.coalesce("canonical_key", "track_key")).drop("alias_key", "canonical_key"))


started = []

# ---------------------------------------------------------------- 1) ingest -> HDFS
if "ingest" in QUERIES:
    raw = (kafka_stream(list(TOPICS.values()), 50000)
           .select("topic", F.col("key").cast("string").alias("key"), F.col("value").cast("string").alias("value"),
                   F.col("timestamp").alias("kafka_ts"), "partition", "offset")
           .withColumn("dt", F.date_format("kafka_ts", "yyyy-MM-dd")))
    started.append(raw.writeStream.format("parquet").queryName("ingest_raw")
                   .option("path", LAKE + "/raw").option("checkpointLocation", CKPT + "/ingest")
                   .partitionBy("topic", "dt")
                   .trigger(processingTime=os.getenv("INGEST_TRIGGER", "1 minute")).start())

# ---------------------------------------------------------------- 2) lượt nghe theo tỉnh/phút
if "plays" in QUERIES:
    agg = (events_stream().withWatermark("ts", "10 minutes")
           .groupBy(F.window("ts", "1 minute"), "province_code", "track_key")
           .agg(F.sum(F.when(F.col("action") == "play", 1).otherwise(0)).alias("plays"),
                F.sum(F.when(F.col("action") == "skip", 1).otherwise(0)).alias("skips"),
                F.sum(F.when(F.col("action") == "like", 1).otherwise(0)).alias("likes"),
                F.approx_count_distinct("user_id").alias("listeners"),
                F.max("ts_ms").alias("last_ts_ms"),
                F.first("title").alias("title"), F.first("artists").alias("artists"), F.first("genre").alias("genre")))
    out = agg.select(
        F.concat_ws("|", "province_code", "track_key", F.date_format("window.start", "yyyyMMddHHmm")).alias("doc_id"),
        (F.col("window.start").cast("long") * 1000).alias("minute"),
        "province_code", "track_key", "title", "artists", "genre", "plays", "skips", "likes", "listeners", "last_ts_ms")
    started.append(out.writeStream.outputMode("update").queryName("rt_plays")
                   .foreachBatch(lambda df, _: es_write(df, "music-rt-plays", "doc_id"))
                   .option("checkpointLocation", CKPT + "/plays")
                   .trigger(processingTime=os.getenv("PLAYS_TRIGGER", "10 seconds")).start())

# ---------------------------------------------------------------- 3) bình luận nhắc tỉnh
if "mentions" in QUERIES:
    comments = parsed("comments", COMMENT_SCHEMA).where("comment_id IS NOT NULL AND text IS NOT NULL")
    # 'self' = người viết tự nhận ở tỉnh đó (trọng số 1), 'mention' = chỉ nhắc tên (0.3);
    # đã bỏ bình luận credit/lời bài hát và tỉnh nằm trong chính tên bài
    mentions = (comments.withColumn("m", F.explode(mention_udf()(F.col("text"), F.col("title"))))
                .withColumn("province_code", F.col("m.province_code")).withColumn("kind", F.col("m.kind"))
                .withColumn("weight", mention_weight_col()).drop("m"))

    def write_mentions(df, _):
        out = canonical(df).select(
            F.concat_ws("|", "comment_id", "province_code").alias("doc_id"),   # cùng bình luận crawl lại -> ghi đè
            "province_code", "track_key", "title", "artists", "video_id", "comment_id", "kind", "weight",
            "author_hash", F.substring("text", 1, 300).alias("text"), "like_count",
            F.coalesce("published_ms", "crawled_ms").alias("published_ms"), "crawled_ms")
        es_write(out, "music-rt-mentions", "doc_id")

    started.append(mentions.writeStream.queryName("rt_mentions").foreachBatch(write_mentions)
                   .option("checkpointLocation", CKPT + "/mentions")
                   .trigger(processingTime=os.getenv("MENTIONS_TRIGGER", "30 seconds")).start())

# ---------------------------------------------------------------- 4) BXH realtime
if "charts" in QUERIES:
    charts = parsed("charts", CHART_SCHEMA).where("rank IS NOT NULL AND chart_id IS NOT NULL")

    def write_charts(df, _):
        w = Window.partitionBy("chart_id", "chart_scope")
        latest = df.withColumn("_mx", F.max("crawled_ms").over(w)).where("crawled_ms = _mx").drop("_mx", "raw_title")
        latest = latest.dropDuplicates(["chart_id", "chart_scope", "rank"])
        es_write(latest.withColumn("doc_id", F.concat_ws("|", "chart_id", "chart_scope", F.col("rank").cast("string"))),
                 "music-rt-charts", "doc_id")

    started.append(charts.writeStream.queryName("rt_charts").foreachBatch(write_charts)
                   .option("checkpointLocation", CKPT + "/charts")
                   .trigger(processingTime=os.getenv("CHARTS_TRIGGER", "30 seconds")).start())

# ---------------------------------------------------------------- 5) gợi ý playlist realtime
RECO_N = int(os.getenv("RECO_SIZE", "20"))
TREND_WEIGHT = float(os.getenv("RECO_TREND_WEIGHT", "0.35"))


def recommend_batch(df, batch_id):
    df = df.persist()  # micro-batch được dùng nhiều lần -> tránh chạy lại phép gộp có trạng thái
    try:
        if not df.isEmpty():
            _recommend(df, batch_id)
    finally:
        df.unpersist()


def _recommend(df, batch_id):
    now = now_ms()
    # Mỗi user có nhiều cửa sổ trượt được cập nhật; cửa sổ bắt đầu sớm nhất = toàn bộ 30 phút gần nhất
    first = Window.partitionBy("user_id").orderBy(F.col("window.start").asc())
    latest = df.withColumn("rn", F.row_number().over(first)).where("rn = 1")
    hist = (latest.select("user_id", "province_code", F.explode("hist").alias("h"))
            .select("user_id", "province_code", "h.*"))
    weight = (F.when(F.col("action") == "like", 2.0).when(F.col("action") == "play", 1.0)
              .when(F.col("action") == "skip", -0.7).otherwise(0.0))
    decay = F.exp(-(F.lit(now) - F.col("ts_ms")) / F.lit(15 * 60 * 1000.0))  # bài nghe càng gần càng quan trọng
    seeds = (canonical(hist).groupBy("user_id", "province_code", "track_key")
             .agg(F.sum(weight * decay).alias("w"), F.max("ts_ms").alias("last_ts")))
    users = seeds.select("user_id", "province_code").distinct()

    sim, trend, catalog = cached_view("item_sim"), cached_view("trending_batch"), cached_view("catalog")
    cand_cols = ["user_id", "province_code", "cand"]
    parts = []
    if sim is not None:  # 5a) collaborative: bài tương tự các bài vừa nghe (co-occurrence từ batch layer)
        cf = (seeds.where("w > 0").join(sim, "track_key")
              .withColumn("contrib", F.col("w") * F.col("sim"))
              .groupBy("user_id", "province_code", F.col("neighbor_key").alias("cand"))
              .agg(F.sum("contrib").alias("cf"), F.max_by("track_key", "contrib").alias("because")))
        mx = Window.partitionBy("user_id")
        parts.append(cf.withColumn("cf", F.col("cf") / F.max("cf").over(mx)))
    if trend is not None:  # 5b) xu hướng tại tỉnh của user (batch view)
        parts.append(users.join(trend.select("province_code", F.col("track_key").alias("cand"),
                                             F.col("score_norm").alias("trend")), "province_code"))
    elif catalog is not None:  # chưa có trending theo tỉnh -> dùng độ phổ biến toàn quốc
        top = catalog.orderBy(F.desc("national_score")).limit(50)
        parts.append(users.crossJoin(top.select(F.col("track_key").alias("cand"),
                                                F.col("national_score").alias("trend"))))
    if not parts:
        return
    cands = parts[0]
    for p in parts[1:]:
        cands = cands.join(p, cand_cols, "full_outer")
    for c in ("cf", "trend"):
        if c not in cands.columns:
            cands = cands.withColumn(c, F.lit(0.0))
    if "because" not in cands.columns:
        cands = cands.withColumn("because", F.lit(None).cast("string"))
    scored = (cands.withColumn("score", F.coalesce("cf", F.lit(0.0)) + TREND_WEIGHT * F.coalesce("trend", F.lit(0.0)))
              .join(seeds.select("user_id", F.col("track_key").alias("cand")), ["user_id", "cand"], "left_anti"))
    rank = Window.partitionBy("user_id").orderBy(F.desc("score"), F.asc("cand"))
    top = scored.withColumn("rank", F.row_number().over(rank)).where(F.col("rank") <= RECO_N)

    titles = hist.groupBy("track_key").agg(F.first("title").alias("because_title"))
    if catalog is not None:
        meta = catalog.select(F.col("track_key").alias("cand"), "title", "artists", "genre", "thumbnail")
        top = top.join(F.broadcast(meta), "cand", "left")
    else:
        top = (top.join(hist.select(F.col("track_key").alias("cand"), "title").dropDuplicates(["cand"]), "cand", "left")
               .withColumn("artists", F.array().cast("array<string>")).withColumn("genre", F.lit(None).cast("string"))
               .withColumn("thumbnail", F.lit(None).cast("string")))
    top = top.join(titles.withColumnRenamed("track_key", "because"), "because", "left")
    reason = (F.when(F.col("because").isNotNull() & (F.col("cf") > 0), F.concat(F.lit("Vì bạn vừa nghe: "), F.col("because_title")))
              .otherwise(F.lit("Đang thịnh hành ở tỉnh của bạn")))
    items = top.select("user_id", "province_code", F.struct(
        "rank", F.col("cand").alias("track_key"), "title", "artists", "genre", "thumbnail",
        F.round("score", 4).alias("score"), F.round("cf", 4).alias("cf"), F.round("trend", 4).alias("trend"),
        reason.alias("reason")).alias("item"))

    recent = (hist.withColumn("r", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("ts_ms"))))
              .where("r <= 5").groupBy("user_id")
              .agg(F.sort_array(F.collect_list(F.struct("r", "track_key", "title", "action"))).alias("based_on"),
                   F.count("*").alias("_n")))
    n_events = hist.groupBy("user_id").agg(F.count("*").alias("events_30m"))
    docs = (items.groupBy("user_id", "province_code").agg(F.sort_array(F.collect_list("item")).alias("tracks"))
            .join(recent.drop("_n"), "user_id", "left").join(n_events, "user_id", "left")
            .withColumn("updated_at", F.lit(now)).withColumn("batch_id", F.lit(batch_id)))
    es_write(docs, "music-recommendations", "user_id")


if "recommend" in QUERIES:
    sessions = (events_stream().withWatermark("ts", "2 minutes")
                # bước trượt 5 phút: mỗi sự kiện nằm trong 6 cửa sổ (bước 1 phút -> 30 cửa sổ, state phình x5 và
                # từng làm tràn heap khi test tải x15). Chỉ giữ trường tối thiểu trong state.
                .groupBy(F.window("ts", os.getenv("RECO_WINDOW", "30 minutes"), os.getenv("RECO_SLIDE", "5 minutes")), "user_id")
                .agg(F.collect_list(F.struct("ts_ms", "track_key", "action", "title")).alias("hist"),
                     F.last("province_code").alias("province_code")))
    started.append(sessions.writeStream.outputMode("update").queryName("recommend")
                   .foreachBatch(recommend_batch)
                   .option("checkpointLocation", CKPT + "/recommend")
                   .trigger(processingTime=os.getenv("RECO_TRIGGER", "5 seconds")).start())

print("Started streaming queries:", [q.name for q in started], flush=True)


def report_progress():
    """Ghi throughput từng query vào ES (music-metrics) để theo dõi khi test scalability."""
    for q in spark.streams.active:
        p = q.lastProgress
        if not p:
            continue
        doc = {"query": q.name, "batch_id": p.get("batchId"), "input_rows": p.get("numInputRows"),
               "input_rps": round(p.get("inputRowsPerSecond") or 0.0, 2),
               "processed_rps": round(p.get("processedRowsPerSecond") or 0.0, 2),
               "duration_ms": (p.get("durationMs") or {}).get("triggerExecution"), "ts_ms": now_ms()}
        print("PROGRESS", doc, flush=True)
        try:
            es_put("music-metrics", q.name, doc)
            es_put("music-metrics-history", f"{q.name}|{doc['batch_id']}", doc)
        except Exception as e:  # noqa: BLE001 - không để lỗi ghi metric làm dừng pipeline
            print("metrics write failed:", e, flush=True)


# 1 query lỗi -> awaitAnyTermination ném exception -> pod thoát -> k8s restart -> phục hồi từ checkpoint
while not spark.streams.awaitAnyTermination(int(os.getenv("PROGRESS_EVERY_SEC", "30"))):
    report_progress()
raise SystemExit("a streaming query stopped")
