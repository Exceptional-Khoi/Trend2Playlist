"""BATCH LAYER - độ tương đồng giữa các bài hát (item-item co-occurrence), CronJob mỗi 3 giờ.

"Giỏ" (basket) = tập bài xuất hiện cùng nhau:
  - playlist thật đã crawl (Zing Top 100 theo thể loại, Spotify editorial)      trọng số 1.0
  - snapshot BXH mới nhất của từng nền tảng                                     trọng số 0.3
  - phiên nghe của 1 người (cùng user, cùng khối 3 giờ, 14 ngày gần nhất)       trọng số 1.0
sim(a,b) = Σ w(giỏ chứa cả a,b) / (sqrt(n_a * n_b) + SHRINK)  (cosine có co), chỉ giữ cặp gặp nhau ≥ 2 giỏ,
          + thưởng nếu cùng nghệ sĩ chính.
Không phải mô hình ML - chỉ là đếm đồng xuất hiện phân tán bằng Spark (self-join + group by).

Đầu ra: HDFS view item_sim (speed layer dùng cho gợi ý realtime) + ES music-item-sim.
"""
import os
import time

from pyspark.sql import Window
from pyspark.sql import functions as F

from jobs_common import es_put, es_write, get_spark, now_ms, read_raw, read_view, write_view

TOP_K = int(os.getenv("SIM_TOP_K", "30"))
MAX_BASKET = int(os.getenv("SIM_MAX_BASKET", "40"))
SESSION_DAYS = int(os.getenv("SIM_SESSION_DAYS", "14"))
ARTIST_BONUS = float(os.getenv("SIM_ARTIST_BONUS", "0.15"))
MIN_SUPPORT = int(os.getenv("SIM_MIN_SUPPORT", "2"))
SHRINK = float(os.getenv("SIM_SHRINK", "3"))

spark = get_spark("music-batch-similarity")
t0 = time.time()
RUN_TS = now_ms()
RUN_ID = time.strftime("%Y%m%d%H%M%S")

alias = read_view(spark, "track_alias")
catalog = read_view(spark, "catalog")


def canon(df):
    if alias is None:
        return df
    return (df.join(F.broadcast(alias), df.track_key == alias.alias_key, "left")
            .withColumn("track_key", F.coalesce("canonical_key", "track_key")).drop("alias_key", "canonical_key"))


# ---------------------------------------------------------------- giỏ từ playlist & BXH
pl = canon(read_raw(spark, "playlists").where("track_key IS NOT NULL"))
pl_latest = pl.join(pl.groupBy("playlist_id").agg(F.max("snapshot_id").alias("snapshot_id")), ["playlist_id", "snapshot_id"])
b_playlist = pl_latest.select(F.concat(F.lit("pl:"), "playlist_id").alias("basket"), "track_key", "position") \
    .withColumn("w", F.lit(1.0))

ch = canon(read_raw(spark, "charts").where("record_type = 'chart_entry' AND track_key IS NOT NULL AND chart_scope = 'VN'"))
ch_latest = ch.join(ch.groupBy("chart_id").agg(F.max("snapshot_id").alias("snapshot_id")), ["chart_id", "snapshot_id"])
b_chart = ch_latest.select(F.concat(F.lit("ch:"), "chart_id").alias("basket"), "track_key", F.col("rank").alias("position")) \
    .withColumn("w", F.lit(0.3))

# ---------------------------------------------------------------- giỏ từ phiên nghe
ev = canon(read_raw(spark, "events").where(F.col("ts_ms") >= F.lit(RUN_TS - SESSION_DAYS * 86400_000))
           .where("action IN ('play','like')").dropDuplicates(["event_id"]))
b_session = (ev.withColumn("basket", F.concat_ws("|", F.lit("s"), "user_id",
                                                 (F.col("ts_ms") / F.lit(3 * 3600_000)).cast("long")))
             .groupBy("basket", "track_key").agg(F.min("ts_ms").alias("position"))
             .withColumn("w", F.lit(1.0)))

# giới hạn kích thước giỏ để số cặp không bùng nổ (n^2)
baskets = b_playlist.unionByName(b_chart).unionByName(b_session.select(b_playlist.columns)) \
    .dropDuplicates(["basket", "track_key"]) \
    .withColumn("_r", F.row_number().over(Window.partitionBy("basket").orderBy("position"))) \
    .where(F.col("_r") <= MAX_BASKET).drop("_r", "position").cache()

n = baskets.groupBy("track_key").agg(F.sum("w").alias("n"))
a, b = baskets.alias("a"), baskets.alias("b")
cooc = (a.join(b, (F.col("a.basket") == F.col("b.basket")) & (F.col("a.track_key") < F.col("b.track_key")))
        .groupBy(F.col("a.track_key").alias("ka"), F.col("b.track_key").alias("kb"))
        .agg(F.sum("a.w").alias("co"), F.count("*").alias("baskets")))
# Cặp chỉ gặp nhau trong 1 giỏ là nhiễu -> bỏ; SHRINK kéo điểm của cặp ít dữ liệu về 0
sim = (cooc.where(F.col("baskets") >= MIN_SUPPORT)
       .join(n.withColumnRenamed("track_key", "ka").withColumnRenamed("n", "na"), "ka")
       .join(n.withColumnRenamed("track_key", "kb").withColumnRenamed("n", "nb"), "kb")
       .withColumn("sim", F.col("co") / (F.sqrt(F.col("na") * F.col("nb")) + F.lit(SHRINK))))

# thưởng cùng nghệ sĩ chính (từ catalog)
if catalog is not None:
    prim = catalog.select("track_key", F.col("artists").getItem(0).alias("artist")).where("artist IS NOT NULL")
    same = (prim.alias("x").join(prim.alias("y"), (F.col("x.artist") == F.col("y.artist")) & (F.col("x.track_key") < F.col("y.track_key")))
            .select(F.col("x.track_key").alias("ka"), F.col("y.track_key").alias("kb")).withColumn("bonus", F.lit(ARTIST_BONUS)))
    sim = (sim.select("ka", "kb", "sim", "baskets").join(same, ["ka", "kb"], "full_outer")
           .fillna(0.0, subset=["sim", "bonus"]).fillna(0, subset=["baskets"])
           .withColumn("sim", F.col("sim") + F.col("bonus")))

both = (sim.select(F.col("ka").alias("track_key"), F.col("kb").alias("neighbor_key"), "sim", "baskets")
        .unionByName(sim.select(F.col("kb").alias("track_key"), F.col("ka").alias("neighbor_key"), "sim", "baskets")))
top = (both.withColumn("r", F.row_number().over(Window.partitionBy("track_key").orderBy(F.desc("sim"), "neighbor_key")))
       .where(F.col("r") <= TOP_K).cache())

write_view(spark, top.select("track_key", "neighbor_key", "sim"), "item_sim", RUN_ID)

docs = top
if catalog is not None:
    docs = docs.join(catalog.select(F.col("track_key").alias("neighbor_key"), F.col("title").alias("neighbor_title"),
                                    F.col("artists").alias("neighbor_artists")), "neighbor_key", "left")
else:
    docs = docs.withColumn("neighbor_title", F.col("neighbor_key")).withColumn("neighbor_artists", F.array())
docs = (docs.groupBy("track_key").agg(F.sort_array(F.collect_list(F.struct(
    F.col("r").alias("rank"), "neighbor_key", "neighbor_title", "neighbor_artists",
    F.round("sim", 4).alias("sim"), "baskets"))).alias("neighbors"))
    .withColumn("run_id", F.lit(RUN_ID)))
es_write(docs, "music-item-sim", "track_key")

stats = {"run_id": RUN_ID, "run_ts": RUN_TS, "baskets": baskets.select("basket").distinct().count(),
         "pairs": top.count(), "tracks": top.select("track_key").distinct().count(),
         "duration_sec": round(time.time() - t0, 1)}
es_put("music-meta", "batch_similarity", stats)
print("BATCH SIMILARITY DONE", stats, flush=True)
spark.stop()
