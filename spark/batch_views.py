"""BATCH LAYER - tính lại toàn bộ view từ master dataset trên HDFS (CronJob mỗi giờ).

Đầu ra:
  HDFS views : catalog, track_alias, trending_batch            (speed layer & simulator dùng lại)
  ES         : music-tracks            catalog hợp nhất đa nền tảng
               music-trending-batch    top 50 bài thịnh hành của từng tỉnh (+ giải thích điểm, độ tin cậy)
               music-an-*              các bảng phân tích cho dashboard
               music-meta              run_id mới nhất

Điểm thịnh hành của bài t tại tỉnh p = trung bình có trọng số của các tín hiệu MÀ TỈNH p CÓ ĐỦ DỮ LIỆU:
    N(t)   : độ phổ biến toàn quốc = Σ_nền tảng max_BXH điểm hạng (Zing, Spotify, Apple, YouTube)   0.35
    G(p,t) : Google Trends - lượt tìm kiếm bài t ở tỉnh p so với bài mốc (gộp 63 tỉnh cũ theo dân số) 0.35
    C(p,t) : bình luận YouTube có nhắc tỉnh p, mỗi người viết tính 1 lần, "tự nhận ở tỉnh" = 1,
             "chỉ nhắc tên" = 0.3; làm mượt Bayes về tỉ lệ toàn quốc                                   0.15
    E(p,t) : lượt nghe 7 ngày của người nghe ở tỉnh p (giả lập + web), làm mượt tương tự                0.15
  score(p,t) = Σ w_i·s_i·a_i(p) / Σ w_i·a_i(p)   với a_i(p) = 1 nếu tỉnh p có đủ dữ liệu cho tín hiệu i
Độ tin cậy của tỉnh chỉ tính từ tín hiệu THẬT (Trends, bình luận), không tính lượt nghe giả lập.
"""
import os
import re
import time

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from jobs_common import (LAKE, es_put, es_replace, es_write, genre_udf, get_spark, mention_udf, mention_weight_col,
                         norm_udf, now_ms, old_provinces_df, path_size_bytes, provinces_df, read_raw, slug_list_udf,
                         write_view)

W_NAT = float(os.getenv("W_NATIONAL", "0.35"))
W_TRD = float(os.getenv("W_TRENDS", "0.35"))
W_CMT = float(os.getenv("W_COMMENTS", "0.15"))
W_EVT = float(os.getenv("W_EVENTS", "0.15"))
ALPHA = float(os.getenv("SMOOTH_ALPHA", "10"))      # độ mượt Bayes cho bình luận
BETA = float(os.getenv("SMOOTH_BETA", "30"))        # độ mượt Bayes cho lượt nghe
MIN_TREND_TRACKS = int(os.getenv("MIN_TREND_TRACKS", "3"))       # tỉnh có >= 3 bài có số liệu Trends
MIN_MENTION_AUTHORS = int(os.getenv("MIN_MENTION_AUTHORS", "3"))  # >= 3 người bình luận nhắc tỉnh
MIN_PLAYS = int(os.getenv("MIN_PLAYS", "20"))
TRENDS_MAX_AGE_DAYS = int(os.getenv("TRENDS_MAX_AGE_DAYS", "3"))
TOP_N = int(os.getenv("TRENDING_TOP_N", "50"))
EVENT_DAYS = int(os.getenv("EVENT_DAYS", "7"))

# Trọng số từng BXH khi tính độ phổ biến toàn quốc
CHART_WEIGHTS = {
    "zing_realtime": 1.0, "zing_week_vn": 0.8, "zing_new_release": 0.3,
    "spotify_top50_vn_daily": 1.0, "spotify_top_songs_vn_weekly": 0.7, "spotify_daily_streams_vn": 0.5,
    "apple_most_played_vn": 0.8,
    "youtube_top_songs_vn_weekly": 1.0, "youtube_top_videos_vn_weekly": 0.6,
}
# BXH có công bố hạng kỳ trước (Apple RSS và playlist Spotify thì không) -> mới tính được "mới vào BXH"
PREV_RANK_CHARTS = ["zing_realtime", "zing_week_vn", "youtube_top_songs_vn_weekly", "youtube_top_videos_vn_weekly",
                    "spotify_daily_streams_vn"]
# BXH dùng để so sánh độ tương đồng giữa các nền tảng
MAIN_CHARTS = ["zing_realtime", "spotify_top50_vn_daily", "apple_most_played_vn", "youtube_top_songs_vn_weekly"]

spark = get_spark("music-batch-views")
t0 = time.time()
RUN_TS = now_ms()
RUN_ID = time.strftime("%Y%m%d%H%M%S")
weights_df = spark.createDataFrame(list(CHART_WEIGHTS.items()), "chart_id string, chart_weight double")
prov = provinces_df(spark)
old_prov = old_provinces_df(spark)
genre_of = genre_udf()
per_prov = Window.partitionBy("province_code")

# ================================================================= 0. đọc master dataset + khử trùng lặp
charts = read_raw(spark, "charts").dropDuplicates(["record_type", "chart_id", "chart_scope", "snapshot_id", "rank"])
playlists = read_raw(spark, "playlists").dropDuplicates(["playlist_id", "snapshot_id", "position"])
comments = (read_raw(spark, "comments").where("comment_id IS NOT NULL")
            .withColumn("_r", F.row_number().over(Window.partitionBy("comment_id").orderBy(F.desc("crawled_ms"))))
            .where("_r = 1").drop("_r"))
events = (read_raw(spark, "events").where(F.col("ts_ms") >= F.lit(RUN_TS - EVENT_DAYS * 86400_000))
          .dropDuplicates(["event_id"]))
trends = (read_raw(spark, "trends").where("geo_code IS NOT NULL AND track_key IS NOT NULL")
          .where(F.col("crawled_ms") >= F.lit(RUN_TS - TRENDS_MAX_AGE_DAYS * 86400_000))
          .dropDuplicates(["snapshot_id", "group_id", "track_key", "geo_code"]))
for df in (charts, playlists, comments, events, trends):
    df.cache()

# ================================================================= 1. entity resolution + catalog
track_cols = ["track_key", "title", "artists", "genres", "source", "platform_id", "url", "thumbnail",
              "duration_s", "release_date", "crawled_ms"]
recs = (charts.where("record_type = 'chart_entry' AND track_key IS NOT NULL").select(*track_cols)
        .unionByName(playlists.where("track_key IS NOT NULL").select(*track_cols)))

# Cùng tên bài (slug) + có chung ít nhất 1 nghệ sĩ -> cùng 1 bài (ví dụ 'laviem__quang-hung-masterd'
# trên YouTube và 'laviem__tinh-ha-say-hi' trên Apple Music). Khoá được nhiều bản ghi nhất làm khoá chuẩn.
key_stats = (recs.withColumn("aslugs", slug_list_udf()("artists"))
             .groupBy("track_key").agg(F.count("*").alias("n"),
                                       F.array_distinct(F.flatten(F.collect_list("aslugs"))).alias("aslugs"))
             .withColumn("title_slug", F.split("track_key", "__").getItem(0)))


_LABEL = re.compile(r"(entertainment|music|records|media|official|channel|studio|production|vevo|-tv$|-ent$)")


def _overlap(xs, ys):
    """Có chung nghệ sĩ? So dạng liền không gạch: 'datg-music' ~ 'dat-g' (tên kênh YouTube vs tên nghệ sĩ)."""
    cx = [x.replace("-", "") for x in xs if x]
    cy = [y.replace("-", "") for y in ys if y]
    return any(len(a) >= 3 and len(b) >= 3 and (a in b or b in a) for a in cx for b in cy)


def _resolve(keys):
    keys = sorted(keys, key=lambda k: (-k["n"], k["track_key"]))
    canon, out = [], []
    for k in keys:
        target = next((c for c in canon if _overlap(c["aslugs"], k["aslugs"])), None)
        if target is None:
            canon.append(k)
        else:
            out.append((k["track_key"], target["track_key"]))
    # Video do label/kênh đăng (artist = 'ST.319 Entertainment') -> gộp vào bài duy nhất còn lại cùng tên.
    # KHÔNG gộp 2 ca sĩ khác nhau (đó là các bản cover khác nhau).
    title_len = len(keys[0]["track_key"].split("__")[0].replace("-", "")) if keys else 0
    labels = [c for c in canon if c["aslugs"] and all(_LABEL.search(a) for a in c["aslugs"])]
    others = [c for c in canon if c not in labels]
    if labels and len(others) == 1 and title_len >= 8:
        a = others[0]["track_key"]
        for lab in labels:
            b = lab["track_key"]
            out = [(x, a if y == b else y) for x, y in out] + [(b, a)]
    return out


resolve_udf = F.udf(_resolve, T.ArrayType(T.StructType([T.StructField("alias_key", T.StringType()),
                                                          T.StructField("canonical_key", T.StringType())])))
track_alias = (key_stats.groupBy("title_slug")
               .agg(F.collect_list(F.struct("n", "track_key", "aslugs")).alias("keys"))
               .where(F.size("keys") > 1)
               .select(F.explode(resolve_udf("keys")).alias("m")).select("m.*")).cache()


def canon(df):
    return (df.join(F.broadcast(track_alias), df.track_key == track_alias.alias_key, "left")
            .withColumn("track_key", F.coalesce("canonical_key", "track_key")).drop("alias_key", "canonical_key"))


charts_c, playlists_c, comments_c, events_c, trends_c = (canon(charts), canon(playlists), canon(comments),
                                                         canon(events), canon(trends))
recs_c = canon(recs)

catalog = (recs_c.groupBy("track_key").agg(
    F.max_by("title", "crawled_ms").alias("title"),
    F.max_by("artists", F.size("artists") * F.lit(10 ** 13) + F.col("crawled_ms")).alias("artists"),
    F.array_distinct(F.flatten(F.collect_list("genres"))).alias("genres"),
    F.array_sort(F.collect_set("source")).alias("sources"),
    F.collect_set(F.when(F.col("platform_id").isNotNull(), F.struct("source", "platform_id", "url"))).alias("links"),
    F.max_by("thumbnail", F.when(F.col("thumbnail").isNotNull(), F.col("crawled_ms"))).alias("thumbnail"),
    F.max("duration_s").alias("duration_s"), F.min("release_date").alias("release_date"),
    F.min("crawled_ms").alias("first_seen_ms"), F.max("crawled_ms").alias("last_seen_ms"))
    .withColumn("genre", genre_of("genres", "title", "artists")))

# ================================================================= 2. độ phổ biến toàn quốc N(t)
ce = charts_c.where("record_type = 'chart_entry' AND chart_scope = 'VN' AND rank IS NOT NULL")
latest_snap = ce.groupBy("chart_id").agg(F.max("snapshot_id").alias("snapshot_id"))
latest = ce.join(latest_snap, ["chart_id", "snapshot_id"]).join(F.broadcast(weights_df), "chart_id", "left") \
    .withColumn("chart_weight", F.coalesce("chart_weight", F.lit(0.3))) \
    .withColumn("points", F.col("chart_weight") * (F.col("chart_size") - F.col("rank") + 1) / F.col("chart_size")) \
    .withColumn("gain", F.when(F.col("previous_rank").isNotNull() & (F.col("previous_rank") > 0),
                               F.col("chart_weight") * (F.col("previous_rank") - F.col("rank")) / F.col("chart_size")))
latest.cache()
# Mỗi nền tảng chỉ tính BXH cho điểm cao nhất (Spotify có 3 BXH -> không được cộng 3 lần), rồi cộng giữa các nền tảng
per_platform = (latest.groupBy("track_key", "source").agg(F.max("points").alias("points"))
                .groupBy("track_key").agg(F.sum("points").alias("points")))
national = (latest.groupBy("track_key").agg(
    F.countDistinct("source").alias("n_platforms"),
    F.sum(F.coalesce("gain", F.lit(0.0))).alias("rank_gain"),
    F.sum(F.when(F.col("chart_id").isin(PREV_RANK_CHARTS) & (F.col("previous_rank").isNull() | (F.col("previous_rank") == 0)), 1)
          .otherwise(0)).alias("new_entries"),
    F.collect_list(F.struct("chart_id", "rank", "metric_value")).alias("chart_ranks"))
    .join(per_platform, "track_key"))
max_pts = national.agg(F.max("points")).first()[0] or 1.0
national = national.withColumn("national_score", F.col("points") / F.lit(max_pts))

# ================================================================= 3. bình luận nhắc tỉnh C(p,t)
# 'self' (tự nhận ở tỉnh) = 1.0, 'mention' (chỉ nhắc tên) = 0.3; bỏ credit/lời bài hát và tỉnh có trong tên bài
mentions = (comments_c.withColumn("m", F.explode(mention_udf()(F.col("text"), F.col("title"))))
            .select(F.col("m.province_code").alias("province_code"), F.col("m.kind").alias("kind"), "track_key",
                    "like_count", "comment_id", "text", "published_ms",
                    F.coalesce("author_hash", F.concat(F.lit("c:"), F.col("comment_id"))).alias("author"))
            .withColumn("weight", mention_weight_col())).cache()
# mỗi người viết chỉ tính 1 lần cho mỗi (tỉnh, bài) - lấy loại mạnh nhất -> chống 1 người spam nhiều bình luận
m_author = mentions.groupBy("province_code", "track_key", "author").agg(
    F.max("weight").alias("weight"), F.max(F.when(F.col("kind") == "self", 1).otherwise(0)).alias("is_self"))
m_pt = m_author.groupBy("province_code", "track_key").agg(
    F.sum("weight").alias("mentions"), F.count("*").alias("mention_authors"), F.sum("is_self").alias("self_mentions"))
m_total = m_pt.agg(F.sum("mention_authors")).first()[0] or 0

# ================================================================= 4. lượt nghe theo tỉnh E(p,t)
plays = events_c.where("action IN ('play','like')")
e_pt = plays.groupBy("province_code", "track_key").agg(
    F.sum(F.when(F.col("action") == "play", 1).otherwise(0)).alias("plays"),
    F.sum(F.when(F.col("action") == "like", 1).otherwise(0)).alias("likes"),
    F.countDistinct("user_id").alias("listeners"))

# ================================================================= 5. Google Trends theo tỉnh G(p,t)
# Mỗi bài lấy lần tra mới nhất. ratio = share(bài) / share(bài mốc) trong cùng 1 tỉnh cũ -> so sánh được giữa các bài.
tr_latest = trends_c.join(trends_c.groupBy("track_key").agg(F.max("crawled_ms").alias("crawled_ms")),
                          ["track_key", "crawled_ms"])
tr = (tr_latest.where("anchor_has_data")
      .withColumn("ratio", F.when(F.col("is_anchor"), F.lit(1.0))
                  .when(F.col("has_data"), F.col("share") / F.greatest(F.col("anchor_share").cast("double"), F.lit(0.5)))
                  .otherwise(F.lit(0.0)))
      .join(F.broadcast(old_prov), "geo_code"))
# gộp 63 tỉnh cũ -> 34 tỉnh mới, trọng số = dân số tỉnh cũ có số liệu
g_pt = (tr.groupBy("province_code", "track_key")
        .agg((F.sum(F.col("ratio") * F.col("old_population")) / F.sum("old_population")).alias("trend_ratio"))
        .where("trend_ratio > 0"))
trend_cov = (tr.select("province_code", "geo_code", "old_population").distinct()
             .groupBy("province_code").agg(F.sum("old_population").alias("_pop_data"))
             .join(prov.select("province_code", "population"), "province_code")
             .select("province_code", F.least(F.lit(1.0), F.col("_pop_data") / F.col("population")).alias("trend_coverage")))
g_mean = g_pt.groupBy("track_key").agg(F.avg("trend_ratio").alias("_g_mean"))
G = (g_pt.join(g_mean, "track_key")
     .withColumn("trend_lift", F.col("trend_ratio") / F.col("_g_mean"))
     .withColumn("trends", F.col("trend_ratio") / F.max("trend_ratio").over(per_prov))
     .drop("_g_mean"))


def smoothed_share(counts, value_col, strength, out_col):
    """(x_pt + k * s_t) / (X_p + k): tỉnh ít dữ liệu -> kéo về tỉ lệ toàn quốc s_t."""
    tot = counts.groupBy("province_code").agg(F.sum(value_col).alias("_Xp"))
    nat = counts.groupBy("track_key").agg(F.sum(value_col).alias("_xt"))
    grand = counts.agg(F.sum(value_col)).first()[0] or 0
    nat = nat.withColumn("_st", F.col("_xt") / F.lit(max(grand, 1)))
    return (universe.join(counts.select("province_code", "track_key", value_col), ["province_code", "track_key"], "left")
            .join(tot, "province_code", "left").join(nat.select("track_key", "_st"), "track_key", "left")
            .fillna(0, subset=[value_col, "_Xp", "_st"])
            .withColumn(out_col, (F.col(value_col) + strength * F.col("_st")) / (F.col("_Xp") + strength))
            .withColumn(out_col + "_lift", F.when(F.col("_st") > 0, F.col(out_col) / F.col("_st")))
            .select("province_code", "track_key", value_col, out_col, out_col + "_lift"))


# tập bài xét: có trên BXH, có số liệu Trends, hoặc được nhắc/được nghe ở bất kỳ đâu
tracks_u = (national.select("track_key").union(g_pt.select("track_key")).union(m_pt.select("track_key"))
            .union(e_pt.select("track_key")).distinct())
universe = prov.select("province_code").crossJoin(tracks_u).cache()
C = smoothed_share(m_pt, "mentions", ALPHA, "c_share")
E = smoothed_share(e_pt, "plays", BETA, "e_share")

# ---- tỉnh nào có đủ dữ liệu cho tín hiệu nào + độ tin cậy (chỉ từ tín hiệu thật)
evidence = (prov.select("province_code", "population")
            .join(g_pt.groupBy("province_code").agg(F.count("*").alias("trend_tracks")), "province_code", "left")
            .join(trend_cov, "province_code", "left")
            .join(m_author.groupBy("province_code").agg(F.countDistinct("author").alias("mention_authors_p"),
                                                          F.sum("is_self").alias("self_mentions_p")), "province_code", "left")
            .join(e_pt.groupBy("province_code").agg(F.sum("plays").alias("plays_p")), "province_code", "left")
            .fillna(0, subset=["trend_tracks", "trend_coverage", "mention_authors_p", "self_mentions_p", "plays_p"])
            .withColumn("a_trd", (F.col("trend_tracks") >= MIN_TREND_TRACKS).cast("double"))
            .withColumn("a_cmt", (F.col("mention_authors_p") >= MIN_MENTION_AUTHORS).cast("double"))
            .withColumn("a_evt", (F.col("plays_p") >= MIN_PLAYS).cast("double"))
            .withColumn("confidence", F.round(
                0.7 * F.least(F.lit(1.0), F.col("trend_tracks") / 20.0) * F.col("trend_coverage")
                + 0.3 * F.least(F.lit(1.0), F.col("mention_authors_p") / 30.0), 3))
            .withColumn("confidence_label", F.when(F.col("confidence") >= 0.6, "cao")
                        .when(F.col("confidence") >= 0.3, "trung bình").otherwise("thấp"))
            .withColumn("signals", F.array_remove(F.array(
                F.lit("toàn quốc"), F.when(F.col("a_trd") > 0, F.lit("Google Trends")).otherwise(F.lit("")),
                F.when(F.col("a_cmt") > 0, F.lit("bình luận")).otherwise(F.lit("")),
                F.when(F.col("a_evt") > 0, F.lit("lượt nghe")).otherwise(F.lit(""))), "")).localCheckpoint())

den = F.lit(W_NAT) + W_TRD * F.col("a_trd") + W_CMT * F.col("a_cmt") + W_EVT * F.col("a_evt")
scores = (universe.join(national.select("track_key", "national_score"), "track_key", "left")
          .join(G.select("province_code", "track_key", "trends", "trend_ratio", "trend_lift"),
                ["province_code", "track_key"], "left")
          .join(C, ["province_code", "track_key"], "left").join(E, ["province_code", "track_key"], "left")
          .join(m_pt.select("province_code", "track_key", "mention_authors", "self_mentions"),
                ["province_code", "track_key"], "left")
          .join(F.broadcast(evidence.select("province_code", "a_trd", "a_cmt", "a_evt")), "province_code")
          .fillna(0.0, subset=["national_score", "trends", "trend_ratio", "c_share", "e_share", "mentions", "plays",
                               "mention_authors", "self_mentions"])
          .withColumn("comment_affinity", F.col("c_share") / F.greatest(F.max("c_share").over(per_prov), F.lit(1e-9)))
          .withColumn("event_affinity", F.col("e_share") / F.greatest(F.max("e_share").over(per_prov), F.lit(1e-9)))
          # phần đóng góp của từng tín hiệu (cộng lại = score) -> dashboard hiển thị được vì sao bài đứng hạng đó
          .withColumn("c_nat", W_NAT * F.col("national_score") / den)
          .withColumn("c_trd", W_TRD * F.col("a_trd") * F.col("trends") / den)
          .withColumn("c_cmt", W_CMT * F.col("a_cmt") * F.col("comment_affinity") / den)
          .withColumn("c_evt", W_EVT * F.col("a_evt") * F.col("event_affinity") / den)
          .withColumn("score", F.col("c_nat") + F.col("c_trd") + F.col("c_cmt") + F.col("c_evt"))
          .withColumn("rank", F.row_number().over(per_prov.orderBy(F.desc("score"), F.asc("track_key"))))
          .localCheckpoint())   # cắt lineage dài (nhiều join + window) -> các bước sau lập kế hoạch nhẹ, driver đỡ tốn RAM
top = scores.where(F.col("rank") <= TOP_N).cache()

cat_small = catalog.select("track_key", "title", "artists", "genre", "thumbnail", "sources")
trending_doc = (top.join(cat_small, "track_key", "left").join(prov, "province_code")
                .join(evidence.select("province_code", "confidence", "confidence_label"), "province_code")
                .withColumn("title", F.coalesce("title", F.col("track_key")))
                .select(F.concat_ws("|", "province_code", F.col("rank").cast("string")).alias("doc_id"),
                        F.lit(RUN_ID).alias("run_id"), F.lit(RUN_TS).alias("run_ts"),
                        "province_code", "province_name", "region", "rank", "track_key", "title", "artists", "genre",
                        "thumbnail", "sources", F.round("score", 5).alias("score"),
                        F.round("national_score", 4).alias("national"), F.round("trends", 4).alias("trends"),
                        F.round("comment_affinity", 4).alias("comment_affinity"),
                        F.round("event_affinity", 4).alias("event_affinity"),
                        *[F.round(c, 5).alias(c) for c in ("c_nat", "c_trd", "c_cmt", "c_evt")],
                        F.round("trend_ratio", 4).alias("trend_ratio"), F.round("trend_lift", 3).alias("trend_lift"),
                        F.round("mentions", 2).alias("mentions"), F.col("mention_authors").cast("long").alias("mention_authors"),
                        F.col("self_mentions").cast("long").alias("self_mentions"), F.col("plays").cast("long").alias("plays"),
                        F.round("c_share_lift", 3).alias("comment_lift"), F.round("e_share_lift", 3).alias("play_lift"),
                        "confidence", "confidence_label"))

# ================================================================= 6. ghi catalog + trending
catalog_out = (catalog.join(national.select("track_key", "national_score", "n_platforms", "chart_ranks", "rank_gain"),
                            "track_key", "left")
               .fillna({"national_score": 0.0, "n_platforms": 0, "rank_gain": 0.0})
               .withColumn("search_text", norm_udf()(F.concat_ws(" ", F.col("title"), F.array_join("artists", " ")))))
catalog_out.cache()
write_view(spark, catalog_out.select("track_key", "title", "artists", "genre", "thumbnail", "national_score"),
           "catalog", RUN_ID)
write_view(spark, track_alias, "track_alias", RUN_ID)
write_view(spark, top.select("province_code", "track_key", "score",
                             (F.col("score") / F.max("score").over(per_prov)).alias("score_norm")),
           "trending_batch", RUN_ID)

es_write(catalog_out.withColumn("run_id", F.lit(RUN_ID)), "music-tracks", "track_key")
es_replace(trending_doc, "music-trending-batch", "doc_id", RUN_ID)

# ================================================================= 7. các bảng phân tích (dashboard)
ev_meta = events_c.join(prov.select("province_code", "region"), "province_code", "left") \
    .withColumn("ts", (F.col("ts_ms") / 1000).cast("timestamp"))

# 7a. cơ cấu thể loại theo tỉnh (lượt nghe) + theo bình luận
genre_play = ev_meta.where("action = 'play'").groupBy("province_code", "genre").agg(F.count("*").alias("plays"))
genre_ment = m_author.join(catalog.select("track_key", "genre"), "track_key").groupBy("province_code", "genre") \
    .agg(F.sum("weight").alias("mentions"))
gp = (genre_play.join(genre_ment, ["province_code", "genre"], "full_outer").fillna(0, subset=["plays", "mentions"])
      .withColumn("play_share", F.col("plays") / F.greatest(F.sum("plays").over(per_prov), F.lit(1)))
      .withColumn("mention_share", F.col("mentions") / F.greatest(F.sum("mentions").over(per_prov), F.lit(1)))
      .join(prov.select("province_code", "province_name", "region"), "province_code")
      .withColumn("doc_id", F.concat_ws("|", "province_code", "genre")).withColumn("run_id", F.lit(RUN_ID)))
es_replace(gp, "music-an-genre-province", "doc_id", RUN_ID)

# 7b. thói quen nghe theo giờ trong ngày x thứ trong tuần x vùng
hourly = (ev_meta.where("action IN ('play','skip')")
          .groupBy("region", F.dayofweek("ts").alias("dow"), F.hour("ts").alias("hour"))
          .agg(F.count("*").alias("listens"), F.avg(F.when(F.col("action") == "skip", 1.0).otherwise(0.0)).alias("skip_rate"))
          .withColumn("doc_id", F.concat_ws("|", "region", "dow", "hour")).withColumn("run_id", F.lit(RUN_ID)))
es_replace(hourly, "music-an-hourly", "doc_id", RUN_ID)

# 7c. độ tương đồng BXH giữa các nền tảng: số bài chung, Jaccard, tương quan hạng Spearman
main = latest.where(F.col("chart_id").isin(MAIN_CHARTS)).groupBy("chart_id", "track_key").agg(F.min("rank").alias("rank"))
a, b = main.alias("a"), main.alias("b")
pairs = a.join(b, (F.col("a.track_key") == F.col("b.track_key")) & (F.col("a.chart_id") < F.col("b.chart_id"))) \
    .select(F.col("a.chart_id").alias("chart_a"), F.col("b.chart_id").alias("chart_b"),
            F.col("a.rank").alias("ra"), F.col("b.rank").alias("rb"))
wa = Window.partitionBy("chart_a", "chart_b")
pairs = pairs.withColumn("ra2", F.row_number().over(wa.orderBy("ra"))).withColumn("rb2", F.row_number().over(wa.orderBy("rb")))
sizes = main.groupBy("chart_id").agg(F.count("*").alias("n"))
overlap = (pairs.groupBy("chart_a", "chart_b").agg(F.count("*").alias("common"),
                                                   F.sum(F.pow(F.col("ra2") - F.col("rb2"), 2)).alias("d2"))
           .join(sizes.withColumnRenamed("chart_id", "chart_a").withColumnRenamed("n", "na"), "chart_a")
           .join(sizes.withColumnRenamed("chart_id", "chart_b").withColumnRenamed("n", "nb"), "chart_b")
           .withColumn("jaccard", F.col("common") / (F.col("na") + F.col("nb") - F.col("common")))
           .withColumn("spearman", F.when(F.col("common") >= 3, 1 - 6 * F.col("d2") /
                                          (F.col("common") * (F.pow(F.col("common"), 2) - 1))))
           .withColumn("doc_id", F.concat_ws("|", "chart_a", "chart_b")).withColumn("run_id", F.lit(RUN_ID)))
es_replace(overlap, "music-an-platform-overlap", "doc_id", RUN_ID)

# 7d. nghệ sĩ nổi bật: toàn quốc (điểm BXH) và theo vùng (lượt nghe)
art_nat = (latest.withColumn("artist", F.explode("artists")).groupBy("artist")
           .agg(F.sum("points").alias("points"), F.countDistinct("track_key").alias("tracks"),
                F.countDistinct("source").alias("platforms"))
           .withColumn("scope", F.lit("VN")))
art_reg = (ev_meta.where("action = 'play'").withColumn("artist", F.explode("artists")).groupBy("region", "artist")
           .agg(F.count("*").alias("points"), F.countDistinct("track_key").alias("tracks"))
           .withColumn("platforms", F.lit(None).cast("long")).withColumnRenamed("region", "scope"))
art_rank = Window.partitionBy("scope").orderBy(F.desc("points"))
artists = (art_nat.unionByName(art_reg.select(art_nat.columns)).withColumn("rank", F.row_number().over(art_rank))
           .where("rank <= 30").withColumn("doc_id", F.concat_ws("|", "scope", "artist")).withColumn("run_id", F.lit(RUN_ID)))
es_replace(artists, "music-an-artists", "doc_id", RUN_ID)

# 7e. bài đang lên hạng nhanh nhất (theo thay đổi hạng mà nền tảng công bố)
rising = (national.where("rank_gain > 0 OR new_entries > 0").join(cat_small, "track_key", "left")
          .withColumn("momentum", F.col("rank_gain") + 0.05 * F.col("new_entries"))
          .orderBy(F.desc("momentum")).limit(40)
          .select("track_key", "title", "artists", "genre", "thumbnail", F.round("momentum", 4).alias("momentum"),
                  "rank_gain", "new_entries", "n_platforms", F.round("national_score", 4).alias("national_score"),
                  "chart_ranks").withColumn("run_id", F.lit(RUN_ID)))
es_replace(rising, "music-an-rising", "track_key", RUN_ID)

# 7f. lịch sử hạng của top 10 mỗi BXH chính
top10 = latest.where((F.col("rank") <= 10) & F.col("chart_id").isin(MAIN_CHARTS)).select("chart_id", "track_key").distinct()
history = (ce.join(top10, ["chart_id", "track_key"]).groupBy("chart_id", "track_key", "snapshot_id")
           .agg(F.min("rank").alias("rank"), F.max("crawled_ms").alias("crawled_ms"), F.first("title").alias("title"))
           .withColumn("doc_id", F.concat_ws("|", "chart_id", "track_key", "snapshot_id")).withColumn("run_id", F.lit(RUN_ID)))
es_replace(history, "music-an-chart-history", "doc_id", RUN_ID)


# 7g. các nguồn tín hiệu có đồng thuận không? Spearman giữa xếp hạng theo Trends và theo bình luận / toàn quốc
def spearman_by_province(df, col_a, col_b, prefix, min_n=5):
    w = Window.partitionBy("province_code")
    ranked = (df.withColumn("_ra", F.row_number().over(w.orderBy(F.desc(col_a), "track_key")))
              .withColumn("_rb", F.row_number().over(w.orderBy(F.desc(col_b), "track_key"))))
    return (ranked.groupBy("province_code")
            .agg(F.count("*").alias(f"n_{prefix}"), F.sum(F.pow(F.col("_ra") - F.col("_rb"), 2)).alias("_d2"))
            .withColumn(f"rho_{prefix}", F.when(F.col(f"n_{prefix}") >= min_n, F.round(
                1 - 6 * F.col("_d2") / (F.col(f"n_{prefix}") * (F.pow(F.col(f"n_{prefix}"), 2) - 1)), 3)))
            .drop("_d2"))


both_tc = g_pt.join(m_pt, ["province_code", "track_key"])                       # bài có cả 2 loại quan sát thật
both_tn = g_pt.join(national.select("track_key", "national_score"), "track_key")
agreement = (prov.select("province_code", "province_name", "region")
             .join(spearman_by_province(both_tc, "trend_ratio", "mentions", "tc"), "province_code", "left")
             .join(spearman_by_province(both_tn, "trend_ratio", "national_score", "tn"), "province_code", "left")
             .fillna(0, subset=["n_tc", "n_tn"]).withColumn("run_id", F.lit(RUN_ID)))
es_replace(agreement, "music-an-signal-agreement", "province_code", RUN_ID)

# 7h. hồ sơ từng tỉnh: dữ liệu có được, độ tin cậy, thể loại chính, bài số 1, "bài đặc trưng" (lift cao nhất)
top_genre = (gp.withColumn("r", F.row_number().over(per_prov.orderBy(F.desc("plays"), F.desc("mentions"))))
             .where("r = 1").select("province_code", F.col("genre").alias("top_genre")))
number_one = trending_doc.where("rank = 1").select("province_code", F.col("title").alias("top_title"),
                                                   F.col("artists").alias("top_artists"), F.col("track_key").alias("top_track"))
# bài đặc trưng: được tỉnh quan tâm nhiều hơn hẳn mức trung bình - CHỈ dùng tín hiệu thật (Trends, bình luận),
# không dùng lượt nghe giả lập. Tỉnh không đủ dữ liệu thật -> để trống thay vì đoán.
fav = (scores.withColumn("lift", F.greatest(
            F.when(F.col("trend_ratio") > 0, F.coalesce("trend_lift", F.lit(0.0))).otherwise(F.lit(0.0)),
            # lift từ bình luận co về 1 khi ít người viết (2 người nói chưa chắc bằng Trends của cả tỉnh)
            F.when((F.col("mention_authors") >= 2) & (F.col("self_mentions") >= 1),
                   1 + (F.coalesce("c_share_lift", F.lit(1.0)) - 1) * F.least(F.lit(1.0), F.col("mention_authors") / 5.0))
             .otherwise(F.lit(0.0))))
       .where("lift > 1.3")
       .withColumn("r", F.row_number().over(per_prov.orderBy(F.desc("lift"), "track_key"))).where("r = 1")
       .join(cat_small.select("track_key", "title"), "track_key", "left")
       .select("province_code", F.col("track_key").alias("fav_track"), F.col("title").alias("fav_title"),
               F.round("lift", 2).alias("fav_lift")))
m_prov = m_pt.groupBy("province_code").agg(F.round(F.sum("mentions"), 1).alias("mentions"),
                                           F.countDistinct("track_key").alias("mentioned_tracks"))
e_prov = ev_meta.where("action = 'play'").groupBy("province_code").agg(
    F.count("*").alias("plays_7d"), F.countDistinct("user_id").alias("listeners_7d"),
    F.countDistinct("track_key").alias("distinct_tracks_7d"))
summary = (prov.join(m_prov, "province_code", "left").join(e_prov, "province_code", "left")
           .join(top_genre, "province_code", "left").join(number_one, "province_code", "left")
           .join(fav, "province_code", "left")
           .join(evidence.select("province_code", "trend_tracks", F.round("trend_coverage", 3).alias("trend_coverage"),
                                 F.col("mention_authors_p").alias("mention_authors"),
                                 F.col("self_mentions_p").alias("self_mentions"),
                                 "confidence", "confidence_label", "signals"), "province_code", "left")
           .fillna(0, subset=["mentions", "mentioned_tracks", "plays_7d", "listeners_7d", "distinct_tracks_7d"])
           .withColumn("run_id", F.lit(RUN_ID)))
es_replace(summary, "music-an-province-summary", "province_code", RUN_ID)

# 7i. KPI hệ thống + độ phủ dữ liệu theo nguồn
src = (charts.where("record_type = 'chart_entry'").groupBy("source")
       .agg(F.count("*").alias("records"), F.countDistinct("snapshot_id").alias("snapshots"),
            F.max("crawled_ms").alias("last_crawl_ms")))
src_rows = [r.asDict() for r in src.collect()]
tr_row = trends.agg(F.count("*").alias("n"), F.countDistinct("track_key").alias("tracks"),
                    F.max("crawled_ms").alias("last")).first()
ev_row = evidence.agg(F.sum(F.when(F.col("confidence_label") == "cao", 1).otherwise(0)).alias("hi"),
                      F.sum(F.when(F.col("confidence_label") == "trung bình", 1).otherwise(0)).alias("mid"),
                      F.sum("self_mentions_p").alias("self")).first()
if tr_row["n"]:
    src_rows.append({"source": "google_trends", "records": tr_row["n"], "snapshots": tr_row["tracks"],
                     "last_crawl_ms": tr_row["last"]})
kpi = {
    "run_id": RUN_ID, "run_ts": RUN_TS,
    "catalog_tracks": catalog.count(), "aliases_merged": track_alias.count(),
    "chart_records": charts.count(), "playlist_records": playlists.count(),
    "comments_unique": comments.count(), "province_mentions": int(m_total), "self_mentions": int(ev_row["self"] or 0),
    "trends_records": int(tr_row["n"] or 0), "trends_tracks": int(tr_row["tracks"] or 0),
    "provinces_high_confidence": int(ev_row["hi"] or 0), "provinces_mid_confidence": int(ev_row["mid"] or 0),
    "events_window": events.count(), "event_days": EVENT_DAYS,
    "lake_bytes": path_size_bytes(spark, LAKE), "sources": src_rows,
    "weights": {"national": W_NAT, "trends": W_TRD, "comments": W_CMT, "events": W_EVT},
    "duration_sec": round(time.time() - t0, 1),
}
es_put("music-an-kpis", "latest", kpi)
es_put("music-meta", "batch_views", {"run_id": RUN_ID, "run_ts": RUN_TS, "duration_sec": kpi["duration_sec"]})
print("BATCH VIEWS DONE", kpi, flush=True)
spark.stop()
