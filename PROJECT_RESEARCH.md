# Research notes for the clarified project

## 1. Data-source roles

| Source | What to collect | Granularity | Primary use |
|---|---|---|---|
| Zing MP3 | chart rank, song/artist metadata, snapshot time | Vietnam | VN-platform trend & cross-platform comparison |
| Spotify Charts | chart rank/streams where export exposes them | global/country/city depending chart | cross-platform trend |
| Spotify Web API | consented user's recent/top items | user | optional live demo/user context; not a public trend corpus |
| Apple Music | Top Songs chart, genre, ISRC, metadata | storefront/city charts | trend + entity matching |
| YouTube | popular music videos, views/likes/comments, comment text | country | engagement + trend + text signal |
| Google Trends | search interest over time and subregions | province/subregion | province interest estimate |
| Own system | play/like/skip/session/province | event | realtime trend + personalized playlist |

## 2. Province estimate

Do not infer commenter province from YouTube API; it does not expose reliable commenter geolocation. Use:

`province_interest(track,p,t) = a*norm(Trends) + b*norm(comment-text province signal) + c*norm(own-system events)`

Keep `a,b,c` configurable and show a confidence/coverage value. Comments should be a weak auxiliary signal.

## 3. Why Lambda is a better fit than the old Kappa proposal

The clarified system explicitly has both: (a) historical multi-platform snapshots that must be replayed/recomputed in batch, and (b) play/like/skip events that must update trends/recommendations in seconds. A Lambda-style split is easy to justify pedagogically:

- Batch layer: Spark over long-term raw history; dedup, entity resolution, cross-platform analytics, model training.
- Speed layer: Kafka + Flink/Spark Streaming; rolling trend counters and session features.
- Serving layer: ClickHouse for dashboard analytics; Redis/feature store for low-latency recommendation state; object store + Iceberg for long-term history.

Use one canonical schema and shared feature definitions to minimize duplicated logic.

## 4. Recommendation algorithm

Use a two-stage hybrid recommender:

1. **Candidate generation (batch)**: implicit-feedback ALS using ONLY your system's user events (play/like/skip), optionally item-item co-occurrence as a simpler baseline.
2. **Realtime reranking**: combine user affinity with `national_trend`, `province_interest`, `rank_velocity`, `cross_platform_consensus`, and short-session context.

Example baseline:

`score = 0.50*personal + 0.18*national_trend + 0.12*province_interest + 0.10*rank_velocity + 0.07*cross_platform + 0.03*session_recency`

Weights are experiment parameters, not claims. Evaluate Recall@K / NDCG@K with time-based train/test split and also report recommendation latency.

## 5. Entity resolution

Normalize each platform item into a `canonical_track_id`. Match by ISRC first (Apple Music is useful here), then normalized title+artist, then fuzzy fallback. Keep source-specific IDs. This is essential for comparing a track's rank across platforms.
