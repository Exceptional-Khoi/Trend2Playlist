# Kiến trúc chi tiết

## 1. Ánh xạ với sơ đồ Lambda trong đề bài

| Khối trong đề | Hiện thực | File |
|---|---|---|
| Data source → Data collecting (Flume/Kafka) | Crawler Python (CronJob) + bộ giả lập → **Kafka 3 broker** | `crawler/`, `k8s/50,51`, `k8s/10` |
| Batch layer: HDFS (All data) | Spark Streaming query `ingest` ghi mọi topic xuống **HDFS** dạng parquet, phân vùng `topic/dt` | `spark/speed_layer.py`, `k8s/20` |
| Batch layer: precompute views | **Spark batch** (thay MapReduce), mỗi giờ tính lại từ toàn bộ master dataset | `spark/batch_views.py`, `spark/batch_similarity.py` |
| Speed layer: Spark real-time | **Spark Structured Streaming** đọc Kafka, cập nhật tăng dần | `spark/speed_layer.py` |
| Serving layer (Elasticsearch) | batch view + realtime view trong **Elasticsearch**; API gộp lúc truy vấn | `serving/api.py` |
| Data query / Analytics / Visualization | REST API + dashboard (ECharts, Leaflet); Kibana tuỳ chọn | `serving/static/`, `k8s/optional/kibana.yaml` |
| Decision feedback loop | Batch view (item similarity, xu hướng tỉnh) được speed layer và bộ giả lập đọc lại → thay đổi gợi ý | `cached_view()` trong `speed_layer.py` |

## 2. Luồng dữ liệu

```
Zing / Spotify / Apple / YouTube ──HTTP──> crawler pod ──produce (acks=all)──> Kafka
                                                                                │
   ┌──────────────────── Spark Structured Streaming (speed layer, 1 driver) ────┤
   │  ingest    : mọi topic ──────────────────────────────> HDFS /datalake/raw (parquet, exactly-once file sink)
   │  plays     : events ─ window 1 phút × tỉnh × bài ─────> ES music-rt-plays       (update mode, id cố định)
   │  mentions  : comments ─ UDF nhận diện tỉnh ─ explode ─> ES music-rt-mentions    (id = comment|tỉnh)
   │  charts    : charts ─ snapshot mới nhất ──────────────> ES music-rt-charts      (id = chart|hạng)
   │  recommend : events ─ cửa sổ trượt 30'/5' mỗi user ──┬> ES music-recommendations (id = user)
   │                                  item_sim, trending ─┘ (đọc từ HDFS view, làm mới 5 phút/lần)
   └────────────────────────────────────────────────────────────────────────────
HDFS /datalake/raw ── Spark batch mỗi giờ ──> HDFS /views/{catalog,track_alias,trending_batch,item_sim}
                                          └─> ES music-tracks, music-trending-batch, music-an-*, music-item-sim
ES ──> FastAPI: trending = 0.6·batch + 0.3·realtime plays + 0.1·realtime mentions ──> dashboard
```

## 3. Lưu trữ

**HDFS**
```
/datalake/raw/topic=music.charts/dt=2026-09-27/part-*.parquet     (topic, key, value JSON, kafka_ts, partition, offset)
/datalake/raw/_spark_metadata/                                     log commit của streaming sink (exactly-once)
/views/<view>/run_id=<yyyyMMddHHmmss>/                             batch view, giữ 3 phiên bản
/views/_latest/<view>/                                             con trỏ tới phiên bản mới nhất
/checkpoints/{ingest,plays,mentions,charts,recommend}/             offset Kafka + state store
```
Giữ JSON gốc trong master dataset: batch layer có thể parse lại với logic mới (ví dụ sửa bộ chuẩn hoá tên bài) mà không cần crawl lại.

**Elasticsearch**: một index template cho `music-*`, chuỗi mặc định kiểu `keyword` để aggregation, các trường `*_ms` kiểu `date`.

| Index | Nguồn | Doc id | Nội dung |
|---|---|---|---|
| `music-rt-plays` | speed | tỉnh\|bài\|phút | plays, skips, likes, listeners theo phút |
| `music-rt-mentions` | speed | comment\|tỉnh | bình luận nhắc tỉnh (đoạn trích 300 ký tự) |
| `music-rt-charts` | speed | chart\|scope\|hạng | snapshot BXH mới nhất |
| `music-recommendations` | speed | user | top 20 bài + lý do + 5 bài gần nhất |
| `music-metrics(-history)` | speed | query(\|batch) | throughput từng streaming query |
| `music-tracks` | batch | track_key | catalog hợp nhất đa nền tảng, national_score |
| `music-trending-batch` | batch | tỉnh\|hạng | top 50 mỗi tỉnh + các thành phần điểm |
| `music-an-*` | batch | tuỳ bảng | genre-province, hourly, platform-overlap, artists, rising, chart-history, province-summary (có độ tin cậy), signal-agreement, kpis |
| `music-item-sim` | batch | track_key | 30 bài tương tự nhất |
| `music-meta` | batch | tên job | run_id, thời gian chạy |

## 4. Công thức

### Độ phổ biến toàn quốc
`points(t, chart) = w_chart · (N − rank + 1)/N`; mỗi nền tảng lấy BXH cho điểm cao nhất (Spotify có 3 BXH, không cộng 3 lần);
`N(t) = Σ_nền tảng max points`, chia cho giá trị lớn nhất để về [0, 1].

### Google Trends theo tỉnh G(p,t)
Crawler (`crawler/src_trends.py`, CronJob mỗi ngày) tra lượt **tìm trên YouTube** trong 7 ngày cho 40 bài hot, mỗi lần
5 từ khoá = 1 bài mốc + 4 bài. Google trả về cho từng tỉnh cũ `o` tỉ lệ % lượt tìm của mỗi từ khoá trong nhóm.
```
ratio(o,t)  = share(o,t) / share(o, mốc)                       (so được giữa mọi nhóm vì cùng 1 bài mốc)
r(p,t)      = Σ_{o∈p} dân_số(o)·ratio(o,t) / Σ_{o∈p} dân_số(o)  (gộp 63 tỉnh cũ -> 34 tỉnh mới, chỉ tỉnh cũ có số liệu)
G(p,t)      = r(p,t) / max_t r(p,t)                              (chuẩn hoá trong tỉnh)
lift_T(p,t) = r(p,t) / trung bình_p r(p,t)                       (tỉnh nào quan tâm bài này hơn hẳn các tỉnh khác)
```
- Từ khoá cùng kiểu "tên bài + nghệ sĩ"; tên bài ≥ 5 từ thì không thêm nghệ sĩ.
- Bài mốc = bài có số liệu ở nhiều tỉnh nhất trong 10 bài đầu. Nếu chọn theo hạng BXH, bài mốc có thể chỉ có số liệu ở 3–5/63 tỉnh.
- Tỉnh cũ không đủ lượt tìm cho bài mốc thì bị bỏ. `trend_coverage(p)` = tỉ lệ dân số tỉnh mới có số liệu.

### Bình luận nhắc tỉnh C(p,t)
- `find_province_mentions(text, title)` trả về loại:
  - `self` (tự nhận ở tỉnh), trọng số 1: có từ "quê/người/dân…" trước tên tỉnh, hoặc đại từ ngôi thứ nhất/"ai" + "ở/từ/tại",
    hoặc "điểm danh/đâu rồi/nè…" ngay sau.
  - `mention` (chỉ nhắc tên), trọng số 0,3.
- Bỏ bình luận dạng credit/lời bài hát (≥ 5 dòng, hoặc có từ "đạo diễn", "location", "credit"…) và tên tỉnh có trong tên bài.
- Mỗi `author_hash` (SHA-256 có muối của ID kênh) chỉ tính 1 lần cho mỗi (tỉnh, bài), lấy loại mạnh nhất.

### Ái lực vùng miền (làm mượt Bayes)
Với `x_pt` là số bình luận (hoặc lượt nghe) của tỉnh p cho bài t, `X_p = Σ_t x_pt`, `s_t = x_t / Σ x`:

```
share(p,t) = (x_pt + k · s_t) / (X_p + k)       k = 10 (bình luận), 30 (lượt nghe)
lift(p,t)  = share(p,t) / s_t                   > 1: tỉnh thích bài này hơn mức cả nước
```
Tỉnh ít dữ liệu thì `share` gần bằng tỉ lệ cả nước, không bị một bình luận lẻ đẩy lên đầu.

### Điểm thịnh hành
```
score(p,t) = Σ wᵢ·aᵢ(p)·sᵢ(p,t) / Σ wᵢ·aᵢ(p)       s ∈ {N, G, C, E},  w = 0.35 / 0.35 / 0.15 / 0.15
aᵢ(p) = 1 nếu tỉnh p đủ dữ liệu:  Trends ≥ 3 bài | bình luận ≥ 3 người | lượt nghe ≥ 20   (toàn quốc luôn = 1)
```
Mỗi tín hiệu được lưu phần đóng góp `c_nat, c_trd, c_cmt, c_evt` (cộng lại bằng score), nên dashboard vẽ được thanh giải thích.
Lúc truy vấn, API gộp với realtime: `0.6·batch_norm + 0.3·plays_60'_norm + 0.1·mentions_7d_norm`.

### Độ tin cậy của tỉnh (chỉ từ dữ liệu thật)
```
confidence(p) = 0.7 · min(1, số bài có Trends / 20) · trend_coverage(p) + 0.3 · min(1, số người bình luận / 30)
nhãn: ≥ 0,6 "cao" · ≥ 0,3 "trung bình" · còn lại "thấp" (xếp hạng chủ yếu dựa trên toàn quốc)
```

### Kiểm chứng chéo (`music-an-signal-agreement`)
Trong mỗi tỉnh, tương quan hạng Spearman giữa xếp hạng theo Trends và theo bình luận (bài có cả 2 quan sát), và giữa Trends và
độ phổ biến toàn quốc. Chỉ tính khi có ≥ 5 bài chung.

### Độ tương đồng bài hát (batch_similarity)
Giỏ = playlist thật (trọng số 1), snapshot BXH (0,3), phiên nghe user × khối 3 giờ (1).
`sim(a,b) = Σ w(giỏ chứa a,b) / (sqrt(n_a·n_b) + 3)`, chỉ giữ cặp xuất hiện ở ≥ 2 giỏ, cộng 0,15 nếu cùng nghệ sĩ chính; giữ 30 hàng xóm/bài.

### Gợi ý realtime (speed layer)
1. Cửa sổ trượt 30 phút, bước 5 phút, theo `user_id` → `collect_list(ts, bài, hành vi)` (state trong Spark, watermark 2 phút).
2. Mỗi micro-batch (5 giây) lấy cửa sổ đầy đủ nhất của các user vừa có sự kiện.
3. Trọng số hạt giống: thích 2, nghe hết 1, bỏ qua −0,7, nhân `exp(−Δt/15 phút)`.
4. Ứng viên = hàng xóm của hạt giống: `cf = Σ w·sim`, chuẩn hoá theo user; cộng `0,35 · xu hướng tại tỉnh của user` (batch view).
5. Loại bài vừa nghe, lấy top 20, gắn lý do (hạt giống đóng góp nhiều nhất), ghi ES theo `user_id`.

### State của Spark Streaming
- Bước trượt 5 phút (không phải 1 phút): mỗi sự kiện nằm trong 6 cửa sổ thay vì 30. Trong state chỉ giữ `(ts, bài, hành vi, tên bài)`.
  Bản cũ (bước 1 phút, giữ cả nghệ sĩ/thể loại) đã **tràn heap** khi test tải ×15 (150 sự kiện/giây).
- `STATE_STORE=rocksdb` (ConfigMap) đưa state ra khỏi heap (RocksDB, tràn xuống đĩa được). Chỉ bật trên k8s/Linux: khi test trên
  Windows, bản JNI của RocksDB bị crash native lúc thiếu RAM.
- Đổi schema của query có trạng thái hoặc đổi state store thì phải dùng checkpoint mới: đặt `CHECKPOINT_PATH`
  (ví dụ `.../checkpoints-v2`); dữ liệu tính lại từ đầu Kafka, ES ghi đè theo id nên không bị trùng.

## 5. Đảm bảo "exactly-once" hiệu quả

- Kafka producer `acks=all`, RF=3, `min.insync.replicas=2`: một broker chết không mất dữ liệu.
- Streaming: offset lưu trong checkpoint HDFS; file sink ghi parquet theo giao dịch (`_spark_metadata`).
- Mọi ghi ES dùng **id cố định**, nên chạy lại một micro-batch sau sự cố chỉ ghi đè, không nhân đôi.
- Batch layer khử trùng theo `event_id`, `comment_id`, `(chart, snapshot, rank)`, nên tự sửa mọi sai lệch của speed layer mỗi giờ.

## 6. Cấu hình tài nguyên

| Thành phần | lite (minikube) | full (cloud) |
|---|---|---|
| Kafka | 3 × 512 MiB | 3 × 512 MiB |
| HDFS | NN + 2 DN × 384 MiB | NN + 3 DN |
| Spark | master + 2 worker × (2 core, 1 GB) | master + 3 worker × (2 core, 3 GB) |
| Speed layer / batch | 2 core mỗi app | 4 core mỗi app |
| Elasticsearch | 1 node, heap 512 MB | 3 node, heap 1 GB, 1 replica |
| API | 2–6 pod (HPA) | 2–6 pod (HPA) |
| Bộ giả lập | 2 × 300 user × 5 sự kiện/giây | 3 replica |
