# Báo cáo dữ liệu đã crawl và vai trò trong dự án

**Ngày chụp dữ liệu:** 06/10/2026  
**Tổng số bản ghi kiểm chứng:** 10.305  
**Phạm vi:** Zing MP3, Spotify, Apple Music, YouTube và Google Trends.

Mỗi file JSONL kiểm chứng chứa message dạng `{"topic": ..., "key": ..., "value": {...}}`, tương ứng với dữ liệu crawler gửi vào Kafka khi chạy hệ thống trên Kubernetes.

## 1. Tổng quan dữ liệu hiện có

| Topic / bảng raw | Số dòng | Độ phủ | Vai trò chính |
|---|---:|---|---|
| `music.charts` | 1.398 | 577 dòng chart toàn quốc + 821 video geotag trên 34 tỉnh; 819 `track_key` | Xu hướng toàn quốc, khác biệt nền tảng và bài tăng hạng; geo chỉ là dữ liệu khám phá phụ |
| `music.playlists` | 3.389 | 36 playlist, 3.101 bài; Zing 3.337 + Spotify 52 | Bổ sung thể loại và tạo cặp bài đồng xuất hiện cho recommendation |
| `music.comments` | 2.998 | 42 video/bài có comment trong cửa sổ 7 ngày | Tín hiệu tỉnh từ nội dung comment; ẩn danh tác giả bằng hash |
| `music.trends` | 2.520 | 40 bài × 63 tỉnh cũ; 455 cặp có dữ liệu, bài mốc `em tôi anh trai` phủ 58/63 tỉnh | Tín hiệu vùng miền chính, sau đó gộp về 34 tỉnh mới |
| `music.events` | Không phải dữ liệu crawl | Được tạo bởi dashboard/simulator: `play`, `skip`, `like` | Xu hướng realtime, thói quen nghe và playlist cá nhân |

Tổng snapshot crawl trong bốn file là **10.305 dòng**. Các khoá khử trùng không trùng trong từng lần crawl.

## 2. Dữ liệu bảng xếp hạng — `music.charts`

| Nguồn / `chart_id` | Scope | Số dòng | Ý nghĩa |
|---|---|---:|---|
| `zing_realtime` | VN | 100 | #zingchart realtime, có score, tổng nghe và tổng thích |
| `zing_week_vn` | VN | 40 | BXH tuần Việt Nam |
| `zing_new_release` | VN | 11 | Bài mới phát hành |
| `spotify_top50_vn_daily` | VN | 50 | Top 50 Việt Nam hằng ngày |
| `spotify_top_songs_vn_weekly` | VN | 50 | Top Songs Việt Nam theo tuần |
| `spotify_daily_streams_vn` | VN | 196 | Hạng và số stream từ bảng tổng hợp Spotify Charts |
| `apple_most_played_vn` | VN | 100 | Apple Music Most Played Việt Nam |
| `youtube_most_popular_music_vn` | VN | 30 | Fallback chính thức của YouTube Data API khi Charts web bị 429 |
| `youtube_geo_search` | 34 tỉnh | 821 | Video music có metadata geotag gần tỉnh; không phải lượt nghe của người trong tỉnh |

### Ví dụ dữ liệu thật ở hạng 1

| Nguồn | Bài | Nghệ sĩ | Chỉ số có sẵn |
|---|---|---|---|
| Zing | Giá Như Anh Là Em | Lệ Quyên | score 3.112; tổng nghe 4.249.968 |
| Spotify | Seven (Explicit Ver.) | Jung Kook, Latto | Hạng 1 Top 50 |
| Apple Music | LAVIEM | TINH HÀ “SAY HI” | Hạng 1 Most Played |
| YouTube fallback | TRỄ GIỜ CƠM | Quang Hùng MasterD, WEAN, Sơn.K, XUÂN ĐỊNH KY | Tổng view 1.827.266 tại thời điểm crawl |

### Vai trò trong dự án

- Xác định bài hát và nghệ sĩ thịnh hành toàn quốc.
- Tính điểm phổ biến `national_score`.
- Phát hiện bài tăng/giảm hạng và bài mới vào chart.
- So sánh độ giao nhau giữa Zing, Spotify, Apple Music và YouTube.
- Tạo lịch sử biến động thứ hạng.
- Cung cấp metadata cho catalog bài hát chung.

`youtube_most_popular_music_vn` là dữ liệu fallback `videos.list(chart=mostPopular, category=Music)`, không được coi là YouTube Music Weekly Chart.

## 3. Dữ liệu playlist — `music.playlists`

| Nguồn | Số playlist | Số dòng |
|---|---:|---:|
| Zing MP3 | 35 | 3.337 |
| Spotify | 1 | 52 |
| **Tổng** | **36** | **3.389** |

Có **3.101 bài hát khác nhau**.

Các trường chính:

```text
playlist_id, playlist_name, snapshot_id, position
track_key, title, artists, genre_hint, genres
source, platform_id
```

### Vai trò trong dự án

1. Bổ sung thể loại cho bài hát, đặc biệt khi nguồn chart không cung cấp genre.
2. Tạo các cặp bài đồng xuất hiện trong cùng playlist.
3. Spark tính bảng `item_sim(track_key, neighbor_key, sim)`.
4. Speed layer dùng `item_sim` để tạo playlist cá nhân với lý do “Vì bạn vừa nghe…”.

## 4. Dữ liệu bình luận — `music.comments`

- **2.998 bình luận không trùng**.
- **42 video/bài hát** có comment trong cửa sổ 7 ngày.
- **18 comment** có tín hiệu tỉnh.
- Phủ được **5 tỉnh**.

| Loại tín hiệu | Số lần | Trọng số |
|---|---:|---:|
| Người viết tự nhận ở tỉnh — `self` | 1 | 1,0 |
| Chỉ nhắc tên tỉnh — `mention` | 17 | 0,3 |

Các trường chính:

```text
comment_id, video_id, track_key, title, artists
text, like_count, published_ms, author_hash, comment_sort
```

### Vai trò trong dự án

- Nhận diện các câu như “Ai ở Nghệ An điểm danh” hoặc “quê mình Trà Vinh”.
- Phân biệt người viết tự nhận ở tỉnh với việc chỉ nhắc tên tỉnh.
- Loại comment dạng lời bài hát, credit, địa điểm quay MV và tên tỉnh nằm trong tên bài.
- Mỗi tác giả chỉ được tính một lần cho mỗi tỉnh × bài để hạn chế spam.
- Không lưu tên hay ID thật của người viết; chỉ lưu `author_hash`.

Comment là tín hiệu thật nhưng thưa: khoảng **0,6%** comment trong snapshot có tín hiệu tỉnh. Vì vậy nó là tín hiệu bổ sung, không phải nguồn địa lý duy nhất.

## 5. Google Trends — `music.trends`

- **40 bài hát**.
- **63 tỉnh/thành cũ**.
- **2.520 dòng**.
- **455 cặp bài × tỉnh** có dữ liệu.
- Bài mốc: **“em tôi anh trai”**, phủ **58/63 tỉnh**.

Các trường chính:

```text
track_key, title, artists, query
geo_code, geo_name, share, has_data
anchor_key, anchor_query, anchor_share
snapshot_id, timeframe
```

### Vai trò trong dự án

Google Trends là tín hiệu địa lý chính. Giá trị so sánh được tính bằng:

```text
trend_ratio = share của bài / share của bài mốc
```

Spark sau đó:

1. Ánh xạ 63 tỉnh cũ về 34 tỉnh mới.
2. Gộp theo trọng số dân số.
3. Chuẩn hoá điểm trong từng tỉnh.
4. Đo độ phủ và confidence.
5. Kết hợp với chart toàn quốc, comment và event.

Kết quả phải được diễn giải là **mức quan tâm tìm kiếm âm nhạc ước lượng**, không phải lượt nghe thực tế của tỉnh.

## 6. YouTube Geo — dữ liệu phụ trợ

Đã lấy được **821 video** có metadata geotag trên 34 tỉnh. Dữ liệu này dùng để khám phá nội dung âm nhạc được quay hoặc gắn với địa phương.

`youtube_geo_search` không được cộng như bằng chứng người dân tại tỉnh đã nghe bài, vì `location` là vị trí gắn với video upload chứ không phải vị trí khán giả.

## 7. Dữ liệu hành vi realtime — `music.events`

Bảng này không được crawl từ nền tảng. Nó sinh ra khi người dùng hoặc simulator thực hiện `play`, `like` hoặc `skip`.

```text
event_id, user_id, province_code
track_key, title, artists, genre
action, listen_ms, duration_ms, ts_ms
```

### Vai trò trong dự án

- Spark gom event theo tỉnh × bài × cửa sổ một phút để cập nhật `music-rt-plays`.
- Lịch sử 30 phút gần nhất của từng user được dùng cho recommendation.
- `like` có trọng số cao, `play` có trọng số dương, `skip` có trọng số âm.
- Event càng mới càng có trọng số lớn.
- Kết quả playlist được ghi vào `music-recommendations` sau mỗi micro-batch khoảng 5 giây.

## 8. Schema raw và tác dụng trong Spark

| Bảng | Khoá/field quan trọng | Spark dùng để làm gì |
|---|---|---|
| `music.charts` | `chart_id`, `chart_scope`, `snapshot_id`, `rank`, `previous_rank`, `metric_*`, `track_key`, `source` | Chuẩn hoá điểm hạng, lấy snapshot mới nhất, tính `national_score`, độ giao nhau nền tảng, lịch sử hạng và momentum |
| `music.playlists` | `playlist_id`, `snapshot_id`, `position`, `genre_hint`, `track_key` | Suy ra thể loại; self-join các bài cùng playlist để tính item similarity |
| `music.comments` | `comment_id`, `track_key`, `text`, `author_hash`, `published_ms` | Nhận diện tỉnh và loại `self`/`mention`; mỗi tác giả chỉ tính một lần trên mỗi tỉnh × bài |
| `music.trends` | `track_key`, `geo_code`, `share`, `anchor_share`, `has_data`, `anchor_key` | Tính `share / anchor_share`, gộp 63 tỉnh cũ về 34 tỉnh mới theo dân số, đo độ phủ/confidence |
| `music.events` | `event_id`, `user_id`, `province_code`, `track_key`, `action`, `listen_ms`, `ts_ms` | Cửa sổ realtime theo phút và lịch sử 30 phút/user; play/like tăng điểm, skip giảm điểm |

## 9. Các bảng trung gian trong HDFS

| View | Nội dung | Consumer |
|---|---|---|
| `catalog` | Một dòng/bài canonical: title, artists, genre, thumbnail, nguồn và `national_score` | Dashboard catalog, simulator và speed-layer recommendation |
| `track_alias` | `alias_key → canonical_key` để gộp cùng bài khác cách ghi giữa nền tảng | Mọi phân tích sau entity resolution |
| `trending_batch` | Điểm top bài theo `province_code × track_key`, đã ghép national + Trends + comments + events | Bản đồ/BXH tỉnh và xu hướng địa phương trong recommendation |
| `item_sim` | `track_key → neighbor_key, sim` từ đồng xuất hiện playlist/phiên nghe | Playlist cá nhân realtime |

## 10. Các bảng phục vụ dashboard trong Elasticsearch

| Index | Vai trò trên dashboard/API |
|---|---|
| `music-tracks` | Catalog hợp nhất đa nền tảng và top toàn quốc |
| `music-trending-batch` | Top 50 bài theo từng tỉnh, có thành phần điểm và confidence |
| `music-rt-charts` | Snapshot chart mới nhất của từng nền tảng |
| `music-rt-plays` | Play/skip/like theo tỉnh × bài × phút |
| `music-rt-mentions` | Comment đã nhận diện tỉnh |
| `music-recommendations` | Playlist mới nhất theo `user_id`, kèm lý do gợi ý |
| `music-item-sim` | Danh sách bài tương tự |
| `music-an-genre-province` | Cơ cấu thể loại theo tỉnh/vùng |
| `music-an-hourly` | Thói quen nghe theo giờ/thứ và skip-rate |
| `music-an-platform-overlap` | Jaccard/Spearman giữa các nền tảng |
| `music-an-artists` | Nghệ sĩ phổ biến theo scope |
| `music-an-rising` | Bài tăng hạng hoặc mới vào chart |
| `music-an-chart-history` | Lịch sử hạng theo snapshot |
| `music-an-signal-agreement` | Mức đồng thuận Trends ↔ comments ↔ toàn quốc |
| `music-an-province-summary` | Hồ sơ, bài/thể loại đặc trưng và confidence từng tỉnh |
| `music-an-kpis`, `music-metrics*`, `music-meta` | Sức khoẻ, throughput, độ tươi và phiên bản batch |

## 11. Dòng chảy dữ liệu

```text
Zing / Spotify / Apple / YouTube / Google Trends
                         ↓
                 Crawler Python
                         ↓
                  Kafka topics
                         ↓
          ┌──────────────┴──────────────┐
          ↓                             ↓
Spark Structured Streaming         HDFS raw history
          ↓                             ↓
Realtime views                   Spark Batch Processing
          └──────────────┬──────────────┘
                         ↓
                  Elasticsearch
                         ↓
 Dashboard bản đồ + bảng phân tích + playlist realtime
```

## 12. Cách xem và kiểm tra lại

```powershell
# Thống kê lại mà không lộ nội dung comment hay API key
.\.venv\Scripts\python.exe local\inspect_crawl.py out\validation

# Xem một message của từng bảng
Get-Content out\validation\music.charts.jsonl -TotalCount 1
Get-Content out\validation\music.playlists.jsonl -TotalCount 1
Get-Content out\validation\music.comments.jsonl -TotalCount 1
Get-Content out\validation\music.trends.jsonl -TotalCount 1
```

Các file dữ liệu kiểm chứng:

- `out/validation/music.charts.jsonl`
- `out/validation/music.playlists.jsonl`
- `out/validation/music.comments.jsonl`
- `out/validation/music.trends.jsonl`

## 13. Kết luận

Dữ liệu hiện tại đủ để chứng minh các phần chính của hệ thống:

- Thu thập đa nguồn và hợp nhất schema.
- Lưu lịch sử lâu dài để phân tích lại.
- Phân tích xu hướng toàn quốc.
- Ước lượng mức quan tâm theo tỉnh.
- So sánh nền tảng và vùng miền.
- Phân tích thể loại, bài tăng hạng và thói quen nghe.
- Xử lý `play`, `like`, `skip` realtime.
- Tạo playlist cá nhân sau vài giây.
- Chịu lỗi khi một nguồn crawl không hoạt động.

Điểm phải trình bày rõ:

> Bảng xếp hạng theo tỉnh là kết quả ước lượng từ Google Trends, bình luận và event của hệ thống; không phải số lượt nghe thật theo tỉnh do các nền tảng công bố.
