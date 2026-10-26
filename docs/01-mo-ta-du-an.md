# Mô tả dự án (Project description)

## Tên đề tài

**Phân tích xu hướng âm nhạc theo tỉnh/thành Việt Nam và gợi ý playlist thời gian thực từ dữ liệu các nền tảng nghe nhạc**
(VN Music Pulse). Hướng: Direction 1, xây dựng và cài đặt kiến trúc dữ liệu lớn (Lambda) trên Kubernetes.

## Nguồn dữ liệu

Crawl trực tiếp từ nền tảng (crawler chạy trong pod k8s, đẩy thẳng vào Kafka). Chỉ lấy nền tảng **nghe nhạc dài**,
không lấy nền tảng video ngắn như TikTok.

| Nền tảng | URL / endpoint | Lấy gì | Tần suất |
|---|---|---|---|
| Zing MP3 | `zingmp3.vn/api/v2/page/get/chart-home` (#zingchart realtime, BXH tuần, nhạc mới) · `/page/get/top-100` · `/page/get/playlist` · `/song/get/info` | hạng, điểm realtime, lượt nghe, lượt thích, thể loại; ~40 playlist "Top 100" theo thể loại | 30 phút; playlist 6 giờ |
| Spotify | `open.spotify.com/embed/playlist/<id>`: Top 50 Vietnam, Top Songs Vietnam (tuần), Hot Hits Vietnam | hạng, tên bài, nghệ sĩ, thời lượng, id bài | 30 phút |
| Spotify (số stream) | `kworb.net/spotify/country/vn_daily.html` (tổng hợp từ Spotify Charts) | lượt stream ngày, tổng stream, thay đổi hạng | 30 phút |
| Apple Music | `rss.marketingtools.apple.com/api/v2/vn/music/most-played/100/songs.json` | hạng, thể loại, ngày phát hành, ảnh bìa | 30 phút |
| YouTube Charts | `charts.youtube.com` (endpoint `youtubei/v1/browse`): Top bài hát, Top video, Top nghệ sĩ Việt Nam theo tuần; khi bị 429 dùng YouTube Data API `videos.list?chart=mostPopular&videoCategoryId=10&regionCode=VN` với `chart_id` riêng | hạng, hạng tuần trước, lượt xem tuần, id video; fallback có tổng view/like nhưng không được coi là BXH tuần | 30 phút |
| YouTube bình luận | video trên BXH YouTube + video của bài hot trên Zing (tìm qua `youtubei/v1/search`); YouTube Data API v3 nếu có key | nội dung bình luận (không lưu tên/ID người viết), lượt thích, thời gian | mỗi giờ, 3 pod song song |
| **Google Trends** | `trends.google.com/trends/api/explore` + `widgetdata/comparedgeo` (property = YouTube search, 7 ngày) | tỉ lệ lượt tìm của từng bài ở **63 tỉnh cũ** (mã ISO VN-xx), so với 1 bài mốc | mỗi ngày, 40 bài |
| YouTube geo (tuỳ chọn) | YouTube Data API `search.list?location=lat,lng&locationRadius=60km` | video âm nhạc có metadata geotag gần từng tỉnh; **không phải vị trí hoặc lượt nghe của khán giả** | hằng ngày |

Nguồn đã thử nhưng loại bỏ: Deezer (bị chặn tại VN), Shazam (chỉ có Top 200 toàn quốc, không có BXH thành phố VN),
Apple Music City Charts (không có thành phố Việt Nam), bình luận Zing MP3 (web không có API bình luận bài hát),
TikTok (nền tảng ngắn, ngoài phạm vi). Google Trends lúc đầu trả 429, sau đó truy cập được khi giữ cookie phiên và chạy chậm.

## Yêu cầu và chức năng

### 1. Thu thập dữ liệu

Năm loại bản ghi, mỗi loại một Kafka topic:

| Topic | Một bản ghi = | Trường chính |
|---|---|---|
| `music.charts` | 1 dòng trên 1 BXH tại 1 lần crawl | `chart_id, source, rank, previous_rank, metric (views/streams/score), track_key, title, artists[], genres[], platform_id, crawled_ms` |
| `music.playlists` | 1 bài trong 1 playlist | `playlist_id, playlist_name, genre_hint, position, track_key, ...` |
| `music.comments` | 1 bình luận YouTube | `comment_id, video_id, track_key, text, like_count, published_ms` |
| `music.events` | 1 lượt nghe (giả lập hoặc từ web) | `user_id, province_code, track_key, action (play/skip/like), listen_ms, ts_ms` |
| `music.trends` | 1 bài × 1 tỉnh cũ trong 1 lần tra Google Trends | `track_key, query, geo_code (VN-xx), share, anchor_share, has_data, anchor_key` |

**Khó khăn và cách xử lý:**

| Khó khăn | Ví dụ thật gặp khi crawl | Cách xử lý |
|---|---|---|
| Không đồng nhất giữa nền tảng (heterogeneity) | Spotify ghi `Tìm Em (feat. Bảo Anh)`, YouTube ghi `TÌM EM`; YouTube dùng tên kênh `DatG Music`, Zing ghi `Đạt G`; video label `ST.319 Entertainment` | Chuẩn hoá tên (bỏ dấu, bỏ feat/Official MV/OST), khoá `tên-bài__nghệ-sĩ-chính`; batch layer gộp khoá trùng khi cùng tên và có chung nghệ sĩ (so khớp mờ) → 11–50 bài được gộp mỗi lần chạy |
| Cấu trúc không đều | Tên video YouTube: `NGHỆ SĨ \| 'TÊN BÀI' \| OFFICIAL MV`, `TÊN - NGHỆ SĨ`, `JACK - J97 \| TÊN \| Album...` | `clean_video_title()` tách phân đoạn, loại phần nhiễu và phần tên nghệ sĩ (có unit test với dữ liệu thật) |
| Thiếu dữ liệu | Spotify embed không có thể loại; Apple không có hạng tuần trước; nhiều video tắt bình luận | Thể loại lấy từ nền tảng khác (Zing/Apple/playlist) sau khi gộp; chỉ tính "mới vào BXH" với BXH có công bố hạng trước |
| Gán nhãn sai | Zing xếp `LAVIEM` (nghệ sĩ Việt) vào playlist "Nhạc Nhật Bản" | Không xếp vào nhóm nhạc ngoại nếu tên bài/nghệ sĩ có dấu tiếng Việt |
| Tín hiệu vùng miền thưa | Khoảng 1% bình luận có nhắc tỉnh | Thêm **Google Trends theo tỉnh** làm tín hiệu chính; bình luận là tín hiệu phụ; làm mượt Bayes; chỉ tính tín hiệu mà tỉnh đủ dữ liệu; hiển thị độ tin cậy |
| Bình luận nhắc tỉnh không có nghĩa người viết ở đó | "Bạn này hát ở Hà Tĩnh", "giống hồ đá ở Đồng Nai", lời bài "Hà Nội mùa thu", credit nơi quay MV | Phân loại "tự nhận ở tỉnh" (trọng số 1) và "nhắc tên" (0,3); bỏ credit/lời bài hát và tên tỉnh có trong tên bài; mỗi người tính 1 lần |
| Google Trends: bài mốc yếu | Bài #1 BXH "xương rồng dangrangto" chỉ có số liệu ở 3–5/63 tỉnh | Tự chọn bài mốc có số liệu ở nhiều tỉnh nhất ("thiên đường với người thương": 63/63) |
| Google Trends: từ khoá không đồng đều, trùng tên | "laviem" (tên trơn) chiếm 86–92% lượt tìm khi so với "tên bài + nghệ sĩ"; "Lưu Niên" trùng bài của ca sĩ khác | Mọi từ khoá cùng kiểu "tên bài + nghệ sĩ" |
| Google Trends: giới hạn truy cập, vẫn dùng 63 tỉnh cũ | HTTP 429; mã VN-57 (Bình Dương), VN-43 (BR-VT)... | Cookie phiên + 45 giây/nhóm + lùi dần khi 429; bảng 63 tỉnh cũ → 34 tỉnh mới theo dân số |
| Nhập nhằng ngôn ngữ | "nghe an yên", "đa năng", "hai phòng karaoke", "Bác Hồ Chí Minh" | Bình luận có dấu thì so khớp nguyên văn; bình luận không dấu thì bỏ các alias dễ nhầm |
| Sáp nhập tỉnh 2025 | Người dùng vẫn viết "Bình Dương", "Trà Vinh", "Quảng Nam" | Bảng alias 63 tỉnh cũ → 34 tỉnh mới (`shared/provinces.py`) |
| Trùng lặp | Crawl lại cùng bình luận/BXH; producer retry | Batch khử trùng theo `comment_id`/`event_id`/snapshot; speed layer ghi ES theo id cố định |
| Nền tảng thay đổi | Zing đổi apiKey/secret theo phiên bản web | Đưa ra biến môi trường; 1 nền tảng lỗi không làm hỏng lượt crawl của nền tảng khác |

### 2. Yêu cầu lưu trữ (ước lượng)

Đo thực tế: 1 lượt crawl BXH khoảng 850 bản ghi; playlist khoảng 3.400; bình luận khoảng 250 bình luận/video.
Parquet nén trong HDFS trung bình **~150 byte/bản ghi** (đo trên 28.800 bản ghi mẫu = 4,3 MB).

| Luồng | Bản ghi/ngày | JSON thô/ngày | Parquet (×2 bản sao HDFS) |
|---|---|---|---|
| BXH (48 lượt/ngày) | ~41.000 | ~50 MB | ~12 MB |
| Playlist (4 lượt/ngày) | ~13.600 | ~15 MB | ~4 MB |
| Bình luận (24 lượt × 3 pod) | ~150.000–250.000 | ~70 MB | ~70 MB |
| Sự kiện nghe (2 replica × 5 sự kiện/giây) | ~860.000 | ~350 MB | ~260 MB |
| Google Trends (1 lượt/ngày) | ~2.500 | ~2 MB | ~0,5 MB |
| **Tổng** | **~1,1 triệu** | **~0,5 GB** | **~350 MB/ngày ≈ 10 GB/tháng** |

Kafka giữ 7–30 ngày (nén gzip, RF=3); HDFS giữ toàn bộ (master dataset bất biến); Elasticsearch chỉ giữ view (vài trăm MB).
Khi test scalability tăng giả lập lên ×3 thì dữ liệu sự kiện cũng tăng ×3.

### 3. Xử lý và phân tích

| Phân tích | Tầng | Thống kê / kỹ thuật | Hiển thị |
|---|---|---|---|
| Top bài thịnh hành từng tỉnh | batch + speed | điểm tổng hợp 4 tín hiệu (toàn quốc, Google Trends, bình luận, lượt nghe), chỉ tính tín hiệu tỉnh đủ dữ liệu, làm mượt Bayes | bản đồ + danh sách có thanh thành phần điểm |
| Độ tin cậy từng tỉnh | batch | độ phủ dân số của Trends × số bài + số người bình luận | ô "Độ tin cậy", bản đồ theo độ tin cậy |
| Các nguồn có đồng thuận? | batch | Spearman Trends ↔ bình luận, Trends ↔ toàn quốc trong từng tỉnh | bảng |
| Xu hướng realtime | speed | đếm theo cửa sổ 1 phút (watermark 10 phút) theo tỉnh × bài | bản đồ bong bóng, biểu đồ lượt/phút |
| BXH toàn quốc hợp nhất | batch | Σ theo nền tảng của max điểm hạng | bảng |
| Độ tương đồng giữa nền tảng | batch | hệ số Jaccard, tương quan hạng Spearman | heatmap |
| Bài đang lên hạng | batch | thay đổi hạng có trọng số | danh sách |
| Cơ cấu thể loại theo vùng/tỉnh | batch | tỉ trọng lượt nghe theo 10 nhóm thể loại | cột chồng 100% |
| Thói quen nghe theo giờ × thứ | batch | đếm theo giờ VN, tỉ lệ bỏ qua | heatmap |
| Tỉnh được nhắc nhiều nhất | batch | số bình luận không trùng | cột ngang |
| "Bài đặc trưng" của tỉnh | batch | **lift** = tỉ lệ tại tỉnh / tỉ lệ cả nước | bảng hồ sơ tỉnh |
| Độ tương đồng bài hát | batch | đồng xuất hiện (co-occurrence) cosine có hệ số co, trên playlist + phiên nghe | dùng cho gợi ý |
| Gợi ý playlist realtime | speed | cửa sổ trượt 30 phút/người, điểm = Σ trọng số hành vi × suy giảm thời gian × độ tương đồng + xu hướng tỉnh | danh sách có lý do |
| Sức khoẻ pipeline | speed | throughput vào/xử lý mỗi streaming query, độ tươi dữ liệu | bảng + KPI |

Đề bài lưu ý "course không phải về Machine learning modeling". Toàn bộ phân tích là **thống kê và đếm phân tán bằng Spark**
(tổng hợp, cửa sổ, self-join đồng xuất hiện), không huấn luyện mô hình.

**Một số nhận xét rút ra được ngay từ dữ liệu crawl thật (27/09/2026):**
- BXH realtime của Zing MP3 gần như **tách biệt** với 3 nền tảng còn lại: chỉ 2–6% bài chung (3–12 bài; tương quan hạng
  trên số bài chung ít ỏi này còn âm, cần thêm nhiều snapshot để kết luận chắc).
  Zing nghiêng về ballad/bolero (Bơ Vơ, Giá Như Anh Là Em), còn Spotify/Apple/YouTube trùng nhau 15–19% và thiên về V-Pop/rap của nghệ sĩ trẻ.
- Chỉ khoảng 8/3.400 bài có mặt trên cả 4 nền tảng, vì vậy việc gộp đa nền tảng là cần thiết để có BXH "đồng thuận".
- Lượt **tìm kiếm trên YouTube** theo tỉnh (Google Trends) lệch khỏi BXH streaming: tương quan hạng giữa hai nguồn trong
  từng tỉnh chỉ từ −0,6 đến 0,1 (5–21 bài). Ví dụ ở TP.HCM và Hà Nội, các bài ballad như "Giá Như Anh Là Em", "Đến Khi Nào" được tìm
  nhiều hơn hẳn thứ hạng trên Spotify/Apple.
- Bình luận nhắc tỉnh tập trung vào nghệ sĩ gắn với quê hương. Ví dụ MV của Phương Mỹ Chi có nhiều bình luận nhắc Trà Vinh
  (nay thuộc Vĩnh Long), An Giang, miền Tây.
