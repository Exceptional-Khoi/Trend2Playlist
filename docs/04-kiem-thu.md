# Kiểm thử: chịu lỗi (fault tolerance) và khả năng mở rộng (scalability)

Hai script chạy từng kịch bản, in trạng thái trước/sau, và dừng chờ Enter để thuyết trình:

```bash
./scripts/test-fault-tolerance.sh [all|kafka|hdfs|worker|driver|master|es|api]
./scripts/test-scalability.sh     [all|stream|batch|crawl|api|hdfs]
```

Trong lúc chạy, mở dashboard tab **Pipeline** (lượt nghe/phút, throughput từng streaming query) và **Spark UI**
(`kubectl -n music port-forward svc/speed-layer-ui 4040:4040`) để thấy hệ thống phản ứng.

## 1. Chịu lỗi

| # | Tắt thành phần | Cơ chế | Kết quả mong đợi | Cách kiểm chứng |
|---|---|---|---|---|
| 1 | 1/3 broker Kafka | RF=3, `min.insync.replicas=2`, producer `acks=all` | ISR 3 → 2, leader chuyển sang broker khác, **không lỗi ghi**; broker quay lại thì ISR về 3 | `kafka-topics --describe`; log `listener-sim` không có lỗi; lượt/phút trên dashboard không gãy |
| 2 | 1 DataNode HDFS | `dfs.replication=2` | Vẫn đọc được mọi file; sau ~2,5 phút NN đánh dấu node chết. Full profile: tự sao chép lại lên node khác; lite: under-replicated tới khi pod quay lại với PVC cũ | `hdfs fsck /datalake`, `hdfs dfsadmin -report` |
| 3 | 1 Spark worker | Spark chạy lại task của executor mất; Deployment tạo worker mới | Streaming chậm vài batch rồi bình thường | log `PROGRESS` của speed layer; Spark master UI |
| 4 | Driver speed layer | Checkpoint trên HDFS + ghi ES idempotent | Pod mới **tiếp tục batch_id cũ** (không từ 0), xử lý phần tồn trong Kafka; các phút bị gián đoạn được bù đủ, không đếm trùng | `PROGRESS batch_id`; biểu đồ lượt/phút không có lỗ; so sánh với batch view |
| 5 | Spark master | `recoveryMode=FILESYSTEM` trên PVC | App đang chạy không bị ảnh hưởng; master mới khôi phục danh sách worker/app | log master: "recovery", "Registered worker" |
| 6 | 1 node Elasticsearch | full: 3 node, replica 1 | Cluster `yellow` nhưng vẫn phục vụ; node quay lại thì `green`. Lite (1 node): API lỗi tạm thời, dữ liệu còn nguyên trên PVC | `_cluster/health`, `_cat/indices` |
| 7 | 1 pod API | 2 replica sau Service, PDB | Request tiếp tục thành công (200) | vòng gọi `/api/health` trong script |

Ngoài ra (không cần script):
- **Crawler lỗi 1 nền tảng**: các nền tảng khác vẫn được crawl; Job exit ≠ 0 để k8s retry (`backoffLimit`).
- **Kafka tạm không nhận**: producer retry 10 lần; crawler lần sau ghi snapshot mới; batch view không phụ thuộc lần crawl nào.
- **Batch job chết giữa chừng**: view cũ vẫn phục vụ vì view mới chỉ được "công bố" (ghi con trỏ `_latest`, xoá doc cũ theo `run_id`) khi chạy xong.

## 2. Mở rộng

| # | Thay đổi | Đo gì | Kỳ vọng |
|---|---|---|---|
| 1 | Bộ giả lập 2 → 6 replica (tải sự kiện ×3) | `vào` và `xử lý` (dòng/s), thời gian batch của query `rt_plays`, `recommend` | Nếu `xử lý < vào` hoặc thời gian batch > trigger: thiếu tài nguyên |
| 1b | Spark worker 2 → 4, `SPEED_CORES_MAX` 2 → 4 | như trên | Thời gian batch giảm, `xử lý ≥ vào` |
| 2 | `batch-views` với 2 core rồi 4 core | thời gian chạy Job | Giảm (không tuyến tính vì dữ liệu nhỏ, có chi phí khởi động) |
| 3 | Crawl bình luận 3 → 6 pod (Indexed Job) | thời gian chạy Job | Giảm gần một nửa (bị giới hạn bởi tốc độ YouTube) |
| 4 | Tải API 60 kết nối đồng thời | `kubectl get hpa`, req/s, p95 | HPA tăng 2 → 4–6 pod, p95 giảm sau khi scale |
| 5 | DataNode 2 → 3 | `dfsadmin -report` | Dung lượng HDFS tăng; `balancer` rải lại block |

Mẫu bảng ghi kết quả cho báo cáo:

| Kịch bản | Cấu hình | Tải (sự kiện/s) | Xử lý (dòng/s) | Thời gian batch (s) | Ghi chú |
|---|---|---|---|---|---|
| Speed layer | 2 worker, 2 core | 10 | | | |
| Speed layer | 2 worker, 2 core | 30 | | | |
| Speed layer | 4 worker, 4 core | 30 | | | |

## 3. Kết quả đã kiểm chứng cục bộ (không k8s)

Chạy trên laptop 8 GB RAM, WSL2 Ubuntu (Java 17, PySpark 3.5.9 local mode, Kafka 3.9.2 1 node), Elasticsearch 8.19.22 trên Windows.

| Hạng mục | Kết quả |
|---|---|
| Crawl BXH 4 nền tảng | 846 bản ghi / 29–70 giây (Zing 152, Spotify 294, Apple 100, YouTube 300 kèm 100 nghệ sĩ) |
| Crawl playlist | 3.390 bản ghi (35 playlist Zing Top 100 + Spotify Hot Hits) / 20 giây |
| Crawl bình luận (1/3 shard, 38 video) | 8.132 bình luận (6.404 không trùng) / 8 phút; 55 lượt nhắc tỉnh (21 tỉnh) sau khử trùng |
| Crawl bình luận lần 2 (1/2 shard, 56 video) | 13.755 bình luận / 17 phút, 100% có `author_hash`; 76 người nhắc tỉnh, 24 người tự nhận ở tỉnh |
| Crawl Google Trends (28 bài) | 1.764 bản ghi / 6,5 phút; 1 lần 429 được xử lý bằng chờ lùi; bài mốc tự chọn "lưu niên jack" (54/63 tỉnh). Nếu lấy bài #1 BXH làm mốc thì chỉ có 3–5/63 tỉnh có số liệu |
| Phân loại bình luận | Câu thật: "quê mình Càng Long Trà Vinh", "có An Giang luôn" → tự nhận; "anh ở Thanh Hóa à", "bạn này hát ở Hà Tĩnh", "giống hồ đá ở Đồng Nai" → chỉ nhắc tên (có unit test) |
| Crawler → Kafka trực tiếp | 846 + 3.390 + 2.500 bản ghi, 0 lỗi |
| `batch_views` | 75–100 giây; catalog 3.399 bài, gộp 24 khoá trùng đa nền tảng, 34 tỉnh × top 50 |
| `batch_views` bản mới (Spark trên Windows, ghi thẳng ES) | 4,5–5 phút (driver 1,2 GB, 2 luồng); 3 tỉnh độ tin cậy cao, 15 trung bình; Vĩnh Long #1 "Thiên Đường Với Người Thương" (Trends gấp 3 lần trung bình + 5 người bình luận); Lai Châu "thấp" → dùng BXH toàn quốc; bài đặc trưng Cà Mau "Đêm Gành Hào Nghe Điệu Hoài Lang" (địa danh Bạc Liêu cũ) |
| Connector Spark → ES | `elasticsearch-spark-30_2.12:8.19.13` ghi đủ 13 index (mảng struct lồng nhau, id cố định, xoá bản cũ theo run_id). Bản 8.19.14+ không tải được vì POM lỗi |
| `batch_similarity` | 25–33 giây; 5.224 giỏ, 16.579 cặp tương tự |
| `speed_layer` (5 query) | ghi đủ 5 đầu ra; gợi ý cho 195 user, được làm mới liên tục (trigger 5 giây; lúc kiểm tra bản mới nhất vừa ghi 8–16 giây trước) |
| Phục hồi driver từ checkpoint | Tắt rồi chạy lại speed layer: 5 query tiếp tục từ `batch_id` cũ (ingest 20, recommend 49, rt_plays 32), không xử lý lại từ đầu |
| API | 16 endpoint trả 200 trên ES thật; tìm kiếm không dấu ("luu nien" → "Lưu Niên"); `POST /api/listen` ghi vào Kafka (acks=all) và trả về partition/offset, dữ liệu sai trả 400 |
| Dashboard | 5 tab hiển thị đúng ở cả giao diện sáng/tối (kiểm tra trên Chrome) |
| Manifest k8s | 43 resource hợp lệ theo schema Kubernetes 1.31 (`kubeconform -strict`) |
| Unit test | 12/12 nhóm test: chuẩn hoá tên bài, tên video YouTube, nhận diện & phân loại tỉnh, 63 tỉnh cũ ↔ mã ISO của Google, thể loại |

**Chưa kiểm chứng**: chạy trên cụm k8s thật (thiếu RAM), và luồng streaming bình luận sau khi đổi sang bộ phân loại mới
(dùng chung hàm với batch layer đã chạy thật). Các kịch bản ở mục 1 và 2 cần chạy khi đã deploy.

## 4. Kết quả chạy thật các kịch bản chịu lỗi & mở rộng (cục bộ, 27/09/2026)

Máy 8 GB RAM không dựng nổi cụm k8s nên các kịch bản được chạy với đúng các thành phần thật chạy thẳng trên Windows:
**Kafka 3.9.2 cụm 3 broker (KRaft, RF=3, min.insync=2)**, Spark 3.5.9 local mode, Elasticsearch 8.19.22, dữ liệu crawl thật.

| Kịch bản | Cách làm | Kết quả |
|---|---|---|
| Tắt 1/3 broker Kafka (không tải) | kill broker 1 | ISR mọi partition 3 → 2, leader chuyển sang broker 2/3; broker quay lại → ISR đủ 3 |
| Tắt 1/3 broker Kafka **khi có tải** | kill broker 2 lúc bộ giả lập đang ghi (acks=all) và speed layer đang đọc | Bộ giả lập **0 lỗi gửi**; speed layer vẫn xử lý (~10 sự kiện/giây); broker quay lại tự đồng bộ, ISR đủ 3 sau **13 giây** |
| Driver speed layer chết | kill driver, sự kiện vẫn đổ vào Kafka ~1–2 phút, chạy lại cùng checkpoint | Tiếp tục từ batch cũ (rt_plays batch 19, xử lý 846 sự kiện dồn lại), không bắt đầu từ 0 |
| **Không mất / không trùng** | so tổng sự kiện trong Kafka với tổng plays+skips+likes speed layer ghi vào ES | **3.433 = 3.433** (sau khi tắt broker + driver chết); **101.051 = 101.051** (sau khi xử lý lại toàn bộ với checkpoint mới) |
| 1 nguồn crawl hỏng | đặt sai khoá Zing | Spotify/Apple/YouTube vẫn gửi 694 bản ghi; Job thoát mã 1 (đã sửa: trước đó thoát 0 nên k8s không thấy lỗi) |
| Tăng tải ×15 (150 sự kiện/giây), Spark 2 core | 3 bộ giả lập × 50 sự kiện/giây | rt_plays theo kịp (160–300 dòng/s); **gợi ý tụt lại** (xử lý 69/s < vào 164/s, batch 35–82 giây) |
| Cùng tải, 4 core, thiết kế cũ | | **Tràn heap (OOM)** ở query gợi ý → phát hiện lỗi thiết kế state (cửa sổ trượt bước 1 phút) |
| Cùng tải, 4 core, thiết kế đã sửa (bước 5 phút, state gọn) | | Gợi ý **xử lý 304/s > vào 163/s**, không OOM; rt_plays 56–137/s do 5 query tranh 4 core trên máy đang swap |
| Crawl song song | bình luận 12 video: 1 tiến trình vs 3 tiến trình (như Indexed Job 3 pod) | 114 giây → 42 giây (**nhanh gấp 2,7 lần**), cùng 1.440 bình luận |
| Tải API | `loadtest.py`, 60 kết nối, 45 giây | 1 worker: 13 req/s, p50 3,6 s, 27 lỗi → 3 worker: **29 req/s**, p50 1,4 s, 1 lỗi |
| Batch job 1 / 2 / 4 core | `batch_similarity` | 58,8 / 55,6 / 59,3 giây: **không nhanh hơn** vì dữ liệu nhỏ và nhiều file nhỏ (160 file ~56 KB cho 1 giờ sự kiện) → cần job gộp file |

**Vẫn cần cụm k8s thật** cho: DataNode HDFS chết/tái sao chép, Spark worker/master chết trong standalone cluster,
ES 3 node mất 1 node, HPA tự tăng pod, PodDisruptionBudget. Các bước nằm sẵn trong `scripts/test-fault-tolerance.sh` và `scripts/test-scalability.sh`.

