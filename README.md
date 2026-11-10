# Music Pulse — Trend2Playlist

Hệ thống Big Data thu thập dữ liệu âm nhạc, xử lý bằng Kafka/Spark, lưu lịch sử trong HDFS và phục vụ dashboard qua Elasticsearch/FastAPI.

## Giao diện hiện tại

Chỉ có một dashboard tại `/`, dùng `serving/static/insights.html`, `insights.js`, `insights.css` và `world.json`:

| Tính năng | Hiển thị |
|---|---|
| Thứ hạng bài hát | Hạng tốt nhất, lịch sử xuất hiện và xu hướng theo từng BXH/kỳ nguồn |
| Bài hát trong playlist | Số playlist khác nhau có bài, tên/link/loại/vị trí của từng playlist |
| Bản đồ âm nhạc | Tất cả quốc gia có dữ liệu trong kỳ, bài nổi bật và nguồn của từng nước |
| Khán giả nói gì? | Cụm từ/chủ đề riêng của bài từ mẫu bình luận, kèm số lần xuất hiện và bằng chứng |

Dashboard demo, ba asset giao diện cũ và các hướng dẫn bàn giao trùng phiên bản đã được xóa. Không còn tab play/skip/like hoặc gợi ý trên giao diện hiện tại. Các tác vụ mô phỏng nghe, batch và recommendation còn trong backend để duy trì pipeline; không coi chúng là tính năng dashboard.

## Bắt đầu

- [Dựng trên tài khoản Google Cloud khác](docs/15-trien-khai-tai-khoan-gcp-khac.md).
- [Deploy local/GKE và cập nhật code](docs/03-trien-khai.md).
- [Tạm dừng/bật lại GKE](docs/17-tam-dung-va-bat-lai-gke.md).
- [API và cách tính bốn tính năng](docs/09-four-features-implementation.md).
- [Kiến trúc hiện hành](docs/02-kien-truc.md).
- [Kiểm thử và giới hạn kiểm chứng](docs/04-kiem-thu.md).
- [Bản sửa giao diện, crawler và OOM ngày 10/11/2026](docs/16-cap-nhat-music-pulse-2026-11-10.md).

Chạy riêng API với archive cục bộ bằng PowerShell, sau khi đã tạo virtual environment:

```powershell
.\.venv\Scripts\python.exe -m pip install -r serving/requirements.txt -r crawler/requirements.txt
$env:INSIGHT_BACKEND = 'archive'
.\.venv\Scripts\python.exe -m uvicorn api:app --app-dir serving --host 127.0.0.1 --port 8000
```

Mở http://127.0.0.1:8000. Archive/dữ liệu crawl không được commit nên checkout mới có thể chưa có dữ liệu; ứng dụng không tự dựng số liệu giả. Chế độ cloud dùng `INSIGHT_BACKEND=elasticsearch`. Báo cáo bình luận cần OpenAI key ở backend; các tab còn lại không cần khóa đó.

## Cấu trúc

```text
crawler/       Crawler hiện hành và simulator → Kafka
spark/         Ingest/streaming, batch views và similarity
serving/       FastAPI, phân tích, báo cáo bình luận và dashboard
shared/        Chuẩn hóa văn bản, metadata địa phương dùng bởi backend
schemas/       Schema tham khảo; schema Spark thực thi ở spark/jobs_common.py
k8s/           Manifest, profile full, checkpoint service và CronJobs
scripts/       Deploy, update code, bootstrap và cloud test runner
tests/         Unit/property/API/security/deployment và cloud tests opt-in
docs/          Tài liệu hiện hành; tài liệu ý tưởng/dữ liệu mẫu có nhãn rõ
crawlers/      Bộ thu thập offline, được pipeline/test dùng
pipeline/      Chuẩn hóa/entity resolution và phân tích offline
local/         Công cụ chạy/nhập dữ liệu cục bộ
```

Không dùng `run_all.py`/`local/` như cách mở dashboard GKE; đây là công cụ offline. Không đưa `.env`, kubeconfig, snapshot cluster, backup hoặc ZIP bàn giao vào Git.

## Kiểm thử

```bash
python -m pip install -r requirements-dev.txt
python -m pytest tests -q
```

Cloud tests mặc định không chạy, cần scope và opt-in qua `scripts/run-cloud-tests.py`. SKIP vì thiếu cluster/browser/archive không phải PASS tích hợp.

## Giới hạn thực tế

- BXH các nền tảng đo các quần thể khác nhau; điểm bản đồ không phải thị phần lượt nghe toàn cầu. iTunes Store không phải số stream.
- Crawl lại một kỳ không tạo thêm kỳ nguồn mới. Lịch sử cần tích lũy thật.
- API đọc tối đa 200.000 raw records đã lọc trước; truy vấn cold còn chậm. Tác vụ phân tích nặng được giới hạn theo process để giảm đỉnh RAM.
- Báo cáo AI chỉ mô tả mẫu; quota/cache SQLite theo pod, không phải quota bền vững toàn cluster.
- GKE tiếp tục tính chi phí khi đóng laptop. Dừng port-forward không dừng máy cloud.
