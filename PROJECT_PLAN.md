# Kế hoạch và phạm vi hiện hành — Music Pulse

Cập nhật 10/11/2026. Đây là phạm vi của dashboard đang được bàn giao; không dùng kế hoạch giao diện theo tỉnh/gợi ý cũ để đánh giá màn hình hiện tại.

## Mục tiêu

Xây dựng luồng dữ liệu âm nhạc thật qua Kafka → Spark → HDFS/Elasticsearch → FastAPI, cung cấp bốn góc nhìn:

1. Thứ hạng bài hát: lịch sử theo từng BXH, nguồn và kỳ, không coi ngày crawl là ngày phát hành.
2. Bài hát trong playlist: đếm membership đã quan sát, hiện nguồn/link và cho phép playlist nhỏ/chưa phân loại.
3. Bản đồ âm nhạc: toàn bộ quốc gia có dữ liệu trong khoảng chọn, tổng hợp thứ hạng từ nhiều nguồn.
4. Khán giả nói gì?: đọc mẫu bình luận theo bài, trích cụm từ/chủ đề có bằng chứng; không suy diễn ý kiến toàn bộ khán giả.

## Thành phần thực thi

- Crawler hiện hành: `crawler/run_crawler.py`, `src_*.py`, `common.py`.
- Streaming: `spark/speed_layer.py`; schema/routines dùng chung ở `spark/jobs_common.py`.
- Batch: `spark/batch_views.py`, `batch_similarity.py`.
- Serving: `serving/api.py`, `insights.py`, `comment_topics.py`.
- Dashboard: chỉ `serving/static/insights.*` và `world.json`.
- Triển khai: manifest `k8s/`, script `scripts/deploy.sh`, `update-code.sh`.

Các tín hiệu theo tỉnh, simulator và recommendation vẫn là backend/luồng thử nghiệm, không có màn hình riêng trong dashboard hiện tại. Bộ `crawlers/`, `pipeline/`, `local/`, `run_all.py` phục vụ xử lý offline và kiểm thử, không phải entrypoint GKE.

## Yêu cầu vận hành

- Profile cloud full dùng cụm Standard ba node; kiểm tra project/context, Billing và quota trước deploy.
- Giữ checkpoint/PVC khi restart; không xóa state để làm lỗi biến mất.
- HDFS có checkpoint service định kỳ, API có vùng ghi báo cáo riêng ngoài ConfigMap.
- Không ghi API key trong source/static/log; cấu hình Secret riêng cho backend khi dùng OpenAI.
- Không tự mở rộng node/đĩa/LB chỉ để vượt qua kiểm thử.

## Ưu tiên tiếp theo

1. Materialized/batch views cho phân tích lịch sử để giảm cold latency và tránh đọc raw quy mô lớn.
2. Đo RAM/độ trễ lâu hơn, kiểm chứng concurrency và recovery trên môi trường cô lập.
3. Quota/cache báo cáo dùng chung bền vững nếu cần nhiều replica API.
4. Đánh giá nhãn bình luận bằng tập được con người gán nhãn; không chỉ kiểm tra model trả JSON.
5. Theo dõi coverage theo nguồn/quốc gia và nguồn-period; không tự lấp ngày hoặc nước thiếu dữ liệu.

Các nguồn kiểm chứng, cách chạy và giới hạn được ghi trong [tài liệu kiểm thử](docs/04-kiem-thu.md). Không kết luận production-ready từ unit tests hoặc một lượt đo cloud.
