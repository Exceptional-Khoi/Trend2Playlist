# Bốn tính năng theo tài liệu 07 — 02/11/2026

Phạm vi: ý tưởng 1, 3, 4, 6. Ý tưởng 2 bỏ, 5 chưa chốt; ý tưởng 7 cần cấu hình nhà cung cấp LLM, không triển khai trong đợt này.

## Chạy cục bộ

```powershell
.\.venv\Scripts\python.exe -m pip install -r serving/requirements.txt -r crawler/requirements.txt pytest
$env:INSIGHT_BACKEND='archive'
.\.venv\Scripts\python.exe -X utf8 -m uvicorn api:app --app-dir serving --host 127.0.0.1 --port 8000
```

Mở http://127.0.0.1:8000. Đây là dashboard duy nhất, gồm bốn tính năng. Dashboard demo và giao diện cũ đã bị xóa; thao tác play/skip/like nằm ngay trong tab Phản hồi nghe.

Archive đọc các file country trong `out/country-*/records`, kết quả charts trong `out-test/*/charts`, `out/music.*.jsonl`, `out-test/*/music.*.jsonl`, sự kiện mẫu `output/user_events.jsonl` và thao tác web mới trong `out/insight-web-events.jsonl`. Có thể đặt `INSIGHT_DATA_DIR` để đọc thêm thư mục JSONL. Giữ nguyên timestamp; không giả vờ dữ liệu cũ là realtime. Nguồn events thiếu nhãn là `unknown`, không tự nhận là người dùng web.

## Nội dung thực hiện

| Tính năng | Phép tính và điều kiện |
|---|---|
| Vòng đời | Dùng toàn bộ raw trong cửa sổ; ID nền tảng trong từng BXH, gộp snapshot cùng kỳ nguồn, hạng tốt nhất, lịch sử, thời gian theo dõi. Ba kỳ: tăng/giảm liên tiếp, ổn định khi biên độ ≤3; chỉ xác nhận rời BXH sau snapshot đủ hạng. Chưa có kỳ nguồn thì hiển thị thiếu lịch sử. |
| Playlist | Chỉ playlist editorial đã xác nhận; loại chart_mirror và unknown khỏi suy luận. Đếm playlist riêng biệt, tổng 1/log2(position+1), so cùng BXH ngày sau 1/3/7 ngày. Chỉ tính cặp đủ dữ liệu; trả số quan sát và trung vị cải thiện hạng. Không suy luận nhân quả. |
| Country | Bản đầu Spotify qua Kworb, cùng ngày nguồn, cùng top N; ID Spotify, Jaccard, rank_score theo toàn bộ chart_size, tỷ trọng stream trong toàn BXH. Thiếu bài trả null. Snapshot thiếu hạng bị loại. |
| Phản hồi | Khử trùng event_id, tách web/simulator/unknown, nhóm bài/thể loại/tỉnh/giờ VN; play/skip/like, user ID, skip/(play+skip). Không suy diễn thời lượng hay hoàn tất. Form ghi thao tác thật của người thử, không phát nhạc. |

Ở chế độ archive, form gọi `/api/listen` và lưu thao tác web cục bộ; phản hồi ghi `mode=archive`. Không nói đã gửi Kafka. Mã tỉnh phải hợp lệ (ví dụ `ha-noi`). Ở chế độ Elasticsearch, API giữ luồng gửi Kafka và Spark cập nhật index raw.

## Pipeline và cloud

- Job crawler mới `countries`, topic riêng `music.country-charts`: không đưa BXH nước ngoài vào điểm VN cũ.
- CronJob `crawl-countries` mỗi 6 giờ. Kworb lưu `source_period`, `period_type`, country, provider, URL và độ dài hạng; Spotify Hot Hits được ghi editorial, playlist bổ sung chưa xác minh là unknown.
- Speed layer bổ sung query `insight_raw` khi bật `ingest`, ghi nguyên JSON charts/playlists/events/country vào `music-insight-raw`. ID topic/partition/offset chống ghi trùng khi replay; checkpoint riêng.
- Kubernetes API đặt `INSIGHT_BACKEND=elasticsearch`. Lỗi ES trả 503, không ngầm dùng archive. API hiện có giới hạn 200.000 raw records mỗi lần đọc; cần materialized batch views trước khi dùng dữ liệu lớn vượt mức này.
- Deploy mới tạo topic và CronJob. Cluster cũ cần chạy lại job khởi tạo topic trước khi restart speed layer; apply manifest 53 và 60, cập nhật ConfigMap bằng update-code. Không xoá checkpoint HDFS.
- Để nạp bằng chứng country cũ vào ES: chuyển tiếp cổng ES, chạy `python scripts/import-insight-archive.py --es http://127.0.0.1:9200`. Import giữ nguyên ngày và có ID ổn định.

## Kiểm thử và giới hạn

Đã chạy 28/28 test đạt trên máy hiện tại. Đã kiểm tra bốn màn hình bằng trình duyệt, bảng country hiện 10 cặp quốc gia và 248 ID trong hợp top 50 của 5 nước. Một thao tác play kiểm thử với user `web-qa-20261102` đã được lưu riêng vào archive và xuất hiện đúng 1 play trong thống kê web. Đây là thao tác kiểm thử của ứng dụng, không phải lượt nghe audio thật. Hai test dùng archive thật sẽ skip nếu máy khác chưa có thư mục `out` (thư mục này không được commit).

`python -m pytest tests -q`: kiểm tra luật vòng đời, snapshot thiếu, gộp kỳ trùng, cùng kỳ giữa nước, mẫu số stream, missing không phải zero, tách nguồn và khử trùng events, playlist và khoảng trễ, API/bộ lọc.

Đối chiếu mẫu thật ngày nguồn 05/10/2026: VN–TH top 50 có 1 ID chung, Jaccard 1/99. Dữ liệu lịch sử playlist hiện chưa đủ nên trả thiếu dữ liệu; không dựng mẫu thành kết quả thật. Chưa chạy integration Kafka/Spark/ES hoặc triển khai Kubernetes trong đợt sửa này vì các dịch vụ đó chưa được khởi động. Cần kiểm chứng cụm trước khi coi thay đổi pipeline đã chạy end-to-end.
