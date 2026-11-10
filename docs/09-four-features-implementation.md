# Bốn tính năng đang phục vụ

Cập nhật 10/11/2026. Đây là hợp đồng hiện tại của dashboard; [tài liệu ý tưởng 07](07-y-tuong-phan-tich-du-lieu-crawl.md) là thiết kế ban đầu, không phải danh sách màn hình đang chạy.

## Endpoint và dữ liệu

| UI | Endpoint | Dữ liệu chính |
|---|---|---|
| Thứ hạng bài hát | `GET /api/insights/lifecycle` | `chart_entry` theo nguồn/BXH/capture/kỳ nguồn |
| Bài hát trong playlist | `GET /api/insights/playlists` | `playlist_item`, metadata tên/link/vị trí/loại |
| Bản đồ âm nhạc | `GET /api/insights/countries` | BXH đủ hạng trong khoảng source period, nhiều nguồn |
| Khán giả nói gì? | `GET /api/comments/tracks`, `POST /api/comments/report` | Bình luận đã chọn theo bài/khoảng ngày/sort |

Dashboard không có tab phản hồi nghe hoặc nút gửi `/api/listen`. Không diễn giải thao tác mở link nghe thành thời lượng phát nhạc. `/legacy`, `/static/index.html`, `/static/app.js`, `/static/style.css` không thuộc giao diện bàn giao.

## Quy tắc chính

- **Vòng đời:** ba kỳ nguồn xác định để phân loại tăng/giảm/ổn định/dao động. Crawl nhiều lần cùng kỳ không tạo thêm kỳ. Chỉ xác nhận rời BXH khi kỳ tiếp theo đủ hạng và không có bài đó.
- **Playlist:** đếm playlist ID khác nhau trong từng nguồn cho một bài trong cửa sổ; gồm editorial, genre collection, unknown và chart mirror. Hiện link có thật/canonical của nguồn, không đoán Zing slug. Kết quả là quan sát membership, không phải chứng minh playlist gây tăng hạng. `analyze_playlist_exposure` là hàm legacy có test riêng, không phải bảng hiện tại.
- **Quốc gia:** mọi nước có dữ liệu hiện cùng lúc; bấm nước chỉ mở chi tiết. Tính điểm thứ hạng theo nguồn/ngày quan sát, dùng chart đủ hạng. Thiếu dữ liệu không phải 0. Các nguồn không cùng quần thể; iTunes Store không phải stream. Jaccard ở `compare_country_tastes` là hàm phân tích legacy, không phải response của bản đồ.
- **Bình luận:** giới hạn mẫu, ngữ cảnh theo bài, structured output, tần suất keyword tính lại từ văn bản. Không dùng nhãn chung để lấp chỗ trống; mô tả chỉ áp dụng cho mẫu. Quota/cache theo SQLite từng pod; cần OpenAI key backend.

## Chế độ chạy và an toàn

`INSIGHT_BACKEND=archive` đọc các JSONL trong các thư mục archive được cấu hình; dữ liệu mẫu không commit và không đổi timestamp để giả realtime. `INSIGHT_BACKEND=elasticsearch` đọc index pipeline, không tự fallback archive khi ES lỗi.

API lọc type/time ở ES, giữ legacy envelope chưa index metadata đọc được qua normalizer. Raw cap 200.000 vẫn áp dụng; vượt cap trả 422. Query/schema không hợp lệ trả 400/422, dependency không đọc được trả lỗi kiểm soát. Có một tác vụ lịch sử nặng mỗi process để giảm OOM; cold latency vẫn đáng kể.

Streaming đưa charts/playlists/country vào `music-insight-raw`, bình luận vào `music-comments`; các raw topics vẫn được ingest vào HDFS. Events/simulator đi luồng backend riêng, không trộn vào raw insight hiện tại.

Chạy/triển khai theo [hướng dẫn hiện tại](03-trien-khai.md), kiểm tra theo [tài liệu test](04-kiem-thu.md). Không dùng số liệu audit cũ làm cam kết độ phủ của một cluster mới.
