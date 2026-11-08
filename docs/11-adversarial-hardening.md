# Quy tắc dữ liệu và vận hành sau kiểm thử đối kháng

Bản sửa này áp dụng cho main `4ab6c756adb36ba96ec7c67883ce448bff928817`.
Test và bằng chứng trước sửa được giữ nguyên trong bộ bàn giao QA. Không cần tạo
lại cụm Kubernetes chỉ để cập nhật source; dùng quy trình cập nhật ConfigMap/image
và rollout của repository sau khi kiểm thử, với môi trường đích được xác định rõ.

## Snapshot và thống kê

- Snapshot được tách theo provider, **nền tảng source**, chart, quốc gia và loại kỳ.
  Không ghép Apple/Spotify chỉ vì provider hoặc ID giống nhau.
- Bản ghi giống hệt nhau trong cùng snapshot chỉ được đếm một lần. Các ID/rank
  xung đột không được dùng làm bằng chứng snapshot đầy đủ. Một snapshot chứa rank
  không hợp lệ vẫn là incomplete sau khi loại các dòng sai.
- Chỉ snapshot có chart_size nguyên dương được khai báo nhất quán, đủ ID riêng và
  đủ hạng 1..N mới chứng minh bài đã rời BXH. Không dùng ngày crawl thay kỳ nguồn.
- Recrawl cùng kỳ chỉ chọn một snapshot nguyên vẹn mới nhất; không union các lần
  crawl thiếu. Nếu timestamp bằng nhau, tie-break bằng nội dung chuẩn hóa để độc
  lập thứ tự input, không tự tạo thêm kỳ quan sát.
  snapshot_id của crawler có thể là bucket30 phút: phải tách thêm capture timestamp.
  Cùng ID có rank xung đột trong một capture chỉ giữ một observation và đánh dấu
  snapshot incomplete, không được biến các dòng đó thành nhiều kỳ nguồn.
- Rank/position loại boolean, số không dương và rank vượt chart_size. Playlist
  chỉ dùng editorial; nếu có playlist_size thì position phải nằm trong [1,size].
- So sánh quốc gia yêu cầu cùng chart family và kỳ daily. Có thể khai báo
  chart_family; mặc định bỏ mã quốc gia khỏi tên chart để nhận diện family.
- Stream phải hữu hạn, không âm. Khi một metric còn thiếu, denominator của cả
  chart là chưa biết và stream share trả null; không thay missing bằng zero.
- Timestamp naive được hiểu là UTC; ts_ms=0 là epoch hợp lệ. Timestamp ngoài
  phạm vi datetime được báo lỗi dữ liệu, không thành lỗi server không kiểm soát.

## Thao tác người dùng

- /api/listen yêu cầu user_id không trống, tối đa 100 ký tự, province/action hợp
  lệ và track tồn tại trong catalog của backend. ES lookup lỗi trả503; không tìm
  thấy trả404. Trong ES mode, catalog là music-tracks: cần batch catalog được nạp
  trước khi nhận thao tác cho một track mới. Archive dùng chart entries đã lưu.
- Web click chỉ là thao tác. listen_ms/duration_ms đều null, không suy diễn rằng
  bấm play là đã nghe hết hoặc skip là nghe 8 giây.
- Event ID giống hệt payload được khử trùng. Nếu cùng ID nhưng payload khác,
  toàn bộ ID đó bị loại khỏi thống kê và xuất trong conflicting_event_ids. Không
  chọn source/action tùy thứ tự đến. Đây không phải cơ chế idempotency của HTTP:
  mỗi POST hiện vẫn tạo một UUID mới; retry client có thể tạo thao tác mới.
- Archive JSONL append có lock giữa thread và process (msvcrt trên Windows,
  flock trên Unix), flush và fsync trước phản hồi thành công. Các writer phải
  dùng cùng helper/lock; chưa cam kết filesystem mạng không hỗ trợ advisory lock.

## API và deployment

- INSIGHT_BACKEND chỉ nhận archive/elasticsearch, giá trị khác trả503. ES lỗi
  không fallback sang archive. Dữ liệu JSON/envelope sai trả422; lỗi IO trả503.
- as_of/chart_date chỉ nhận YYYY-MM-DD hợp lệ. Mã quốc gia kiểm danh mục ISO
  alpha-2; các giới hạn query có cả lower/upper bounds.
- /api/health là readiness: ES mode yêu cầu ES green/yellow; archive mode không
  phụ thuộc ES. /api/live chỉ kiểm process phục vụ. Kubernetes liveness dùng
  /api/live để tránh restart hàng loạt khi ES bị gián đoạn.
- Spark master tắt Service links, tránh SPARK_MASTER_PORT dạng tcp:// do
  Kubernetes chèn vào. Stateful recommend_batch tiêu thụ toàn bộ DataFrame qua
  count trước callback và luôn unpersist trong finally, kể cả callback return sớm.
- Khi triển khai bản query-state đã sửa, nếu checkpoint cũ đã hỏng thì cần chọn
  CHECKPOINT_PATH mới và đối chiếu offset/replay trong môi trường test trước.
  Bản sửa không tự xóa hoặc sửa checkpoint đang có.
- es_replace đọc các run_id cũ trước write và chỉ xóa đúng tập generation đó
  sau write thành công. Không dùng must_not current_run vì có thể xóa batch mới.
  Cleanup có version conflicts=proceed. Cách này bảo vệ khỏi stale cleanup;
  publication vẫn chưa phải transaction/atomic index swap, và không có cam kết
  phân xử mọi tình huống các writer cùng overwrite một ID đồng thời.

## Ingest và legacy

- Không phát track thiếu title/artist; Kworb từ chối rank sai hoặc ID/rank trùng;
  pagination YouTube dừng token lặp và khử trùng comment ID.
- JSONL/Kafka không serialize NaN/Infinity. Tên topic và ID file local được
  kiểm tra để không ghi vượt thư mục output.
- Track key Latin được giữ ổn định. Với script không Latin, thêm hash của tên
  chuẩn hóa thay vì gộp tất cả thành unknown. Nếu đã lưu các khóa unknown cũ,
  cần reingest/rebuild view có kiểm soát; không tự hợp nhất khóa cũ với khóa mới.
- Resolver legacy giữ remix/live/edition trong ngoặc, tách nghệ sĩ bằng separator
  có boundary và xử lý Đ/đ. Representative cùng platform chọn theo event_time
  rồi best rank, cuối cùng lexical record để kết quả độc lập thứ tự file; output
  các set được sort ổn định. Không suy ra mọi recording cùng title/artist là một.
- Dữ liệu Apple fallback mang synthetic/provenance rõ ràng, bị loại khỏi entity
  resolution. Không gọi sample là bằng chứng chart thực tế.

## Giới hạn xác minh

Test local bao gồm ASGI HTTP, ES, Kafka, Spark state/checkpoint và browser DOM
với phạm vi ghi rõ trong từng case. Các scenario full Kafka→Spark→HDFS→ES→API,
watermark/fault/restart toàn cụm vẫn cần harness riêng. Không gọi các test bị
BLOCKED là PASS hoặc kết luận exactly-once từ các test thành phần.
