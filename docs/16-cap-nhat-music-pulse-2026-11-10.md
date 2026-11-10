# Music Pulse — thay đổi ngày 10/11/2026

Các thay đổi này được bàn giao trên nhánh `dev`. Dashboard mặc định là `/`, dùng `serving/static/insights.html`. Theo yêu cầu dọn repository trước khi bàn giao, các asset legacy `index.html`, `app.js`, `style.css` và server `demo_dashboard.py` đã được xóa.

## Tính năng và giao diện

- Giao diện theo mẫu nền tím tối, sidebar và đĩa vinyl. Chữ chính 16–19px, phụ 14–15px; ô bảng trên điện thoại 16px. Bỏ ghi chú lặp dài, thu gọn bằng chứng bình luận.
- Playlist: tính cả playlist nhỏ, chưa phân loại và chart mirror trong bảng membership hiện hành; khử trùng theo nguồn/playlist ID, hiện tên/link/loại/vị trí. Công thức phân tích exposure legacy vẫn được kiểm tra riêng.
- Bản đồ: bỏ dropdown quốc gia; chọn một nước mở thẻ chi tiết, không loại các nước khác. Nguồn có Spotify, Apple Music, iTunes Store, YouTube/Kworb và Zing. Cấu hình tối đa 76 market, mỗi feed lấy top 20, có timeout/circuit breaker. Không coi điểm BXH là thị phần stream toàn cầu.
- Bình luận: đưa tên bài/nghệ sĩ có sẵn vào ngữ cảnh model, ưu tiên cụm từ/chi tiết riêng có bằng chứng, loại nhãn chung và đếm tần suất từ mẫu. Kết quả không đại diện toàn bộ khán giả.
- Spark giữ metadata type/thời gian/kỳ nguồn cho truy vấn raw và artists cho bình luận. Đường checkpoint recommendation ưu tiên `RECOMMEND_CHECKPOINT_PATH` nếu đã cấu hình.
- API có nơi ghi SQLite báo cáo nằm ngoài code ConfigMap; storage/quota này theo pod, không phải quota toàn cluster.

## Sửa OOM và mất kết nối

Trên cụm thử nghiệm, API từng bị `OOMKilled` khi hai truy vấn lịch sử chạy đồng thời trong process giới hạn 1GiB. Port-forward đang gắn vào pod đó mất kết nối theo.

API mới giới hạn một tác vụ raw-history nặng mỗi process, tái kiểm tra cache sau khi chờ và giải phóng raw list trước JSON validation. Snapshot dedup giữ digest SHA256, không giữ thêm toàn bộ chuỗi JSON cho mỗi record/candidate. Health/static không dùng khóa tác vụ này. Giới hạn raw 200.000 và validation/error contract vẫn giữ.

Đã kiểm tra hai request lifecycle/countries bắt đầu đồng thời trên cùng một pod GKE: cả hai HTTP 200, 76 nước/16.420 kết quả vòng đời trong lượt đo; peak cgroup khoảng 642,5MiB/1GiB, không restart thêm trong 80,2 giây quan sát. Thời gian phản hồi cold là 38,4 và 80,3 giây, đã gồm thời gian chờ. Đây là lượt kiểm tra có giới hạn, không phải soak test hoặc chứng nhận mọi dataset đều hết OOM. Batch views cho lịch sử lớn và độ trễ cold vẫn cần cải thiện.

## Test được cập nhật theo yêu cầu mới

- Regression HTTP đồng thời, health vẫn phản hồi, cache dùng lại cho request trùng; giới hạn allocation snapshot và deterministic tie order.
- Crawler public dùng fixtures không cần API key; Apple hỗ trợ ngày ISO/RFC2822, YouTube HTML giữ UTF-8.
- G12 xác nhận asset cũ `app.js`/`index.html`/`style.css`, `/legacy` và các đường traversal trả 404. Không còn hai phiên bản giao diện được bàn giao song song.
- Test cloud/DOM thay các giả định đã bỏ: mirror bị loại, dropdown quốc gia, selector bản đồ và nội dung mặc định cũ. Kiểm tra pagination, chống chèn HTML, response đến muộn và retry bình luận vẫn giữ.

## Dọn file trước bàn giao

Xóa demo/static cũ, bản PDF/ZIP/ảnh và hướng dẫn GCP trùng phiên bản, patch theo commit cũ và các audit không còn đại diện trạng thái hiện tại. Các script fault/scalability cũ sửa trực tiếp namespace `music` được thay bằng runner cloud có scope/ownership guard; bỏ script kiểm tra archive ngày cố định và helper chiến dịch cũ không còn dùng. Không xóa testcase đang thực thi để làm xanh suite.

README, PROJECT_PLAN, kiến trúc, deploy, bốn tính năng và kiểm thử được cập nhật thống nhất. Tài liệu đề cương/ý tưởng/dữ liệu mẫu cần cho báo cáo môn học có cảnh báo rõ ở đầu; không dùng làm hợp đồng UI hoặc số liệu cloud. Dữ liệu, `.env` và backup cá nhân không được push. Script update-code chủ động bỏ ba khóa static đã nghỉ khỏi ConfigMap để lần deploy sau không giữ asset cũ do patch thủ công trước đây.

Kiểm tra local thông thường:

```bash
python -m pip install -r requirements-dev.txt
python -m pytest tests -q
```

Cloud tests chỉ opt-in bằng runner/scope được duyệt. Test cần dịch vụ hoặc browser chưa có phải ghi SKIP/BLOCKED, không coi là đã kiểm chứng E2E. Không đưa `.env`, kubeconfig, snapshot cluster, ZIP bàn giao hoặc thư mục backup lên Git.
