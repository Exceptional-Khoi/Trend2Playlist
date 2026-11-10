# Deploy và cập nhật code

## GKE trên tài khoản/project mới

Làm theo [hướng dẫn từng bước hiện tại](15-trien-khai-tai-khoan-gcp-khac.md): Billing → project → service account → cụm Standard → code → deploy → crawl → Web Preview. Không dùng lệnh có project của người bàn giao cho tài khoản khác.

Trong đúng thư mục source, đúng Kubernetes context:

```bash
kubectl config current-context
PROFILE=full bash scripts/deploy.sh
```

Profile full có cấu hình worker một slot, RAM API 1GiB và HDFS checkpoint service. Không chạy profile lite trên cụm full chỉ để cập nhật UI: script deploy còn áp lại cấu hình và Job init.

## Chạy Kubernetes cục bộ

Cần đủ tài nguyên cho các service trong manifest. Profile lite nhắm cluster khoảng 12GiB RAM trống; kiểm tra requests thực tế và resource hệ thống, không mặc định máy RAM 8GB chạy được toàn bộ stack.

```bash
PROFILE=lite bash scripts/deploy.sh
```

Trên Windows dùng Git Bash, hoặc `scripts/run.ps1 deploy -Profile full` cho cluster cloud đã chọn đúng context. Shell script dùng LF theo `.gitattributes`.

## Cập nhật ứng dụng đã chạy

```bash
git status --short
git rev-parse HEAD
kubectl config current-context
bash scripts/update-code.sh
kubectl -n music rollout status deployment/music-api --timeout=10m
kubectl -n music rollout status deployment/speed-layer --timeout=15m
```

`git pull` chỉ đổi source trên máy; không tự đổi ConfigMap hoặc process đang chạy. Script update nạp code mới và restart những workload đã khai báo. CronJob dùng code mới ở lượt sau. Cần đánh giá thay đổi state/schema trước khi restart Spark; không xóa checkpoint/PVC. Nếu API đã được cấu hình Secret `music-openai` riêng, giữ tham chiếu đó khi áp lại manifest gốc.

## Mở web

Chạy trên laptop có kubectl đã xác thực:

```bash
kubectl -n music port-forward svc/music-api 8000:80
```

Giữ terminal chạy và mở http://127.0.0.1:8000. Ctrl+C chỉ ngắt đường nối, không tắt cluster. Nếu pod được chọn restart/reschedule, port-forward có thể kết thúc; kết nối lại và kiểm tra lý do restart. Cloud Shell dùng `8080:80` và Web Preview theo hướng dẫn GKE, không dùng localhost của laptop để truy cập terminal ở Google.

## Dữ liệu và lỗi

Tạo Job từ các CronJob charts/playlists/countries, sau đó comments theo mục 7 hướng dẫn GKE. Không cần chạy backfill nghe giả lập 150.000 event để mở bốn tab hiện tại.

Pod Pending: `kubectl -n music describe pod TEN_POD`. Pod restart: xem `logs --previous` và `lastState` để phân biệt OOM, lỗi Python và dependency. HDFS safe mode cần kiểm tra block/dung lượng/checkpoint; không ép leave hoặc xóa dữ liệu để bỏ lỗi.

Nghỉ/bật lại theo [quy trình hiện tại](17-tam-dung-va-bat-lai-gke.md). Đóng browser không giảm số VM; không dùng lệnh xóa cluster như thao tác nghỉ hằng ngày.
