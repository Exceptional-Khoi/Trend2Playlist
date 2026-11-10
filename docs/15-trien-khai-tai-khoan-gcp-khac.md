# Dựng Trend2Playlist trên tài khoản Google Cloud khác

Đối chiếu mã tại máy ngày **10/11/2026**. Đây là hướng dẫn dựng **hệ thống mới**: tài khoản mới → project mới → GKE mới → code hiện tại → crawl lại dữ liệu. Không chuyển Billing, khóa API hoặc dữ liệu lịch sử của cụm cũ.

Tất cả lệnh bên dưới chạy trong **Google Cloud Shell**, không chạy bằng PowerShell trên laptop. Tài liệu này chưa được thực thi trên một tài khoản mới; các script và cấu hình được đối chiếu với source hiện tại. Không có tài nguyên cloud mới nào được tạo khi soạn hướng dẫn.

## 1. Đăng nhập tài khoản mới, tạo project và bật Billing

1. Mở cửa sổ trình duyệt riêng hoặc ẩn danh để tránh nhầm tài khoản.
2. Vào [Google Cloud Console](https://console.cloud.google.com/), đăng nhập tài khoản nhận bàn giao.
3. Bấm ô chọn project cạnh logo → **New Project** → đặt tên, ví dụ `Music Pulse Demo` → **Create**.
4. Ghi **Project ID** của project vừa tạo. Không dùng `trendtoplaylist`: đó là project cũ.
5. Vào **Billing**, tạo hoặc chọn Billing account đang hoạt động rồi liên kết với project mới. Project cần Billing hoạt động để dùng GKE. [Hướng dẫn Billing của Google](https://docs.cloud.google.com/billing/docs/how-to/modify-project).

Nếu tài khoản đủ điều kiện có thể đăng ký [Free Trial](https://cloud.google.com/free); kiểm tra credit và ngày hết hạn trong Billing, không mặc định đổi email là có ưu đãi mới. Nếu thẻ bị declined hoặc Billing closed thì xử lý Billing trước, không tiếp tục tạo cluster. [Điều kiện trial](https://docs.cloud.google.com/free/docs/free-cloud-features).

## 2. Mở Cloud Shell và chọn project

Bấm biểu tượng **`>_`** góc trên bên phải Console, gần chuông. Nếu không thấy, vào [Cloud Shell](https://shell.cloud.google.com/). Bấm **Authorize** khi được hỏi.

Thay đúng Project ID ở dòng đầu rồi dán cả khối:

```bash
export MUSIC_PROJECT_ID="DIEN_PROJECT_ID_MOI"
export MUSIC_ZONE="asia-southeast1-b"
export MUSIC_CLUSTER="music"

gcloud config set project "$MUSIC_PROJECT_ID"
gcloud auth list --filter=status:ACTIVE
gcloud billing projects describe "$MUSIC_PROJECT_ID"
```

Phải thấy email tài khoản mới và `billingEnabled: true`. Tên cụm `music` có thể giống cụm cũ vì project khác nhau.

Lưu các biến không chứa bí mật để dùng ở tab sau:

```bash
printf 'export MUSIC_PROJECT_ID=%q\nexport MUSIC_ZONE=%q\nexport MUSIC_CLUSTER=%q\n' \
  "$MUSIC_PROJECT_ID" "$MUSIC_ZONE" "$MUSIC_CLUSTER" > "$HOME/music-project.env"
```

Trong **mỗi tab Cloud Shell mới** chạy `source "$HOME/music-project.env"` trước khi dùng các biến trên.

## 3. Bật API và tạo danh tính cho node

```bash
gcloud services enable compute.googleapis.com container.googleapis.com iam.googleapis.com \
  --project="$MUSIC_PROJECT_ID"

gcloud iam service-accounts create music-gke-nodes \
  --display-name="Music Pulse GKE nodes" --project="$MUSIC_PROJECT_ID"

export MUSIC_NODE_SA="music-gke-nodes@${MUSIC_PROJECT_ID}.iam.gserviceaccount.com"
gcloud projects add-iam-policy-binding "$MUSIC_PROJECT_ID" \
  --member="serviceAccount:${MUSIC_NODE_SA}" \
  --role="roles/container.defaultNodeServiceAccount" --condition=None
```

Nếu service account đã tồn tại trong project mới thì dùng lại, không tạo trùng. Với tài khoản tổ chức, cần người quản trị cấp quyền tạo cụm, tạo/gán service account và dùng service account node; không tự cấp Editor để vượt lỗi quyền. Không cần tải khóa JSON cho service account. [Cấu hình node service account](https://docs.cloud.google.com/kubernetes-engine/security/configure-node-service-accounts).

## 4. Tạo cụm GKE mới

**Bước này bắt đầu tạo tài nguyên tính phí hoặc tiêu hao credit.** Cấu hình cùng quy mô demo đang dùng: 3 máy `e2-standard-4`, mỗi máy 4 vCPU/16GB RAM, boot disk 50GB; các PVC dữ liệu sẽ được tạo thêm khi deploy. Kiểm tra quota CPU và đĩa ở Singapore trước; thiếu quota thì dừng để xử lý, không giảm máy tùy ý vì các dịch vụ có thể không đủ chỗ.

```bash
gcloud container clusters create "$MUSIC_CLUSTER" \
  --project="$MUSIC_PROJECT_ID" \
  --zone="$MUSIC_ZONE" \
  --node-locations="$MUSIC_ZONE" \
  --num-nodes=3 \
  --machine-type=e2-standard-4 \
  --disk-type=pd-standard \
  --disk-size=50 \
  --release-channel=regular \
  --service-account="$MUSIC_NODE_SA"
```

Đợi `RUNNING`, rồi kết nối:

```bash
gcloud container clusters get-credentials "$MUSIC_CLUSTER" \
  --project="$MUSIC_PROJECT_ID" --zone="$MUSIC_ZONE"
kubectl config current-context
kubectl get nodes
```

Context phải chứa **Project ID mới** và phải có 3 node `Ready`. `get-credentials` đổi context hiện hành; kiểm tra trước mỗi lần deploy. Nếu tổ chức không có VPC `default`, cần network/subnet phù hợp do quản trị viên cung cấp. [Tạo cụm zonal](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/creating-a-zonal-cluster), [kết nối kubectl](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl).

## 5. Đưa đúng code mới lên Cloud Shell

Nhánh `dev` được cập nhật kèm tài liệu này có giao diện mới, nguồn crawl bổ sung và bản sửa API hết RAM. Với một thư mục Cloud Shell mới, có thể lấy code bằng Git:

```bash
cd "$HOME"
git clone --branch dev --single-branch https://github.com/Exceptional-Khoi/Trend2Playlist.git trend2playlist-gcp
cd "$HOME/trend2playlist-gcp"
git rev-parse HEAD
```

Ghi commit SHA và đối chiếu SHA người bàn giao cung cấp. Sau đó bỏ qua phần upload/giải nén, chạy bước chuẩn hóa xuống dòng bên dưới rồi sang mục 6.

Nếu nhận bàn giao bằng ZIP: **`Trend2Playlist-GCP-current-2026-11-10.zip`** nằm trong `handover/` trên máy người bàn giao, được gửi riêng chứ không đưa lên Git. Gói không có `.env`, `.git`, dữ liệu crawl, kubeconfig hay khóa tài khoản.

Không chỉ `git clone dev` nếu các thay đổi hiện tại chưa được commit/push: giao diện có thể quay về bản cũ. Sau khi đã bàn giao bằng Git, có thể clone đúng commit được người bàn giao xác nhận thay cho ZIP.

Trong Cloud Shell, bấm **⋮ → Upload file**, chọn ZIP. Sau upload:

```bash
cd "$HOME"
unzip Trend2Playlist-GCP-current-2026-11-10.zip
cd "$HOME/trend2playlist-gcp"

# File có thể xuất phát từ Windows; chuẩn hóa xuống dòng của shell scripts.
find scripts spark -type f -name '*.sh' -exec sed -i 's/\r$//' {} +
chmod +x scripts/*.sh spark/submit.sh
```

Đây là thư mục mới cho lần dựng đầu; không giải nén đè một checkout đang có chỉnh sửa. `HANDOVER_MANIFEST.json` trong gói ghi SHA256 từng file để đối chiếu phiên bản.

## 6. Deploy toàn bộ hệ thống

Đầu tiên có thể tạo `.env` rỗng từ mẫu; không chép `.env` của người khác:

```bash
cp .env.example .env
kubectl config current-context
PROFILE=full bash scripts/deploy.sh
```

Chờ script hoàn tất. Code đi vào ConfigMap, dùng các image nền đã khai báo; không cần build image ứng dụng riêng. Script dựng Kafka, HDFS và checkpoint service, Elasticsearch, Spark master/worker/streaming, API và các lịch crawl/batch. Profile `full` có cấu hình worker và RAM API phù hợp với quy mô này.

```bash
kubectl -n music get pods
kubectl -n music get pvc
kubectl -n music rollout status deployment/music-api --timeout=10m
kubectl -n music rollout status deployment/speed-layer --timeout=15m
kubectl -n music logs deployment/speed-layer --tail=60
```

Các dịch vụ phải Ready, PVC phải `Bound`. Streaming hiện có các query `ingest_raw`, `insight_raw`, `comment_samples`, `rt_plays`, `rt_mentions`, `rt_charts`, `recommend` khi bật đủ cấu hình mặc định. Cụm mới tạo checkpoint riêng từ dữ liệu mới; không copy checkpoint hỏng hoặc đường khôi phục riêng của cluster cũ.

Nếu bị ngắt giữa chừng, xem trạng thái resource và lỗi trước khi chạy lại toàn bộ deploy. Script có tạo lại Job init; không dùng nó để “bật web” mỗi lần mở máy.

## 7. Crawl dữ liệu cho dashboard hiện tại

Cluster mới ban đầu không có lịch sử. Bốn tính năng hiện tại là **Thứ hạng bài hát / Bài hát trong playlist / Bản đồ âm nhạc / Khán giả nói gì?**; không cần sinh 150.000 sự kiện nghe giả lập để các tab này có dữ liệu.

```bash
export MUSIC_RUN="$(date -u +%Y%m%d%H%M%S)"
kubectl -n music create job --from=cronjob/crawl-charts "first-charts-$MUSIC_RUN"
kubectl -n music create job --from=cronjob/crawl-playlists "first-playlists-$MUSIC_RUN"
kubectl -n music create job --from=cronjob/crawl-countries "first-countries-$MUSIC_RUN"
kubectl -n music get jobs
```

Để lấy bình luận, chạy sau khi charts đã có dữ liệu và Spark đang xử lý:

```bash
kubectl -n music create job --from=cronjob/crawl-comments "first-comments-$MUSIC_RUN"
kubectl -n music get jobs
```

Lượt crawl bình luận theo cấu hình hiện tại có 3 shard, tối đa 60 video trong cấu hình crawler; có thể mất nhiều phút hoặc bị nguồn giới hạn. Xem log:

```bash
kubectl -n music logs "job/first-countries-$MUSIC_RUN" --tail=30
kubectl -n music logs "job/first-comments-$MUSIC_RUN" --tail=30
kubectl -n music logs deployment/speed-layer --tail=40
```

Job Indexed có nhiều pod; lệnh logs có thể chỉ chọn một pod, cần `get pods` để xem các shard khác. Đợi Spark ghi ES rồi bấm **Khám phá**. Một số nước/nguồn có thể chưa lấy được; không mặc định cụm mới ngay lập tức có đủ 76 nước như ảnh cụm cũ. Vòng đời cần nhiều kỳ nguồn, không tự tạo đủ lịch sử bằng cách crawl lại cùng kỳ.

`scripts/bootstrap-data.sh` là bootstrap rộng hơn có backfill nghe giả lập, Trends và batch; chỉ dùng nếu thực sự cần luồng legacy đó. Không chạy nhiều bootstrap đồng thời; script có xóa/tạo lại các Job tên cố định.

## 8. Mở web trên tài khoản mới

Trong **tab Cloud Shell riêng**:

```bash
source "$HOME/music-project.env"
gcloud container clusters get-credentials "$MUSIC_CLUSTER" \
  --project="$MUSIC_PROJECT_ID" --zone="$MUSIC_ZONE"
kubectl -n music port-forward --address=0.0.0.0 svc/music-api 8080:80
```

Giữ terminal chạy. Bấm **Web Preview → Preview on port 8080** trên thanh Cloud Shell. Đây là link HTTPS của phiên tài khoản mới. `localhost:8000` trên laptop đang dùng cho cụm cũ không tự trỏ sang cụm mới; không dùng lại link đó. Nếu port-forward mất kết nối, mở lại lệnh. [Web Preview chính thức](https://docs.cloud.google.com/shell/docs/using-web-preview).

Trên cùng địa chỉ preview, `/api/health` kiểm tra ES, `/docs` xem API. Muốn xem Spark/HDFS, mở tab terminal riêng và forward `svc/spark-master 8081:8080`, `svc/hdfs-namenode 9870:9870`, `svc/speed-layer-ui 4040:4040`, rồi đổi port trong Web Preview.

## 9. Khóa phân tích bình luận — tùy chọn

Dashboard/BXH/playlist/bản đồ mở được mà không có OpenAI key. Nút phân tích bình luận cần khóa backend của người nhận; chi phí API này tách khỏi credit GCP. Không dùng chung khóa người bàn giao một cách mặc định.

Tạo Secret riêng chỉ backend tham chiếu. Nhập khóa tại terminal; ký tự không hiện lên màn hình, không đưa khóa vào Git hoặc chat:

```bash
read -rsp "Nhap OPENAI_API_KEY: " MUSIC_OPENAI_KEY
printf '\n'
printf '%s' "$MUSIC_OPENAI_KEY" | kubectl -n music create secret generic music-openai \
  --from-file=OPENAI_API_KEY=/dev/stdin --dry-run=client -o yaml | kubectl apply -f -
unset MUSIC_OPENAI_KEY

kubectl -n music set env deployment/music-api --from=secret/music-openai
kubectl -n music rollout status deployment/music-api --timeout=10m
```

Không bật `set -x` khi nhập khóa. Lệnh `set env` tạo tham chiếu Secret cho API; không gắn Secret này vào crawler/Spark. Nếu chạy lại `deploy.sh` với manifest gốc, cần áp lại tham chiếu `music-openai` vì manifest mặc định trỏ `music-secrets`. Giới hạn/cache báo cáo hiện là theo pod, không phải quota toàn cụm.

## 10. Chi phí, nghỉ và chuyển dữ liệu

- Đóng laptop/Cloud Shell không dừng VM GKE. Cụm mới bắt đầu chi phí riêng; cụm cũ vẫn chạy nếu chưa tắt.
- Đặt budget trong **Billing → Budgets**. Loại Alerts-only chỉ cảnh báo, không tự dừng chi tiêu; nếu dùng Spend cap cần kiểm tra rõ dịch vụ/phạm vi được hỗ trợ. [Tài liệu budgets](https://docs.cloud.google.com/billing/docs/how-to/budgets).
- Khi muốn tạm dừng, lưu trạng thái workload, dừng lịch/job và ứng dụng trước, rồi mới giảm node pool về 0; giữ PVC vẫn có chi phí đĩa và cluster có thể còn phí quản lý. Không copy lệnh có project `trendtoplaylist` từ tài liệu cụm cũ sang tài khoản mới.
- Làm theo [quy trình tạm dừng/bật lại hiện tại](17-tam-dung-va-bat-lai-gke.md), dùng đúng project của cụm mới; bao gồm `hdfs-checkpoint` và khôi phục lịch CronJob theo trạng thái đã lưu.
- Muốn mang cả lịch sử cũ sang cần kế hoạch export/import HDFS/ES và kiểm tra nhất quán riêng. ZIP code không chuyển dữ liệu, PVC, Secret hay tiến độ Kafka.

## Lỗi thường gặp

| Hiện tượng | Kiểm tra |
|---|---|
| Billing not found / API 403 | Project mới đã liên kết Billing hoạt động và tài khoản có quyền chưa? |
| Quota exceeded | Quota CPU/E2 CPU, đĩa/IP đúng region; không lặp tạo cluster liên tục |
| Pod Pending | `kubectl -n music describe pod TEN_POD`: thiếu RAM/CPU, PVC, scheduling |
| CrashLoopBackOff | `kubectl -n music logs TEN_POD --previous --tail=80` |
| Giao diện cũ | Dùng đúng ZIP; ConfigMap chứa code mới; Ctrl+F5; không chỉ git pull bản chưa có thay đổi |
| Giao diện chưa có dữ liệu | Xem Job crawl, Kafka/Spark và ngày chọn; không chỉ nhìn Pod Running |
| Phân tích bình luận 503/502 | Secret/khóa/quota API, có bình luận trong kỳ chưa; không gửi khóa qua chat |
| Cloud Shell reconnect | Các Job trên GKE vẫn có thể chạy; reconnect và xem trạng thái trước khi tạo lại |
