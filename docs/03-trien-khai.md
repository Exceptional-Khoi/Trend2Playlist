# Triển khai

Đề bài yêu cầu "Environment: k8s or cloud (No docker)". Toàn bộ hệ thống chạy trên **Kubernetes**; không dùng
docker-compose và không build image. Code được nạp vào pod qua ConfigMap (`scripts/update-code.sh`).

## 1. Chọn môi trường

| Môi trường | Khi nào dùng | RAM cho cluster | "Phân tán" thế nào |
|---|---|---|---|
| **GKE / AKS (khuyến nghị để demo)** | máy nhóm ≤ 8 GB RAM, demo cuối kỳ | 3 node × 16 GB | phân tán thật trên 3 máy ảo; pod Kafka/HDFS/ES/Spark rải trên các node (anti-affinity) |
| minikube 1 node | máy ≥ 16 GB RAM, phát triển hằng ngày | 12 GB | phân tán ở mức tiến trình: 3 broker, 2 DataNode, 2 Spark worker là các pod độc lập |
| minikube nhiều node | máy ≥ 24 GB RAM | 3 × 6 GB | cần addon `csi-hostpath-driver` (storage mặc định của minikube không hỗ trợ đa node) |

Máy chỉ có 8 GB RAM **không chạy nổi** profile lite (Kafka + HDFS + Spark + ES cần ~8 GB request). Hãy dùng cloud,
hoặc máy của thành viên có ≥ 16 GB.

## 2. minikube trên Windows

```powershell
# 1. Cài: kubectl (đã có kèm Docker Desktop), minikube: winget install Kubernetes.minikube
# 2. Docker Desktop -> Settings -> Resources: cấp >= 12 GB RAM (backend WSL2: đặt memory=14GB trong %UserProfile%\.wslconfig)
minikube start --driver=docker --cpus=6 --memory=12g --disk-size=40g
minikube addons enable metrics-server          # cần cho HPA

cd "D:\Data Storage and Processing\Project"
copy .env.example .env                          # nên điền YOUTUBE_API_KEY để có fallback ổn định
.\scripts\run.ps1 deploy                        # ~10-15 phút lần đầu (kéo image ~4 GB)
.\scripts\run.ps1 bootstrap-data                # crawl + backfill + batch view đầu tiên (~20 phút)
minikube service -n music music-api             # mở dashboard
```

Ở đây minikube dùng Docker Desktop **làm máy ảo để chạy node Kubernetes**; ứng dụng vẫn chạy hoàn toàn trên k8s.
Nếu giảng viên yêu cầu không có Docker ở bất kỳ tầng nào, hãy dùng `--driver=hyperv` (Windows Pro) hoặc cloud.

## 3. Google Kubernetes Engine (GKE)

Tài khoản mới có $300 credit. Cụm dưới đây tốn khoảng $0,5/giờ; **xoá cụm sau khi demo**.

```bash
gcloud container clusters create music --zone asia-southeast1-b \
  --num-nodes 3 --machine-type e2-standard-4 --disk-size 50
gcloud container clusters get-credentials music --zone asia-southeast1-b

PROFILE=full ./scripts/deploy.sh      # Cloud Shell hoặc Git Bash
./scripts/bootstrap-data.sh
kubectl -n music port-forward svc/music-api 8000:80        # http://localhost:8000
# hoặc mở ra ngoài: kubectl -n music patch svc music-api -p '{"spec":{"type":"LoadBalancer"}}'

gcloud container clusters delete music --zone asia-southeast1-b   # dọn dẹp
```

AKS (Azure for Students): `az aks create -g rg -n music --node-count 3 --node-vm-size Standard_D4s_v3`,
`az aks get-credentials -g rg -n music`, rồi chạy `PROFILE=full ./scripts/deploy.sh`.

## 4. Giao diện quản trị

```bash
kubectl -n music port-forward svc/music-api 8000:80          # Dashboard + API (/docs: Swagger)
kubectl -n music port-forward svc/spark-master 8080:8080     # Spark master: worker, ứng dụng đang chạy
kubectl -n music port-forward svc/speed-layer-ui 4040:4040   # Spark UI của speed layer: tab Structured Streaming
kubectl -n music port-forward svc/hdfs-namenode 9870:9870    # HDFS: DataNode, block, dung lượng
kubectl apply -f k8s/optional/kibana.yaml && kubectl -n music port-forward svc/kibana 5601:5601
```

Lệnh hữu ích:

```bash
kubectl -n music get pods -o wide                                   # pod nằm trên node nào
kubectl -n music logs deploy/speed-layer | grep PROGRESS            # throughput streaming
kubectl -n music logs job/batch-views-init | grep "BATCH VIEWS DONE"
kubectl -n music exec hdfs-namenode-0 -- hdfs dfs -du -h /datalake/raw
kubectl -n music exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
kubectl -n music create job --from=cronjob/crawl-charts crawl-now    # crawl ngay
kubectl -n music create job --from=cronjob/crawl-trends trends-now   # tra Google Trends ngay (~15 phút)
kubectl -n music logs job/trends-now | grep "bài mốc\|nhóm"          # xem bài mốc được chọn và tiến độ
```

Sau khi sửa code: `./scripts/update-code.sh` (cập nhật ConfigMap và restart speed layer, API, bộ giả lập).

## 5. Sự cố thường gặp

| Triệu chứng | Nguyên nhân / cách xử lý |
|---|---|
| Pod `Pending` | Thiếu RAM/CPU: `kubectl -n music describe pod <pod>`. Tăng RAM minikube, hoặc giảm `listener-sim`, `spark-worker` |
| `ImagePullBackOff` | Mạng chậm. Kéo trước: `minikube image pull apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` |
| Kafka CrashLoop "cluster id mismatch" | Đã đổi `CLUSTER_ID` sau khi tạo PVC: xoá PVC `data-kafka-*` rồi deploy lại |
| DataNode "Incompatible clusterIDs" | NameNode bị format lại (mất PVC): xoá PVC `data-hdfs-datanode-*` |
| Spark "Initial job has not accepted any resources" | Tổng `spark.cores.max` của các app vượt số core worker: tăng worker hoặc giảm `SPEED_CORES_MAX`/`BATCH_CORES_MAX` |
| Speed layer restart liên tục sau khi sửa query có trạng thái | Checkpoint cũ không tương thích: đặt `CHECKPOINT_PATH=hdfs://hdfs-namenode.music.svc.cluster.local:8020/checkpoints-v2` trong ConfigMap rồi restart (hoặc xoá `/checkpoints/<query>`) |
| Speed layer OOM khi tải lớn | Bật `STATE_STORE=rocksdb` (mặc định trong ConfigMap), tăng `EXECUTOR_MEMORY`/số worker; giảm tải `SIM_EVENTS_PER_SEC` |
| Spark job trên Windows crash trong `librocksdbjni` | Chỉ dùng RocksDB trên Linux/k8s; khi test cục bộ trên Windows để trống `STATE_STORE` |
| Crawl Zing lỗi `err=-201`/chữ ký | Zing đổi khoá web: cập nhật `ZING_API_KEY`, `ZING_SECRET_KEY`, `ZING_VERSION` trong `.env` rồi deploy lại |
| YouTube Charts lỗi `429` | Điền `YOUTUBE_API_KEY`; crawler tự fallback sang `videos.list(chart=mostPopular, category=Music, region=VN)` và lưu bằng `chart_id` riêng |
| Bình luận YouTube = 0 | Bị giới hạn tạm thời: đặt `YOUTUBE_API_KEY`, hoặc giảm `YT_COMMENT_VIDEOS` |
| Job báo `PARTIAL SUCCESS` | Một nguồn lỗi nhưng nguồn khác đã ghi Kafka thành công; mặc định không chặn bootstrap. Đặt `FAIL_ON_PARTIAL=true` nếu muốn chế độ nghiêm ngặt |
| Google Trends 429 liên tục | Tăng `TRENDS_SLEEP_SEC` (60–90) hoặc giảm `TRENDS_TOP_N` trong CronJob `crawl-trends`; Google chặn theo IP nên tránh chạy nhiều lần liền |
| Tỉnh nào cũng độ tin cậy "thấp" | Chưa có số liệu Trends (CronJob chạy lúc 4h sáng): tạo job `trends-now` như trên rồi chạy lại `batch-views` |
| Dashboard không hiện bản đồ | Trình duyệt cần Internet để tải nền bản đồ Esri và thư viện ECharts/Leaflet (CDN jsDelivr) |
| Muốn làm lại từ đầu | `kubectl delete namespace music` (xoá cả PVC, tức toàn bộ dữ liệu) |

## 6. Phát triển không cần Kubernetes

```bash
pip install -r crawler/requirements.txt
python crawler/run_crawler.py --job charts --sink stdout                 # xem dữ liệu crawl
python crawler/run_crawler.py --job all --sink jsonl --jsonl-dir out      # lưu mẫu để test Spark
python -m pytest tests -q                                                # unit test

# Spark local (Linux/WSL, cần Java 17 + pyspark 3.5.9):
spark-submit local/seed_lake.py out file:///tmp/music
DATA_BASE=file:///tmp/music LOCAL_ES_OUT=/tmp/music/es_out \
  spark-submit --py-files shared/provinces.py,shared/textnorm.py,spark/jobs_common.py spark/batch_views.py
python local/load_es_out.py /tmp/music/es_out --es http://localhost:9200 --shift-now
ES_URL=http://localhost:9200 uvicorn api:app --app-dir serving                      # dashboard: http://localhost:8000
```
