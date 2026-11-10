# Tạm dừng và bật lại GKE

Áp dụng cho cluster **test riêng**, profile full, một node pool `default-pool`, ba node và chưa bật autoscaling node pool như hướng dẫn dựng mới. Nếu cấu hình khác hoặc có workload chung, không chạy nguyên lệnh này. Tài liệu là quy trình thao tác, không tự tắt cluster khi đọc.

**Đóng tab/port-forward không tắt VM.** Đưa node về 0 vẫn giữ cluster/PVC và có thể còn phí quản lý/đĩa. Xem [giá GKE](https://cloud.google.com/kubernetes-engine/pricing) và [resize Standard cluster](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/resizing-a-cluster).

## 1. Chọn đúng đích và lưu trạng thái trước khi nghỉ

Trong Cloud Shell đã tạo file biến theo hướng dẫn tài khoản mới:

```bash
source "$HOME/music-project.env"
export MUSIC_NODE_POOL="default-pool"
export MUSIC_STATE="$HOME/music-ops/${MUSIC_PROJECT_ID}-${MUSIC_CLUSTER}"
gcloud container clusters get-credentials "$MUSIC_CLUSTER" \
  --project="$MUSIC_PROJECT_ID" --zone="$MUSIC_ZONE"
kubectl config current-context
gcloud container node-pools describe "$MUSIC_NODE_POOL" \
  --cluster="$MUSIC_CLUSTER" --project="$MUSIC_PROJECT_ID" --zone="$MUSIC_ZONE" \
  --format='value(autoscaling.enabled)'
mkdir -p "$MUSIC_STATE"
```

Kiểm tra project/context và node pool không bật autoscaling. Nếu `before-pause.json` đã có từ lần nghỉ chưa phục hồi, không ghi đè; tiếp tục kiểm tra trạng thái lần đó trước.

```bash
test ! -e "$MUSIC_STATE/before-pause.json" && \
  kubectl -n music get deployment,statefulset,cronjob -o json > "$MUSIC_STATE/before-pause.json"
test -s "$MUSIC_STATE/before-pause.json"
```

Giữ file trạng thái riêng ở Cloud Shell; không commit snapshot cluster vào Git.

## 2. Ngừng lịch và chờ Job đang chạy

```bash
for task in $(kubectl -n music get cronjob -o name); do
  kubectl -n music patch "$task" --type=merge -p '{"spec":{"suspend":true}}'
done
kubectl -n music get jobs
```

Suspend không dừng Job đang chạy. Đợi tất cả Job active kết thúc hoặc xử lý lỗi bằng log trước khi tiếp tục; không xóa dữ liệu/PVC để dừng Job.

## 3. Dừng ứng dụng rồi node

```bash
kubectl -n music scale deployment/music-api statefulset/listener-sim --replicas=0
kubectl -n music wait --for=delete pod -l 'app in (music-api,listener-sim)' --timeout=5m
kubectl -n music scale deployment/speed-layer --replicas=0
kubectl -n music wait --for=delete pod -l app=speed-layer --timeout=5m
kubectl -n music scale deployment/spark-worker deployment/spark-master deployment/hdfs-checkpoint --replicas=0
kubectl -n music wait --for=delete pod -l 'app in (spark-worker,spark-master,hdfs-checkpoint)' --timeout=5m
kubectl -n music scale statefulset/kafka statefulset/hdfs-datanode statefulset/hdfs-namenode statefulset/elasticsearch --replicas=0
kubectl -n music wait --for=delete pod -l 'app in (kafka,hdfs-datanode,hdfs-namenode,elasticsearch)' --timeout=10m
```

Nếu không còn pod nên `wait` báo không tìm thấy, xác minh bằng `get pods`. Nếu timeout còn Terminating, xử lý trước khi resize. API HPA cấu hình min > 0 ngừng điều chỉnh target được đặt thủ công về 0; không apply lại deployment trong lúc nghỉ. [Kubernetes maintenance mode](https://v1-35.docs.kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/#implicit-maintenance-mode-deactivation).

```bash
gcloud container clusters resize "$MUSIC_CLUSTER" --node-pool="$MUSIC_NODE_POOL" \
  --num-nodes=0 --project="$MUSIC_PROJECT_ID" --zone="$MUSIC_ZONE"
gcloud compute instance-groups managed list --project="$MUSIC_PROJECT_ID" \
  --format='table(name,zone.basename(),targetSize)'
kubectl -n music get deployment,statefulset,cronjob,pvc
```

Đọc đích trong câu hỏi xác nhận của gcloud. Thành công khi node pool đúng về 0, workload 0, lịch suspend, PVC còn nguyên. Không xóa namespace hoặc volume để nghỉ.

## 4. Bật lại

Mở tab mới thì chạy lại các biến ở mục 1 nhưng **không ghi đè snapshot**. Sau đó:

```bash
gcloud container clusters resize "$MUSIC_CLUSTER" --node-pool="$MUSIC_NODE_POOL" \
  --num-nodes=3 --project="$MUSIC_PROJECT_ID" --zone="$MUSIC_ZONE"
kubectl get nodes
```

Đợi ba node Ready. Khởi động dữ liệu trước:

```bash
kubectl -n music scale statefulset/kafka statefulset/hdfs-datanode statefulset/elasticsearch --replicas=3
kubectl -n music scale statefulset/hdfs-namenode --replicas=1
for task in kafka hdfs-namenode hdfs-datanode elasticsearch; do
  kubectl -n music rollout status "statefulset/$task" --timeout=15m
done
kubectl -n music exec hdfs-namenode-0 -- hdfs dfsadmin -safemode get
```

HDFS phải rời safe mode bình thường sau kiểm tra block. Nếu còn ON, xem log/dung lượng/Live DataNodes; không ép leave để bỏ lỗi.

```bash
kubectl -n music scale deployment/hdfs-checkpoint deployment/spark-master --replicas=1
kubectl -n music scale deployment/spark-worker --replicas=3
kubectl -n music rollout status deployment/spark-master --timeout=10m
kubectl -n music rollout status deployment/spark-worker --timeout=10m
kubectl -n music scale deployment/speed-layer --replicas=1
kubectl -n music rollout status deployment/speed-layer --timeout=15m
kubectl -n music scale deployment/music-api --replicas=2
kubectl -n music scale statefulset/listener-sim --replicas=3
kubectl -n music rollout status deployment/music-api --timeout=10m
```

Khôi phục lịch đúng trạng thái đã lưu, kể cả lịch vốn suspend. Chạy trong cùng tab có `MUSIC_STATE`:

```bash
python3 - <<'PY'
import json, os, pathlib, subprocess
path=pathlib.Path(os.environ['MUSIC_STATE'])/'before-pause.json'
snapshot=json.loads(path.read_text())
for item in snapshot['items']:
    if item['kind']!='CronJob': continue
    body={'spec':{'suspend':item['spec'].get('suspend',False),'startingDeadlineSeconds':600}}
    subprocess.run(['kubectl','-n','music','patch','cronjob',item['metadata']['name'],
                    '--type=merge','-p',json.dumps(body)],check=True)
PY
kubectl -n music get deployment,statefulset,cronjob,pvc
```

Sau khi kiểm tra đủ dịch vụ và query tiếp tục tiến độ, đổi tên file trạng thái để lần nghỉ sau có bản chụp mới:

```bash
mv "$MUSIC_STATE/before-pause.json" "$MUSIC_STATE/resumed-$(date -u +%Y%m%d%H%M%S).json"
```

Mở lại web theo [hướng dẫn deploy](03-trien-khai.md). Không bootstrap hoặc xóa checkpoint chỉ để bật lại. Giữ PVC không bảo đảm sự kiện Kafka tồn tại vô hạn sau retention.
