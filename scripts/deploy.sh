#!/usr/bin/env bash
# Triển khai toàn bộ hệ thống lên cluster Kubernetes mà kubectl đang trỏ tới (minikube / GKE / AKS...).
#   ./scripts/deploy.sh               # profile lite (minikube, ~12GB RAM)
#   PROFILE=full ./scripts/deploy.sh  # profile full (cloud, 3 node x 16GB)
# Trên Windows chạy bằng Git Bash, hoặc dùng scripts/deploy.ps1.
set -euo pipefail
export MSYS_NO_PATHCONV=1            # Git Bash: không tự đổi đường dẫn /opt/... thành C:/...
cd "$(dirname "$0")/.."
PROFILE=${PROFILE:-lite}
NS=music
k() { kubectl -n "$NS" "$@"; }
step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

step "Cluster: $(kubectl config current-context)  |  profile: $PROFILE"
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-config.yaml
if [ -f .env ]; then
  k create secret generic music-secrets --from-env-file=.env --dry-run=client -o yaml | kubectl apply -f -
fi

step "Nạp code vào ConfigMap (không build image Docker)"
./scripts/update-code.sh --no-restart

step "Kafka (3 broker KRaft), HDFS, Elasticsearch"
kubectl apply -f k8s/10-kafka.yaml -f k8s/20-hdfs.yaml
if [ "$PROFILE" = "full" ]; then
  kubectl apply -f k8s/profiles/full/30-elasticsearch.yaml
  k patch configmap music-env --type merge -p '{"data":{"SPEED_CORES_MAX":"4","BATCH_CORES_MAX":"4","EXECUTOR_MEMORY":"1g","HDFS_REPLICATION":"3","ES_REPLICAS":"1"}}'
else
  kubectl apply -f k8s/30-elasticsearch.yaml
fi
k rollout status statefulset/kafka --timeout=15m
k rollout status statefulset/hdfs-namenode --timeout=15m
k rollout status statefulset/elasticsearch --timeout=15m

step "Tạo Kafka topic + ES index template"
k delete job kafka-init-topics es-setup --ignore-not-found
kubectl apply -f k8s/11-kafka-topics.yaml -f k8s/31-es-setup.yaml
k wait --for=condition=complete job/kafka-init-topics job/es-setup --timeout=10m

step "Spark cluster (master + worker)"
kubectl apply -f k8s/40-spark-cluster.yaml
if [ "$PROFILE" = "full" ]; then
  k scale statefulset/hdfs-datanode --replicas=3
  k set env deployment/spark-worker WORKER_MEMORY=3g WORKER_CORES=3   # 3 worker x 3 core = 9 >= speed 4 + batch 4
  k set resources deployment/spark-worker --requests=cpu=1,memory=3Gi --limits=memory=4Gi
  k scale deployment/spark-worker --replicas=3
fi
k rollout status statefulset/hdfs-datanode --timeout=15m
k rollout status deployment/spark-master --timeout=10m
k rollout status deployment/spark-worker --timeout=10m

step "Speed layer, batch CronJob, crawler, bộ giả lập, API + dashboard"
kubectl apply -f k8s/41-speed-layer.yaml -f k8s/42-batch-jobs.yaml -f k8s/50-crawlers.yaml \
              -f k8s/51-simulator.yaml -f k8s/60-serving.yaml
if [ "$PROFILE" = "full" ]; then
  k scale statefulset/listener-sim --replicas=3
fi
k rollout status deployment/music-api --timeout=10m

step "Xong. Trạng thái:"
k get pods -o wide
cat <<EOF

Tiếp theo:
  1) Tạo dữ liệu ban đầu:   ./scripts/bootstrap-data.sh
  2) Mở dashboard:          minikube service -n music music-api
                            (hoặc) kubectl -n music port-forward svc/music-api 8000:80  ->  http://localhost:8000
  3) Giao diện hệ thống:    kubectl -n music port-forward svc/spark-master 8080:8080     (Spark master UI)
                            kubectl -n music port-forward svc/hdfs-namenode 9870:9870    (HDFS NameNode UI)
                            kubectl -n music port-forward svc/speed-layer-ui 4040:4040   (Spark Streaming UI)
EOF
