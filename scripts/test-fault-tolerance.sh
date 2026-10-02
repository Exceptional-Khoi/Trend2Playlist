#!/usr/bin/env bash
# KỊCH BẢN KIỂM THỬ CHỊU LỖI - mỗi bước: xem trạng thái -> "giết" 1 thành phần -> quan sát hệ thống tự phục hồi.
#   ./scripts/test-fault-tolerance.sh            # chạy lần lượt tất cả
#   ./scripts/test-fault-tolerance.sh kafka      # chỉ 1 kịch bản: kafka | hdfs | worker | driver | master | es | api
set -uo pipefail
export MSYS_NO_PATHCONV=1
NS=music
k() { kubectl -n "$NS" "$@"; }
say() { printf '\n\033[1;34m### %s\033[0m\n' "$*"; }
pause() { read -r -p $'\n[Enter] để tiếp tục... ' _ || true; }
es() { k exec deploy/music-api -- python -c "import urllib.request,sys;print(urllib.request.urlopen('http://elasticsearch:9200$1',timeout=10).read().decode()[:600])"; }
events_last_min() {  # số lượt nghe (theo speed layer) ở phút gần nhất có dữ liệu
  k exec deploy/music-api -- python -c "
import json,urllib.request
d=json.load(urllib.request.urlopen('http://localhost:8000/api/realtime/timeline?minutes=10'))['items']
print([(x['minute']//60000%60, x['plays']+x['skips']) for x in d][-6:])"
}

kafka() {
  say "1. KAFKA: tắt 1 trong 3 broker (replication 3, min.insync 2)"
  k exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic music.events | head -4
  pause
  k delete pod kafka-1 --wait=false
  sleep 10
  echo "-> ISR chỉ còn 2 replica, leader chuyển sang broker khác; producer (acks=all) vẫn ghi được:"
  k exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic music.events | head -4
  k logs statefulset/listener-sim --tail=3
  echo "-> số sự kiện/phút vẫn liên tục:"; events_last_min
  k wait --for=condition=ready pod/kafka-1 --timeout=5m
  sleep 20
  echo "-> kafka-1 quay lại, tự đồng bộ, ISR đủ 3:"
  k exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic music.events | head -4
}

hdfs() {
  say "2. HDFS: tắt 1 DataNode (dfs.replication = 2)"
  k exec hdfs-namenode-0 -- hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes|Name:" || true
  k exec hdfs-namenode-0 -- hdfs fsck /datalake | tail -12
  pause
  k delete pod hdfs-datanode-1 --wait=false
  sleep 5
  echo "-> dữ liệu vẫn đọc được nhờ bản sao trên DataNode còn lại:"
  f=$(k exec hdfs-namenode-0 -- bash -c "hdfs dfs -ls -R /datalake/raw | grep parquet | head -1 | awk '{print \$NF}'")
  k exec hdfs-namenode-0 -- hdfs dfs -ls "$f"
  k exec hdfs-namenode-0 -- bash -c "hdfs dfs -cat '$f' | wc -c"
  echo "-> pod DataNode được StatefulSet tạo lại, gắn lại đúng PVC cũ:"
  k wait --for=condition=ready pod/hdfs-datanode-1 --timeout=5m
  sleep 30
  k exec hdfs-namenode-0 -- hdfs fsck /datalake | tail -12
}

worker() {
  say "3. SPARK WORKER: tắt 1 worker đang chạy executor của speed layer"
  k get pods -l app=spark-worker -o wide
  pause
  w=$(k get pods -l app=spark-worker -o jsonpath='{.items[0].metadata.name}')
  k delete pod "$w" --wait=false
  echo "-> task của executor bị mất được Spark chạy lại trên executor khác; Deployment tạo worker mới:"
  sleep 40
  k get pods -l app=spark-worker
  k logs deploy/speed-layer --tail=200 | grep PROGRESS | tail -5
  events_last_min
}

driver() {
  say "4. SPARK DRIVER (speed layer): tắt driver -> phục hồi từ checkpoint trên HDFS, không mất/không trùng dữ liệu"
  k logs deploy/speed-layer --tail=400 | grep PROGRESS | tail -5
  pause
  k delete pod -l app=speed-layer --wait=false
  echo "-> trong lúc driver chết, crawler & simulator vẫn ghi vào Kafka (Kafka giữ dữ liệu)..."
  sleep 30
  k get pods -l app=speed-layer
  k rollout status deploy/speed-layer --timeout=10m
  echo "-> đợi driver mới đọc checkpoint và xử lý phần tồn đọng (batch_id tiếp nối, không bắt đầu lại từ 0):"
  sleep 120
  k logs deploy/speed-layer --tail=400 | grep PROGRESS | tail -5
  echo "-> các phút lúc driver chết đã được bù đủ (ghi ES theo id cố định -> idempotent):"
  events_last_min
}

master() {
  say "5. SPARK MASTER: tắt master -> ứng dụng đang chạy không bị ảnh hưởng, master mới khôi phục trạng thái từ PVC"
  pause
  k delete pod -l app=spark-master --wait=false
  sleep 20
  k logs deploy/speed-layer --tail=100 | grep PROGRESS | tail -3
  k rollout status deploy/spark-master --timeout=5m
  sleep 20
  k logs deploy/spark-master --tail=30 | grep -iE "recover|registered|ALIVE" || true
}

esnode() {
  say "6. ELASTICSEARCH: tắt 1 node (profile full: 3 node, replica 1 -> vẫn phục vụ; lite: 1 node -> dữ liệu còn trên PVC)"
  es "/_cat/nodes?v"; es "/_cluster/health"
  pause
  k delete pod elasticsearch-0 --wait=false
  sleep 15
  es "/_cluster/health" || echo "(lite: API tạm lỗi trong lúc ES khởi động lại)"
  k wait --for=condition=ready pod/elasticsearch-0 --timeout=10m
  es "/_cat/indices/music-*?v&h=index,health,docs.count"
}

api() {
  say "7. API: tắt 1 trong 2 replica -> Service chuyển request sang pod còn lại"
  k get pods -l app=music-api
  pause
  p=$(k get pods -l app=music-api -o jsonpath='{.items[0].metadata.name}')
  k delete pod "$p" --wait=false
  echo "-> gọi API 10 lần liên tiếp từ trong cluster (mã HTTP):"
  k exec listener-sim-0 -- python -c "
import time, urllib.request
for _ in range(10):
    try: print(urllib.request.urlopen('http://music-api/api/health', timeout=3).status, end=' ', flush=True)
    except Exception as e: print('x', end=' ', flush=True)
    time.sleep(1)"
  echo; k get pods -l app=music-api
}

case "${1:-all}" in
  all) kafka; hdfs; worker; driver; master; esnode; api ;;
  kafka) kafka ;; hdfs) hdfs ;; worker) worker ;; driver) driver ;; master) master ;; es) esnode ;; api) api ;;
  *) echo "usage: $0 [all|kafka|hdfs|worker|driver|master|es|api]"; exit 1 ;;
esac
