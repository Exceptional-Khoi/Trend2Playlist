#!/usr/bin/env bash
# KỊCH BẢN KIỂM THỬ KHẢ NĂNG MỞ RỘNG - tăng tải / tăng tài nguyên và đo lại.
#   ./scripts/test-scalability.sh              # tất cả
#   ./scripts/test-scalability.sh stream       # stream | batch | crawl | api | hdfs
set -uo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
NS=music
k() { kubectl -n "$NS" "$@"; }
say() { printf '\n\033[1;34m### %s\033[0m\n' "$*"; }
pause() { read -r -p $'\n[Enter] để tiếp tục... ' _ || true; }
streaming() {  # throughput của các streaming query (speed layer ghi mỗi 30 giây)
  k exec deploy/music-api -- python -c "
import json,urllib.request
d=json.load(urllib.request.urlopen('http://localhost:8000/api/realtime/streaming?minutes=5'))['latest']
for q in sorted(d, key=lambda x: x['query']):
    print(f\"{q['query']:12} batch={q['batch_id']:>6}  vào={q['input_rps']:8.1f} dòng/s  xử lý={q['processed_rps']:8.1f} dòng/s  thời gian batch={(q['duration_ms'] or 0)/1000:5.1f}s\")"
}
job_seconds() {  # thời gian chạy của 1 Job
  k get job "$1" -o jsonpath='{.status.startTime}{" "}{.status.completionTime}' | python -c "
import sys,datetime as d
a,b=sys.stdin.read().split(); f=lambda s: d.datetime.fromisoformat(s.replace('Z','+00:00'))
print(f'{(f(b)-f(a)).total_seconds():.0f} giây')"
}

stream() {
  say "1. STREAMING: tăng tải sự kiện nghe x3 (2 -> 6 replica bộ giả lập)"
  streaming
  pause
  k scale statefulset/listener-sim --replicas=6
  echo "đợi 3 phút cho tải ổn định..."; sleep 180
  streaming
  echo "-> Nếu 'xử lý' < 'vào' kéo dài: speed layer thiếu tài nguyên. Tăng worker + core cho speed layer:"
  pause
  k scale deployment/spark-worker --replicas=4
  k patch configmap music-env --type merge -p '{"data":{"SPEED_CORES_MAX":"4"}}'
  k rollout status deployment/spark-worker --timeout=10m
  k rollout restart deployment/speed-layer    # driver mới xin thêm executor, tiếp tục từ checkpoint
  echo "đợi 3 phút..."; sleep 180
  streaming
  echo "(trả lại tải ban đầu)"; k scale statefulset/listener-sim --replicas=2
}

batch() {
  say "2. BATCH: cùng 1 job batch-views với 2 core và 4 core"
  k patch configmap music-env --type merge -p '{"data":{"BATCH_CORES_MAX":"2"}}'
  k delete job scale-batch-2 --ignore-not-found; k create job --from=cronjob/batch-views scale-batch-2
  k wait --for=condition=complete job/scale-batch-2 --timeout=60m; echo "2 core: $(job_seconds scale-batch-2)"
  pause
  k scale deployment/spark-worker --replicas=4; k rollout status deployment/spark-worker --timeout=10m
  k patch configmap music-env --type merge -p '{"data":{"BATCH_CORES_MAX":"4"}}'
  k delete job scale-batch-4 --ignore-not-found; k create job --from=cronjob/batch-views scale-batch-4
  k wait --for=condition=complete job/scale-batch-4 --timeout=60m; echo "4 core: $(job_seconds scale-batch-4)"
  k logs job/scale-batch-4 | grep "BATCH VIEWS DONE" | head -c 400; echo
}

crawl() {
  say "3. CRAWL: crawl bình luận với 3 pod rồi 6 pod song song (Indexed Job)"
  for n in 3 6; do
    k delete job "scale-crawl-$n" --ignore-not-found
    k create job --from=cronjob/crawl-comments "scale-crawl-$n" --dry-run=client -o json | python -c "
import json,sys; j=json.load(sys.stdin); n=$n
j['spec']['completions']=n; j['spec']['parallelism']=n
for c in j['spec']['template']['spec']['containers']:
    c['env']=[e for e in c.get('env',[]) if e['name']!='SHARD_COUNT']+[{'name':'SHARD_COUNT','value':str(n)}]
print(json.dumps(j))" | kubectl apply -f -
    k wait --for=condition=complete "job/scale-crawl-$n" --timeout=60m
    echo "$n pod: $(job_seconds scale-crawl-$n)"
  done
}

api() {
  say "4. API: tạo tải lớn, HPA tự tăng số pod music-api (cần metrics-server)"
  k get hpa music-api
  pause
  k create configmap loadtest-code --from-file=scripts/loadtest.py --dry-run=client -o yaml | kubectl apply -f -
  k delete pod loadtest --ignore-not-found
  cat <<'EOF' | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata: { name: loadtest, namespace: music }
spec:
  restartPolicy: Never
  containers:
    - name: loadtest
      image: python:3.12-slim
      command: ["sh", "-c", "pip install -q httpx==0.28.1 && python /t/loadtest.py --concurrency 60 --seconds 240"]
      volumeMounts: [{ name: t, mountPath: /t }]
  volumes: [{ name: t, configMap: { name: loadtest-code } }]
EOF
  for i in $(seq 1 10); do sleep 30; k get hpa music-api --no-headers; done
  k logs loadtest --tail=5
  k get pods -l app=music-api
}

hdfs() {
  say "5. HDFS: thêm 1 DataNode -> dung lượng tăng, cân bằng lại block"
  k exec hdfs-namenode-0 -- hdfs dfsadmin -report | grep -E "Configured Capacity|Live datanodes" | head -3
  pause
  n=$(k get statefulset hdfs-datanode -o jsonpath='{.spec.replicas}')
  k scale statefulset/hdfs-datanode --replicas=$((n + 1))
  k rollout status statefulset/hdfs-datanode --timeout=10m
  sleep 20
  k exec hdfs-namenode-0 -- hdfs dfsadmin -report | grep -E "Configured Capacity|Live datanodes" | head -3
  k exec hdfs-namenode-0 -- hdfs balancer -threshold 10 | tail -3
}

case "${1:-all}" in
  all) stream; batch; crawl; api; hdfs ;;
  stream) stream ;; batch) batch ;; crawl) crawl ;; api) api ;; hdfs) hdfs ;;
  *) echo "usage: $0 [all|stream|batch|crawl|api|hdfs]"; exit 1 ;;
esac
