#!/usr/bin/env bash
# Đóng gói code Python/HTML vào ConfigMap rồi khởi động lại các pod đang chạy để nhận code mới.
#   ./scripts/update-code.sh              # cập nhật + restart
#   ./scripts/update-code.sh --no-restart
set -euo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
NS=music
SHARED=(--from-file=shared/provinces.py --from-file=shared/textnorm.py)
cm() {
  local name=$1; shift
  kubectl -n "$NS" create configmap "$name" "$@" --dry-run=client -o yaml | kubectl apply -f -
}
cm crawler-code --from-file=crawler/ "${SHARED[@]}"
cm spark-code --from-file=spark/ "${SHARED[@]}"
cm serving-code --from-file=serving/api.py --from-file=serving/es_setup.py --from-file=serving/requirements.txt "${SHARED[@]}"
cm dashboard-static --from-file=serving/static/

if [ "${1:-}" != "--no-restart" ]; then
  for r in deployment/speed-layer deployment/music-api statefulset/listener-sim; do
    kubectl -n "$NS" get "$r" >/dev/null 2>&1 && kubectl -n "$NS" rollout restart "$r"
  done
  echo "CronJob (crawler, batch) tự dùng code mới ở lần chạy kế tiếp."
fi
