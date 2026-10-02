#!/usr/bin/env bash
# Tạo dữ liệu ban đầu ngay sau khi deploy (không phải đợi CronJob tới giờ):
#   crawl BXH + playlist -> backfill bình luận & lịch sử nghe -> chạy batch view + similarity lần đầu.
set -euo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
NS=music
k() { kubectl -n "$NS" "$@"; }
step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
run_from_cron() {  # tạo Job từ CronJob với tên cố định (xoá bản cũ nếu có)
  k delete job "$2" --ignore-not-found >/dev/null
  k create job --from="cronjob/$1" "$2"
}

step "Crawl BXH + playlist từ Zing MP3, Spotify, Apple Music, YouTube -> Kafka"
run_from_cron crawl-charts crawl-charts-init
run_from_cron crawl-playlists crawl-playlists-init
k wait --for=condition=complete job/crawl-charts-init job/crawl-playlists-init --timeout=20m
k logs job/crawl-charts-init --tail=3

step "Backfill: bình luận YouTube (3 pod song song, chạy nền ~15-20 phút) + 7 ngày lịch sử nghe + Google Trends theo tỉnh"
k delete job comments-backfill events-backfill --ignore-not-found >/dev/null
kubectl apply -f k8s/52-bootstrap-jobs.yaml
run_from_cron crawl-trends crawl-trends-init          # ~15 phút (chạy chậm để Google không chặn)
k wait --for=condition=complete job/events-backfill --timeout=30m
k wait --for=condition=complete job/crawl-trends-init --timeout=90m || echo "(Google Trends chưa xong - batch sẽ dùng khi có)"

step "Đợi speed layer ghi dữ liệu từ Kafka xuống HDFS (trigger 1 phút)"
sleep 90
k exec hdfs-namenode-0 -- hdfs dfs -du -h /datalake/raw || true

step "Batch view + item similarity lần đầu"
run_from_cron batch-views batch-views-init
k wait --for=condition=complete job/batch-views-init --timeout=45m
k logs job/batch-views-init | grep "BATCH VIEWS DONE" || true
run_from_cron batch-similarity batch-similarity-init
k wait --for=condition=complete job/batch-similarity-init --timeout=45m
k logs job/batch-similarity-init | grep "BATCH SIMILARITY DONE" || true

step "Xong. Bình luận vẫn đang được backfill; batch-views sẽ tự chạy lại mỗi giờ để cập nhật."
k get jobs
