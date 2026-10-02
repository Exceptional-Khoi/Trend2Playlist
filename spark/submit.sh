#!/bin/bash
# Dùng chung cho mọi Spark driver chạy trong pod k8s (client mode, executor chạy trên spark-worker).
#   bash /opt/app/submit.sh <file.py> <tên-app> <số core tối đa>
# Biến môi trường lấy từ ConfigMap music-env: SPARK_MASTER, SPARK_PACKAGES, KAFKA_BOOTSTRAP, ES_NODES, DATA_BASE...
set -euo pipefail
APP_FILE=$1
APP_NAME=$2
CORES=${3:-2}
export HOME=/tmp PYSPARK_PYTHON=python3
exec /opt/spark/bin/spark-submit \
  --master "$SPARK_MASTER" --deploy-mode client --name "$APP_NAME" \
  --driver-memory "${DRIVER_MEMORY:-600m}" \
  --conf spark.driver.host="$POD_IP" --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=7078 --conf spark.blockManager.port=7079 \
  --conf spark.cores.max="$CORES" --conf spark.executor.cores=1 \
  --conf spark.executor.memory="${EXECUTOR_MEMORY:-512m}" \
  --conf spark.sql.shuffle.partitions="${SHUFFLE_PARTITIONS:-6}" \
  --conf spark.jars.ivy=/tmp/.ivy2 --packages "$SPARK_PACKAGES" \
  --conf spark.hadoop.dfs.replication="${HDFS_REPLICATION:-2}" \
  --conf spark.sql.streaming.minBatchesToRetain=20 \
  --conf spark.executorEnv.TZ_NAME="${TZ_NAME:-Asia/Ho_Chi_Minh}" \
  --py-files /opt/app/provinces.py,/opt/app/textnorm.py,/opt/app/jobs_common.py \
  "/opt/app/$APP_FILE"
