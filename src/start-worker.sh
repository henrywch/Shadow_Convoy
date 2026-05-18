#!/usr/bin/env bash
# Join this pod to the cluster as a worker. Reads the master endpoint from
# the shared discovery file — no DNS required.
set -euo pipefail
source /inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/src/env.sh

if [ ! -s "$MASTER_ENDPOINT_FILE" ]; then
  echo "[worker] no master endpoint at $MASTER_ENDPOINT_FILE — start the master first" >&2
  exit 1
fi
master="$(cat "$MASTER_ENDPOINT_FILE")"

: "${SPARK_WORKER_CORES:=$(nproc)}"
: "${SPARK_WORKER_MEMORY:=256g}"
export SPARK_WORKER_CORES SPARK_WORKER_MEMORY

"$SPARK_HOME/sbin/start-worker.sh" \
  --host "$SPARK_LOCAL_IP" \
  --webui-port "$SPARK_WORKER_WEBUI_PORT" \
  "$master"

echo "[worker] joined $master  (cores=$SPARK_WORKER_CORES mem=$SPARK_WORKER_MEMORY)"
