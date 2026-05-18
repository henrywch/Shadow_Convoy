#!/usr/bin/env bash
# Start the Spark standalone master on this pod and publish its endpoint
# to the shared discovery file.
set -euo pipefail
source /inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/src/env.sh

export SPARK_MASTER_HOST="$SPARK_LOCAL_IP"
"$SPARK_HOME/sbin/start-master.sh" \
  --host "$SPARK_MASTER_HOST" \
  --port "$SPARK_MASTER_PORT" \
  --webui-port "$SPARK_MASTER_WEBUI_PORT"

endpoint="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT"
echo "$endpoint" > "$MASTER_ENDPOINT_FILE"
echo "[master] up at $endpoint  (UI http://$SPARK_MASTER_HOST:$SPARK_MASTER_WEBUI_PORT)"
