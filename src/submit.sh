#!/usr/bin/env bash
# Thin spark-submit wrapper: targets the discovered master and pins the
# client-mode driver to this pod's IP so executors can reach it.
set -euo pipefail
source /inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/src/env.sh

master="$(cat "$MASTER_ENDPOINT_FILE")"
exec "$SPARK_HOME/bin/spark-submit" \
  --master "$master" \
  --conf "spark.driver.host=$SPARK_LOCAL_IP" \
  --conf "spark.driver.bindAddress=0.0.0.0" \
  "$@"
