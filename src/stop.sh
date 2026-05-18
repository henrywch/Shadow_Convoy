#!/usr/bin/env bash
# Stop whichever Spark daemons are running on this pod.
set -euo pipefail
source /inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/src/env.sh
"$SPARK_HOME/sbin/stop-worker.sh" 2>/dev/null || true
"$SPARK_HOME/sbin/stop-master.sh" 2>/dev/null || true
