#!/usr/bin/env bash
# Common environment for the Spark-on-Slurm cluster.
#
# Layout:
#   - Spark + JDK are baked into the container image at /opt (see image/).
#   - Runtime state (logs, pids, master discovery) lives on shared GPFS
#     so every node in the Slurm allocation sees it.

export JAVA_HOME="${JAVA_HOME:-/opt/jdk}"
export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

# Per-node runtime state, namespaced by hostname so concurrent nodes don't
# clobber each other's PID files on the shared filesystem.
export CLUSTER_DIR="${CLUSTER_DIR:-/inspire/hdd/project/video-understanding/public/personal/chwang/spark-cluster}"
export SPARK_LOG_DIR="$CLUSTER_DIR/logs/$(hostname)"
export SPARK_PID_DIR="$CLUSTER_DIR/pids/$(hostname)"
export SPARK_WORKER_DIR="$CLUSTER_DIR/work/$(hostname)"

# Master discovery: master writes its endpoint here, workers read it.
# Sidesteps DNS, which is unreliable for dynamic node IPs.
export MASTER_ENDPOINT_FILE="$CLUSTER_DIR/master.endpoint"

export SPARK_MASTER_PORT="${SPARK_MASTER_PORT:-7077}"
export SPARK_MASTER_WEBUI_PORT="${SPARK_MASTER_WEBUI_PORT:-8080}"
export SPARK_WORKER_WEBUI_PORT="${SPARK_WORKER_WEBUI_PORT:-8081}"

# Pin Spark to this node's primary intranet IP — the only address other
# nodes can route to under the dynamic-IP overlay network.
export SPARK_LOCAL_IP="$(hostname -I | awk '{print $1}')"

mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR" "$SPARK_WORKER_DIR" "$(dirname "$MASTER_ENDPOINT_FILE")"
