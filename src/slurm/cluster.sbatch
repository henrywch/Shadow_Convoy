#!/bin/bash
# Allocate N container tasks, stand up a Spark standalone cluster across them,
# run one job in client mode from the master task, then tear everything down.
#
# Submit:
#   INPUT=/inspire/.../data/31.csv \
#   OUTPUT=/inspire/.../spark-cluster/out/basic-stats \
#     sbatch src/slurm/cluster.sbatch
#
# Optional overrides: JOB, EXECUTOR_MEMORY, EXECUTOR_CORES, DRIVER_MEMORY.
#
# SBATCH -o /hpc_logs/slurm-%j.out
# SBATCH -e /hpc_logs/slurm-%j.err
# SBATCH --ntasks=5
# SBATCH --cpus-per-task=32
# SBATCH --mem=256G
# SBATCH --time=04:00:00

set -euo pipefail
REPO=/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/src
source "$REPO/env.sh"

: "${INPUT:?set INPUT to the CSV path on GPFS}"
: "${OUTPUT:?set OUTPUT to a result directory on GPFS}"
JOB="${JOB:-$REPO/job/basic_stats.py}"

NODES=($(scontrol show hostnames "$SLURM_NODELIST"))
MASTER_NODE="${NODES[0]}"
echo "[cluster] master=$MASTER_NODE  tasks=$SLURM_NTASKS  job=$JOB"

# Stale endpoint from a prior allocation sharing CLUSTER_DIR would mislead workers.
rm -f "$MASTER_ENDPOINT_FILE"

srun --ntasks=1 --nodelist="$MASTER_NODE" "$REPO/start-master.sh"

# GPFS is fast but the file write isn't synchronous from another node's view.
for _ in $(seq 1 30); do [ -s "$MASTER_ENDPOINT_FILE" ] && break; sleep 1; done

# Tell each worker what it has to offer. Defaults tuned for 20-core / 100 GB
# nodes: keep ~2 cores and ~15 GB for OS + Spark/JVM bookkeeping + the driver
# (which shares the master node with its local worker).
export SPARK_WORKER_CORES="${SPARK_WORKER_CORES:-18}"
export SPARK_WORKER_MEMORY="${SPARK_WORKER_MEMORY:-85g}"

# One worker per allocated task — the master task also runs a worker to
# avoid wasting its cores.
srun --ntasks="$SLURM_NTASKS" "$REPO/start-worker.sh"

# Driver runs in client mode on the master task so executors can reach it.
srun --ntasks=1 --nodelist="$MASTER_NODE" \
    "$REPO/submit.sh" \
        --executor-memory "${EXECUTOR_MEMORY:-75g}" \
        --executor-cores  "${EXECUTOR_CORES:-18}" \
        --driver-memory   "${DRIVER_MEMORY:-12g}" \
        "$JOB" "$INPUT" "$OUTPUT" ${JOB_ARGS:-}

srun --ntasks="$SLURM_NTASKS" "$REPO/stop.sh"
