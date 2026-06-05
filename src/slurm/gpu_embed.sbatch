#!/bin/bash
# GPU launcher for Method 2 (trajectory embedding) — Stages 2 & 3.
#
# Stage 1 (build_sequences.py) is a Spark job and runs on the EXISTING
# CPU cluster launcher (src/slurm/cluster.sbatch). This script covers the
# parts the Spark launcher CANNOT run: PyTorch training/encoding on GPU and
# the clustering+confirmation step.
#
# Submit (single GPU node, scale --nodes / GPUS_PER_NODE up for multi-GPU):
#   SEQUENCES=/inspire/.../data/output/embed_31/sequences \
#   RAW_CSV=/inspire/.../data/input/31.csv \
#   EMBED_OUT=/inspire/.../data/output/embed_31/vectors \
#   FLEETS_OUT=/inspire/.../data/output/embed_31 \
#     sbatch src/slurm/gpu_embed.sbatch
#
# Optional: GPUS_PER_NODE, TRAIN_ARGS, CLUSTER_ARGS, PYTHON.
#
# SBATCH -o /hpc_logs/embed-%j.out
# SBATCH -e /hpc_logs/embed-%j.err
# SBATCH --nodes=1
# SBATCH --ntasks-per-node=1
# SBATCH --gres=gpu:1            # ← set to gpu:N to use N GPUs per node
# SBATCH --cpus-per-task=16
# SBATCH --mem=128G
# SBATCH --time=08:00:00

set -euo pipefail
REPO=/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop
EMBED="$REPO/src/embed"

: "${SEQUENCES:?set SEQUENCES to the Stage-1 parquet dir}"
: "${RAW_CSV:?set RAW_CSV to the original sightings CSV (for confirmation)}"
: "${EMBED_OUT:?set EMBED_OUT to where embeddings.npy should land}"
: "${FLEETS_OUT:?set FLEETS_OUT to where fleets.csv should land}"

PYTHON="${PYTHON:-python}"
GPUS_PER_NODE="${GPUS_PER_NODE:-1}"
NNODES="${SLURM_NNODES:-1}"
MASTER_ADDR=$(scontrol show hostnames "${SLURM_NODELIST:-$(hostname)}" | head -1)
RDZV_PORT="${RDZV_PORT:-29500}"

echo "[gpu-embed] nodes=$NNODES gpus/node=$GPUS_PER_NODE master=$MASTER_ADDR"
echo "[gpu-embed] sequences=$SEQUENCES -> embeddings=$EMBED_OUT"

# ── Stage 2: distributed training + encode (one torchrun per node) ───────
srun --ntasks="$NNODES" --ntasks-per-node=1 \
  "$PYTHON" -m torch.distributed.run \
    --nnodes="$NNODES" --nproc_per_node="$GPUS_PER_NODE" \
    --rdzv_backend=c10d --rdzv_endpoint="$MASTER_ADDR:$RDZV_PORT" \
    "$EMBED/train_encode.py" "$SEQUENCES" "$EMBED_OUT" ${TRAIN_ARGS:-}

# ── Stage 3: cluster + temporal confirmation (single process, rank 0) ────
# Uses cuML on GPU if importable, else scikit-learn. One process is enough;
# clustering 22 M × 256-dim vectors is seconds on a GPU.
"$PYTHON" "$EMBED/cluster_confirm.py" \
    "$EMBED_OUT" "$RAW_CSV" "$FLEETS_OUT" ${CLUSTER_ARGS:-}

echo "[gpu-embed] done — fleets at $FLEETS_OUT/fleets.csv"
