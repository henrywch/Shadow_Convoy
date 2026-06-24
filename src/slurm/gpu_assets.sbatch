#!/bin/bash
# GPU launcher for Method 2 — Stage 4 (embedding asset services).
#
# Runs the two thin services that make the (3.83 M, 256) embedding matrix
# queryable (doc/Downstream_Tasks.md §0.5), AFTER Stages 2-3 (gpu_embed.sbatch)
# have produced vectors/ and fleets.csv:
#
#   4a  ann_index.py   — build a cuVS CAGRA ANN index ("find vehicles like X")
#   4b  project_2d.py  — cuML UMAP → 2-D scatter, colored by embedding fleet
#
# Both auto-detect GPU (cuVS / cuML) and fall back to CPU (sklearn). Use the
# CANONICAL vectors dir: embed_31/vectors/ — vectors_0610/ and vectors_0609/
# are all-NaN failed runs and the scripts will refuse them.
#
# Submit:
#   EMBED_DIR=/inspire/.../data/output/embed_31/vectors \
#   FLEETS=/inspire/.../data/output/embed_31/fleets.csv \
#   QUERY=477634,491688,3271142 \
#     sbatch src/slurm/gpu_assets.sbatch
#
# Optional: ANN_ARGS, PROJ_ARGS, PYTHON, K, MAX_POINTS.
#
# SBATCH -o /hpc_logs/embed-assets-%j.out
# SBATCH -e /hpc_logs/embed-assets-%j.err
# SBATCH --nodes=1
# SBATCH --ntasks-per-node=1
# SBATCH --gres=gpu:1
# SBATCH --cpus-per-task=16
# SBATCH --mem=128G
# SBATCH --time=02:00:00

set -euo pipefail
REPO=/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop
ASSETS="$REPO/scripts/downstream/embedding_assets"
OUT="$REPO/data/downstream/embedding_assets"

: "${EMBED_DIR:?set EMBED_DIR to the canonical vectors dir (embed_31/vectors)}"
FLEETS="${FLEETS:-$REPO/data/output/embed_31/fleets.csv}"
PYTHON="${PYTHON:-python}"
K="${K:-20}"
MAX_POINTS="${MAX_POINTS:-200000}"

mkdir -p "$OUT/ann"
echo "[gpu-assets] embed_dir=$EMBED_DIR fleets=$FLEETS out=$OUT"

# ── Stage 4a: ANN index (+ optional seed query) ──────────────────────────
"$PYTHON" "$ASSETS/ann_index.py" "$EMBED_DIR" \
    --save-index "$OUT/ann" --k "$K" \
    ${QUERY:+--query "$QUERY" --neighbors-out "$OUT/ann/neighbors.csv"} \
    ${ANN_ARGS:-}

# ── Stage 4b: 2-D projection for the dashboard scatter ───────────────────
"$PYTHON" "$ASSETS/project_2d.py" "$EMBED_DIR" \
    --fleets "$FLEETS" --max-points "$MAX_POINTS" \
    --out-dir "$OUT/projection" \
    ${PROJ_ARGS:-}

echo "[gpu-assets] done — ANN index at $OUT/ann, projection at $OUT/projection"
