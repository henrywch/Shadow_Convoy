# Method 2 — Trajectory-Embedding Companion Detection

> Implementation of Method 2 from `doc/ML_Approaches.md` (§3, §8.4): a t2vec-style
> denoising autoencoder that embeds each plate's route, clusters the embeddings, and
> confirms which clusters are *actual companions* (not just same-route vehicles).
> Hybrid by design — **Spark/CPU for ETL, GPU for the model and clustering** (`doc/ML_Approaches.md §8`).

## Pipeline

```
                 ┌─ Stage 1: build_sequences.py ─┐   Spark / CPU
 31.csv ───────► │ prefilter plates → ordered    │ ──────────────► sequences/  (Parquet)
 (276 M rows)    │ (camera,time) sequence/plate  │                 plate, cameras[], times[], n
                 └───────────────────────────────┘
                 ┌─ Stage 2: train_encode.py ────┐   PyTorch / GPU
 sequences/ ───► │ denoising GRU seq2seq AE;      │ ──────────────► vectors/
                 │ encode every plate → vector   │                 embeddings.npy, plates.npy,
                 └───────────────────────────────┘                 vocab.json, model.pt
                 ┌─ Stage 3: cluster_confirm.py ─┐   cuML(GPU)/sklearn(CPU)
 vectors/ ─────► │ cluster embeddings → groups;  │ ──────────────► fleets.csv
 + 31.csv        │ confirm via co-occurrence     │                 cluster_id, n_plates,
                 └───────────────────────────────┘                 n_cooccur_windows, confirmed, members
                 ┌─ Stage 4: asset services ─────┐   cuVS / cuML (GPU), sklearn (CPU)
 vectors/ ─────► │ 4a ann_index.py  : ANN search │ ──────────────► ann/{cagra.idx, neighbors.csv}
 + fleets.csv    │ 4b project_2d.py : UMAP → 2-D │                 projection/{projection.csv, .png}
                 └───────────────────────────────┘
```

Stage 4 makes the `(3.83 M, 256)` matrix *queryable* (doc/Downstream_Tasks.md §0.5):
`ann_index.py` answers "find vehicles like plate X" via a cuVS CAGRA index (sklearn
brute-cosine on CPU); `project_2d.py` lays the fleet space out in 2-D (cuML UMAP on GPU,
t-SNE/PCA on CPU), colored by embedding fleet. Both are read-only consumers — run them
after Stage 3. As downstream consumers they live under
**`scripts/downstream/embedding_assets/`** and write to **`data/downstream/embedding_assets/`**
(launcher: `src/slurm/gpu_assets.sbatch`).

> **Canonical vectors dir is `embed_31/vectors/`.** `vectors_0610/` and `vectors_0609/`
> are all-NaN failed Stage-2 runs; every Stage-3/4 script now drops non-finite rows and
> refuses an all-NaN matrix with a pointer to `vectors/`.

**Output is a *cluster* of plates** (a fleet), same shape as `graph_communities.py`'s
`fleets.csv` — not a route/sequence (that's MaxGrowth). The sequence is consumed to build
the embedding; the result is the grouping. Stage 3's confirmation is what distinguishes a
real companion group from "two commuters with the same route a week apart."

## What runs on which launcher

| Stage | Script | Engine | Launcher | Current scripts cover it? |
|---|---|---|---|---|
| 1 | `build_sequences.py` | PySpark | **`src/slurm/cluster.sbatch`** (existing, unchanged) | **Yes** |
| 2 | `train_encode.py` | PyTorch (CUDA) | **`src/slurm/gpu_embed.sbatch`** (new) | No — needed a GPU launcher |
| 3 | `cluster_confirm.py` | cuML / sklearn | **`src/slurm/gpu_embed.sbatch`** (new) | No — runs after Stage 2 on the GPU node |

The existing Spark launcher runs Stage 1 verbatim (it just spark-submits `$JOB`). Stages 2–3
are not Spark jobs, so they get a dedicated GPU Slurm script that allocates `--gres=gpu` and
drives PyTorch via `torchrun` (multi-GPU/multi-node ready).

## Running it

```bash
REPO=/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop

# ── Stage 1 (Spark, existing launcher) ──────────────────────────────────
INPUT=$REPO/data/input/31.csv \
OUTPUT=$REPO/data/output/embed_31/sequences \
JOB=$REPO/src/embed/build_sequences.py \
JOB_ARGS="--min-observations 10 --min-visits 3 --max-plates 500000 --max-len 128" \
  sbatch $REPO/src/slurm/cluster.sbatch

# ── Stages 2+3 (new GPU launcher) ───────────────────────────────────────
SEQUENCES=$REPO/data/output/embed_31/sequences \
RAW_CSV=$REPO/data/input/31.csv \
EMBED_OUT=$REPO/data/output/embed_31/vectors \
FLEETS_OUT=$REPO/data/output/embed_31 \
GPUS_PER_NODE=1 \
TRAIN_ARGS="--epochs 15 --batch-size 512 --hidden 256 --emb-dim 128 --drop-prob 0.3" \
CLUSTER_ARGS="--algo hdbscan --min-cluster-size 3 --min-cooccur-windows 3" \
  sbatch $REPO/src/slurm/gpu_embed.sbatch

# ── Stage 4 — asset services (ANN + 2-D projection) ─────────────────────
EMBED_DIR=$REPO/data/output/embed_31/vectors \
FLEETS=$REPO/data/output/embed_31/fleets.csv \
QUERY=477634,491688,3271142 \
  sbatch $REPO/src/slurm/gpu_assets.sbatch

# …or run a service directly (auto-detects GPU, falls back to CPU):
.venv-gpu/bin/python scripts/downstream/embedding_assets/ann_index.py \
    $REPO/data/output/embed_31/vectors \
    --query 477634 --k 20 --save-index $REPO/data/downstream/embedding_assets/ann
.venv-gpu/bin/python scripts/downstream/embedding_assets/project_2d.py \
    $REPO/data/output/embed_31/vectors \
    --fleets $REPO/data/output/embed_31/fleets.csv --max-points 200000 \
    --out-dir $REPO/data/downstream/embedding_assets/projection
```

## CPU / GPU behavior

The code auto-detects hardware so the **data path is testable on a CPU box** and the
**compute accelerates transparently on GPU**:

- **Stage 2** uses `cuda` if `torch.cuda.is_available()`, else CPU. Multi-GPU is enabled
  automatically when `gpu_embed.sbatch` launches it under `torchrun` (reads `RANK`/`WORLD_SIZE`).
- **Stage 3** imports `cuml.cluster` (GPU HDBSCAN/KMeans/DBSCAN) if present, else falls back
  to `sklearn.cluster`. Same flags either way.

Install the CUDA torch wheel + RAPIDS on the GPU instance (see `requirements-gpu.txt`). On
GPU, HDBSCAN over millions of vectors is seconds; on CPU it is the slow step (use
`--algo kmeans` for CPU smoke tests).

### Two environments

| venv | python | for | notes |
|---|---|---|---|
| `.venv`     | 3.12 | CPU testing | torch-cpu, sklearn, pyspark, pyarrow |
| `.venv-gpu` | 3.12 | GPU runs | CUDA-12 torch + RAPIDS cuML/cuDF + cuVS (GPU ANN) |

`.venv-gpu` is built from `requirements-gpu.txt` (CUDA 12.x). It is assembled on the CPU box
for convenience, but GPU libraries (cuML/cuDF) **cannot be import-verified without a CUDA
device** — finalize and verify on the GPU instance:

```bash
.venv-gpu/bin/pip install -r src/embed/requirements-gpu.txt \
    --extra-index-url https://download.pytorch.org/whl/cu124 \
    --extra-index-url https://pypi.nvidia.com
.venv-gpu/bin/python -c "import torch; print(torch.cuda.is_available())"   # expect True on GPU
.venv-gpu/bin/python -c "import cuml; print(cuml.__version__)"             # GPU-only import
```

Adjust the CUDA suffix (`cu121`/`cu128`, RAPIDS `-cu12`) to match your GPU instance.

## Smoke-tested (CPU, this repo)

All three stages verified end-to-end on a 2 M-row slice of `day1.csv` (CPU-only box,
no GPU): Stage 1 produced 211,742 prefiltered sequences (Parquet); Stage 2 trained the
autoencoder (loss 4.75 → 3.28) and wrote `211742×64` embeddings; Stage 3 (KMeans path)
clustered them and the co-occurrence confirmation produced varying window counts
(12–1089 per cluster). The HDBSCAN path is the same code — left for the GPU instance where
cuML makes it fast.

## Tuning knobs

| symptom | knob | direction |
|---|---|---|
| too many tiny/!confirmed clusters | `--min-cooccur-windows` | up |
| embeddings don't separate routes | `--epochs` / `--hidden` | up |
| not robust to missed detections | `--drop-prob` | up (0.4–0.5) |
| Stage 2 OOM on GPU | `--batch-size` | down; or more GPUs via `GPUS_PER_NODE` |
| clusters too coarse (kmeans) | `--n-clusters` | up; or switch `--algo hdbscan` |
| Stage 1 driver heavy | `--max-plates` | down (mirrors MaxGrowth prefilter) |
