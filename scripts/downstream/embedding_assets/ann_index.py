"""Stage 4a — ANN "find vehicles like this one" over the trajectory embeddings.

The `(3.83 M, 256)` embedding matrix (`vectors_*/embeddings.npy`, aligned with
`plates.npy`) is the most reusable asset in the repo, but nothing queries it yet.
This builds an approximate-nearest-neighbour index over it so that, given one
plate, you get its k nearest-route neighbours in O(1) — the trajectory
similarity-search primitive behind the seed-vehicle query (doc/Downstream_Tasks.md
§0.5 / §2.1).

Backends, auto-detected (same philosophy as cluster_confirm.py):

  * GPU  — NVIDIA **cuVS** CAGRA (already in `.venv-gpu`; RAPIDS pulls cuvs-cu12,
           which is why the repo needs no faiss — see requirements-gpu.txt). Builds
           a graph index over millions of vectors in seconds and can be serialized.
  * CPU  — scikit-learn brute-force NearestNeighbors (cosine). Exact, not "A"NN;
           fine for smoke tests / a handful of queries. Use `--max-rows` to cap.

Vectors are L2-normalized first, so an L2 / inner-product ranking equals a cosine
ranking (the natural metric for route-shape embeddings).

Usage:
    # build + serialize the index, then answer a query in one run
    python ann_index.py <embed_dir> --save-index <embed_dir>/ann \\
        --query 477634 --k 20 --neighbors-out <embed_dir>/ann/neighbors.csv

    # query several seed plates at once
    python ann_index.py <embed_dir> --query 477634,491688,3271142 --k 20

    # load a previously serialized GPU index instead of rebuilding (cuVS only)
    python ann_index.py <embed_dir> --load-index <embed_dir>/ann --query 477634
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

import numpy as np


def l2_normalize(x: np.ndarray) -> np.ndarray:
    x = x.astype("float32")
    return x / (np.linalg.norm(x, axis=1, keepdims=True) + 1e-9)


# ── GPU backend (cuVS CAGRA) ─────────────────────────────────────────────────

def cuvs_build(dataset: np.ndarray):
    """Build a CAGRA graph index on GPU. Returns (index, cupy_module)."""
    import cupy as cp
    from cuvs.neighbors import cagra
    d = cp.asarray(dataset)
    index = cagra.build(cagra.IndexParams(), d)
    return index, cp


def cuvs_search(index, queries: np.ndarray, k: int, cp):
    from cuvs.neighbors import cagra
    q = cp.asarray(queries)
    distances, neighbors = cagra.search(cagra.SearchParams(), index, q, k)
    return cp.asnumpy(neighbors), cp.asnumpy(distances)


# ── CPU backend (sklearn brute cosine) ───────────────────────────────────────

def sklearn_build(dataset: np.ndarray):
    from sklearn.neighbors import NearestNeighbors
    nn = NearestNeighbors(metric="cosine", algorithm="brute")
    nn.fit(dataset)
    return nn


def sklearn_search(nn, queries: np.ndarray, k: int):
    distances, neighbors = nn.kneighbors(queries, n_neighbors=k)
    return neighbors, distances  # distances are cosine distance (1 - cos sim)


# ── orchestration ────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("embed_dir", help="dir with embeddings.npy + plates.npy")
    p.add_argument("--query", default="",
                   help="seed plate, or comma-separated plates, to find neighbours of")
    p.add_argument("--query-file", default="",
                   help="file with one seed plate per line (alternative to --query)")
    p.add_argument("--k", type=int, default=20, help="neighbours per query")
    p.add_argument("--save-index", default="",
                   help="serialize the built index to this dir (cuVS/GPU only)")
    p.add_argument("--load-index", default="",
                   help="load a serialized cuVS index from this dir instead of building")
    p.add_argument("--neighbors-out", default="",
                   help="write the query results to this CSV")
    p.add_argument("--max-rows", type=int, default=0,
                   help="cap #vectors indexed (0 = all); use on CPU for smoke tests")
    p.add_argument("--cpu", action="store_true", help="force the sklearn CPU backend")
    a = p.parse_args()

    embed_dir = Path(a.embed_dir)
    embs = np.load(embed_dir / "embeddings.npy")
    plates = np.load(embed_dir / "plates.npy")
    if a.max_rows and a.max_rows < len(embs):
        embs, plates = embs[:a.max_rows], plates[:a.max_rows]
    print(f"[embed-ann] {embs.shape[0]:,} embeddings ({embs.shape[1]}-dim)")

    # Drop non-finite rows. Some Stage-2 runs wrote all-NaN matrices
    # (e.g. vectors_0610/ is 100% NaN — use vectors/); guard against it here.
    finite = np.isfinite(embs).all(axis=1)
    if not finite.all():
        n_bad = int((~finite).sum())
        print(f"[embed-ann] dropping {n_bad:,} non-finite rows "
              f"({100 * n_bad / len(embs):.2f}%)")
        if finite.sum() == 0:
            raise SystemExit("[embed-ann] all rows non-finite — wrong vectors dir? "
                             "use embed_31/vectors/, not vectors_0610/")
        embs, plates = embs[finite], plates[finite]

    dataset = l2_normalize(embs)
    plate_to_row = {int(pl): i for i, pl in enumerate(plates)}

    # pick backend
    backend = "sklearn"
    index = cp = None
    if not a.cpu:
        try:
            from cuvs.neighbors import cagra  # noqa: F401  (import probe)
            backend = "cuvs"
        except Exception as e:
            print(f"[embed-ann] cuVS unavailable ({type(e).__name__}); using sklearn CPU")

    if backend == "cuvs":
        import cupy as cp
        from cuvs.neighbors import cagra
        if a.load_index:
            index = cagra.load(str(Path(a.load_index) / "cagra.idx"))
            print(f"[embed-ann] backend=cuvs (loaded {a.load_index})")
        else:
            index, cp = cuvs_build(dataset)
            print(f"[embed-ann] backend=cuvs (built CAGRA over {len(dataset):,} vectors)")
        if a.save_index:
            out = Path(a.save_index); out.mkdir(parents=True, exist_ok=True)
            cagra.save(str(out / "cagra.idx"), index)
            np.save(out / "plates.npy", plates)
            print(f"[embed-ann] serialized index → {out}/cagra.idx")
    else:
        if a.load_index or a.save_index:
            print("[embed-ann] note: index (de)serialization is cuVS/GPU-only; "
                  "the CPU backend rebuilds in-memory each run")
        index = sklearn_build(dataset)
        print(f"[embed-ann] backend=sklearn brute-cosine over {len(dataset):,} vectors")

    # collect query plates
    seeds: list[int] = []
    if a.query:
        seeds += [int(s) for s in a.query.split(",") if s.strip()]
    if a.query_file:
        seeds += [int(line) for line in Path(a.query_file).read_text().split() if line.strip()]
    if not seeds:
        print("[embed-ann] index ready; no --query given, nothing to look up.")
        return

    missing = [s for s in seeds if s not in plate_to_row]
    seeds = [s for s in seeds if s in plate_to_row]
    if missing:
        print(f"[embed-ann] {len(missing)} seed plate(s) not in this embedding set: "
              f"{missing[:10]}{'…' if len(missing) > 10 else ''}")
    if not seeds:
        return

    q = np.stack([dataset[plate_to_row[s]] for s in seeds]).astype("float32")
    if backend == "cuvs":
        nbr_idx, dist = cuvs_search(index, q, a.k + 1, cp)
        to_sim = lambda d: 1.0 - d / 2.0  # sqeuclidean on unit vectors → cosine sim
    else:
        nbr_idx, dist = sklearn_search(index, q, a.k + 1)
        to_sim = lambda d: 1.0 - d        # sklearn returns cosine *distance*

    rows = []
    for s, idxs, dists in zip(seeds, nbr_idx, dist):
        printed = 0
        print(f"\n[embed-ann] neighbours of plate {s}:")
        for rank_i, (j, dd) in enumerate(zip(idxs, dists)):
            neigh = int(plates[int(j)])
            if neigh == s:                      # skip the seed itself
                continue
            sim = float(to_sim(float(dd)))
            print(f"    {printed + 1:>3}. plate {neigh:<10} cos≈{sim:.4f}")
            rows.append({"seed": s, "rank": printed + 1, "neighbor": neigh,
                         "cosine_sim": round(sim, 6)})
            printed += 1
            if printed >= a.k:
                break

    if a.neighbors_out:
        out = Path(a.neighbors_out); out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["seed", "rank", "neighbor", "cosine_sim"])
            w.writeheader()
            w.writerows(rows)
        print(f"\n[embed-ann] wrote {len(rows)} rows → {out}")


if __name__ == "__main__":
    main()
