"""Tier 3 — camera transition graph + node2vec (geospatial without GPS).

The cameras are bare integer IDs 0…976 with **no lat/long table**. But their
spatial structure can be recovered from the data (doc/Downstream_Tasks.md §3.1):
build a directed graph where edge i→j is weighted by how many plates hop camera
i then camera j (consecutive sightings within --max-dt), then embed it. This one
artifact powers three things:
  (a) travel-time priors for clone detection (§2.4 / #8) — per-edge median Δt;
  (b) a node2vec camera embedding → an approximate map *without* GPS;
  (c) corridor layout for the dashboard.

node2vec without gensim: we use the well-known equivalence that DeepWalk/node2vec
is implicit matrix factorization — generate weighted random walks, accumulate a
windowed co-occurrence matrix, take its PPMI, and truncated-SVD it. For 977 nodes
this is instant and dependency-free (polars + sklearn only).

Input:   raw sightings CSV (plate, camera, timestamp).
Output (data/downstream/camera_graph/ by default):
  edges.csv          src, dst, weight, median_dt, mean_dt   (directed; #8 priors)
  camera_vectors.csv camera, e0..e{d-1}                      (node2vec embedding)
  camera_map.csv     camera, x, y                            (2-D PCA of embedding)
  camera_map.png     the recovered camera map, top edges drawn
  summary.txt        counts + busiest directed transitions

Usage:
    python camera_graph.py data/input/31.csv          # full run
    python camera_graph.py data/input/day1.csv --dim 64 --max-dt 1800
"""
import argparse
import csv
from pathlib import Path

import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = "/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop"


def build_edges(raw_csv: Path, max_dt: int):
    """Directed camera→camera transitions from consecutive per-plate sightings."""
    import polars as pl
    df = (pl.read_csv(raw_csv, has_header=False, new_columns=["plate", "camera", "t"])
            .sort(["plate", "t"]))
    df = df.with_columns([
        pl.col("camera").shift(1).over("plate").alias("prev_cam"),
        pl.col("t").shift(1).over("plate").alias("prev_t"),
    ]).drop_nulls(["prev_cam"])
    df = df.with_columns((pl.col("t") - pl.col("prev_t")).alias("dt"))
    df = df.filter((pl.col("dt") > 0) & (pl.col("dt") <= max_dt)
                   & (pl.col("camera") != pl.col("prev_cam")))
    edges = (df.group_by(["prev_cam", "camera"])
               .agg([pl.len().alias("weight"),
                     pl.col("dt").median().alias("median_dt"),
                     pl.col("dt").mean().alias("mean_dt")])
               .sort("weight", descending=True))
    return edges.rename({"prev_cam": "src", "camera": "dst"})


def random_walks(adj, weights, ids, n_walks, walk_len, rng):
    """Weighted random walks (DeepWalk; p=q=1). Returns list of index-walks."""
    walks = []
    n = len(ids)
    for _ in range(n_walks):
        order = rng.permutation(n)
        for start in order:
            if not adj[start]:
                continue
            walk = [start]
            cur = start
            for _ in range(walk_len - 1):
                nbrs, w = adj[cur], weights[cur]
                if not nbrs:
                    break
                cur = nbrs[rng.choice(len(nbrs), p=w)]
                walk.append(cur)
            walks.append(walk)
    return walks


def node2vec_embed(edges, ids, dim, n_walks, walk_len, window, seed):
    """DeepWalk-as-matrix-factorization: walks → windowed co-occ → PPMI → SVD."""
    from sklearn.decomposition import TruncatedSVD
    idx = {c: i for i, c in enumerate(ids)}
    n = len(ids)
    # symmetrize weights for the walk graph (a map is undirected)
    adj_w = {i: {} for i in range(n)}
    for s, d, w in edges:
        a, b = idx[s], idx[d]
        adj_w[a][b] = adj_w[a].get(b, 0) + w
        adj_w[b][a] = adj_w[b].get(a, 0) + w
    adj = [list(adj_w[i].keys()) for i in range(n)]
    weights = []
    for i in range(n):
        w = np.array([adj_w[i][j] for j in adj[i]], dtype="float64")
        weights.append(w / w.sum() if w.sum() else w)

    rng = np.random.default_rng(seed)
    walks = random_walks(adj, weights, ids, n_walks, walk_len, rng)

    # windowed co-occurrence
    C = np.zeros((n, n), dtype="float64")
    for walk in walks:
        L = len(walk)
        for i in range(L):
            lo, hi = max(0, i - window), min(L, i + window + 1)
            for j in range(lo, hi):
                if j != i:
                    C[walk[i], walk[j]] += 1.0
    # PPMI
    total = C.sum()
    if total == 0:
        return np.zeros((n, dim)), walks
    row = C.sum(1, keepdims=True); col = C.sum(0, keepdims=True)
    with np.errstate(divide="ignore", invalid="ignore"):
        pmi = np.log((C * total) / (row * col))
    ppmi = np.nan_to_num(np.maximum(pmi, 0.0), nan=0.0, posinf=0.0, neginf=0.0)
    d = min(dim, n - 1)
    emb = TruncatedSVD(n_components=d, random_state=seed).fit_transform(ppmi)
    if d < dim:
        emb = np.pad(emb, ((0, 0), (0, dim - d)))
    return emb, walks


def plot_map(coords, ids, edges, path):
    idx = {c: i for i, c in enumerate(ids)}
    fig, ax = plt.subplots(figsize=(9, 8))
    top = edges[:300]
    for s, d, w in top:
        if s in idx and d in idx:
            x = [coords[idx[s], 0], coords[idx[d], 0]]
            y = [coords[idx[s], 1], coords[idx[d], 1]]
            ax.plot(x, y, color="steelblue", alpha=0.15, linewidth=0.6, zorder=1)
    ax.scatter(coords[:, 0], coords[:, 1], s=10, c="darkorange", zorder=2)
    ax.set(title="Recovered camera map (node2vec → PCA-2D; top 300 transitions)",
           xlabel="dim-1", ylabel="dim-2")
    fig.tight_layout(); fig.savefig(path, dpi=130); plt.close(fig)


def main(raw_csv, out_dir, dim, n_walks, walk_len, window, max_dt, seed):
    from sklearn.decomposition import PCA
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)

    edf = build_edges(Path(raw_csv), max_dt)
    edge_rows = list(zip(edf["src"].to_list(), edf["dst"].to_list(),
                         edf["weight"].to_list()))
    ids = sorted(set(edf["src"].to_list()) | set(edf["dst"].to_list()))
    print(f"[cam-graph] {len(ids):,} cameras, {len(edge_rows):,} directed edges "
          f"(max_dt={max_dt}s)")

    edf.write_csv(out / "edges.csv")

    emb, walks = node2vec_embed(edge_rows, ids, dim, n_walks, walk_len, window, seed)
    print(f"[cam-graph] node2vec: {len(walks):,} walks → {emb.shape} embedding")
    with (out / "camera_vectors.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["camera"] + [f"e{i}" for i in range(emb.shape[1])])
        for c, v in zip(ids, emb):
            w.writerow([c] + [round(float(x), 6) for x in v])

    coords = PCA(n_components=2, random_state=seed).fit_transform(emb)
    with (out / "camera_map.csv").open("w", newline="") as f:
        w = csv.writer(f); w.writerow(["camera", "x", "y"])
        for c, (x, y) in zip(ids, coords):
            w.writerow([c, round(float(x), 5), round(float(y), 5)])
    plot_map(coords, ids, edge_rows, out / "camera_map.png")

    busiest = "\n".join(
        f"  {w:>7}  {s}->{d}  (median {md:.0f}s)" for s, d, w, md in
        zip(edf["src"][:15], edf["dst"][:15], edf["weight"][:15], edf["median_dt"][:15]))
    summary = [
        f"Camera transition graph from {Path(raw_csv).name}",
        f"  cameras (nodes):        {len(ids):,}",
        f"  directed transitions:   {len(edge_rows):,}",
        f"  total hops counted:     {int(edf['weight'].sum()):,}",
        f"  node2vec embedding:     {emb.shape[0]} × {emb.shape[1]}",
        f"  max hop gap (--max-dt): {max_dt}s",
        "",
        "== Busiest directed transitions (the corridors) ==",
        busiest,
    ]
    (out / "summary.txt").write_text("\n".join(summary) + "\n")
    print("\n".join(summary))
    print(f"\n[cam-graph] wrote edges.csv, camera_vectors.csv, camera_map.{{csv,png}}, "
          f"summary.txt to {out}/")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("raw_csv", help="raw sightings CSV (plate, camera, timestamp)")
    p.add_argument("--out-dir", default=f"{REPO}/data/downstream/camera_graph")
    p.add_argument("--dim", type=int, default=64, help="node2vec embedding dim")
    p.add_argument("--n-walks", type=int, default=10, help="random walks per node")
    p.add_argument("--walk-len", type=int, default=40, help="random walk length")
    p.add_argument("--window", type=int, default=5, help="co-occurrence window")
    p.add_argument("--max-dt", type=int, default=1800,
                   help="max seconds between consecutive sightings to count as a hop")
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    main(a.raw_csv, a.out_dir, a.dim, a.n_walks, a.walk_len, a.window,
         a.max_dt, a.seed)
