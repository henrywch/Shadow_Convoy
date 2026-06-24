"""Stage 4b — UMAP/t-SNE 2-D projection of the trajectory embeddings.

Projects the `(N, 256)` embedding matrix down to 2-D so the fleet space can be
plotted (doc/Downstream_Tasks.md §0.5). Each point is one plate, colored by the
embedding fleet it belongs to (`embed_31/fleets.csv`, cluster_id → members), so
confirmed companion groups show up as tight islands. Feeds the dashboard scatter
(§3.3) and is a quick sanity check that the embedding actually separates routes.

Backends, auto-detected (same pattern as cluster_confirm.py / ann_index.py):

  * GPU  — RAPIDS **cuML** `UMAP` (`.venv-gpu`). Handles millions of points.
  * CPU  — `umap-learn` if installed, else scikit-learn t-SNE, else PCA. t-SNE
           is slow, so on CPU keep `--max-points` modest (≤ 20 k).

Plotting 3.8 M points is meaningless, so we subsample to `--max-points`
(default 200 k) with a fixed seed before projecting.

Output (into <embed_dir>/projection/ by default):
    projection.csv   plate, x, y, fleet_id        (fleet_id = -1 if in no fleet)
    projection.png   scatter, top fleets colored, the rest grey

Usage:
    python project_2d.py <embed_dir> --fleets <embed_dir>/fleets.csv \\
        --max-points 200000 --top-fleets 20
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def l2_normalize(x: np.ndarray) -> np.ndarray:
    x = x.astype("float32")
    return x / (np.linalg.norm(x, axis=1, keepdims=True) + 1e-9)


def load_fleet_map(fleets_csv: Path, confirmed_only: bool) -> dict[int, int]:
    """plate -> cluster_id, from the embedding fleets.csv. Larger fleets win on
    overlap so a plate gets its most prominent fleet's color."""
    if not fleets_csv or not fleets_csv.exists():
        return {}
    import pandas as pd
    df = pd.read_csv(fleets_csv)
    if confirmed_only and "confirmed" in df.columns:
        df = df[df["confirmed"].astype(str).str.lower() == "true"]
    df = df.sort_values("n_plates")  # ascending → bigger fleets overwrite
    plate_to_fleet: dict[int, int] = {}
    for cid, members in zip(df["cluster_id"], df["members"]):
        for pl in str(members).split(","):
            if pl.strip():
                plate_to_fleet[int(pl)] = int(cid)
    return plate_to_fleet


def project(x: np.ndarray, force_cpu: bool) -> tuple[np.ndarray, str]:
    """Return (N,2) coords and the backend name."""
    if not force_cpu:
        try:
            from cuml.manifold import UMAP  # GPU
            print(f"[embed-proj] backend=cuml.UMAP n={len(x):,}")
            return np.asarray(UMAP(n_neighbors=15, min_dist=0.1,
                                   random_state=42).fit_transform(x)), "cuml-umap"
        except Exception as e:
            print(f"[embed-proj] cuML unavailable ({type(e).__name__}); trying CPU")
    try:
        import umap  # umap-learn
        print(f"[embed-proj] backend=umap-learn n={len(x):,}")
        return np.asarray(umap.UMAP(n_neighbors=15, min_dist=0.1,
                                    random_state=42).fit_transform(x)), "umap-learn"
    except Exception:
        pass
    if len(x) <= 30000:
        from sklearn.manifold import TSNE
        print(f"[embed-proj] backend=sklearn.TSNE n={len(x):,}")
        return TSNE(n_components=2, init="pca", random_state=42).fit_transform(x), "tsne"
    from sklearn.decomposition import PCA
    print(f"[embed-proj] backend=PCA (fallback; install umap-learn for CPU UMAP) n={len(x):,}")
    return PCA(n_components=2, random_state=42).fit_transform(x), "pca"


def plot(coords, fleet_ids, top_fleets: int, path: Path, backend: str) -> None:
    from collections import Counter
    sizes = Counter(f for f in fleet_ids if f >= 0)
    top = {cid for cid, _ in sizes.most_common(top_fleets)}
    fig, ax = plt.subplots(figsize=(9, 8))
    # background: plates in no fleet, or in a small fleet
    bg = np.array([f not in top for f in fleet_ids])
    ax.scatter(coords[bg, 0], coords[bg, 1], s=2, c="lightgrey",
               alpha=0.4, linewidths=0, label="other / unfleeted")
    cmap = plt.get_cmap("tab20")
    for i, cid in enumerate(sorted(top, key=lambda c: -sizes[c])):
        m = np.array([f == cid for f in fleet_ids])
        ax.scatter(coords[m, 0], coords[m, 1], s=6, color=cmap(i % 20),
                   alpha=0.8, linewidths=0, label=f"fleet {cid} (n={sizes[cid]})")
    ax.set(title=f"Trajectory-embedding fleet space ({backend}, top {top_fleets} fleets)",
           xlabel="dim-1", ylabel="dim-2")
    ax.legend(markerscale=2, fontsize=7, loc="best", framealpha=0.8)
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("embed_dir", help="dir with embeddings.npy + plates.npy")
    p.add_argument("--fleets", default="",
                   help="fleets.csv to color points by (cluster_id, members)")
    p.add_argument("--out-dir", default="",
                   help="where to write projection.{csv,png} (default <embed_dir>/projection)")
    p.add_argument("--max-points", type=int, default=200_000,
                   help="subsample to this many points before projecting (0 = all)")
    p.add_argument("--top-fleets", type=int, default=20,
                   help="how many largest fleets to color distinctly")
    p.add_argument("--confirmed-only", action="store_true",
                   help="color only by fleets with confirmed=True")
    p.add_argument("--seed", type=int, default=42, help="subsample seed")
    p.add_argument("--cpu", action="store_true", help="force a CPU backend")
    a = p.parse_args()

    embed_dir = Path(a.embed_dir)
    embs = np.load(embed_dir / "embeddings.npy")
    plates = np.load(embed_dir / "plates.npy")
    print(f"[embed-proj] {embs.shape[0]:,} embeddings ({embs.shape[1]}-dim)")

    # Drop non-finite rows (vectors_0610/ is all-NaN; canonical matrix is vectors/).
    finite = np.isfinite(embs).all(axis=1)
    if not finite.all():
        n_bad = int((~finite).sum())
        print(f"[embed-proj] dropping {n_bad:,} non-finite rows "
              f"({100 * n_bad / len(embs):.2f}%)")
        if finite.sum() == 0:
            raise SystemExit("[embed-proj] all rows non-finite — wrong vectors dir? "
                             "use embed_31/vectors/, not vectors_0610/")
        embs, plates = embs[finite], plates[finite]

    if a.max_points and a.max_points < len(embs):
        rng = np.random.default_rng(a.seed)
        sel = rng.choice(len(embs), size=a.max_points, replace=False)
        sel.sort()
        embs, plates = embs[sel], plates[sel]
        print(f"[embed-proj] subsampled to {len(embs):,} points (seed={a.seed})")

    coords, backend = project(l2_normalize(embs), a.cpu)

    fleet_map = load_fleet_map(Path(a.fleets) if a.fleets else Path(),
                               a.confirmed_only)
    fleet_ids = [fleet_map.get(int(pl), -1) for pl in plates]
    n_fleeted = sum(1 for f in fleet_ids if f >= 0)
    print(f"[embed-proj] {n_fleeted:,}/{len(plates):,} points carry a fleet color")

    out = Path(a.out_dir) if a.out_dir else embed_dir / "projection"
    out.mkdir(parents=True, exist_ok=True)
    with (out / "projection.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["plate", "x", "y", "fleet_id"])
        for pl, (x, y), f in zip(plates, coords, fleet_ids):
            w.writerow([int(pl), round(float(x), 5), round(float(y), 5), f])
    plot(coords, fleet_ids, a.top_fleets, out / "projection.png", backend)
    print(f"[embed-proj] wrote {out}/projection.csv and projection.png")


if __name__ == "__main__":
    main()
