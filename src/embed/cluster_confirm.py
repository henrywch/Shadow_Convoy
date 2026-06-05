"""Stage 3 — cluster the embeddings, then confirm companionship (GPU or CPU).

Two steps:

  A. CLUSTER the per-plate embedding vectors → candidate fleets.
     Uses RAPIDS cuML (GPU) if importable, else scikit-learn (CPU).

  B. CONFIRM each candidate is an actual companion group, not just
     similar-route vehicles. This is the step that closes the gap flagged in
     doc/ML_Approaches.md §0/§4.1: an embedding cluster groups plates with
     *similar routes*, which includes two commuters who drive the same road a
     week apart and never travel together. We re-read the raw sightings and
     keep only clusters whose members actually co-occur — same camera within ε
     seconds — enough times. Co-occurrence is what makes them companions.

Output:
    fleets.csv   cluster_id, n_plates, n_cooccur_windows, n_cooccur_pairs,
                 confirmed, members

    python cluster_confirm.py <embed_dir> <raw_csv> <out_dir> [options]
"""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from itertools import combinations
from pathlib import Path

import numpy as np


def cluster(embs, algo, min_cluster_size, n_clusters, eps, normalize):
    """Return an int label per row (-1 = noise). cuML if present, else sklearn."""
    X = embs.astype("float32")
    if normalize:
        X = X / (np.linalg.norm(X, axis=1, keepdims=True) + 1e-9)
    try:
        import cuml.cluster as C          # GPU
        backend = "cuml"
    except Exception:
        import sklearn.cluster as C       # CPU fallback
        backend = "sklearn"
    print(f"[embed-cluster] backend={backend} algo={algo} n={len(X):,}")
    if algo == "hdbscan":
        model = C.HDBSCAN(min_cluster_size=min_cluster_size)
    elif algo == "kmeans":
        model = C.KMeans(n_clusters=n_clusters, random_state=42)
    elif algo == "dbscan":
        model = C.DBSCAN(eps=eps, min_samples=min_cluster_size)
    else:
        raise ValueError(f"unknown algo {algo}")
    labels = model.fit_predict(X)
    return np.asarray(labels)


def load_sightings(raw_csv, keep_plates, eps, chunksize=2_000_000):
    """Stream the raw CSV, keep only clustered plates, bucket by (camera, ε).

    Returns dict: plate -> list of (camera, bucket) at offsets {0, ε/2}.
    Only clustered plates are retained, so memory is bounded by the candidate
    population, not the full 22 M.
    """
    import pandas as pd
    keep = set(int(p) for p in keep_plates)
    out: dict[int, list[tuple[int, int]]] = defaultdict(list)
    reader = pd.read_csv(raw_csv, header=None, names=["plate", "camera", "t"],
                         chunksize=chunksize)
    for chunk in reader:
        chunk = chunk[chunk["plate"].isin(keep)]
        if chunk.empty:
            continue
        for plate, cam, t in zip(chunk["plate"], chunk["camera"], chunk["t"]):
            b0 = t // eps
            b1 = (t - eps // 2) // eps
            out[int(plate)].append((int(cam), int(b0)))
            out[int(plate)].append((int(cam), -int(b1) - 1))  # disjoint key space
    return out


def confirm(labels, plates, sightings, min_windows):
    """Per cluster, count co-occurrence windows and pairs; flag confirmed."""
    # cluster_id -> [plates]
    members = defaultdict(list)
    for lab, pl in zip(labels, plates):
        if lab >= 0:
            members[int(lab)].append(int(pl))

    results = []
    for cid, mem in members.items():
        memset = set(mem)
        # (camera,bucket) -> members of THIS cluster present there
        window_members = defaultdict(set)
        for pl in mem:
            for key in sightings.get(pl, ()):
                window_members[key].add(pl)
        cooccur_windows = 0
        pairs = set()
        for present in window_members.values():
            if len(present) >= 2:
                cooccur_windows += 1
                for a, b in combinations(sorted(present), 2):
                    pairs.add((a, b))
        confirmed = cooccur_windows >= min_windows
        results.append({
            "cluster_id": cid,
            "n_plates": len(mem),
            "n_cooccur_windows": cooccur_windows,
            "n_cooccur_pairs": len(pairs),
            "confirmed": confirmed,
            "members": sorted(memset),
        })
    results.sort(key=lambda r: (-int(r["confirmed"]), -r["n_cooccur_windows"]))
    return results


def main():
    p = argparse.ArgumentParser()
    p.add_argument("embed_dir", help="dir with embeddings.npy + plates.npy")
    p.add_argument("raw_csv", help="original sightings CSV for confirmation")
    p.add_argument("out_dir")
    p.add_argument("--algo", choices=["hdbscan", "kmeans", "dbscan"], default="hdbscan")
    p.add_argument("--min-cluster-size", type=int, default=2)
    p.add_argument("--n-clusters", type=int, default=1000, help="kmeans only")
    p.add_argument("--dbscan-eps", type=float, default=0.5, help="dbscan only")
    p.add_argument("--no-normalize", action="store_true",
                   help="skip L2 normalization (default normalizes → cosine-like)")
    p.add_argument("--eps", type=int, default=300,
                   help="ε seconds for the co-occurrence window in confirmation")
    p.add_argument("--min-cooccur-windows", type=int, default=2,
                   help="a cluster is a confirmed companion group only if its "
                        "members share ≥ N (camera, ε-window) events")
    a = p.parse_args()

    embs = np.load(Path(a.embed_dir) / "embeddings.npy")
    plates = np.load(Path(a.embed_dir) / "plates.npy")
    print(f"[embed-cluster] {embs.shape[0]:,} embeddings ({embs.shape[1]}-dim)")

    labels = cluster(embs, a.algo, a.min_cluster_size, a.n_clusters,
                     a.dbscan_eps, normalize=not a.no_normalize)
    n_clusters = len({int(l) for l in labels if l >= 0})
    n_noise = int((labels < 0).sum())
    print(f"[embed-cluster] {n_clusters:,} candidate clusters, {n_noise:,} noise points")

    clustered_plates = [int(pl) for pl, lab in zip(plates, labels) if lab >= 0]
    sightings = load_sightings(a.raw_csv, clustered_plates, a.eps)
    results = confirm(labels, plates, sightings, a.min_cooccur_windows)
    n_conf = sum(1 for r in results if r["confirmed"])
    print(f"[embed-cluster] {n_conf:,}/{len(results):,} clusters confirmed as "
          f"companion groups (≥{a.min_cooccur_windows} co-occurrence windows)")

    out = Path(a.out_dir); out.mkdir(parents=True, exist_ok=True)
    with (out / "fleets.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["cluster_id", "n_plates", "n_cooccur_windows",
                    "n_cooccur_pairs", "confirmed", "members"])
        for r in results:
            w.writerow([r["cluster_id"], r["n_plates"], r["n_cooccur_windows"],
                        r["n_cooccur_pairs"], r["confirmed"],
                        ",".join(str(p) for p in r["members"])])
    print(f"[embed-cluster] wrote {out}/fleets.csv")


if __name__ == "__main__":
    main()
