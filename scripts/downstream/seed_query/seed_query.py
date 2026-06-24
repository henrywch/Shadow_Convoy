"""Tier 2 — seed-vehicle convoy query: "who travels with plate X?"

The canonical ANPR forensic use case (doc/Downstream_Tasks.md §2.1) and the
headline demo. The real workflow is not "dump all fleets" — it is *"here is one
known vehicle, find its companions."* We answer it three independent ways and
merge, so a companion endorsed by more routes ranks higher (the same consensus
logic as the registry):

  1. REGISTRY  (fast, default)   — X's fused group(s) in the consensus registry
                                   (§1.1); companions = co-members, with the
                                   group's agreement level + score as evidence.
  2. ANN       (--embed-dir)     — nearest-route neighbours of X in the 256-D
                                   embedding (§0.5); reuses embedding_assets/
                                   ann_index.py. cuVS on GPU, sklearn on CPU.
  3. COOCCUR   (--raw-csv)       — on-demand co-occurrence: stream the raw
                                   sightings, collect X's (camera, ε-bucket)
                                   keys, then count plates that share them
                                   (the cluster_confirm machinery, scoped to X).

Each companion is scored by how many routes surface it (n_sources, the primary
sort) then by registry agreement / co-occurrence count / cosine similarity.

Output (data/downstream/seed_query/ by default):
  companions_<plate>.csv   companion, n_sources, registry_agree, registry_groups,
                           cooccur_buckets, ann_cosine, evidence

Usage:
    python seed_query.py 393966
    python seed_query.py 393966 --embed-dir data/output/embed_31/vectors --k 20
    python seed_query.py 393966 --raw-csv data/input/day1.csv --eps 300
"""
import argparse
import csv
import importlib.util
from collections import defaultdict
from pathlib import Path

REPO = "/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop"


# ── route 1: registry lookup ─────────────────────────────────────────────────

def from_registry(plate: int, registry_csv: Path):
    """Return {companion: [(group_id, agreement, score, detectors)]}."""
    out = defaultdict(list)
    groups = []
    if not registry_csv.exists():
        print(f"[seed] registry not found at {registry_csv}; skipping route 1")
        return out, groups
    with registry_csv.open() as f:
        for r in csv.DictReader(f):
            members = [int(p) for p in r["members"].split(",")]
            if plate in members:
                g = (r["group_id"], int(r["detectors_agreeing"]),
                     float(r["consensus_score"]), r["detectors"])
                groups.append((g, members))
                for m in members:
                    if m != plate:
                        out[m].append(g)
    print(f"[seed] registry: plate in {len(groups)} group(s) → "
          f"{len(out)} distinct companions")
    return out, groups


# ── route 2: ANN (reuse embedding_assets/ann_index.py) ───────────────────────

def from_ann(plate: int, embed_dir: Path, k: int, force_cpu: bool, max_rows: int):
    import numpy as np
    spec = importlib.util.spec_from_file_location(
        "ann_index", Path(REPO) / "scripts/downstream/embedding_assets/ann_index.py")
    ann = importlib.util.module_from_spec(spec); spec.loader.exec_module(ann)

    embs = np.load(embed_dir / "embeddings.npy")
    plates = np.load(embed_dir / "plates.npy")
    if max_rows and max_rows < len(embs):
        embs, plates = embs[:max_rows], plates[:max_rows]
    finite = np.isfinite(embs).all(axis=1)
    embs, plates = embs[finite], plates[finite]
    row_of = {int(p): i for i, p in enumerate(plates)}
    if plate not in row_of:
        print(f"[seed] ann: plate {plate} not in this embedding set; skipping route 2")
        return {}
    data = ann.l2_normalize(embs)
    q = data[row_of[plate]][None, :]
    backend = "sklearn"
    if not force_cpu:
        try:
            import cuvs.neighbors.cagra  # noqa
            backend = "cuvs"
        except Exception:
            pass
    if backend == "cuvs":
        idx, cp = ann.cuvs_build(data)
        nbr, dist = ann.cuvs_search(idx, q, k + 1, cp)
        sims = 1.0 - dist[0] / 2.0
    else:
        nn = ann.sklearn_build(data)
        nbr, dist = ann.sklearn_search(nn, q, k + 1)
        sims = 1.0 - dist[0]
    out = {}
    for j, s in zip(nbr[0], sims):
        nb = int(plates[int(j)])
        if nb != plate and len(out) < k:
            out[nb] = float(s)
    print(f"[seed] ann ({backend}): {len(out)} nearest-route neighbours")
    return out


# ── route 3: on-demand co-occurrence over the raw sightings ──────────────────

def from_cooccur(plate: int, raw_csv: Path, eps: int, chunksize: int = 2_000_000):
    import pandas as pd
    def keys_of(cam, t):
        b0 = t // eps
        b1 = (t - eps // 2) // eps
        return (cam, int(b0)), (cam, -int(b1) - 1)   # disjoint dual-offset grid

    # pass 1: X's (camera, bucket) keys
    seed_keys = set()
    for chunk in pd.read_csv(raw_csv, header=None, names=["plate", "camera", "t"],
                             chunksize=chunksize):
        sub = chunk[chunk["plate"] == plate]
        for cam, t in zip(sub["camera"], sub["t"]):
            seed_keys.update(keys_of(int(cam), int(t)))
    if not seed_keys:
        print(f"[seed] cooccur: plate {plate} not seen in {raw_csv.name}; skipping route 3")
        return {}
    # pass 2: plates sharing those keys (count distinct shared buckets)
    shared = defaultdict(set)
    for chunk in pd.read_csv(raw_csv, header=None, names=["plate", "camera", "t"],
                             chunksize=chunksize):
        for pl, cam, t in zip(chunk["plate"], chunk["camera"], chunk["t"]):
            if pl == plate:
                continue
            for key in keys_of(int(cam), int(t)):
                if key in seed_keys:
                    shared[int(pl)].add(key)
    out = {pl: len(ks) for pl, ks in shared.items()}
    print(f"[seed] cooccur: {len(seed_keys)} seed buckets → {len(out)} co-occurring plates")
    return out


# ── merge & rank ─────────────────────────────────────────────────────────────

def main(plate, registry_csv, embed_dir, raw_csv, k, eps, out_dir, cpu, max_rows):
    reg, reg_groups = from_registry(plate, Path(registry_csv))
    ann = from_ann(plate, Path(embed_dir), k, cpu, max_rows) if embed_dir else {}
    coo = from_cooccur(plate, Path(raw_csv), eps) if raw_csv else {}

    companions = set(reg) | set(ann) | set(coo)
    rows = []
    for c in companions:
        groups = reg.get(c, [])
        reg_agree = max((g[1] for g in groups), default=0)
        n_sources = (1 if groups else 0) + (1 if c in ann else 0) + (1 if c in coo else 0)
        ev = []
        if groups:
            ev.append("registry[" + ",".join(f"g{g[0]}:{g[1]}of3" for g in groups[:3]) + "]")
        if c in coo: ev.append(f"cooccur×{coo[c]}")
        if c in ann: ev.append(f"cos={ann[c]:.3f}")
        rows.append({
            "companion": c, "n_sources": n_sources, "registry_agree": reg_agree,
            "registry_groups": "|".join(g[0] for g in groups),
            "cooccur_buckets": coo.get(c, ""),
            "ann_cosine": round(ann[c], 4) if c in ann else "",
            "evidence": "; ".join(ev),
        })
    rows.sort(key=lambda r: (r["n_sources"], r["registry_agree"],
                             r["cooccur_buckets"] or 0, r["ann_cosine"] or 0),
              reverse=True)

    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    dst = out / f"companions_{plate}.csv"
    with dst.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["companion", "n_sources", "registry_agree",
                                          "registry_groups", "cooccur_buckets",
                                          "ann_cosine", "evidence"])
        w.writeheader(); w.writerows(rows)

    print(f"\n[seed] === companions of plate {plate} (top {min(15, len(rows))} of {len(rows)}) ===")
    for r in rows[:15]:
        print(f"  {r['companion']:<10} sources={r['n_sources']}  {r['evidence']}")
    print(f"\n[seed] wrote {len(rows)} companions → {dst}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("plate", type=int, help="the seed plate to find companions of")
    p.add_argument("--registry", default=f"{REPO}/data/downstream/consensus_registry/registry.csv")
    p.add_argument("--embed-dir", default="", help="vectors dir to enable the ANN route")
    p.add_argument("--raw-csv", default="", help="raw sightings CSV to enable the co-occurrence route")
    p.add_argument("--k", type=int, default=20, help="ANN neighbours to return")
    p.add_argument("--eps", type=int, default=300, help="co-occurrence ε seconds")
    p.add_argument("--out-dir", default=f"{REPO}/data/downstream/seed_query")
    p.add_argument("--cpu", action="store_true", help="force sklearn CPU for ANN")
    p.add_argument("--max-rows", type=int, default=0, help="cap ANN rows (CPU smoke tests)")
    a = p.parse_args()
    main(a.plate, a.registry, a.embed_dir, a.raw_csv, a.k, a.eps, a.out_dir,
         a.cpu, a.max_rows)
