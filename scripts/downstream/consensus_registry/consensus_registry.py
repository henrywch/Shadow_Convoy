"""Tier 1 keystone — unified companion-group registry with consensus scoring.

Fuses the three detectors' fleet labellings into ONE ranked, deduplicated,
confidence-scored table (doc/Downstream_Tasks.md §1.1). A group that two or three
detectors endorse — across three *different* metric spaces (cell co-occurrence,
directed-route co-traversal, embedding similarity) — is far stronger evidence
than any single detector's top-K. Every Tier 2–3 application queries this table,
not the three raw CSVs.

Inputs (three independent fleet labellings):
  * convoy fleets   data/output/communities/fleets.csv      (FP-Growth → Louvain)
  * pattern fleets  data/downstream/pattern_graph/fleets.csv (MaxGrowth → Louvain)
  * embed fleets    data/output/embed_31/fleets.csv          (embedding clusters)

Method (composite-label consensus — no pairwise edge blow-up):
  Each plate gets a label in each detector = the fleet id it belongs to (or none).
  Plates that share the SAME label tuple under ≥2 detectors are, by construction,
  grouped together by those detectors → a consensus block. We group by:
    - the full triple  (conv, patt, emb)            → 3-of-3 agreement
    - each detector pair                            → 2-of-3 agreement
  keeping every block of ≥2 plates, deduplicating identical member-sets (highest
  agreement wins).

Scoring:
  consensus_score = detectors_agreeing + mean(normalized native strengths of the
  contributing fleets). Agreement dominates (a 3-of-3 always outranks a 2-of-3);
  native strength (convoy edge weight, pattern route weight, embedding
  co-occurrence windows) breaks ties. Strengths are log-min-max normalized to
  [0,1] across blocks so the three scales are comparable.

Output (data/downstream/consensus_registry/ by default):
  registry.csv   group_id, detectors_agreeing, detectors, n_members,
                 conv_fleet, patt_fleet, emb_fleet, conv_weight, patt_weight,
                 emb_windows, emb_confirmed, consensus_score, members
  summary.txt    headline counts + the top consensus groups

Usage:
    python consensus_registry.py [--convoy CSV] [--patterns CSV] [--embed CSV]
                                 [--out-dir DIR]
"""
import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path

import pandas as pd

REPO = "/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop"


def load_labels(csv_path: Path, id_col: str, strength_cols: list[str]):
    """Return (plate -> fleet_id, fleet_id -> {strength metrics})."""
    df = pd.read_csv(csv_path)
    plate_label, fleet_strength = {}, {}
    for _, r in df.iterrows():
        fid = int(r[id_col])
        fleet_strength[fid] = {c: float(r[c]) for c in strength_cols if c in df.columns}
        if "confirmed" in df.columns:
            fleet_strength[fid]["confirmed"] = str(r["confirmed"]).lower() == "true"
        for p in str(r["members"]).split(","):
            if p.strip():
                plate_label[int(p)] = fid
    return plate_label, fleet_strength


def log_minmax(values: dict):
    """Map a dict id->raw to id->[0,1] via log1p then min-max. Empty → {}."""
    if not values:
        return {}
    logs = {k: math.log1p(max(v, 0.0)) for k, v in values.items()}
    lo, hi = min(logs.values()), max(logs.values())
    span = hi - lo or 1.0
    return {k: (v - lo) / span for k, v in logs.items()}


def main(convoy, patterns, embed, out_dir):
    conv_lab, conv_str = load_labels(Path(convoy), "community_id", ["total_weight"])
    patt_lab, patt_str = load_labels(Path(patterns), "community_id", ["total_weight", "n_routes"])
    emb_lab,  emb_str  = load_labels(Path(embed), "cluster_id", ["n_cooccur_windows"])
    print(f"[registry] convoy {len(conv_str)} fleets / {len(conv_lab):,} plates · "
          f"pattern {len(patt_str)} / {len(patt_lab):,} · embed {len(emb_str):,} / {len(emb_lab):,}")

    plates = set(conv_lab) | set(patt_lab) | set(emb_lab)

    # label tuple per plate (None where the detector doesn't place it)
    def lab(p):
        return (conv_lab.get(p), patt_lab.get(p), emb_lab.get(p))

    # build consensus blocks: group plates by a composite key over an agreeing subset
    # of detectors. Each entry: detectors-frozenset -> composite-key -> [plates]
    DET = ["conv", "patt", "emb"]
    blocks: dict = defaultdict(lambda: defaultdict(list))
    for p in plates:
        l = lab(p)
        present = [i for i in range(3) if l[i] is not None]
        if len(present) < 2:
            continue
        # 3-of-3 (if all present) and every 2-subset that is fully present
        subsets = []
        if len(present) == 3:
            subsets.append((0, 1, 2))
        for combo in [(0, 1), (0, 2), (1, 2)]:
            if all(l[i] is not None for i in combo):
                subsets.append(combo)
        for combo in subsets:
            key = tuple(l[i] for i in combo)
            blocks[combo][key].append(p)

    # materialize groups, dedup identical member-sets (highest agreement wins)
    raw = []
    for combo, keyed in blocks.items():
        dets = [DET[i] for i in combo]
        for key, members in keyed.items():
            if len(members) < 2:
                continue
            l = {DET[i]: key[j] for j, i in enumerate(combo)}
            raw.append({"agree": len(combo), "dets": dets, "label": l,
                        "members": frozenset(members)})

    best: dict = {}
    for g in raw:
        k = g["members"]
        if k not in best or g["agree"] > best[k]["agree"]:
            best[k] = g
    groups = list(best.values())

    # normalize native strengths across the groups that use each detector
    conv_w = {fid: s.get("total_weight", 0.0) for fid, s in conv_str.items()}
    patt_w = {fid: s.get("total_weight", 0.0) for fid, s in patt_str.items()}
    emb_w  = {fid: s.get("n_cooccur_windows", 0.0) for fid, s in emb_str.items()}
    conv_n, patt_n, emb_n = log_minmax(conv_w), log_minmax(patt_w), log_minmax(emb_w)

    for g in groups:
        comps, lab_ = [], g["label"]
        if "conv" in lab_: comps.append(conv_n.get(lab_["conv"], 0.0))
        if "patt" in lab_: comps.append(patt_n.get(lab_["patt"], 0.0))
        if "emb"  in lab_: comps.append(emb_n.get(lab_["emb"], 0.0))
        g["score"] = g["agree"] + (sum(comps) / len(comps) if comps else 0.0)

    groups.sort(key=lambda g: (-g["agree"], -g["score"], -len(g["members"])))

    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    with (out / "registry.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["group_id", "detectors_agreeing", "detectors", "n_members",
                    "conv_fleet", "patt_fleet", "emb_fleet", "conv_weight",
                    "patt_weight", "emb_windows", "emb_confirmed",
                    "consensus_score", "members"])
        for gid, g in enumerate(groups):
            lab_ = g["label"]
            cf, pf, ef = lab_.get("conv"), lab_.get("patt"), lab_.get("emb")
            w.writerow([
                gid, g["agree"], "+".join(g["dets"]), len(g["members"]),
                cf if cf is not None else "", pf if pf is not None else "",
                ef if ef is not None else "",
                round(conv_w.get(cf, 0.0)) if cf is not None else "",
                round(patt_w.get(pf, 0.0)) if pf is not None else "",
                round(emb_w.get(ef, 0.0)) if ef is not None else "",
                emb_str.get(ef, {}).get("confirmed", "") if ef is not None else "",
                round(g["score"], 4),
                ",".join(str(p) for p in sorted(g["members"])),
            ])

    n3 = sum(1 for g in groups if g["agree"] == 3)
    summary = [
        f"Consensus registry: {len(groups):,} fused groups",
        f"  3-of-3 (all detectors agree): {n3:,}",
        f"  2-of-3:                       {len(groups) - n3:,}",
        f"Plates in ≥2 detectors: {len([p for p in plates if sum(x is not None for x in lab(p)) >= 2]):,}",
        f"Plates in all 3 detectors: {len([p for p in plates if all(x is not None for x in lab(p))]):,}",
        "",
        "== Top 15 consensus groups ==",
    ]
    for gid, g in enumerate(groups[:15]):
        mem = sorted(g["members"])
        shown = ",".join(str(p) for p in mem[:6]) + ("…" if len(mem) > 6 else "")
        summary.append(f"  #{gid:<3} {g['agree']}-of-3 [{'+'.join(g['dets'])}]  "
                       f"n={len(mem):<3} score={g['score']:.3f}  {{{shown}}}")
    (out / "summary.txt").write_text("\n".join(summary) + "\n")
    print("\n".join(summary))
    print(f"\n[registry] wrote registry.csv ({len(groups):,} groups) and summary.txt to {out}/")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--convoy", default=f"{REPO}/data/output/communities/fleets.csv")
    p.add_argument("--patterns", default=f"{REPO}/data/downstream/pattern_graph/fleets.csv")
    p.add_argument("--embed", default=f"{REPO}/data/output/embed_31/fleets.csv")
    p.add_argument("--out-dir", default=f"{REPO}/data/downstream/consensus_registry")
    a = p.parse_args()
    main(a.convoy, a.patterns, a.embed, a.out_dir)
