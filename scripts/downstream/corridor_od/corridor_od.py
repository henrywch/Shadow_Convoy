"""Tier 3 — convoy-corridor & OD-flow analysis (the unique payoff of MaxGrowth).

MaxGrowth patterns are **directed** routes (`11->227->557`), so unlike FP-Growth's
unordered convoy sets they carry origin→destination flow (doc/Downstream_Tasks.md
§3.2). This aggregates the 223 k patterns two ways and draws them on the recovered
camera map (#7):

  * OD flows      — each route's (origin = first camera, destination = last) →
                    the busiest group OD pairs (where convoys travel *to/from*).
  * corridor segs — each route split into consecutive camera hops → the segments
                    that groups traverse as units, weighted by group size.

Both carry distinct-plate counts (a corridor used by 500 plates beats one used by
5), mirroring analyze_patterns' corridor view but with direction + a map layout.

Inputs:  patterns.csv (#0/MaxGrowth) + camera_map.csv (#7, camera→x,y).
Output (data/downstream/corridor_od/ by default):
  od_flows.csv         origin, dest, n_patterns, n_plates, n_routes
  corridor_segments.csv src, dst, n_patterns, n_plates
  corridor_map.png     top corridor segments drawn on the camera map (#7)
  summary.txt          busiest OD pairs + segments

Usage:
    python corridor_od.py --patterns data/output/max_growth_31/patterns.csv \
        --camera-map data/downstream/camera_graph/camera_map.csv
"""
import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = "/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop"


def load_patterns(patterns_csv: Path):
    import pandas as pd
    df = pd.read_csv(patterns_csv)
    df["route_list"] = df["route"].str.split("->")
    df["members_list"] = df["members"].str.split(",")
    return df


def aggregate(df):
    """OD-pair and corridor-segment aggregates with distinct-plate counts."""
    od_patterns = defaultdict(int)
    od_plates = defaultdict(set)
    od_routes = defaultdict(set)
    seg_patterns = defaultdict(int)
    seg_plates = defaultdict(set)
    for route, members in zip(df["route_list"], df["members_list"]):
        o, d = int(route[0]), int(route[-1])
        od_patterns[(o, d)] += 1
        od_plates[(o, d)].update(members)
        od_routes[(o, d)].add("->".join(route))
        for a, b in zip(route, route[1:]):
            seg = (int(a), int(b))
            seg_patterns[seg] += 1
            seg_plates[seg].update(members)
    od = [{"origin": o, "dest": d, "n_patterns": od_patterns[(o, d)],
           "n_plates": len(od_plates[(o, d)]), "n_routes": len(od_routes[(o, d)])}
          for (o, d) in od_patterns]
    seg = [{"src": a, "dst": b, "n_patterns": seg_patterns[(a, b)],
            "n_plates": len(seg_plates[(a, b)])} for (a, b) in seg_patterns]
    od.sort(key=lambda r: (-r["n_plates"], -r["n_patterns"]))
    seg.sort(key=lambda r: (-r["n_plates"], -r["n_patterns"]))
    return od, seg


def plot_corridor_map(seg, coords, top, path):
    import numpy as np
    top_seg = [s for s in seg if s["src"] in coords and s["dst"] in coords][:top]
    if not top_seg:
        print("[corridor] no segments with mapped cameras; skipping map")
        return
    wmax = max(s["n_plates"] for s in top_seg)
    xs = [c[0] for c in coords.values()]; ys = [c[1] for c in coords.values()]
    fig, ax = plt.subplots(figsize=(9, 8))
    ax.scatter(xs, ys, s=6, c="lightgrey", zorder=1, linewidths=0)
    cmap = plt.get_cmap("viridis")
    for s in reversed(top_seg):
        (x0, y0), (x1, y1) = coords[s["src"]], coords[s["dst"]]
        f = s["n_plates"] / wmax
        ax.annotate("", xy=(x1, y1), xytext=(x0, y0), zorder=2,
                    arrowprops=dict(arrowstyle="-|>", color=cmap(f),
                                    alpha=0.5 + 0.5 * f, lw=0.5 + 3.5 * f))
    ax.set(title=f"Convoy corridors — top {len(top_seg)} group-flow segments on the camera map",
           xlabel="map dim-1", ylabel="map dim-2")
    sm = plt.cm.ScalarMappable(cmap=cmap,
                               norm=plt.Normalize(vmin=0, vmax=wmax))
    fig.colorbar(sm, ax=ax, label="distinct plates traversing segment (as a group)")
    fig.tight_layout(); fig.savefig(path, dpi=130); plt.close(fig)


def main(patterns_csv, camera_map_csv, out_dir, top):
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    df = load_patterns(Path(patterns_csv))
    od, seg = aggregate(df)
    print(f"[corridor] {len(df):,} patterns → {len(od):,} OD pairs, {len(seg):,} corridor segments")

    coords = {}
    cm = Path(camera_map_csv)
    if cm.exists():
        with cm.open() as f:
            for r in csv.DictReader(f):
                coords[int(r["camera"])] = (float(r["x"]), float(r["y"]))
        print(f"[corridor] camera map: {len(coords):,} cameras with coords")
    else:
        print(f"[corridor] camera map {cm} not found; OD/segment CSVs only, no map")

    with (out / "od_flows.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["origin", "dest", "n_patterns", "n_plates", "n_routes"])
        w.writeheader(); w.writerows(od)
    with (out / "corridor_segments.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["src", "dst", "n_patterns", "n_plates"])
        w.writeheader(); w.writerows(seg)
    if coords:
        plot_corridor_map(seg, coords, top, out / "corridor_map.png")

    summary = [
        f"Convoy-corridor & OD-flow analysis from {Path(patterns_csv).name}",
        f"  patterns:           {len(df):,}",
        f"  distinct OD pairs:  {len(od):,}",
        f"  corridor segments:  {len(seg):,}",
        "",
        "== Busiest group OD pairs (origin -> destination) ==",
    ]
    for r in od[:15]:
        summary.append(f"  {r['n_plates']:>5} plates  {r['n_patterns']:>5} pat  "
                       f"{r['n_routes']:>4} routes   {r['origin']} -> {r['dest']}")
    summary.append("")
    summary.append("== Busiest corridor segments (consecutive group hops) ==")
    for r in seg[:15]:
        summary.append(f"  {r['n_plates']:>5} plates  {r['n_patterns']:>5} pat   "
                       f"{r['src']} -> {r['dst']}")
    (out / "summary.txt").write_text("\n".join(summary) + "\n")
    print("\n".join(summary))
    print(f"\n[corridor] wrote od_flows.csv, corridor_segments.csv, corridor_map.png, "
          f"summary.txt to {out}/")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--patterns", default=f"{REPO}/data/output/max_growth_31/patterns.csv")
    p.add_argument("--camera-map", default=f"{REPO}/data/downstream/camera_graph/camera_map.csv")
    p.add_argument("--out-dir", default=f"{REPO}/data/downstream/corridor_od")
    p.add_argument("--top", type=int, default=200, help="top segments to draw on the map")
    a = p.parse_args()
    main(a.patterns, a.camera_map, a.out_dir, a.top)
