"""Promote MaxGrowth patterns into labeled fleets via graph community detection.

The route-evidence sibling of scripts/graph_communities.py (which only reads the
convoy CSV). This reads the MaxGrowth `patterns.csv` and produces a SECOND,
independent fleet labelling — the input the consensus registry (doc/
Downstream_Tasks.md §1.1) fuses against the convoy fleets and embedding clusters.

Input:   patterns.csv (schema: route, n_cameras, members, n_plates).
Output (data/downstream/pattern_graph/ by default):
  * communities.csv   plate, community_id, degree
  * fleets.csv        community_id, n_plates, total_weight, n_routes, members

Algorithm:
  Build a weighted undirected graph
      V = distinct plates appearing in any pattern
      E = {(a, b)} unrolled from each pattern's members (C(n,2) per pattern),
          weighted by n_cameras — a longer co-traversed route is stronger
          glue than a short one — and SUMMED across every pattern the pair
          shares (pre-aggregated in a dict, so a recurring pair on long routes
          rises to the top). Optionally prune edges below --min-weight before
          community detection to drop one-off weak links and shrink the graph.
  Run Louvain (networkx, weighted), else label propagation.

Usage:
    python pattern_graph.py <max_growth_output_dir> [--out-dir DIR] [--min-weight W]
"""
import argparse
import csv
from collections import defaultdict
from itertools import combinations
from pathlib import Path

import networkx as nx


def build_graph(patterns_csv: Path, min_weight: int):
    """Pair-projection of the patterns, edge weight = Σ n_cameras over shared
    patterns. Also track how many distinct patterns each pair co-traversed."""
    weights: dict[tuple[str, str], int] = defaultdict(int)
    npatterns: dict[tuple[str, str], int] = defaultdict(int)
    n_rows = 0
    with patterns_csv.open() as f:
        for row in csv.DictReader(f):
            members = row["members"].split(",")
            ncam = int(row["n_cameras"])
            n_rows += 1
            for a, b in combinations(sorted(members), 2):
                weights[(a, b)] += ncam
                npatterns[(a, b)] += 1
    raw_edges = len(weights)
    g = nx.Graph()
    for (a, b), w in weights.items():
        if w >= min_weight:
            g.add_edge(a, b, weight=w, n_patterns=npatterns[(a, b)])
    print(f"[pattern-graph] {n_rows:,} patterns → {raw_edges:,} distinct pairs; "
          f"kept {g.number_of_edges():,} edges (w≥{min_weight}), "
          f"{g.number_of_nodes():,} plates")
    return g


def detect(g: nx.Graph) -> list[set]:
    """Louvain if available, else label propagation. Both honour edge weights."""
    try:
        from networkx.algorithms.community import louvain_communities
        return louvain_communities(g, weight="weight", seed=42)
    except ImportError:
        from networkx.algorithms.community import label_propagation_communities
        return list(label_propagation_communities(g))


def main(in_dir: str, out_dir: str, min_weight: int) -> None:
    patterns_csv = Path(in_dir) / "patterns.csv"
    if not patterns_csv.exists():
        raise SystemExit(f"no patterns.csv under {in_dir}")
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)

    g = build_graph(patterns_csv, min_weight)
    if g.number_of_edges() == 0:
        raise SystemExit("[pattern-graph] no edges survived --min-weight; lower it")

    comms = detect(g)
    comms.sort(key=len, reverse=True)
    print(f"[pattern-graph] {len(comms):,} communities; top sizes: "
          f"{[len(c) for c in comms[:8]]}{' …' if len(comms) > 8 else ''}")

    with (out / "communities.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["plate", "community_id", "degree"])
        for cid, members in enumerate(comms):
            for p in sorted(members, key=int):
                w.writerow([p, cid, g.degree(p)])

    with (out / "fleets.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["community_id", "n_plates", "total_weight", "n_routes", "members"])
        for cid, members in enumerate(comms):
            sub = g.subgraph(members)
            tw = sum(d["weight"] for _, _, d in sub.edges(data=True))
            nr = sum(d["n_patterns"] for _, _, d in sub.edges(data=True))
            w.writerow([cid, len(members), tw, nr,
                        ",".join(sorted(members, key=int))])

    print(f"[pattern-graph] wrote communities.csv and fleets.csv under {out}/")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("max_growth_output_dir", help="dir containing patterns.csv")
    p.add_argument("--out-dir", default="data/downstream/pattern_graph",
                   help="default: data/downstream/pattern_graph")
    p.add_argument("--min-weight", type=int, default=0,
                   help="drop edges with summed weight below this before "
                        "community detection (0 = keep all)")
    a = p.parse_args()
    main(a.max_growth_output_dir, a.out_dir, a.min_weight)
