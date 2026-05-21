"""Promote convoy pairs into labeled fleets via graph community detection.

Input:   the convoy_fpgrowth output CSV (`plates, k, count`).
Output:
  * `communities.csv`   one row per plate: (plate, community_id, degree)
  * `fleets.csv`        one row per community: (community_id, n_plates,
                        total_weight, member_plates)

Algorithm:
  Build a weighted undirected graph
      V = distinct plates appearing in any convoy
      E = {(a, b, count)} unrolled from every itemset of size ≥ 2
                          (for k>2 we add C(k,2) edges weighted by the
                           itemset's count, giving size-k members a
                           proportionally larger pull on each other).
  Run Louvain (preferred; uses networkx's community module if available)
  or fall back to Label Propagation.

We use networkx — the convoy population is small (hundreds of plates),
so single-machine community detection is the right tool.

Usage:
    python graph_communities.py <convoy_csv> <out_dir>
"""
import argparse
import csv
from collections import defaultdict
from itertools import combinations
from pathlib import Path

import networkx as nx


def build_graph(convoy_csv: str) -> nx.Graph:
    """Pair-projection of the frequent-itemset output, weights summed."""
    weights: dict[tuple[str, str], int] = defaultdict(int)
    with open(convoy_csv) as f:
        for row in csv.DictReader(f):
            plates = row["plates"].split(",")
            count  = int(row["count"])
            for a, b in combinations(sorted(plates), 2):
                weights[(a, b)] += count

    g = nx.Graph()
    for (a, b), w in weights.items():
        g.add_edge(a, b, weight=w)
    return g


def detect(g: nx.Graph) -> list[set[str]]:
    """Louvain if available, else label propagation. Both honour edge weights."""
    try:
        from networkx.algorithms.community import louvain_communities
        return louvain_communities(g, weight="weight", seed=42)
    except ImportError:
        from networkx.algorithms.community import label_propagation_communities
        return list(label_propagation_communities(g))


def main(convoy_csv: str, out_dir: str) -> None:
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)

    g = build_graph(convoy_csv)
    print(f"[graph] {g.number_of_nodes():,} plates, {g.number_of_edges():,} edges")

    comms = detect(g)
    comms.sort(key=len, reverse=True)
    print(f"[graph] {len(comms)} communities; sizes: "
          f"{[len(c) for c in comms[:8]]}{' ...' if len(comms) > 8 else ''}")

    # plate-level CSV: plate, community, degree
    with (out / "communities.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["plate", "community_id", "degree"])
        for cid, members in enumerate(comms):
            for p in sorted(members):
                w.writerow([p, cid, g.degree(p)])

    # community-level CSV: id, size, weight, members
    with (out / "fleets.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["community_id", "n_plates", "total_weight", "members"])
        for cid, members in enumerate(comms):
            tw = sum(d["weight"] for u, v, d in g.subgraph(members).edges(data=True))
            w.writerow([cid, len(members), tw, ",".join(sorted(members))])

    print(f"[graph] wrote communities.csv and fleets.csv under {out}/")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("convoy_csv")
    p.add_argument("out_dir")
    main(*vars(p.parse_args()).values())
