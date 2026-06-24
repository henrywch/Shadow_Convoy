"""Tier 2 — cloned-plate (套牌车) detection via graph-based impossible travel.

The forensic dual of convoy mining (doc/Downstream_Tasks.md §2.4): instead of
plates that move *together*, flag a single plate whose own sightings are
**physically impossible** — it appears at two cameras in less time than anyone
could travel between them. That plate is almost certainly cloned (two physical
vehicles sharing one number) or a data error.

We have no GPS, so "travel time between cameras" comes from the camera transition
graph (#7, §3.1): build a directed graph with edge weight = the learned median
hop time, then the shortest-path time SP(i,j) is the fastest anyone has been
observed to get from camera i to camera j. For a plate's consecutive sightings
(cam_i,t_i)→(cam_j,t_j):

    Δt = t_j - t_i   is IMPOSSIBLE if   Δt < SP(i,j) · tolerance        (too fast)
                                   or   i,j graph-distant and Δt tiny   (teleport)

A plate with several impossible transitions (the two clones keep interleaving) is
a strong clone candidate; a single one may be a timestamp glitch — so we rank by
the count and gate on --min-hits.

Inputs:  raw sightings CSV (plate, camera, timestamp) + camera_graph/edges.csv (#7).
Output (data/downstream/clone_detection/ by default):
  clones.csv    plate, n_transitions, n_impossible, impossible_ratio,
                worst_example, also_ocr_candidate
  summary.txt   counts + the top clone candidates

Usage:
    python clone_detection.py data/input/31.csv \
        --edges data/downstream/camera_graph/edges.csv
"""
import argparse
import csv
from pathlib import Path

REPO = "/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop"


def shortest_path_times(edges_csv: Path, cutoff: float):
    """All-pairs shortest *travel time* over the directed camera graph
    (edge weight = median_dt). Pruned at `cutoff` seconds — pairs farther than
    that are treated as unreachable (and handled by the teleport rule)."""
    import networkx as nx
    import polars as pl
    e = pl.read_csv(edges_csv)
    g = nx.DiGraph()
    for s, d, w in zip(e["src"], e["dst"], e["median_dt"]):
        g.add_edge(int(s), int(d), t=float(w))
    rows = []
    for src, dists in nx.all_pairs_dijkstra_path_length(g, cutoff=cutoff, weight="t"):
        for dst, t in dists.items():
            if src != dst:
                rows.append((src, dst, t))
    sp = pl.DataFrame(rows, schema=["cam_i", "cam_j", "sp_t"], orient="row")
    print(f"[clone] camera graph: {g.number_of_nodes()} nodes, "
          f"{g.number_of_edges():,} edges → {len(sp):,} reachable pairs (cutoff {cutoff:.0f}s)")
    return sp


def transitions(raw_csv: Path, max_check_dt: int):
    """Per-plate consecutive (cam_i→cam_j, Δt). Returns (all_counts, candidates)
    where candidates are the Δt ≤ max_check_dt, distinct-camera transitions worth
    checking."""
    import polars as pl
    df = (pl.read_csv(raw_csv, has_header=False, new_columns=["plate", "camera", "t"])
            .sort(["plate", "t"]))
    df = df.with_columns([
        pl.col("camera").shift(1).over("plate").alias("cam_i"),
        pl.col("t").shift(1).over("plate").alias("t_i"),
    ]).drop_nulls(["cam_i"]).rename({"camera": "cam_j", "t": "t_j"})
    df = df.with_columns((pl.col("t_j") - pl.col("t_i")).alias("dt"))
    df = df.filter((pl.col("dt") > 0) & (pl.col("cam_i") != pl.col("cam_j")))
    counts = df.group_by("plate").len().rename({"len": "n_transitions"})
    cand = df.filter(pl.col("dt") <= max_check_dt).select(
        ["plate", "cam_i", "cam_j", "t_i", "dt"])
    return counts, cand


def main(raw_csv, edges_csv, out_dir, tol, min_gap, teleport_dt, min_hits,
         max_check_dt, ocr_csv):
    import polars as pl
    sp = shortest_path_times(Path(edges_csv), cutoff=max_check_dt / max(tol, 0.05))
    counts, cand = transitions(Path(raw_csv), max_check_dt)
    print(f"[clone] {counts['n_transitions'].sum():,} inter-camera transitions; "
          f"{len(cand):,} within {max_check_dt}s to check")

    j = cand.join(sp, on=["cam_i", "cam_j"], how="left")
    # impossible: too-fast on a known route, OR teleport between graph-distant cams
    j = j.with_columns(
        (((pl.col("sp_t").is_not_null()) & (pl.col("dt") < pl.col("sp_t") * tol)
          & (pl.col("sp_t") - pl.col("dt") >= min_gap))
         | ((pl.col("sp_t").is_null()) & (pl.col("dt") <= teleport_dt))).alias("impossible")
    )
    imp = j.filter(pl.col("impossible"))
    print(f"[clone] {len(imp):,} impossible transitions flagged")

    # per-plate aggregation + worst (smallest-dt) example
    imp = imp.with_columns(
        (pl.col("cam_i").cast(str) + "->" + pl.col("cam_j").cast(str)
         + " in " + pl.col("dt").cast(str) + "s (sp="
         + pl.col("sp_t").fill_null(-1).round(0).cast(int).cast(str) + "s)").alias("ex"))
    per = (imp.sort("dt")
              .group_by("plate")
              .agg([pl.len().alias("n_impossible"), pl.col("ex").first().alias("worst_example")]))
    per = per.join(counts, on="plate", how="left").with_columns(
        (pl.col("n_impossible") / pl.col("n_transitions")).round(4).alias("impossible_ratio"))
    per = per.filter(pl.col("n_impossible") >= min_hits).sort(
        ["n_impossible", "impossible_ratio"], descending=True)

    # annotate plates that also appear as OCR-confusion candidates (different cause)
    ocr_plates = set()
    if ocr_csv and Path(ocr_csv).exists():
        o = pl.read_csv(ocr_csv)
        ocr_plates = set(o["plate_a"].to_list()) | set(o["plate_b"].to_list())
    per = per.with_columns(
        pl.col("plate").is_in(list(ocr_plates)).alias("also_ocr_candidate"))

    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    per.select(["plate", "n_transitions", "n_impossible", "impossible_ratio",
                "worst_example", "also_ocr_candidate"]).write_csv(out / "clones.csv")

    summary = [
        f"Cloned-plate (套牌车) detection from {Path(raw_csv).name}",
        f"  tolerance={tol}  min_gap={min_gap}s  teleport_dt={teleport_dt}s  min_hits={min_hits}",
        f"  impossible transitions flagged: {len(imp):,}",
        f"  clone-candidate plates (≥{min_hits} hits): {len(per):,}",
        f"  …of which also OCR-confusion candidates: {int(per['also_ocr_candidate'].sum()) if len(per) else 0}",
        "",
        "== Top clone candidates ==",
    ]
    for r in per.head(15).iter_rows(named=True):
        flag = " [OCR?]" if r["also_ocr_candidate"] else ""
        summary.append(f"  plate {r['plate']:<10} {r['n_impossible']:>3} impossible / "
                       f"{r['n_transitions']:<5} ({r['impossible_ratio']:.1%}){flag}  "
                       f"ex: {r['worst_example']}")
    (out / "summary.txt").write_text("\n".join(summary) + "\n")
    print("\n".join(summary))
    print(f"\n[clone] wrote clones.csv ({len(per):,} candidates) and summary.txt to {out}/")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("raw_csv", help="raw sightings CSV (plate, camera, timestamp)")
    p.add_argument("--edges", default=f"{REPO}/data/downstream/camera_graph/edges.csv",
                   help="camera transition graph edges.csv (from #7)")
    p.add_argument("--out-dir", default=f"{REPO}/data/downstream/clone_detection")
    p.add_argument("--ocr", default=f"{REPO}/data/output/ocr_candidates.csv",
                   help="ocr_candidates.csv to cross-annotate (OCR ghost vs real clone)")
    p.add_argument("--tol", type=float, default=0.5,
                   help="flag if Δt < shortest-path-time × tol (lower = stricter)")
    p.add_argument("--min-gap", type=int, default=60,
                   help="minimum sp−Δt seconds to flag (avoids borderline noise)")
    p.add_argument("--teleport-dt", type=int, default=120,
                   help="graph-distant cameras seen within this many seconds = teleport")
    p.add_argument("--min-hits", type=int, default=3,
                   help="min impossible transitions for a plate to be a candidate")
    p.add_argument("--max-check-dt", type=int, default=3600,
                   help="only check transitions with Δt ≤ this (perf bound)")
    a = p.parse_args()
    main(a.raw_csv, a.edges, a.out_dir, a.tol, a.min_gap, a.teleport_dt,
         a.min_hits, a.max_check_dt, a.ocr)
