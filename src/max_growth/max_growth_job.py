"""MaxGrowth platoon miner — Spark wrapper.

Architecture (industrial-grade distribution):

  1. Plate-side prefilter           Spark groupBy   — drops the long tail of
                                                     one-off plates.
  2. Trajectory build               Spark groupBy   — sorted (time, camera)
                                                     list per surviving plate.
  3. Cluster materialization        Spark groupBy   — dual-offset bucketize by
                                                     (camera, ε-window), keep
                                                     ε-close groups of size ≥ m.
  4. Driver collect (small!)        Spark .collect()— only the surviving
                                                     trajectories + clusters.
  5. Broadcast                      Spark broadcast — trajectories, position
                                                     indices, clusters_by_camera.
  6. Parallel growth                Spark RDD       — `grow_from_root` runs in
                                                     parallel across executors,
                                                     one task per slice of roots.
  7. Distinct + collect patterns                    — dedup in Spark before
                                                     pulling back to driver.
  8. Maximality filter on driver                    — small input; O(N²) is fine.

The whole-data parts (1–3, 6–7) live in Spark; the driver only handles
post-broadcast bookkeeping and the small final maximality filter.
"""
import argparse
import csv
import sys
from itertools import islice
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

sys.path.insert(0, str(Path(__file__).resolve().parent))
from core import Cluster, Pattern, grow_from_root, maximal_only         # noqa: E402

SCHEMA = StructType([
    StructField("plate",     StringType(), nullable=False),
    StructField("location",  StringType(), nullable=False),
    StructField("timestamp", LongType(),   nullable=False),
])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("input")
    p.add_argument("output")
    p.add_argument("--min-size",   type=int, default=2,   help="m: min plates per platoon")
    p.add_argument("--min-length", type=int, default=3,   help="k: min cameras per route")
    p.add_argument("--gap",        type=int, default=1,   help="d: max missing cameras")
    p.add_argument("--eps",        type=int, default=300, help="ε: co-arrival seconds at one camera")
    p.add_argument("--min-visits",       type=int, default=0,
                   help="drop plates seen at < N distinct cameras (default = --min-length)")
    p.add_argument("--min-observations", type=int, default=10,
                   help="drop plates with < N total observations — the dominant "
                        "prefilter; mirrors FP-Growth's min_count.")
    p.add_argument("--max-plates",       type=int, default=500_000,
                   help="hard cap on plates surviving prefilter; top-N by "
                        "observation count. Distributed enumeration handles "
                        "much larger plate sets than driver-only would.")
    p.add_argument("--max-cluster-size", type=int, default=500,
                   help="drop ε-clusters with more plates than this. A 1k-plate "
                        "cluster has C(1k,m) combinatorial children and never "
                        "carries platoon signal; analogous to FP-Growth's "
                        "density_cap=2000.")
    p.add_argument("--root-slices", type=int, default=200,
                   help="number of Spark partitions for the root-growth fan-out. "
                        "Should be ≥ executor cores × 2 for balanced parallelism.")
    p.add_argument("--skip-maximal-filter", action="store_true",
                   help="emit raw deduped patterns without the maximality filter. "
                        "Use when post-growth pattern count > ~500k and you want "
                        "the run to finish; downstream tools can dedupe further.")
    a = p.parse_args()
    if a.min_visits == 0:
        a.min_visits = a.min_length

    spark = (
        SparkSession.builder
        .appName("acars-max-growth")
        .config("spark.sql.shuffle.partitions", "1000")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.driver.maxResultSize", "0")
        # 1 GB hard cap on broadcast — keep it tight to fail fast if our
        # collected data outgrows the architecture. Bump if needed.
        .config("spark.broadcast.blockSize", "16m")
        .getOrCreate()
    )
    sc = spark.sparkContext

    # Ship `core.py` to every executor so the broadcast-pickled closure
    # (which references `grow_from_root` by module name) can deserialize
    # there. Driver-side `sys.path.insert` above only fixes the driver.
    sc.addPyFile(str(Path(__file__).resolve().parent / "core.py"))

    df = (
        spark.read.schema(SCHEMA).option("header", "false").csv(a.input)
        .selectExpr("CAST(plate AS INT) AS plate",
                    "CAST(location AS INT) AS camera",
                    "timestamp AS t")
    )

    # ────────────────────────────────────────────────────────────────
    # 1. PLATE-SIDE PREFILTER
    # ────────────────────────────────────────────────────────────────
    visit_stats = (
        df.groupBy("plate")
          .agg(F.countDistinct("camera").alias("nc"),
               F.count("*").alias("nv"))
          .filter((F.col("nc") >= a.min_visits) &
                  (F.col("nv") >= a.min_observations))
          .orderBy(F.col("nv").desc())
          .limit(a.max_plates)
    )
    keep_plates = visit_stats.select("plate")
    df = df.join(F.broadcast(keep_plates), "plate", "left_semi")
    n_plates = visit_stats.count()
    print(f"[max-growth] prefilter kept {n_plates:,} plates "
          f"(min_visits={a.min_visits}, min_observations={a.min_observations}, "
          f"max_plates={a.max_plates})")

    # ────────────────────────────────────────────────────────────────
    # 2. TRAJECTORY BUILD (distributed)
    # ────────────────────────────────────────────────────────────────
    traj_rows = (
        df.groupBy("plate")
          .agg(F.sort_array(F.collect_list(F.struct("t", "camera"))).alias("traj"))
          .collect()
    )
    trajectories: dict[int, list[tuple[int, int]]] = {
        r["plate"]: [(int(s["t"]), int(s["camera"])) for s in r["traj"]]
        for r in traj_rows
    }
    n_visits = sum(len(t) for t in trajectories.values())
    print(f"[max-growth] collected trajectories: {n_visits:,} total visits "
          f"(~{n_visits * 32 // (1024 * 1024)} MiB raw)")

    # ────────────────────────────────────────────────────────────────
    # 3. CLUSTER MATERIALIZATION (distributed, dual-offset for boundary safety)
    # ────────────────────────────────────────────────────────────────
    def clusters_at_offset(offset: int):
        return (
            df.withColumn("bucket", F.floor((F.col("t") - offset) / a.eps))
              .groupBy("camera", "bucket")
              .agg(F.collect_set("plate").alias("members"))
              .filter((F.size("members") >= a.min_size)
                    & (F.size("members") <= a.max_cluster_size))
              .select("camera", "members")
        )
    cluster_rows = (
        clusters_at_offset(0)
          .unionByName(clusters_at_offset(a.eps // 2))
          .dropDuplicates(["camera", "members"])
          .collect()
    )
    clusters: list[Cluster] = [
        Cluster(int(r["camera"]), frozenset(int(p) for p in r["members"]))
        for r in cluster_rows
    ]
    by_camera: dict[int, list[Cluster]] = {}
    for cl in clusters:
        by_camera.setdefault(cl.camera, []).append(cl)
    print(f"[max-growth] {len(clusters):,} ε-clusters across "
          f"{len(by_camera):,} cameras")

    # ────────────────────────────────────────────────────────────────
    # 4. POSITION INDICES (driver, cheap)
    # ────────────────────────────────────────────────────────────────
    pos = {
        plate: {cam: idx for idx, (_, cam) in enumerate(traj)}
        for plate, traj in trajectories.items()
    }
    next_cams_per_plate = {p: [c for _, c in t] for p, t in trajectories.items()}

    # ────────────────────────────────────────────────────────────────
    # 5. BROADCAST
    # ────────────────────────────────────────────────────────────────
    by_camera_bc = sc.broadcast(by_camera)
    pos_bc       = sc.broadcast(pos)
    next_cams_bc = sc.broadcast(next_cams_per_plate)

    # ────────────────────────────────────────────────────────────────
    # 6. PARALLEL GROWTH
    # ────────────────────────────────────────────────────────────────
    m, k, d = a.min_size, a.min_length, a.gap

    def grow_partition(roots_iter):
        bc_clusters = by_camera_bc.value
        bc_pos      = pos_bc.value
        bc_nexts    = next_cams_bc.value
        for root in roots_iter:
            for pat in grow_from_root(root, bc_clusters, bc_pos, bc_nexts, m, k, d):
                yield (pat.route, tuple(sorted(pat.members)))

    roots_rdd = sc.parallelize(clusters, numSlices=a.root_slices)
    pattern_pairs = (
        roots_rdd.mapPartitions(grow_partition)
                 .distinct()                       # dedupe in Spark
                 .collect()
    )
    print(f"[max-growth] raw maximal-candidate patterns: {len(pattern_pairs):,}")

    # ────────────────────────────────────────────────────────────────
    # 7. MAXIMALITY FILTER (driver, post-dedup so input is small)
    # ────────────────────────────────────────────────────────────────
    patterns = [Pattern(route=r, members=frozenset(mem)) for r, mem in pattern_pairs]
    if a.skip_maximal_filter:
        print(f"[max-growth] skipping maximal filter (--skip-maximal-filter); "
              f"emitting {len(patterns):,} raw deduped patterns")
        maximal = patterns
    else:
        import time as _t
        t0 = _t.monotonic()
        maximal = maximal_only(patterns, d)
        print(f"[max-growth] maximal filter: {len(patterns):,} → {len(maximal):,} "
              f"in {_t.monotonic() - t0:.1f}s")
    maximal.sort(key=lambda p: (-len(p.members), -len(p.route)))

    spark.stop()

    # ────────────────────────────────────────────────────────────────
    # 8. WRITE
    # ────────────────────────────────────────────────────────────────
    out_dir = Path(a.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "patterns.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["route", "n_cameras", "members", "n_plates"])
        for pat in maximal:
            w.writerow([
                "->".join(str(c) for c in pat.route),
                len(pat.route),
                ",".join(str(p) for p in sorted(pat.members)),
                len(pat.members),
            ])
    print(f"[max-growth] written to {out_dir}/patterns.csv")


if __name__ == "__main__":
    main()
