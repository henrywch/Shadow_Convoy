"""Stage 1 — build per-plate camera sequences (Spark, CPU).

Method 2 of doc/ML_Approaches.md (trajectory embedding). This is the
*data-parallel ETL* stage: turn 276 M raw sightings into one compact
trajectory token-sequence per surviving plate, which the GPU embedding
model (Stage 2) consumes.

CSV (no header):  plate STRING, location STRING, timestamp BIGINT (unix s)

Output (Parquet directory):
    plate   INT          the vehicle id
    cameras ARRAY<INT>   cameras visited, ordered by time
    times   ARRAY<LONG>  matching visit timestamps (kept for Stage 3's
                         temporal co-occurrence confirmation)
    n       INT          sequence length

Runs on the EXISTING Spark/Slurm infra unchanged:

    INPUT=.../data/input/31.csv \
    OUTPUT=.../data/output/embed_31/sequences \
    JOB=.../src/embed/build_sequences.py \
      sbatch src/slurm/cluster.sbatch

Prefilter flags mirror max_growth_job.py so the surviving plate set is
directly comparable across the two pipelines.
"""
import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

SCHEMA = StructType([
    StructField("plate",     StringType(), nullable=False),
    StructField("location",  StringType(), nullable=False),
    StructField("timestamp", LongType(),   nullable=False),
])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("input")
    p.add_argument("output")
    p.add_argument("--min-observations", type=int, default=10,
                   help="drop plates with < N total sightings (dominant prefilter)")
    p.add_argument("--min-visits", type=int, default=3,
                   help="drop plates seen at < N distinct cameras — a 1-camera "
                        "plate has no route to embed")
    p.add_argument("--max-plates", type=int, default=500_000,
                   help="cap surviving plates (top-N by sighting count); 0 = no cap")
    p.add_argument("--max-len", type=int, default=128,
                   help="truncate sequences to the last N visits (keeps the embed "
                        "model's input bounded; long-tail taxis/buses are outliers). "
                        "Pass 0 (or any non-positive value) to disable truncation.")
    a = p.parse_args()

    spark = (
        SparkSession.builder
        .appName("acars-embed-sequences")
        .config("spark.sql.shuffle.partitions", "1000")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    df = (
        spark.read.schema(SCHEMA).option("header", "false").csv(a.input)
        .selectExpr("CAST(plate AS INT) AS plate",
                    "CAST(location AS INT) AS camera",
                    "timestamp AS t")
    )

    # ── Plate-side prefilter (same shape as max_growth) ──────────────────
    stats = (
        df.groupBy("plate")
          .agg(F.countDistinct("camera").alias("nc"),
               F.count("*").alias("nv"))
          .filter((F.col("nc") >= a.min_visits) &
                  (F.col("nv") >= a.min_observations))
    )
    if a.max_plates > 0:
        stats = stats.orderBy(F.col("nv").desc()).limit(a.max_plates)
    keep = stats.select("plate")
    df = df.join(F.broadcast(keep), "plate", "left_semi")

    # ── Build the ordered sequence per plate ─────────────────────────────
    # sort_array on struct(t, camera) orders by t first, so the camera list
    # comes out chronological; we then keep cameras and times in lockstep.
    seq = (
        df.groupBy("plate")
          .agg(F.sort_array(F.collect_list(F.struct("t", "camera"))).alias("tc"))
          .withColumn("cameras", F.expr("transform(tc, x -> x.camera)"))
          .withColumn("times",   F.expr("transform(tc, x -> x.t)"))
          .withColumn("n", F.size("cameras"))
    )
    if a.max_len > 0:
        # keep only the last max_len visits if longer
        seq = (
            seq.withColumn("cameras",
                           F.expr(f"slice(cameras, greatest(1, n - {a.max_len} + 1), {a.max_len})"))
               .withColumn("times",
                           F.expr(f"slice(times, greatest(1, n - {a.max_len} + 1), {a.max_len})"))
               .withColumn("n", F.size("cameras"))
        )
    seq = seq.select("plate", "cameras", "times", "n")

    n_plates = seq.count()
    print(f"[embed-seq] {n_plates:,} plates survive prefilter "
          f"(min_observations={a.min_observations}, min_visits={a.min_visits}, "
          f"max_plates={a.max_plates}, max_len={a.max_len})")

    seq.write.mode("overwrite").parquet(a.output)
    print(f"[embed-seq] sequences written to {a.output}")
    spark.stop()


if __name__ == "__main__":
    main()
