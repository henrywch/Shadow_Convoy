"""Mine accompanying-vehicle convoys with FP-Growth.

CSV (no header):  plate STRING, location STRING, timestamp BIGINT (unix s)

For each (location, time-bucket) we form a transaction = the distinct plates
seen at that location during that bucket. Frequent itemsets across all
transactions are vehicle groups that recurrently co-locate — i.e. convoys.

Two offset grids (0 and window/2) catch convoy members straddling a bucket
boundary; we keep max(freq_A, freq_B) per itemset, which is the same
boundary-smoothing trick the previous accompany_FPG.py used.

Usage:
    spark-submit convoy_fpgrowth.py <input_csv> <output_dir> [options]
"""
import argparse

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.storagelevel import StorageLevel

SCHEMA = StructType([
    StructField("plate", StringType(), nullable=False),
    StructField("location", StringType(), nullable=False),
    StructField("timestamp", LongType(), nullable=False),
])


def transactions(df, window, offset, density_cap):
    """(location, bucket) → distinct plates seen there.

    Singletons can't form pairs; mega-buckets (e.g. 10k+ plates at a busy
    intersection in 5 min) blow up FP-tree memory and contain no
    convoy-relevant signal anyway.
    """
    return (
        df.withColumn("bucket", F.floor((F.col("timestamp") - offset) / window))
        .groupBy("location", "bucket")
        .agg(F.collect_set("plate").alias("items"))
        .filter((F.size("items") >= 2) & (F.size("items") <= density_cap))
        .select("items")
    )


def mine(txns, min_support):
    """Frequent itemsets with `items` array_sort-ed so they compare across grids."""
    model = FPGrowth(
        itemsCol="items", minSupport=min_support, minConfidence=0.0
    ).fit(txns)
    return model.freqItemsets.withColumn("items", F.array_sort("items"))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("input")
    p.add_argument("output")
    p.add_argument("--window", type=int, default=300, help="bucket size, seconds")
    p.add_argument("--min-count", type=int, default=10, help="minimum convoy frequency")
    p.add_argument("--min-cars", type=int, default=2)
    p.add_argument("--max-cars", type=int, default=6)
    p.add_argument(
        "--density-cap", type=int, default=2000,
        help="drop buckets with more plates than this (looser → larger). "
             "Default 2000 ≈ 15%% above the largest legitimate bucket "
             "observed in the 31-day data; excludes the 23k outlier at loc 31.",
    )
    p.add_argument(
        "--min-plate-count", type=int, default=1,
        help="drop plates appearing fewer times globally (1 = no filter)",
    )
    a = p.parse_args()

    spark = (
        SparkSession.builder
        .appName("acars-convoy-fpgrowth")
        .config("spark.sql.shuffle.partitions", "1000")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # FP-tree path depth scales with transaction width; Kryo serializes
        # those paths recursively, blowing the default 1 MB thread stack on
        # 2000-wide buckets. 16 MB handles ~16k-deep trees with margin.
        .config("spark.executor.extraJavaOptions", "-Xss16m")
        .config("spark.driver.extraJavaOptions",   "-Xss16m")
        # FP-Growth's conditional-tree build can stall a task for a minute+;
        # default 120 s timeout was killing executors that were merely busy.
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "30s")
        .getOrCreate()
    )

    df = spark.read.schema(SCHEMA).option("header", "false").csv(a.input)

    # Loose noise filter; off by default — cluster can carry the full plate set.
    if a.min_plate_count > 1:
        keep = (
            df.groupBy("plate").count()
            .filter(F.col("count") >= a.min_plate_count)
            .select("plate")
        )
        df = df.join(keep, "plate", "left_semi")

    # Don't cache the raw frame: it's the biggest single thing in the job
    # (~13 GB decompressed across the cluster) and the least useful — each
    # downstream grid does exactly one scan of it, and GPFS reads are fast.
    # Caching it squeezes FP-Growth's heap, forcing spill/GC thrash and
    # eventually heartbeat-timeout cascades. Let Spark re-read the CSV.
    txns_a = transactions(df, a.window, 0, a.density_cap) \
        .persist(StorageLevel.MEMORY_AND_DISK)
    txns_b = transactions(df, a.window, a.window // 2, a.density_cap) \
        .persist(StorageLevel.MEMORY_AND_DISK)

    n = max(txns_a.count(), txns_b.count())
    min_support = a.min_count / n
    print(f"[convoy] transactions={n:,}  min_support={min_support:.2e} (={a.min_count}/{n})")

    res_a = mine(txns_a, min_support).withColumnRenamed("freq", "freq_a")
    res_b = mine(txns_b, min_support).withColumnRenamed("freq", "freq_b")

    convoys = (
        res_a.join(res_b, on="items", how="full_outer")
        .withColumn(
            "freq",
            F.greatest(
                F.coalesce("freq_a", F.lit(0)),
                F.coalesce("freq_b", F.lit(0)),
            ),
        )
        .filter(
            (F.size("items") >= a.min_cars)
            & (F.size("items") <= a.max_cars)
            & (F.col("freq") >= a.min_count)
        )
        .select(
            F.concat_ws(",", "items").alias("plates"),
            F.size("items").alias("k"),
            F.col("freq").alias("count"),
        )
        .orderBy(F.col("count").desc())
    )

    convoys.coalesce(1).write.mode("overwrite").option("header", "true").csv(a.output)
    print(f"[convoy] written to {a.output}")

    spark.stop()


if __name__ == "__main__":
    main()
