"""Quick descriptive statistics for the Accompanying Cars CSV.

Schema (no header):
    plate     STRING
    location  STRING
    timestamp BIGINT   -- unix seconds

Usage:
    spark-submit basic_stats.py <input_csv> <output_dir>
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    count,
    max as smax,
    min as smin,
)
from pyspark.sql.types import LongType, StringType, StructField, StructType

SCHEMA = StructType([
    StructField("plate", StringType(), nullable=False),
    StructField("location", StringType(), nullable=False),
    StructField("timestamp", LongType(), nullable=False),
])


def main(input_path: str, output_dir: str) -> None:
    spark = SparkSession.builder.appName("acars-basic-stats").getOrCreate()

    df = (
        spark.read
        .schema(SCHEMA)
        .option("header", "false")
        .csv(input_path)
    )

    stats = df.agg(
        smin("timestamp").alias("min_ts"),
        smax("timestamp").alias("max_ts"),
        count("*").alias("rows"),
        approx_count_distinct("plate").alias("unique_plates"),
        approx_count_distinct("location").alias("unique_locs"),
    )

    (
        stats.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(output_dir)
    )

    r = stats.collect()[0]
    print(
        f"rows={r['rows']:,}  "
        f"span={(r['max_ts'] - r['min_ts']) / 86400:.2f}d  "
        f"plates~{r['unique_plates']:,}  "
        f"locations~{r['unique_locs']:,}"
    )

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("Usage: spark-submit basic_stats.py <input_csv> <output_dir>")
    main(sys.argv[1], sys.argv[2])
