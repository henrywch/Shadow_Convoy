"""Per-location time-bucket density across several window widths.

For each window W in seconds, for every location, count how many entries
fall into each non-empty W-second bucket, then summarize the per-bucket
counts (mean / p50 / p95 / p99 / max). This is the right diagnostic for
picking `density_cap` in the convoy FP-Growth job: a bucket whose width
exceeds a location's 99th-percentile is rush-hour traffic, not a convoy.

Implementation notes:
  - Single-machine, polars-based. polars's Rust/Rayon threadpool saturates
    every core for each query; `collect_all` runs the per-window queries
    in parallel and shares the underlying CSV scan, so the file is read
    exactly once regardless of how many windows we ask for.
  - Streaming engine; peak RAM stays modest even on the full 5.8 GB CSV.

Output layout (in <out_dir>):
  density_<W>s.csv   per-location stats for window W, sorted by avg desc
  summary.txt        the compact format you asked for, all windows stacked

Usage:
    python density_stats.py <input_csv> [<out_dir>]
"""
import sys
from pathlib import Path

import polars as pl

WINDOWS = (300, 200, 100, 50)
SCHEMA = {"plate": pl.Utf8, "location": pl.Utf8, "timestamp": pl.Int64}


def per_location_stats(df: pl.LazyFrame, w: int) -> pl.LazyFrame:
    """Per-location summary of non-empty W-second bucket sizes."""
    bucket_counts = (
        df.with_columns((pl.col("timestamp") // w).alias("bucket"))
        .group_by("location", "bucket")
        .agg(pl.len().alias("n"))
    )
    return (
        bucket_counts.group_by("location")
        .agg(
            pl.col("n").mean().alias("avg"),
            pl.col("n").median().alias("p50"),
            pl.col("n").quantile(0.95).alias("p95"),
            pl.col("n").quantile(0.99).alias("p99"),
            pl.col("n").max().alias("max"),
        )
        .sort("avg", descending=True)
    )


def main(input_csv: str, out_dir: str = "density_stats") -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    df = pl.scan_csv(
        input_csv,
        has_header=False,
        new_columns=list(SCHEMA),
        schema_overrides=SCHEMA,
    )

    # One CSV scan; the four per-window queries run in parallel.
    results = pl.collect_all([per_location_stats(df, w) for w in WINDOWS])

    summary = out / "summary.txt"
    with summary.open("w") as f:
        for w, per_loc in zip(WINDOWS, results):
            per_loc.write_csv(out / f"density_{w}s.csv")
            f.write(f"{w}s\n")
            for row in per_loc.iter_rows(named=True):
                f.write(f"{row['location']},{row['avg']:.2f}\n")
            f.write(f"loc_avg,{per_loc['avg'].mean():.2f}\n\n")

    print(f"Wrote {len(WINDOWS)} per-window CSVs and summary.txt under {out}/")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(f"Usage: {sys.argv[0]} <input_csv> [<out_dir>]")
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "density_stats")
