"""Quick descriptive analysis of the convoy_fpgrowth output.

Inputs:   the single part-*.csv under the convoy job's output directory.
Outputs:  a `summary.txt` (top-5 per k, super-connector plates, distribution
          numbers) and three PNG charts in the same directory.

Usage:
    python analyze_convoys.py <convoy_output_dir>
"""
import sys
from collections import Counter
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


def load(out_dir: Path) -> pd.DataFrame:
    parts = list(out_dir.glob("part-*.csv"))
    if not parts:
        sys.exit(f"no part-*.csv under {out_dir}")
    df = pd.concat(pd.read_csv(p) for p in parts).reset_index(drop=True)
    df["plates_list"] = df["plates"].str.split(",")
    return df


def top_n_per_k(df: pd.DataFrame, n: int = 5) -> str:
    out = []
    for k in sorted(df["k"].unique()):
        top = df[df["k"] == k].nlargest(n, "count")[["plates", "count"]]
        out.append(f"--- k={k}  (top {n} of {(df['k']==k).sum()}) ---")
        for _, r in top.iterrows():
            out.append(f"  {r['count']:>5}  {r['plates']}")
    return "\n".join(out)


def super_connectors(df: pd.DataFrame, n: int = 15) -> pd.Series:
    """Plates ranked by the number of distinct convoys they appear in."""
    c = Counter()
    for plates in df["plates_list"]:
        c.update(plates)
    return pd.Series(c).sort_values(ascending=False).head(n)


def plot_size_distribution(df: pd.DataFrame, path: Path) -> None:
    counts = df["k"].value_counts().sort_index()
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(counts.index, counts.values, color="steelblue")
    for k, v in counts.items():
        ax.text(k, v, f"{v:,}", ha="center", va="bottom", fontsize=9)
    ax.set(xlabel="convoy size k", ylabel="# convoys",
           title="Convoys per size (the inverted-U is fleet signal)")
    ax.set_xticks(counts.index)
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def plot_count_distribution(df: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(7, 4))
    for k in sorted(df["k"].unique()):
        ax.hist(df.loc[df["k"] == k, "count"], bins=50, alpha=0.5,
                label=f"k={k}", log=True)
    ax.set(xlabel="co-occurrence count over 31 days",
           ylabel="# convoys (log)",
           title="Co-occurrence frequency distribution by k")
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def plot_super_connectors(series: pd.Series, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(7, 5))
    series[::-1].plot.barh(ax=ax, color="darkorange")
    ax.set(xlabel="# convoys this plate appears in",
           title="Super-connector plates (top 15)")
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def main(out_dir: str) -> None:
    out = Path(out_dir)
    df = load(out)

    summary = []
    summary.append(f"Total convoys: {len(df):,}")
    summary.append(f"Convoy-count range:  [{df['count'].min()}, {df['count'].max()}]")
    summary.append(f"Distinct plates in any convoy: {len({p for ps in df.plates_list for p in ps}):,}")
    summary.append("")
    summary.append("== Top 5 by count, per convoy size ==")
    summary.append(top_n_per_k(df, 5))
    summary.append("")
    summary.append("== Super-connector plates (appear in the most distinct convoys) ==")
    sc = super_connectors(df, 15)
    for plate, n in sc.items():
        summary.append(f"  {n:>4}  plate {plate}")

    summary_path = out / "summary.txt"
    summary_path.write_text("\n".join(summary) + "\n")

    plot_size_distribution(df, out / "size_distribution.png")
    plot_count_distribution(df, out / "count_distribution.png")
    plot_super_connectors(sc, out / "super_connectors.png")

    print("\n".join(summary))
    print(f"\nWrote charts and summary.txt to {out}/")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(f"Usage: {sys.argv[0]} <convoy_output_dir>")
    main(sys.argv[1])
