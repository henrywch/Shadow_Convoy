"""Quick descriptive analysis of the MaxGrowth (platoon-pattern) output.

The convoy analyzer's sibling for MaxGrowth — closes the biggest downstream gap
flagged in doc/Downstream_Tasks.md §0.3: 223 k directed-route patterns are
completely un-triaged today (no ranking, no distributions, no corridors).

Inputs:   `patterns.csv` under the MaxGrowth output dir
          (schema: route, n_cameras, members, n_plates).
Outputs:  written to data/downstream/analyze_patterns/ (override with --out-dir):
          a `summary.txt` (top patterns by significance, super-connector plates,
          busiest corridors, distribution numbers) and four PNG charts.

Significance score = n_plates × n_cameras. A longer *shared route* is stronger
evidence of group movement than a bigger crowd at fewer cameras, so a
6-plate × 5-camera co-traversal (30) outranks a 20-plate × 3-camera one (60 —
still high, but the score rewards route length, not just size). Co-traversal of
a 3-camera route is unambiguous; co-occurrence at one camera is weak.

Usage:
    python analyze_patterns.py <max_growth_output_dir> [--out-dir DIR]
"""
import argparse
import sys
from collections import Counter, defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


def load(in_dir: Path) -> pd.DataFrame:
    csv = in_dir / "patterns.csv"
    if not csv.exists():
        sys.exit(f"no patterns.csv under {in_dir}")
    df = pd.read_csv(csv)
    df["members_list"] = df["members"].str.split(",")
    df["significance"] = df["n_plates"] * df["n_cameras"]
    return df


def top_by_significance(df: pd.DataFrame, n: int = 15) -> str:
    """Top patterns overall, deduplicated on (route, members) so exact-duplicate
    rows don't fill the list. Ties broken by route length then group size."""
    uniq = df.drop_duplicates(subset=["route", "members"])
    top = uniq.sort_values(["significance", "n_cameras", "n_plates"],
                           ascending=False).head(n)
    out = [f"(deduplicated {len(df) - len(uniq):,} exact-duplicate rows)"]
    for _, r in top.iterrows():
        mem = ",".join(r["members_list"][:6]) + ("…" if r["n_plates"] > 6 else "")
        out.append(f"  sig={r['significance']:>4}  {r['n_cameras']}cam×{r['n_plates']:>2}pl  "
                   f"{r['route']:<22}  [{mem}]")
    return "\n".join(out)


def top_per_length(df: pd.DataFrame, n: int = 5) -> str:
    out = []
    for L in sorted(df["n_cameras"].unique()):
        sub = df[df["n_cameras"] == L].nlargest(n, "n_plates")[["route", "n_plates"]]
        out.append(f"--- route length {L}  (top {n} of {(df['n_cameras']==L).sum():,}) ---")
        for _, r in sub.iterrows():
            out.append(f"  {r['n_plates']:>3} plates  {r['route']}")
    return "\n".join(out)


def super_connectors(df: pd.DataFrame, n: int = 15) -> pd.Series:
    """Plates ranked by the number of distinct patterns they appear in."""
    c = Counter()
    for members in df["members_list"]:
        c.update(members)
    return pd.Series(c).sort_values(ascending=False).head(n)


def corridors(df: pd.DataFrame, n: int = 15) -> pd.DataFrame:
    """Busiest directed routes — the 'convoy corridors'. Per route: how many
    distinct member-sets traverse it and how many distinct plates total. This is
    the unique payoff of having a *directed* detector (doc/Downstream_Tasks.md §3.2)."""
    plates_by_route: dict[str, set] = defaultdict(set)
    sets_by_route: Counter = Counter()
    for route, members in zip(df["route"], df["members_list"]):
        plates_by_route[route].update(members)
        sets_by_route[route] += 1
    rows = [{"route": r, "n_patterns": sets_by_route[r],
             "n_distinct_plates": len(plates_by_route[r])}
            for r in plates_by_route]
    return (pd.DataFrame(rows)
            .sort_values(["n_distinct_plates", "n_patterns"], ascending=False)
            .head(n).reset_index(drop=True))


# ── charts (mirror analyze_convoys.py style) ─────────────────────────────────

def plot_route_length(df: pd.DataFrame, path: Path) -> None:
    counts = df["n_cameras"].value_counts().sort_index()
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(counts.index, counts.values, color="steelblue", log=True)
    for L, v in counts.items():
        ax.text(L, v, f"{v:,}", ha="center", va="bottom", fontsize=8)
    ax.set(xlabel="route length (# cameras)", ylabel="# patterns (log)",
           title="Patterns per route length (longer routes are rarer & stronger)")
    ax.set_xticks(counts.index)
    fig.tight_layout(); fig.savefig(path, dpi=130); plt.close(fig)


def plot_members(df: pd.DataFrame, path: Path) -> None:
    counts = df["n_plates"].value_counts().sort_index()
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(counts.index, counts.values, color="seagreen", log=True)
    ax.set(xlabel="members per pattern (# plates)", ylabel="# patterns (log)",
           title="Group-size distribution")
    ax.set_xticks(counts.index)
    fig.tight_layout(); fig.savefig(path, dpi=130); plt.close(fig)


def plot_corridors(cor: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    cor[::-1].plot.barh(x="route", y="n_distinct_plates", ax=ax,
                        color="mediumpurple", legend=False)
    ax.set(xlabel="# distinct plates traversing this route",
           ylabel="directed route",
           title="Busiest convoy corridors (top 15)")
    fig.tight_layout(); fig.savefig(path, dpi=130); plt.close(fig)


def plot_super_connectors(series: pd.Series, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(7, 5))
    series[::-1].plot.barh(ax=ax, color="darkorange")
    ax.set(xlabel="# patterns this plate appears in",
           title="Super-connector plates (top 15)")
    fig.tight_layout(); fig.savefig(path, dpi=130); plt.close(fig)


def main(in_dir: str, out_dir: str) -> None:
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    df = load(Path(in_dir))
    all_plates = {p for ps in df["members_list"] for p in ps}

    summary = []
    summary.append(f"Total patterns: {len(df):,}")
    summary.append(f"Distinct directed routes: {df['route'].nunique():,}")
    summary.append(f"Distinct plates in any pattern: {len(all_plates):,}")
    summary.append(f"Route length (cameras):  [{df['n_cameras'].min()}, {df['n_cameras'].max()}]")
    summary.append(f"Members per pattern:     [{df['n_plates'].min()}, {df['n_plates'].max()}]")
    summary.append(f"Significance (n_plates×n_cameras) max: {df['significance'].max()}")
    summary.append("")
    summary.append("== Top patterns by significance (route length × group size) ==")
    summary.append(top_by_significance(df, 15))
    summary.append("")
    summary.append("== Largest groups, per route length ==")
    summary.append(top_per_length(df, 5))
    summary.append("")
    summary.append("== Busiest convoy corridors (directed routes) ==")
    cor = corridors(df, 15)
    for _, r in cor.iterrows():
        summary.append(f"  {r['n_distinct_plates']:>4} plates  {r['n_patterns']:>5} patterns  "
                       f"{r['route']}")
    summary.append("")
    summary.append("== Super-connector plates (appear in the most distinct patterns) ==")
    sc = super_connectors(df, 15)
    for plate, n in sc.items():
        summary.append(f"  {n:>5}  plate {plate}")

    (out / "summary.txt").write_text("\n".join(summary) + "\n")

    plot_route_length(df, out / "route_length_distribution.png")
    plot_members(df, out / "members_distribution.png")
    plot_corridors(cor, out / "top_corridors.png")
    plot_super_connectors(sc, out / "super_connectors.png")

    print("\n".join(summary))
    print(f"\nWrote summary.txt and 4 charts to {out}/")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("max_growth_output_dir", help="dir containing patterns.csv")
    p.add_argument("--out-dir", default="data/downstream/analyze_patterns",
                   help="where to write summary.txt + charts "
                        "(default: data/downstream/analyze_patterns)")
    a = p.parse_args()
    main(a.max_growth_output_dir, a.out_dir)
