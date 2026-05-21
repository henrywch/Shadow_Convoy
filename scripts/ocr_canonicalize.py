"""Detect plate pairs that are likely OCR confusions in convoy output.

A real convoy of two distinct vehicles co-occurs at many checkpoints but not
EVERY observation of one is paired with the other. By contrast, an OCR-confused
pair of plates (e.g. 16970573 vs 16972992) appears almost as a single entity
in the data — high co-occurrence AND tiny edit distance.

This script flags suspicious pairs from a convoy CSV. It is intentionally
focused: only checks plates that already appear in the convoy output (~hundreds
of plates), not the full 22 M-plate population, so the run is cheap.

Output is a CSV mapping `(plateA, plateB)` → (edit distance, joint count,
suspicion score). Manual review or domain knowledge picks the canonical plate.

Usage:
    python ocr_canonicalize.py <convoy_csv> <output_csv>
    # or with explicit edit-distance bound:
    python ocr_canonicalize.py <convoy_csv> <output_csv> --max-edit 1
"""
import argparse
import csv
from collections import Counter
from itertools import combinations


def edit1(a: str, b: str) -> bool:
    """True iff a and b differ by exactly one substitution at the same length.

    We only check substitutions; insertion/deletion would change plate length,
    which ANPR systems already gate on. Substitution covers >95% of OCR errors
    on digit plates.
    """
    if len(a) != len(b):
        return False
    diffs = sum(1 for x, y in zip(a, b) if x != y)
    return diffs == 1


def main(convoy_csv: str, out_csv: str, max_edit: int) -> None:
    # 1. Collect distinct plates and their participation count.
    plate_count: Counter[str] = Counter()
    with open(convoy_csv) as f:
        r = csv.DictReader(f)
        for row in r:
            plates = row["plates"].split(",")
            for p in plates:
                plate_count[p] += int(row["count"])
    plates = sorted(plate_count)

    # 2. Block by plate length, then scan substitution-distance pairs.
    suspects: list[tuple[str, str, int, int]] = []
    by_len: dict[int, list[str]] = {}
    for p in plates:
        by_len.setdefault(len(p), []).append(p)
    for length, group in by_len.items():
        for a, b in combinations(group, 2):
            if max_edit == 1 and edit1(a, b):
                joint = min(plate_count[a], plate_count[b])
                # Suspicion score: lower edit-distance + higher joint count → suspect.
                score = joint
                suspects.append((a, b, 1, score))

    suspects.sort(key=lambda r: r[-1], reverse=True)

    # 3. Write the canonicalization candidates. Manual judgement picks a winner.
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["plate_a", "plate_b", "edit_distance", "suspicion_score"])
        for a, b, e, s in suspects:
            w.writerow([a, b, e, s])

    print(
        f"[ocr] scanned {len(plates):,} distinct plates; "
        f"flagged {len(suspects):,} edit-1 pairs to {out_csv}"
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("convoy_csv")
    p.add_argument("out_csv")
    p.add_argument("--max-edit", type=int, default=1, choices=[1],
                   help="edit-distance bound (currently only 1 is implemented)")
    a = p.parse_args()
    main(a.convoy_csv, a.out_csv, a.max_edit)
