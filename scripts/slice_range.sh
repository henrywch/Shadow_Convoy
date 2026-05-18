#!/usr/bin/env bash
# Extract a contiguous range of days from the 31-day traffic CSV.
# Streaming awk filter — single sequential pass over the input.
#
# Usage:
#   scripts/slice_range.sh <input.csv> <output.csv> <first_day> [last_day]
#
# Day 1 = 2015-01-01 UTC = unix 1420041600.  last_day defaults to first_day
# (i.e. a single-day slice).  Both ends inclusive.
set -euo pipefail

IN="${1:?input csv path}"
OUT="${2:?output csv path}"
FIRST="${3:?first day (1-based)}"
LAST="${4:-$FIRST}"

DAY0=1420041600                                 # 2015-01-01 00:00:00 UTC
START=$(( DAY0 + (FIRST - 1) * 86400 ))
END=$((   DAY0 +  LAST       * 86400 ))         # exclusive upper bound

echo "[slice] days $FIRST..$LAST  [$START, $END)  $IN -> $OUT"
awk -F, -v s="$START" -v e="$END" '$3 >= s && $3 < e' "$IN" > "$OUT"

echo "[slice] $(wc -l < "$OUT") rows, $(du -h "$OUT" | cut -f1)"
