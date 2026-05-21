# `src/max_growth/` — MaxGrowth platoon-pattern miner

Implementation of the **MaxGrowth** relaxed-co-movement algorithm from *Bei et al., "Mining Platoon Patterns from Traffic Videos", VLDB 2024* (arXiv 2412.20177), targeting the same `(plate, location, timestamp)` CSV that `src/job/convoy_fpgrowth.py` consumes.

## Why this and not FP-Growth

FP-Growth treats each `(location, time-bucket)` cell as an independent transaction. A pair "co-occurred 50 times" can mean 50 buckets at one camera — informative but weak signal.

MaxGrowth instead enumerates **routes**: a pattern `⟨O, P⟩` is a group `O` of vehicles that share a *sequence* of cameras `P`, with at most `d` missing cameras tolerated between adjacent ones. A 3-camera co-traversal by 4 plates is unambiguous physical evidence of group movement; a single-bucket co-occurrence is not.

The cost (vs FP-Growth) is paid in algorithm complexity, not data scale: the search is over trajectories per plate, not transactions per bucket.

## Parameters

| symbol | flag | what it means |
|---|---|---|
| **m** | `--min-size`   | min plates in a platoon (≥ 2)          |
| **k** | `--min-length` | min route length in cameras (≥ 2)      |
| **d** | `--gap`        | max missing cameras between two adjacent route nodes |
| **ε** | `--eps`        | max time gap at the same camera (seconds) for two plates to be "co-arriving" |

Defaults: `m=2, k=3, d=1, ε=300`. The `k=3` default is the principled difference from FP-Growth — only patterns spanning at least three cameras are reported, which is precisely the kind of evidence FP-Growth couldn't require.

## Pipeline shape

```
read CSV → schema (plate, location, timestamp)
   ↓
prefilter plates with ≥ k visits        (Spark, embarrassingly parallel)
   ↓
build per-plate trajectories             (Spark groupBy → collect_list)
   ↓
compute clusters per camera              (Spark groupBy + sliding window)
   ↓
keep clusters of size ≥ m                (Spark filter)
   ↓
COLLECT to driver
   ↓
Growth() recursion in Python             (single-driver)
   ↓
write maximal patterns to CSV            (driver write)
```

The collected cluster set is small in practice — after Apriori-style pre-filtering, only frequent plates and dense `(camera, ε-window)` clusters survive — so the single-driver enumeration is the right choice. The expensive parts (CSV scan, trajectory build, cluster mining) stay distributed.

## Files

- `core.py` — pure-Python algorithm: `find_clusters`, `growth`, maximal-filter.
- `max_growth_job.py` — PySpark wrapper, callable from `cluster.sbatch` exactly like `src/job/convoy_fpgrowth.py`. Re-uses `submit.sh`, `env.sh`, the whole bring-up sequence.

## Submitting

Identical to the FP-Growth job; just point `JOB` at this module:

```bash
export INPUT=/inspire/.../data/input/31.csv
export OUTPUT=/inspire/.../data/output/max_growth_31
export JOB=/inspire/.../src/max_growth/max_growth_job.py
export JOB_ARGS="--min-size 2 --min-length 3 --gap 1 --eps 300"
bash /inspire/.../src/slurm/cluster.sbatch
```

No changes to `cluster.sbatch`, `env.sh`, `start-master.sh`, etc.
