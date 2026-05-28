# MaxGrowth — Pipeline Reference

> Engineering notes on our implementation of the MaxGrowth platoon-pattern miner (Bei et al., VLDB 2024) for the checkpoint ANPR dataset. Companion to `src/max_growth/`. Read alongside `doc/Methods.md` for the academic context.

---

## 1. Problem statement, in our terms

Each record in the source CSV is a single sighting:

```
(plate, location, timestamp)         e.g.   (323390, 28, 1420041602)
```

A **platoon pattern** is a pair `⟨O, P⟩` where `O` is a set of plates (the *members*), `P` is an ordered sequence of cameras (the *route*), every plate `p ∈ O` actually visited every camera in `P` in that order, at each camera the members arrived within ε seconds of each other, and at most `d` extra cameras were skipped between any two consecutive cameras in P (per the visiting plate's own trajectory).

The mining problem: find every **maximal** such pattern with `|O| ≥ m` and `|P| ≥ k`. Maximal = not contained in any larger pattern (either more members or longer route, with the smaller being a d-subsequence of the larger).

This is fundamentally different from FP-Growth's bucketed-set problem (see `doc/Methods.md`, §3 for the contrast). The trajectory structure makes the signal stronger: co-occurrence at one camera is weak; co-traversal of a 3-camera route is unambiguous group movement.

---

## 2. Parameters

| symbol | flag                  | default | what it bounds |
|--------|-----------------------|---------|----------------|
| `m`    | `--min-size`          | 2       | minimum plates per platoon |
| `k`    | `--min-length`        | 3       | minimum cameras per route |
| `d`    | `--gap`               | 1       | max cameras a plate can skip between adjacent route steps |
| `ε`    | `--eps`               | 300 s   | max time gap at one camera for two plates to count as "co-arriving" |

Auxiliary knobs (engineering, not algorithm):

| flag                    | default     | purpose |
|-------------------------|-------------|---------|
| `--min-visits`          | = `k`       | Apriori prefilter: drop plates seen at < N distinct cameras |
| `--min-observations`    | 10          | drop plates with < N total sightings — the dominant prefilter on ANPR scale, mirrors FP-Growth's `min_count` |
| `--max-plates`          | 500,000     | hard cap on surviving plates (top-N by sighting count) |
| `--max-cluster-size`    | 500         | drop ε-clusters with more plates than this — analogous to FP-Growth's `density_cap` |
| `--root-slices`         | 200         | Spark partitions for the root-growth fan-out |
| `--skip-maximal-filter` | off         | emit raw deduped patterns without dominance filtering (use if post-growth count is excessive) |

---

## 3. Pipeline — eight stages

```
                                       SPARK
   ┌───────────────────────────────────────────────────────────────┐
   │  1. CSV scan                                                  │
   │     Parse (plate, location, timestamp); cast types.           │
   ├───────────────────────────────────────────────────────────────┤
   │  2. Plate-side prefilter                                      │
   │     groupBy(plate).agg(countDistinct(camera), count(*))       │
   │       filter (≥ min_visits AND ≥ min_observations)            │
   │       orderBy(count desc).limit(max_plates)                   │
   │     left-semi-join back onto df.                              │
   ├───────────────────────────────────────────────────────────────┤
   │  3. Trajectory build                                          │
   │     For each surviving plate, collect_list(struct(t, camera)) │
   │     sorted by t.  collect() to driver.                        │
   ├───────────────────────────────────────────────────────────────┤
   │  4. Cluster materialization (dual offset)                     │
   │     bucketize by floor((t - offset) / ε)  for offset ∈ {0, ε/2} │
   │     groupBy(camera, bucket).agg(collect_set(plate))           │
   │     filter (size ∈ [m, max_cluster_size])                     │
   │     dropDuplicates(camera, members).  collect() to driver.    │
   └─────────────────────────────────┬─────────────────────────────┘
                                     │
                                  DRIVER
   ┌─────────────────────────────────┴─────────────────────────────┐
   │  5. Build position index + next-cameras lookup                │
   │     pos[plate][camera]            = index in trajectory       │
   │     next_cams_per_plate[plate]    = list of cameras in order  │
   ├───────────────────────────────────────────────────────────────┤
   │  6. Broadcast                                                 │
   │     sc.broadcast(by_camera, pos, next_cams_per_plate)         │
   └─────────────────────────────────┬─────────────────────────────┘
                                     │
                                   SPARK
   ┌─────────────────────────────────┴─────────────────────────────┐
   │  7. Parallel growth                                           │
   │     sc.parallelize(clusters, numSlices=root_slices)           │
   │       .mapPartitions(grow_partition)                          │
   │       .distinct()                                             │
   │       .collect()                                              │
   │     Each task: for each root in its slice, run grow_from_root │
   │     against the broadcast read-only state, yield patterns.    │
   └─────────────────────────────────┬─────────────────────────────┘
                                     │
                                  DRIVER
   ┌─────────────────────────────────┴─────────────────────────────┐
   │  8. Maximality filter + write                                 │
   │     maximal_only(patterns, d)  via inverted index             │
   │     sort by (-|O|, -|P|)                                      │
   │     CSV → <output>/patterns.csv                               │
   └───────────────────────────────────────────────────────────────┘
```

Distribution split:

| where | does what | scale-determining factor |
|---|---|---|
| Spark | set-of-rows aggregations (groupBy / filter / collect_set) + flatMap fan-out over cluster roots | bytes of raw data |
| Broadcast | trajectories, position index, clusters-by-camera | post-prefilter plate count |
| Driver | recursion setup, maximality filter, CSV write | post-pruning pattern count |

This split keeps the heavy work in Spark and the irregular, recursive work on either the driver (when small) or executors (via broadcast + parallel fan-out, when growth fans out widely).

---

## 4. The two key data structures

### 4.1 Trajectory + position index

```
trajectories[plate]            = [(t₁, c₁), (t₂, c₂), …]   sorted by t
pos[plate][camera]             = index of camera in plate's sorted visits
next_cams_per_plate[plate]     = [c₁, c₂, c₃, …]            same order as trajectories
```

`pos[p][c]` answers "where in plate p's life did it pass camera c?" — needed for the gap predicate `0 < pos(p, c') − pos(p, c) ≤ d+1`.

### 4.2 Cluster

```
Cluster(camera: int, members: frozenset[int])
```

A single ε-window snapshot at one camera, deduplicated by membership. Indexed twice: as a flat list (the *roots* we parallelize over), and via `by_camera[camera] → list[Cluster]` (the *candidates* during growth extension). Both are constructed once and broadcast.

---

## 5. The Growth recursion — what each call computes

```python
recurse(stack):                      # stack = current route as [Cluster, ...]
    last     = stack[-1]
    members  = last.members          # what we still have to continue with
    proposals = {}                   # camera → set of plates voting for it
    for p in members:
        cur_pos = pos[p][last.camera]
        for c2 in next_cams_per_plate[p]:
            if 0 < pos[p][c2] - cur_pos ≤ d+1:
                proposals.setdefault(c2, set()).add(p)

    extension_keeps_members = False
    for c2, supporters in proposals.items():
        if len(supporters) < m: continue
        for cl2 in by_camera[c2]:
            shared = cl2.members & frozenset(supporters)
            if len(shared) < m: continue
            if shared == members: extension_keeps_members = True
            recurse(stack + [Cluster(c2, shared)])

    if not extension_keeps_members and len(stack) >= k:
        emit pattern(route(stack), members)
```

Three things worth pausing on.

**Proposals come from members, not from clusters.** Each current member of the pattern individually nominates candidate next cameras based on *its own* trajectory. The union of nominations is the candidate set; we keep cameras supported by ≥ m members.

**Cluster membership is intersected, not just looked up.** Even if camera c′ has a cluster CL₂ with 50 plates, only the subset of those 50 that *also belong to the current pattern* counts. `shared = CL₂.members ∩ supporters`.

**Local maximality pruning** is the last block: if at least one extension keeps the full current membership, the longer route strictly dominates this pattern (same members, longer P, current P is a d-subsequence of the new one), so skip emission. This is the single optimization that turned 2.6 M raw patterns into something tractable. Without it, every prefix of every length-L path is emitted; with it, only the points where the recursion is forced to either shrink membership or stop are emitted. Empirically ~10×–100× reduction.

---

## 6. The maximality filter — inverted-index approach

After `.distinct()` collects the union of per-root patterns, we still need a global dominance check: pattern A dominates B iff `A.members ⊇ B.members` AND `B.route is a d-subsequence of A.route` AND `A != B`.

The naive O(N²) scan is unusable at million-pattern scale (we saw it run for 19+ hours on day1's 2.6 M raw patterns). The fast version uses an inverted index:

```
plate → list of pattern indices containing it
```

To check whether B is dominated, we don't compare against all patterns — only against patterns that contain **every member of B**. That candidate set is exactly the intersection of inverted lists for B's plates. We seed the search with B's *rarest* member (small inverted list), then check each candidate against the remaining members directly.

For day1's actual data: 2.6 M patterns × naive O(N²) ≈ 19 h on one core; post-pruning ~tens of thousands of patterns × inverted-index ≈ seconds.

---

## 7. What's distributed vs. driver, and why

| component | location | why |
|---|---|---|
| CSV parse + prefilter | Spark | row-parallel; trivially shards |
| Trajectory build | Spark | groupBy plate; one shuffle |
| Cluster materialization | Spark | groupBy (camera, bucket); one shuffle |
| Position index + broadcast prep | Driver | small dict construction after collect; needs full trajectory data co-located |
| Growth recursion | **Spark, broadcast-then-fan-out** | recursion has irregular access into trajectories and clusters; broadcasting read-only state and partitioning the *cluster roots* is the cleanest parallelism |
| Maximality filter | Driver | input is small after local pruning + distinct; an O(N²) algorithm hidden by an inverted index runs in seconds on tens of thousands of patterns |
| CSV write | Driver | tiny output |

The broadcast envelope is the limiting resource. For day1's numbers — 199,949 plates after prefilter, 3,477,173 trajectory rows, ~106 MiB trajectory broadcast, 293,187 clusters, a few hundred MiB cluster broadcast — total broadcast stays under ~1 GiB. That's comfortable on 75 GiB executors. If a future run needs broadcast > 1–2 GiB, the next step is a Pregel-style level-synced shuffle join (extend patterns one camera at a time, joining against clusters), but we haven't needed it yet.

---

## 8. Failure modes encountered + resolutions

| # | symptom | root cause | fix |
|---|---|---|---|
| 1 | `Total size of serialized results (1046 MiB) > maxResultSize (1024 MiB)` | Spark's default 1 GB cap on `collect()` is conservative for our 32 GB driver | `spark.driver.maxResultSize=0` (driver heap is the real bound) |
| 2 | OOM-kill (exit 137) after 2 h 42 min of driver-side enumeration | Paper-safe `--min-visits=k=3` is too permissive at ANPR scale: ~10⁶ plates qualify | added `--min-observations` + `--max-plates` (FP-Growth-style frequency prune) |
| 3 | `ModuleNotFoundError: No module named 'core'` on executors | Driver's `sys.path.insert` doesn't follow the closure to executors | `sc.addPyFile(core.py)` before parallelize |
| 4 | Driver stuck > 19 h on `maximal_only()` | O(N²) filter on 2.6 M raw patterns; most were sub-patterns of their own extensions | local maximality pruning during growth + inverted-index global filter |

The first three were *plumbing*, the fourth was *algorithmic*. Both categories needed attention before the implementation matched the paper's promised behavior at our data scale.

---

## 9. Tuning knobs by symptom

| if you see… | turn this knob | direction |
|---|---|---|
| Driver OOM after collect | `--min-observations` | up (50, 100) |
| Driver OOM after collect | `--max-plates` | down (200k, 100k) |
| Spark stage hangs in cluster materialization | `--max-cluster-size` | down (200, 100) |
| Spark stage hangs in growth | `--root-slices` | up (400, 800) — better load balance |
| Maximality filter slow | `--skip-maximal-filter` | use it; post-process offline |
| Too few patterns | `--min-observations` | down (5, 1) |
| Too few patterns | `--min-length` | down (2) |
| Too noisy output | `--min-length` | up (4, 5) |
| Too noisy output | `--gap` | down (0 = strict consecutive) |

---

## 10. What hasn't been built yet

**Distributed maximality filter.** Currently driver-side after local pruning shrinks the input. If pattern count after pruning still exceeds ~few hundred thousand, broadcast all patterns and partition the dominance check across executors. ~50 lines of code.

**Pregel-style growth.** If broadcast trajectories outgrow ~2 GiB (year-scale data, no prefilter), switch from broadcast-then-fan-out to level-synced shuffle joins. A bigger refactor.

**Streaming variant.** For live camera feeds (per the Flink 2025 paper). Out of scope for our batch reproduction.

---

## Appendix — Full-31 launch command on 5×{20-core, 100 GB} nodes

Day1 ran fine with the stock cluster.sbatch defaults plus `DRIVER_MEMORY=32g`. The full-31 attempt at the same config OOM-killed the master node at the broadcast moment — root cause: the master node co-hosts the driver *and* one of the five executors, so `32g (driver) + 75g (local executor) + ~3g (daemons) ≈ 110 GB` already overcommitted the 100 GB cgroup before PySpark Python heap is added on top. At day1 scale the Python footprint stayed small enough to hide it; at 31-day scale (112 M visits, 9.1 M ε-clusters) it didn't.

Three changes from defaults:

1. **Memory rebalance** — shrink the executor on every node so the master node has ~40 GB of headroom for the driver JVM and its Python interpreter heap.
2. **Halve PySpark worker concurrency per executor** (`EXECUTOR_CORES=9`) — each Python worker independently deserializes the broadcasts (`by_camera`, `pos`, `next_cams_per_plate`), so per-executor Python heap scales linearly with the number of concurrent Python workers. Total cores go from 90 → 45; Spark-side stages roughly double in wall time.
3. **Stronger algorithmic prune** — `--max-cluster-size 100` (was 500 default), `--min-observations 100`, `--max-plates 200000`. Targets ~ <2 M ε-clusters and a correspondingly smaller driver state.

```bash
set -o pipefail
mkdir -p /inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/data/logs

export INPUT=/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/data/input/31.csv
export OUTPUT=/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/data/output/max_growth_31
export JOB=/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/src/max_growth/max_growth_job.py

# Stronger Apriori-style cut: --min-observations 50→100, --max-plates default→200k,
# --max-cluster-size default 500→100 (the big one — last run produced 9.1M clusters).
# --root-slices 200→400 for better load balance on the now-fewer cores.
export JOB_ARGS="--min-size 2 --min-length 3 --gap 1 --eps 300 \
                 --min-visits 10 --min-observations 100 \
                 --max-plates 200000 --max-cluster-size 100 \
                 --root-slices 400"

# Memory rebalance — master node hosts driver + 1 executor + 2 daemons in 100 GB.
# Previous: 32 (drv) + 75 (exec) ≈ 107 → OOM. New: 40 + 50 ≈ 90, leaves Python room.
export SPARK_WORKER_MEMORY=55g
export EXECUTOR_MEMORY=50g
export DRIVER_MEMORY=40g

# Halve per-executor Python-worker concurrency: each Python worker holds its own
# decoded copy of the broadcasts (trajectories, position index, by_camera).
# 9 workers × ~few GB each fits, 18 didn't.
export SPARK_WORKER_CORES=9
export EXECUTOR_CORES=9

LOG=/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/data/logs/max_growth.31.log.2
bash /inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/src/slurm/cluster.sbatch 2>&1 | tee "$LOG"
```

### Checkpoints to verify the fix

- After cluster materialization: `[max-growth] N,NNN,NNN ε-clusters across NNN cameras` — target **< 2 M** clusters. If still > 5 M, drop `--max-cluster-size` to 50 and raise `--min-observations` to 200.
- After the first growth task completes (`Finished task 0.0 in stage 33`): no `Lost executor` or `Disassociated` since the broadcast lines = past the memory cliff. If executors are lost here, the per-worker Python heap is still too large — cut `EXECUTOR_CORES` to 6 and re-run.
- If growth finishes but `maximal_only()` on the driver runs hot, add `--skip-maximal-filter` to land raw deduped patterns on disk for offline filtering.

---

## Appendix B — Second OOM at the same phase; the structural fix (driver-only master node)

The Appendix A config above (5 × 50g executor + 40g driver + 9 cores) ran further than the previous attempt — got through prefilter (200 k plates), trajectory build (74 M visits / ~2.3 GiB raw), cluster materialization (**8.3 M ε-clusters across 847 cameras**), and into stage 33 broadcast distribution — but then died with the same signature: simultaneous `Disassociated` from all four remote workers + `Connection refused` on the master node's `:7077`. Master daemon + local executor + driver all reaped together by the master-node cgroup OOM.

The data-shrink lever wasn't enough on its own:

| | first 31-day attempt | second (Appendix A config) | reduction |
|---|---|---|---|
| visits collected     | 112,560,485 | 74,145,007  | 1.5× |
| ε-clusters           | 9,112,681   | **8,331,071** | 1.1× only |

`--max-cluster-size 100` barely moved cluster count — at 31-day scale most clusters are *small but numerous*, not large-and-rare. The cluster count stays north of 8 M no matter how we tune that knob.

### The structural problem

`cluster.sbatch:47` launches a Spark worker on **every** Slurm task, including the master task. So slurmd-0 hosts a master daemon, a worker daemon, a 50 GB local executor, *and* the driver. On a 100 GB node:

```
1 GB master + 1 GB worker daemon + 50 GB executor JVM + 40 GB driver JVM
= 92 GB JVM committed, 8 GB free
```

That 8 GB has to absorb:
- driver Python interpreter (`trajectories` + `pos` + `next_cams` + `clusters` dicts on 74 M visits / 8.3 M clusters ⇒ ~15–25 GB Python heap),
- 9 PySpark worker processes on the *local* executor, each independently deserializing ~1 GB of pickled broadcast into Python heap (~5 GB × 9 ≈ 45 GB),
- and the OS.

Demand is 60–80 GB on top of 92 GB JVM. The previous knob-tuning (cutting executor cores from 18 to 9, etc.) helped the four *worker* nodes but did nothing for the master node, where the binding constraint is the *driver's* Python heap — independent of executor cores.

### The fix: drain the master node of executor work

Edit `src/slurm/cluster.sbatch` so the master node only hosts the master daemon + driver, no worker / executor:

```bash
# Before:
srun --ntasks="$SLURM_NTASKS" "$REPO/start-worker.sh"

# After:
NON_MASTER_NODES=$(printf '%s,' "${NODES[@]:1}" | sed 's/,$//')
srun --ntasks=$((SLURM_NTASKS - 1)) --nodelist="$NON_MASTER_NODES" "$REPO/start-worker.sh"
```

New per-node budgets:

| node     | role               | committed                                                                   | / 100 GB |
|----------|--------------------|------------------------------------------------------------------------------|----------|
| slurmd-0 | driver only        | 1 GB master daemon + 64 GB driver JVM + ~25 GB driver Python                | ~90 GB ✓ |
| slurmd-1..4 | executor only   | 1 GB worker daemon + 50 GB executor JVM + 6 PySpark workers × ~5 GB Python  | ~81 GB ✓ |

We trade 1 executor (20 % of executor capacity) for a workable memory budget.

### Resulting launch command

```bash
set -o pipefail
mkdir -p /inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/data/logs

export INPUT=/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/data/input/31.csv
export OUTPUT=/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/data/output/max_growth_31
export JOB=/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/src/max_growth/max_growth_job.py

# Aggressive prune — 8.3M clusters still OOM'd the executors' Python heaps under
# the previous config. Halve --max-cluster-size and tighten plate filters further.
export JOB_ARGS="--min-size 2 --min-length 3 --gap 1 --eps 300 \
                 --min-visits 15 --min-observations 200 \
                 --max-plates 100000 --max-cluster-size 50 \
                 --root-slices 400"

# Master node no longer hosts an executor — give the driver real room.
export DRIVER_MEMORY=64g

# Worker nodes: 4 executors only. Plenty of node memory; the binding constraint
# is per-worker Python heap. 6 cores × 4 nodes = 24 task slots.
export SPARK_WORKER_CORES=6
export SPARK_WORKER_MEMORY=55g
export EXECUTOR_CORES=6
export EXECUTOR_MEMORY=50g

LOG=/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/data/logs/max_growth.31.log.3
bash /inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop/src/slurm/cluster.sbatch 2>&1 | tee "$LOG"
```

### Checkpoints

- `[max-growth] N,NNN,NNN ε-clusters across NNN cameras` — target **< 3 M** (was 8.3 M). If still > 5 M, drop `--max-cluster-size` to 30.
- Stage 33 task-size warning (`Stage 33 contains a task of very large size`): the previous run logged 1.8 MiB/task because the cluster list was being serialized into each closure (8.3 M ÷ 400 slices × cluster size). After prefilter cuts this should fall below 1 MiB.
- First growth task completion (`Finished task 0.0 in stage 33`) without `Disassociated` / `Lost executor` = past the memory cliff. If a node dies before any task completes, the executor Python heap is still too large — drop `EXECUTOR_CORES=4` and re-run.
- The master-node-OOM signature to watch for: simultaneous `Disassociated` from all four remote workers + `Connection refused` on `10.x.x.x:7077`. With this fix the master node carries far less load, so a recurrence would point to a *different* node OOMing instead, which the worker logs at `$SPARK_LOG_DIR/.../worker-*.out` will identify by IP.
