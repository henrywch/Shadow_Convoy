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
