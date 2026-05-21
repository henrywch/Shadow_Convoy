# Methods — Co-Movement Pattern Mining for Checkpoint Vehicle Data

> Working notes on the academic lineage of "convoy / platoon / co-occurrence" mining, our project's place in it, and the current best methods for our exact problem setting. Compiled from a literature scan, May 2026.

---

## 1. The problem, named precisely

Our data is **checkpoint co-occurrence**: discrete `(plate, location, timestamp)` events generated when a vehicle triggers an ANPR camera at a fixed checkpoint. It is **not** continuous GPS trajectory data — vehicles only exist where and when they're seen, and there are gaps (occlusion, misreads, sensor downtime).

This matters because the literature treats continuous trajectories and checkpoint streams as different problems with different algorithms.


| Our data type               | Discrete sightings, fixed sparse spatial domain (~10³ checkpoints), 31-day stream |
| --------------------------- | --------------------------------------------------------------------------------- |
| **The right academic name** | "Frequent Co-occurrence Pattern" (FCP) or "Platoon pattern"                       |
| **The wrong academic name** | "Convoy" (in the Jeung 2008 sense — that needs continuous trajectories)           |


This was a quiet confusion in our project's previous framing. Calling the output "convoys" is fine for plain English; for the methodology section of any report, "co-occurrence patterns" or "platoons" is what the field uses.

---

## 2. Eighteen years of co-movement pattern mining — a timeline

```
2006 │ Flock         │ Gudmundsson & van Kreveld
     │               │ ≥ m objects in a disk of radius r for k consecutive frames.
     │               │ Computational geometry primitive. GPS-only.
     │
2008 │ Convoy        │ Jeung, Yiu, Zhou, Jensen, Shen — ICDE
     │               │ ≥ m objects density-connected (DBSCAN) for k consecutive
     │               │ timestamps. Loosens flock's disk constraint. GPS-only.
     │
2010 │ Swarm         │ Li, Ding, Han, Kays — VLDB
     │               │ ≥ m objects share the same cluster at k timestamps,
     │               │ NOT necessarily consecutive. Loosens convoy's time axis.
     │
2012 │ Travelling    │ Tang, Zheng, Yuan, Xie, Yang, Sun — KDD
     │ Companion     │ Streaming variant of convoy for live trajectory feeds.
     │
2013 │ Gathering     │ Zheng et al.
     │               │ Larger crowds at hotspots, looser cohesion.
     │
2015 │ Platoon       │ Li et al. — efficient mining of platoon patterns
     │               │ Common-route co-movement; bucket-based; tolerates
     │               │ non-consecutive snapshots. Closer to our setting.
     │
2015 │ FCP / streams │ Yu — EDBT
     │               │ "Mining Frequent Co-occurrence Patterns Across
     │               │ Multiple Data Streams". Vehicle plate readers at
     │               │ multiple checkpoints, sliding window. THIS IS
     │               │ OUR PROBLEM, named.
     │
2016 │ Distributed   │ Orakzai, Devogele, Calabretto — IEEE BigData
     │ Convoy        │ MapReduce/Spark version of classical convoy mining.
     │
2019 │ k/2-hop       │ Orakzai et al. — PVLDB
     │               │ Pruning insight: a convoy of length k must appear in
     │               │ snapshots k/2 apart, so we can skip k/2 timestamps.
     │               │ Orders-of-magnitude faster than prior convoy miners.
     │
2024 │ MaxGrowth     │ Bei et al. — VLDB
     │               │ Platoon patterns from real traffic-camera data with
     │               │ occlusion and OCR error. Removes consecutive-checkpoint
     │               │ requirement; no filter-and-refine. Two orders of
     │               │ magnitude faster than baselines.
     │
2025 │ Flink stream  │ Liu et al. — KAIS
     │               │ Two-stage clustering + mining on Apache Flink for
     │               │ live trajectory streams. Real-time, scalable.
```

What this tells us about our FP-Growth baseline:

- It's a 2015-era approach (treat each `(location, bucket)` as a transaction,
mine frequent itemsets) — perfectly valid, well-understood, but **not current state of the art**.
- The 2024 SOTA (MaxGrowth) is the direct successor: same data shape, but
with relaxed-consecutiveness and a different enumeration strategy that is much faster *and* tolerates missing detections.

---

## 3. Why FP-Growth on bucketed transactions is a reasonable but dated baseline

What we did:

```
(plate, location, timestamp)
   ↓ bucketize at offset {0, window/2}
(location, bucket) → collect_set(plate) = a "transaction"
   ↓ Parallel FP-Growth, minSupport = min_count / |transactions|
frequent itemsets of plates  →  convoys
```

Strengths:

- Apriori principle applies cleanly — `min_count` prunes the search at
the F-list step, so the cost scales with the number of *frequent* plates, not the number of total plates.
- Maps naturally to Spark MLlib's distributed FPGrowth (PFP).
- Dual-grid offset (0, window/2) recovers convoys split by bucket
boundaries.
- Simple to reason about; output is interpretable.

Weaknesses, which are what the newer literature addresses:

- **No tolerance for missing detections.** A six-vehicle convoy that
misses one checkpoint in one bucket becomes two separate 5-cars and a singleton. MaxGrowth (2024) explicitly relaxes this.
- **FP-tree depth scales with transaction width.** We hit this — needed
`density_cap=2000` and `-Xss16m` to keep the JVM stable.
- **No use of trajectory structure.** We treat each bucket as an
independent set; the temporal/spatial sequence is invisible to the miner. k/2-hop and plane-sweep methods exploit this structure.
- **No notion of OCR error.** Plates one digit apart could be the same
vehicle; FP-Growth treats them as wholly distinct.
- **Batch, not streaming.** Re-running on a moving 31-day window is
full-recompute, not incremental.

---

## 4. The current best methods for our exact problem

Ordered by direct applicability to our `(plate, location, timestamp)` data.

### 4.1 MaxGrowth — Bei et al., VLDB 2024 — most relevant SOTA

Paper: *Mining Platoon Patterns from Traffic Videos* (PVLDB v18).

What it does:

- Defines a **relaxed co-movement pattern** that drops the
"must be in consecutive checkpoints" constraint and tolerates a bounded number of missing detections.
- Enumerates valid patterns directly with a growth-based search
(analogous in spirit to FP-Growth, but the enumeration is over trajectory-prefix-extensions rather than itemset extensions), **with no candidate verification step**.
- Sliding-window enumeration with a hashing-based dominance eliminator
removes redundant maximal-pattern checks.

Reported gains: **up to 100× faster** than prior best.

Why it matters to us:

- Solves the OCR-error caveat we flagged in §6 of the In Process page
(we noted that pairs of plates one digit apart might be the same vehicle).
- Solves the bucket-density problem at the algorithm level (no
`density_cap` needed; no FP-tree depth explosion).
- Directly designed for our data type — traffic camera streams.

Implementation cost: medium. The paper's pseudocode is detailed; no public PyPI package, but the algorithm reduces to ~600 lines of PySpark + a hash-index data structure.

### 4.2 k/2-hop — Orakzai et al., PVLDB 2019 — best classical convoy

If we ever want to use the strict-consecutive convoy definition (e.g. for a stricter "operating-as-a-unit" criterion), this is the speed leader.

Insight: a convoy of length k consecutive timestamps must have two of its members visible in *two* snapshots that are exactly `k/2` apart. So scan in steps of `k/2`, not 1.

Practical when convoy length k is large and the data is dense in time. Our data is moderate-frequency checkpoint reads, so this is less critical, but the pruning concept transfers.

### 4.3 Distributed Co-Movement on Flink — Liu et al., KAIS 2025

Two-stage clustering + mining built on Apache Flink. Targets live camera feeds rather than historical batches.

Relevant if the project ever needs to move to a **streaming** mode (e.g. live alerts when a known convoy re-appears).

### 4.4 Graph + community detection — the deployed-industry approach

Two-pass system:

1. **Mine pair-level co-occurrence counts.** Either with FP-Growth as we already do, or with a custom pair counter that streams through buckets emitting `(plateA, plateB, +1)` for every co-bucket pair.
2. **Build a weighted plate graph**: vertices = plates, edges = pair counts above threshold.
3. **Run a community detection algorithm** (Louvain, Leiden, or Label Propagation) to partition plates into "fleets".

Spark GraphFrames provides `connectedComponents()` and `labelPropagation(maxIter)` out of the box. Louvain on Spark needs a third-party package (`graphframes-louvain`) or a Pregel-based implementation.

This is what Chinese ANPR-at-scale systems do in production — see the US patent "Vehicle data fusion based on spatiotemporal information and subgraph similarity" for the deployed architecture.

Why it's a strong fit for our project:

- It is **complementary** to FP-Growth / MaxGrowth, not a replacement.
Whichever miner we use, we get pair counts at the end; the graph step turns "5,447 itemsets" into "N labeled fleets" without any ad-hoc inspection of top-K tables.
- Cheap to add: one extra Spark stage on the existing output.
- Produces a *systematic* fleet labelling, replacing the manual
Fleet A / B / D inspection on our current In Process page.

### 4.5 OCR-error-aware pre-processing

Before any miner runs, cluster plates by Levenshtein distance ≤ 1 (or by an embedding similarity if numeric/alphabetic structure helps). Treat each cluster as a single canonical plate.

Cheapest fix to the data-quality caveat we already surfaced. Order of implementation effort: low. The Bei 2024 MaxGrowth paper handles this inside the algorithm; if we don't adopt MaxGrowth, we should still do this as a pre-processing step.

---

## 5. What to do next, recommended ordering

Based on cost vs. payoff for what remains of the project:

1. **(Day-scale) OCR-noise canonicalization** — Levenshtein-clustering of plates as a pre-processing step. Improves *every* downstream stat immediately; no algorithm changes needed.
2. **(Day-scale) Graph community detection on existing convoy output** — build the plate co-occurrence graph from `convoy_fpg_31/part-*.csv` and run Label Propagation in GraphFrames. Produces a labelled fleet table that replaces the manual Fleet A / B / D in the report.
3. **(Week-scale) Implement MaxGrowth in PySpark** — the SOTA forward step. Drops FP-Growth's depth/density caveats, gives us a 2024 reference for the academic write-up.
4. **(Future) Streaming variant on Flink** — only if the project scope expands to live camera feeds.

This ordering gives us **two guaranteed improvements that ship with modest effort (#1 and #2)** plus **one ambitious SOTA target (#3)** that produces a stronger academic deliverable if time allows.

---

## Sources

Primary references:

- Bei et al., **Mining Platoon Patterns from Traffic Videos**, PVLDB 18 (2024).
[https://www.vldb.org/pvldb/vol18/p1839-bei.pdf](https://www.vldb.org/pvldb/vol18/p1839-bei.pdf) · [https://arxiv.org/abs/2412.20177](https://arxiv.org/abs/2412.20177)
- Orakzai, Calabretto, Devogele, Sablayrolles, **k/2-hop: fast mining of
convoy patterns with effective pruning**, PVLDB 12 (2019). [https://dl.acm.org/doi/abs/10.14778/3329772.3329773](https://dl.acm.org/doi/abs/10.14778/3329772.3329773)
- Liu et al., **An efficient distributed co-movement pattern detection
framework for streaming trajectory**, KAIS (2025). [https://link.springer.com/article/10.1007/s10115-025-02369-7](https://link.springer.com/article/10.1007/s10115-025-02369-7)
- Orakzai, Devogele, Calabretto, **Towards Distributed Convoy Pattern
Mining**, IEEE BigData (2016). [https://arxiv.org/pdf/1512.08150](https://arxiv.org/pdf/1512.08150)
- Yu, **Mining Frequent Co-occurrence Patterns Across Multiple Data
Streams**, EDBT (2015). [https://openproceedings.org/2015/conf/edbt/paper-82.pdf](https://openproceedings.org/2015/conf/edbt/paper-82.pdf)
- Jeung, Yiu, Zhou, Jensen, Shen, **Discovery of Convoys in Trajectory
Databases**, ICDE / VLDB Journal (2008–2010). [https://arxiv.org/pdf/1002.0963](https://arxiv.org/pdf/1002.0963)

Survey / context:

- **Co-Movement Pattern Mining from Videos**, PVLDB (2024).
[https://dl.acm.org/doi/10.14778/3632093.3632119](https://dl.acm.org/doi/10.14778/3632093.3632119)
- **Spatiotemporal co-occurrence pattern mining on ship trajectory data**,
SAGE (2024). [https://journals.sagepub.com/doi/full/10.1177/16878132241274449](https://journals.sagepub.com/doi/full/10.1177/16878132241274449)
- **Mining moving object gathering pattern based on Spark RDD + R-tree**,
ScienceDirect (2019). [https://www.sciencedirect.com/science/article/abs/pii/S0925231219310501](https://www.sciencedirect.com/science/article/abs/pii/S0925231219310501)
- **Anomalous Trajectory Detection Between ROIs Based on ANPR**, Springer (2018).
[https://link.springer.com/chapter/10.1007/978-3-319-93701-4_50](https://link.springer.com/chapter/10.1007/978-3-319-93701-4_50)

