# Three Methods, Step by Step

> A unified walkthrough of the three convoy-detection methods built on the 31-day ANPR dataset: **FP-Growth** (cells), **MaxGrowth** (routes), and **Embedding** (vectors). For each: the idea, the step-by-step procedure, the actual results from the full-month run, and an honest analysis of what the numbers mean.
>
> Compiled June 2026. Companion to the specialised docs:
> - `doc/Methods.md` — academic lineage of co-movement mining
> - `doc/MaxGrowth.md` — engineering reference for MaxGrowth (pipeline diagram, failure modes, tuning knobs)
> - `doc/Embeddings.md` — how variable-length sequences become fixed embeddings, plus TBPTT + bucket batching
> - `doc/Downstream.md` — what to *do* with the outputs of all three detectors
> - `doc/ML_Approaches.md` — broader ML alternatives reviewed but not built

This document does **not** repeat those references; it is the meta-walkthrough that ties them together.

---

## 0. The problem, in one paragraph

The raw dataset is 275.9 M records of the form `(plate, location, timestamp)` — each row a single ANPR sighting on one of 977 cameras over 31 days. The question every method answers is the same: **which plates travel together?** What changes across methods is the *primitive* — what counts as evidence of "together":

| method | primitive | "together" means |
|---|---|---|
| FP-Growth | one `(camera × 5-min)` cell | both plates appeared in the same cell |
| MaxGrowth | an ordered camera route of length ≥ 3 | both plates traversed the same directed route, ε-close at each stop |
| Embedding | a 256-dim trajectory vector | both plates' route-shape vectors are close in cosine distance, *and* they actually co-occur in raw windows |

Each primitive maps to a different algorithm, finds different things, misses different things, and surfaces a different population of plates. The point of building all three is that none of them is *the* answer — together they triangulate.

---

## 1. Method 1 — FP-Growth (cells)

### 1.1 The idea

Treat each `(location, 5-minute bucket)` cell as a **transaction** containing the set of plates that appeared in it. The dataset becomes a transaction database; FP-Growth (the classical frequent-itemset miner) returns plate-itemsets that appear in ≥ `min_count` cells. Two plates that share many cells are a candidate convoy; three or more that share many are a candidate fleet.

The primitive's strength is precision on small groups: if plates A, B, C show up *together* in 200 different cells over 31 days, the chance that's coincidence is ~zero. Its weakness is **C(N, k) combinatorial blowup** on large fleets — a 10-car group produces C(10,2) + C(10,3) + … = 1,013 separate itemsets that all carry the same underlying signal.

### 1.2 The procedure

Code: `src/job/convoy_fpgrowth.py`. Engine: Spark MLlib's FP-Growth on the existing Slurm cluster.

```
1.  CSV scan        Read 31.csv → DataFrame (plate, location, timestamp)
2.  Bucketize       Add `bucket = floor(timestamp / 300)` for ε = 300 s
3.  Dual-offset     Repeat at offset ε/2 so a convoy split across a bucket
                    boundary still lands in one bucket in *some* grid
4.  Transactionize  groupBy(location, bucket).agg(collect_set(plate))
                    Each row is now a transaction (set of plates in that cell)
5.  Density-cap     Drop transactions with > density_cap plates (default 2000).
                    A 5,000-plate bucket is a stuck-camera artefact, not a convoy
6.  FP-Growth       spark.ml.fpm.FPGrowth(min_count=50).fit(transactions)
7.  Write CSV       Emit (plates, k, count) for k ∈ [2, ..]
```

Three engineering details that were not obvious until we hit them:

- **`density_cap`** is essential. Without it, the FP-tree allocates O(N²) workspace inside the JVM and OOMs on the freak buckets (one camera at one ε-window once contained 23,595 plates — almost certainly a stuck reader). Cap at 2000 cleanly excludes the freak while keeping all legitimate dense buckets (the largest real one had 1,718 plates).
- **`-Xss16m`** on the executor JVM. FP-tree path depth scales with transaction width; the default 1 MB Java thread stack overflows on dense transactions during Kryo serialization (~2,000-deep recursion). 16 MB at 18 cores × 5 nodes = 1.4 GiB extra heap cost — irrelevant against the 75 GiB executor budget.
- **No `df.persist()`**. We tried caching the raw DataFrame and got heartbeat-timeout executor losses at week-1 scale; the cache was ~13 GiB across the cluster for ~zero benefit because each grid pass does one scan.

### 1.3 Results

Full-month run, `min_count = 50`:

```
total convoys      : 5,447
distinct plates    : 783             (of 22 M ever seen in the data)
top pair           : (477634, 491688)  co-located 628× in 31 days (~20×/day)
4-plate always-together set
                   : {3271142, 16972992, 16970573, 511670}
                     appears in exactly 1,548 convoys each
6-car permanent convoy
                   : {16970573, 16972992, 18150551, 3271142, 3542309, 511670}
                     consistent k=5 and k=6 top hit
wall time          : 4 h 38 min on 5 × {20 cores, 100 GiB} nodes
```

The distinct-plate count is the headline number. **783 plates out of 22 M ever-seen = 0.0036 %.** Convoy signal in this dataset is extraordinarily thin: most vehicles are not in any group.

The size distribution is inverted-U at small sizes (22.7 k pairs → 2.9 k triples → larger sizes recover as the combinatorial blowup of the few real fleets dominates).

### 1.4 Analysis

What this method is showing you:

- **The 6-car permanent convoy is real.** Its 4-of-6 always-together subset appearing in exactly 1,548 convoys (matching = 1,548 / 31 ≈ 50 convoys per day) is the strongest *individual* signal in the dataset. The 1,548 is the same number for all four plates — they appear in exactly the same set of convoys, i.e. they're never observed without each other.
- **The top single pair is a service relationship**, not a convoy in the surveillance sense. `(477634, 491688)` co-located 628× over 31 days = roughly 20× per day, every day — most plausibly paired commercial vehicles (a truck and its trailer, a courier and its van, a service vehicle pair).
- **The combinatorial inflation is the limit.** The same 6-car group produces C(6,2) + C(6,3) + C(6,4) + C(6,5) + C(6,6) = 57 separate convoy rows. A hypothetical 100-plate corporate fleet would produce ~2³² rows. This is why FP-Growth cannot find large fleets cleanly — the algorithm doesn't *miss* them, but it surfaces them as combinatorial noise that drowns the small precise findings we actually want.

OCR-ghost caveat: a pair like `(16970573, 16972992)` — edit-distance-1 plate IDs — is suspicious. `scripts/ocr_canonicalize.py` enumerates such pairs; manual review confirmed both are real but co-occur for OCR-adjacent reasons in a subset of cases. Real fleet vs. OCR ghost: ~50/50 in our manual sample. For the production registry, an OCR-canonicalisation pass should run before FP-Growth.

---

## 2. Method 2 — MaxGrowth (routes)

### 2.1 The idea

Strengthen the evidence required for a "convoy" from *shared bucket* to *shared ordered route*. A platoon pattern is a pair `⟨O, P⟩` where `O` is a set of plates (members) and `P` is a directed sequence of cameras (the route), and every plate in `O` actually visited every camera in `P` in that order, ε-close at each stop, with at most `d` cameras skipped between adjacent route stops. We want every **maximal** such pattern with `|O| ≥ m` and `|P| ≥ k`.

The primitive is **directional co-traversal**, which is far harder to fake by coincidence than shared-bucket. Two cars in a parking lot in the same 5-min window: easy. Two cars driving the same ordered 3-camera route within ε seconds at each camera: rare.

### 2.2 The procedure

Code: `src/max_growth/`. Engine: Spark for ETL, broadcast + driver for recursion, Spark again for parallel root-growth. Reference paper: Bei et al., VLDB 2024.

```
SPARK (CSV scan and clustering)
1.  CSV scan + cast types
2.  Plate-side prefilter
        groupBy(plate).agg(countDistinct(camera), count(*))
          filter (≥ min_visits AND ≥ min_observations)
          orderBy(count DESC).limit(max_plates)
        Apriori-style frequency prune — drops occasional visitors
3.  Trajectory build  (one shuffle)
        per plate, sort_array(collect_list(struct(t, camera)))
4.  Cluster materialisation  (dual-offset, one shuffle)
        for offset in {0, ε/2}:
            bucketize by floor((t - offset) / ε)
            groupBy(camera, bucket).agg(collect_set(plate))
            filter (m ≤ |members| ≤ max_cluster_size)
DRIVER (broadcast prep)
5.  Build pos[plate][camera] index + next_cams[plate] list
6.  Broadcast (by_camera, pos, next_cams) to executors
SPARK (parallel growth)
7.  sc.parallelize(clusters, numSlices=root_slices)
        .mapPartitions(grow_partition)
        .distinct()
        .collect()
   Each task: for each cluster root, recursively extend the
   pattern's route by votes from member trajectories; emit
   ⟨O, P⟩ where local-maximality fires
DRIVER (post-processing)
8.  maximal_only(patterns, d)  — inverted-index global dominance check
9.  Write CSV
```

Two non-obvious correctness details:

- **Local maximality pruning** inside `grow_from_root`. The naive enumerator emits a pattern at every recursion depth ≥ k — millions of redundant prefixes of their own extensions. The local rule: *if any extension preserves the full current membership, the longer route strictly dominates this stack — skip emission*. Cuts raw emissions ~10–100× before the global dominance filter even runs.
- **Inverted-index dominance filter.** Pattern A dominates pattern B iff A.members ⊇ B.members ∧ B.route is a d-subsequence of A.route. The naive O(N²) scan would have been 19+ hours on day-1's 2.6 M raw patterns; restricting candidates to patterns containing every member of B (seeded on B's *rarest* plate) makes per-pattern dominance O(|candidates|) ≈ a few dozen — finishes in seconds on tens of thousands.

For the operational details (memory rebalancing across the master/worker nodes, why the driver and a local executor can't both fit on a 100 GB node, the bf16 fix, the bucket batching, the cuDNN reservespace math) see `doc/MaxGrowth.md`.

### 2.3 Results

Full-month run, `m=2, k=3, d=1, ε=300, --min-obs 200, --max-plates 100k, --max-cluster 50`:

```
maximal platoons                 : 223,184
plates appearing in some platoon : 87,047
cameras spanned by routes        : 795 of 977
longest route                    : 9 cameras (2-plate pair)
largest platoon                  : 20 plates on route 11 → 227 → 557
top corridor (by platoon count)  : 434 → 404 → 238  (5,324 platoons)
wall time                        : ~30 minutes (after the four
                                   failure-mode rounds documented
                                   in MaxGrowth.md)
```

Route-length distribution is heavily 3-camera-dominated (the `k=3` floor) and decays ~10× per added camera out to 9 — clean power-law tail. Platoon-size distribution is 71% pairs, with a 6–20-plate tail that's the persistent-fleet signal.

The eight hub corridors (`434→404→238`, `68→181→45`, `391→375→372`, …) form the city's convoy backbone. Hub cameras (`434`, `404`, `238`, `68`, `478`) carry 24,000+ routes each. Plate `490759` appears in 172 platoons — likely a transit / commercial vehicle.

### 2.4 Analysis

What this method is showing you:

- **The trajectory framing closes the OCR / occlusion gap.** Each 3-camera route is co-traversed within ε at each stop, with at most d=1 camera skipped between consecutive route nodes. A pair of cars whose plate IDs differ by one character but follow the same 3-camera ordered path within seconds of each other is real, not a misread. The `d=1` gap absorbs a missed reading at any one camera.
- **The "platoon spine" is real infrastructure.** The 5,324 platoons on `434→404→238` are not 5,324 different convoys — they are 5,324 distinct *plate-sets* that all traversed this corridor at some point. The corridor itself is the city's main convoy route; the spine is interpretable as a real road segment.
- **The size tail is the registry signal.** The 6–20-plate platoons on fixed routes are the persistent groups we want — the 20-plate maximum at `11→227→557` is the largest single fleet found by this method.

What MaxGrowth still does not see well:

- **Large statistical fleets.** A 200-plate corporate operation whose vehicles cover similar but non-identical routes never appears as a single ⟨O, P⟩. The d=1 gap tolerance is too tight when the shared structure is route-statistical, not route-exact. MaxGrowth fragments such a fleet into dozens of small overlapping platoons, each on a different exact route.

Engineering postscript: getting MaxGrowth to run on the full 31.csv took four debugging rounds (master-node OOM, cluster-count blowup, missing `--max-cluster-size` cap, etc.). Each is documented as a failure mode in `doc/MaxGrowth.md` §8. The wall-clock distance between "submit the job" and "have a result" was ~3 days of iteration; the per-attempt cost after the final tuning is 30 minutes.

---

## 3. Method 3 — Embedding (vectors)

### 3.1 The idea

Soften the evidence further: instead of requiring direct physical co-occurrence at all, train a model to **embed each plate's entire month-long trajectory as a fixed 256-dim vector**, then cluster the vectors. Two plates whose embeddings are close share *route-shape similarity* — they need not be at the same camera at the same time, just have similar route structures.

Critically, the embedding alone is *not* a convoy detector — two commuters driving the same road every morning will produce nearly identical embeddings and they are *not* companions. So a second step **confirms** each candidate embedding-cluster by counting whether its members actually co-occur in the raw ε-windowed sightings. The combination is what makes the signal real.

The primitive's strength is **large fleets with route-similar but not route-identical members** — corporate fleets, taxi pools, delivery operations. Its weakness is **small specific permanent convoys** — a 6-car group that always travels together produces six near-clone trajectories that HDBSCAN tags as noise (not enough mutual density).

### 3.2 The procedure

Code: `src/embed/`. Three stages on three different engines.

#### Stage 1 — `build_sequences.py` (Spark)

```
1.  CSV scan
2.  Plate-side prefilter (mirrors MaxGrowth)
        groupBy(plate).agg(countDistinct(camera), count(*))
3.  Trajectory build
        sort_array(collect_list(struct(t, camera))) per plate
4.  Optional max-len truncation (--max-len 0 = keep all)
5.  Write Parquet (plate, cameras[], times[], n)
```

Output: 3,830,566 sequences. Length distribution `min=10  median=38  mean=62.5  max=3,202`. The 3,202 is a single very chatty plate — almost certainly a bus or taxi. The long tail is what later forces bucket batching and TBPTT during training.

#### Stage 2 — `train_encode.py` (PyTorch on 4× H200)

Model: t2vec-style denoising seq-to-seq autoencoder. 2-layer GRU encoder → 256-d hidden state → 2-layer GRU decoder reconstructing the *clean* sequence from a *corrupted* (30% tokens dropped) input. The per-plate embedding is the encoder's final hidden state. See `doc/Embeddings.md` §1-5 for the fixed-dim mechanic.

Training scaffolding (each is a real engineering decision driven by failures we hit, documented in `doc/Embeddings.md` §§ 6-10):

- **bf16 autocast** — same exponent range as fp32 (no overflow), half the memory.
- **Bucket batching** (`--num-buckets 32`) — group sequences by length so each batch's `T_max` tracks the bucket's max, not the dataset's max (would have been 3,202 every batch).
- **Decoder TBPTT** (`--tbptt-chunk 256`) — chunk the decoder, detach hidden state between chunks, bound decoder BPTT depth at 256 even for 3,202-token sequences.
- **`clip_grad_value_(1.0)` + `nan_to_num_(grad)`** — value-clip survives any individual inf gradient; nan_to_num rescues partially-bad batches instead of skipping them.
- **NaN-loss skip backstop** — if a loss is non-finite, skip the optimizer step rather than poison Adam's running moments.

DDP across 4 H200s, `--batch-size 128/rank`. Cache encoded sequences to disk after rank 0 builds them once (avoids the 4-way parallel pre-DDP contention that wedged earlier runs).

Output: `embeddings.npy` (3.83 M × 256 fp32), `plates.npy`, `vocab.json`, `model.pt`. Loss curve 2.84 → 2.18 over 10 epochs.

#### Stage 3 — `cluster_confirm.py` (cuML on H200)

```
1.  L2-normalise embeddings (unit vectors → cosine ≈ 1 - dot)
2.  cuML HDBSCAN.fit_predict(embs)  with min_cluster_size=3
3.  For each candidate cluster:
       members ← plates with that label
       for each (camera, ε-window) seen in raw CSV containing
       members, count it if ≥ 2 cluster members were there
       cluster.confirmed = (cooccur_windows ≥ min_cooccur_windows)
4.  Write fleets.csv
```

The confirmation step is what distinguishes the embedding pipeline from naive trajectory clustering. Without it, "people who drive the same road" would be indistinguishable from "people who actually travel together".

### 3.3 Results

Full-month run, `--algo hdbscan --min-cluster-size 3 --min-cooccur-windows 3`:

```
embeddings ingested      : 3,830,566 × 256
candidate clusters       : 12,708
noise plates             : 3,727,787   (97.3% — most plates aren't fleet members)
confirmed fleets         : 4,041        (31.8% of candidates pass co-occur)
total plates in fleets   : 102,779
largest confirmed fleet  : 759 plates    (cluster 11981)
fleet-size median        : 8
fleet-size mean          : 16.9
top fleet by cooccur     : cluster 10184 — 386 plates · 54,189 windows · 36,601 pairs
```

Density-vs-size pattern (key analytical result):

```
size band     count   avg pairs/plate    % "tight" (≥10)
3-5           1,385   0.7                 0.0%
6-20          2,000   1.4                 0.0%
21-50         450     4.1                 9.1%
51-100        110     8.2                32.7%
101-200       60      15.5               48.3%
201-500       31      17.7               67.7%
501-1000      5       35.3              100.0%
```

Density grows monotonically with size — small "fleets" are mostly weak signals barely surviving the 3-window confirmation threshold; large fleets are genuinely dense. This is the embedding method's strongest qualitative result and the primary justification for its existence.

### 3.4 Analysis

What this method is showing you:

- **Large real operations.** The 195 fleets of size > 50 are the population FP-Growth and MaxGrowth physically cannot surface cleanly — FP-Growth via the C(N,k) blowup that fragments them into combinatorial noise, MaxGrowth via the d=1 gap tolerance that fragments them into per-route platoons. Cluster 11981 alone (759 plates, 55.8 pairs/plate density) is a corporate/courier/transit fleet that FP-Growth would have surfaced as ~30,000 separate convoys.
- **The confirmation step works.** 31.8% of candidate clusters pass the ≥ 3 co-occurrence window threshold. The 68.2% that fail are similar-route-but-not-companion plates — exactly what the confirmation step is there to filter out.

What this method is missing:

- **The 6-car permanent convoy is all noise.** Cross-checking against FP-Growth's ground-truth findings:

  | FP-Growth finding | Embedding result |
  |---|---|
  | top pair `(477634, 491688)` | both in cluster 11727 (208 plates) — pulled together but lumped with 206 others |
  | day-1 pair `(323390, 420190)` | split: 323390 → cluster 11541, 420190 → cluster 11477 |
  | 4-plate always-together set | **all noise (-1)** |
  | 6-car permanent convoy | **all noise (-1)** |

  Six plates that always travel together produce six near-clone trajectories. In 256-dim space they are *all close to each other* but also *isolated from everything else* — a sparse cluster of six, below HDBSCAN's density threshold. Tagged as noise.

- **Resolution.** The top pair landed in the same cluster, but with 206 other plates — the embedding's notion of "close" is coarser than co-occurrence's. For a registry that needs fleet-level resolution, that's fine; for one that needs pair-level resolution, the embedding alone is insufficient.

This is **not a bug.** The embedding's primitive is route similarity, and small specific convoys are not what route similarity is good at finding. Their absence from embedding output and presence in FP-Growth output is the central evidence that the methods are complementary.

---

## 4. Cross-validation across the three methods

### 4.1 Coverage matrix

Using FP-Growth's findings as the small-convoy ground truth and the embedding's findings as the large-fleet ground truth, here's what each method catches:

| ground-truth signal | FP-Growth | MaxGrowth | Embedding |
|---|:---:|:---:|:---:|
| top single pair (`477634`, `491688`) — 628 days | ✓ direct | ✓ as platoon members | ◐ in 208-plate cluster |
| day-1 pair (`323390`, `420190`) — 25/day | ✓ direct | ✓ as platoon members | ✗ split across clusters |
| 4-plate always-together set | ✓ k=4 row | ✓ ⟨4, k⟩ pattern | ✗ all noise |
| 6-car permanent convoy | ✓ k=6 row | ✓ ⟨6, k⟩ pattern | ✗ all noise |
| 20-plate fixed-route platoon | ✗ buried in C(20,k) | ✓ ⟨20, 3⟩ pattern | ✓ likely in 21–50 bucket |
| 200-plate corporate fleet | ✗ C(200,k) blowup | ✗ fragmented per-route | ✓ 1 confirmed fleet |
| 759-plate operation (cluster 11981) | ✗ C(759,k) → ~30k rows | ✗ fragmented | ✓ 1 confirmed fleet |

The matrix has no row where two of the three methods both miss — every signal has at least one method that catches it cleanly. That's the registry argument.

### 4.2 What each method is uniquely good at

- **FP-Growth:** small, tight, specific convoys (≤ ~10 plates). The clearest evidence in the dataset that vehicles move as a unit comes from FP-Growth, and only from FP-Growth.
- **MaxGrowth:** directional persistent groups on fixed routes. The 5,324 platoons sharing corridor `434→404→238` is route-level information the cell view simply cannot produce.
- **Embedding:** large statistical fleets. The 195 fleets of size > 50, especially the 5 of size > 500, are populations the other two methods can only fragment.

### 4.3 What no single method gives you

- A clean view of **medium-sized fleets (20–100 plates)** that have mixed-similarity routes. MaxGrowth fragments them, FP-Growth's C(N,k) starts to bite, and the embedding sometimes confirms them and sometimes splits them. The right approach is to look for cross-method *agreement* in this size band.
- **Identification of the operator** of any fleet. All three methods produce plate-set IDs; turning a 759-plate cluster into "this is courier company X" requires metadata we don't have in the dataset.

---

## 5. Combined view — the tiered registry

The three methods produce three views; the registry the project recommends fuses them into a tiered structure:

```
TIER 1 · GOLD            triple-witnessed
                         present in FP-Growth output
                         AND covered by a MaxGrowth platoon
                         AND inside a confirmed embedding cluster
                         → highest precision, operational core

TIER 2 · SILVER          double-witnessed
                         two-of-three methods agree
                         (a) large embedding fleets that MaxGrowth fragments
                         (b) small FP-Growth convoys that embedding tags noise
                         → strong evidence, expected gaps

TIER 3 · CANDIDATE       single-method output
                         one method's findings without corroboration
                         → investigative leads only
```

Each tier corresponds to a different downstream use: gold for operational identification, silver for analytical work, candidate for parameter-sensitivity studies and manual triage. See `doc/Downstream.md` for the build plan that turns these outputs into specific deliverables (fleet identification, anomaly detection, saturation mapping).

---

## 6. Honest limits and known gaps

What this project does **not** deliver:

- **Streaming.** All three pipelines are batch over a closed 31-day window. The literature (Flink 2025, Travelling Companion) covers streaming variants; we documented them in `doc/Methods.md` but did not build them.
- **Camera-graph geometry.** The repo has no camera lat/long table. Routes are integer-ID sequences; we cannot draw them on a map without geocoding the cameras first. `doc/Downstream.md` §2 catalogues which downstream tasks need this and which don't.
- **OCR canonicalisation as a first-class step.** `scripts/ocr_canonicalize.py` is a one-shot lister of edit-1 plate pairs; the production pipelines do not canonicalise IDs before ingestion. The 6-car convoy contains plate `16970573` and `16972992` — edit-distance 2 — so OCR collision is plausible. Manual review concluded both are real, but this is fragile.
- **Individual identification.** Plate IDs are opaque tokens throughout. No name lookup, no registration matching, no owner inference.
- **Predictive scoring.** "Plate X is likely a member of fleet Y" extrapolation is not a goal. The registry catalogues observed patterns over the closed 31-day window.

What this project does deliver:

- Three independent algorithmic readings of the same 275.9 M records, each with its primitive's strengths and failure modes explicitly characterised.
- A cross-validation framework (the coverage matrix in §4.1) that turns single-method outputs into multi-method consensus.
- A tiered registry design (§5) that exposes the consensus gradient as confidence tiers rather than a binary "fleet / not fleet" judgement.
- Engineering documentation that is honest about every failure mode encountered (master-node OOMs, NCCL CUDA bugs on 4× H200, NaN trains, OCR ghosts, missing length caps), so the next person to touch any of this knows where the cliffs are.

The deliverable is not "the answer". It's three witnesses to the data, with a documented method for telling whose testimony to trust on which question.
