# ML Approaches — Clustering & Learning for Companion-Vehicle Detection

> Research notes on **machine-learning** alternatives and complements to our FP-Growth /
> MaxGrowth pattern miners, for the checkpoint-ANPR companion-vehicle problem.
> Companion to `doc/Methods.md` (which surveys the *database* pattern-mining lineage:
> flock → convoy → swarm → platoon → FCP). This doc covers the *ML* side: density
> clustering, trajectory clustering, representation learning, graph/GNN, and deep
> sequence models. Compiled from a literature scan, May 2026.

---

## 0. The framing — and one trap to avoid first

Our data is **276 M discrete sightings** `(plate, location, timestamp)` over 31 days,
~22 M plates, ~977 cameras, on a constrained Spark/Slurm cluster. The goal is to find
**sets of plates that travel together** (companions / 伴随车辆 / platoons).

The user's seed idea was: *cluster with `{location, time}` as the distance.* This is the
right instinct, but **where** you apply it decides whether you find companions or just
crowds. There is a trap worth stating up front, because it reframes the whole survey:

> **Clustering raw sightings in joint `(location, time)` space reproduces FP-Growth's
> transactions, not its convoys.** One dense cluster in `(loc, time)` space = all cars at
> one camera in one ε-window = exactly one `collect_set(plate)` bucket in
> `convoy_fpgrowth.py`. So ST-DBSCAN on raw sightings gives you the *bucket-level
> co-occurrence groups* — the **input** to convoy mining, not the output. The companion
> signal is not "who shared one bucket" but "who shared **many** buckets across the
> month." That signal lives in a **different space**: per-plate trajectory space, or the
> plate-plate co-occurrence graph.

So the ML toolbox splits cleanly by *what gets embedded into the metric space*:

| What you cluster | Metric space | What a cluster means | Role |
|---|---|---|---|
| **Raw sightings** | `(location, time)` | A crowd at one camera/window | = FP-Growth transaction builder (a *pre-stage*, not a detector) |
| **Per-plate trajectories** | trajectory-similarity (DTW/LCSS/embedding) | Vehicles with similar routes/timing | Candidate companion groups (end-to-end-ish) |
| **Plate-plate co-occurrence graph** | graph proximity / community | A fleet that recurrently co-occurs | The actual companion detector (complement *or* end-to-end) |

Everything below is organized by these three "what gets embedded" choices.

---

## 1. Spatiotemporal density clustering — `(location, time)` space

### 1.1 ST-DBSCAN (Birant & Kut, *Data & Knowledge Engineering* 60(1):208–221, 2007)

Core idea: DBSCAN with **two radii** — a spatial `eps1` and a temporal `eps2` — so a point
is a neighbor only if it is within `eps1` in space **and** `eps2` in time. Adds handling
for clusters of differing density and for noise. This is the canonical realization of the
user's "`{location, time}` as distance" idea.

- **Distance:** composite — `dist_spatial ≤ eps1 ∧ dist_temporal ≤ eps2`. (Not a single
  fused metric; two thresholds, which sidesteps the unit-mismatch problem of summing
  meters + seconds.)
- **Treats `{location, time}` as a metric space?** Yes — explicitly, the defining feature.
- **Scalable to 10⁸?** Single-machine reference impl: **no**. But the *idea* is the basis
  of every distributed ST clustering below.
- **Role for us:** **pre-stage only** — see the §0 trap. On raw sightings it rebuilds our
  transactions. Where it *is* directly useful: replacing our fixed `floor(t/window)`
  bucketization with **adaptive** ε-windows (a quiet rural camera and a rush-hour
  intersection get different effective windows), which the dual-grid offset hack only
  crudely approximates.
- **Impl:** `eubr-bigsea/py-st-dbscan` (Python), `stdbscan` (R/CRAN).

### 1.2 ST-OPTICS / variable-density variants

OPTICS removes the global `eps` (orders points by reachability, extracts clusters at
multiple densities). ST-OPTICS is its spatiotemporal form. **Why it matters here:** our
density varies wildly (loc 31 hotspot: 23,595 cars in one 300 s bucket vs. singletons
elsewhere) — exactly the case a single global `eps` handles badly. **But** it inherits the
§0 trap and is harder to distribute than DBSCAN. Lower priority.

### 1.3 Distributed DBSCAN — the part that scales

This is where `{loc,time}` clustering becomes feasible at 276 M points.

| Impl | Approach | Scale claim | Notes |
|---|---|---|---|
| **NG-DBSCAN** (Lulli, Dell'Amico et al., *PVLDB* 10(3), 2016) | **Approximate**, arbitrary symmetric distance, graph-based neighbor approximation | "very large datasets", distributed by design | Best fit for an arbitrary `(loc,time)` distance; approximate is fine for us |
| **MR-DBSCAN** (He et al.) | MapReduce, 4-stage, handles skewed data | billions of points in literature | The classic skew-aware design — relevant given loc-31 skew |
| `irvingc/dbscan-on-spark` (Scala) | Grid partition, loosely follows MR-DBSCAN | mid-scale | Maintained, JVM-native — fits our Spark stack |
| `mraad/dbscan-spark` (Scala) | "Fishnet" grid cells, group same-cell points | mid-scale | Simple, geospatial-oriented |
| `bwoneill/pypardis` (PySpark) | KD-tree partitioner along max-variance axis | mid-scale | Pure Python; easiest to read/modify |
| "Fast DBSCAN w/ Spark" (Han et al., 2018) | grid + merge | 137× @ 512 cores / 1 M pts | Benchmarks show core-scaling reality |

**Honest scaling note:** distributed DBSCAN's hard part is **skew** (the loc-31 problem) and
the **merge** step across partition borders. Reported speedups (e.g. 137× at 512 cores on
1 M points) are on *uniform* data; our hotspot would be the bottleneck. Mitigation is the
same `density_cap`/`max_cluster_size` lever we already use in FP-Growth/MaxGrowth.

---

## 2. Trajectory clustering — per-plate trajectory space

Here a *vehicle* (its whole sequence of sightings) is the object, and clusters are groups
of vehicles with **similar routes**. This is closer to "companions" — but note similar
*routes over the month* ≠ *traveling together at the same time*; you still need a temporal
co-occurrence check to confirm companionship (similar commuters are not a convoy).

### 2.1 TRACLUS (Lee, Han, Whang, *SIGMOD* 2007, pp. 593–604)

Partition-and-group: cut each trajectory into line segments via **MDL**, then density-cluster
the *segments* (not whole trajectories) to find **common sub-trajectories**.

- **Distance:** a 3-component segment distance — perpendicular + parallel + angular.
- **Designed for continuous GPS.** Our data is **sparse checkpoint hops**, not dense
  polylines — the MDL partitioning has little to bite on (a "segment" is just camera→camera).
  Applicable in spirit (common sub-routes = shared corridors) but the segment-distance
  assumptions don't transfer cleanly. **Medium-low fit.**
- **Impl:** `traclus-python` (PyPI), `AdrielAmoguis/TRACLUS`. Single-machine.

### 2.2 Trajectory-similarity distances (DTW / LCSS / EDR / Fréchet / Hausdorff)

The pairwise measures you'd feed into k-medoids / hierarchical / DBSCAN-on-trajectories:

| Measure | Handles time-shift | Handles gaps/noise | Cost | Fit for checkpoint hops |
|---|---|---|---|---|
| **DTW** | yes (warping) | poorly | O(n·m) | ok for route shape, ignores absolute time |
| **LCSS** | yes | **yes** (skips outliers) | O(n·m) | **good** — gap-tolerant matches our misses/occlusion |
| **EDR** | yes | yes | O(n·m) | good, edit-distance flavor |
| **Fréchet / Hausdorff** | no / set-based | no | O(n·m) / O(n·m) | poor — sensitive to missing detections |

- **Scalability:** all are O(n·m) **per pair** and clustering is O(N²) pairs → **22 M
  plates is hopeless** without blocking. This is the killer. You *must* pre-filter pairs
  (our `MinHash LSH` plan in `doc/PLAN.md §2.2` is exactly the right blocker), or move to
  embeddings (§3) which make similarity O(1) dot-products after an O(N) encode.
- **Role:** refinement/scoring on *candidate* pairs from a cheaper stage, never global.

---

## 3. Trajectory representation learning — embed, then cluster

The scalability fix for §2: learn a fixed-length vector per trajectory **once** (O(N)),
then clustering / nearest-neighbor is cheap (ANN, dot-products, k-means). This is the most
promising "modern ML" direction and the best route to an **end-to-end** detector.

### 3.1 Checkpoint/route embeddings via graph walks (cheapest, do-first)

Treat the **977 cameras as a graph** (edge camera_i→camera_j weighted by how many vehicles
transition between them). Run **DeepWalk / node2vec** to get a vector per camera; a vehicle's
trajectory = sequence/avg of its camera vectors.

- **node2vec** (Grover & Leskovec, *KDD* 2016): biased BFS/DFS walks + skip-gram.
- **Distance/loss:** skip-gram (cars sharing routes get nearby camera vectors).
- **Scalable?** The camera graph is **tiny** (977 nodes) → node2vec is **trivial** here;
  this is the single cheapest learned-representation option. (Caveat: vanilla
  Spark-node2vec OOMs on *large* graphs — **Fast-Node2Vec**, Pregel-based, He et al. 2018,
  handles billions of edges — but we don't need it at 977 nodes.)
- **Role:** complement — gives plates a cheap route-similarity feature for blocking/§2.2.

### 3.2 t2vec (Li, Zhao, Cong, Jensen, Wei, *ICDE* 2018, pp. 617–628)

First deep approach to trajectory similarity: a **GRU seq2seq autoencoder** with a
**spatial-proximity-aware loss**; encoder vector = trajectory embedding.

- **Loss:** reconstruction + spatial-proximity penalty (robust to low sampling / noise).
- **Key selling point: robust to low data quality** — built for *down-sampled, noisy*
  trajectories, which is precisely our occlusion/misread regime.
- **Scalable?** Training is GPU; **inference is O(N) and embarrassingly parallel**, then
  ANN search. ~1 order of magnitude faster than DTW-style search at query time.
- **Impl:** `boathit/t2vec` (PyTorch). **Note our cluster has no GPU in the Slurm spec** —
  training would need a GPU node or CPU-bound patience. **Medium fit, high payoff.**

### 3.3 Self-supervised / contrastive (current SOTA)

- **Trembr** (Fu & Lee, *ACM TKDD* 2020): RNN encoder-decoder that reconstructs **both
  roads and timestamps** — temporal-aware embeddings.
- **START** (Jiang et al., *ICDE* 2023, arXiv:2211.09510): graph-attention road embedding
  (TPE-GAT) + time-aware encoder, trained with **span-masked recovery + trajectory
  contrastive learning**. Consistently beats prior TRL; strong on temporal regularities.
- **RED** (Zhou et al., *PVLDB* vol. 18, p. 80, 2025): efficient TRL, recent.
- **Distance/loss:** contrastive (InfoNCE-style: augmented views of a trajectory pull
  together, others push apart) + masked reconstruction.
- **Scalable?** Same shape as t2vec — GPU train, O(N) encode, cheap clustering after.
- **Role:** the **end-to-end** play — embed every plate's trajectory, then ST-DBSCAN /
  k-means / ANN on the embeddings, and a final temporal-co-occurrence check confirms
  companionship. Highest ceiling, highest effort (GPU + no off-the-shelf ANPR impl).

---

## 4. Graph & GNN — plate-plate co-occurrence space (best practical fit)

This is where companion detection is *naturally* posed, and we already do the first half
(`scripts/graph_communities.py` runs Louvain/label-prop on the convoy output).

### 4.1 Co-occurrence graph + community detection (we already do this — extend it)

Build `V = plates`, `E = (plateA, plateB, co-occurrence count)`; partition into fleets.

- **Today:** `graph_communities.py` builds edges from FP-Growth itemsets and runs
  **Louvain** (networkx, single-machine — fine because the convoy population is only
  hundreds of plates *after* mining).
- **The scalable upgrade:** build the pair graph **directly** from raw buckets in Spark
  (stream each bucket emitting `(p1,p2,+1)`), skipping FP-Growth entirely, then run
  **distributed community detection**:
  - GraphFrames `connectedComponents()` / `labelPropagation(maxIter)` — built-in, scales.
  - **Louvain on Spark** needs a 3rd-party pkg or Pregel impl; see "Large-Scale Graphs
    Community Detection using Spark GraphFrames" (arXiv:2408.03966) for K-Cliques /
    Louvain / FastGreedy on GraphFrames.
- **Distance:** edge weight = co-occurrence frequency (implicitly `{loc,time}`-derived).
- **Role:** **complement *and* end-to-end.** As complement it turns "5,447 itemsets" into
  "N labeled fleets" (replaces the manual Fleet A/B/D in the In-Process page). As
  end-to-end, the direct-pair-count → community path is a *full standalone detector* that
  never needs FP-Growth. **This is the highest ROI item.**

### 4.2 Graph embeddings / GNN for companion groups

- node2vec/DeepWalk on the **plate** graph (not the camera graph) → plate vectors → cluster.
  But the plate graph has ~22 M nodes; vanilla Spark-node2vec OOMs → need **Fast-Node2Vec**
  (Pregel) or run only on the post-threshold subgraph.
- **GNNs** (GCN/GraphSAGE/GAT, PyTorch Geometric) can learn fleet membership if you have
  labels or a self-supervised objective — but **no GPU** + 22 M nodes makes full-graph GNN
  impractical here. Sample-based GraphSAGE on the thresholded subgraph is the only realistic
  variant. **Research-tier, not first-try.**

### 4.3 Subgraph similarity (the deployed-industry approach)

- **US Patent 12,555,393 — "Vehicle data fusion based on spatiotemporal information and
  subgraph similarity"**: builds a network-analysis graph from distance/time-travel info,
  flags anomalous plate nodes, extracts subgraphs, and scores **subgraph-pair similarity**.
  (Also US11254325B2, vehicle-data analytics.) This is the production pattern Chinese
  ANPR-at-scale systems and these patents converge on: **graph + subgraph similarity**,
  not raw frequent-itemset tables.

---

## 5. Deep / sequence models & the directly-analogous ITS literature

This exact problem (伴随车辆 / 同行车 / 套牌车 on 卡口 data) is studied heavily in Chinese
ITS work — closer to our data than any GPS-trajectory paper.

- **PlatoonFinder — 基于车牌识别流数据的车辆伴随模式发现方法** (*Journal of Software /
  软件学报*, art. 5220): recasts companion discovery as **frequent-sequence mining with
  customized spatiotemporal constraints** (vs. position-only Apriori/PrefixSpan), uses
  pseudo-projection, and runs **streaming** — latency below the min inter-record interval
  of real ANPR streams. The closest published analog to our problem; a sequence-mining
  cousin of MaxGrowth, streaming-first.
- **Trajectory Semanticization: Accompany Vehicle Discovery Inspired by Semantic
  Similarity** (Xu, *J. Advanced Transportation*, Wiley, 2026): treats trajectories as
  "documents" of checkpoints and uses **semantic-similarity embeddings** (word2vec/doc2vec
  lineage) — i.e. §3 applied to ANPR specifically, with deep learning for accompany-vehicle
  discovery and trajectory clustering. Direct evidence the embedding route works on our data
  shape.
- **套牌车 (fake-plate) detection via spatiotemporal reachability** (e.g. ECNU 学报 2018;
  HIT parallel method): the *dual* problem — same plate physically impossible to be at two
  far checkpoints in a short interval. Useful as a **data-cleaning pre-stage** (drop
  impossible sightings before mining) and shares all the windowing machinery.
- **General/streaming co-movement platforms** (relevant for distribution):
  - **"A General and Parallel Platform for Mining Co-Movement Patterns"** (Fan et al.,
    *PVLDB* 10, p. 313, 2017): unifies flock/convoy/swarm/platoon as a **two-stage
    clustering-then-enumeration** pipeline, parallelized — the reference architecture for
    "clustering + mining" at scale.
  - **Liu et al., KAIS 2025** (already in `Methods.md`): two-stage clustering+mining on
    **Flink**, streaming.
- **Sequence DL (LSTM/Transformer) & anomaly detection:** general next-location and
  trajectory-anomaly models exist, but supervised companion detection needs labels we don't
  have, and Transformers need GPUs we don't have. **Out of scope as a first step**; the
  self-supervised embeddings in §3.3 are the realistic deep-learning entry point.

---

## 6. Master comparison table

| Method | Space / what's embedded | Distance / loss | Treats `{loc,time}`? | Scales to 10⁸? | Distributed impl | Complement or End-to-end | Strength | Weakness |
|---|---|---|---|---|---|---|---|---|
| **ST-DBSCAN** | raw sightings | eps1(space)+eps2(time) | yes (core) | no (single-node) | via §1.3 | Pre-stage (builds transactions) | adaptive windows; matches the idea | **rebuilds buckets, not convoys** |
| ST-OPTICS | raw sightings | reachability, multi-density | yes | no | rare | Pre-stage | handles density skew | hard to distribute; same trap |
| **NG-DBSCAN** | raw sightings | any symmetric (approx) | yes (custom) | **yes** (approx) | yes (native) | Pre-stage | arbitrary distance, scales | approximate; merge/skew cost |
| MR-DBSCAN / Spark-DBSCAN | raw sightings | eps (grid) | yes | yes | yes (Scala/PySpark) | Pre-stage | skew-aware, on our stack | border-merge complexity |
| TRACLUS | sub-trajectory segments | perp+parallel+angular | partial | no | no | Complement (corridors) | finds common sub-routes | built for dense GPS, not hops |
| DTW/LCSS/EDR + cluster | trajectory pairs | warping / edit | route only | **no** (O(N²)) | no | Complement (scoring) | LCSS gap-tolerant | O(N²) pairs — needs blocking |
| node2vec on **camera** graph | 977 cameras | skip-gram | route via co-transit | **yes** (tiny graph) | trivial | Complement (features) | cheap, easy, on-stack | route-only, no timing |
| **t2vec** | per-plate trajectory | seq2seq + spatial loss | route + robustness | yes (O(N) encode) | inference parallel | End-to-end (embed→cluster) | robust to noise/occlusion | **needs GPU to train** |
| START / Trembr / RED | per-plate trajectory | contrastive + masked recon | route + **time** | yes (O(N) encode) | inference parallel | End-to-end | SOTA; temporal-aware | GPU; no ANPR off-the-shelf impl |
| **Co-occur graph + community** | plate-plate graph | co-occur weight | implicit (from buckets) | **yes** | GraphFrames LPA / Louvain-Spark | **Complement & End-to-end** | natural fit; we half-do it | Louvain-on-Spark needs 3rd-party |
| node2vec/GNN on **plate** graph | 22 M-node graph | skip-gram / message-pass | implicit | partial | Fast-Node2Vec (Pregel) | End-to-end | learned fleet features | OOM at 22 M; GPU for GNN |
| Subgraph similarity (patent) | plate subgraphs | subgraph sim score | implicit | yes (industry) | proprietary | Complement | deployed at scale | no open impl |
| PlatoonFinder (软件学报) | plate stream | freq-seq + ST constraints | yes | yes (streaming) | streaming | End-to-end | closest analog; real-time | no public code |
| LSTM/Transformer/GNN supervised | trajectory | task loss | yes | training-heavy | — | End-to-end | high ceiling | needs labels **and** GPUs |

---

## 7. Recommended shortlist — what to try first on *this* cluster

Ranked by (payoff ÷ effort) given **no GPU**, 5 modest Spark nodes, and that we already
produce convoy output:

1. **Direct pair-graph → distributed community detection (GraphFrames).** *Do first.*
   Stream raw buckets → `(p1,p2,+1)` pair counts in Spark → GraphFrames
   `labelPropagation` / connectedComponents (or Spark-Louvain). This is a **complete
   end-to-end companion detector** that skips FP-Growth, **and** a complement that labels
   fleets automatically. Pure Spark/Scala, no GPU, on-stack today. Extends
   `graph_communities.py` from single-node to cluster-scale. **Highest ROI.**

2. **node2vec on the 977-camera graph for cheap route features + MinHash blocking.**
   The camera graph is tiny → node2vec is trivial. Gives each plate a route-similarity
   vector that powers the MinHash LSH pre-filter already planned in `PLAN.md §2.2`, so the
   expensive LCSS/DTW scoring (§2.2) runs only on real candidate pairs. Cheap, no GPU,
   directly attacks the O(N²) wall.

3. **NG-DBSCAN / Spark-DBSCAN as an *adaptive* transaction builder** (not a detector).
   Replace `floor((t-offset)/window)` bucketization with density clustering in
   `(loc,time)` so windows adapt to camera density — cleaner than the dual-grid offset
   hack, and it tames the loc-31 skew at the source. Feeds either miner.

4. **(Stretch, if a GPU node becomes available) t2vec or START trajectory embeddings →
   cluster.** The genuine "modern deep learning" end-to-end play, and the Wiley-2026
   accompany-vehicle paper is direct evidence it works on ANPR data. Park until GPU
   access exists; until then #1–#3 deliver without it.

**Bottom line on the seed idea:** "`{location,time}` clustering" is correct *as a
transaction/window builder* (#3) but is **not** itself a companion detector — clustering
raw sightings gives crowds, not convoys. The companion signal lives in the **plate-plate
co-occurrence graph (#1)** and in **learned trajectory embeddings (#4)**. Start with #1.

---

## 8. GPU + HPC hybrid — the recommended execution model (GPUs available)

With GPUs on the Slurm cluster, the winning pattern is **not** "GPU instead of Spark" but
a **two-tier hybrid**: Spark/Slurm does the embarrassingly-parallel ETL over 276 M rows on
CPU; it then hands **compact intermediates** (an edge list, per-plate trajectory sequences,
or candidate-pair batches — all 10⁶–10⁹ scale, GPU-resident) to GPU stages. This is exactly
the architecture of the best published reference for our problem:

> **Accelerated co-movement patterns mining: a heterogeneous framework based on GPU
> clusters** (FGCS, 2025) — integrates **PySpark** workflow control with GPU HPC in a
> **three-level parallel architecture**: (1) distributed multi-core CPU parallelism, (2)
> intra-node multi-CUDA-stream concurrency, (3) fine-grained GPU thread execution, plus a
> multi-level memory manager. Spatial projection, hybrid indexing, and filter-verification
> all run **on GPU**; it is *non-clustering* (so it complements MaxGrowth's enumerate-then-
> verify directly). This is the literal "HPC + GPU in mind" blueprint.

### 8.1 The division of labor

| Stage | Where | Why |
|---|---|---|
| CSV scan, type cast, plate/visit prefilter | **Spark (CPU)** | 276 M rows; I/O-bound, trivially sharded; GPU adds nothing |
| Trajectory build / co-occurrence pair counting / bucketization | **Spark (CPU)** | one shuffle each; output is compact (edge list, sequences) |
| Community detection, clustering, embedding train+infer, ANN | **GPU** | compute-dense on the *compact* intermediate; 10–100× wins live here |
| Maximality / filter-verification | **GPU** (FGCS 2025) or driver | distance-heavy verification maps to GPU threads |

Orchestration: Slurm `--gres=gpu:N`; **RAPIDS + Dask-CUDA** for multi-node-multi-GPU
(MNMG) compute, **Spark-RAPIDS plugin** if you want GPU-accelerated Spark SQL too, and
**PyTorch DDP / `torchrun`** under Slurm for the embedding models. Keep one GPU stage's
output (vectors, labels) small enough to collect back to Spark for the final join.

### 8.2 GPU feasibility at our scale (memory budgets)

- **Plate-plate co-occurrence graph:** cuGraph holds **~500 M edges on one 32 GB GPU**;
  MNMG (Dask) scales to **billions**. Our thresholded graph fits comfortably on 1–2 GPUs —
  so we can run community detection on the **full** 22 M-plate graph, not just the few
  hundred plates left after FP-Growth.
- **Trajectory embeddings:** 22 M plates × 128-dim × 4 B ≈ **11 GB** → fits one GPU.
  Encode is O(N) batched inference; FAISS-GPU/cuVS does **billion-scale** ANN, so 22 M is
  trivial.
- **Raw-sighting DBSCAN:** 276 M points won't fit one GPU; use **per-camera partitioning
  in Spark → cuML DBSCAN per partition** (each partition is small), or **MNMG cuML DBSCAN
  via Dask**. (Still subject to the §0 trap — it's a transaction builder, not a detector.)

### 8.3 The GPU stack, mapped to our methods

| Need | GPU library | Replaces / accelerates | Scale proven |
|---|---|---|---|
| Community detection | **cuGraph** Louvain / **Leiden** / LPA / connected-components | `graph_communities.py` (networkx, single-node) | 500 M edges/GPU, billions MNMG |
| Graph embeddings | **cuGraph** node2vec | §4.2 plate-graph embeddings | multi-GPU |
| Clustering | **cuML** DBSCAN (MNMG), HDBSCAN, KMeans, UMAP | §1, §3-downstream | 10–50× CPU; 3M×300 HDBSCAN ~23 min vs >24 h |
| Embedding model train/infer | **PyTorch + PyG/DGL** (t2vec, START, GraphSAGE) | §3.2–3.3, §4.2 | DDP multi-GPU |
| ANN companion search | **FAISS-GPU / cuVS (CAGRA, IVF-PQ)** | §2.2 O(N²) pair wall | billion-scale, multi-GPU |
| Co-movement verify | **FGCS-2025 heterogeneous framework** (PySpark+GPU) | MaxGrowth filter-verify | GPU-cluster |

### 8.4 Revised shortlist (GPUs available) — best HPC+GPU methods

1. **cuGraph Leiden on the full plate-plate co-occurrence graph.** *Do first.* Spark builds
   the weighted edge list from raw buckets (`(p1,p2,+1)`, CPU, distributed); cuGraph runs
   **Leiden** on GPU. **Leiden over Louvain** matters: Louvain (what `graph_communities.py`
   uses today) can return *internally disconnected* communities — a real defect for "fleet"
   semantics; Leiden guarantees well-connected communities. This is the lowest-effort,
   highest-confidence GPU win: a full end-to-end detector **and** an auto fleet-labeler, on
   the whole graph instead of the post-FPG remnant. Single GPU likely suffices; Dask-CUDA if
   edges exceed one card.

2. **Trajectory-embedding pipeline (now the premier end-to-end detector).** Spark emits
   per-plate camera sequences → **PyTorch DDP** trains **t2vec** (robust to occlusion/
   misreads — our exact regime) or **START** (contrastive + time-aware, current SOTA) across
   Slurm GPU nodes → batch-encode 22 M plates → **FAISS-GPU/cuVS CAGRA** for companion
   candidates → **cuML HDBSCAN** to group, then a temporal co-occurrence check confirms
   companionship. The Wiley-2026 accompany-vehicle paper validates the embedding route on
   ANPR specifically. Highest ceiling; GPUs remove the only blocker that parked it before.

3. **Port co-movement verification to the FGCS-2025 heterogeneous (PySpark+GPU) framework.**
   The closest published "HPC+GPU" design for *exactly* co-movement mining. Use it to move
   MaxGrowth's distance-heavy filter-verification (or the §2.2 pairwise DTW/LCSS scoring)
   onto GPU threads while PySpark keeps the data-parallel control flow. Complements MaxGrowth
   rather than replacing it; biggest speedup on the stage that currently bottlenecks the
   driver.

4. **(Optional) cuML GPU DBSCAN/HDBSCAN as an adaptive transaction builder**, per-camera-
   partitioned on Spark then GPU per partition — replaces the dual-grid offset hack and tames
   loc-31 skew, feeding either miner. Useful but lower-impact than #1–#3 (still the §0 trap).

**GPU-aware bottom line:** #1 (cuGraph Leiden) is the immediate, near-zero-risk upgrade —
swap networkx-single-node for GPU-on-the-full-graph and gain well-connected fleets. #2 is the
strategic end-to-end play GPUs unlock. #3 is the principled way to keep MaxGrowth and push
its hot stage onto the GPU. All three are explicitly Spark-for-ETL + GPU-for-compute.

---

## Sources

Density clustering:
- Birant & Kut, *ST-DBSCAN: An algorithm for clustering spatial–temporal data*, Data & Knowledge Engineering 60(1):208–221, 2007. https://www.sciencedirect.com/science/article/pii/S0169023X06000218 · impl https://github.com/eubr-bigsea/py-st-dbscan
- Lulli, Dell'Amico, Michiardi, Ricci, *NG-DBSCAN: scalable density-based clustering for arbitrary data*, PVLDB 10(3), 2016. https://dl.acm.org/doi/10.14778/3021924.3021932
- DBSCAN on Spark: https://github.com/irvingc/dbscan-on-spark · https://github.com/mraad/dbscan-spark · https://github.com/bwoneill/pypardis
- Han et al., *A Fast DBSCAN Algorithm with Spark Implementation*, 2018. http://cucis.ece.northwestern.edu/publications/pdf/HAL18.pdf

Trajectory clustering & similarity:
- Lee, Han, Whang, *Trajectory Clustering: A Partition-and-Group Framework (TRACLUS)*, SIGMOD 2007, 593–604. https://hanj.cs.illinois.edu/pdf/sigmod07_jglee.pdf · impl https://github.com/AdrielAmoguis/TRACLUS

Representation learning:
- Li, Zhao, Cong, Jensen, Wei, *Deep Representation Learning for Trajectory Similarity Computation (t2vec)*, ICDE 2018, 617–628. https://dblp.org/rec/conf/icde/LiZCJW18.html · impl https://github.com/boathit/t2vec
- Jiang et al., *Self-supervised Trajectory Representation Learning with Temporal Regularities and Travel Semantics (START)*, ICDE 2023. https://arxiv.org/abs/2211.09510
- Fu & Lee, *Trembr: Embedding Trajectories Through Road Networks*, ACM TKDD, 2020.
- Zhou et al., *RED: Effective Trajectory Representation Learning*, PVLDB vol. 18, p. 80, 2025. https://www.vldb.org/pvldb/vol18/p80-zhou.pdf
- Self-supervised contrastive representation learning for large-scale trajectories, FGCS 2023. https://www.sciencedirect.com/science/article/abs/pii/S0167739X23002376

Graph / GNN / industry:
- Grover & Leskovec, *node2vec: Scalable Feature Learning for Networks*, KDD 2016. https://cs.stanford.edu/~jure/pubs/node2vec-kdd16.pdf
- He et al., *Efficient Graph Computation for Node2Vec (Fast-Node2Vec)*, 2018. https://arxiv.org/abs/1805.00280
- *Large-Scale Graphs Community Detection using Spark GraphFrames*, 2024. https://arxiv.org/html/2408.03966v1
- US Patent 12,555,393, *Vehicle data fusion based on spatiotemporal information and subgraph similarity*. https://image-ppubs.uspto.gov/dirsearch-public/print/downloadPdf/12555393 · related US11254325B2 https://patents.google.com/patent/US11254325B2/en

ANPR / 卡口 companion-vehicle literature:
- *基于车牌识别流数据的车辆伴随模式发现方法 (PlatoonFinder)*, Journal of Software. https://www.jos.org.cn/jos/article/abstract/5220
- Xu, *Trajectory Semanticization: A Method of Accompany Vehicle Discovery Inspired by Semantic Similarity*, J. Advanced Transportation (Wiley), 2026. https://onlinelibrary.wiley.com/doi/10.1155/atr/1464526
- *基于卡口监测数据流的套牌车检测*, 华东师范大学学报, 2018. https://xblk.ecnu.edu.cn/CN/10.3969/j.issn.1000-5641.2018.02.007

Co-movement platforms (distribution reference):
- Fan et al., *A General and Parallel Platform for Mining Co-Movement Patterns*, PVLDB 10, p. 313, 2017. http://www.vldb.org/pvldb/vol10/p313-fan.pdf
- Liu et al., *An efficient distributed co-movement pattern detection framework for streaming trajectory*, KAIS 2025. https://link.springer.com/article/10.1007/s10115-025-02369-7

GPU + HPC hybrid acceleration:
- *Accelerated co-movement patterns mining: A heterogeneous framework based on GPU clusters*, Future Generation Computer Systems, 2025. https://www.sciencedirect.com/science/article/abs/pii/S0167739X25005965
- RAPIDS **cuGraph** — Louvain / Leiden / node2vec, multi-GPU (≈500 M edges/32 GB GPU, billions MNMG). https://docs.rapids.ai/api/cugraph/stable/ · Leiden: https://developer.nvidia.com/blog/how-to-accelerate-community-detection-in-python-using-gpu-powered-leiden/
- RAPIDS **cuML** — GPU DBSCAN (MNMG via Dask), HDBSCAN, KMeans, UMAP (10–50× CPU). https://github.com/rapidsai/cuml · HDBSCAN: https://developer.nvidia.com/blog/gpu-accelerated-hierarchical-dbscan-with-rapids-cuml-lets-get-back-to-the-future/
- **FAISS-GPU / NVIDIA cuVS** (CAGRA, IVF-PQ) — billion-scale ANN, multi-GPU. https://developer.nvidia.com/blog/enhancing-gpu-accelerated-vector-search-in-faiss-with-nvidia-cuvs/ · Johnson, Douze, Jégou, *Billion-Scale Similarity Search with GPUs*, 2017. https://arxiv.org/abs/1702.08734
- He et al., *Efficient Graph Computation for Node2Vec (Fast-Node2Vec, Pregel)*, 2018. https://arxiv.org/abs/1805.00280
