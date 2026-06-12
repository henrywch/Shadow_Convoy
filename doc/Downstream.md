# Downstream Tasks — What to Build on Convoys, Sequences & Clusters

> The pipeline now has **three detectors** that each turn 275.9 M raw sightings into
> *groups of plates that move together*. This document is the missing other half: what to
> **do with those groups**. It catalogs downstream tasks, grounds each in the *actual*
> output files we have on disk, states scale/effort on our cluster, and gives a recommended
> build order.
>
> Companion to `doc/Methods.md` (DB pattern-mining lineage), `doc/ML_Approaches.md`
> (ML alternatives), `doc/MaxGrowth.md` and `doc/Embeddings.md` (the two newer detectors).
> Compiled June 2026.

---

## 0. Where the three detectors end today

Everything below consumes one of these three outputs. Know their exact shape first.

| Detector | Code | Output (verified on disk) | Schema | Size today |
|---|---|---|---|---|
| **FP-Growth — Convoys** | `src/job/convoy_fpgrowth.py` | `data/output/convoy_fpg_31/part-*.csv` | `plates, k, count` | 5,447 convoys · 783 distinct plates · counts ∈ [50, 628] |
| **MaxGrowth — Sequences** | `src/max_growth/` | `data/output/max_growth_31/patterns.csv` | `route, n_cameras, members, n_plates` | **223,184 patterns** · directed camera routes (`11->227->557`) |
| **Embedding — Clusters** | `src/embed/` | `data/output/embed_31/vectors_0610/{embeddings.npy, plates.npy}` | `(N, 256)` float + aligned plate IDs | **3,830,566 × 256** vectors |

What *already* consumes them:

- **Convoys** → `scripts/analyze_convoys.py` (summary.txt + 3 charts) **and**
  `scripts/graph_communities.py` (Louvain → `data/output/communities/fleets.csv`,
  132 fleets). This is the only detector with a finished downstream.
- **Sequences** → **nothing.** `patterns.csv` (223 k rows) has no analyzer, no graph, no
  ranking. This is the single biggest gap.
- **Clusters** → `src/embed/cluster_confirm.py` exists (cluster + co-occurrence confirm →
  `fleets.csv`) but is the *last GPU stage* of the embedding pipeline itself, not a
  cross-cutting consumer. The 3.83 M embedding matrix is a **reusable asset** that nothing
  else queries yet.

Two facts that constrain every geospatial idea below: **cameras are bare integer IDs
`0…976`** — the repo has **no camera lat/long table** — and the **embedding `fleets.csv`
has not been generated at full-31 scale** yet (only `sequences/` and `vectors_*/` exist).

### The core observation

The three detectors disagree by construction, and that is the opportunity:

| Detector | What "together" means | Evidence type | Failure mode |
|---|---|---|---|
| FP-Growth | shared the **same (camera, ε-bucket)** many times | co-occurrence count | counts crowds; buses/taxis dominate |
| MaxGrowth | **co-traversed a directed route** of ≥ k cameras | route + members | sensitive to gap `d`; 223 k near-dup patterns |
| Embedding | **similar route shape** in 256-D space | cosine proximity | similar ≠ simultaneous (commuters a week apart) |

A group flagged by **two or three** of these — across three *different metric spaces* — is
far stronger evidence than any single detector's top-K. **Fusion (Tier 1) is the keystone
downstream task**; the per-detector tasks (Tier 0) feed it; the applications (Tier 2–3) sit
on top of the fused registry.

---

## Tier 0 — Per-detector completion (cheap, do-first, all CPU)

Bring all three detectors to the same "finished" state Convoys already enjoys.

### 0.1 `analyze_patterns.py` — the missing MaxGrowth analyzer ★ highest ROI in Tier 0

**Input** `max_growth_31/patterns.csv`. **Why** 223 k patterns are completely un-triaged;
most are near-duplicates (prefixes/subsets of one another even after maximality filtering).
**Build** the direct analogue of `analyze_convoys.py`:

- Rank patterns by a **significance score** `n_plates × n_cameras` (a 6-plate × 5-camera
  co-traversal outranks a 20-plate × 3-camera one — longer shared routes are stronger).
- Distributions: members-per-pattern, route-length histogram, **most-traveled directed
  routes** (group by `route`, count distinct member-sets) → "convoy corridors."
- Super-connector plates (which plates appear in the most patterns) and super-routes.
- `summary.txt` + charts, mirroring the convoy analyzer so the report sections match.

**Scale/effort** pure pandas on 223 k rows, single machine, seconds. **~120 lines.**

### 0.2 Pattern → fleet graph (MaxGrowth's `graph_communities`)

`graph_communities.py` only reads the convoy CSV. Generalize it (or add a sibling) to read
`patterns.csv`: unroll each pattern's `members` into `C(n,2)` edges weighted by
`n_cameras` (route length = edge strength), then Louvain/Leiden. Produces a **second,
independent fleet labelling** from route evidence — a direct input to fusion (§1.1).
**Effort** low; reuses the existing graph code. Edge count is the watch-item (a 20-member
pattern adds 190 edges); pre-aggregate weights before building the graph.

### 0.3 Embedding asset services — ANN index + 2-D projection

The `(3.83 M, 256)` matrix is the most reusable artifact in the repo. Two thin services:

- **ANN "find vehicles like this one"** — build a FAISS-GPU / cuVS-CAGRA index over
  `embeddings.npy` (per `ML_Approaches.md §8.3`; 3.83 M × 256 ≈ 3.9 GB, trivial on one
  GPU). Query a plate → its k nearest-route neighbors in O(1). This is the literal
  *trajectory-similarity-search* primitive (VeTraSS, ACM CSUR survey) and the engine behind
  the seed-vehicle query (§2.1).
- **UMAP/t-SNE → 2-D** (`cuML`, GPU) for a fleet-space scatter the website (§3.3) can plot,
  colored by fused fleet ID.

**Effort** low–medium; both are standard RAPIDS calls. Gated only on a GPU node (already
required for embedding training).

### 0.4 Convoy temporal-stability pass (extend the convoy analyzer)

We have per-day slices (`day1`, `week1`, full `31`). Re-run convoy mining per day and track
**which pairs/fleets persist day-to-day** — a stable 31-day convoy is a real fleet; a
one-day co-occurrence is noise. Cheap (slices exist) and directly raises precision.

---

## Tier 1 — Cross-detector fusion (the keystone)

### 1.1 Unified companion-group registry + consensus scoring ★★ the central task

Reconcile all three (now four, counting both graph fleet-labellings) outputs into one
ranked, deduplicated, confidence-scored table. This is what turns three research artifacts
into a single queryable product.

**Inputs** `convoy_fpg_31`, `patterns.csv`, embedding `fleets.csv` (once generated), the
two `communities` fleet tables.

**Method**

1. Normalize each detector's output to candidate groups `{plate-set, evidence}`.
2. **Match groups across detectors** by member overlap (Jaccard ≥ τ, or treat each detector
   as edges into one meta-graph and re-cluster — a "consensus clustering" / ensemble step).
3. **Score** each fused group by how many detectors endorse it and their native strengths:
   `support = w_fpg·log(count) + w_mg·(n_cameras·n_plates) + w_emb·cohesion`, plus a
   **method-agreement multiplier** (2-of-3 ≫ 1-of-3).
4. Emit `registry.csv`: `group_id, members, detectors_agreeing, fpg_count, mg_route_len,
   emb_cohesion, consensus_score, first_seen, last_seen, n_active_days`.

**Why it's the keystone** every Tier 2–3 application queries *this* table, not three raw
CSVs. It also replaces the manual "Fleet A/B/D" inspection on the In-Process page with a
systematic, defensible ranking. **Effort** medium, pure CPU (operates on the small mined
outputs, not raw data). **Validation hook** the known super-connector fleet
(plates `16970573, 16972992, 3271142, 511670, …` — appear in 1,548 convoys *and* dominate
embedding-community 9 at weight 2,030,732) should land at/near the top with 3-of-3 agreement;
if it doesn't, the scoring weights are wrong.

---

## Tier 2 — Applications (the "so what")

This is what ANPR convoy systems are actually *for* — grounded in the policing/forensics
literature (i2group, Surrey POLARBEAR, Homayounfar & Ho).

### 2.1 Seed-vehicle convoy query — "who travels with plate X?" ★ the canonical use case

The real-world workflow is **not** "dump all fleets" — it's *"here is one known vehicle,
find its companions."* UK forces do this manually today; the published systems automate
exactly this. We can serve it three ways and cross-check:

- look up X's fused group(s) in the registry (§1.1);
- ANN nearest-route neighbors of X (§0.3);
- on-demand co-occurrence: stream X's `(camera, ε-bucket)` keys, intersect with all plates
  sharing those buckets (the `cluster_confirm.py` co-occurrence machinery, scoped to one
  plate).

Returns a ranked companion list with evidence. **Effort** low once §0.3/§1.1 exist; it's a
thin query layer. **This is the headline demo** for the report and the website.

### 2.2 Cloned-plate (套牌车) detection — the dual problem

A **complement detector** that reuses the *same* trajectory build as MaxGrowth/embedding but
inverts it: flag a single plate whose own sightings are **physically impossible** — two
cameras far apart within an interval too short to traverse (the "impossible travel" test;
see ECNU 2018, the USPTO cloned-identifier patent, TfL's 36,794 clone fines in one year).

**Method** per plate, sort sightings by time; for consecutive `(cam_i, t_i)→(cam_j, t_j)`
flag if `dist(cam_i,cam_j)/Δt > v_max`. We lack camera coordinates, so bootstrap
`dist` from the **camera transition graph** (§3.1): pairs never seen as legal consecutive
hops, or requiring impossible implied speed given typical inter-camera travel times learned
from the data itself. **Why it belongs here** it's a high-value forensic output, it's a
natural data-cleaning *pre*-stage (drop cloned-plate sightings before mining), and it shares
all the windowing/trajectory code. **Effort** medium; Spark-parallel per plate.

### 2.3 Vehicle-role classification — filter the super-connector false positives

The convoy "super-connectors" (plate `3271142` in **1,548** convoys) are almost certainly
**buses/taxis/commercial** vehicles, not criminal convoys — they co-occur with everyone.
Classify each plate by trajectory signature (visit count, distinct cameras, periodicity,
embedding region) into transit/commercial vs. private, and **down-weight or exclude** transit
vehicles from companion scoring. Directly raises registry precision. **Effort** low–medium;
features are already computable from the trajectory build. Unsupervised first (the embedding
already separates these); a tiny hand-labeled set could calibrate a threshold.

### 2.4 Anomaly / alert ranking

On the fused registry, rank groups by *suspiciousness* rather than raw size: tight timing
(small ε spread), unusual hours, long shared routes through many cameras, coordinated
appearance/disappearance. Surfaces the few groups worth human review out of thousands —
the intelligence-led-policing payload.

---

## Tier 3 — Spatiotemporal structure & visualization

### 3.1 Camera transition graph + node2vec (unlocks the geospatial tier)

We have no camera coordinates, but we can **recover camera structure from the data**: build a
directed graph over the 977 cameras, edge `i→j` weighted by how many plates hop `i` then `j`.
This single artifact powers several tasks: (a) `dist`/travel-time priors for clone detection
(§2.2), (b) a 977-node **node2vec** embedding (tiny graph, trivial per `ML_Approaches.md §3.1`)
giving each camera a spatial vector → an approximate map *without* GPS, (c) corridor layout.
**Effort** low; one Spark shuffle for the edge list, node2vec on 977 nodes is instant.

### 3.2 Convoy-corridor & OD-flow analysis (consumes MaxGrowth routes)

MaxGrowth routes are **directed** (`11->227->557`), so they carry origin→destination flow
that FP-Growth's unordered sets cannot. Aggregate patterns by route to find the **corridors
groups travel as units**, and the busiest group OD pairs. Lay them out with §3.1's camera
embedding. **Effort** low (extends §0.1); this is the unique value of having the sequence
detector at all.

### 3.3 Results dashboard — feed the existing site real data

`pages/` already has the cyberpunk scrollytelling shell (`PLAN.md §4`) with **placeholder**
results. Wire it to real outputs: the fused fleet network graph (§1.1), the convoy-corridor
map (§3.2), the embedding 2-D projection (§0.3), the seed-vehicle query demo (§2.1), and the
clone-detection hits (§2.2). Turns the deck into a live artifact. **Effort** medium (front-end
glue + a small JSON export step from each table).

### 3.4 Temporal dynamics — fleet formation & dissolution over 31 days

Generalize §0.4 across all detectors: track when each fused fleet first/last appears, its
activity calendar, and split/merge events across the month. Distinguishes persistent
organized fleets from transient coincidences and gives the report a time axis.

---

## Recommended build order (payoff ÷ effort, this cluster)

| # | Task | Tier | Engine | Effort | Why this slot |
|---|---|---|---|---|---|
| 1 | **`analyze_patterns.py`** (§0.1) | 0 | CPU | S | 223 k patterns are blind today; unblocks every MaxGrowth-based task |
| 2 | **Pattern→fleet graph** (§0.2) | 0 | CPU | S | second independent fleet labelling for fusion |
| 3 | **Unified registry + consensus** (§1.1) | 1 | CPU | M | keystone; everything downstream queries it |
| 4 | **Vehicle-role filter** (§2.3) | 2 | CPU | S–M | removes the bus/taxi false positives that pollute #3 |
| 5 | **Seed-vehicle query** (§2.1) | 2 | CPU/GPU | S | the canonical use case + headline demo |
| 6 | **Camera transition graph + node2vec** (§3.1) | 3 | Spark | S | unlocks clone detection & geospatial without GPS |
| 7 | **Clone-plate (套牌车) detection** (§2.2) | 2 | Spark | M | high-value forensic output + data-cleaning pre-stage |
| 8 | **Corridor / OD analysis** (§3.2) | 3 | CPU | S | unique payoff of the directed-route detector |
| 9 | **Embedding ANN + UMAP** (§0.3) | 0 | GPU | M | reusable similarity service; powers #5 and the dashboard |
| 10 | **Dashboard wiring** (§3.3) | 3 | web | M | makes all of the above visible |

Items 1–4 and 8 are **pure CPU on the already-mined small outputs** — they need no cluster
time and no GPU, so they ship first. 6–7 are Spark passes over the raw CSV. 5/9/10 layer the
GPU embedding asset and the front-end on top.

**Bottom line:** the highest-leverage gap is that **MaxGrowth's 223 k patterns are
unconsumed** — fix that (§0.1/§0.2), then **fuse all three detectors into one scored registry
(§1.1)**, and the forensic applications (§2.1 seed query, §2.2 clone detection) and the
dashboard fall out cheaply on top.

---

## Sources

Forensic / intelligence-led convoy analysis (the downstream use case):

- *Multi-vehicle convoy analysis based on ANPR data*, Homayounfar & Ho. https://www.researchgate.net/publication/271473438_Multi-vehicle_convoy_analysis_based_on_ANPR_data
- *Forensic Vehicle Convoy Analysis Using ANPR Data*. https://www.researchgate.net/publication/320324839_Forensic_Vehicle_Convoy_Analysis_Using_ANPR_Data
- POLARBEAR — *Pattern of Life ANPR Behaviour Extraction Analysis and Recognition*, University of Surrey. https://www.surrey.ac.uk/research-projects/polarbear-pattern-life-automatic-number-plate-recognition-behaviour-extraction-analysis-and
- i2 Group — *Leveraging ANPR for intelligence-led policing*. https://i2group.com/articles/leveraging-anpr-for-intelligence-led-policing

Cloned-plate (套牌车) detection — the dual problem:

- *Automatic identification of cloned vehicle identifiers*, USPTO 10,255,514. https://image-ppubs.uspto.gov/dirsearch-public/print/downloadPdf/10255514
- *基于卡口监测数据流的套牌车检测*, 华东师范大学学报 (ECNU), 2018. https://xblk.ecnu.edu.cn/CN/10.3969/j.issn.1000-5641.2018.02.007
- *Ghost plates / ANPR cloning* (TfL fine statistics), ITN Business. https://business.itn.co.uk/ghost-plates-how-motorists-are-exploiting-anpr-vulnerabilities-and-the-tech-fighting-back/

Trajectory-similarity search / embedding downstream:

- *VeTraSS: Vehicle Trajectory Similarity Search Through Graph Modeling and Representation Learning*. https://arxiv.org/pdf/2404.08021
- *Vehicle Trajectory Similarity: Models, Methods, and Applications*, ACM Computing Surveys 53(5). https://dl.acm.org/doi/10.1145/3406096
- *Vehicle Trajectory Data Processing, Analytics, and Applications: A Survey*, ACM Computing Surveys. https://dl.acm.org/doi/10.1145/3715902

Internal: `doc/Methods.md`, `doc/ML_Approaches.md` (esp. §4 graph, §8 GPU+HPC), `doc/MaxGrowth.md`,
`doc/Embeddings.md`, `scripts/graph_communities.py`, `scripts/analyze_convoys.py`,
`src/embed/cluster_confirm.py`.
</content>
</invoke>
