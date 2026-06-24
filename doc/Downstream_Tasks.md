# Downstream Tasks — Recommendations on Convoys, Routes & Clusters

> The car-mining pipeline now has **three detectors** that each turn 275.9 M raw ANPR
> sightings `(plate, camera, timestamp)` — 977 cameras, 31 days — into *groups of plates that
> move together*. This document is the **recommendation half**: what to **build on top** of
> those groups. Every task below is grounded in the *actual* output files verified on disk
> and cross-checked against the ANPR / trajectory-mining literature.
>
> Companion to: `doc/ThreeMethods.md` (the meta-walkthrough), `doc/Methods.md` (DB
> pattern-mining lineage), `doc/MaxGrowth.md`, `doc/Embeddings.md`, `doc/ML_Approaches.md`,
> and the earlier `doc/Downstream.md` (which this file updates — see §0.1 for what changed).
> Compiled June 2026.

---

## 0. Where the three detectors actually stand today (verified on disk)

Everything below consumes one of these three outputs. Know their exact shape first.

| Detector | Code | Output (verified on disk) | Schema | Scale today | Downstream state |
|---|---|---|---|---|---|
| **FP-Growth — Convoys** | `src/job/convoy_fpgrowth.py` | `data/output/convoy_fpg_31/part-*.csv` | `plates, k, count` | **5,447** convoys · **783** distinct plates · counts ∈ [50, 628] | ✅ analyzer + Louvain graph |
| **MaxGrowth — Routes** | `src/max_growth/` | `data/output/max_growth_31/patterns.csv` | `route, n_cameras, members, n_plates` | **223,184** patterns · **19,620** routes · **87,047** plates | ⚠️ analyzer built (§0.3); graph (§0.4) still open |
| **Embedding — Clusters** | `src/embed/` | `data/output/embed_31/vectors/{embeddings.npy, plates.npy}` + `fleets.csv` | `(3.83 M, 256)` float + aligned plates; `fleets.csv` = `cluster_id, n_plates, …, confirmed, members` | **3,830,566 × 256** vectors · **12,709** clusters (**4,041 confirmed**) | ✅ `fleets.csv` + Stage-4 ANN/UMAP (§0.5) |

**Layout convention.** New downstream work lives under **`scripts/downstream/<task>/*.py`** with
results written to **`data/downstream/<task>/`** — one folder per task (`analyze_patterns`,
`pattern_graph`, `embedding_assets`, …). The original per-detector analyzers
(`scripts/analyze_convoys.py`, `scripts/graph_communities.py`) predate this and still write next
to their inputs under `data/output/`.

What *already* consumes them:

- **Convoys** → `scripts/analyze_convoys.py` (`summary.txt` + 3 charts) **and**
  `scripts/graph_communities.py` (Louvain → `data/output/communities/fleets.csv`, **132 fleets**
  over **783** plates). This is the only detector with a finished downstream.
- **Routes (MaxGrowth)** → `scripts/downstream/analyze_patterns/` (summary + 4 charts — §0.3) and
  `scripts/downstream/pattern_graph/` (396 route-evidence fleets — §0.4). Both **built**; outputs
  under `data/downstream/`. Now feeds fusion (§1.1) alongside the convoy and embedding fleets.
- **Clusters** → `src/embed/cluster_confirm.py` produced `embed_31/fleets.csv` (12,709 clusters,
  4,041 `confirmed=True` via co-occurrence). But that is the *last GPU stage of the embedding
  pipeline itself*, not a cross-cutting consumer. The **3.83 M × 256 matrix is a reusable asset
  that nothing else queries yet.**

### 0.1 What changed since `doc/Downstream.md`

Two gaps that the earlier doc listed as open are now **partly closed**, which moves the frontier up:

- **Embedding `fleets.csv` now exists at full-31 scale** (the old doc said only `sequences/` and
  `vectors_*/` existed). The cluster labelling is available for fusion *today*.
- **An OCR-confusion candidate detector shipped**: `scripts/ocr_canonicalize.py` →
  `data/output/ocr_candidates.csv` (18 edit-distance-1 plate pairs, e.g. `17210840 / 17210845`).
  ⚠️ This catches **OCR misreads**, which is a *different* failure mode from **physical plate
  cloning** (套牌车) — see §2.5. Both belong in a pre-mining data-cleaning stage.

Two constraints that still shape every geospatial idea below: **cameras are bare integer IDs
`0…976`** — the repo has **no camera lat/long table** — and the dataset is a 31-day window with
per-day / per-week slices available (`day1`, `week1`, full `31`).

### 0.2 The core observation — why three detectors is the asset

The three detectors disagree *by construction*, and that disagreement is the opportunity:

| Detector | What "together" means | Evidence type | Failure mode |
|---|---|---|---|
| FP-Growth | shared the **same (camera, ε-bucket)** cell many times | co-occurrence count | counts crowds; buses/taxis dominate |
| MaxGrowth | **co-traversed a directed route** of ≥ 3 cameras, ε-close at each stop | route + members | sensitive to gap `d`; 223 k near-dup patterns |
| Embedding | **similar route shape** in 256-D space (+ raw co-occurrence confirm) | cosine proximity | similar ≠ simultaneous (commuters a week apart) |

A group flagged by **two or three** of these — across three *different metric spaces* — is far
stronger evidence than any single detector's top-K. **Fusion (§1.1) is the keystone.** Per-detector
completion (§0.x) feeds it; the forensic applications (§2) and visualization (§3) sit on top.

---

## Tier 0 — Per-detector completion (cheap, do-first, all CPU)

### 0.3 `analyze_patterns.py` — the missing MaxGrowth analyzer  ✅ BUILT

`scripts/downstream/analyze_patterns/analyze_patterns.py` → `data/downstream/analyze_patterns/
{summary.txt, route_length_distribution.png, members_distribution.png, top_corridors.png,
super_connectors.png}`. Ranks patterns by a
**significance score** `n_plates × n_cameras` (longer shared routes beat bigger crowds at fewer
hops), histograms route length and group size, lists the **busiest directed corridors** (group by
`route` → distinct plates / member-sets), and the super-connector plates. Mirrors
`analyze_convoys.py` so the report sections line up. Pure pandas, runs in seconds.

**What the full run showed (and why fusion matters):**

- **87,047** distinct plates appear in patterns — vs. only **783** in convoys. MaxGrowth and
  FP-Growth surface *almost disjoint populations*, exactly the disagreement §0.2 predicts.
- The convoy super-connectors (`3271142`, `16972992`, …) are **absent** from MaxGrowth's
  super-connectors (`490759`, `535928`, `1507211`, …), which are also far less concentrated
  (max 172 patterns vs. convoys' 1,548). Different metric → different "central" plates.
- **0 exact-duplicate (route, members) rows** — the maximality filter is clean; the near-
  duplication is overlapping member-sets / prefix routes, which the corridor view (group-by
  route) collapses. Top corridor `434->404->238`: **9,296** distinct plates over 5,324 patterns.
- Corridors **chain** (`330->268->478`, `268->478->481`, `330->478->481` are segments of one
  longer path) — a direct lead-in to corridor stitching / OD analysis (§3.2).

### 0.4 Pattern → fleet graph  ✅ BUILT

`scripts/downstream/pattern_graph/pattern_graph.py` → `data/downstream/pattern_graph/
{communities.csv, fleets.csv}`. Unrolls each pattern's `members` into `C(n,2)` edges weighted
by `n_cameras` (route length = edge strength), **pre-aggregated** (Σ weight + pattern count per
pair) so recurring co-traversals rise, then Louvain (networkx, weighted). Optional `--min-weight`
prunes one-off weak links. Produces the **second, independent fleet labelling** from route
evidence — a direct input to fusion (§1.1).

**Full run:** 223,184 patterns → **412,481** distinct pairs (the pre-aggregation kept the graph
small — many patterns are 2-member) over **87,047** plates → **396 communities** in ~19 s on CPU.
Top communities are large (4,059 / 3,987 / 2,557 plates) — route-sharing is looser than convoy
co-occurrence (convoy fleets top out ~108), which is exactly why fusion (§1.1) is needed to find
the *tight* groups all detectors agree on.

### 0.5 Embedding asset services — ANN index + 2-D projection  ✅ BUILT

The `(3.83 M, 256)` matrix is the most reusable artifact in the repo. Two thin services,
both shipped in `src/embed/` (Stage 4; launcher `src/slurm/gpu_assets.sbatch`):

- **ANN "find vehicles like this one"** — `scripts/downstream/embedding_assets/ann_index.py`.
  A **cuVS CAGRA** index over `embeddings.npy` on GPU (sklearn brute-cosine fallback on CPU; the
  repo deliberately ships cuVS, not faiss — see `requirements-gpu.txt`). Query a plate → its k
  nearest-route neighbours; writes `data/downstream/embedding_assets/ann/neighbors.csv`. This is
  the literal *trajectory-similarity-search* primitive and the engine behind the seed query (§2.1).
- **UMAP / t-SNE → 2-D** — `scripts/downstream/embedding_assets/project_2d.py`. **cuML UMAP** on
  GPU (umap-learn / t-SNE / PCA fallback on CPU), subsampled to `--max-points`, colored by
  embedding fleet → `data/downstream/embedding_assets/projection/{projection.csv, projection.png}`
  for the dashboard scatter (§3.3).

**Status** verified CPU-side on a 4 k-plate slice: ANN returns sensible cos≈0.93–0.95 neighbours;
the 2-D projection renders the top fleets as cleanly separated islands (the embedding does
separate route-shapes). GPU path (cuVS/cuML) runs on a `.venv-gpu` node.

> ⚠️ **Canonical vectors dir is `embed_31/vectors/`, not `vectors_0610/`.** Discovered while
> building this: `vectors_0610/` and `vectors_0609/` are **100 % NaN** (failed Stage-2 runs).
> `fleets.csv` was built from `vectors/` (clean, 0 non-finite rows). Both Stage-4 scripts now
> drop non-finite rows and refuse an all-NaN matrix. The §0 table's `vectors_0610/` reference
> is superseded by `vectors/`.

**Effort** low–medium; standard RAPIDS calls, gated only on a GPU node.

### 0.6 Convoy temporal-stability pass

Re-run convoy mining per day (slices exist) and track **which pairs/fleets persist day-to-day**.
A stable 31-day convoy is a real fleet; a one-day co-occurrence is noise. Cheap and directly
raises precision.

---

## Tier 1 — Cross-detector fusion (the keystone)

### 1.1 Unified companion-group registry + consensus scoring  ✅ BUILT — the central task

`scripts/downstream/consensus_registry/consensus_registry.py` →
`data/downstream/consensus_registry/{registry.csv, summary.txt}`. Fuses the three fleet
labellings into one ranked, deduplicated, confidence-scored table — the product every Tier 2–3
task queries instead of three raw CSVs.

**Inputs** the three fleet labellings: `communities/fleets.csv` (FP-Growth→Louvain),
`pattern_graph/fleets.csv` (MaxGrowth→Louvain, §0.4), `embed_31/fleets.csv` (embedding clusters).

**Method (composite-label consensus — no pairwise edge blow-up).** Each plate gets a label per
detector = its fleet id there. Plates sharing the **same label tuple under ≥2 detectors** form a
consensus block (group by the full triple → 3-of-3; by each detector pair → 2-of-3); keep blocks
of ≥2 plates, dedup identical member-sets (highest agreement wins).
`consensus_score = detectors_agreeing + mean(log-min-max-normalized native strengths)` — agreement
dominates, native strength (convoy edge weight / pattern route weight / embedding co-occurrence
windows) breaks ties.

**Full run:** **1,167 fused groups** over the 10,749 plates seen by ≥2 detectors — **19 with
full 3-of-3 agreement**, 1,148 at 2-of-3. The top group is a **51-plate 3-of-3** companion group
(convoy fleet 0 ∩ pattern fleet 0 ∩ confirmed embedding fleet 11477).

> **The validation hook flipped — and that's the headline result.** The original hypothesis was
> that the always-together super-connectors `{3271142, 16972992, 16970573, 511670, …}` (each in
> **1,548** convoys) would top the registry at 3-of-3. They **don't appear in the registry at
> all**: `16972992/511670/16970573/3542309` are in **no** pattern or embedding fleet, and
> `3271142` is convoy+pattern only with no co-grouped partner. So fusion **automatically
> down-ranks the FP-Growth super-connectors** — exactly the buses/taxis §2.3 wanted filtered.
> They were never a real companion group; they're co-occurrence artifacts that only one metric
> space (cell co-occurrence) ever endorsed. This is the clearest possible demonstration of *why*
> cross-detector consensus beats any single detector's top-K — and it means much of §2.3's
> false-positive filtering falls out of fusion for free.

**Why it's the keystone** every Tier 2–3 task queries *this* table. **Effort** medium, pure CPU.
Not yet added (cheap follow-ups): per-group `first_seen/last_seen/n_active_days` (needs the §0.6
temporal pass) and Jaccard-overlap matching as an alternative to exact label-tuple blocks.

---

## Tier 2 — Applications (the "so what")

This is what ANPR convoy systems are actually *for* — grounded in the policing/forensics
literature.

### 2.1 Seed-vehicle convoy query — "who travels with plate X?"  ✅ BUILT — the canonical use case

`scripts/downstream/seed_query/seed_query.py` → `data/downstream/seed_query/companions_<plate>.csv`.
The real-world workflow is **not** "dump all fleets" — it's *"here is one known vehicle, find its
companions."* Serves it three independent ways and **merges by consensus** (a companion surfaced
by more routes ranks higher — `n_sources` is the primary sort):

1. **registry** (default, instant) — X's fused group(s) in the registry (§1.1); companions =
   co-members with agreement level + score.
2. **ANN** (`--embed-dir`) — nearest-route neighbours of X (§0.5), reusing `embedding_assets/
   ann_index.py` (cuVS GPU / sklearn CPU).
3. **co-occurrence** (`--raw-csv`) — streams X's `(camera, ε-bucket)` keys then counts plates
   sharing them (the `cluster_confirm.py` machinery scoped to one plate).

**Verified, all three routes:** for seed `393966`, the registry returns its 3-of-3 co-members
first; adding ANN promotes the four it also confirms to `n_sources=2` with cos≈0.93–0.95; the
co-occurrence route ran on `day1.csv` (200 MB, two passes) in **~11 s**, surfacing 864 same-day
co-movers ranked by shared buckets (top: `1817603` ×38). **This is the headline demo.**

### 2.2 Vehicle-role classification — filter the super-connector false positives

The convoy "super-connectors" — plate **`3271142` in 1,548 convoys**, plus `16972992`,
`16970573`, `511670` (all 1,548), `3542309` (1,533), `18150551` (1,495) — are almost certainly
**buses / taxis / commercial** vehicles, not criminal convoys: they co-occur with everyone.
Classify each plate by trajectory signature (visit count, distinct cameras, periodicity,
embedding region) into transit/commercial vs. private and **down-weight or exclude** transit
vehicles from companion scoring. Directly raises registry precision. Unsupervised first (the
embedding already separates these); a tiny hand-labeled set can calibrate a threshold. **Effort**
low–medium.

### 2.3 Anomaly / alert ranking

On the fused registry, rank groups by *suspiciousness* rather than raw size: tight timing (small
ε spread), unusual hours, long shared routes through many cameras, coordinated
appearance/disappearance. Surfaces the few groups worth human review out of thousands — the
intelligence-led-policing payload.

### 2.4 Cloned-plate (套牌车) detection — the forensic dual problem  ✅ BUILT

`scripts/downstream/clone_detection/clone_detection.py` → `data/downstream/clone_detection/
{clones.csv, summary.txt}`. The **complement detector**: instead of plates that move *together*,
it flags a single plate whose own sightings are **physically impossible** (the **"impossible
travel" test**). With no GPS, "distance" comes from the camera transition graph (§3.1): build a
directed graph weighted by learned median hop times, take **all-pairs shortest-path travel time**
`SP(i,j)`, and flag a consecutive sighting pair when `Δt < SP(i,j)·tol` (too fast for the route)
or graph-distant cameras seen within `--teleport-dt` (teleport). Plates are ranked by impossible-
transition count (gated by `--min-hits`, since one hit may be a timestamp glitch). It also
cross-annotates `ocr_candidates.csv` to separate **physical clones from OCR ghosts**.

**Full 31-day run** (`31.csv`, ~6 min): **235.9 M** inter-camera transitions → **221,132
impossible** → **8,668 clone-candidate plates** (≥3 hits, 0.04 % of the population). The strongest
is plate `505029` — **108** impossible transitions across the month (e.g. `79→306` in **1 s** vs
an 82 s minimum). Only **8** of the 8,668 also appear on the OCR-confusion list, confirming clones
and OCR ghosts are near-disjoint populations (as `ThreeMethods.md` argued). High-value forensic
output **and** a natural data-cleaning *pre*-stage (drop cloned-plate sightings before mining).

> **Caveat (a tuning lead, not a bug):** a few high-*ratio* candidates flag the same edge
> `785→786` (~18 s vs sp=103 s). 785/786 are almost certainly a **co-located camera pair** with no
> direct edge in the graph, so the shortest path detours and over-estimates the true hop time →
> false positives. Fix by adding observed-minimum (not just median) hop times to `edges.csv`, or
> by merging co-located cameras. The high-*count*, low-ratio candidates (e.g. `505029` across many
> distinct cameras) are unaffected and are the robust clones.

### 2.5 OCR-canonicalization pre-stage (extend what already exists)

`ocr_candidates.csv` already lists edit-distance-1 plate pairs (OCR ghosts, **distinct from §2.4
physical clones**). Promote this into a **canonicalization pass that runs before FP-Growth**, so
OCR-adjacent pairs like `(16970573, 16972992)` don't masquerade as convoys. `ThreeMethods.md`
notes the real-fleet-vs-OCR-ghost split was ~50/50 in a manual sample — so this materially affects
convoy precision. **Effort** low; the candidate generator exists, needs a merge/resolve policy.

---

## Tier 3 — Spatiotemporal structure & visualization

### 3.1 Camera transition graph + node2vec — unlocks geospatial without GPS  ✅ BUILT

`scripts/downstream/camera_graph/camera_graph.py` → `data/downstream/camera_graph/{edges.csv,
camera_vectors.csv, camera_map.csv, camera_map.png, summary.txt}`. Builds the directed graph
(edge `i→j` weighted by per-plate consecutive hops within `--max-dt`), embeds it with **node2vec**,
and projects to a 2-D map. node2vec is implemented dependency-free as DeepWalk-as-matrix-
factorization (weighted random walks → windowed co-occurrence → PPMI → truncated SVD; polars +
sklearn only — gensim/node2vec aren't installed). `edges.csv` carries **per-edge `median_dt`/
`mean_dt`** — the travel-time priors §2.4/#8 needs.

**Full 31-day run (~1m42s, polars):** **853 cameras, 410,697 directed edges, 120.6 M hops**,
64-D node2vec embedding. Two independent cross-checks validate it: (a) the busiest transitions
(`237→376`, `181→45`, `391→375→372`, `68→181`, `316→73`, `631→35`) **reproduce MaxGrowth's top
corridors** — the platoon routes *are* the high-traffic camera hops; (b) the PCA-2D map shows
coherent corridor/cluster structure, a plausible city layout recovered without any GPS.

### 3.2 Convoy-corridor & OD-flow analysis — the unique payoff of MaxGrowth  ✅ BUILT

`scripts/downstream/corridor_od/corridor_od.py` → `data/downstream/corridor_od/{od_flows.csv,
corridor_segments.csv, corridor_map.png, summary.txt}`. MaxGrowth routes are **directed**
(`11->227->557`), so unlike FP-Growth's unordered sets they carry origin→destination flow.
Aggregates the 223 k patterns into **group OD pairs** (route first→last camera) and **corridor
segments** (consecutive hops), both weighted by distinct plates, then draws the top segments as
flow arrows on the recovered camera map (#7, §3.1).

**Full run:** 223,184 patterns → **11,168 OD pairs, 9,205 corridor segments**. Busiest OD
`434→238` (9,300 plates); busiest segment `434→404` (14,724 plates). The OD view exposes structure
the segment/route views can't: `391→372` is served by **6 distinct routes** and `348→45` by **7**
— groups take multiple paths between the same endpoints. The segments cross-validate #7's
all-traffic camera graph (same corridors surface), and `corridor_map.png` shows the convoy flow
concentrating in a central hub with feeders — a convoy-flow map recovered with **no GPS**.

### 3.3 Results dashboard — feed the existing site real data

`pages/` already has the cyberpunk scrollytelling shell with **placeholder** results. Wire it to
real outputs: the fused fleet network (§1.1), the convoy-corridor map (§3.2), the embedding 2-D
projection (§0.5), the seed-vehicle demo (§2.1), and the clone-detection hits (§2.4). **Effort**
medium (front-end glue + a small JSON export step per table).

### 3.4 Temporal dynamics — fleet formation & dissolution over 31 days

Generalize §0.6 across all detectors: track when each fused fleet first/last appears, its activity
calendar, and split/merge events across the month. Distinguishes persistent organized fleets from
transient coincidences and gives the report a time axis.

---

## Recommended build order (payoff ÷ effort, this cluster)

| # | Task | Tier | Engine | Effort | Why this slot |
|---|---|---|---|---|---|
| 1 | **`analyze_patterns.py`** (§0.3) ✅ | 0 | CPU | S | 223 k patterns are blind today; unblocks every MaxGrowth task — **built** |
| 2 | **Pattern → fleet graph** (§0.4) ✅ | 0 | CPU | S | second independent fleet labelling for fusion — **built** |
| 3 | **Unified registry + consensus** (§1.1) ✅ | 1 | CPU | M | keystone; everything downstream queries it — **built** |
| 4 | **Vehicle-role filter** (§2.2) | 2 | CPU | S–M | removes bus/taxi false positives that pollute #3 |
| 5 | **Seed-vehicle query** (§2.1) ✅ | 2 | CPU/GPU | S | the canonical use case + headline demo — **built** |
| 6 | **OCR-canonicalization pre-stage** (§2.5) ◐ | 2 | CPU | S | candidate generator `scripts/ocr_canonicalize.py` + `ocr_candidates.csv` already exist; only the canonicalize-before-mining merge step remains |
| 7 | **Camera transition graph + node2vec** (§3.1) ✅ | 3 | polars | S | unlocks clone detection & geospatial without GPS — **built** (853 nodes, 410 k edges) |
| 8 | **Clone-plate (套牌车) detection** (§2.4) ✅ | 2 | polars+nx | M | high-value forensic output + data-cleaning pre-stage — **built** (8,668 candidates on 31d) |
| 9 | **Corridor / OD analysis** (§3.2) ✅ | 3 | CPU | S | unique payoff of the directed-route detector — **built** (11 k OD pairs, flow map) |
| 10 | **Embedding ANN + UMAP** (§0.5) ✅ | 0 | GPU | M | reusable similarity service; powers #5 and the dashboard — **built** |
| 11 | **Dashboard wiring** (§3.3) | 3 | web | M | makes all of the above visible — *deferred* |

Items 1–6 and 9 are **pure CPU on the already-mined small outputs** — no cluster time, no GPU —
so they ship first. 7–8 are Spark passes over the raw CSV. 5/10/11 layer the GPU embedding asset
and the front-end on top.

**Bottom line:** the highest-leverage gap is that **MaxGrowth's 223 k patterns are unconsumed** —
fix that (§0.3/§0.4), then **fuse all three detectors into one scored registry (§1.1)**, and the
forensic applications (§2.1 seed query, §2.4 clone detection) and the dashboard fall out cheaply
on top.

**Progress (this session):** ✅ #1 analyze_patterns · ✅ #2 pattern_graph · ✅ #3 consensus
registry · ✅ #5 seed-vehicle query · ✅ #7 camera_graph · ✅ #8 clone_detection · ✅ #9 corridor_od
· ✅ #10 embedding ANN+UMAP — **8 of the 10 build-order tasks shipped**, all under
`scripts/downstream/<task>/` → `data/downstream/<task>/`. #4 (vehicle-role filter) is **dropped as
redundant** — the registry already auto-filters the bus/taxi super-connectors (§1.1). #6
OCR-canonicalization is ◐ (the `scripts/ocr_canonicalize.py` generator + `ocr_candidates.csv`
predate this session and are intact — only the canonicalize-before-mining merge remains). Only
**#11 dashboard** (deferred) is left — wire `pages/` to the JSON exports of the tables above.

---

## Sources

Forensic / intelligence-led convoy analysis (the downstream use case):

- i2 Group — *Leveraging ANPR for intelligence-led policing*. https://i2group.com/articles/leveraging-anpr-for-intelligence-led-policing
- *Multi-vehicle convoy analysis based on ANPR data*, Homayounfar & Ho. https://www.researchgate.net/publication/271473438_Multi-vehicle_convoy_analysis_based_on_ANPR_data
- POLARBEAR — *Pattern of Life ANPR Behaviour Extraction Analysis and Recognition*, University of Surrey. https://www.surrey.ac.uk/research-projects/polarbear-pattern-life-automatic-number-plate-recognition-behaviour-extraction-analysis-and

Cloned-plate (套牌车) detection — the dual problem:

- *Automatic identification of cloned vehicle identifiers*, USPTO 10,255,514. https://image-ppubs.uspto.gov/dirsearch-public/print/downloadPdf/10255514
- *Method and apparatus for identifying a cloned number plate*, GB2448780A. https://patents.google.com/patent/GB2448780A/en
- *How AI ANPR Detects & Prevents License Plate Cloning* (impossible-travel test). https://www.titanhz.com/blog/how-ai-anpr-detects-prevents-license-plate-cloning.aspx

Trajectory similarity search / embedding downstream:

- *VeTraSS: Vehicle Trajectory Similarity Search Through Graph Modeling and Representation Learning*. https://arxiv.org/pdf/2404.08021
- *UniTE: A Survey and Unified Pipeline for Pre-training Spatiotemporal Trajectory Embeddings*. https://arxiv.org/pdf/2407.12550
- *Real-time taxi spatial anomaly detection based on vehicle trajectory prediction*, ScienceDirect. https://www.sciencedirect.com/science/article/abs/pii/S2214367X23001497

Co-movement pattern mining (flock / convoy / swarm taxonomy) & geospatial recovery:

- *An efficient distributed co-movement pattern detection framework for streaming trajectory*, KAIS 2025. https://link.springer.com/article/10.1007/s10115-025-02369-7
- *Co-Movement Pattern Mining from Videos*, VLDB. https://dl.acm.org/doi/10.14778/3632093.3632119
- *Can We Predict Your Next Move Without Breaking Your Privacy?* (node2vec / next-location). https://arxiv.org/html/2507.08843v1

Internal: `doc/ThreeMethods.md`, `doc/Methods.md`, `doc/MaxGrowth.md`, `doc/Embeddings.md`,
`doc/ML_Approaches.md`, `doc/Downstream.md`, `scripts/analyze_convoys.py`,
`scripts/graph_communities.py`, `scripts/ocr_canonicalize.py`, `src/embed/cluster_confirm.py`.
</content>
</invoke>
