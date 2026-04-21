# Accompanying Cars Project — Refinement Plan

> **Context:** 275.9M records (plate, location, timestamp) over 31 days, ~22M unique plates, ~977 locations.
> Current stack: PySpark + Hadoop YARN on 5 nodes × 8-core / 8GB RAM (constrained).
> Cluster is k8s-managed; no root to start containers — use **Slurm** to schedule compute nodes.

---

## Part 1 — Slurm-Based Dynamic Hadoop/Spark Cluster on K8s Nodes

### 1.1 Architecture Overview

```
Login Node (head)
  └─ sbatch cluster_launch.sh
       ├─ Node 0 (Slurm rank 0) → HDFS NameNode + Spark Master
       └─ Node 1..N              → HDFS DataNode + Spark Worker
```

Since we cannot start k8s containers ourselves, we request nodes via Slurm, and inside the Slurm job we bootstrap Hadoop + Spark processes as regular user-space daemons. No root needed — all processes run as the submitting user.

### 1.2 Key Insight: Slurm Gives You Hostnames Dynamically

Slurm provides `$SLURM_NODELIST` and `$SLURM_JOB_NODELIST`. We parse these to dynamically:

1. Elect Node 0 as NameNode/Master
2. Write `workers` / `slaves` files at runtime
3. Patch `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml` with the real hostnames

### 1.3 Slurm Batch Script — `cluster_launch.sh`

```bash
#!/bin/bash
#SBATCH --job-name=hadoop-spark-cluster
#SBATCH --nodes=5
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=18G                  # leave 2G for OS
#SBATCH --time=04:00:00
#SBATCH --partition=compute
#SBATCH --output=logs/cluster-%j.out

# ── 1. Resolve node list ────────────────────────────────────────────────────
NODES=($(scontrol show hostnames "$SLURM_NODELIST"))
MASTER=${NODES[0]}
WORKERS=("${NODES[@]:1}")

echo "[INFO] Master: $MASTER"
echo "[INFO] Workers: ${WORKERS[*]}"

# ── 2. Write dynamic Hadoop config ─────────────────────────────────────────
HADOOP_CONF=$HADOOP_HOME/etc/hadoop

cat > $HADOOP_CONF/core-site.xml <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${MASTER}:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/tmp/hadoop-${USER}-${SLURM_JOB_ID}</value>
  </property>
</configuration>
EOF

cat > $HADOOP_CONF/hdfs-site.xml <<EOF
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/tmp/hadoop-${USER}-${SLURM_JOB_ID}/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/tmp/hadoop-${USER}-${SLURM_JOB_ID}/datanode</value>
  </property>
  <property>
    <name>dfs.blocksize</name>
    <value>134217728</value>   <!-- 128 MB -->
  </property>
</configuration>
EOF

cat > $HADOOP_CONF/yarn-site.xml <<EOF
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>${MASTER}</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>16384</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>8</value>
  </property>
</configuration>
EOF

# Write workers file for Spark
printf "%s\n" "${WORKERS[@]}" > $SPARK_HOME/conf/workers

# ── 3. Distribute config to all nodes ──────────────────────────────────────
for NODE in "${WORKERS[@]}"; do
  scp -q $HADOOP_CONF/core-site.xml  ${NODE}:$HADOOP_CONF/
  scp -q $HADOOP_CONF/hdfs-site.xml  ${NODE}:$HADOOP_CONF/
  scp -q $HADOOP_CONF/yarn-site.xml  ${NODE}:$HADOOP_CONF/
  scp -q $SPARK_HOME/conf/workers    ${NODE}:$SPARK_HOME/conf/
done

# ── 4. Format & start HDFS (first run only) ────────────────────────────────
ssh $MASTER "hdfs namenode -format -force -nonInteractive 2>/dev/null"
ssh $MASTER "$HADOOP_HOME/sbin/start-dfs.sh"

# Start DataNodes on workers via srun
srun --nodes=${#WORKERS[@]} --nodelist=$(IFS=,; echo "${WORKERS[*]}") \
     hdfs datanode &

# ── 5. Start Spark standalone cluster ──────────────────────────────────────
ssh $MASTER "$SPARK_HOME/sbin/start-master.sh"

SPARK_MASTER_URL="spark://${MASTER}:7077"
for NODE in "${WORKERS[@]}"; do
  ssh $NODE "$SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER_URL \
    --cores 7 --memory 14G" &
done
wait

echo "[INFO] Cluster ready. Master: $SPARK_MASTER_URL"
echo "[INFO] HDFS: hdfs://${MASTER}:9000"

# ── 6. Run the Spark job ────────────────────────────────────────────────────
spark-submit \
  --master $SPARK_MASTER_URL \
  --deploy-mode client \
  --driver-memory 8G \
  --executor-memory 12G \
  --executor-cores 6 \
  --num-executors 4 \
  --conf spark.sql.shuffle.partitions=500 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  $PROJECT_DIR/accompany_waterfall.py \
    hdfs://${MASTER}:9000/data/31.csv \
    hdfs://${MASTER}:9000/results/output \
    300 10 5 0.25

# ── 7. Teardown ─────────────────────────────────────────────────────────────
ssh $MASTER "$SPARK_HOME/sbin/stop-all.sh"
ssh $MASTER "$HADOOP_HOME/sbin/stop-dfs.sh"
```

### 1.4 Code Modifications Required


| File                     | Change                                                                                            |
| ------------------------ | ------------------------------------------------------------------------------------------------- |
| `accompany_waterfall.py` | Replace hardcoded HDFS URIs with env vars (`HDFS_NAMENODE`, `SPARK_MASTER`)                       |
| `accompany_FPG.py`       | Same; also read `partition_hours` from env                                                        |
| `data_analyzing.py`      | Parameterize input/output paths via env                                                           |
| All scripts              | Replace `HDFSLogger` URI construction — use `os.environ.get("HDFS_URI", "hdfs://localhost:9000")` |


Add to every script's `main()`:

```python
import os
HDFS_URI   = os.environ.get("HDFS_URI",   "hdfs://localhost:9000")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
```

### 1.5 Data Pre-staging

Before submitting the Slurm job, upload data once to shared storage (NFS/HDFS):

```bash
# From login node (after a test cluster run establishes HDFS)
hdfs dfs -mkdir -p /data /results
hdfs dfs -put ./31.csv /data/
```

For subsequent jobs, skip the upload — data persists across Slurm runs if HDFS namenode data dir is on shared NFS.

---

## Part 2 — Algorithm Enhancements

### 2.1 Current Bottleneck Analysis

From data analysis results:

- Window 300s → avg **30,902** records/bucket globally; hotspot location 31 → **23,595** in one bucket
- Self-join on a bucket of N cars = **O(N²)** pairs → hotspot generates ~556M pair candidates per bucket
- `explode(array(base_bucket, base_bucket+1))` doubles dataset size before the join

### 2.2 Recommended Algorithm Stack (Layered Approach)

#### Level 1 (Implement First): Sort-Based Plane Sweep — replaces `mine_range()`

Eliminates the self-join entirely. For each (location, time_chunk) group, sort by timestamp and use a two-pointer sliding window. Complexity drops from **O(N²)** to **O(N log N + N·k)** where k = average neighbors per car in the window.

```python
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, StringType, LongType

PAIR_SCHEMA = StructType([
    StructField("p1", StringType()),
    StructField("p2", StringType()),
    StructField("mid_t", LongType()),
])

def make_plane_sweep_udf(time_window: int, chunk_start: int, chunk_end: int):
    @pandas_udf(PAIR_SCHEMA, PandasUDFType.GROUPED_MAP)
    def plane_sweep(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf = pdf.sort_values("timestamp").reset_index(drop=True)
        timestamps = pdf["timestamp"].values
        plates     = pdf["plate"].values
        n = len(timestamps)
        results = []

        left = 0
        for right in range(n):
            # Slide left pointer: drop records outside window
            while timestamps[right] - timestamps[left] > time_window:
                left += 1
            # All records in [left, right-1] are within window of right
            for i in range(left, right):
                t1, t2 = timestamps[i], timestamps[right]
                mid = (int(t1) + int(t2)) // 2
                # Ownership: only count if midpoint belongs to this chunk
                if chunk_start <= mid < chunk_end:
                    p1, p2 = sorted([plates[i], plates[right]])
                    results.append((p1, p2, mid))

        if not results:
            return pd.DataFrame(columns=["p1", "p2", "mid_t"])
        return pd.DataFrame(results, columns=["p1", "p2", "mid_t"])

    return plane_sweep

# Usage: replaces mine_range()
def mine_range_sweep(df, time_window, chunk_start, chunk_end):
    udf = make_plane_sweep_udf(time_window, chunk_start, chunk_end)
    return df.groupBy("location").apply(udf).drop("mid_t")
```

**Why better than current approach:**

- Zero data duplication (no `explode`)
- No shuffle-heavy self-join
- For sparse locations (most of 977 locations), inner loop rarely executes
- Dense hotspot (location 31): still O(N·k) not O(N²) — k bounded by how many cars fit in a time window

#### Level 2 (After Level 1): MinHash Pre-filter — prune impossible pairs globally

Before the main loop, build a car trajectory fingerprint and use MinHash to identify car pairs that share ≥ T% of locations. Only pairs that pass the similarity filter enter the time-window analysis. Eliminates 95%+ of stranger-pairs.

```python
from pyspark.ml.feature import MinHashLSH
from pyspark.ml.linalg import Vectors, SparseVector

# 1. Build location-set per plate (as sparse vector over location vocabulary)
loc_vocab = df.select("location").distinct().rdd.map(lambda r: r[0]) \
               .zipWithIndex().collectAsMap()   # {loc_id -> index}
VOC_SIZE = len(loc_vocab)

def to_sparse(locs):
    idxs = sorted([loc_vocab[l] for l in set(locs) if l in loc_vocab])
    return Vectors.sparse(VOC_SIZE, idxs, [1.0]*len(idxs))

plate_vecs = df.groupBy("plate").agg(F.collect_set("location").alias("locs")) \
               .rdd.map(lambda r: (r["plate"], to_sparse(r["locs"]))) \
               .toDF(["plate", "features"])

# 2. MinHash LSH — find approximate Jaccard neighbors
mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=20)
model = mh.fit(plate_vecs)

# approxSimilarityJoin finds pairs with Jaccard >= threshold efficiently
candidate_pairs = model.approxSimilarityJoin(
    plate_vecs, plate_vecs,
    threshold=0.05,   # tune: cars sharing <5% locations can't be convoys
    distCol="jaccardDist"
).filter(F.col("datasetA.plate") < F.col("datasetB.plate")) \
 .select(
     F.col("datasetA.plate").alias("p1"),
     F.col("datasetB.plate").alias("p2")
 )

# 3. Broadcast candidate pairs as a filter into the main mining loop
candidate_pairs_bc = spark.sparkContext.broadcast(
    set(candidate_pairs.rdd.map(lambda r: (r.p1, r.p2)).collect())
)
```

#### Level 3 (Optional / Research): GraphFrames — Convoy Group Discovery

After pairs are found, promote pairs to multi-car convoy groups:

```python
from graphframes import GraphFrame

# Build graph from confirmed pairs
vertices = result_df.select(F.col("p1").alias("id")).union(
           result_df.select(F.col("p2").alias("id"))).distinct()
edges = result_df.select(
    F.col("p1").alias("src"),
    F.col("p2").alias("dst"),
    F.col("total_count").alias("weight")
)
g = GraphFrame(vertices, edges)

# Find all connected convoy groups
components = g.connectedComponents()
# Find dense cliques (actual convoys) — use label propagation
communities = g.labelPropagation(maxIter=5)
```

### 2.3 FP-Growth Assessment

`accompany_FPG.py` already uses Spark MLlib FP-Growth — this is strong for discovering **convoy groups of size ≥ 3** but has a known weakness: the dual-grid (Grid A + Grid B) approach for boundary handling doubles compute time. Once the Plane Sweep (Level 1) is implemented, consider replacing FP-Growth's bucket construction with the sweep-based transaction builder too.

### 2.4 Recommended Implementation Order

```
Phase A: Plane Sweep UDF → replaces mine_range() in accompany_waterfall.py
Phase B: MinHash pre-filter → add as Phase 0 before the time loop
Phase C: GraphFrames convoy grouping → post-processing on final output
Phase D: Integrate into FPG script (replace inner bucket logic)
```

---

## Part 3 — Scaling to Larger Machines

### 3.1 Hardware Tiers


| Tier    | Nodes | Cores/Node | RAM/Node | Total Cores | Total RAM |
| ------- | ----- | ---------- | -------- | ----------- | --------- |
| Current | 5     | 8          | 8 GB     | 40          | 40 GB     |
| Mid     | 3–5   | 8          | 20 GB    | 24–40       | 60–100 GB |
| Large   | 5     | 55         | 550 GB   | 275         | 2.75 TB   |


### 3.2 Spark Config by Tier

**General formula (per executor):**

- `executor-cores` = 4–6 (avoid >6 — HDFS throughput degrades per executor at higher core counts)
- `executor-memory` = `(node_RAM - OS_overhead - driver_overhead) / executors_per_node`
- `executor-memory-overhead` = max(384MB, 0.1 × executor_memory)
- `shuffle_partitions` = total_cores × 2–4

#### Mid Tier (5 × 8-core / 20GB)

```bash
spark-submit \
  --num-executors 10 \          # 2 per node
  --executor-cores 4 \
  --executor-memory 8G \
  --driver-memory 4G \
  --conf spark.executor.memoryOverhead=1G \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryoserializer.buffer.max=512m \
  --conf spark.network.timeout=600s \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true
```

#### Large Tier (5 × 55-core / 550GB)

```bash
spark-submit \
  --num-executors 45 \          # 9 executors per node, 6 cores each
  --executor-cores 6 \
  --executor-memory 55G \       # (550 - 5G OS - 5G driver) / 9 ≈ 60G, cap at 55G
  --driver-memory 20G \
  --conf spark.executor.memoryOverhead=6G \
  --conf spark.sql.shuffle.partitions=2000 \
  --conf spark.default.parallelism=2000 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryoserializer.buffer.max=1g \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.hadoop.dfs.replication=3 \
  --conf spark.network.timeout=800s \
  --conf spark.rpc.message.maxSize=512
```

### 3.3 Code Changes for Scale


| Change                                                    | Why                                                           |
| --------------------------------------------------------- | ------------------------------------------------------------- |
| Remove `.coalesce(1)` from all outputs                    | Single file write is a bottleneck; use `.repartition(N)`      |
| Increase `partition_hours` from 0.25 to 1–2h              | At large scale, chunking too fine wastes scheduling overhead  |
| Use off-heap memory (`spark.memory.offHeap.enabled=true`) | At 550GB RAM, off-heap avoids GC pressure on huge objects     |
| Switch HDFS block size to 256MB                           | Fewer blocks = less NameNode metadata pressure                |
| Increase `dfs.replication` to 3                           | 5-node large cluster has capacity; better fault tolerance     |
| Partition parquet by `(day_id, location_bucket)`          | Enables partition pruning on both dimensions in chunk loading |


### 3.4 HDFS Config for Large Tier

```xml
<!-- hdfs-site.xml -->
<property><name>dfs.blocksize</name><value>268435456</value></property>  <!-- 256MB -->
<property><name>dfs.replication</name><value>3</value></property>
<property><name>dfs.namenode.handler.count</name><value>100</value></property>
<property><name>dfs.datanode.handler.count</name><value>30</value></property>
<property><name>dfs.datanode.max.transfer.threads</name><value>8192</value></property>
```

### 3.5 Slurm Script Adjustments per Tier

Only `#SBATCH` header changes are needed — the dynamic config injection handles the rest:

```bash
# Mid tier
#SBATCH --nodes=5
#SBATCH --cpus-per-task=8
#SBATCH --mem=18G

# Large tier
#SBATCH --nodes=5
#SBATCH --cpus-per-task=55
#SBATCH --mem=520G
```

---

## Part 4 — Cyberpunk Scrollytelling Project Website

### 4.1 Concept

A single-page **scrollytelling** website that functions as an interactive project presentation — replacing a static PPT. Each scroll section reveals one narrative beat of the project: problem → data → algorithm → results → scale.

**Aesthetic:** Dark background (`#0a0a0f`), neon cyan/magenta/green accent colors (`#00f5ff`, `#ff00ff`, `#39ff14`), glitch effects, scanline overlays, monospace fonts (JetBrains Mono / Share Tech Mono), animated data visualizations.

### 4.2 Page Structure (Scroll Sections)

```
Section 0 — HERO / TITLE
  Full-screen. Animated particle network (cars as nodes). Glitching title text.
  "275,893,209 Records. 22M Vehicles. 977 Locations. 31 Days."
  Subtitle pulses in neon: "Find the shadows."

Section 1 — THE PROBLEM
  Split layout: left = scrolling raw data terminal output (fake typewriter),
  right = city map with blinking checkpoint nodes.
  Text: Definition of "accompanying vehicle". Stakes.

Section 2 — DATA ANALYSIS
  Animated bar chart (Chart.js / D3): bucket density across window sizes.
  Highlight the hotspot (location 31, 23,595 cars). Neon glow on peak bar.
  Scanline effect over the chart.

Section 3 — THE ALGORITHM EVOLUTION
  Horizontal timeline (scroll = advance timeline):
    V1 (Scala brute force) → V2 (Iterative + boundary fix) →
    V3 (Waterfall aggregation) → V4 (FP-Growth) → V5 (Plane Sweep + MinHash)
  Each version: code snippet with syntax highlight, complexity label,
  animated "memory usage" gauge dropping.

Section 4 — RESULTS
  Dark table with neon borders: top convoy pairs + counts.
  Animated counter ticking up to final counts.
  Optional: small graph visualization of convoy network (GraphFrames output).

Section 5 — INFRASTRUCTURE
  Isometric diagram of the cluster (SVG animated): nodes light up as Slurm
  allocates them. Spark job progress bar animates.
  Labels show HDFS NameNode, DataNodes, Spark Master/Workers.

Section 6 — SCALING VISION
  Three hardware tier cards flip in on scroll (CSS 3D flip).
  Each card shows: specs, Spark config, estimated speedup.
  Neon particle burst on "5 × 55-core / 550GB".

Section 7 — OUTRO / CTA
  Full-screen glitch animation. Final stat counter.
  Links: GitHub repo, report PDF, contact.
```

### 4.3 Tech Stack


| Concern           | Choice                                        | Reason                                                                     |
| ----------------- | --------------------------------------------- | -------------------------------------------------------------------------- |
| Scroll engine     | **GSAP ScrollTrigger**                        | Industry standard; precise pin/scrub control                               |
| Fullpage sections | **fullPage.js** or GSAP alone                 | fullPage.js for snap-scroll; GSAP for animated transitions within sections |
| Charts            | **D3.js**                                     | Data-driven; fits the raw data narrative                                   |
| Particles         | **tsParticles** or Canvas API                 | Lightweight car/network animation                                          |
| Fonts             | JetBrains Mono (code), Orbitron (headers)     | Cyberpunk feel                                                             |
| Glitch effects    | Pure CSS `@keyframes` clip-path animation     | No library needed                                                          |
| Framework         | **Vanilla HTML/CSS/JS** or **Astro** (static) | Avoids React overhead; fast load                                           |
| Hosting           | GitHub Pages / Vercel                         | Free, zero-config                                                          |


### 4.4 Key CSS Design Tokens

```css
:root {
  --bg:        #0a0a0f;
  --bg-panel:  #0f0f1a;
  --neon-cyan: #00f5ff;
  --neon-pink: #ff00ff;
  --neon-green:#39ff14;
  --text:      #c0c0d0;
  --font-mono: 'JetBrains Mono', monospace;
  --font-head: 'Orbitron', sans-serif;
  --glow-cyan: 0 0 10px #00f5ff, 0 0 40px #00f5ff44;
  --glow-pink: 0 0 10px #ff00ff, 0 0 40px #ff00ff44;
  --scanline:  repeating-linear-gradient(
                 transparent 0px, transparent 2px,
                 rgba(0,0,0,0.15) 2px, rgba(0,0,0,0.15) 4px);
}
```

### 4.5 Glitch Text Effect (CSS)

```css
.glitch {
  position: relative;
  animation: glitch-main 3s infinite;
}
.glitch::before, .glitch::after {
  content: attr(data-text);
  position: absolute; top: 0; left: 0;
  width: 100%;
}
.glitch::before {
  color: var(--neon-cyan);
  animation: glitch-top 3s infinite;
  clip-path: polygon(0 0, 100% 0, 100% 35%, 0 35%);
}
.glitch::after {
  color: var(--neon-pink);
  animation: glitch-bottom 3s infinite;
  clip-path: polygon(0 65%, 100% 65%, 100% 100%, 0 100%);
}
@keyframes glitch-top {
  0%, 90%, 100% { transform: none; }
  92% { transform: translate(-3px, -2px); }
  96% { transform: translate(3px, 2px); }
}
```

### 4.6 GSAP ScrollTrigger Skeleton

```javascript
import { gsap } from "gsap";
import { ScrollTrigger } from "gsap/ScrollTrigger";
gsap.registerPlugin(ScrollTrigger);

// Pin Section 2 (Data Analysis) while chart animates
ScrollTrigger.create({
  trigger: "#section-data",
  start: "top top",
  end: "+=600",
  pin: true,
  onEnter: () => animateBarChart(),
});

// Algorithm timeline: scrub horizontal scroll
gsap.to("#algo-timeline", {
  x: () => -(document.querySelector("#algo-timeline").scrollWidth - window.innerWidth),
  ease: "none",
  scrollTrigger: {
    trigger: "#section-algo",
    start: "top top",
    end: "+=2000",
    pin: true,
    scrub: 1,
  }
});

// Node cluster lights up on scroll
gsap.from(".cluster-node", {
  opacity: 0, scale: 0, stagger: 0.15,
  scrollTrigger: {
    trigger: "#section-infra",
    start: "top 70%",
  }
});
```

### 4.7 Implementation Milestones


| Milestone | Deliverable                                                   |
| --------- | ------------------------------------------------------------- |
| M1        | Static HTML scaffold with all 8 sections, fonts, color tokens |
| M2        | GSAP ScrollTrigger: pin + scrub working on all sections       |
| M3        | D3 animated charts (bucket density + convoy count)            |
| M4        | Glitch hero, particle network, scanline overlays              |
| M5        | Algorithm timeline with code snippets + complexity labels     |
| M6        | Cluster isometric SVG animation                               |
| M7        | Mobile responsiveness + performance audit                     |
| M8        | Deploy to GitHub Pages                                        |


### 4.8 GitHub Pages Deployment

> **Goal:** Publish `pages/` as a live preview website — not a docs site, so the root of the
> GitHub Pages URL should open the cyberpunk scrollytelling page directly.

#### Step 1 — Prepare the repository

The site lives in `pages/` inside this repo. GitHub Pages can serve either:
- The repo **root** `/`
- A `/docs` folder
- Any branch root via a dedicated **`gh-pages` branch** ← best choice here, keeps the big data
  code and the website cleanly separated

#### Step 2 — Create a `gh-pages` orphan branch containing only `pages/`

```bash
# From the repo root on the main branch
git checkout --orphan gh-pages

# Remove everything tracked by git (does not delete your working files)
git rm -rf .

# Copy only the website files into the branch root
cp -r pages/* .
rm -rf pages/          # remove the now-empty subdirectory

# Commit
git add index.html css/ js/ assets/
git commit -m "deploy: cyberpunk preview website"

# Push
git push origin gh-pages --force

# Return to main
git checkout main
```

> **Why orphan?** An orphan branch has no shared history with `main`, so GitHub Pages
> doesn't inherit the Hadoop code, reports, or large binary files (`.docx`, `.pptx`, `.pdf`).
> The deployment is a clean, minimal snapshot of just the website.

#### Step 3 — Enable GitHub Pages in repo settings

1. Go to **Settings → Pages** in your GitHub repo
2. Under **Source**, choose **Branch: `gh-pages`** / **folder: `/ (root)`**
3. Click **Save**

GitHub will build and publish within ~60 seconds. Your URL will be:
```
https://<your-username>.github.io/<repo-name>/
```

#### Step 4 — Update `index.html` before deploy (self-contained links)

The two links in the Outro section currently point to relative paths inside the repo
(`../base/reports.md`, `../base/PJ_AccompanyCars/codes/...`) which won't exist on
`gh-pages`. Before deploying, update them to point to GitHub blob URLs or simply
remove/disable them:

```html
<!-- Replace in Section 7 Outro before deploying -->
<a class="cyber-btn" href="https://github.com/<user>/<repo>/blob/main/base/reports.md"
   target="_blank">VIEW REPORT</a>
<a class="cyber-btn secondary"
   href="https://github.com/<user>/<repo>/tree/main/base/PJ_AccompanyCars/codes"
   target="_blank">VIEW CODE</a>
```

#### Step 5 — Automate future deploys with GitHub Actions (optional but recommended)

Create `.github/workflows/deploy.yml` on the `main` branch so every push to `main`
automatically re-deploys the site:

```yaml
name: Deploy Preview Website

on:
  push:
    branches: [main]
    paths:
      - 'pages/**'   # only trigger when website files change

permissions:
  contents: write

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Deploy pages/ to gh-pages
        uses: peaceiris/actions-gh-pages@v4
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: ./pages        # source: pages/ folder on main
          publish_branch: gh-pages    # target: gh-pages branch root
          force_orphan: true          # keep gh-pages history clean
          commit_message: "deploy: ${{ github.event.head_commit.message }}"
```

With this in place, your workflow becomes:
```
edit pages/* on main  →  git push  →  GitHub Action runs  →  site live in ~60s
```

#### Step 6 — Custom domain (optional)

If you want `convoy.yourdomain.com` instead of the `github.io` URL:

1. Add a `CNAME` file to `pages/` containing just your domain:
   ```
   convoy.yourdomain.com
   ```
2. Add a CNAME DNS record at your registrar pointing to `<user>.github.io`
3. In GitHub Pages settings, enter the custom domain and enable **Enforce HTTPS**

The Actions workflow above will copy `CNAME` automatically via `publish_dir: ./pages`.

---

## Summary & Execution Order

```
Sprint 1 (Infrastructure):
  ├─ Write cluster_launch.sh Slurm script
  ├─ Modify Python scripts to accept env-var paths
  └─ Test on 2-node Slurm allocation first

Sprint 2 (Algorithm):
  ├─ Implement Plane Sweep UDF (replace mine_range)
  ├─ Benchmark vs current waterfall on same data
  └─ Add MinHash pre-filter if hotspot OOM persists

Sprint 3 (Scaling):
  ├─ Request mid-tier nodes (3–5 × 8-core/20GB)
  ├─ Tune Spark config per table in §3.2
  └─ Validate on full 275M dataset

Sprint 4 (Website):
  ├─ M1–M3: Structure + ScrollTrigger + Charts
  ├─ M4–M6: Visual effects + animations
  └─ M7–M8: Polish + deploy
```

