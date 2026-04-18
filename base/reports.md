## Accompanying Cars Searching Project Report



### Purpose



The primary objective of this project is to design and implement a distributed big data algorithm to identify "accompanying vehicles" (or convoy patterns) from massive-scale traffic surveillance data. An "accompanying relationship" is defined as two unique vehicles appearing at the **same location** within a specific **time window** (e.g., 300 seconds) with a frequency exceeding a set **threshold** (e.g., $\ge$ 10 times).

Beyond the theoretical algorithm, the critical engineering purpose of this project is to overcome the challenges of processing **High-Volume Data (275.9 Million records)** on **Resource-Constrained Infrastructure**. Specifically, the project aims to:

1.  **Mining Spatiotemporal Co-occurrences:**
    To accurately detect vehicle pairs that travel together by transforming raw checkpoints (`plate`, `location`, `time`) into trajectory pairs. This involves solving the "Boundary Effect" problem using spatiotemporal sliding windows to ensure no valid pairs are missed at the edges of time partitions.

2.  **Optimizing for Limited Memory (OOM Prevention):**
    Standard Big Data approaches (like global joins) fail on our cluster infrastructure (5 nodes, 8GB RAM per node) due to memory overflows. A core purpose of this implementation is to develop a **Streamed Partitioning Strategy**, processing data in micro-batches (e.g., 0.25 hours) to ensure memory usage remains stable regardless of the total dataset size.

3.  **Managing Storage Constraints (Disk Space & Inode Limits):**
    With a total cluster storage limit of ~200GB, naive intermediate storage strategies lead to disk exhaustion and "Small File Problems" (millions of inodes). The project aims to implement an **Iterative Aggregation and Compaction Pipeline**. This involves a "Compute-Aggregate-Delete" cycle that compresses intermediate results on the fly, ensuring the disk usage never exceeds the cluster's capacity during the intermediate shuffle phases.

In summary, the purpose is to prove that by using advanced optimization techniques—such as **Dynamic Partitioning**, **Broadcast Joins**, and **Iterative Compaction**—it is possible to process Terabyte-scale logic on Gigabyte-scale hardware using Apache Spark.



### Progress



#### Basic Logic



**Find and count every car (plate) pair that appears at the same location in a time window**, which means in each time window, we have to traverse all the car pairs and and count them. Quite straight-forward, isn't it?



> Note that in all the versions below, there are two main lines of improvement: **the codes to be run** and **the command to launch**. I'll pick codes changes as the main line and attach command changes to certain steps if there is one.

#### Version 1

- Codes

```scala
package com.acars.project

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object AccompanyingCars {

  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      System.err.println("Usage: AccompanyingCars <inputPath> <outputPath> <timeWindowSeconds> <frequencyThreshold> <minPlateCount>")
      sys.exit(1)
    }
    val inputPath          = args(0)
    val outputPath         = args(1)
    val timeWindowSeconds  = args(2).toInt
    val frequencyThreshold = args(3).toInt
    val minPlateCount      = args(4).toInt

    val spark = SparkSession.builder()
      .appName("Accompanying Cars Mining")
      .getOrCreate()
    import spark.implicits._

    // --- 1. Load and Pre-process Data ---
    val schema = StructType(Array(
      StructField("plate", StringType, nullable = false),
      StructField("location", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false)
    ))

    val rawDf: DataFrame = spark.read
      .schema(schema)
      .option("header", "false")
      .csv(inputPath)

    // Filter out infrequent cars to significantly reduce the search space.
    val plateCounts = rawDf.groupBy("plate").count()
    val frequentPlates = plateCounts.filter($"count" >= minPlateCount).select("plate")
    val df = rawDf.join(frequentPlates, "plate")
    df.cache()

    // --- 2. Create Spatiotemporal Bins ---
    // Group records by location and a discrete time window ("bucket").
    val dfWithBucket = df.withColumn("time_bucket", floor($"timestamp" / timeWindowSeconds))

    // --- 3. Aggregate Cars within Bins ---
    // Collect all cars that appeared in the same place at roughly the same time.
    val groupedByWindow = dfWithBucket
      .groupBy("location", "time_bucket")
      .agg(collect_list("plate").alias("plates"))
      .filter(size($"plates") > 1) // A car cannot accompany itself.

    // --- 4. Generate Candidate Pairs (More Idiomatic Scala) ---
    // Define a function that takes a list of plates and returns all unique pairs.
    // Using `combinations` is more concise and functional than nested loops.
    val generatePairs = (plates: Seq[String]) => {
      plates.distinct.sorted
        .combinations(2)
        .map { case Seq(p1, p2) => (p1, p2) }
        .toSeq
    }

    // Register the function as a UDF.
    val generatePairsUDF = udf(generatePairs)

    // Apply the UDF and explode the resulting array of pairs into rows.
    val candidatePairs = groupedByWindow
      .withColumn("pair", explode(generatePairsUDF($"plates")))
      .select($"pair._1".as("plate1"), $"pair._2".as("plate2"))

    // --- 5. Count Pair Frequency and Filter ---
    // Count how many times each pair appeared together.
    val accompanyingCounts = candidatePairs.groupBy("plate1", "plate2").count()

    // Filter for pairs that meet the minimum frequency threshold.
    val results = accompanyingCounts
      .filter($"count" >= frequencyThreshold)
      .orderBy($"count".desc)

    // --- 6. Save Results ---
    val resultCount = results.count()
    println(s"Found $resultCount accompanying car pairs. Saving results to $outputPath")

    // For a small test sample, coalesce(1) is fine to get a single output file.
    // For the full 2.7B dataset, remove .coalesce(1) to allow parallel writing.
    results
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(outputPath)

    // --- 7. Clean Up ---
    df.unpersist()
    spark.stop()
  }
}
```

- Steps
    1. **Initialization and Configuration**

       The program accepts five command-line arguments to control the execution dynamically:

       - **Input/Output Paths:** Location of source data and destination for results.
       - **Time Window (`timeWindowSeconds`):** The interval size (e.g., 300 seconds) used to group cars.
       - **Frequency Threshold (`frequencyThreshold`):** The minimum number of times two cars must be seen together to be considered "accompanying."
       - **Min Plate Count:** A pre-filtering threshold.

       A `SparkSession` is initialized, which serves as the entry point for all DataFrame operations.

    2. **Data Loading and Pre-processing**

       **Logic:**
       The code defines a strict schema (`StructType`) consisting of `plate` (String), `location` (String), and `timestamp` (Long). Enforcing a schema is more efficient than letting Spark infer it.

       **Optimization (Pre-filtering):**
       Before processing potential pairs, the algorithm performs a heuristic optimization:

       ```scala
       val plateCounts = rawDf.groupBy("plate").count()
       val frequentPlates = plateCounts.filter($"count" >= minPlateCount).select("plate")
       val df = rawDf.join(frequentPlates, "plate")
       ```

       *   **Rationale:** Vehicles that appear in the dataset only once or twice (e.g., passing through the city once) are statistically unlikely to form a "frequent" accompanying pair (which requires finding a pair $N$ times).
       *   **Effect:** By filtering these distinct low-frequency vehicles out early via an inner join, the dataset size is significantly reduced, lowering the memory and shuffle overhead for subsequent steps.

    3. **Spatiotemporal Binning**

       **Logic:**

       ```scala
       val dfWithBucket = df.withColumn("time_bucket", floor($"timestamp" / timeWindowSeconds))
       ```

       This is the core discretization step. It converts continuous timestamps into discrete "Buckets."

       *   *Example:* If the window is 300s, timestamps 0-299 become Bucket 0, and 300-599 become Bucket 1.
       *   Two cars are considered "co-located" in this version if they share the same **Location ID** and the same **Time Bucket ID**.

    4. **Aggregation**

       **Logic:**

       ```scala
       val groupedByWindow = dfWithBucket
         .groupBy("location", "time_bucket")
         .agg(collect_list("plate").alias("plates"))
         .filter(size($"plates") > 1)
       ```

       *   **Grouping:** The data is reshuffled so that all records occurring at the same place and in the same time bucket end up on the same executor.
       *   **Collection:** `collect_list` gathers all License Plates into a single array (e.g., `[CarA, CarB, CarC]`).
       *   **Filter:** Groups with only 1 car are discarded immediately, as a single car cannot form a pair.

    5. **Candidate Pair Generation (UDF)**

       **Logic:**
       The code uses a Scala User Defined Function (UDF) to transform the list of cars into pairs.

       ```scala
       val generatePairs = (plates: Seq[String]) => {
         plates.distinct.sorted
           .combinations(2)
           .map { case Seq(p1, p2) => (p1, p2) }
           .toSeq
       }
       ```

       1.  **Distinct:** Removes duplicates if the same car was captured twice in the same window (data cleaning).
       2.  **Sorted:** This is crucial. It ensures that the pair `{CarA, CarB}` is identical to `{CarB, CarA}`. By sorting alphabetically, we enforce a canonical representation.
       3.  **Combinations(2):** Generates all unique subsets of size 2. If a bucket has cars `[A, B, C]`, this generates `(A,B), (A,C), (B,C)`.

       **Explode:**
       The `explode` function is then used to flatten the list of pairs. A single row containing a list of 3 pairs becomes 3 distinct rows in the DataFrame.

    6. **Global Frequency Counting**

       **Logic:**

       ```scala
       val accompanyingCounts = candidatePairs.groupBy("plate1", "plate2").count()
       val results = accompanyingCounts.filter($"count" >= frequencyThreshold)
       ```

       At this stage, the DataFrame consists of millions of candidate pairs. The code performs a global aggregation to count how many times each specific pair (`plate1`, `plate2`) appeared across *all* locations and *all* time buckets.

       *   Pairs exceeding the `frequencyThreshold` are retained as the final result.

    7. **Output**

       The results are sorted by frequency (descending) and written to CSV.

       *   *Note on `coalesce(1)`:* The code uses `coalesce(1)` to force the result into a single CSV file. While convenient for small datasets, this can be a performance bottleneck for large data as it forces all data to a single worker node.

- Command

```bash 
spark-submit \
  --class com.fudan.project.AccompanyingCars \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 2g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 10 \
  target/AccompanyingCars-assembly-1.0.jar \
  "project/accomp_cars/data/31.csv" \
  "project/accomp_cars/results" \
  300 \
  10 \
  5
```



#### Version 2

- Codes

```scala
package com.acars.project

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path, FSDataOutputStream}
import java.io.{BufferedWriter, OutputStreamWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// --- 1. Configuration ---
case class JobConfig(
    inputPath: String,
    outputPath: String,
    timeWindow: Int,
    freqThreshold: Int,
    minPlateCount: Int,
    partitionHours: Int
)

// --- Helper: HDFS Logger ---
class HDFSLogger(spark: SparkSession, logFilePath: String) {
  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  private val path = new Path(logFilePath)

  private val os: FSDataOutputStream = fs.create(path, true)
  private val writer = new BufferedWriter(new OutputStreamWriter(os))
  private val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def log(message: String): Unit = {
    val timestamp = LocalDateTime.now().format(dtf)
    val formattedMsg = s"[$timestamp] $message"
    
    // 1. Write to HDFS File
    try {
      writer.write(formattedMsg)
      writer.newLine()
      
      writer.flush() // Flushes Java buffer to OS
      os.hflush()    // Flushes OS buffer to HDFS DataNodes (Makes it visible live)
      
    } catch {
      case e: Exception => System.err.println(s"Failed to write to log file: ${e.getMessage}")
    }
    
    // 2. Also print to Stdout
    println(formattedMsg)
  }

  def close(): Unit = {
    try {
      writer.close()
    } catch {
      case _: Exception => 
    }
  }
}

// --- 2. Data Processor ---
object DataProcessor {
  val schema = StructType(Array(
    StructField("plate", StringType, nullable = false),
    StructField("location", StringType, nullable = false),
    StructField("timestamp", LongType, nullable = false)
  ))

  def loadAndClean(spark: SparkSession, config: JobConfig, logger: HDFSLogger): DataFrame = {
    import spark.implicits._
    
    logger.log(s"[PROGRESS] Loading data from ${config.inputPath}...")
    val rawDf = spark.read.schema(schema).option("header", "false").csv(config.inputPath)

    logger.log(s"[PROGRESS] Filtering vehicles with frequency < ${config.minPlateCount}...")
    val frequentPlates = rawDf.groupBy("plate").count()
      .filter($"count" >= config.minPlateCount)
      .select("plate")

    rawDf.join(frequentPlates, "plate")
  }
}

// --- 3. Algorithm Engine ---
object AlgorithmEngine {

  def mineRange(df: DataFrame, window: Int, chunkStart: Long, chunkEnd: Long): DataFrame = {
    import df.sparkSession.implicits._

    // A. Boundary Handling
    val dfBuckets = df
      .withColumn("base_bucket", floor($"timestamp" / window))
      .withColumn("time_bucket", explode(array($"base_bucket", $"base_bucket" + 1)))
      .drop("base_bucket")

    // B. Self-Join
    val dfA = dfBuckets.as("a")
    val dfB = dfBuckets.as("b")

    val candidates = dfA.join(dfB, 
      $"a.location" === $"b.location" &&
      $"a.time_bucket" === $"b.time_bucket" &&
      $"a.plate" < $"b.plate"
    )
    .select(
      $"a.plate".as("p1"), $"a.timestamp".as("t1"),
      $"b.plate".as("p2"), $"b.timestamp".as("t2"),
      $"a.time_bucket".as("bucket")
    )

    // C. Strict Filter & Deduplication
    candidates.filter { row =>
      val t1 = row.getLong(1)
      val t2 = row.getLong(3)
      val bucket = row.getLong(4)

      if (math.abs(t1 - t2) > window) false
      else {
        val midPoint = (t1 + t2) / 2.0
        val ownerBucket = math.floor(midPoint / window).toLong
        
        if (ownerBucket != bucket) {
          false 
        } else {
          // Check Chunk Ownership
          if (midPoint >= chunkStart && midPoint < chunkEnd) true else false
        }
      }
    }
    .select("p1", "p2")
  }
}

// --- 4. Main Application Controller ---
object AccompanyingCars {

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println("Usage: <input> <output> <window> <freq> <minPlate> <partitionHours>")
      sys.exit(1)
    }

    val config = JobConfig(
      inputPath = args(0),
      outputPath = args(1),
      timeWindow = args(2).toInt,
      freqThreshold = args(3).toInt,
      minPlateCount = args(4).toInt,
      partitionHours = args(5).toInt
    )

    val spark = SparkSession.builder()
      .appName("Accompanying Cars Iterative")
      .config("spark.sql.shuffle.partitions", "2000") 
      .getOrCreate()
    import spark.implicits._

    val tempOutputPath = config.outputPath + "_temp"
    val logFilePath    = config.outputPath + "_progress.log"

    val logger = new HDFSLogger(spark, logFilePath)
    logger.log(s"[PROGRESS] Job Started. Log file: $logFilePath")
    logger.log(s"[CONFIG] Window=${config.timeWindow}s, Freq=${config.freqThreshold}, Partition=${config.partitionHours}h")

    try {
      // 1. Load Data
      val fullDf = DataProcessor.loadAndClean(spark, config, logger)
      fullDf.cache() 

      // 2. Determine Time Range
      logger.log("[PROGRESS] Analyzing dataset time range...")
      val timeStats = fullDf.agg(min("timestamp"), max("timestamp")).head()
      
      if (timeStats.isNullAt(0)) {
        logger.log("[ERROR] Dataset empty after filtering. Exiting.")
        spark.stop(); sys.exit(0)
      }
      val (minTime, maxTime) = (timeStats.getLong(0), timeStats.getLong(1))
      
      val partitionSeconds = config.partitionHours * 3600L
      val totalDurationHours = (maxTime - minTime) / 3600.0
      logger.log(f"[PROGRESS] Time Range: $minTime to $maxTime ($totalDurationHours%.2f hours).")

      // 3. Iterative Processing Loop
      var currentStart = minTime
      var chunkId = 0
      val totalChunks = math.ceil((maxTime - minTime).toDouble / partitionSeconds).toInt

      while (currentStart <= maxTime) {
        val currentEnd = currentStart + partitionSeconds
        
        logger.log(s"[PROGRESS] Processing Chunk ${chunkId + 1}/$totalChunks: Range [$currentStart - $currentEnd]...")

        val buffer = config.timeWindow
        val rangeDf = fullDf.filter(
          $"timestamp" >= (currentStart - buffer) && 
          $"timestamp" < (currentEnd + buffer)
        )

        val chunkPairs = AlgorithmEngine.mineRange(rangeDf, config.timeWindow, currentStart, currentEnd)
        
        val chunkCounts = chunkPairs.groupBy("p1", "p2").count()
        chunkCounts.write.mode("append").parquet(tempOutputPath)

        logger.log(s"[PROGRESS] Chunk ${chunkId + 1} completed and saved.")
        
        currentStart = currentEnd
        chunkId += 1
      }

      // 4. Global Aggregation
      logger.log("[PROGRESS] All chunks processed. Starting Global Aggregation...")
      
      val allIntermediate = spark.read.parquet(tempOutputPath)
      
      val finalResults = allIntermediate
        .groupBy("p1", "p2")
        .agg(sum("count").alias("total_count"))
        .filter($"total_count" >= config.freqThreshold)
        .orderBy($"total_count".desc)

      // 5. Save Final Result
      logger.log(s"[PROGRESS] Saving final results to ${config.outputPath}...")
      
      finalResults
        .coalesce(1) 
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(config.outputPath)

      logger.log("[SUCCESS] Job Finished Successfully.")

    } catch {
      case e: Exception => 
        logger.log(s"[ERROR] Job Failed: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      logger.log("[PROGRESS] Cleaning up temporary files...")
      try {
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        fs.delete(new Path(tempOutputPath), true)
      } catch {
        case e: Exception => logger.log(s"[WARNING] Cleanup failed: ${e.getMessage}")
      }
      logger.close()
      spark.stop()
    }
  }
}
```

- Refinements

    1. **Modular Object-Oriented Design**

       To improve maintainability, testability, and code clarity, the monolithic logic from Version 1 was refactored into distinct, cohesive components:

       *   **`JobConfig`:** A dedicated case class to encapsulate all runtime parameters (paths, thresholds, time windows). This separates configuration management from business logic.
       *   **`DataProcessor`:** A singleton object responsible solely for data ingestion, schema enforcement, and initial filtering. This isolation allows for easier changes to data sources without affecting the core algorithm.
       *   **`AlgorithmEngine`:** Encapsulates the complex logic for candidate generation and deduplication. It exposes a clean API (`mineRange`) that takes a DataFrame and returns accompanying pairs, abstracting away the low-level Spark transformations.
       *   **`AccompanyingCars` (Controller):** The main entry point that orchestrates the workflow, manages the Spark Session, handles iterative execution loops, and ensures resource cleanup.

    2. **Solving the Boundary Effect (Overlapping Windows)**

       Version 1 suffered from a "hard binning" limitation where vehicles appearing near the edge of a time bucket (e.g., at 299s and 301s) would be missed. Version 2 solves this using a **Sliding Window / Overlapping Bucket** strategy combined with a **Midpoint Ownership** deduplication rule.

       **Logic:**

       - **Data Duplication:** Each vehicle record is mapped to *two* time buckets: its calculated bucket ($B$) and the next bucket ($B+1$). This ensures that any two vehicles within the `timeWindow` distance are guaranteed to coexist in at least one bucket.

          ```scala
          .withColumn("time_bucket", explode(array($"base_bucket", $"base_bucket" + 1)))
          ```

       - **Deduplication Strategy:** Since data is duplicated, valid pairs might be detected twice. To prevent double counting, the algorithm calculates the midpoint time of every candidate pair ($T_{mid} = (t1 + t2) / 2$). A pair is counted *if and only if* the bucket being processed "owns" that midpoint.

          *   *Rule:* `if floor(midPoint / window) == currentBucket`

    3. **Iterative Chunk Processing (Memory Safety)**

       To prevent Out-Of-Memory (OOM) errors caused by massive shuffles on limited hardware, Version 2 implements an iterative processing model. Instead of processing the entire 31-day dataset at once, the data is sliced into smaller temporal chunks (configurable via `partitionHours`, e.g., 0.25 hours).

       **Workflow:**

       1.  **Time Range Analysis:** The application first scans the dataset to determine the global start and end timestamps.
       2.  **Sequential Execution:** A loop iterates through the dataset in increments of `partitionHours`.
       3.  **Strict Partitioning:** For each iteration, only data strictly falling within the current time range (plus a necessary buffer for boundary pairs) is loaded and processed.
           *   *Global Ownership Check:* Similar to the bucket logic, a pair is only saved if its midpoint falls strictly within the current *Chunk's* time range. This allows seamless stitching of results from adjacent chunks without duplicates.
       4.  **Incremental Persistence:** Intermediate results from each chunk are written to HDFS immediately. This clears the Spark execution graph and frees up memory for the next chunk.
       5.  **Final Aggregation:** After all chunks are processed, a lightweight global aggregation step sums the counts from all intermediate files to produce the final output.

    4. **Robust Logging and Observability**

       A custom `HDFSLogger` class was introduced to provide real-time visibility into the job's progress.

       *   It writes timestamped logs to both the YARN stdout (for historical debugging) and a dedicated file in HDFS.
       *   The `hflush()` method is used to ensure log entries are immediately visible to users (e.g., via `tail -f`) even while the application is still running, which is critical for monitoring long-running batch jobs.

- Command

```bash
spark-submit \
  --class com.acars.project.AccompanyingCars \
  --master yarn \
  --deploy-mode client \
  --driver-memory 4G \
  --num-executors 5 \
  --executor-memory 4G \
  --executor-cores 2 \
  --conf spark.network.timeout=600s \
  target/scala-2.12/AccompanyingCars_2.12-1.0.jar \
  "project/accomp_cars/data/31.csv" \
  "project/accomp_cars/results" \
  300 \
  10 \
  5 \
  0.25
```



  

#### Version 3

- Codes

```python
# accompany_waterfall.py

import sys
import math
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min, max, floor, explode, array, lit, sum as _sum, abs as _abs, broadcast
)
from pyspark.sql.types import StructType, StructField, StringType, LongType

# ==========================================
# HELPER CLASSES
# ==========================================

class HDFSLogger:
    def __init__(self, spark, log_file_path):
        self.sc = spark.sparkContext
        self.fs = self.sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            self.sc._jvm.java.net.URI(log_file_path),
            self.sc._jsc.hadoopConfiguration()
        )
        self.path = self.sc._jvm.org.apache.hadoop.fs.Path(log_file_path)
        self.os = self.fs.create(self.path, True)
    
    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_msg = f"[{timestamp}] {message}"
        print(formatted_msg)
        try:
            data = (formatted_msg + "\n").encode('utf-8')
            self.os.write(data)
            self.os.hflush()
        except Exception as e:
            print(f"!! Logger Error: {e}")

    def close(self):
        try: self.os.close()
        except: pass

class WaterfallAggregator:
    """
    Manages the hierarchical aggregation (Raw -> L1 -> L2) and deletion of source files.
    """
    def __init__(self, spark, base_dir, logger, l1_threshold=10, l2_threshold=10):
        self.spark = spark
        self.fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jsc.hadoopConfiguration()
        )
        self.base_dir = base_dir.rstrip("/")
        self.logger = logger
        self.l1_threshold = l1_threshold
        self.l2_threshold = l2_threshold
        
        # Tracking lists for file paths
        self.pending_raw = []  # List of raw chunk paths
        self.pending_l1 = []   # List of L1 aggregated paths
        self.final_l2_paths = [] # List of L2 paths (to be read at very end)
        
        # Create directories
        self.dirs = {
            "raw": f"{self.base_dir}/_temp_raw",
            "l1": f"{self.base_dir}/_temp_l1",
            "l2": f"{self.base_dir}/_temp_l2"
        }

    def add_raw_chunk(self, df_chunk, chunk_id):
        """Saves a raw mining result and checks if L1 compaction is needed."""
        path = f"{self.dirs['raw']}/chunk_{chunk_id}"
        
        # Write Raw
        df_chunk.write.mode("overwrite").parquet(path)
        self.pending_raw.append(path)
        
        # Check L1 Trigger
        if len(self.pending_raw) >= self.l1_threshold:
            self._compact_level_1()

    def _compact_level_1(self):
        """Aggregates pending RAW files into one L1 file."""
        batch_id = str(uuid.uuid4())[:8]
        output_path = f"{self.dirs['l1']}/batch_{batch_id}"
        
        self.logger.log(f"[COMPACT L1] Aggregating {len(self.pending_raw)} raw files...")
        
        # Load, Agg, Write
        self._aggregate_and_write(self.pending_raw, output_path)
        
        # Cleanup Raw
        self._delete_paths(self.pending_raw)
        self.pending_raw = [] # Reset list
        
        # Track L1
        self.pending_l1.append(output_path)
        
        # Check L2 Trigger
        if len(self.pending_l1) >= self.l2_threshold:
            self._compact_level_2()

    def _compact_level_2(self):
        """Aggregates pending L1 files into one L2 file."""
        batch_id = str(uuid.uuid4())[:8]
        output_path = f"{self.dirs['l2']}/batch_{batch_id}"
        
        self.logger.log(f"[COMPACT L2] Aggregating {len(self.pending_l1)} L1 files...")
        
        # Load, Agg, Write
        self._aggregate_and_write(self.pending_l1, output_path)
        
        # Cleanup L1
        self._delete_paths(self.pending_l1)
        self.pending_l1 = [] # Reset list
        
        # Track L2
        self.final_l2_paths.append(output_path)
        self.logger.log(f"[COMPACT L2] Created L2 file: {output_path}")

    def finalize(self):
        """Flushes any remaining files up the chain."""
        self.logger.log("[FINALIZE] Flushing remaining files...")
        
        if self.pending_raw:
            self._compact_level_1()
            
        if self.pending_l1:
            self._compact_level_2()
            
        return self.final_l2_paths

    def _aggregate_and_write(self, input_paths, output_path):
        """Generic helper to read a list of paths, sum counts, and write."""
        # Reading list of paths allows Spark to only pick specific folders
        df = self.spark.read.parquet(*input_paths)
        
        df.groupBy("p1", "p2") \
          .agg(_sum("count").alias("count")) \
          .coalesce(1) \
          .write.mode("overwrite").parquet(output_path)

    def _delete_paths(self, paths):
        """Deletes HDFS paths."""
        for p in paths:
            try:
                self.fs.delete(self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(p), True)
            except Exception as e:
                self.logger.log(f"[WARN] Failed to delete {p}: {e}")

    def cleanup_all(self):
        """Removes the temporary directories entirely."""
        for d in self.dirs.values():
            try:
                self.fs.delete(self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(d), True)
            except: pass

# ==========================================
# MINING LOGIC
# ==========================================

def mine_range(df, window, chunk_start, chunk_end):
    # Optimize: Only look at relevant columns
    df_slim = df.select("plate", "location", "timestamp")
    
    df_buckets = df_slim \
        .withColumn("base_bucket", floor(col("timestamp") / window)) \
        .withColumn("time_bucket", explode(array(col("base_bucket"), col("base_bucket") + 1))) \
        .drop("base_bucket")

    # Self-join to find candidates
    candidates = df_buckets.alias("a").join(df_buckets.alias("b"), [
        col("a.location") == col("b.location"),
        col("a.time_bucket") == col("b.time_bucket"),
        col("a.plate") < col("b.plate") # Avoid duplicates and self-loops
    ]).select(
        col("a.plate").alias("p1"), col("a.timestamp").alias("t1"),
        col("b.plate").alias("p2"), col("b.timestamp").alias("t2"),
        col("a.time_bucket").alias("bucket")
    )
    
    midpoint = (col("t1") + col("t2")) / 2.0
    owner_bucket = floor(midpoint / window).cast("long")
    
    # Filter constraints
    return candidates.filter(
        (_abs(col("t1") - col("t2")) <= window) & 
        (owner_bucket == col("bucket")) & 
        (midpoint >= chunk_start) & (midpoint < chunk_end)
    ).select("p1", "p2")

# ==========================================
# MAIN EXECUTION
# ==========================================

def main():
    if len(sys.argv) < 7:
        print("Usage: <input> <output> <window_sec> <freq_threshold> <min_plate_count> <partition_hours>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    time_window = int(sys.argv[3])
    freq_threshold = int(sys.argv[4])
    min_plate_count = int(sys.argv[5])
    partition_hours = float(sys.argv[6])

    # Paths
    parquet_storage_path = output_path + "_optimized_source"
    log_file_path = output_path + "_detailed.log"
    temp_storage_base = output_path + "_temp_workspace"

    spark = SparkSession.builder \
        .appName("AccompanyingCars_Waterfall") \
        .config("spark.sql.shuffle.partitions", "3000") \
        .getOrCreate()

    logger = HDFSLogger(spark, log_file_path)
    
    # Initialize the Waterfall Aggregator
    # Triggers: 10 Raw -> 1 L1; 10 L1 -> 1 L2
    aggregator = WaterfallAggregator(spark, temp_storage_base, logger, l1_threshold=10, l2_threshold=10)

    logger.log(f"=== JOB START ===")
    logger.log(f"Config: Window={time_window}s, Partition={partition_hours}h")
    logger.log(f"Strategy: Waterfall Aggregation (10 Raw->1 L1, 10 L1->1 L2)")

    try:
        # ---------------------------------------------------------
        # PHASE 1: PREPROCESSING (CSV -> Optimized Parquet)
        # ---------------------------------------------------------
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
        if not fs.exists(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(parquet_storage_path)):
            logger.log("[PHASE 1] Converting CSV to Partitioned Parquet...")
            
            raw_schema = StructType([
                StructField("plate", StringType(), False),
                StructField("location", StringType(), False),
                StructField("timestamp", LongType(), False)
            ])
            
            raw_df = spark.read.schema(raw_schema).option("header", "false").csv(input_path)
            
            # Filter infrequent plates globally
            freq_plates = raw_df.groupBy("plate").count().filter(col("count") >= min_plate_count).select("plate")
            clean_df = raw_df.join(broadcast(freq_plates), "plate")
            
            # Partition by Day
            clean_df.withColumn("day_id", floor(col("timestamp") / 86400)) \
                .write.partitionBy("day_id").mode("overwrite").parquet(parquet_storage_path)
            logger.log("[PHASE 1] Conversion Complete.")
        else:
            logger.log("[PHASE 1] Optimized data found. Skipping.")

        # ---------------------------------------------------------
        # PHASE 2: STREAMING MINING & WATERFALL AGGREGATION
        # ---------------------------------------------------------
        spark.conf.set("spark.sql.shuffle.partitions", "30") # Reduce shuffle for small chunks
        
        parquet_df = spark.read.parquet(parquet_storage_path)
        stats = parquet_df.agg(min("timestamp"), max("timestamp")).head()
        min_time, max_time = stats[0], stats[1]
        
        partition_seconds = partition_hours * 3600
        total_chunks = math.ceil((max_time - min_time) / partition_seconds)
        
        current_start = min_time
        chunk_id = 0

        while current_start <= max_time:
            current_end = current_start + partition_seconds
            
            # Smart Filtering: Only load relevant partitions
            buffer = time_window
            start_day = math.floor((current_start - buffer) / 86400)
            end_day = math.floor((current_end + buffer) / 86400)
            
            chunk_df = parquet_df.filter(
                (col("day_id") >= start_day) & 
                (col("day_id") <= end_day) & 
                (col("timestamp") >= (current_start - buffer)) & 
                (col("timestamp") < (current_end + buffer))
            )
            
            # Mine Pairs
            pairs_df = mine_range(chunk_df, time_window, current_start, current_end)
            
            # Local aggregation for this specific time chunk (p1, p2, count)
            chunk_result = pairs_df.groupBy("p1", "p2").count()
            
            # Pass to Aggregator (Writes to disk, handles compaction/deletion logic)
            aggregator.add_raw_chunk(chunk_result, chunk_id)
            
            if chunk_id % 5 == 0:
                logger.log(f"[PROGRESS] Processed Chunk {chunk_id + 1}/{total_chunks}")
            
            current_start = current_end
            chunk_id += 1

        # ---------------------------------------------------------
        # PHASE 3: FINAL AGGREGATION
        # ---------------------------------------------------------
        # logger.log("[PHASE 3] Finalizing Waterfall Aggregation...")
        
        # # Ensure all L1 and L2 files are flushed and get the list of huge L2 files
        # final_l2_paths = aggregator.finalize()
        
        # if not final_l2_paths:
        #     logger.log("[WARN] No results found in the entire dataset.")
        #     sys.exit(0)

        # logger.log(f"[PHASE 3] Global Aggregation on {len(final_l2_paths)} L2 files...")
        
        # spark.conf.set("spark.sql.shuffle.partitions", "200")
        
        # # Read only the L2 files
        # final_df = spark.read.parquet(*final_l2_paths)
        
        # # Final Sum
        # result_df = final_df.groupBy("p1", "p2") \
        #     .agg(_sum("count").alias("total_count")) \
        #     .filter(col("total_count") >= freq_threshold) \
        #     .orderBy(col("total_count").desc())
            
        # logger.log(f"[OUTPUT] Writing final result to {output_path}...")
        # result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        
        logger.log("[SUCCESS] Job Finished Successfully.")

    except Exception as e:
        logger.log(f"[ERROR] Critical Failure: {e}")
        import traceback
        logger.log(traceback.format_exc())
        sys.exit(1)
        
    finally:
        # logger.log("[CLEANUP] Deleting temp workspace...")
        # aggregator.cleanup_all()
        logger.log("[EXIT] Files Ready...")
        logger.close()
        spark.stop()

if __name__ == "__main__":
    main()
```

- Refinements
  1. **Migration to PySpark Ecosystem**

     The entire application logic has been ported from Scala to **PySpark** to align with the Python-centric data science ecosystem.
     *   **Interoperability:** To maintain robust logging capabilities (specifically writing directly to HDFS with `hflush` for real-time monitoring), the code utilizes the `py4j` gateway (`sc._jvm`) to access the underlying Java Hadoop FileSystem API directly from Python.
     *   **Vectorized Performance:** The mining logic leverages Spark SQL expressions (columnar operations) which are executed in the JVM, ensuring that the performance penalty of using Python is negligible for heavy computational tasks.

  2. **Waterfall Aggregation (Hierarchical Compaction)**

     To address the "Small File Problem" and "Disk Space Exhaustion" caused by generating thousands of intermediate result files during the 31-day processing loop, a multi-level compaction strategy was introduced.

     **Logic:**
     The `WaterfallAggregator` class manages a three-tier storage hierarchy:
     *   **Level 0 (Raw Chunks):** As the main loop runs, each 15-minute batch produces a small Parquet file.
     *   **Level 1 Compaction:** Once **10 Raw files** accumulate, the system automatically aggregates them into a single **L1 file** and **immediately deletes** the source files to free up disk space.
     *   **Level 2 Compaction:** Once **10 L1 files** accumulate, they are further aggregated into a single **L2 file**, and the L1 sources are deleted. This ensures disk usage remains stable rather than growing linearly.

  3. **Optimized Two-Phase Execution**

     The workflow is strictly divided into two phases to optimize resource utilization and I/O latency:
     *   **Phase 1 (One-Time Conversion):** The raw CSV data is converted to Parquet format, partitioned by **Day**. This expensive step happens only once.
     *   **Phase 2 (Partition Pruning):** The mining loop reads from the Parquet data. By calculating the specific days involved in a time window, the application pushes filters down to the file system, causing Spark to read only the relevant daily folders instead of scanning the entire dataset.

  4. **Dynamic Resource Tuning**

     The code dynamically adjusts Spark configurations during runtime to match the specific workload size of each phase:
     *   **High Partitions (3000):** Used during Phase 1 (Conversion) to handle the massive 2.7 billion record shuffle without memory overflow.
     *   **Low Partitions (30):** Used during Phase 2 (Mining Loop). Since each 15-minute chunk is small, reducing the partition count prevents the generation of millions of tiny files, alleviating pressure on the HDFS NameNode.

- Command:

```bash
spark-submit --master yarn \
  --deploy-mode cluster \
  --driver-memory 2g \
  --executor-memory 3g \
  --executor-cores 2 \
  --num-executors 4 \
  --conf spark.yarn.executor.memoryOverhead=2048 \
  --conf spark.network.timeout=600s \
  --conf spark.shuffle.io.maxRetries=1000000 \
  --conf spark.pyspark.python=/usr/bin/python3 \
  --conf spark.pyspark.driver.python=/usr/bin/python3 \
  accompany_waterfall.py \
  projects/accomp_cars/data/31.csv \
  projects/accomp_cars/results/full_run \
  300 \
  100 \ 
  100 \
  0.25
```







### Tools



#### Log Watcher

As we haven't find the way to access WebUI through ssh forward (port forwarding seems useless), we produce a `safe-start.sh` bash script to start the hadoop and spark system and supervise the resource manager, node manager, namenode, datanode logs.

```bash
#!/bin/bash

# 1. Setup Variables
# Adjust HADOOP_HOME if it's not set in your environment
# : ${HADOOP_HOME:=/opt/hadoop-3.3.6}
LOG_DIR="$HADOOP_HOME/logs"
CURRENT_USER=$(whoami)
HOSTNAME=$(hostname)

# Define the 4 specific log files to watch
NN_LOG="$LOG_DIR/hadoop-$CURRENT_USER-namenode-$HOSTNAME.log"
DN_LOG="$LOG_DIR/hadoop-$CURRENT_USER-datanode-$HOSTNAME.log"
RM_LOG="$LOG_DIR/hadoop-$CURRENT_USER-resourcemanager-$HOSTNAME.log"
NM_LOG="$LOG_DIR/hadoop-$CURRENT_USER-nodemanager-$HOSTNAME.log"

echo "=========================================================="
echo " Preparing to watch logs... "
echo " headers (==>) will ONLY appear if an ERROR follows."
echo "=========================================================="

# 2. Start Monitoring in the Background

tail -n 0 -F "$NN_LOG" "$DN_LOG" "$RM_LOG" "$NM_LOG" 2>/dev/null \
| awk '
    # 1. If line starts with "==>", save it to variable "h", but DO NOT print yet.
    /^==>/ { h=$0; next } 
    
    # 2. If line contains "ERROR", do the following:
    /ERROR/ { 
        # If we have a saved header "h", print it now.
        if (h != "") { 
            print h; 
            h=""; # Clear it so we don'\''t print the header twice for the same batch
        }
        # Print the actual Error line
        print $0 "\n"; 
        
        # FORCE output to appear immediately (disable buffering)
        fflush(); 
    }
' &

# Capture the Process ID (PID) of the tail command so we can kill it later
TAIL_PID=$!

# 3. Define a cleanup function to stop the monitor when you press Ctrl+C
cleanup() {
    echo ""
    echo "Stopping log monitor..."
    kill $TAIL_PID
    exit
}
# Trap the Interrupt signal (Ctrl+C)
trap cleanup SIGINT

# 4. Run the Start Command
echo ">>> Executing start-all.sh now..."
echo ""
$HADOOP_HOME/sbin/start-all.sh

echo ""
echo ">>> Startup command finished."
echo ">>> Continuing to monitor logs for errors. Press Ctrl+C to stop watching."
echo "=========================================================="

# 5. Keep the script running to hold the terminal open for the background monitor
wait $TAIL_PID

```



#### Storage Balancer



As the output might be stored in the same machine sometimes, we need to balance it on all the cluster machines, so we use `crontab -e` to add a balancing bash script to balance the storage every hour.

```bash
#!/bin/bash

# --- Configuration ---
# Log file location
LOG_FILE="/var/log/hdfs_balancer.log"

# Threshold: 10% deviation allowed between nodes
THRESHOLD=10

# Bandwidth Limit: 20MB/s (20 * 1024 * 1024 = 20971520)
# We limit this to prevent slowing down your running Spark jobs too much.
# If Spark is NOT running, you can increase this to 100MB/s (104857600).
BANDWIDTH=20971520

# Lock file to prevent multiple instances running simultaneously
LOCK_FILE="/tmp/hdfs_balancer.lock"

# --- Environment Setup (Crucial for Cron) ---

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64
export HADOOP_HOME=/opt/hadoop-3.3.6
HDFS_CMD="$HADOOP_HOME/bin/hdfs"

# --- Execution Logic ---

# 1. Acquire Lock (Exit if script is already running)
exec 200>$LOCK_FILE
flock -n 200 || { echo "[$(date)] Skipped: Balancer is already running." >> $LOG_FILE; exit 1; }

echo "========================================================" >> $LOG_FILE
echo "[$(date)] STARTING HDFS BALANCER" >> $LOG_FILE

# 2. Set Bandwidth Limit (Safe for concurrent Spark jobs)
echo "[$(date)] Setting bandwidth to 20MB/s..." >> $LOG_FILE
$HDFS_CMD dfsadmin -setBalancerBandwidth $BANDWIDTH >> $LOG_FILE 2>&1

# 3. Run Balancer
# The script will pause here until balancing is done or threshold met
$HDFS_CMD balancer -threshold $THRESHOLD >> $LOG_FILE 2>&1

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "[$(date)] SUCCESS: Cluster is balanced." >> $LOG_FILE
else
    echo "[$(date)] FINISHED: Balancer stopped (Exit Code: $EXIT_CODE)." >> $LOG_FILE
fi

echo "========================================================" >> $LOG_FILE

```





### Result



### Conclusion

