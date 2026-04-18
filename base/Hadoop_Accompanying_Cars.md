## Hadoop Accompanying Cars



### Purpose



The primary objective of this project is to identify "accompanying vehicles" from a massive dataset of traffic surveillance records. An "accompanying relationship" is defined as two vehicles appearing at the **same location** within a **short time window** (e.g., 5 minutes) frequently (e.g., more than 10 times).

The algorithm aims to:

1.  Efficiently process massive datasets (hundreds of millions of records).
2.  Solve the "Boundary Effect" problem where vehicles appearing at the edge of a time window might be missed.
3.  Avoid duplicate counting caused by the solution to the boundary effect.
4.  Output a ranked list of vehicle pairs that are likely travelling together (convoys, friends, etc.).



### Procudures



#### Environment Setup

- Local Editor Access Remote Server: 

  ```ini
  Host Hadoop01
    HostName 10.176.62.206
    Port 22
    User root
    IdentityFile ~/.ssh/hadoop
    RemoteForward 8888 localhost:7897
  ```

  - `IdentityFile ~/.ssh/hadoop`: No password is needed when login. You need to upload pubkey to `~/.ssh/authorized_keys` and modify `/etc/ssh/sshd_config` on remote server.

    ```ini
    PermitRootLogin yes
    PubkeyAuthentication yes
    ```
  - `RemoteForward 8888 localhost:7897`: Forward `localhost:7897` to remote port 8888, making Clash on local machine to be a proxy for internet (~~foreign net~~) access. Remember to set your Clash proxy port to be 7897.

- Download Spark and Config

   - It's not that hard to find the link~: https://dlcdn.apache.org/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3.tgz

   - Unzip to your intended directory, in my case, `/opt/spark-3.5.7`.

   - Download and unzip `jdk-17.0.16` to path you want, in my case, `/usr/lib/jvm/jdk-17.0.16`.

   - Download and unzip `scala-2.12.15` to path you'd like, in my case, `/opt/scala-2.12.15`.

   - Config in `/opt/spark-3.5.7/conf/spark-env.sh`:

      ```bash 
      export JAVA_HOME=/usr/lib/jvm/jdk-17.0.16
      export HADOOP_HOME=/opt/hadoop-3.3.6
      export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
      export SPARK_MASTER_IP=Hadoop01
      export MASTER=spark://Hadoop01:7077
      export SPARK_LOCAL_DIRS=/opt/spark-3.5.7
      export SPARK_DRIVER_MEMORY=1G
      ```


- Round up Config

  - Save a `workers` file in both `/opt/spark-3.5.7/conf` and `/opt/hadoop-3.3.6/etc/hadoop`.

    ```ini
    Hadoop01
    Hadoop02
    Hadoop03
    Hadoop04
    ```

  - Broadcast the `hadoop` and `spark` to all servers.



#### Algorithm Design

The solution is implemented in Scala using Apache Spark. The code is structured in an Object-Oriented manner for modularity and readability. The core logic is divided into three main components: **Data Processing**, **Algorithm Engine**, and the **Application Controller**.

**1. Configuration Management (`JobConfig`)**
To ensure clean method signatures and maintainable code, all runtime parameters are encapsulated in a `JobConfig` case class.

*   **Inputs:** `inputPath`, `outputPath`.
*   **Parameters:** `timeWindow` (Δt, max time difference to be considered "together"), `freqThreshold` (min occurrences to be "accompanying"), and `minPlateCount` (optimization threshold).

```scala
// Encapsulating parameters to separate configuration from logic
case class JobConfig(
    inputPath: String,
    outputPath: String,
    timeWindow: Int,
    freqThreshold: Int,
    minPlateCount: Int
)
```

**2. Data Ingestion & Pre-processing (`DataProcessor` Object)**

*   **Schema Definition:** A strict `StructType` schema is defined (`plate`, `location`, `timestamp`) to ensure type safety and avoid parsing errors.
*   **Optimization (Pre-filtering):**
    *   *Logic:* The code first calculates the total appearance count of every vehicle.
    *   *Reasoning:* If a vehicle appears only once or twice in the entire month, it is statistically impossible for it to form a frequent accompanying pair (which requires >10 matches). Filtering these distinct low-frequency vehicles early significantly reduces the dataset size and shuffle overhead in subsequent steps.

```scala
object DataProcessor {
  def loadAndClean(spark: SparkSession, config: JobConfig): DataFrame = {
    // ... schema definition ...
    
    // Filter Infrequent Cars (Optimization)
    val frequentPlates = rawDf.groupBy("plate").count()
      .filter($"count" >= config.minPlateCount)
      .select("plate")

    // Return the joined, clean DataFrame only containing relevant cars
    rawDf.join(frequentPlates, "plate")
  }
}
```

**3. Spatiotemporal Binning with Overlapping Buckets (`AlgorithmEngine` - Step A)**

* **The Problem:** A standard time bucket (e.g., dividing time by 300s) creates hard boundaries. If Car A appears at 299s and Car B at 301s, they fall into different buckets (0 and 1) and would never be compared, despite being only 2 seconds apart.

* **The Solution:** The **Sliding Window / Overlapping Bucket** technique. Each record is duplicated into two buckets: its calculated bucket ($B$) and the next bucket ($B+1$). This guarantees that any two vehicles within the time window distance will coexist in at least one common bucket.

```scala
// AlgorithmEngine.scala
val dfWithBuckets = df
  .withColumn("base_bucket", floor($"timestamp" / timeWindow))
  // Assign to current bucket AND the next bucket to handle boundary overlap
  .withColumn("time_bucket", explode(array($"base_bucket", $"base_bucket" + 1)))
  .drop("base_bucket")
```

**4. Candidate Generation & Deduplication (`AlgorithmEngine` - Step B & C)**
This is the core of the distributed computation.

*   **Grouping:** Data is grouped by `location` and `time_bucket`. This gathers all cars that *might* be together into a single list.
*   **UDF Logic (Pair Generation):** A custom User Defined Function (UDF) processes each group:
    1.  **Sorting:** The list of cars is sorted by plate number to ensure the pair `(A, B)` is treated the same as `(B, A)`.
    2.  **Precision Check:** The UDF iterates through all combinations. It calculates `abs(t1 - t2)`. If the difference is greater than `timeWindow`, the pair is discarded (even if they are in the same bucket).
    3.  **Deduplication (Midpoint Ownership Strategy):** Since Step A duplicated the data, a valid pair might appear in both Bucket $K$ and Bucket $K+1$. To prevent double counting, we calculate the midpoint time ($T_{mid}$) and check if the current bucket "owns" that midpoint.

```scala
// Inside the UDF logic
val logic = (cars: Seq[Row], currentBucket: Long, window: Int) => {
  // ... nested loops to compare car i and car j ...
  val (p1, t1) = carData(i)
  val (p2, t2) = carData(j)

  // 1. Precision Check: Are they strictly within the time window?
  if (math.abs(t1 - t2) <= window) {
    
    // 2. Deduplication: "Midpoint Ownership" strategy
    val midPoint = (t1 + t2) / 2.0
    val ownerBucket = math.floor(midPoint / window).toLong
    
    // Only count if the midpoint belongs to the current processing bucket
    if (ownerBucket == currentBucket) {
      pairs += ((p1, p2))
    }
  }
  pairs
}
```

**5. Aggregation & Filtering (`AccompanyingCarsApp`)**

*   **Counting:** The candidate pairs are flattened and grouped by `(plate1, plate2)`.
*   **Thresholding:** `filter($"count" >= config.freqThreshold)` removes accidental meetings, leaving only statistically significant accompanying pairs.
*   **Output:** The results are sorted by frequency and written to HDFS in CSV format.

```scala
// Main Application Controller
val results = candidatePairs
  .groupBy("plate1", "plate2")
  .count()
  .filter($"count" >= config.freqThreshold) // Thresholding
  .orderBy($"count".desc)

results.write.csv(config.outputPath)
```



#### Code Debugging



**ERROR LOGS**



### Results



### Conclusions



1.  **Efficiency:** The design avoids a global Cartesian product ($O(N^2)$) by using spatiotemporal bucketing, reducing the complexity to $O(M^2)$ within small buckets (where M is the number of cars in a specific place at a specific time).
2.  **Accuracy:** The overlapping bucket strategy combined with the midpoint ownership check ensures 100% recall (no missed pairs at boundaries) and 100% precision (no duplicate pairs).
3.  **Scalability:** The code is built on Spark SQL and DataFrame APIs, making it suitable for scaling from gigabytes to terabytes of data on the Hadoop cluster. The Object-Oriented design allows for easy maintenance and testing.



- Sampling

```bash
spark-submit \
  --class com.acars.project.ReadCSV \
  target/scala-2.12/AccompanyingCars-assembly-1.0.jar \
  sample \
  projects/accomp_cars/data/31.csv \
  projects/accomp_cars/data/samp_1w.csv \
  10000
```

- Features

```bash
spark-submit --class com.acars.project.DataFeatureAnalysis --master yarn --deploy-mode cluster --driver-memory 2g --executor-memory 4g --executor-cores 2 --num-executors 10 --conf spark.sql.shuffle.partitions=500 target/scala-2.12/AccompanyingCars-assembly-1.0.jar projects/accomp_cars/data/31.csv projects/accomp_cars/anal
```

- Pairing

```bash
    spark-submit --class com.acars.project.AccompanyingCars --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 6g --executor-cores 4 --num-executors 10 --conf spark.sql.shuffle.partitions=500 --conf spark.network.timeout=300s target/scala-2.12/AccompanyingCars-assembly-1.0.jar projects/accomp_cars/data/31.csv projects/accomp_cars/results/ranged 300 10 5
```

