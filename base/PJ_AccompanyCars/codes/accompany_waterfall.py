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