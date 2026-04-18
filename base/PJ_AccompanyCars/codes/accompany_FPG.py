import sys
import math
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.ml.fpm import FPGrowth

# ==========================================
# 1. HDFS LOGGER
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
        except: pass

    def close(self):
        try: self.os.close()
        except: pass

# ==========================================
# 2. HELPER: SINGLE GRID MINER
# ==========================================
def run_fpgrowth_on_chunk(spark, df, time_window, offset_seconds, min_support_rel, min_cars, max_cars):
    """
    Runs FP-Growth on a small time chunk with specific window offsets.
    """
    # 1. Bucketize with Shift
    grid_df = df.withColumn("shifted_time", F.col("timestamp") - offset_seconds)
    
    transactions = grid_df.withColumn("bucket_id", F.floor(F.col("shifted_time") / time_window)) \
        .groupBy("location", "bucket_id") \
        .agg(F.collect_set("plate").alias("items"))
    
    # Filter: Valid buckets must have at least 2 cars to form a pair
    transactions_filtered = transactions.filter(F.size(F.col("items")) >= 2)

    # Repartition to distribute massive buckets
    transactions_optimized = transactions_filtered.repartition(1000)

    # 2. Run FP-Growth
    fp = FPGrowth(itemsCol="items", minSupport=min_support_rel, minConfidence=0.0)
    model = fp.fit(transactions_optimized)
    
    # 3. Output Filter
    return model.freqItemsets.filter(
        (F.size(F.col("items")) >= min_cars) & 
        (F.size(F.col("items")) <= max_cars)
    )

# ==========================================
# 3. MAIN CONTROLLER
# ==========================================
def main():
    if len(sys.argv) < 10:
        print("Usage: <input> <output> <window> <freq> <min_plate> <density_cap> <chunk_hr> <min_cars> <max_cars>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    time_window = int(sys.argv[3])
    freq_threshold = int(sys.argv[4])
    min_plate_count = int(sys.argv[5])
    density_cap = int(sys.argv[6])
    chunk_hours = float(sys.argv[7])
    min_cars_in_convoy = int(sys.argv[8])
    max_cars_in_convoy = int(sys.argv[9])

    temp_results_path = output_path + "_temp"
    clean_data_path = output_path + "_clean"
    
    spark = SparkSession.builder \
        .appName("AccompanyingCars_FPGrowth") \
        .config("spark.sql.shuffle.partitions", "1000") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
        
    logger = HDFSLogger(spark, output_path + "_run.log")
    logger.log("=== JOB START ===")
    logger.log(f"Config: Window={time_window}s, DensityCap={density_cap}, Chunk={chunk_hours}h")

    try:
        # --- PHASE 1: LOAD, FILTER & MATERIALIZE ---
        raw_schema = StructType([
            StructField("plate", StringType(), False),
            StructField("location", StringType(), False),
            StructField("timestamp", LongType(), False)
        ])
        
        logger.log("[PHASE 1] Loading Data...")
        df = spark.read.schema(raw_schema).option("header", "false").csv(input_path)
        
        # 1. Global Frequency Filter (Keep only frequent plates)
        plate_counts = df.groupBy("plate").count()
        valid_plates = plate_counts.filter(F.col("count") >= min_plate_count)
        df_filtered = df.join(valid_plates, "plate", "left_semi")

        # 2. Density Filter (Remove buckets with > 10,000 cars)
        # We calculate the bucket ID based on the window size
        df_filtered = df_filtered.withColumn("t_bucket", F.floor(F.col("timestamp") / time_window))
        
        bucket_counts = df_filtered.groupBy("location", "t_bucket").count()
        valid_buckets = bucket_counts.filter(F.col("count") <= density_cap)
        
        # Keep only rows belonging to valid (non-dense) buckets
        df_final = df_filtered.join(valid_buckets, ["location", "t_bucket"], "left_semi") \
                              .select("plate", "location", "timestamp")

        # 3. Materialize to Disk (Avoid OOM)
        logger.log("[PHASE 1] Materializing filtered data to HDFS...")
        df_final.write.mode("overwrite").parquet(clean_data_path)
        
        # 4. Count & Log Remaining Entries
        clean_df = spark.read.parquet(clean_data_path)
        remaining_count = clean_df.count()
        logger.log(f"[STATS] Entries remaining after density filter (>{density_cap / 1000}k removed): {remaining_count}")
        
        stats = clean_df.agg(F.min("timestamp"), F.max("timestamp")).head()
        min_t, max_t = stats[0], stats[1]
        logger.log(f"Time Range: {min_t} to {max_t}")
        
        # --- PHASE 2: ITERATIVE CHUNK PROCESSING ---
        chunk_seconds = int(chunk_hours * 3600)
        curr_t = min_t
        batch_id = 0
        local_min_support = 0.0 
        
        while curr_t < max_t:
            batch_id += 1
            next_t = curr_t + chunk_seconds
            logger.log(f"--- Processing Batch {batch_id} (Time: {curr_t} - {next_t}) ---")
            
            chunk_df = clean_df.filter(
                (F.col("timestamp") >= curr_t) & (F.col("timestamp") < next_t)
            )
            
            n_transactions = chunk_df.count() 
            if n_transactions == 0:
                curr_t = next_t
                continue
            
            local_min_support = 1.0 / n_transactions

            # Grid A
            res_a = run_fpgrowth_on_chunk(spark, chunk_df, time_window, 0, local_min_support, min_cars_in_convoy, max_cars_in_convoy)
            res_a = res_a.withColumnRenamed("freq", "freq_A")
            
            # Grid B
            res_b = run_fpgrowth_on_chunk(spark, chunk_df, time_window, int(time_window/2), local_min_support, min_cars_in_convoy, max_cars_in_convoy)
            res_b = res_b.withColumnRenamed("freq", "freq_B")
            
            # Max-Merge
            sort_udf = F.udf(lambda x: sorted(x), F.ArrayType(StringType()))
            res_a_s = res_a.withColumn("items_sorted", sort_udf(F.col("items")))
            res_b_s = res_b.withColumn("items_sorted", sort_udf(F.col("items")))
            
            joined = res_a_s.join(res_b_s, on="items_sorted", how="full_outer")
            
            local_results = joined.withColumn(
                "local_count", 
                F.greatest(F.coalesce(F.col("freq_A"), F.lit(0)), F.coalesce(F.col("freq_B"), F.lit(0)))
            )
            
            local_results.select("items_sorted", "local_count") \
                .write.mode("append").parquet(temp_results_path)
            
            curr_t = next_t

        # --- PHASE 3: GLOBAL AGGREGATION ---
        logger.log("[PHASE 3] Global Aggregation...")
        all_chunks = spark.read.parquet(temp_results_path)
        
        global_results = all_chunks.groupBy("items_sorted") \
            .agg(F.sum("local_count").alias("total_count"))
            
        # --- PHASE 4: FINAL OUTPUT ---
        logger.log(f"[PHASE 4] Filtering Threshold >= {freq_threshold}...")
        
        final_output = global_results.filter(F.col("total_count") >= freq_threshold) \
            .withColumn("cars", F.concat_ws(",", "items_sorted")) \
            .select("cars", "total_count") \
            .orderBy(F.col("total_count").desc())
            
        final_output.write.mode("overwrite").option("header", "true").csv(output_path)
        
        # Cleanup
        try:
            fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
            fs.delete(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(temp_results_path), True)
            fs.delete(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(clean_data_path), True)
        except: pass
        
        logger.log("[SUCCESS] Job Completed.")

    except Exception as e:
        logger.log(f"[ERROR] {e}")
        import traceback
        logger.log(traceback.format_exc())
        sys.exit(1)
    finally:
        logger.close()
        spark.stop()

if __name__ == "__main__":
    main()