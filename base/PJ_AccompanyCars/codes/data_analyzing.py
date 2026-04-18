import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min, max, count, floor, approx_count_distinct, 
    avg, expr, percentile_approx
)
from pyspark.sql.types import StructType, StructField, StringType, LongType

class HDFSLogger:
    def __init__(self, spark, log_file_path):
        self.sc = spark.sparkContext
        self.fs = self.sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            self.sc._jvm.java.net.URI(log_file_path),
            self.sc._jsc.hadoopConfiguration()
        )
        self.path = self.sc._jvm.org.apache.hadoop.fs.Path(log_file_path)
        # Create output stream (overwrite if exists)
        self.os = self.fs.create(self.path, True)
    
    def log(self, message):
        # 1. Print to Console (Standard Output)
        print(message)
        
        # 2. Write to HDFS File
        try:
            # Add simple timestamp for file log context, or keep clean if preferred. 
            # Here we keep it exactly as printed to console for readability.
            data = (message + "\n").encode('utf-8')
            self.os.write(data)
            self.os.hflush() # Ensure data is visible immediately
        except Exception as e:
            print(f"!! Logger Error: {e}")

    def close(self):
        try: self.os.close()
        except: pass

class Analyzer:
    def __init__(self, spark, input_path, logger):
        self.spark = spark
        self.input_path = input_path
        self.logger = logger
        self.schema = StructType([
            StructField("plate", StringType(), False),
            StructField("location", StringType(), False),
            StructField("timestamp", LongType(), False)
        ])
        
    def load_data(self):
        self.logger.log(f"[LOAD] Reading {self.input_path}...")
        self.df = self.spark.read.schema(self.schema).option("header", "false").csv(self.input_path)
        # Caching is crucial here as we will compute multiple stats on the same data
        self.df.cache()
        self.logger.log(f"[LOAD] Data cached. Starting analysis...")

    def basic_stats(self):
        self.logger.log("\n--- 1. Basic Dataset Statistics ---")
        start = time.time()
        
        # We calculate everything in one pass for efficiency
        stats = self.df.agg(
            min("timestamp").alias("min_t"),
            max("timestamp").alias("max_t"),
            count("*").alias("total_rows"),
            approx_count_distinct("plate").alias("unique_plates"),
            approx_count_distinct("location").alias("unique_locs")
        ).collect()[0]
        
        duration = stats['max_t'] - stats['min_t']
        days = duration / 86400.0
        
        self.logger.log(f"Total Entries:      {stats['total_rows']:,}")
        self.logger.log(f"Time Span (Unix):   {stats['min_t']} to {stats['max_t']}")
        self.logger.log(f"Duration:           {days:.2f} Days")
        self.logger.log(f"Unique Plates:      ~{stats['unique_plates']:,} (Approx)")
        self.logger.log(f"Unique Locations:   ~{stats['unique_locs']:,} (Approx)")
        self.logger.log(f"Time Taken:         {time.time() - start:.2f}s")

    def window_analysis(self, windows, output_base):
        self.logger.log(f"\n--- 2 & 3. Window & Location Analysis {windows} ---")
        
        for w in windows:
            self.logger.log(f"\n>>> Processing Window Size: {w} seconds")
            start = time.time()
            
            # 1. Assign Bucket ID
            df_w = self.df.withColumn("bucket", floor(col("timestamp") / w))
            
            # --- Global Bucket Stats ---
            # Group by Bucket only -> How busy is the whole city in this window?
            global_counts = df_w.groupBy("bucket").count()
            
            g_stats = global_counts.agg(
                max("count").alias("max"),
                min("count").alias("min"),
                avg("count").alias("avg"),
                percentile_approx("count", 0.5).alias("median")
            ).collect()[0]
            
            self.logger.log(f"  [Global] Max Entries/Bucket: {g_stats['max']:,}")
            self.logger.log(f"  [Global] Min Entries/Bucket: {g_stats['min']:,}")
            self.logger.log(f"  [Global] Avg Entries/Bucket: {g_stats['avg']:.2f}")
            self.logger.log(f"  [Global] Med Entries/Bucket: {g_stats['median']:,}")

            # --- Per-Location Bucket Stats ---
            # Group by (Location, Bucket) -> How busy is a specific spot?
            loc_bucket_counts = df_w.groupBy("location", "bucket").count()
            
            # Calculate Max/Min/Avg/Median load FOR EACH location
            loc_stats = loc_bucket_counts.groupBy("location").agg(
                max("count").alias("max_load"),
                min("count").alias("min_load"),
                avg("count").alias("avg_load"),
                percentile_approx("count", 0.5).alias("median_load")
            )
            
            # We print the "Worst Case Location" stats to the screen
            worst_loc = loc_stats.orderBy(col("max_load").desc()).first()
            self.logger.log(f"  [Hotspot] Most crowded location ID: {worst_loc['location']}")
            self.logger.log(f"  [Hotspot] Max entries in one bucket there: {worst_loc['max_load']:,}")
            
            # Save the detailed per-location stats to CSV for plotting later
            out_path = f"{output_base}/window_{w}_loc_stats"
            self.logger.log(f"  [Save] Saving per-location stats to {out_path}...")
            loc_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_path)
            
            self.logger.log(f"  Time Taken: {time.time() - start:.2f}s")

def main():
    if len(sys.argv) < 3:
        print("Usage: spark-submit data_analysis.py <input_csv> <output_dir>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_dir = sys.argv[2]
    
    # Define log file path inside the output directory
    log_file_path = f"{output_dir}/analysis_report.txt"
    
    # Analyze these window sizes
    WINDOWS = [50, 100, 200, 300]

    spark = SparkSession.builder \
        .appName("CarData_EDA") \
        .config("spark.sql.shuffle.partitions", "1000") \
        .getOrCreate()

    # Initialize logger
    logger = HDFSLogger(spark, log_file_path)
    logger.log(f"=== Analysis Started at {datetime.now()} ===")
    logger.log(f"Report File: {log_file_path}")

    analyzer = Analyzer(spark, input_path, logger)
    
    try:
        analyzer.load_data()
        analyzer.basic_stats()
        analyzer.window_analysis(WINDOWS, output_dir)
        logger.log(f"=== Analysis Finished at {datetime.now()} ===")
    except Exception as e:
        logger.log(f"[ERROR] Analysis failed: {e}")
        raise e
    finally:
        logger.close()
        spark.stop()

if __name__ == "__main__":
    main()