from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

def process_month_to_silver(year, month):
    spark = SparkSession.builder \
        .appName(f"WikiIngestion-Silver-Bulk-{year}-{month}") \
        .getOrCreate()

    # 1. Use Wildcards for the entire month
    # This reads all days (01-31) in one shot
    input_path = f"s3://wikipulse/bronze/year={year}/month={month:02d}/*/*.gz"
    output_path = f"s3://wikipulse/silver/year={year}/month={month:02d}/"

    # 2. Read data
    # We use baseResource to capture the 'day' from the folder structure if needed,
    # but since you are saving with partitionBy, Spark handles this naturally.
    df_raw = spark.read.format("csv") \
        .option("sep", " ") \
        .load(input_path) \
        .toDF("domain_code", "page_title", "count_views", "response_size")

    # 3. Add the 'day' column from the file path so we don't lose it
    # Spark's 'input_file_name' can extract the day from the S3 path string
    df_with_date = df_raw.withColumn("day", F.lpad(F.regexp_extract(F.input_file_name(), r"day=(\d+)", 1), 2, "0"))

    # 4. Filters & Consolidation
    unwanted_pattern = r"^(Special:|File:|Category:|User:|Talk:|Wikipedia:|Help:|\-|Portal:|Draft:|Main_Page)"
    
    df_clean = df_with_date.filter(
        (F.col("domain_code").isin("en", "en.m")) & 
        (~F.col("page_title").rlike(unwanted_pattern))
    ).withColumn("views", F.col("count_views").cast("integer"))

    # 5. Aggregate by Day, and Title
    df_silver = df_clean.groupBy("day", "page_title") \
        .agg(F.sum("views").alias("daily_views"))

    # 6. Write with Partitioning
    # This creates folders: silver/year=2026/month=03/day=01/, etc.
    # We use repartition(1) per day to ensure one file per folder
    print(f"Writing Silver layer in bulk to: {output_path}")

    df_final = df_silver.repartition("day")

    (df_final.write.mode("overwrite")
        .partitionBy("day")
        .parquet(output_path))
    
if __name__ == "__main__":
    y = int(sys.argv[1])
    m = int(sys.argv[2])
    process_month_to_silver(y, m)
    