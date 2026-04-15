from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def generate_gold_layer():
    spark = SparkSession.builder \
        .appName("Wiki-Gold-Aggregation") \
        .getOrCreate()

    # 1. Load Silver Layer
    df_silver = spark.read.parquet("s3://wikipulse/silver/") \
        .filter((F.col("year") == 2026) & (F.col("month") == 3))
    
    # Create date column
    df_silver = df_silver.withColumn("date", F.to_date(F.concat_ws("-", F.col("year"), F.col("month"), F.col("day"))))

    # 2. Window for Growth Calculation
    window_spec = Window.partitionBy("page_title").orderBy("date")

    # Calculate Delta with your Significance Threshold (prev_day > 500)
    df_metrics = df_silver.withColumn("prev_day_views", F.lag("daily_views").over(window_spec)) \
        .withColumn("view_delta_pct", 
            F.when((F.col("prev_day_views") >= 500) & (F.col("daily_views") > 0), 
                   ((F.col("daily_views") - F.col("prev_day_views")) / F.col("prev_day_views")) * 100)
            .otherwise(0))

    # 3. Rank by Volume (Top Viewed)
    vol_window = Window.partitionBy("date").orderBy(F.desc("daily_views"))
    df_vol = df_metrics.withColumn("rank_volume", F.row_number().over(vol_window))

    # 4. Rank by Growth (Top Trending)
    # Note: This will only rank pages that met our 500-view threshold
    trend_window = Window.partitionBy("date").orderBy(F.desc("view_delta_pct"))
    df_final = df_vol.withColumn("rank_trending", F.row_number().over(trend_window))

    # 5. Filter for the "Best of Both Worlds"
    # We keep a row if it's in the top 500 for volume OR top 500 for trending
    df_gold = df_final.filter((F.col("rank_volume") <= 500) | (F.col("rank_trending") <= 500))

    # 6. Save as a single optimized Parquet
    output_path = "s3://wikipulse/gold/final_dashboard_data.parquet"
    df_gold.repartition(1).write.mode("overwrite").parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    generate_gold_layer()
