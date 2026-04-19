from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def generate_gold_layer():
    spark = SparkSession.builder.appName("Gold-Local").getOrCreate()

    df_silver = spark.read.parquet("../local_silver/")

    window_spec = Window.partitionBy("page_title").orderBy("date")

    df_metrics = (
        df_silver
        .withColumn("prev_day_views", F.lag("daily_views").over(window_spec))
        .withColumn("abs_change", F.col("daily_views") - F.col("prev_day_views"))
        .withColumn(
            "trend_score",
            F.when(
                F.col("prev_day_views").isNotNull(),
                F.col("abs_change") * F.log1p(F.col("daily_views"))
            )
        )
    )

    vol_window = Window.partitionBy("date").orderBy(F.desc("daily_views"))
    trend_window = Window.partitionBy("date").orderBy(F.desc("trend_score"))

    df_gold = (
        df_metrics
        .withColumn("rank_volume", F.row_number().over(vol_window))
        .withColumn("rank_trending", F.row_number().over(trend_window))
    )

    print("GOLD SAMPLE")
    df_gold.show(20, truncate=False)

    output_path = "../local_gold"

    df_gold.write.mode("overwrite").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    generate_gold_layer()
