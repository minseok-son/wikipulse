from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys


def process_day_to_silver_local(year, month, day):
    spark = (
        SparkSession.builder
        .appName(f"WikiIngestion-Silver-Local-{year}-{month}-{day}")
        .getOrCreate()
    )

    input_path = f"../local_bronze/day{day:02d}/*.gz"
    output_path = f"../local_silver/day={day:02d}"

    df_raw = (
        spark.read.format("csv")
        .option("sep", " ")
        .load(input_path)
        .toDF("domain_code", "page_title", "count_views", "response_size")
    )

    unwanted_pattern = r"^(Special:|File:|Category:|User:|Talk:|Wikipedia:|Help:|\-|Portal:|Draft:|Main_Page)"

    df_clean = (
        df_raw
        .filter(
            (F.col("domain_code").isin("en", "en.m")) &
            (~F.col("page_title").rlike(unwanted_pattern))
        )
        .withColumn("views", F.col("count_views").cast("long"))
        .withColumn("year", F.lit(year).cast("int"))
        .withColumn("month", F.lit(month).cast("int"))
        .withColumn("day", F.lit(day).cast("int"))
    )

    df_clean = df_clean.withColumn(
        "date",
        F.to_date(
            F.concat_ws(
                "-",
                F.col("year"),
                F.lpad(F.col("month").cast("string"), 2, "0"),
                F.lpad(F.col("day").cast("string"), 2, "0")
            )
        )
    )

    df_silver = (
        df_clean
        .groupBy("year", "month", "day", "date", "page_title")
        .agg(F.sum("views").alias("daily_views"))
    )

    print("RAW SAMPLE")
    df_raw.show(10, truncate=False)

    print("CLEAN SAMPLE")
    df_clean.show(10, truncate=False)

    print("SILVER SAMPLE")
    df_silver.orderBy(F.desc("daily_views")).show(20, truncate=False)

    print("raw rows:", df_raw.count())
    print("clean rows:", df_clean.count())
    print("silver rows:", df_silver.count())

    (
        df_silver.write
        .mode("overwrite")
        .parquet(output_path)
    )

    spark.stop()


if __name__ == "__main__":
    y = int(sys.argv[1])
    m = int(sys.argv[2])
    d = int(sys.argv[3])
    process_day_to_silver_local(y, m, d)
