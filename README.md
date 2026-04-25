# WikiPulse

WikiPulse is a Wikimedia pageview analytics pipeline. It ingests hourly Wikimedia pageview dumps, stores the raw files in a bronze layer, aggregates English article traffic into a silver layer, computes dashboard-ready rankings in a gold layer, and serves the final output in a Streamlit dashboard.

## Pipeline Overview

```text
Wikimedia pageview dumps
        |
        v
ingestion/uploader.py
        |
        v
S3 bronze: s3://wikipulse/bronze/year=YYYY/month=MM/day=DD/*.gz
        |
        v
processing/silver_processing.py
        |
        v
S3 silver: s3://wikipulse/silver/year=YYYY/month=MM/day=DD/*.parquet
        |
        v
processing/gold_processing.py
        |
        v
S3 gold: s3://wikipulse/gold/final_dashboard_data.parquet
        |
        v
visualization/app.py
```

A local development path is also available through `local_bronze/`, `local_silver/`, and `local_gold/`.

## Repository Layout

```text
ingestion/
  uploader.py             # Streams Wikimedia pageview files into S3 bronze
  ingestion_tmp.py        # Test variant that only uploads the first 5 files
processing/
  silver_processing.py    # S3 bronze -> S3 silver monthly Spark job
  silver_job_local.py     # Local bronze -> local silver daily Spark job
  gold_processing.py      # S3 silver -> S3 gold dashboard dataset
  gold_job_local.py       # Local silver -> local gold dashboard dataset
visualization/
  app.py                  # Streamlit dashboard
  dashboard_utils.py      # Dashboard data loading and metric helpers
  REAL_DATA_SETUP.md      # Extra dashboard data-source notes
requirements.txt          # Python dependencies
```

## Data Input

The source data comes from Wikimedia's public pageview dumps:

```text
https://dumps.wikimedia.org/other/pageviews/YYYY/YYYY-MM/
```

Each hourly file is named like:

```text
pageviews-20260301-010000.gz
```

Each row in the raw file is space-delimited and is read with this schema:

```text
domain_code page_title count_views response_size
```

The pipeline keeps English desktop and mobile traffic only:

```text
domain_code in ("en", "en.m")
```

It filters out non-article namespaces and utility pages such as `Special:`, `File:`, `Category:`, `User:`, `Talk:`, `Wikipedia:`, `Help:`, `Portal:`, `Draft:`, and `Main_Page`.

## Setup

Create and activate a Python environment, then install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

For S3-backed runs, configure AWS credentials with access to the target bucket:

```bash
aws configure
```

The current S3 scripts use this bucket path:

```text
s3://wikipulse
```

If you use a different bucket, update the bucket name in `ingestion/uploader.py`, `processing/silver_processing.py`, and `processing/gold_processing.py`.

## Walkthrough: Input To Final Output

### 1. Ingest Wikimedia Dumps To Bronze

Run the uploader:

```bash
python ingestion/uploader.py
```

By default, `uploader.py` is configured for March 2026:

```python
BUCKET = "wikipulse"
YEAR = 2026
MONTH = 3
```

For each file found on the Wikimedia monthly dump page, the ingestor streams the `.gz` file directly to S3 without saving it locally. The date is parsed from the filename and written using Hive-style partitions:

```text
s3://wikipulse/bronze/year=2026/month=03/day=01/pageviews-20260301-010000.gz
```

Use `ingestion/ingestion_tmp.py` if you only want to upload the first 5 files for a small test run.

### 2. Transform Bronze To Silver

Run the monthly Spark job:

```bash
spark-submit processing/silver_processing.py 2026 3
```

The silver job reads all raw hourly files for the month:

```text
s3://wikipulse/bronze/year=2026/month=03/*/*.gz
```

It then:

1. Reads the raw space-delimited pageview rows.
2. Extracts `day` from the input file path.
3. Filters to English desktop/mobile articles.
4. Casts `count_views` to numeric `views`.
5. Groups by `day` and `page_title`.
6. Writes daily article totals as partitioned Parquet.

Silver output path:

```text
s3://wikipulse/silver/year=2026/month=03/day=DD/*.parquet
```

Silver records contain the daily article-level metric:

```text
day, page_title, daily_views
```

When Spark reads the partitioned dataset, `year`, `month`, and `day` are also available from the directory structure.

### 3. Transform Silver To Gold

Run the gold Spark job:

```bash
spark-submit processing/gold_processing.py
```

The gold job reads:

```text
s3://wikipulse/silver/
```

It currently filters to March 2026 in code:

```python
(F.col("year") == 2026) & (F.col("month") == 3)
```

It then creates dashboard-ready metrics:

```text
date              # built from year, month, day
prev_day_views    # previous daily_views for the same page_title
view_delta_pct    # percent growth when previous-day views >= 500
rank_volume       # daily rank by total views
rank_trending     # daily rank by percent growth
```

The output keeps rows that are either in the top 500 by volume or top 500 by trending rank for a given day.

Gold output path:

```text
s3://wikipulse/gold/final_dashboard_data.parquet
```

This is the final dataset consumed by the dashboard.

### 4. Run The Dashboard

Start Streamlit from the repository root:

```bash
streamlit run visualization/app.py
```

The dashboard looks for a gold Parquet dataset in this order:

1. `WIKIPULSE_DATA_PATH`, if set.
2. `gold/final_dashboard_data.parquet`
3. `data/final_dashboard_data.parquet`
4. `final_dashboard_data.parquet`

For S3-backed dashboard loading, set:

```bash
export WIKIPULSE_DATA_PATH=s3://wikipulse/gold/final_dashboard_data.parquet
streamlit run visualization/app.py
```

For local development, place the final Parquet output at one of the local paths above or point directly to it:

```bash
export WIKIPULSE_DATA_PATH=/full/path/to/final_dashboard_data.parquet
streamlit run visualization/app.py
```

The dashboard renders:

- Total views, unique article count, peak day, and top article KPIs
- Top N most viewed articles
- Top N trending articles
- Traffic over time
- Pareto view distribution
- A table of the underlying filtered rows

## Local Development Flow

Use the local Spark jobs when working without S3.

Expected local input layout:

```text
local_bronze/day01/*.gz
local_bronze/day02/*.gz
```

The local scripts use `../local_*` paths, so run them from the `processing/` directory:

Generate silver for one day:

```bash
cd processing
spark-submit silver_job_local.py 2026 3 1
```

This reads:

```text
../local_bronze/day01/*.gz
```

and writes:

```text
../local_silver/day=01
```

Generate local gold output:

```bash
spark-submit gold_job_local.py
```

This reads:

```text
../local_silver/
```

and writes:

```text
../local_gold
```

Then point the dashboard at the local gold dataset:

```bash
export WIKIPULSE_DATA_PATH=/full/path/to/local_gold
streamlit run visualization/app.py
```

## Final Output Contract

The dashboard requires these columns:

```text
page_title    string
daily_views   numeric
date          date or timestamp
```

These columns are optional but preferred:

```text
prev_day_views
rank_volume
rank_trending
```

If `date` is missing, `visualization/dashboard_utils.py` can derive it from `year`, `month`, and `day`. If `prev_day_views` is missing, the dashboard derives it by sorting each article by date and applying a one-day lag.

## Notes And Current Assumptions

- The production scripts are currently hard-coded to bucket `wikipulse`.
- The gold S3 job is currently hard-coded to March 2026.
- `ingestion_tmp.py` is a limited test uploader; `uploader.py` is the full-month uploader.
- Local data and Spark output directories are ignored by Git.
- The dashboard can read either a Parquet file or a Parquet directory, depending on how Spark writes the output.
