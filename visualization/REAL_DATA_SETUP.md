# Real Data Setup Notes

This dashboard is now real-data-only. It assumes the visualization layer will eventually read the Spark gold output from a Parquet dataset, either from a local file during development or from S3 in the deployed version.

## Expected Data Source

Preferred dataset:

- `gold/final_dashboard_data.parquet`
- or another Parquet path passed through the sidebar input
- or `WIKIPULSE_DATA_PATH`

Current intended long-term S3 target:

- `s3://wikipulse/gold/final_dashboard_data.parquet`

## Expected Schema

The dashboard is built around these columns:

Required:

- `page_title`: string
- `daily_views`: numeric
- `date`: date or timestamp

Optional but strongly preferred:

- `prev_day_views`: numeric
- `rank_volume`: numeric
- `rank_trending`: numeric

Fallback-supported:

- `year`, `month`, `day`

If `date` is missing, the dashboard will try to build it from `year`, `month`, and `day`.

If `prev_day_views` is missing, the dashboard will derive it by sorting by `page_title` and `date` and using a one-day lag.

## Current Dashboard Metrics

The dashboard currently supports:

- Top N most viewed articles
- Top N trending articles
- Traffic over time
- Pareto view distribution

Trending uses:

`trend_score = (daily_views - prev_day_views) * log(1 + daily_views)`

This is intentionally simple for the class project. It rewards both growth and scale, which is better than raw percent change for avoiding tiny-baseline spikes.

## Real Data Loading

### Local development

Put the Parquet file in one of:

- `gold/final_dashboard_data.parquet`
- `data/final_dashboard_data.parquet`
- `final_dashboard_data.parquet`

Or set:

```bash
export WIKIPULSE_DATA_PATH=/full/path/to/final_dashboard_data.parquet
```

### Future S3 usage

Later, when AWS access is ready, set:

```bash
export WIKIPULSE_DATA_PATH=s3://wikipulse/gold/final_dashboard_data.parquet
```

The local environment will also need valid AWS credentials. Typical options:

- `aws configure`
- environment variables such as `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
- an AWS profile if pandas/pyarrow/s3fs are configured to use it

If the app shows `ACCESS_DENIED`, the issue is credentials or bucket permissions, not the dashboard code.

## If the Spark Output Changes

If the gold dataset shape changes, check these areas first:

- [dashboard_utils.py](./dashboard_utils.py): schema normalization and derived fields
- [app.py](./app.py): chart assumptions and labels

Common updates:

1. If the output is partitioned instead of a single Parquet object:
   Point `WIKIPULSE_DATA_PATH` at the Parquet directory, not a single file.

2. If `date` is renamed:
   Update `prepare_dashboard_data()` in `dashboard_utils.py`.

3. If article title is renamed:
   Update the required columns in `prepare_dashboard_data()`.

4. If trending is precomputed differently in Spark:
   Replace the pandas-derived `trend_score` logic with the Spark column.

5. If the dashboard should use a broader dataset than top-500:
   No visualization rewrite is required, but performance and chart defaults may need tuning.

## If More Time Is Available

Reasonable next improvements:

- Move S3 path and environment settings into a small config file
- Add a separate article detail view
- Add a real data freshness indicator
- Support larger datasets with filtering before full dataframe loading
- Align dashboard metric names exactly with the final Spark output contract
