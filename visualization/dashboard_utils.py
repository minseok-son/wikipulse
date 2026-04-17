from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_LOCAL_PATHS = (
    "gold/final_dashboard_data.parquet",
    "data/final_dashboard_data.parquet",
    "final_dashboard_data.parquet",
)
DEFAULT_S3_PATH = "s3://wikipulse/gold/final_dashboard_data.parquet"


def local_data_candidates() -> tuple[str, ...]:
    """Return the likely local parquet locations for the dashboard dataset."""
    return DEFAULT_LOCAL_PATHS


def detect_data_path() -> str | None:
    """Return a configured or local data path, without assuming S3 access."""
    configured_path = os.getenv("WIKIPULSE_DATA_PATH")
    if configured_path:
        return configured_path

    for candidate in DEFAULT_LOCAL_PATHS:
        if Path(candidate).exists():
            return candidate

    return None


def load_dashboard_data(data_path: str) -> pd.DataFrame:
    """Load the gold parquet and normalize the columns used by Streamlit."""
    df = pd.read_parquet(data_path)
    return prepare_dashboard_data(df)


def prepare_dashboard_data(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize the dataframe so the dashboard can work with minor schema drift."""
    normalized = df.copy()

    if "date" not in normalized.columns:
        partition_cols = {"year", "month", "day"}
        if partition_cols.issubset(normalized.columns):
            normalized["date"] = pd.to_datetime(
                {
                    "year": normalized["year"],
                    "month": normalized["month"],
                    "day": normalized["day"],
                },
                errors="coerce",
            )
        else:
            raise ValueError(
                "The dashboard needs a 'date' column or year/month/day partition columns."
            )

    required_columns = {"page_title", "daily_views", "date"}
    missing = required_columns.difference(normalized.columns)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(f"Missing required columns: {missing_list}")

    normalized["page_title"] = normalized["page_title"].astype(str)
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce").dt.normalize()
    normalized["daily_views"] = pd.to_numeric(
        normalized["daily_views"], errors="coerce"
    ).fillna(0)

    if "prev_day_views" not in normalized.columns:
        normalized = normalized.sort_values(["page_title", "date"])
        normalized["prev_day_views"] = normalized.groupby("page_title")["daily_views"].shift(1)

    normalized["prev_day_views"] = pd.to_numeric(
        normalized["prev_day_views"], errors="coerce"
    )
    normalized["abs_change"] = normalized["daily_views"] - normalized["prev_day_views"]
    normalized["trend_score"] = np.where(
        normalized["prev_day_views"].notna(),
        normalized["abs_change"] * np.log1p(normalized["daily_views"]),
        np.nan,
    )

    normalized = normalized.dropna(subset=["date", "page_title"]).copy()
    normalized = normalized.sort_values(["date", "daily_views"], ascending=[True, False])

    if "rank_volume" not in normalized.columns:
        normalized["rank_volume"] = normalized.groupby("date")["daily_views"].rank(
            method="first", ascending=False
        )

    if "rank_trending" not in normalized.columns:
        normalized["rank_trending"] = normalized.groupby("date")["trend_score"].rank(
            method="first", ascending=False, na_option="bottom"
        )

    return normalized


def filter_by_date(df: pd.DataFrame, start_date, end_date) -> pd.DataFrame:
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    return df[(df["date"] >= start_ts) & (df["date"] <= end_ts)].copy()


def aggregate_top_articles(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    return (
        df.groupby("page_title", as_index=False)["daily_views"]
        .sum()
        .sort_values("daily_views", ascending=False)
        .head(top_n)
    )


def top_trending_articles(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    eligible = df[df["trend_score"].notna()].copy()
    return eligible.sort_values("trend_score", ascending=False).head(top_n)


def daily_traffic(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("date", as_index=False)["daily_views"]
        .sum()
        .sort_values("date")
        .rename(columns={"daily_views": "total_views"})
    )


def pareto_distribution(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    total_views = df["daily_views"].sum()
    pareto = aggregate_top_articles(df, top_n).copy()
    if total_views <= 0:
        pareto["cumulative_share"] = 0.0
        return pareto

    pareto["cumulative_views"] = pareto["daily_views"].cumsum()
    pareto["cumulative_share"] = pareto["cumulative_views"] / total_views
    return pareto


def format_compact_number(value: float) -> str:
    if pd.isna(value):
        return "N/A"

    abs_value = abs(value)
    if abs_value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs_value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:,.0f}"
