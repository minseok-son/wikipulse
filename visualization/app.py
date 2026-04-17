from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from dashboard_utils import (
    aggregate_top_articles,
    daily_traffic,
    detect_data_path,
    filter_by_date,
    format_compact_number,
    DEFAULT_S3_PATH,
    local_data_candidates,
    load_dashboard_data,
    pareto_distribution,
    top_trending_articles,
)


st.set_page_config(page_title="WikiPulse Dashboard", layout="wide")
alt.data_transformers.disable_max_rows()


@st.cache_data(show_spinner=False)
def cached_load_data(data_path: str) -> pd.DataFrame:
    return load_dashboard_data(data_path)


def build_top_articles_chart(df: pd.DataFrame) -> alt.Chart:
    chart_df = df.copy()
    chart_df["label"] = chart_df["daily_views"].map(format_compact_number)

    bars = (
        alt.Chart(chart_df)
        .mark_bar(color="#2a6f97", cornerRadiusEnd=4)
        .encode(
            x=alt.X("daily_views:Q", title="Views"),
            y=alt.Y("page_title:N", sort="-x", title=None),
            tooltip=[
                alt.Tooltip("page_title:N", title="Article"),
                alt.Tooltip("daily_views:Q", title="Views", format=","),
            ],
        )
    )

    labels = bars.mark_text(align="left", dx=4).encode(text="label:N")
    return (bars + labels).properties(height=420)


def build_trending_chart(df: pd.DataFrame) -> alt.Chart:
    chart_df = df.copy()
    chart_df["label"] = chart_df["trend_score"].round(0).astype("Int64").astype(str)

    bars = (
        alt.Chart(chart_df)
        .mark_bar(color="#ee6c4d", cornerRadiusEnd=4)
        .encode(
            x=alt.X("trend_score:Q", title="Trend score"),
            y=alt.Y("page_title:N", sort="-x", title=None),
            tooltip=[
                alt.Tooltip("page_title:N", title="Article"),
                alt.Tooltip("daily_views:Q", title="Views", format=","),
                alt.Tooltip("prev_day_views:Q", title="Previous day", format=","),
                alt.Tooltip("abs_change:Q", title="Daily change", format=","),
                alt.Tooltip("trend_score:Q", title="Trend score", format=",.0f"),
                alt.Tooltip("date:T", title="Date"),
            ],
        )
    )

    labels = bars.mark_text(align="left", dx=4).encode(text="label:N")
    return (bars + labels).properties(height=420)


def build_traffic_chart(df: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(df)
        .mark_line(color="#264653", point=True)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("total_views:Q", title="Total views"),
            tooltip=[
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("total_views:Q", title="Views", format=","),
            ],
        )
        .properties(height=320)
    )


def build_pareto_chart(df: pd.DataFrame) -> alt.Chart:
    bars = (
        alt.Chart(df)
        .mark_bar(color="#8ab17d", opacity=0.9)
        .encode(
            x=alt.X("page_title:N", sort=None, title="Article"),
            y=alt.Y("daily_views:Q", title="Views"),
            tooltip=[
                alt.Tooltip("page_title:N", title="Article"),
                alt.Tooltip("daily_views:Q", title="Views", format=","),
                alt.Tooltip("cumulative_share:Q", title="Cumulative share", format=".1%"),
            ],
        )
    )

    line = (
        alt.Chart(df)
        .mark_line(color="#bc4749", point=True)
        .encode(
            x=alt.X("page_title:N", sort=None, title="Article"),
            y=alt.Y("cumulative_share:Q", title="Cumulative share", axis=alt.Axis(format="%")),
        )
    )

    return alt.layer(bars, line).resolve_scale(y="independent").properties(height=320)


def render_kpis(filtered_df: pd.DataFrame, traffic_df: pd.DataFrame) -> None:
    total_views = filtered_df["daily_views"].sum()
    unique_articles = filtered_df["page_title"].nunique()

    if traffic_df.empty:
        peak_day_text = "N/A"
    else:
        peak_day = traffic_df.loc[traffic_df["total_views"].idxmax()]
        peak_day_text = f"{peak_day['date'].date()} ({format_compact_number(peak_day['total_views'])})"

    if filtered_df.empty:
        top_article_text = "N/A"
    else:
        top_article = (
            filtered_df.groupby("page_title", as_index=False)["daily_views"]
            .sum()
            .sort_values("daily_views", ascending=False)
            .iloc[0]
        )
        top_article_text = f"{top_article['page_title']} ({format_compact_number(top_article['daily_views'])})"

    kpi_1, kpi_2, kpi_3, kpi_4 = st.columns(4)
    kpi_1.metric("Total Views", format_compact_number(total_views))
    kpi_2.metric("Unique Articles", f"{unique_articles:,}")
    kpi_3.metric("Peak Day", peak_day_text)
    kpi_4.metric("Top Article", top_article_text)


def main() -> None:
    st.title("WikiPulse Analytics Dashboard")
    st.caption(
        "Real-data dashboard for the Spark gold Parquet output."
    )

    detected_path = detect_data_path()

    st.sidebar.header("Data Source")
    data_path = st.sidebar.text_input(
        "Parquet path",
        value=detected_path or "",
        help=(
            "Use a local Parquet file during development. Later this can point to an "
            "S3 Parquet path once AWS credentials and bucket access are configured."
        ),
    )
    source_label = data_path

    if not data_path:
        st.warning("No dashboard dataset was found yet.")
        st.markdown("When real data is ready, place the Parquet file in one of these locations:")
        for candidate in local_data_candidates():
            st.code(candidate)
        st.markdown(
            f"For future AWS use, provide an explicit S3 path such as `{DEFAULT_S3_PATH}` once credentials are ready."
        )
        st.markdown(
            "See [REAL_DATA_SETUP.md](./REAL_DATA_SETUP.md) in the `visualization/` folder for the expected schema and update notes."
        )
        st.stop()

    try:
        df = cached_load_data(data_path)
    except FileNotFoundError:
        st.error(
            "Could not find the dashboard dataset. Set `WIKIPULSE_DATA_PATH` or place "
            "`final_dashboard_data.parquet` in `gold/`, `data/`, or the repo root."
        )
        st.markdown(
            "See [REAL_DATA_SETUP.md](./REAL_DATA_SETUP.md) in the `visualization/` folder for the expected schema and loading instructions."
        )
        st.stop()
    except Exception as exc:
        error_text = str(exc)
        if "ACCESS_DENIED" in error_text or "AccessDenied" in error_text:
            st.error(
                "The configured S3 path could not be read with the current AWS credentials."
            )
            st.markdown(
                "This dashboard is ready for S3-backed Parquet, but the environment still needs valid AWS access."
            )
            st.markdown(
                "See [REAL_DATA_SETUP.md](./REAL_DATA_SETUP.md) in the `visualization/` folder for the expected path, schema, and setup notes."
            )
            st.stop()
        st.error(f"Failed to load dashboard data from `{data_path}`: {exc}")
        st.stop()

    min_date = df["date"].min().date()
    max_date = df["date"].max().date()

    st.sidebar.header("Filters")
    selected_dates = st.sidebar.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    top_n = st.sidebar.slider("Top N", min_value=5, max_value=50, value=20, step=5)

    if isinstance(selected_dates, (tuple, list)) and len(selected_dates) == 2:
        start_date, end_date = selected_dates
    else:
        start_date = end_date = selected_dates

    filtered_df = filter_by_date(df, start_date, end_date)
    if filtered_df.empty:
        st.warning("No records are available for the selected date range.")
        st.stop()

    traffic_df = daily_traffic(filtered_df)
    top_articles_df = aggregate_top_articles(filtered_df, top_n)

    latest_day = filtered_df["date"].max()
    latest_day_df = filtered_df[filtered_df["date"] == latest_day].copy()
    trending_df = top_trending_articles(latest_day_df, top_n)
    pareto_df = pareto_distribution(filtered_df, top_n)

    render_kpis(filtered_df, traffic_df)
    st.markdown("")

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader(f"Top {top_n} Most Viewed Articles")
        st.altair_chart(build_top_articles_chart(top_articles_df), use_container_width=True)

    with col_right:
        st.subheader(f"Top {top_n} Trending Articles")
        st.caption(
            f"Trending uses the latest day in range ({latest_day.date()}) with "
            "`trend_score = (daily_views - prev_day_views) * log(1 + daily_views)`."
        )
        if trending_df.empty:
            st.info("No rows have both current and previous-day views for the selected range.")
        else:
            st.altair_chart(build_trending_chart(trending_df), use_container_width=True)

    st.subheader("Traffic Over Time")
    st.altair_chart(build_traffic_chart(traffic_df), use_container_width=True)

    st.subheader("View Distribution (Pareto)")
    st.caption(
        f"Shows how much of the selected range's traffic comes from the top {top_n} articles."
    )
    st.altair_chart(build_pareto_chart(pareto_df), use_container_width=True)

    with st.expander("View underlying data"):
        st.write(f"Data source: `{source_label}`")
        st.dataframe(
            filtered_df.sort_values(["date", "daily_views"], ascending=[False, False]),
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    main()
