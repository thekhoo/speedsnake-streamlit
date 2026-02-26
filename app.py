import streamlit as st
import polars as pl
import plotly.express as px
from datetime import datetime, timezone

from dashboard_helpers import (
    GRANULARITY_OPTIONS,
    TIME_COL,
    get_aggregated_data,
    get_or_create_cache_dir,
    profiled,
)


st.set_page_config(page_title="Speedsnake", layout="wide")
st.title("Speedtest Dashboard")


@st.cache_data
def load_data() -> pl.DataFrame:
    import glob

    frames = []
    for path in glob.glob("uploads/**/*.parquet", recursive=True):
        frame = pl.read_parquet(path, hive_partitioning=True).select(
            pl.col("timestamp").cast(pl.Datetime("us", "UTC")),
            pl.col("download").cast(pl.Float64),
            pl.col("upload").cast(pl.Float64),
            pl.col("ping").cast(pl.Float64),
        )
        frames.append(frame)
    df = pl.concat(frames)
    return df.with_columns(
        (pl.col("download") / 1_000_000).alias("download_mbps"),
        (pl.col("upload") / 1_000_000).alias("upload_mbps"),
        pl.col("ping").alias("ping_ms"),
    )


with profiled("Data load") as p_load:
    df = load_data()

min_date = df["timestamp"].min().date()
max_date = df["timestamp"].max().date()

col1, col2, col3 = st.columns(3)
with col1:
    start_date = st.date_input("Start date", value=min_date, min_value=min_date, max_value=max_date)
with col2:
    end_date = st.date_input("End date", value=max_date, min_value=min_date, max_value=max_date)
with col3:
    granularity = st.selectbox(
        "Granularity",
        options=list(GRANULARITY_OPTIONS.keys()),
        index=1,  # Default to "Hourly"
    )

filtered = df.filter(
    (pl.col("timestamp") >= datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc))
    & (pl.col("timestamp") <= datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc))
)

cache_dir = get_or_create_cache_dir(st.session_state)

with profiled("Filter + aggregation") as p_agg:
    aggregated, cache_hit = get_aggregated_data(
        filtered, start_date, end_date, granularity, cache_dir
    )

# Profiling info
with st.expander("Profiling"):
    st.caption(f"Data load: {p_load.elapsed:.3f}s")
    st.caption(f"Filter + aggregation: {p_agg.elapsed:.3f}s")
    st.caption(f"Cache: {'hit' if cache_hit else 'miss'}")

st.subheader("Download & Upload Speed (Higher is better)")
speed_long = aggregated.unpivot(
    index=TIME_COL,
    on=["download_mbps", "upload_mbps"],
    variable_name="metric",
    value_name="Mbps",
)
fig_speed = px.line(
    speed_long.to_pandas(),
    x=TIME_COL,
    y="Mbps",
    color="metric",
    labels={TIME_COL: "Time", "metric": "Metric"},
)
st.plotly_chart(fig_speed, use_container_width=True)

st.subheader("Ping Latency (Lower is better)")
ping_long = aggregated.unpivot(
    index=TIME_COL,
    on=["ping_ms"],
    variable_name="metric",
    value_name="Ping (ms)",
)
fig_ping = px.line(
    ping_long.to_pandas(),
    x=TIME_COL,
    y="Ping (ms)",
    color="metric",
    labels={TIME_COL: "Time", "metric": "Metric"},
)
st.plotly_chart(fig_ping, use_container_width=True)
