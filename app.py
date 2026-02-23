import streamlit as st
import polars as pl
import plotly.express as px
from datetime import datetime, timezone


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


df = load_data()

min_date = df["timestamp"].min().date()
max_date = df["timestamp"].max().date()

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start date", value=min_date, min_value=min_date, max_value=max_date)
with col2:
    end_date = st.date_input("End date", value=max_date, min_value=min_date, max_value=max_date)

filtered = df.filter(
    (pl.col("timestamp") >= datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc))
    & (pl.col("timestamp") <= datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc))
)

hourly = (
    filtered
    .with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
    .group_by("hour")
    .agg(
        pl.col("download_mbps").mean(),
        pl.col("upload_mbps").mean(),
        pl.col("ping_ms").mean(),
    )
    .sort("hour")
)

st.subheader("Download & Upload Speed (Higher is better)")
speed_long = hourly.unpivot(
    index="hour",
    on=["download_mbps", "upload_mbps"],
    variable_name="metric",
    value_name="Mbps",
)
fig_speed = px.line(
    speed_long.to_pandas(),
    x="hour",
    y="Mbps",
    color="metric",
    labels={"hour": "Time", "metric": "Metric"},
)
st.plotly_chart(fig_speed, use_container_width=True)

st.subheader("Ping Latency (Lower is better)")
fig_ping = px.line(
    hourly.to_pandas(),
    x="hour",
    y="ping_ms",
    labels={"hour": "Time", "ping_ms": "Ping (ms)"},
)
st.plotly_chart(fig_ping, use_container_width=True)
