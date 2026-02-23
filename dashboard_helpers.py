"""Helpers for the Speedsnake Streamlit dashboard.

Provides granularity-based aggregation, CSV caching, and profiling utilities.
"""

import atexit
import hashlib
import logging
import shutil
import tempfile
import time
from datetime import date
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

# Granularity options: label -> Polars truncate interval (None = raw)
GRANULARITY_OPTIONS: dict[str, str | None] = {
    "Raw": None,
    "Hourly": "1h",
    "3-Hourly": "3h",
    "6-Hourly": "6h",
    "12-Hourly": "12h",
    "Daily": "1d",
}

METRIC_COLS = ["download_mbps", "upload_mbps", "ping_ms"]
TIME_COL = "time"


def build_cache_key(start_date: date, end_date: date, granularity: str) -> str:
    """Return a deterministic filename for a given query combination."""
    raw = f"{start_date.isoformat()}|{end_date.isoformat()}|{granularity}"
    digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return f"{digest}.csv"


def get_or_create_cache_dir(session_state: dict) -> str:
    """Return a session-scoped temp directory, creating it if needed.

    Registers an atexit handler on first creation so the directory
    is cleaned up when the process exits.
    """
    key = "_cache_dir"
    if key not in session_state:
        cache_dir = tempfile.mkdtemp(prefix="speedsnake_cache_")
        session_state[key] = cache_dir
        atexit.register(shutil.rmtree, cache_dir, ignore_errors=True)
        logger.info("Created cache directory: %s", cache_dir)
    return session_state[key]


def aggregate(
    df: pl.DataFrame,
    granularity: str,
) -> pl.DataFrame:
    """Aggregate a filtered DataFrame according to the chosen granularity.

    Args:
        df: Filtered DataFrame with a 'timestamp' column and metric columns.
        granularity: One of the keys in GRANULARITY_OPTIONS.

    Returns:
        DataFrame with a 'time' column and aggregated metric columns.
    """
    interval = GRANULARITY_OPTIONS[granularity]

    if interval is None:
        # Raw: just rename timestamp -> time, keep metric cols
        return df.select(
            pl.col("timestamp").alias(TIME_COL),
            *[pl.col(c) for c in METRIC_COLS],
        ).sort(TIME_COL)

    return (
        df.with_columns(pl.col("timestamp").dt.truncate(interval).alias(TIME_COL))
        .group_by(TIME_COL)
        .agg(*[pl.col(c).mean() for c in METRIC_COLS])
        .sort(TIME_COL)
    )


def get_aggregated_data(
    df: pl.DataFrame,
    start_date: date,
    end_date: date,
    granularity: str,
    cache_dir: str,
) -> tuple[pl.DataFrame, bool]:
    """Return aggregated data, using CSV cache if available.

    Args:
        df: Filtered DataFrame.
        start_date: Query start date (for cache key).
        end_date: Query end date (for cache key).
        granularity: Granularity label.
        cache_dir: Path to session cache directory.

    Returns:
        Tuple of (result DataFrame, cache_hit bool).
    """
    cache_file = Path(cache_dir) / build_cache_key(start_date, end_date, granularity)

    if cache_file.exists():
        logger.info("Cache hit: %s", cache_file.name)
        result = pl.read_csv(cache_file, try_parse_dates=True)
        return result, True

    logger.info("Cache miss: computing aggregation for %s", granularity)
    result = aggregate(df, granularity)
    result.write_csv(cache_file)
    return result, False


def profiled(label: str):
    """Context manager that measures elapsed time for a code block.

    Usage:
        with profiled("Data load") as p:
            do_work()
        print(p.elapsed)  # seconds as float
    """
    return _ProfileContext(label)


class _ProfileContext:
    """Simple profiling context manager."""

    def __init__(self, label: str):
        self.label = label
        self.elapsed: float = 0.0

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.elapsed = time.perf_counter() - self._start
        logger.info("%s took %.3fs", self.label, self.elapsed)
        return False
