"""Tests for dashboard_helpers module."""

import tempfile
from datetime import date, datetime, timezone

import polars as pl
import pytest

from dashboard_helpers import (
    GRANULARITY_OPTIONS,
    METRIC_COLS,
    TIME_COL,
    _ProfileContext,
    aggregate,
    build_cache_key,
    get_aggregated_data,
    get_or_create_cache_dir,
)


@pytest.fixture
def sample_df() -> pl.DataFrame:
    """Create a small DataFrame spanning 2 days with known values."""
    timestamps = [
        datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 2, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 6, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
    ]
    return pl.DataFrame(
        {
            "timestamp": timestamps,
            "download_mbps": [100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
            "upload_mbps": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            "ping_ms": [5.0, 10.0, 15.0, 20.0, 25.0, 30.0],
        }
    )


# --- build_cache_key ---


class TestBuildCacheKey:
    def test_deterministic(self):
        """Same inputs always produce the same key."""
        key1 = build_cache_key(date(2024, 1, 1), date(2024, 1, 31), "Hourly")
        key2 = build_cache_key(date(2024, 1, 1), date(2024, 1, 31), "Hourly")
        assert key1 == key2

    def test_different_granularity_different_key(self):
        """Different granularity produces different key."""
        key1 = build_cache_key(date(2024, 1, 1), date(2024, 1, 31), "Hourly")
        key2 = build_cache_key(date(2024, 1, 1), date(2024, 1, 31), "Daily")
        assert key1 != key2

    def test_different_dates_different_key(self):
        """Different date range produces different key."""
        key1 = build_cache_key(date(2024, 1, 1), date(2024, 1, 31), "Hourly")
        key2 = build_cache_key(date(2024, 2, 1), date(2024, 2, 28), "Hourly")
        assert key1 != key2

    def test_ends_with_csv(self):
        key = build_cache_key(date(2024, 1, 1), date(2024, 1, 31), "Hourly")
        assert key.endswith(".csv")


# --- get_or_create_cache_dir ---


class TestGetOrCreateCacheDir:
    def test_creates_directory(self):
        state = {}
        cache_dir = get_or_create_cache_dir(state)
        assert cache_dir is not None
        import os

        assert os.path.isdir(cache_dir)

    def test_reuses_existing(self):
        state = {}
        dir1 = get_or_create_cache_dir(state)
        dir2 = get_or_create_cache_dir(state)
        assert dir1 == dir2


# --- aggregate ---


class TestAggregate:
    def test_raw_returns_all_rows(self, sample_df):
        result = aggregate(sample_df, "Raw")
        assert len(result) == len(sample_df)
        assert TIME_COL in result.columns
        assert "timestamp" not in result.columns

    def test_raw_preserves_values(self, sample_df):
        result = aggregate(sample_df, "Raw")
        assert result[TIME_COL].to_list() == sample_df["timestamp"].sort().to_list()

    def test_raw_has_metric_columns(self, sample_df):
        result = aggregate(sample_df, "Raw")
        for col in METRIC_COLS:
            assert col in result.columns

    def test_hourly_groups_correctly(self, sample_df):
        """Each unique hour should produce one row."""
        result = aggregate(sample_df, "Hourly")
        # sample_df has 6 rows at 6 distinct hours → 6 groups
        assert len(result) == 6
        assert TIME_COL in result.columns

    def test_three_hourly_groups(self, sample_df):
        """3-hourly should merge hours 0,1,2 into one bucket."""
        result = aggregate(sample_df, "3-Hourly")
        # Day 1: hours 0-2 → bucket 0:00, hour 6 → bucket 6:00 (2 buckets)
        # Day 2: hour 0 → bucket 0:00, hour 12 → bucket 12:00 (2 buckets)
        assert len(result) == 4

    def test_daily_groups(self, sample_df):
        """Daily should produce one row per day."""
        result = aggregate(sample_df, "Daily")
        assert len(result) == 2

    def test_daily_averages_correctly(self, sample_df):
        """Check that daily aggregation computes the mean correctly."""
        result = aggregate(sample_df, "Daily")
        # Day 1 values: 100, 200, 300, 400 → mean = 250
        day1 = result.sort(TIME_COL).row(0, named=True)
        assert day1["download_mbps"] == pytest.approx(250.0)

    def test_result_is_sorted(self, sample_df):
        result = aggregate(sample_df, "Hourly")
        times = result[TIME_COL].to_list()
        assert times == sorted(times)

    def test_invalid_granularity_raises(self, sample_df):
        with pytest.raises(KeyError):
            aggregate(sample_df, "InvalidGranularity")


# --- get_aggregated_data ---


class TestGetAggregatedData:
    def test_cache_miss_then_hit(self, sample_df):
        with tempfile.TemporaryDirectory() as cache_dir:
            result1, hit1 = get_aggregated_data(
                sample_df, date(2024, 1, 1), date(2024, 1, 2), "Hourly", cache_dir
            )
            assert hit1 is False
            assert len(result1) > 0

            result2, hit2 = get_aggregated_data(
                sample_df, date(2024, 1, 1), date(2024, 1, 2), "Hourly", cache_dir
            )
            assert hit2 is True
            assert result1.shape == result2.shape

    def test_different_granularity_no_cache_hit(self, sample_df):
        with tempfile.TemporaryDirectory() as cache_dir:
            _, hit1 = get_aggregated_data(
                sample_df, date(2024, 1, 1), date(2024, 1, 2), "Hourly", cache_dir
            )
            assert hit1 is False

            _, hit2 = get_aggregated_data(
                sample_df, date(2024, 1, 1), date(2024, 1, 2), "Daily", cache_dir
            )
            assert hit2 is False


# --- _ProfileContext ---


class TestProfileContext:
    def test_measures_elapsed_time(self):
        with _ProfileContext("test") as p:
            # Small sleep to ensure measurable time
            import time

            time.sleep(0.01)
        assert p.elapsed > 0

    def test_label_stored(self):
        ctx = _ProfileContext("my_label")
        assert ctx.label == "my_label"
