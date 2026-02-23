# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`speedsnake-analysis` is a Python data analysis project for speedtest data stored as Parquet files. The project is in early stages — `main.py` is currently a stub.

## Commands

This project uses [uv](https://docs.astral.sh/uv/) for package management.

```bash
# Run the main script
uv run main.py

# Add a dependency
uv add <package>

# Run a Python script with uv
uv run python <script.py>
```

## Data Structure

Speedtest data is stored under `uploads/` in Hive-style partitioned Parquet format:

```
uploads/
  location=<location>/
    year=<YYYY>/
      month=<MM>/
        day=<DD>/
          speedtest_001.parquet
```

The `uploads/` directory is git-ignored. When reading data, use the partition columns (`location`, `year`, `month`, `day`) to filter efficiently — libraries like `polars` or `pyarrow` can scan this layout natively.

## Python Version

Python 3.13 (see `.python-version`).
