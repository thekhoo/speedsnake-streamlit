# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`speedsnake-analysis` is a Python data analysis project for speedtest data stored as Parquet files. The project is in early stages — `main.py` is currently a stub.

## Always Do This

- When implementing a feature, always come up wiht a plan before making any changes
- You are a TDD style developer, always create unit tests to capture how you want the code to behave
- If requirements are not clear, always ask for clarity, do not guess
- Always commit in small chunks. Unit tests and linting must pass before committing
- Be clear and concise with code. Leave comments explaining why things are done if not obvious
- Add regular logging without being too verbose (i.e. at different checkpoints in the code)
- Write code in a reusable manner
- At the end of each plan, let the user know of any unresolved questions

## Never Do This
- Change main code when there are no tests that capture functionality; add unit tests before making any changes
- Do not hardcode any secrets or ARNs within the code; any secrets required should be taken from SSM
- Do not expose any secrets or tenanted information within the logs.

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
