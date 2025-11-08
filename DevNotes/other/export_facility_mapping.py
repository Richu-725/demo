"""Utility script to extract facility metadata from cached parquet files.

Usage:
    python export_facility_mapping.py [--cache-dir data/cache] [--output DevNotes/facility_mapping.csv]

The script looks for `facilities_catalog.parquet` in the cache directory. If it
does not exist, it falls back to the most recent metrics parquet and extracts
the facility columns from there. The resulting CSV contains facility IDs,
names, fuels, and states, making it easy to inspect the catalogue without
opening the parquet manually.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd


def _find_latest_parquet(files: Iterable[Path]) -> Path:
    try:
        return max(files, key=lambda path: path.stat().st_mtime)
    except ValueError as exc:
        raise FileNotFoundError("No parquet files found in the cache directory.") from exc


def _load_facility_dataframe(cache_dir: Path) -> pd.DataFrame:
    catalogue_path = cache_dir / "facilities_catalog.parquet"
    if catalogue_path.exists():
        df = pd.read_parquet(catalogue_path)
        if df.empty:
            raise RuntimeError(f"Facility catalogue at {catalogue_path} is empty.")
        return df

    parquet_files = list(cache_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in cache directory {cache_dir}.")

    latest = _find_latest_parquet(parquet_files)
    df = pd.read_parquet(latest)
    if df.empty:
        raise RuntimeError(f"Latest parquet {latest} is empty; cannot derive facility metadata.")

    required_cols = {"facility_id", "name", "fuel", "state"}
    if not required_cols.issubset(df.columns):
        missing = required_cols.difference(df.columns)
        raise RuntimeError(
            f"Parquet {latest} missing required columns {missing}; run a full build to populate metadata."
        )
    return df[list(required_cols)]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Export facility ID-to-name mapping from cached parquet files.")
    parser.add_argument("--cache-dir", default="data/cache", help="Directory containing cached parquet files.")
    parser.add_argument(
        "--output",
        default="DevNotes/facility_mapping.csv",
        help="Destination CSV path for the facility mapping.",
    )
    args = parser.parse_args(argv)

    cache_dir = Path(args.cache_dir).resolve()
    if not cache_dir.exists():
        parser.error(f"Cache directory {cache_dir} does not exist.")

    df = _load_facility_dataframe(cache_dir)
    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_sorted = df.sort_values("facility_id").reset_index(drop=True)
    df_sorted.to_csv(output_path, index=False)
    print(f"Wrote facility mapping to {output_path} (rows={len(df_sorted)})")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover - convenience script
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
