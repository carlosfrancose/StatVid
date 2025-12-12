"""I/O helpers for Parquet files and safe writes."""
from __future__ import annotations

from typing import Any
import os
import pandas as pd


def ensure_dir(path: str) -> None:
    """Create directory if it does not exist."""
    os.makedirs(path, exist_ok=True)


def write_parquet(df: "Any", path: str) -> None:
    """Write dataframe to parquet at path. Placeholder."""
    dirpath = os.path.dirname(path) or "."
    ensure_dir(dirpath)
    df.to_parquet(path, index=False)


def read_parquet(path: str) -> "Any":
    """Read parquet into a dataframe. Placeholder."""
    return pd.read_parquet(path)
