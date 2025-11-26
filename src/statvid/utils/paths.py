"""Centralized path helpers for data lake directories."""
from __future__ import annotations

import os
from dataclasses import dataclass
from ..config import get_config


@dataclass(frozen=True)
class DataPaths:
    base: str
    bronze: str
    silver: str
    gold: str
    interim: str
    external: str


def get_paths() -> DataPaths:
    cfg = get_config()
    base = os.path.abspath(cfg.data_dir)
    return DataPaths(
        base=base,
        bronze=os.path.join(base, "bronze"),
        silver=os.path.join(base, "silver"),
        gold=os.path.join(base, "gold"),
        interim=os.path.join(base, "interim"),
        external=os.path.join(base, "external"),
    )

