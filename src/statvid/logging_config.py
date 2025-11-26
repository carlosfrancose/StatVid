"""Structured logging configuration."""
from __future__ import annotations

import logging
import sys
from .config import get_config


def configure_logging() -> None:
    cfg = get_config()
    level = getattr(logging, cfg.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

