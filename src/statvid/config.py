"""Configuration loading via environment variables with sane defaults."""
from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class AppConfig:
    data_dir: str = os.getenv("DATA_DIR", "./data")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    youtube_api_key: str | None = os.getenv("YOUTUBE_API_KEY")


def get_config() -> AppConfig:
    """Return loaded app configuration."""
    return AppConfig()

