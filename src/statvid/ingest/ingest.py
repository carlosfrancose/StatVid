"""Ingestion orchestration: fetch from API and write Bronze parquet."""
from __future__ import annotations

from typing import Iterable
from .youtube_client import YouTubeClient


def ingest_videos(video_ids: Iterable[str], output_dir: str) -> None:
    """Fetch metadata for given IDs and write to Bronze."""
    # Intentionally not implemented
    pass


def ingest_channel(channel_id: str, limit: int, output_dir: str) -> None:
    """Fetch recent uploads for a channel and write to Bronze."""
    # Intentionally not implemented
    pass

