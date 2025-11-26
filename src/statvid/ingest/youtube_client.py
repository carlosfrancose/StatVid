"""Thin client for YouTube Data API v3."""
from __future__ import annotations

from typing import Iterable, Any, Dict, List, Optional


class YouTubeClient:
    """Wrapper around google-api-python-client. Methods return raw dicts."""

    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key = api_key

    def fetch_video_metadata(self, video_ids: Iterable[str]) -> List[Dict[str, Any]]:
        """Fetch metadata for a list of video IDs."""
        # Intentionally not implemented
        return []

    def search_channel_uploads(self, channel_id: str, limit: int = 50) -> List[str]:
        """Return recent video IDs for a channel."""
        # Intentionally not implemented
        return []

