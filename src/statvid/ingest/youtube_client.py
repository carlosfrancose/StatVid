"""Thin client for YouTube Data API v3 with retry/backoff."""
from __future__ import annotations

import logging
import random
import time
from typing import Iterable, Any, Dict, List, Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from ..config import get_config

MAX_IDS_PER_CALL = 50  # YouTube Data API limit for id-based list endpoints


class YouTubeClient:
    """Wrapper around google-api-python-client. Methods return raw dicts."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        max_retries: int = 3,
        backoff_seconds: float = 1.5,
    ) -> None:
        cfg = get_config()
        self.api_key = api_key or cfg.youtube_api_key
        if not self.api_key:
            raise ValueError("YOUTUBE_API_KEY is required to initialize YouTubeClient.")
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self._service_obj = None
        self.log = logging.getLogger(__name__)

    def _get_service(self):
        if self._service_obj is None:
            # cache_discovery False to avoid filesystem writes in potentially containerized environments
            self._service_obj = build("youtube", "v3", developerKey=self.api_key, cache_discovery=False)
        return self._service_obj

    def _execute(self, request, label: str) -> Dict[str, Any]:
        """Execute a YouTube request with simple exponential backoff."""
        for attempt in range(self.max_retries + 1):
            try:
                return request.execute()
            except HttpError as exc:
                status = getattr(exc.resp, "status", None)
                if status in (403, 500, 503) and attempt < self.max_retries:
                    sleep_for = self.backoff_seconds * (2**attempt) + random.random()
                    self.log.warning(
                        "YouTube %s failed with status %s. Retry %d/%d in %.1fs",
                        label,
                        status,
                        attempt + 1,
                        self.max_retries,
                        sleep_for,
                    )
                    time.sleep(sleep_for)
                    continue
                raise
        raise RuntimeError(f"Exceeded retries for YouTube request: {label}")

    def fetch_video_metadata(self, video_ids: Iterable[str]) -> List[Dict[str, Any]]:
        """Fetch snippet/statistics/contentDetails for a list of video IDs."""
        ids = [vid for vid in video_ids if vid]
        results: List[Dict[str, Any]] = []
        for i in range(0, len(ids), MAX_IDS_PER_CALL):
            chunk = ids[i : i + MAX_IDS_PER_CALL]
            req = self._get_service().videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(chunk),
                maxResults=len(chunk),
            )
            resp = self._execute(req, "videos.list")
            results.extend(resp.get("items", []))
        return results

    def fetch_channel_metadata(self, channel_ids: Iterable[str]) -> List[Dict[str, Any]]:
        """Fetch snippet/statistics/contentDetails for channel ids."""
        ids = [cid for cid in channel_ids if cid]
        results: List[Dict[str, Any]] = []
        for i in range(0, len(ids), MAX_IDS_PER_CALL):
            chunk = ids[i : i + MAX_IDS_PER_CALL]
            req = self._get_service().channels().list(
                part="snippet,statistics,contentDetails",
                id=",".join(chunk),
                maxResults=len(chunk),
            )
            resp = self._execute(req, "channels.list")
            results.extend(resp.get("items", []))
        return results

    def search_videos_by_category(
        self,
        category_id: int,
        *,
        region_code: str = "US",
        page_size: int = 50,
        max_pages: int = 5,
        order: str = "date",
        published_after: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Search videos for a category and return raw search items."""
        items: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        pages_fetched = 0
        while pages_fetched < max_pages:
            req = self._get_service().search().list(
                part="id,snippet",
                type="video",
                videoCategoryId=str(category_id),
                maxResults=page_size,
                regionCode=region_code,
                order=order,
                publishedAfter=published_after,
                pageToken=page_token,
            )
            resp = self._execute(req, f"search.list category={category_id}")
            items.extend(resp.get("items", []))
            page_token = resp.get("nextPageToken")
            pages_fetched += 1
            if not page_token:
                break
        return items

    def get_uploads_playlist_id(self, channel_id: str) -> Optional[str]:
        """Return uploads playlist id for a channel."""
        req = self._get_service().channels().list(
            part="contentDetails",
            id=channel_id,
            maxResults=1,
        )
        resp = self._execute(req, "channels.list(contentDetails)")
        items = resp.get("items", [])
        if not items:
            return None
        return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

    def fetch_playlist_items(
        self,
        playlist_id: str,
        *,
        page_size: int = 50,
        max_pages: int = 5,
    ) -> List[Dict[str, Any]]:
        """Fetch playlist items (e.g., uploads) with pagination."""
        items: List[Dict[str, Any]] = []
        token: Optional[str] = None
        pages = 0
        while pages < max_pages:
            req = self._get_service().playlistItems().list(
                part="snippet,contentDetails",
                playlistId=playlist_id,
                maxResults=page_size,
                pageToken=token,
            )
            resp = self._execute(req, "playlistItems.list")
            items.extend(resp.get("items", []))
            token = resp.get("nextPageToken")
            pages += 1
            if not token:
                break
        return items

    def search_channel_uploads(self, channel_id: str, limit: int = 50) -> List[str]:
        """Return recent video IDs for a channel."""
        uploads_playlist = self.get_uploads_playlist_id(channel_id)
        if not uploads_playlist:
            return []
        max_pages = max(1, limit // MAX_IDS_PER_CALL + 1)
        items = self.fetch_playlist_items(
            uploads_playlist,
            page_size=min(limit, MAX_IDS_PER_CALL),
            max_pages=max_pages,
        )
        video_ids: List[str] = []
        for item in items:
            vid = item.get("contentDetails", {}).get("videoId")
            if vid:
                video_ids.append(vid)
                if len(video_ids) >= limit:
                    break
        return video_ids
