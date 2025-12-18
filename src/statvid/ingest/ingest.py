"""Ingestion orchestration: fetch from API and write Bronze parquet."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable, Dict, List, Optional, Tuple

import pandas as pd

from ..config import get_config
from ..logging_config import configure_logging
from ..utils.io import ensure_dir, write_parquet
from ..utils.paths import get_paths
from .youtube_client import YouTubeClient

log = logging.getLogger(__name__)

# Category IDs requested by the user
CATEGORY_MAP: Dict[int, str] = {
    2: "Autos & Vehicles",
    15: "Pets & Animals",
    17: "Sports",
    19: "Travel & Events",
    20: "Gaming",
    22: "People & Blogs",
    23: "Comedy",
    24: "Entertainment",
    26: "Howto & Style",
    27: "Education",
    28: "Science & Technology",

}

MIN_SUBSCRIBERS = 10_000
MIN_UPLOADS_LAST_YEAR = 10
TARGET_CHANNELS_PER_CATEGORY = 50
DEFAULT_OVERFETCH_FACTOR = 4  # gather more candidates to balance subs


def _bronze_dir(output_dir: Optional[str]) -> str:
    paths = get_paths()
    base = output_dir or os.path.join(paths.bronze, "youtube")
    ensure_dir(base)
    return base


def _parse_published_at(published_at: Optional[str]) -> Optional[datetime]:
    if not published_at:
        return None
    try:
        return datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    except ValueError:
        return None


def _count_recent_uploads(uploads: List[Dict[str, object]], *, lookback_days: int) -> Tuple[int, List[str]]:
    """Return (number of uploads in window, video ids) for playlist items."""
    threshold = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    count = 0
    video_ids: List[str] = []
    for video in uploads:
        published_at = (
            video.get("contentDetails", {}).get("videoPublishedAt")
            or video.get("snippet", {}).get("publishedAt")
        )
        dt = _parse_published_at(published_at)
        if dt and dt >= threshold:
            count += 1
            vid_id = video.get("contentDetails", {}).get("videoId")
            if vid_id:
                video_ids.append(vid_id)
    return count, video_ids


def _to_frame(items: List[Dict[str, object]]) -> pd.DataFrame:
    """Normalize list of dicts to a DataFrame that is safe for Parquet writes."""
    if not items:
        return pd.DataFrame({"_empty": []})
    return pd.json_normalize(items)


def _normalize_channels_df(channel_items: List[Dict[str, object]]) -> pd.DataFrame:
    df = pd.json_normalize(channel_items)
    if "statistics.subscriberCount" in df.columns:
        df["subscriberCount"] = pd.to_numeric(df["statistics.subscriberCount"], errors="coerce").fillna(0).astype(int)
    else:
        df["subscriberCount"] = 0
    if "statistics.videoCount" in df.columns:
        df["videoCount"] = pd.to_numeric(df["statistics.videoCount"], errors="coerce").fillna(0).astype(int)
    else:
        df["videoCount"] = 0
    if "snippet.country" in df.columns:
        df["country"] = df["snippet.country"]
    else:
        df["country"] = ""
    if "contentDetails.relatedPlaylists.uploads" in df.columns:
        df["uploads_playlist_id"] = df["contentDetails.relatedPlaylists.uploads"]
    else:
        df["uploads_playlist_id"] = None
    return df


def _pick_balanced_channels(df: pd.DataFrame, target: int) -> pd.DataFrame:
    """Select channels across subscriber quantiles to balance representation."""
    if df.empty:
        return df
    work = df.copy()
    # buckets by quantile; duplicates="drop" to handle few unique values
    bins = min(5, max(1, work["subscriberCount"].nunique()))
    work["sub_bucket"] = pd.qcut(
        work["subscriberCount"].rank(method="first"),
        q=bins,
        labels=False,
        duplicates="drop",
    )
    selections = []
    per_bucket = max(1, target // max(1, work["sub_bucket"].nunique()))
    for bucket in sorted(work["sub_bucket"].dropna().unique()):
        bucket_df = work[work["sub_bucket"] == bucket].sort_values("subscriberCount", ascending=False)
        selections.append(bucket_df.head(per_bucket))
    chosen = pd.concat(selections, ignore_index=True) if selections else pd.DataFrame(columns=work.columns)
    if len(chosen) < target:
        remaining = work.drop(chosen.index, errors="ignore").sort_values("subscriberCount", ascending=False)
        chosen = pd.concat([chosen, remaining.head(target - len(chosen))], ignore_index=True)
    return chosen.head(target).drop(columns=["sub_bucket"], errors="ignore")


def ingest_videos(video_ids: Iterable[str], output_dir: Optional[str] = None, client: Optional[YouTubeClient] = None) -> str:
    """Fetch metadata for given IDs and write to Bronze. Returns Parquet path."""
    cfg = get_config()
    client = client or YouTubeClient(api_key=cfg.youtube_api_key)
    bronze_dir = os.path.join(_bronze_dir(output_dir), "videos")
    ensure_dir(bronze_dir)

    meta = client.fetch_video_metadata(video_ids)
    if not meta:
        raise ValueError("No video metadata returned.")

    df = pd.json_normalize(meta)
    dest = os.path.join(bronze_dir, f"videos_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.parquet")
    write_parquet(df, dest)
    log.info("Wrote %d videos to %s", len(df), dest)
    return dest


def ingest_channel(channel_id: str, limit: int = 100, output_dir: Optional[str] = None, client: Optional[YouTubeClient] = None) -> Dict[str, str]:
    """Fetch recent uploads for a channel and write to Bronze (videos + playlist)."""
    cfg = get_config()
    client = client or YouTubeClient(api_key=cfg.youtube_api_key)
    bronze_dir = _bronze_dir(output_dir)
    uploads_dir = os.path.join(bronze_dir, "uploads")
    ensure_dir(uploads_dir)

    video_ids = client.search_channel_uploads(channel_id, limit=limit)
    uploads_playlist_id = client.get_uploads_playlist_id(channel_id)
    playlist_items = (
        client.fetch_playlist_items(
            uploads_playlist_id,
            page_size=min(limit, client.page_max),
            max_pages=max(1, (limit + client.page_max - 1) // client.page_max),
        )
        if uploads_playlist_id
        else []
    )

    uploads_path = os.path.join(uploads_dir, f"{channel_id}.parquet")
    uploads_df = _to_frame(playlist_items)
    write_parquet(uploads_df, uploads_path)

    videos_path = ingest_videos(video_ids, output_dir=output_dir, client=client)
    log.info("Ingested channel %s with %d videos", channel_id, len(video_ids))
    return {"videos": videos_path, "uploads": uploads_path}


def _discover_category(
    category_id: int,
    category_name: str,
    *,
    per_category: int,
    min_subscribers: int,
    min_uploads_last_year: int,
    lookback_days: int,
    search_pages: int,
    overfetch_factor: int,
    output_dir: Optional[str],
    client: YouTubeClient,
    published_after_iso: Optional[str],
) -> Optional[pd.DataFrame]:
    bronze_dir = _bronze_dir(output_dir)
    search_dir = os.path.join(bronze_dir, "search")
    channels_dir = os.path.join(bronze_dir, "channels")
    uploads_dir = os.path.join(bronze_dir, "uploads")
    selection_dir = os.path.join(bronze_dir, "channel_selection")
    for d in (search_dir, channels_dir, uploads_dir, selection_dir):
        ensure_dir(d)

    log.info("Discovering channels for category %s (%s)", category_name, category_id)
    search_items = client.search_videos_by_category(
        category_id,
        region_code="US",
        max_pages=search_pages,
        order="viewCount",
        published_after=published_after_iso,
    )
    search_df = _to_frame(search_items)
    write_parquet(search_df, os.path.join(search_dir, f"category_{category_id}.parquet"))

    channel_ids: List[str] = []
    seen: set[str] = set()
    for item in search_items:
        channel_id = item.get("snippet", {}).get("channelId")
        if channel_id and channel_id not in seen:
            seen.add(channel_id)
            channel_ids.append(channel_id)
    if not channel_ids:
        log.warning("No channels found for category %s", category_id)
        return None

    # overfetch to allow filtering and balancing
    overfetch_total = max(per_category * overfetch_factor, len(channel_ids))
    channel_ids = channel_ids[:overfetch_total]

    channel_meta = client.fetch_channel_metadata(channel_ids)
    channels_df = _normalize_channels_df(channel_meta)
    channels_df["category_id"] = category_id
    channels_df["category_name"] = category_name
    write_parquet(channels_df, os.path.join(channels_dir, f"category_{category_id}.parquet"))

    # filter for US, subscriber threshold, available uploads playlist
    candidates = channels_df[
        (channels_df["subscriberCount"] >= min_subscribers)
        & (channels_df["uploads_playlist_id"].notna())
        & (channels_df["country"].fillna("") == "US")
    ].copy()

    if candidates.empty:
        log.warning("No eligible channels after initial filters for category %s", category_id)
        return None

    # activity check via uploads playlist
    activity_rows: List[Dict[str, object]] = []
    for _, row in candidates.iterrows():
        playlist_id = row["uploads_playlist_id"]
        playlist_items = client.fetch_playlist_items(playlist_id, page_size=50, max_pages=5)
        uploads_count, recent_video_ids = _count_recent_uploads(playlist_items, lookback_days=lookback_days)
        uploads_df = _to_frame(playlist_items)
        write_parquet(uploads_df, os.path.join(uploads_dir, f"{row['id']}.parquet"))

        if uploads_count >= min_uploads_last_year:
            row_data = row.to_dict()
            row_data["uploads_last_year"] = uploads_count
            row_data["recent_video_ids"] = recent_video_ids
            activity_rows.append(row_data)

    if not activity_rows:
        log.warning("No active channels passed activity filter for category %s", category_id)
        return None

    activity_df = pd.DataFrame(activity_rows)
    selected = _pick_balanced_channels(activity_df, per_category)
    selected["category_id"] = category_id
    selected["category_name"] = category_name

    write_parquet(selected, os.path.join(selection_dir, f"category_{category_id}.parquet"))
    return selected


def discover_channels(
    *,
    per_category: int = TARGET_CHANNELS_PER_CATEGORY,
    min_subscribers: int = MIN_SUBSCRIBERS,
    min_uploads_last_year: int = MIN_UPLOADS_LAST_YEAR,
    lookback_days: int = 365,
    search_pages: int = 8,
    overfetch_factor: int = DEFAULT_OVERFETCH_FACTOR,
    output_dir: Optional[str] = None,
    client: Optional[YouTubeClient] = None,
    published_after_days: int = 120,
) -> pd.DataFrame:
    """
    Discover candidate channels per category and persist Bronze artifacts.

    Returns a dataframe of selected channels across the 10 categories.
    """
    cfg = get_config()
    client = client or YouTubeClient(api_key=cfg.youtube_api_key)

    published_after_iso = None
    if published_after_days:
        published_after_iso = (datetime.now(timezone.utc) - timedelta(days=published_after_days)).strftime("%Y-%m-%dT%H:%M:%SZ")

    selections: List[pd.DataFrame] = []

    for category_id, category_name in CATEGORY_MAP.items():
        selected = _discover_category(
            category_id,
            category_name,
            per_category=per_category,
            min_subscribers=min_subscribers,
            min_uploads_last_year=min_uploads_last_year,
            lookback_days=lookback_days,
            search_pages=search_pages,
            overfetch_factor=overfetch_factor,
            output_dir=output_dir,
            client=client,
            published_after_iso=published_after_iso,
        )
        if selected is not None:
            selections.append(selected)

    if not selections:
        raise RuntimeError("No channels selected across categories.")

    final_df = pd.concat(selections, ignore_index=True)
    selection_dir = os.path.join(_bronze_dir(output_dir), "channel_selection")
    ensure_dir(selection_dir)
    final_path = os.path.join(selection_dir, "all_categories.parquet")
    write_parquet(final_df, final_path)
    log.info("Channel discovery complete. Selected %d channels written to %s", len(final_df), final_path)
    return final_df
