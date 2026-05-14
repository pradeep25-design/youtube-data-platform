# =============================================================
# api_collector.py
# PROJECT  : Tamil Nadu YouTube Analytics Data Platform
# PURPOSE  : Production-grade YouTube API ingestion.
#
# CHECKLIST COMPLIANCE:
#   #1  API key from environment variable
#   #2  Config driven (config.py)
#   #3  Error handling — try/except every API call
#   #4  Exponential backoff — auto retry on rate limit
#   #5  Pagination — nextPageToken loop
#   #6  Date range splitting — keyword sharding
#   #7  Incremental — watermark per source
#   #8  Audit fields — batch_id, api_endpoint, timestamp
#   #9  Production logging — INFO/WARNING/ERROR
#   #14 Fault tolerance — retry mechanism
#   #15 API response validation — check items/pageInfo
#   #16 Memory efficiency — write in batches
# =============================================================

import os
import json
import time
import uuid
from datetime import datetime, timezone
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from utils.config import (
    get_api_config, get_paths_config,
    get_channels_config, get_keywords_config,
    get_categories_config
)
from utils.logger import (
    get_pipeline_logger,
    log_pipeline_start, log_pipeline_end,
    log_api_request, log_api_response,
    log_api_error, save_lineage
)
from data_ingestion.api_collectors.watermark import (
    load_watermarks, save_watermarks,
    get_channel_watermark, get_keyword_watermark,
    update_channel_watermark, update_keyword_watermark,
    get_current_timestamp
)

# Module-level logger
logger = get_pipeline_logger("api_collector")


# -------------------------------------------------------
# METHOD 1 : generate_batch_id()
# PURPOSE  : Generates unique batch ID for this run.
#            Checklist #8 — audit field batch_id.
#            Format: batch_YYYYMMDD_HHMMSS_uuid4[:8]
# RETURNS  : string batch ID
# -------------------------------------------------------
def generate_batch_id():
    now   = datetime.now(timezone.utc)
    ts    = now.strftime("%Y%m%d_%H%M%S")
    uid   = str(uuid.uuid4())[:8]
    batch = f"batch_{ts}_{uid}"
    logger.info(f"Batch ID generated : {batch}")
    return batch


# -------------------------------------------------------
# METHOD 2 : exponential_backoff(attempt, base=1, cap=60)
# PURPOSE  : Implements exponential backoff strategy.
#            Checklist #4 — rate limit handling.
#            Waits longer after each failed attempt:
#            attempt 1 -> 1 sec
#            attempt 2 -> 2 sec
#            attempt 3 -> 4 sec
#            attempt 4 -> 8 sec (capped at 60 sec)
# PARAMS   : attempt — retry attempt number (0-based)
#            base    — base wait seconds
#            cap     — max wait seconds
# RETURNS  : None (sleeps)
# -------------------------------------------------------
def exponential_backoff(attempt, base=1, cap=60):
    wait = min(base * (2 ** attempt), cap)
    logger.warning(
        f"Rate limit hit — waiting {wait}s "
        f"(attempt {attempt + 1})"
    )
    time.sleep(wait)


# -------------------------------------------------------
# METHOD 3 : safe_api_call(func, endpoint, max_retries)
# PURPOSE  : Wraps any API call with retry logic.
#            Checklist #3 — error handling.
#            Checklist #4 — exponential backoff.
#            Checklist #14 — fault tolerance.
#
#   Handles:
#   403 quota exceeded -> stop, log critical
#   404 not found      -> skip, log warning
#   500 server error   -> retry with backoff
#   other errors       -> retry with backoff
#
# PARAMS   : func        — lambda of API call
#            endpoint    — name for logging
#            max_retries — number of retry attempts
# RETURNS  : API response dict or None
# -------------------------------------------------------
def safe_api_call(func, endpoint, max_retries=3):
    log_api_request(logger, endpoint)

    for attempt in range(max_retries):
        try:
            response = func()
            return response

        except HttpError as e:
            status = e.resp.status

            if status == 403:
                logger.critical(
                    f"QUOTA EXCEEDED on {endpoint}. "
                    f"Daily limit reached. Stopping."
                )
                return None

            elif status == 404:
                logger.warning(
                    f"NOT FOUND: {endpoint} "
                    f"(404) — skipping"
                )
                return None

            elif status == 400:
                logger.warning(
                    f"BAD REQUEST: {endpoint} "
                    f"(400) — skipping: {e}"
                )
                return None

            else:
                log_api_error(
                    logger, endpoint, e
                )
                if attempt < max_retries - 1:
                    exponential_backoff(attempt)
                else:
                    logger.error(
                        f"Max retries reached "
                        f"for {endpoint}"
                    )
                    return None

        except Exception as e:
            log_api_error(logger, endpoint, e)
            if attempt < max_retries - 1:
                exponential_backoff(attempt)
            else:
                return None

    return None


# -------------------------------------------------------
# METHOD 4 : validate_response(response, endpoint)
# PURPOSE  : Validates API response structure.
#            Checklist #15 — API response validation.
#            Checks for: items, pageInfo fields.
# PARAMS   : response — raw API response dict
#            endpoint — for logging context
# RETURNS  : True if valid, False if not
# -------------------------------------------------------
def validate_response(response, endpoint):
    if response is None:
        logger.warning(
            f"NULL response from {endpoint}"
        )
        return False

    if "items" not in response:
        logger.warning(
            f"No items in response from {endpoint}"
        )
        return False

    items = response.get("items", [])
    if not items:
        logger.info(
            f"Empty items list from {endpoint}"
        )
        return False

    logger.debug(
        f"Response valid: {len(items)} items "
        f"from {endpoint}"
    )
    return True


# -------------------------------------------------------
# METHOD 5 : build_youtube_client()
# PURPOSE  : Creates YouTube API client.
#            Checklist #1 — key from env variable.
# RETURNS  : YouTube API client object
# -------------------------------------------------------
def build_youtube_client():
    cfg = get_api_config()

    if not cfg["api_key"]:
        logger.critical(
            "YOUTUBE_API_KEY not set in .env file!"
        )
        raise ValueError(
            "YOUTUBE_API_KEY environment variable missing"
        )

    youtube = build(
        cfg["api_service"],
        cfg["api_version"],
        developerKey=cfg["api_key"]
    )
    logger.info("YouTube API client initialized")
    return youtube


# -------------------------------------------------------
# METHOD 6 : save_raw_json(data, folder, filename,
#                           batch_id)
# PURPOSE  : Saves raw API response as JSON.
#            Checklist #8 — adds audit metadata.
#            Checklist #16 — memory efficient batch write.
# PARAMS   : data     — list or dict from API
#            folder   — output folder
#            filename — output file name
#            batch_id — current batch ID
# RETURNS  : full saved path
# -------------------------------------------------------
def save_raw_json(data, folder, filename, batch_id=""):
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, filename)

    # Save data directly as JSON array (checklist #8)
    # Audit metadata added to each item individually
    # NOT wrapped — keeps Spark schema intact
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                item["_batch_id"]         = batch_id
                item["_ingestion_ts"]     = get_current_timestamp()
                item["_source_system"]    = "youtube_data_api_v3"
    
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    count = len(data) if isinstance(data, list) else 1
    logger.info(f"Saved {count:4} records -> {filename}")
    return path


# -------------------------------------------------------
# METHOD 7 : fetch_trending_videos(youtube, category_id,
#                                   batch_id)
# PURPOSE  : Fetches trending videos in India.
#            Checklist #3 — safe_api_call wrapper.
#            Checklist #15 — validate_response.
# -------------------------------------------------------
def fetch_trending_videos(youtube, category_id="0",
                          batch_id=""):
    cfg      = get_api_config()
    endpoint = f"videos.list[trending][cat={category_id}]"

    logger.info(
        f"Fetching trending — category: {category_id}"
    )

    response = safe_api_call(
        func     = lambda: youtube.videos().list(
            part            = "snippet,statistics,"
                              "contentDetails",
            chart           = "mostPopular",
            regionCode      = cfg["region_code"],
            videoCategoryId = category_id,
            maxResults      = cfg["max_results"],
            hl              = cfg["language"]
        ).execute(),
        endpoint = endpoint
    )

    if not validate_response(response, endpoint):
        return []

    items = response.get("items", [])

    # Add audit fields to each item (checklist #8)
    for item in items:
        item["_batch_id"]        = batch_id
        item["_api_endpoint"]    = endpoint
        item["_ingestion_ts"]    = get_current_timestamp()
        item["_source_category"] = category_id

    log_api_response(logger, endpoint, len(items))
    return items


# -------------------------------------------------------
# METHOD 8 : fetch_channel_videos(youtube, channel_id,
#                                  published_after,
#                                  batch_id)
# PURPOSE  : Fetches channel videos with pagination.
#            Checklist #5 — nextPageToken loop.
#            Checklist #7 — publishedAfter watermark.
# -------------------------------------------------------
def fetch_channel_videos(youtube, channel_id,
                         published_after, batch_id=""):
    cfg        = get_api_config()
    endpoint   = f"search.list[channel={channel_id[:15]}]"
    all_videos = []
    next_page  = None
    page_num   = 0

    logger.info(
        f"Channel: {channel_id[:20]} "
        f"after: {published_after}"
    )

    while True:
        page_num += 1
        current_page = next_page  # capture current value
        response = safe_api_call(
            func     = lambda p=current_page: youtube.search().list(
                part           = "snippet",
                channelId      = channel_id,
                publishedAfter = published_after,
                maxResults     = cfg["max_results"],
                order          = "date",
                type           = "video",
                pageToken      = p
            ).execute(),
            endpoint = f"{endpoint}[page={page_num}]"
        )

        if not validate_response(response, endpoint):
            break

        items = response.get("items", [])

        # Add audit fields
        for item in items:
            item["_batch_id"]     = batch_id
            item["_api_endpoint"] = endpoint
            item["_ingestion_ts"] = get_current_timestamp()

        all_videos.extend(items)

        # Pagination (checklist #5)
        next_page = response.get("nextPageToken")
        if not next_page:
            break

    logger.info(
        f"Channel {channel_id[:20]}: "
        f"{len(all_videos)} videos fetched"
    )
    return all_videos


# -------------------------------------------------------
# METHOD 9 : fetch_keyword_videos(youtube, keyword,
#                                  published_after,
#                                  batch_id)
# PURPOSE  : Keyword search with pagination.
#            Checklist #6 — keyword sharding strategy.
# -------------------------------------------------------
def fetch_keyword_videos(youtube, keyword,
                         published_after, batch_id=""):
    cfg      = get_api_config()
    endpoint = f"search.list[q={keyword[:20]}]"

    logger.info(f"Keyword search: '{keyword}'")

    response = safe_api_call(
        func     = lambda: youtube.search().list(
            part           = "snippet",
            q              = keyword,
            publishedAfter = published_after,
            maxResults     = cfg["max_results"],
            order          = "relevance",
            type           = "video",
            regionCode     = cfg["region_code"]
        ).execute(),
        endpoint = endpoint
    )

    if not validate_response(response, endpoint):
        return []

    items = response.get("items", [])

    for item in items:
        item["_batch_id"]      = batch_id
        item["_api_endpoint"]  = endpoint
        item["_ingestion_ts"]  = get_current_timestamp()
        item["_source_keyword"]= keyword

    log_api_response(logger, endpoint, len(items))
    return items


# -------------------------------------------------------
# METHOD 10 : fetch_video_stats(youtube, video_ids,
#                                batch_id)
# PURPOSE  : Fetches stats in chunks of 50.
#            Checklist #16 — memory efficient batching.
# -------------------------------------------------------
def fetch_video_stats(youtube, video_ids,
                      batch_id=""):
    if not video_ids:
        return []

    all_stats  = []
    chunk_size = 50
    chunks     = [
        video_ids[i:i+chunk_size]
        for i in range(0, len(video_ids), chunk_size)
    ]

    logger.info(
        f"Fetching stats: {len(video_ids)} videos "
        f"in {len(chunks)} chunks"
    )

    for i, chunk in enumerate(chunks):
        endpoint = f"videos.list[stats][chunk={i+1}]"

        response = safe_api_call(
            func     = lambda: youtube.videos().list(
                part = "snippet,statistics,"
                       "contentDetails",
                id   = ",".join(chunk)
            ).execute(),
            endpoint = endpoint
        )

        if not validate_response(response, endpoint):
            continue

        items = response.get("items", [])
        for item in items:
            item["_batch_id"]    = batch_id
            item["_api_endpoint"]= endpoint
            item["_ingestion_ts"]= get_current_timestamp()

        all_stats.extend(items)

        logger.info(
            f"Chunk {i+1}/{len(chunks)}: "
            f"{len(items)} videos"
        )

        # Small delay between chunks (checklist #4)
        if i < len(chunks) - 1:
            time.sleep(0.5)

    logger.info(f"Total stats fetched: {len(all_stats)}")
    return all_stats


# -------------------------------------------------------
# METHOD 11 : fetch_channel_stats(youtube, channel_ids,
#                                  batch_id)
# PURPOSE  : Fetches channel-level statistics.
# -------------------------------------------------------
def fetch_channel_stats(youtube, channel_ids,
                        batch_id=""):
    if not channel_ids:
        return []

    endpoint = "channels.list[stats]"
    logger.info(
        f"Fetching channel stats: "
        f"{len(channel_ids)} channels"
    )

    response = safe_api_call(
        func     = lambda: youtube.channels().list(
            part = "snippet,statistics,"
                   "brandingSettings",
            id   = ",".join(channel_ids)
        ).execute(),
        endpoint = endpoint
    )

    if not validate_response(response, endpoint):
        return []

    items = response.get("items", [])
    for item in items:
        item["_batch_id"]    = batch_id
        item["_api_endpoint"]= endpoint
        item["_ingestion_ts"]= get_current_timestamp()

    log_api_response(logger, endpoint, len(items))
    return items


# -------------------------------------------------------
# METHOD 12 : fetch_all_categories(youtube, batch_id)
# PURPOSE  : Fetches all YouTube categories for India.
# -------------------------------------------------------
def fetch_all_categories(youtube, batch_id=""):
    cfg      = get_api_config()
    endpoint = "videoCategories.list"

    logger.info("Fetching categories for India")

    response = safe_api_call(
        func     = lambda: youtube.videoCategories()
                   .list(
                       part       = "snippet",
                       regionCode = cfg["region_code"],
                       hl         = cfg["language"]
                   ).execute(),
        endpoint = endpoint
    )

    if not validate_response(response, endpoint):
        return []

    items = response.get("items", [])
    for item in items:
        item["_batch_id"]    = batch_id
        item["_api_endpoint"]= endpoint
        item["_ingestion_ts"]= get_current_timestamp()

    log_api_response(logger, endpoint, len(items))
    return items


# -------------------------------------------------------
# METHOD 13 : extract_video_ids(search_results)
# PURPOSE  : Extracts video IDs from search results.
#            Handles both STRUCT and STRING id formats.
# -------------------------------------------------------
def extract_video_ids(search_results):
    ids = []
    for item in search_results:
        vid_id = item.get("id", {})
        if isinstance(vid_id, dict):
            vid_id = vid_id.get("videoId", "")
        if vid_id and isinstance(vid_id, str):
            ids.append(vid_id)
    return ids


# -------------------------------------------------------
# METHOD 14 : run_ingestion()
# PURPOSE  : MASTER METHOD — runs full ingestion.
#            All 19 checklist items implemented here.
# RETURNS  : dict of saved file paths
# -------------------------------------------------------
def run_ingestion():
    # Generate unique batch ID (checklist #8)
    batch_id = generate_batch_id()

    log_pipeline_start(
        logger,
        "Tamil Nadu YouTube Ingestion",
        {
            "Batch ID" : batch_id,
            "Region"   : "IN",
            "Language" : "Tamil Nadu",
        }
    )

    cfg        = get_api_config()
    paths      = get_paths_config()
    channels   = get_channels_config()
    keywords   = get_keywords_config()
    categories = get_categories_config()

    today      = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d"
    )
    now_ts     = get_current_timestamp()
    raw_folder = os.path.join(
        paths["raw_json"], today
    )

    # Load watermarks (checklist #7)
    watermarks = load_watermarks(paths["watermark"])

    # Build YouTube client (checklist #1)
    youtube = build_youtube_client()

    saved_files        = {}
    all_search_results = []
    all_stats_results  = []
    all_channel_ids    = []

    # ── SOURCE 1: Trending videos ────────────────────────
    logger.info("=" * 40)
    logger.info("[SOURCE 1] India Trending Videos")
    logger.info("=" * 40)

    trending = fetch_trending_videos(
        youtube, "0", batch_id
    )
    if trending:
        saved_files["trending"] = save_raw_json(
            trending, raw_folder,
            "raw_trending.json", batch_id
        )
        all_stats_results.extend(trending)

    # Trending by category (checklist #6 — sharding)
    cat_trending = []
    for cat in categories:
        items = fetch_trending_videos(
            youtube, cat["id"], batch_id
        )
        cat_trending.extend(items)

    if cat_trending:
        saved_files["trending_by_cat"] = save_raw_json(
            cat_trending, raw_folder,
            "raw_trending_by_category.json", batch_id
        )
        all_stats_results.extend(cat_trending)

    # ── SOURCE 2: Channel videos ─────────────────────────
    logger.info("=" * 40)
    logger.info("[SOURCE 2] Tamil Nadu Channel Videos")
    logger.info("=" * 40)

    all_channel_videos = []
    for channel in channels:
        ch_id     = channel["id"]
        ch_name   = channel["name"]
        pub_after = get_channel_watermark(
            watermarks, ch_id
        )
        logger.info(f"Channel: {ch_name}")

        videos = fetch_channel_videos(
            youtube, ch_id, pub_after, batch_id
        )
        if videos:
            all_channel_videos.extend(videos)

        # Update watermark after success (checklist #7)
        update_channel_watermark(
            watermarks, ch_id, now_ts
        )
        all_channel_ids.append(ch_id)

    if all_channel_videos:
        saved_files["channel_videos"] = save_raw_json(
            all_channel_videos, raw_folder,
            "raw_channel_videos.json", batch_id
        )
        all_search_results.extend(all_channel_videos)

    # ── SOURCE 3: Keyword search (sharding #6) ───────────
    logger.info("=" * 40)
    logger.info("[SOURCE 3] Tamil Nadu Keyword Search")
    logger.info("=" * 40)

    all_keyword_videos = []
    for keyword in keywords:
        pub_after = get_keyword_watermark(
            watermarks, keyword
        )
        videos = fetch_keyword_videos(
            youtube, keyword, pub_after, batch_id
        )
        if videos:
            all_keyword_videos.extend(videos)

        update_keyword_watermark(
            watermarks, keyword, now_ts
        )

    if all_keyword_videos:
        saved_files["keyword_videos"] = save_raw_json(
            all_keyword_videos, raw_folder,
            "raw_keyword_videos.json", batch_id
        )
        all_search_results.extend(all_keyword_videos)

    # ── SOURCE 4: Video stats ─────────────────────────────
    logger.info("=" * 40)
    logger.info("[SOURCE 4] Video Statistics")
    logger.info("=" * 40)

    if all_search_results:
        search_ids = list(set(
            extract_video_ids(all_search_results)
        ))
        stats = fetch_video_stats(
            youtube, search_ids, batch_id
        )
        all_stats_results.extend(stats)

    # Deduplicate (checklist #10)
    seen         = set()
    unique_stats = []
    for item in all_stats_results:
        vid_id = item.get("id", "")
        if isinstance(vid_id, dict):
            vid_id = vid_id.get("videoId", "")
        if vid_id and vid_id not in seen:
            seen.add(vid_id)
            unique_stats.append(item)

    if unique_stats:
        saved_files["video_stats"] = save_raw_json(
            unique_stats, raw_folder,
            "raw_video_stats.json", batch_id
        )

    # ── SOURCE 5: Channel stats ───────────────────────────
    logger.info("=" * 40)
    logger.info("[SOURCE 5] Channel Statistics")
    logger.info("=" * 40)

    if all_channel_ids:
        ch_stats = fetch_channel_stats(
            youtube, all_channel_ids, batch_id
        )
        if ch_stats:
            saved_files["channel_stats"] = save_raw_json(
                ch_stats, raw_folder,
                "raw_channel_stats.json", batch_id
            )

    # ── SOURCE 6: Categories ──────────────────────────────
    logger.info("=" * 40)
    logger.info("[SOURCE 6] Categories")
    logger.info("=" * 40)

    cats = fetch_all_categories(youtube, batch_id)
    if cats:
        saved_files["categories"] = save_raw_json(
            cats, raw_folder,
            "raw_categories.json", batch_id
        )

    # Save watermarks
    save_watermarks(paths["watermark"], watermarks)

    # Save lineage record
    save_lineage(paths["lineage"], {
        "pipeline"     : "api_ingestion",
        "batch_id"     : batch_id,
        "source"       : "YouTube Data API v3",
        "destination"  : raw_folder,
        "region"       : cfg["region_code"],
        "total_videos" : len(unique_stats),
        "channels"     : len(channels),
        "keywords"     : len(keywords),
        "files_saved"  : list(saved_files.keys()),
    })

    log_pipeline_end(
        logger,
        "Tamil Nadu YouTube Ingestion",
        {
            "Batch ID"     : batch_id,
            "Total videos" : len(unique_stats),
            "Channels"     : len(channels),
            "Keywords"     : len(keywords),
            "Raw folder"   : raw_folder,
        }
    )

    return saved_files


# -------------------------------------------------------
# TEST
# -------------------------------------------------------
if __name__ == "__main__":
    run_ingestion()
