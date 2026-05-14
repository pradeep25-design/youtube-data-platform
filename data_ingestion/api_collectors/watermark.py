# =============================================================
# watermark.py
# PURPOSE  : Tracks last ingestion time per source.
#            Ensures we fetch only NEW data each run.
#            Each channel/keyword has its own timestamp.
#
# WATERMARK FILE (data/watermark/watermark.json):
# {
#   "channels": {
#     "UCF4pnJWNyEGp-REQXS1BFPA": "2025-01-01T00:00:00Z"
#   },
#   "keywords": {
#     "tamilnadu trending": "2025-01-01T00:00:00Z"
#   }
# }
# =============================================================

import json
import os
from datetime import datetime, timezone

# Default start date — fetch from this date on first run
DEFAULT_DATE = "2025-01-01T00:00:00Z"


# -------------------------------------------------------
# METHOD 1 : load_watermarks(watermark_dir)
# PURPOSE  : Reads watermark file into a dict.
#            Returns empty structure if first run.
# PARAMS   : watermark_dir — folder containing watermark.json
# RETURNS  : dict with all watermarks
# -------------------------------------------------------
def load_watermarks(watermark_dir):
    os.makedirs(watermark_dir, exist_ok=True)
    wm_file = os.path.join(watermark_dir, "watermark.json")

    if os.path.exists(wm_file):
        with open(wm_file, "r") as f:
            data = json.load(f)
        print(f"  Watermarks loaded from : {wm_file}")
        return data

    print("  No watermark found — first run, using default date")
    return {
        "channels" : {},
        "keywords" : {},
        "trending" : DEFAULT_DATE
    }


# -------------------------------------------------------
# METHOD 2 : save_watermarks(watermark_dir, watermarks)
# PURPOSE  : Saves updated watermarks back to file.
#            Called AFTER successful ingestion.
# PARAMS   : watermark_dir — folder to save in
#            watermarks    — updated dict to save
# RETURNS  : None
# -------------------------------------------------------
def save_watermarks(watermark_dir, watermarks):
    os.makedirs(watermark_dir, exist_ok=True)
    wm_file = os.path.join(watermark_dir, "watermark.json")

    watermarks["last_saved_at"] = get_current_timestamp()

    with open(wm_file, "w") as f:
        json.dump(watermarks, f, indent=2)

    print(f"  Watermarks saved to    : {wm_file}")


# -------------------------------------------------------
# METHOD 3 : get_channel_watermark(watermarks, channel_id)
# PURPOSE  : Returns last fetch time for a channel.
#            Returns DEFAULT_DATE if channel never fetched.
# PARAMS   : watermarks — loaded watermarks dict
#            channel_id — YouTube channel ID string
# RETURNS  : timestamp string
# -------------------------------------------------------
def get_channel_watermark(watermarks, channel_id):
    return watermarks.get(
        "channels", {}
    ).get(channel_id, DEFAULT_DATE)


# -------------------------------------------------------
# METHOD 4 : get_keyword_watermark(watermarks, keyword)
# PURPOSE  : Returns last fetch time for a keyword search.
# PARAMS   : watermarks — loaded watermarks dict
#            keyword    — search keyword string
# RETURNS  : timestamp string
# -------------------------------------------------------
def get_keyword_watermark(watermarks, keyword):
    return watermarks.get(
        "keywords", {}
    ).get(keyword, DEFAULT_DATE)


# -------------------------------------------------------
# METHOD 5 : update_channel_watermark(watermarks,
#                                      channel_id, ts)
# PURPOSE  : Updates watermark for one channel to now.
#            Called after successful channel ingestion.
# PARAMS   : watermarks — dict to update
#            channel_id — channel that was just fetched
#            ts         — new timestamp to set
# RETURNS  : None (updates dict in place)
# -------------------------------------------------------
def update_channel_watermark(watermarks, channel_id, ts):
    if "channels" not in watermarks:
        watermarks["channels"] = {}
    watermarks["channels"][channel_id] = ts


# -------------------------------------------------------
# METHOD 6 : update_keyword_watermark(watermarks,
#                                      keyword, ts)
# PURPOSE  : Updates watermark for one keyword to now.
# PARAMS   : watermarks — dict to update
#            keyword    — keyword that was just searched
#            ts         — new timestamp to set
# RETURNS  : None (updates dict in place)
# -------------------------------------------------------
def update_keyword_watermark(watermarks, keyword, ts):
    if "keywords" not in watermarks:
        watermarks["keywords"] = {}
    watermarks["keywords"][keyword] = ts


# -------------------------------------------------------
# METHOD 7 : get_current_timestamp()
# PURPOSE  : Returns current UTC time formatted for
#            YouTube API (ISO 8601 format with Z suffix)
# RETURNS  : string like "2025-05-09T10:30:00Z"
# -------------------------------------------------------
def get_current_timestamp():
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")


# -------------------------------------------------------
# TEST — run: python data_ingestion/api_collectors/watermark.py
# -------------------------------------------------------
if __name__ == "__main__":
    from utils.config import get_paths_config
    paths = get_paths_config()

    print("=== Testing Watermark ===")

    # Test 1: Load (first run — no file)
    wms = load_watermarks(paths["watermark"])
    print(f"  Loaded: {wms}")

    # Test 2: Update channel watermark
    now = get_current_timestamp()
    update_channel_watermark(
        wms, "UCF4pnJWNyEGp-REQXS1BFPA", now
    )
    update_keyword_watermark(
        wms, "tamilnadu trending today", now
    )

    # Test 3: Save
    save_watermarks(paths["watermark"], wms)

    # Test 4: Load again — should show saved values
    wms2 = load_watermarks(paths["watermark"])
    print(f"  Channel WM : {get_channel_watermark(wms2, 'UCF4pnJWNyEGp-REQXS1BFPA')}")
    print(f"  Keyword WM : {get_keyword_watermark(wms2, 'tamilnadu trending today')}")

    print("\nwatermark.py working correctly!")
