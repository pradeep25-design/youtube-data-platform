# =============================================================
# config.py
# PROJECT  : Tamil Nadu YouTube Analytics Data Platform
# PURPOSE  : Single place for ALL project settings.
#            Every other file imports from here.
#            To change anything — change it ONLY here.
# =============================================================

import os
from dotenv import load_dotenv

load_dotenv()


# -------------------------------------------------------
# METHOD 1 : get_api_config()
# PURPOSE  : YouTube Data API v3 settings
# -------------------------------------------------------
def get_api_config():
    config = {
        "api_key"     : os.getenv("YOUTUBE_API_KEY", ""),
        "api_service" : "youtube",
        "api_version" : "v3",
        "region_code" : "IN",        # India region
        "language"    : "ta",        # Tamil language
        "max_results" : 50,          # max per API call
    }
    return config


# -------------------------------------------------------
# METHOD 2 : get_channels_config()
# PURPOSE  : Tamil Nadu YouTube channels to track.
#            Add any channel ID here — pipeline picks
#            it up automatically on next run.
# -------------------------------------------------------
def get_channels_config():
    channels = [
        # Tamil Channels — verified correct IDs
        {"id": "UCBnxEdpoZwstJqC1yZpOjRA", "name": "SunTV"},
        {"id": "UCvrhwpnp2DHYQ1CbXby9ypQ",  "name": "VijayTV"},
        {"id": "UC_wIGmvdyAQLtl-U2nHV9rg",  "name": "ZeeTamil"},
        {"id": "UCkWOEJUCEDWJCA6YAluDCNg",  "name": "PolimerTV"},
        {"id": "UCdMtj4CiqlZTgzFT5FdDd7A",  "name": "NakkheeranTV"},
    ]
    return channels


# -------------------------------------------------------
# METHOD 3 : get_keywords_config()
# PURPOSE  : Tamil Nadu trending keywords to search.
#            These drive keyword-based ingestion.
# -------------------------------------------------------
def get_keywords_config():
    keywords = [
        "tamilnadu trending today",
        "vijay thalapathy 2025",
        "ajith kumar 2025",
        "tamil movies 2025",
        "tamilnadu politics 2025",
        "kollywood latest news",
        "IPL 2025 tamil",
        "tamil comedy 2025",
        "sun tv serials 2025",
        "vijay tv shows 2025",
    ]
    return keywords


# -------------------------------------------------------
# METHOD 4 : get_categories_config()
# PURPOSE  : YouTube category IDs popular in Tamil Nadu.
#            Used to fetch trending videos per category.
# -------------------------------------------------------
def get_categories_config():
    categories = [
        {"id": "1",  "name": "Film & Animation"},
        {"id": "10", "name": "Music"},
        {"id": "17", "name": "Sports"},
        {"id": "22", "name": "People & Blogs"},
        {"id": "23", "name": "Comedy"},
        {"id": "24", "name": "Entertainment"},
        {"id": "25", "name": "News & Politics"},
        {"id": "28", "name": "Science & Technology"},
    ]
    return categories


# -------------------------------------------------------
# METHOD 5 : get_paths_config()
# PURPOSE  : ALL folder paths used across project.
#            Using local paths now.
#            For HDFS: replace "data/" with "hdfs://..."
# -------------------------------------------------------
def get_paths_config():
    paths = {
        # Raw JSON from API (landing zone)
        "raw_json"    : "data/raw_json",

        # Data lake layers
        "bronze"      : "data/bronze",
        "silver"      : "data/silver",
        "gold"        : "data/gold",

        # Bad records from quality checks
        "quarantine"  : "data/quarantine",

        # Hive warehouse
        "warehouse"   : "data/hive/warehouse",

        # Incremental ingestion tracking
        "watermark"   : "data/watermark",

        # Data lineage logs
        "lineage"     : "data/lineage",
    }
    return paths


# -------------------------------------------------------
# METHOD 6 : get_spark_config()
# PURPOSE  : PySpark session settings
# -------------------------------------------------------
def get_spark_config():
    config = {
        "app_name"          : "TamilNaduYouTubeAnalytics",
        "master"            : "local[*]",
        "log_level"         : "WARN",
        "shuffle_partitions": "4",
        "timezone"          : "UTC",
        "warehouse_dir"     : "data/hive/warehouse",
    }
    return config


# -------------------------------------------------------
# METHOD 7 : get_schema_config()
# PURPOSE  : Data quality rules — which columns must
#            never be null, which must be non-negative.
#            Used by silver pipeline validation.
# -------------------------------------------------------
def get_schema_config():
    schema = {
        # Columns that must NEVER be null
        "required_video_cols"    : [
            "video_id", "title", "channel_id"
        ],
        "required_channel_cols"  : [
            "channel_id", "subscriber_count"
        ],
        "required_category_cols" : [
            "category_id", "category_name"
        ],

        # Numeric columns that must be >= 0
        "non_negative_cols"      : [
            "views", "likes",
            "comment_count", "subscriber_count"
        ],
    }
    return schema


# -------------------------------------------------------
# METHOD 8 : get_warehouse_tables_config()
# PURPOSE  : Hive warehouse table names and their
#            HDFS/local paths. All external tables.
# -------------------------------------------------------
def get_warehouse_tables_config():
    warehouse = "data/hive/warehouse"
    tables = {
        # Dimension tables
        "dim_video"            : f"{warehouse}/dim_video",
        "dim_channel"          : f"{warehouse}/dim_channel",
        "dim_category"         : f"{warehouse}/dim_category",
        "dim_date"             : f"{warehouse}/dim_date",

        # Fact table
        "fact_video_performance": f"{warehouse}/fact_video_performance",
    }
    return tables


# -------------------------------------------------------
# TEST — run: python utils/config.py
# -------------------------------------------------------
if __name__ == "__main__":
    print("=" * 50)
    print("Tamil Nadu YouTube Analytics Platform")
    print("=" * 50)
    print(f"Region         : {get_api_config()['region_code']}")
    print(f"Language       : {get_api_config()['language']}")
    print(f"Channels       : {len(get_channels_config())}")
    print(f"Keywords       : {len(get_keywords_config())}")
    print(f"Categories     : {len(get_categories_config())}")
    print(f"Warehouse      : {get_paths_config()['warehouse']}")
    print("=" * 50)
    print("config.py loaded successfully!")