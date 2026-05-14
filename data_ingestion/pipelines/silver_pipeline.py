# =============================================================
# silver_pipeline.py
# PROJECT  : Tamil Nadu YouTube Analytics Data Platform
# PURPOSE  : Reads Bronze layer, validates data quality,
#            flattens nested JSON structs, standardizes
#            types, writes clean data to Silver layer.
#
# SILVER LAYER RULES (from PDF):
#   - Schema enforced (correct column types)
#   - Null handling (required cols must not be null)
#   - Deduplication (no duplicate video_id)
#   - Type standardization (string counts -> long)
#   - Bad records saved to quarantine separately
#   - Partition by ingestion_date
#
# FLOW:
#   Bronze Parquet -> flatten -> validate -> Silver Parquet
#                                         -> Quarantine
# =============================================================

import os
from datetime import datetime, timezone
from pyspark.sql import functions as F

from utils.spark_session import (
    create_spark_session, stop_spark_session
)
from utils.config import get_paths_config, get_schema_config
from utils.data_quality import run_all_checks
from utils.logger import (
    log_pipeline_start, log_pipeline_end,
    log_step, log_error, save_lineage
)


# -------------------------------------------------------
# METHOD 1 : read_bronze(spark, bronze_path, table_name)
# PURPOSE  : Reads a Bronze table into Spark DataFrame.
#            Reads all partitions automatically.
# PARAMS   : spark       — active SparkSession
#            bronze_path — root bronze folder
#            table_name  — bronze table to read
# RETURNS  : DataFrame or None
# -------------------------------------------------------
def read_bronze(spark, bronze_path, table_name):
    path = os.path.join(bronze_path, table_name)

    if not os.path.exists(path):
        log_step("READ", f"Not found: {table_name}")
        return None

    log_step("READ", f"Reading: {table_name}")
    df    = spark.read.parquet(path)
    count = df.count()
    log_step("READ", f"Rows: {count}")
    return df


# -------------------------------------------------------
# METHOD 2 : flatten_video_stats(df)
# PURPOSE  : Flattens nested YouTube API response for
#            bronze_video_stats table.
#            YouTube API returns nested structs like:
#            statistics.viewCount, snippet.title etc.
#            We extract and cast them to flat columns.
#
#   IMPORTANT: YouTube returns ALL counts as STRINGS!
#   viewCount = "1234567" not 1234567
#   We cast them to LONG (big integer) here.
# PARAMS   : df — raw bronze_video_stats DataFrame
# RETURNS  : flattened DataFrame with correct types
# -------------------------------------------------------
def flatten_video_stats(df):
    log_step("FLATTEN", "Flattening video stats...")

    df = (
        df
        # video_id — plain string in videos API
        .withColumn("video_id",
            F.col("id"))
        # Basic info from snippet
        .withColumn("title",
            F.col("snippet.title"))
        .withColumn("channel_id",
            F.col("snippet.channelId"))
        .withColumn("channel_title",
            F.col("snippet.channelTitle"))
        .withColumn("category_id",
            F.col("snippet.categoryId"))
        .withColumn("published_at",
            F.to_timestamp("snippet.publishedAt"))
        .withColumn("description",
            F.col("snippet.description"))
        .withColumn("duration",
            F.col("contentDetails.duration"))
        # Tags array -> count of tags
        .withColumn("tag_count",
            F.when(
                F.col("snippet.tags").isNotNull(),
                F.size(F.col("snippet.tags"))
            ).otherwise(F.lit(0))
        )
        # Statistics — cast STRING -> LONG
        .withColumn("views",
            F.col("statistics.viewCount")
             .cast("long"))
        .withColumn("likes",
            F.col("statistics.likeCount")
             .cast("long"))
        .withColumn("comment_count",
            F.col("statistics.commentCount")
             .cast("long"))
        .withColumn("favorite_count",
            F.col("statistics.favoriteCount")
             .cast("long"))
        # Thumbnail URL
        .withColumn("thumbnail_url",
            F.col("snippet.thumbnails.high.url"))
        # Select only needed columns
        .select(
            "video_id", "title", "channel_id",
            "channel_title", "category_id",
            "published_at", "description",
            "duration", "tag_count",
            "views", "likes", "comment_count",
            "favorite_count", "thumbnail_url",
            "ingestion_date", "ingestion_timestamp",
            "source"
        )
    )

    log_step("FLATTEN",
             f"Done — {df.count()} rows flattened")
    return df


# -------------------------------------------------------
# METHOD 3 : flatten_channel_stats(df)
# PURPOSE  : Flattens nested channel stats from
#            bronze_channel_stats table.
# PARAMS   : df — raw bronze_channel_stats DataFrame
# RETURNS  : flattened DataFrame
# -------------------------------------------------------
def flatten_channel_stats(df):
    log_step("FLATTEN", "Flattening channel stats...")

    df = (
        df
        .withColumn("channel_id",
            F.col("id"))
        .withColumn("channel_name",
            F.col("snippet.title"))
        .withColumn("description",
            F.col("snippet.description"))
        .withColumn("country",
            F.col("snippet.country"))
        .withColumn("published_at",
            F.to_timestamp("snippet.publishedAt"))
        .withColumn("subscriber_count",
            F.col("statistics.subscriberCount")
             .cast("long"))
        .withColumn("total_views",
            F.col("statistics.viewCount")
             .cast("long"))
        .withColumn("video_count",
            F.col("statistics.videoCount")
             .cast("long"))
        .select(
            "channel_id", "channel_name",
            "description", "country", "published_at",
            "subscriber_count", "total_views",
            "video_count",
            "ingestion_date", "ingestion_timestamp",
            "source"
        )
    )

    log_step("FLATTEN",
             f"Done — {df.count()} rows flattened")
    return df


# -------------------------------------------------------
# METHOD 4 : flatten_categories(df)
# PURPOSE  : Flattens category data from
#            bronze_categories table.
# PARAMS   : df — raw bronze_categories DataFrame
# RETURNS  : flattened DataFrame
# -------------------------------------------------------
def flatten_categories(df):
    log_step("FLATTEN", "Flattening categories...")

    df = (
        df
        .withColumn("category_id",
            F.col("id"))
        .withColumn("category_name",
            F.col("snippet.title"))
        .withColumn("assignable",
            F.col("snippet.assignable"))
        .select(
            "category_id", "category_name",
            "assignable",
            "ingestion_date", "ingestion_timestamp",
            "source"
        )
    )

    log_step("FLATTEN",
             f"Done — {df.count()} rows flattened")
    return df


# -------------------------------------------------------
# METHOD 5 : flatten_keyword_videos(df)
# PURPOSE  : Flattens keyword/channel search results.
#            Search API returns id as STRUCT not string.
# PARAMS   : df — raw bronze_keyword_videos DataFrame
# RETURNS  : flattened DataFrame
# -------------------------------------------------------
def flatten_keyword_videos(df):
    log_step("FLATTEN", "Flattening keyword videos...")

    df = (
        df
        # id is a STRUCT in search results
        .withColumn("video_id",
            F.col("id.videoId"))
        .withColumn("title",
            F.col("snippet.title"))
        .withColumn("channel_id",
            F.col("snippet.channelId"))
        .withColumn("channel_title",
            F.col("snippet.channelTitle"))
        .withColumn("published_at",
            F.to_timestamp("snippet.publishedAt"))
        .withColumn("description",
            F.col("snippet.description"))
        .withColumn("thumbnail_url",
            F.col("snippet.thumbnails.high.url"))
        .withColumn("source_keyword",
            F.when(
                F.col("_source_keyword").isNotNull(),
                F.col("_source_keyword")
            ).otherwise(F.lit("unknown"))
        )
        .select(
            "video_id", "title", "channel_id",
            "channel_title", "published_at",
            "description", "thumbnail_url",
            "source_keyword",
            "ingestion_date", "ingestion_timestamp",
            "source"
        )
    )

    log_step("FLATTEN",
             f"Done — {df.count()} rows flattened")
    return df


# -------------------------------------------------------
# METHOD 6 : write_to_silver(df, silver_path, table_name)
# PURPOSE  : Writes clean DataFrame to Silver layer.
#            Partitioned by ingestion_date.
# PARAMS   : df          — clean validated DataFrame
#            silver_path — root silver folder
#            table_name  — e.g. "silver_video_stats"
# RETURNS  : output path
# -------------------------------------------------------
def write_to_silver(df, silver_path, table_name):
    path = os.path.join(silver_path, table_name)
    log_step("WRITE", f"Writing: {table_name}")

    (
        df.write
        .mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(path)
    )

    log_step("WRITE", f"Done -> {path}")
    return path


# -------------------------------------------------------
# METHOD 7 : write_quarantine(df, qpath, table_name)
# PURPOSE  : Saves bad rows to Quarantine.
#            Never delete bad data — always save it.
#            Required by PDF section 5.
# PARAMS   : df         — bad rows DataFrame
#            qpath      — quarantine root folder
#            table_name — quarantine table name
# RETURNS  : path or None
# -------------------------------------------------------
def write_quarantine(df, qpath, table_name):
    try:
        count = df.count()
        if count == 0:
            log_step("QUARANTINE", "No bad rows")
            return None

        path = os.path.join(qpath, table_name)
        log_step("QUARANTINE",
                 f"{count} bad rows -> {table_name}")
        df.write.mode("append").parquet(path)
        return path
    except Exception as e:
        log_step("QUARANTINE", f"Skipped: {e}")
        return None


# -------------------------------------------------------
# METHOD 8 : run_silver_pipeline()
# PURPOSE  : MASTER METHOD — processes all bronze tables
#            into clean silver tables.
#            Each table processed independently.
#            This is what Airflow calls daily.
# RETURNS  : dict of written silver paths
# -------------------------------------------------------
def run_silver_pipeline():
    log_pipeline_start(
        "Silver Pipeline",
        {"Layer": "Silver", "Format": "Parquet"}
    )

    paths  = get_paths_config()
    schema = get_schema_config()
    spark  = create_spark_session("SilverPipeline")

    written = {}

    # ── TABLE 1: silver_video_stats ─────────────────────
    try:
        log_step("PIPELINE",
                 "--- silver_video_stats ---")
        df = read_bronze(
            spark, paths["bronze"], "bronze_video_stats"
        )
        if df:
            df = flatten_video_stats(df)
            clean, bad = run_all_checks(
                df,
                required_cols     = schema["required_video_cols"],
                non_negative_cols = schema["non_negative_cols"],
                key_col           = "video_id"
            )
            written["silver_video_stats"] =                 write_to_silver(
                    clean, paths["silver"],
                    "silver_video_stats"
                )
            write_quarantine(
                bad, paths["quarantine"],
                "quarantine_video_stats"
            )
    except Exception as e:
        log_error("silver_video_stats", e)

    # ── TABLE 2: silver_categories ───────────────────────
    try:
        log_step("PIPELINE",
                 "--- silver_categories ---")
        df = read_bronze(
            spark, paths["bronze"], "bronze_categories"
        )
        if df:
            df = flatten_categories(df)
            clean, bad = run_all_checks(
                df,
                required_cols     = schema["required_category_cols"],
                non_negative_cols = [],
                key_col           = "category_id"
            )
            written["silver_categories"] =                 write_to_silver(
                    clean, paths["silver"],
                    "silver_categories"
                )
            write_quarantine(
                bad, paths["quarantine"],
                "quarantine_categories"
            )
    except Exception as e:
        log_error("silver_categories", e)

    # ── TABLE 3: silver_keyword_videos ──────────────────
    try:
        log_step("PIPELINE",
                 "--- silver_keyword_videos ---")
        df = read_bronze(
            spark, paths["bronze"],
            "bronze_keyword_videos"
        )
        if df:
            df = flatten_keyword_videos(df)
            clean, bad = run_all_checks(
                df,
                required_cols     = schema["required_video_cols"],
                non_negative_cols = [],
                key_col           = "video_id"
            )
            written["silver_keyword_videos"] =                 write_to_silver(
                    clean, paths["silver"],
                    "silver_keyword_videos"
                )
            write_quarantine(
                bad, paths["quarantine"],
                "quarantine_keyword_videos"
            )
    except Exception as e:
        log_error("silver_keyword_videos", e)

    # ── TABLE 4: silver_trending ────────────────────────
    try:
        log_step("PIPELINE", "--- silver_trending ---")
        df = read_bronze(
            spark, paths["bronze"], "bronze_trending"
        )
        if df:
            # Trending uses same structure as video_stats
            df = flatten_video_stats(df)
            clean, bad = run_all_checks(
                df,
                required_cols     = schema["required_video_cols"],
                non_negative_cols = schema["non_negative_cols"],
                key_col           = "video_id"
            )
            written["silver_trending"] =                 write_to_silver(
                    clean, paths["silver"],
                    "silver_trending"
                )
            write_quarantine(
                bad, paths["quarantine"],
                "quarantine_trending"
            )
    except Exception as e:
        log_error("silver_trending", e)

    # Save lineage
    save_lineage(paths["lineage"], {
        "pipeline"    : "silver_pipeline",
        "source"      : paths["bronze"],
        "destination" : paths["silver"],
        "tables"      : list(written.keys()),
    })

    stop_spark_session(spark)

    log_pipeline_end(
        "Silver Pipeline",
        {"Tables written": len(written)}
    )

    return written


# -------------------------------------------------------
# TEST
# -------------------------------------------------------
if __name__ == "__main__":
    run_silver_pipeline()
