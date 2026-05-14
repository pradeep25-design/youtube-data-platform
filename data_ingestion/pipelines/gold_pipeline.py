# =============================================================
# gold_pipeline.py
# PROJECT  : Tamil Nadu YouTube Analytics Data Platform
# PURPOSE  : Reads Silver layer, joins tables, computes
#            business metrics, writes Gold layer datasets.
#
# GOLD LAYER RULES (from PDF):
#   - Business-ready datasets
#   - Joined across silver tables
#   - Computed metrics (engagement_rate, like_ratio)
#   - Aggregated per channel
#   - Partitioned by year and month
#
# TABLES PRODUCED:
#   gold_video_engagement    : one row per video + metrics
#   gold_channel_performance : one row per channel
#   gold_category_performance: one row per category
#
# FLOW:
#   silver_video_stats + silver_categories
#       -> join -> compute -> Gold Parquet
# =============================================================

import os
from datetime import datetime, timezone
from pyspark.sql import functions as F

from utils.spark_session import (
    create_spark_session, stop_spark_session
)
from utils.config import get_paths_config
from utils.logger import (
    log_pipeline_start, log_pipeline_end,
    log_step, log_error, save_lineage
)


# -------------------------------------------------------
# METHOD 1 : read_silver(spark, silver_path, table_name)
# PURPOSE  : Reads a Silver table into Spark DataFrame.
# PARAMS   : spark       — active SparkSession
#            silver_path — root silver folder
#            table_name  — silver table to read
# RETURNS  : DataFrame or None
# -------------------------------------------------------
def read_silver(spark, silver_path, table_name):
    path = os.path.join(silver_path, table_name)
    if not os.path.exists(path):
        log_step("READ", f"Not found: {table_name}")
        return None
    log_step("READ", f"Reading: {table_name}")
    df = spark.read.parquet(path)
    log_step("READ", f"Rows: {df.count()}")
    return df


# -------------------------------------------------------
# METHOD 2 : compute_engagement_rate(df)
# PURPOSE  : Computes engagement_rate for each video.
#
#   Formula:
#   engagement_rate = (likes + comments) / views * 100
#
#   What it means:
#   Out of everyone who watched, what % interacted?
#   Higher = audience more engaged with the content.
#   This is the KEY metric for content creators.
#
#   Example:
#   views=100000, likes=8000, comments=2000
#   rate = (8000+2000)/100000*100 = 10.0%
#
# PARAMS   : df — DataFrame with views, likes, comments
# RETURNS  : DataFrame with engagement_rate column
# -------------------------------------------------------
def compute_engagement_rate(df):
    df = df.withColumn(
        "engagement_rate",
        F.when(
            F.col("views") > 0,
            F.round(
                (
                    F.coalesce(F.col("likes"), F.lit(0)) +
                    F.coalesce(F.col("comment_count"),
                               F.lit(0))
                ) / F.col("views") * 100,
                4
            )
        ).otherwise(F.lit(0.0))
    )
    log_step("METRIC", "engagement_rate computed")
    return df


# -------------------------------------------------------
# METHOD 3 : compute_like_ratio(df)
# PURPOSE  : Computes like_ratio for each video.
#
#   Formula:
#   like_ratio = likes / views * 100
#
#   What it means:
#   What % of viewers liked the video?
#   Useful for measuring content quality.
#
# PARAMS   : df — DataFrame with views and likes
# RETURNS  : DataFrame with like_ratio column
# -------------------------------------------------------
def compute_like_ratio(df):
    df = df.withColumn(
        "like_ratio",
        F.when(
            F.col("views") > 0,
            F.round(
                F.coalesce(F.col("likes"), F.lit(0)) /
                F.col("views") * 100,
                4
            )
        ).otherwise(F.lit(0.0))
    )
    log_step("METRIC", "like_ratio computed")
    return df


# -------------------------------------------------------
# METHOD 4 : compute_view_velocity(df)
# PURPOSE  : Computes how fast a video gained views
#            relative to how many days since published.
#
#   Formula:
#   view_velocity = views / days_since_published
#
#   What it means:
#   Videos published yesterday with 1M views have
#   higher velocity than videos from 2020 with 1M views.
#   Useful for finding CURRENTLY TRENDING content.
#
# PARAMS   : df — DataFrame with views, published_at
# RETURNS  : DataFrame with view_velocity column
# -------------------------------------------------------
def compute_view_velocity(df):
    df = df.withColumn(
        "days_since_published",
        F.greatest(
            F.datediff(
                F.current_date(),
                F.to_date("published_at")
            ),
            F.lit(1)
        )
    ).withColumn(
        "view_velocity",
        F.round(
            F.coalesce(F.col("views"), F.lit(0)) /
            F.col("days_since_published"),
            2
        )
    )
    log_step("METRIC", "view_velocity computed")
    return df


# -------------------------------------------------------
# METHOD 5 : build_video_engagement(spark, silver_path)
# PURPOSE  : Builds gold_video_engagement table.
#            Joins video stats with categories.
#            Computes all engagement metrics.
#            One complete row per video.
#
#   silver_video_stats + silver_categories
#       = gold_video_engagement
#
# PARAMS   : spark       — SparkSession
#            silver_path — root silver folder
# RETURNS  : gold DataFrame or None
# -------------------------------------------------------
def build_video_engagement(spark, silver_path):
    log_step("GOLD", "Building gold_video_engagement")

    # Read silver tables
    stats_df = read_silver(
        spark, silver_path, "silver_video_stats"
    )
    cats_df  = read_silver(
        spark, silver_path, "silver_categories"
    )
    trend_df = read_silver(
        spark, silver_path, "silver_trending"
    )

    if stats_df is None:
        log_step("GOLD",
                 "ERROR: silver_video_stats missing")
        return None

    # Combine video_stats and trending
    # (trending has same structure after flattening)
    if trend_df is not None:
        # Keep only columns that exist in both
        common = [c for c in stats_df.columns
                  if c in trend_df.columns]
        combined = stats_df.union(
            trend_df.select(common)
        ).dropDuplicates(["video_id"])
        log_step("GOLD",
                 f"Combined stats+trending: "
                 f"{combined.count()} videos")
    else:
        combined = stats_df

    # Compute metrics
    combined = compute_engagement_rate(combined)
    combined = compute_like_ratio(combined)
    combined = compute_view_velocity(combined)

    # Join with categories for category_name
    if cats_df is not None:
        cats_clean = (
            cats_df
            .select("category_id", "category_name")
            .dropDuplicates(["category_id"])
        )
        combined = combined.join(
            cats_clean,
            on  = "category_id",
            how = "left"
        )
        log_step("GOLD", "Joined categories")

    # Add year and month for partitioning
    combined = (
        combined
        .withColumn("year",
            F.year("published_at"))
        .withColumn("month",
            F.month("published_at"))
    )

    # Select final gold columns
    gold_cols = [
        "video_id", "title", "channel_id",
        "channel_title", "category_id", "category_name",
        "published_at", "duration", "tag_count",
        "views", "likes", "comment_count",
        "favorite_count", "thumbnail_url",
        "engagement_rate", "like_ratio",
        "view_velocity", "days_since_published",
        "year", "month",
        "ingestion_date", "ingestion_timestamp"
    ]
    existing = [c for c in gold_cols
                if c in combined.columns]
    gold_df  = combined.select(existing)

    log_step("GOLD",
             f"gold_video_engagement: "
             f"{gold_df.count()} rows, "
             f"{len(gold_df.columns)} columns")
    return gold_df


# -------------------------------------------------------
# METHOD 6 : build_channel_performance(spark, silver_path)
# PURPOSE  : Builds gold_channel_performance table.
#            Aggregates all video metrics per channel.
#
#   Metrics:
#   total_videos, total_views, total_likes,
#   avg_views, avg_engagement, max_views
#
# PARAMS   : spark       — SparkSession
#            silver_path — root silver folder
# RETURNS  : gold DataFrame or None
# -------------------------------------------------------
def build_channel_performance(spark, silver_path):
    log_step("GOLD",
             "Building gold_channel_performance")

    stats_df = read_silver(
        spark, silver_path, "silver_video_stats"
    )
    if stats_df is None:
        return None

    stats_df = compute_engagement_rate(stats_df)

    gold_df = stats_df.groupBy("channel_id").agg(
        F.first("channel_title")    .alias("channel_title"),
        F.count("video_id")         .alias("total_videos"),
        F.sum("views")              .alias("total_views"),
        F.sum("likes")              .alias("total_likes"),
        F.sum("comment_count")      .alias("total_comments"),
        F.round(F.avg("views"), 0)  .alias("avg_views"),
        F.max("views")              .alias("max_views"),
        F.min("views")              .alias("min_views"),
        F.round(
            F.avg("engagement_rate"), 4
        )                           .alias("avg_engagement_rate"),

    )

    today = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d"
    )
    gold_df = gold_df.withColumn(
        "ingestion_date", F.lit(today)
    )

    log_step("GOLD",
             f"gold_channel_performance: "
             f"{gold_df.count()} channels")
    return gold_df


# -------------------------------------------------------
# METHOD 7 : build_category_performance(spark,
#                                        silver_path)
# PURPOSE  : Builds gold_category_performance table.
#            Shows which content categories perform best
#            in Tamil Nadu — Music, Comedy, News etc.
#
# PARAMS   : spark       — SparkSession
#            silver_path — root silver folder
# RETURNS  : gold DataFrame or None
# -------------------------------------------------------
def build_category_performance(spark, silver_path):
    log_step("GOLD",
             "Building gold_category_performance")

    stats_df = read_silver(
        spark, silver_path, "silver_video_stats"
    )
    cats_df  = read_silver(
        spark, silver_path, "silver_categories"
    )

    if stats_df is None or cats_df is None:
        return None

    stats_df = compute_engagement_rate(stats_df)

    # Join to get category names
    cats_clean = (
        cats_df
        .select("category_id", "category_name")
        .dropDuplicates(["category_id"])
    )
    joined = stats_df.join(
        cats_clean, on="category_id", how="left"
    )

    gold_df = joined.groupBy(
        "category_id", "category_name"
    ).agg(
        F.count("video_id")         .alias("total_videos"),
        F.sum("views")              .alias("total_views"),
        F.sum("likes")              .alias("total_likes"),
        F.round(F.avg("views"), 0)  .alias("avg_views"),
        F.max("views")              .alias("max_views"),
        F.round(
            F.avg("engagement_rate"), 4
        )                           .alias("avg_engagement_rate"),
    ).orderBy(F.col("total_views").desc())

    today = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d"
    )
    gold_df = gold_df.withColumn(
        "ingestion_date", F.lit(today)
    )

    log_step("GOLD",
             f"gold_category_performance: "
             f"{gold_df.count()} categories")
    return gold_df


# -------------------------------------------------------
# METHOD 8 : write_to_gold(df, gold_path, table_name,
#                          partition_cols)
# PURPOSE  : Writes gold DataFrame to Gold layer.
#            Video engagement partitioned by year/month.
#            Channel/category partitioned by date.
# PARAMS   : df             — gold DataFrame
#            gold_path      — root gold folder
#            table_name     — output table name
#            partition_cols — list of partition columns
# RETURNS  : output path
# -------------------------------------------------------
def write_to_gold(df, gold_path, table_name,
                  partition_cols=None):
    if partition_cols is None:
        partition_cols = ["ingestion_date"]

    # Only use partition cols that exist in df
    partition_cols = [
        c for c in partition_cols if c in df.columns
    ]

    path = os.path.join(gold_path, table_name)
    log_step("WRITE", f"Writing: {table_name}")
    log_step("WRITE",
             f"Partitioned by: {partition_cols}")

    (
        df.write
        .mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(path)
    )

    log_step("WRITE", f"Done -> {path}")
    return path


# -------------------------------------------------------
# METHOD 9 : run_gold_pipeline()
# PURPOSE  : MASTER METHOD — builds all gold tables.
#            This is what Airflow calls daily.
# RETURNS  : dict of written gold paths
# -------------------------------------------------------
def run_gold_pipeline():
    log_pipeline_start(
        "Gold Pipeline",
        {"Layer": "Gold", "Region": "Tamil Nadu"}
    )

    paths = get_paths_config()
    spark = create_spark_session("GoldPipeline")

    written = {}

    # ── gold_video_engagement ────────────────────────────
    try:
        df = build_video_engagement(
            spark, paths["silver"]
        )
        if df is not None:
            written["gold_video_engagement"] =                 write_to_gold(
                    df, paths["gold"],
                    "gold_video_engagement",
                    partition_cols=["year", "month"]
                )
    except Exception as e:
        log_error("gold_video_engagement", e)

    # ── gold_channel_performance ─────────────────────────
    try:
        df = build_channel_performance(
            spark, paths["silver"]
        )
        if df is not None:
            written["gold_channel_performance"] =                 write_to_gold(
                    df, paths["gold"],
                    "gold_channel_performance",
                    partition_cols=["ingestion_date"]
                )
    except Exception as e:
        log_error("gold_channel_performance", e)

    # ── gold_category_performance ────────────────────────
    try:
        df = build_category_performance(
            spark, paths["silver"]
        )
        if df is not None:
            written["gold_category_performance"] =                 write_to_gold(
                    df, paths["gold"],
                    "gold_category_performance",
                    partition_cols=["ingestion_date"]
                )
    except Exception as e:
        log_error("gold_category_performance", e)

    # Save lineage
    save_lineage(paths["lineage"], {
        "pipeline"    : "gold_pipeline",
        "source"      : paths["silver"],
        "destination" : paths["gold"],
        "tables"      : list(written.keys()),
    })

    stop_spark_session(spark)

    log_pipeline_end(
        "Gold Pipeline",
        {"Tables written": len(written)}
    )

    return written


# -------------------------------------------------------
# TEST
# -------------------------------------------------------
if __name__ == "__main__":
    run_gold_pipeline()
