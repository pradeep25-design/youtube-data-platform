# =============================================================
# warehouse_pipeline.py
# PROJECT  : Tamil Nadu YouTube Analytics Data Platform
# PURPOSE  : Loads Gold layer data into Hive warehouse.
#            Implements Kimball star schema.
#            Implements SCD Type 2 for dim_channel.
#
# TABLES LOADED:
#   dim_video            : from gold_video_engagement
#   dim_channel          : from silver_channel_stats (SCD2)
#   dim_category         : from silver_categories
#   dim_date             : generated from published dates
#   fact_video_performance: from gold_video_engagement
#
# FLOW:
#   Gold Parquet -> dim tables -> fact table -> Hive warehouse
# =============================================================

import os
from datetime import datetime, timezone, date
from pyspark.sql import functions as F

from utils.spark_session import stop_spark_session
from utils.config import get_paths_config
from utils.logger import (
    log_pipeline_start, log_pipeline_end,
    log_step, log_error, save_lineage
)


# -------------------------------------------------------
# METHOD 1 : get_warehouse_spark()
# PURPOSE  : Creates SparkSession with warehouse config.
# RETURNS  : SparkSession
# -------------------------------------------------------
def get_warehouse_spark():
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("WarehousePipeline")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.warehouse.dir",
                os.path.abspath("data/hive/warehouse"))
        .config("spark.sql.catalogImplementation",
                "in-memory")
        .config("spark.sql.parquet.compression.codec",
                "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log_step("SPARK", "Warehouse SparkSession created")
    return spark


# -------------------------------------------------------
# METHOD 2 : read_gold(spark, gold_path, table_name)
# PURPOSE  : Reads a Gold table into Spark DataFrame.
# PARAMS   : spark      — SparkSession
#            gold_path  — root gold folder
#            table_name — gold table to read
# RETURNS  : DataFrame or None
# -------------------------------------------------------
def read_gold(spark, gold_path, table_name):
    path = os.path.join(gold_path, table_name)
    if not os.path.exists(path):
        log_step("READ", f"Not found: {table_name}")
        return None
    log_step("READ", f"Reading gold: {table_name}")
    df = spark.read.parquet(path)
    log_step("READ", f"Rows: {df.count()}")
    return df


# -------------------------------------------------------
# METHOD 3 : read_silver(spark, silver_path, table_name)
# PURPOSE  : Reads a Silver table into Spark DataFrame.
# -------------------------------------------------------
def read_silver(spark, silver_path, table_name):
    path = os.path.join(silver_path, table_name)
    if not os.path.exists(path):
        log_step("READ", f"Not found: {table_name}")
        return None
    log_step("READ", f"Reading silver: {table_name}")
    df = spark.read.parquet(path)
    log_step("READ", f"Rows: {df.count()}")
    return df


# -------------------------------------------------------
# METHOD 4 : write_dim(df, warehouse_path, table_name)
# PURPOSE  : Writes a dimension table to warehouse.
# -------------------------------------------------------
def write_dim(df, warehouse_path, table_name):
    path = os.path.join(warehouse_path, table_name)
    log_step("WRITE", f"Writing: {table_name}")
    df.write.mode("overwrite").parquet(path)
    log_step("WRITE",
             f"Done -> {table_name} "
             f"({df.count()} rows)")
    return path


# -------------------------------------------------------
# METHOD 5 : load_dim_video(spark, gold_df,
#                            warehouse_path)
# PURPOSE  : Loads dim_video from gold_video_engagement.
#            One row per unique video.
# -------------------------------------------------------
def load_dim_video(spark, gold_df, warehouse_path):
    log_step("DIM", "--- Loading dim_video ---")

    want = [
        "video_id", "title", "channel_id",
        "channel_title", "category_id",
        "published_at", "duration",
        "tag_count", "thumbnail_url"
    ]
    cols   = [c for c in want if c in gold_df.columns]
    dim_df = (
        gold_df
        .select(cols)
        .dropDuplicates(["video_id"])
        .filter(F.col("video_id").isNotNull())
    )

    return write_dim(dim_df, warehouse_path, "dim_video")


# -------------------------------------------------------
# METHOD 6 : load_dim_category(spark, silver_path,
#                               warehouse_path)
# PURPOSE  : Loads dim_category from silver_categories.
# -------------------------------------------------------
def load_dim_category(spark, silver_path,
                      warehouse_path):
    log_step("DIM", "--- Loading dim_category ---")

    df = read_silver(
        spark, silver_path, "silver_categories"
    )
    if df is None:
        return None

    dim_df = (
        df
        .select("category_id", "category_name",
                "assignable")
        .dropDuplicates(["category_id"])
        .filter(F.col("category_id").isNotNull())
    )

    return write_dim(
        dim_df, warehouse_path, "dim_category"
    )


# -------------------------------------------------------
# METHOD 7 : load_dim_date(spark, gold_df,
#                           warehouse_path)
# PURPOSE  : Builds dim_date from unique publish dates.
#            Extracts year, month, quarter, day_of_week.
# -------------------------------------------------------
def load_dim_date(spark, gold_df, warehouse_path):
    log_step("DIM", "--- Loading dim_date ---")

    dim_df = (
        gold_df
        .select(
            F.to_date("published_at").alias("full_date")
        )
        .dropDuplicates()
        .filter(F.col("full_date").isNotNull())
        .withColumn("date_id",
            F.date_format("full_date", "yyyyMMdd"))
        .withColumn("year",
            F.year("full_date"))
        .withColumn("month",
            F.month("full_date"))
        .withColumn("month_name",
            F.date_format("full_date", "MMMM"))
        .withColumn("quarter",
            F.quarter("full_date"))
        .withColumn("day_of_week",
            F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend",
            F.dayofweek("full_date").isin([1, 7]))
    )

    return write_dim(dim_df, warehouse_path, "dim_date")


# -------------------------------------------------------
# METHOD 8 : apply_scd2(spark, new_df, warehouse_path)
# PURPOSE  : Implements SCD Type 2 for dim_channel.
#
#   WHAT IS SCD TYPE 2?
#   When channel subscriber count changes we dont
#   overwrite. We keep history:
#
#   channel_id | subs | start      | end        | current
#   SunTV      | 5M   | 2025-01-01 | 2025-06-01 | False
#   SunTV      | 6M   | 2025-06-01 | 9999-12-31 | True
#
#   This lets you ask: what was SunTV subs in March 2025?
#
# PARAMS   : spark         — SparkSession
#            new_df        — new channel data from silver
#            warehouse_path — warehouse folder
# RETURNS  : output path
# -------------------------------------------------------
def apply_scd2(spark, new_df, warehouse_path):
    log_step("SCD2", "--- Applying SCD2 dim_channel ---")

    today      = date.today()
    far_future = date(9999, 12, 31)
    out_path   = os.path.join(
        warehouse_path, "dim_channel"
    )

    new_df = (
        new_df
        .select(
            "channel_id", "channel_name", "country",
            "subscriber_count", "total_views",
            "video_count"
        )
        .dropDuplicates(["channel_id"])
        .filter(F.col("channel_id").isNotNull())
    )

    if os.path.exists(out_path):
        log_step("SCD2",
                 "Existing dim_channel found")
        existing = spark.read.parquet(out_path)
        current  = existing.filter(
            F.col("is_current") == True
        )

        # Find channels where subscriber_count changed
        changed = (
            new_df.alias("n")
            .join(current.alias("o"),
                  on="channel_id", how="inner")
            .filter(
                F.col("n.subscriber_count") !=
                F.col("o.subscriber_count")
            )
            .select("n.channel_id")
        )
        changed_ids = [
            r.channel_id for r in changed.collect()
        ]
        log_step("SCD2",
                 f"Changed channels: {len(changed_ids)}")

        # Expire old records for changed channels
        if changed_ids:
            updated = existing.withColumn(
                "effective_end_date",
                F.when(
                    F.col("channel_id").isin(changed_ids)
                    & F.col("is_current"),
                    F.lit(today)
                ).otherwise(F.col("effective_end_date"))
            ).withColumn(
                "is_current",
                F.when(
                    F.col("channel_id").isin(changed_ids)
                    & F.col("is_current"),
                    F.lit(False)
                ).otherwise(F.col("is_current"))
            )
        else:
            updated = existing

        # New rows for changed channels
        new_rows = (
            new_df
            .filter(
                F.col("channel_id").isin(changed_ids)
            )
            .withColumn("channel_key",
                F.monotonically_increasing_id()
                 .cast("int"))
            .withColumn("effective_start_date",
                F.lit(today))
            .withColumn("effective_end_date",
                F.lit(far_future))
            .withColumn("is_current", F.lit(True))
        )

        # Truly new channels not in existing
        exist_ids = [
            r.channel_id for r in
            existing.select("channel_id")
                    .distinct().collect()
        ]
        truly_new = (
            new_df
            .filter(~F.col("channel_id").isin(exist_ids))
            .withColumn("channel_key",
                F.monotonically_increasing_id()
                 .cast("int"))
            .withColumn("effective_start_date",
                F.lit(today))
            .withColumn("effective_end_date",
                F.lit(far_future))
            .withColumn("is_current", F.lit(True))
        )

        final = updated
        if changed_ids:
            final = final.union(
                new_rows.select(updated.columns)
            )
        if truly_new.count() > 0:
            final = final.union(
                truly_new.select(updated.columns)
            )

    else:
        log_step("SCD2",
                 "First load — creating dim_channel")
        final = (
            new_df
            .withColumn("channel_key",
                F.monotonically_increasing_id()
                 .cast("int"))
            .withColumn("effective_start_date",
                F.lit(today))
            .withColumn("effective_end_date",
                F.lit(far_future))
            .withColumn("is_current", F.lit(True))
        )

    log_step("SCD2",
             f"Total rows: {final.count()}")
    final.write.mode("overwrite").parquet(out_path)
    log_step("SCD2", f"Written -> {out_path}")
    return out_path


# -------------------------------------------------------
# METHOD 9 : load_fact_video_performance(spark, gold_df,
#                                         warehouse_path)
# PURPOSE  : Loads fact_video_performance — central table.
#            One row per video, all metrics as measures,
#            foreign keys to all dimension tables.
#            Partitioned by year and month.
# -------------------------------------------------------
def load_fact_video_performance(spark, gold_df,
                                warehouse_path):
    log_step("FACT",
             "--- Loading fact_video_performance ---")

    fact_df = (
        gold_df
        .withColumn("date_id",
            F.date_format(
                F.to_date("published_at"), "yyyyMMdd"
            )
        )
    )

    want = [
        "video_id", "channel_id", "category_id",
        "date_id", "views", "likes", "comment_count",
        "favorite_count", "tag_count",
        "engagement_rate", "like_ratio",
        "view_velocity", "year", "month"
    ]
    cols    = [c for c in want if c in fact_df.columns]
    fact_df = (
        fact_df
        .select(cols)
        .filter(F.col("video_id").isNotNull())
        .dropDuplicates(["video_id"])
    )

    out_path = os.path.join(
        warehouse_path, "fact_video_performance"
    )
    log_step("FACT", f"Rows: {fact_df.count()}")

    (
        fact_df.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(out_path)
    )

    log_step("FACT", f"Written -> {out_path}")
    return out_path


# -------------------------------------------------------
# METHOD 10 : run_warehouse_pipeline()
# PURPOSE  : MASTER METHOD — loads all warehouse tables.
#            This is what Airflow calls daily.
# RETURNS  : dict of written paths
# -------------------------------------------------------
def run_warehouse_pipeline():
    log_pipeline_start(
        "Warehouse Pipeline",
        {
            "Schema"  : "Kimball Star Schema",
            "Format"  : "External Parquet Tables",
            "SCD"     : "Type 2 on dim_channel"
        }
    )

    paths          = get_paths_config()
    warehouse_path = paths["warehouse"]
    os.makedirs(warehouse_path, exist_ok=True)

    spark   = get_warehouse_spark()
    written = {}

    # Read gold_video_engagement (main source)
    gold_df = read_gold(
        spark, paths["gold"], "gold_video_engagement"
    )
    if gold_df is None:
        log_step("ERROR",
                 "gold_video_engagement missing!")
        stop_spark_session(spark)
        return {}

    # ── Load dimension tables ────────────────────────────
    try:
        written["dim_video"] = load_dim_video(
            spark, gold_df, warehouse_path
        )
    except Exception as e:
        log_error("dim_video", e)

    try:
        written["dim_category"] = load_dim_category(
            spark, paths["silver"], warehouse_path
        )
    except Exception as e:
        log_error("dim_category", e)

    try:
        written["dim_date"] = load_dim_date(
            spark, gold_df, warehouse_path
        )
    except Exception as e:
        log_error("dim_date", e)

    # SCD2 for dim_channel
    # Use gold_channel_performance as source
    try:
        ch_gold = read_gold(
            spark, paths["gold"],
            "gold_channel_performance"
        )
        if ch_gold is not None:
            # Rename to match SCD2 expected columns
            ch_df = ch_gold.withColumnRenamed(
                "channel_title", "channel_name"
            )
            if "country" not in ch_df.columns:
                ch_df = ch_df.withColumn(
                    "country", F.lit(None).cast("string")
                )
            if "total_views" not in ch_df.columns:
                ch_df = ch_df.withColumn(
                    "total_views", F.col("total_views")
                    if "total_views" in ch_df.columns
                    else F.lit(0).cast("long")
                )
            if "subscriber_count" not in ch_df.columns:
                ch_df = ch_df.withColumn(
                    "subscriber_count",
                    F.lit(0).cast("long")
                )
            if "video_count" not in ch_df.columns:
                ch_df = ch_df.withColumn(
                    "video_count",
                    F.col("total_videos")
                    if "total_videos" in ch_df.columns
                    else F.lit(0).cast("long")
                )
            written["dim_channel"] = apply_scd2(
                spark, ch_df, warehouse_path
            )
    except Exception as e:
        log_error("dim_channel SCD2", e)

    # ── Load fact table ──────────────────────────────────
    try:
        written["fact_video_performance"] =             load_fact_video_performance(
                spark, gold_df, warehouse_path
            )
    except Exception as e:
        log_error("fact_video_performance", e)

    # Save lineage
    save_lineage(paths["lineage"], {
        "pipeline"    : "warehouse_pipeline",
        "source"      : paths["gold"],
        "destination" : warehouse_path,
        "tables"      : list(written.keys()),
        "schema"      : "Kimball Star Schema",
        "scd_type"    : "SCD2 on dim_channel",
    })

    stop_spark_session(spark)

    log_pipeline_end(
        "Warehouse Pipeline",
        {"Tables loaded": len(written)}
    )

    return written


# -------------------------------------------------------
# TEST
# -------------------------------------------------------
if __name__ == "__main__":
    run_warehouse_pipeline()
