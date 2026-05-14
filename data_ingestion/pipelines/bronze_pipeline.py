# =============================================================
# bronze_pipeline.py
# PROJECT  : Tamil Nadu YouTube Analytics Data Platform
# PURPOSE  : Reads raw JSON files saved by api_collector,
#            adds ingestion metadata, writes to Bronze layer
#            as partitioned Parquet files.
#
# BRONZE LAYER RULES (from PDF):
#   - Raw JSON as-is — NO cleaning or transformation
#   - Append only — never overwrite old data
#   - Add ingestion metadata (date, source, timestamp)
#   - Partition by ingestion_date
#   - Schema-on-read (keep nested structures)
#
# FLOW:
#   raw JSON files -> PySpark -> add metadata -> Parquet
# =============================================================

import os
from datetime import datetime, timezone
from pyspark.sql import functions as F

from utils.spark_session import create_spark_session, stop_spark_session
from utils.config import get_paths_config
from utils.logger import (
    log_pipeline_start, log_pipeline_end,
    log_step, log_error, save_lineage
)


# -------------------------------------------------------
# METHOD 1 : load_raw_json(spark, json_path)
# PURPOSE  : Reads a raw JSON file into Spark DataFrame.
#            multiLine=True because each file is one big
#            JSON array [...] not one object per line.
#            PERMISSIVE mode means bad records dont crash.
# PARAMS   : spark     — active SparkSession
#            json_path — full path to JSON file
# RETURNS  : DataFrame or None if file not found
# -------------------------------------------------------
def load_raw_json(spark, json_path):
    if not os.path.exists(json_path):
        log_step("LOAD", f"File not found: {json_path}")
        return None

    log_step("LOAD", f"Reading: {json_path}")

    df = (
        spark.read
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .json(json_path)
    )

    count = df.count()
    log_step("LOAD",
             f"Rows: {count} | Cols: {len(df.columns)}")
    return df


# -------------------------------------------------------
# METHOD 2 : add_ingestion_metadata(df, source_name)
# PURPOSE  : Adds 3 tracking columns to every row.
#            These columns are essential for data lineage
#            and incremental processing downstream.
#
#   ingestion_date      : "2026-05-12" (for partitioning)
#   ingestion_timestamp : "2026-05-12 08:44:05" (exact time)
#   source              : "youtube_api_trending" (origin)
#
# PARAMS   : df          — input DataFrame
#            source_name — label for this data source
# RETURNS  : DataFrame with 3 extra metadata columns
# -------------------------------------------------------
def add_ingestion_metadata(df, source_name):
    now       = datetime.now(timezone.utc)
    today_str = now.strftime("%Y-%m-%d")
    now_str   = now.strftime("%Y-%m-%d %H:%M:%S")

    df = (
        df
        .withColumn("ingestion_date",
                    F.lit(today_str))
        .withColumn("ingestion_timestamp",
                    F.lit(now_str))
        .withColumn("source",
                    F.lit(source_name))
    )

    log_step("META",
             f"Added metadata — source: {source_name}"
             f" date: {today_str}")
    return df


# -------------------------------------------------------
# METHOD 3 : write_to_bronze(df, bronze_path, table_name)
# PURPOSE  : Writes DataFrame to Bronze layer as Parquet.
#            Partitioned by ingestion_date so queries
#            on specific dates only read that partition.
#            mode=append means old data is never lost.
# PARAMS   : df          — DataFrame with metadata
#            bronze_path — root bronze folder
#            table_name  — e.g. "bronze_video_stats"
# RETURNS  : full output path
# -------------------------------------------------------
def write_to_bronze(df, bronze_path, table_name):
    output_path = os.path.join(bronze_path, table_name)

    log_step("WRITE",
             f"Writing to: {output_path}")

    (
        df.write
        .mode("append")
        .partitionBy("ingestion_date")
        .parquet(output_path)
    )

    log_step("WRITE",
             f"Done -> {table_name} "
             f"(partitioned by ingestion_date)")
    return output_path


# -------------------------------------------------------
# METHOD 4 : process_source(spark, json_path,
#                            bronze_path, table_name,
#                            source_label)
# PURPOSE  : Single method that handles ONE source file:
#            Load JSON -> Add metadata -> Write Bronze
#            Every source calls this same method.
#            If this source fails, others still run.
# PARAMS   : spark        — SparkSession
#            json_path    — path to raw JSON file
#            bronze_path  — bronze root folder
#            table_name   — bronze table name
#            source_label — source tag for lineage
# RETURNS  : output path or None if failed
# -------------------------------------------------------
def process_source(spark, json_path, bronze_path,
                   table_name, source_label):
    log_step("SOURCE", f"Processing: {table_name}")

    df = load_raw_json(spark, json_path)
    if df is None:
        log_step("SOURCE",
                 f"Skipping {table_name} — file missing")
        return None

    df = add_ingestion_metadata(df, source_label)
    return write_to_bronze(df, bronze_path, table_name)


# -------------------------------------------------------
# METHOD 5 : run_bronze_pipeline()
# PURPOSE  : MASTER METHOD — processes all raw JSON files
#            into Bronze layer tables.
#            Each source is processed independently.
#            Uses a source map so adding new sources
#            only requires adding one line to the map.
#            This is what Airflow calls daily.
# RETURNS  : dict of written bronze table paths
# -------------------------------------------------------
def run_bronze_pipeline():
    log_pipeline_start(
        "Bronze Pipeline",
        {"Layer": "Bronze", "Format": "Parquet"}
    )

    paths = get_paths_config()
    spark = create_spark_session("BronzePipeline")

    today      = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d"
    )
    raw_folder  = os.path.join(paths["raw_json"], today)
    bronze_path = paths["bronze"]

    log_step("CONFIG", f"Raw folder  : {raw_folder}")
    log_step("CONFIG", f"Bronze path : {bronze_path}")

    # Source map:
    # json_filename -> (bronze_table_name, source_label)
    # To add a new source — add one line here only!
    sources = {
        "raw_trending.json": (
            "bronze_trending",
            "youtube_api_trending"
        ),
        "raw_trending_by_category.json": (
            "bronze_trending_by_category",
            "youtube_api_trending_category"
        ),
        "raw_channel_videos.json": (
            "bronze_channel_videos",
            "youtube_api_channel_search"
        ),
        "raw_keyword_videos.json": (
            "bronze_keyword_videos",
            "youtube_api_keyword_search"
        ),
        "raw_video_stats.json": (
            "bronze_video_stats",
            "youtube_api_video_stats"
        ),
        "raw_channel_stats.json": (
            "bronze_channel_stats",
            "youtube_api_channel_stats"
        ),
        "raw_categories.json": (
            "bronze_categories",
            "youtube_api_categories"
        ),
    }

    written = {}

    for filename, (table_name, source_label) in             sources.items():
        try:
            json_path = os.path.join(
                raw_folder, filename
            )
            path = process_source(
                spark, json_path, bronze_path,
                table_name, source_label
            )
            if path:
                written[table_name] = path
        except Exception as e:
            log_error(table_name, e)

    # Save lineage
    save_lineage(paths["lineage"], {
        "pipeline"    : "bronze_pipeline",
        "source"      : "raw_json",
        "destination" : bronze_path,
        "tables"      : list(written.keys()),
        "date"        : today,
    })

    stop_spark_session(spark)

    log_pipeline_end(
        "Bronze Pipeline",
        {
            "Tables written" : len(written),
            "Bronze path"    : bronze_path,
        }
    )

    return written


# -------------------------------------------------------
# TEST — run: python data_ingestion/pipelines/bronze_pipeline.py
# -------------------------------------------------------
if __name__ == "__main__":
    run_bronze_pipeline()
