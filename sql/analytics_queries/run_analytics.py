# =============================================================
# run_analytics.py
# PROJECT  : Tamil Nadu YouTube Analytics Data Platform
# PURPOSE  : Executes SQL analytics queries from .sql files
#            using PySpark SQL engine.
#            Loads warehouse tables as Spark temp views
#            then runs each .sql file against them.
# PDF REF  : Section 10 - Analytics Queries
# =============================================================

import os
from pyspark.sql import SparkSession
from utils.logger import log_step, log_error


# -------------------------------------------------------
# METHOD 1 : create_analytics_spark()
# PURPOSE  : Creates SparkSession for analytics queries.
# -------------------------------------------------------
def create_analytics_spark():
    spark = (
        SparkSession.builder
        .appName("TamilNaduAnalytics")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# -------------------------------------------------------
# METHOD 2 : load_warehouse_tables(spark)
# PURPOSE  : Loads all warehouse parquet files as
#            Spark SQL temporary views so SQL queries
#            can run against them directly.
# PARAMS   : spark — SparkSession
# RETURNS  : list of loaded table names
# -------------------------------------------------------
def load_warehouse_tables(spark,
        warehouse_path="data/hive/warehouse"):

    tables = [
        "fact_video_performance",
        "dim_video",
        "dim_channel",
        "dim_category",
        "dim_date",
    ]

    loaded = []
    log_step("LOAD", "Loading warehouse tables...")

    for table in tables:
        path = os.path.join(warehouse_path, table)
        if os.path.exists(path):
            df = spark.read.parquet(path)
            df.createOrReplaceTempView(table)
            log_step("LOAD",
                     f"{table} ({df.count()} rows)")
            loaded.append(table)
        else:
            log_step("LOAD",
                     f"MISSING: {table}")

    return loaded


# -------------------------------------------------------
# METHOD 3 : run_sql_file(spark, sql_file_path)
# PURPOSE  : Reads and executes one .sql file.
#            Prints results to terminal.
# PARAMS   : spark         — SparkSession
#            sql_file_path — path to .sql file
# RETURNS  : result DataFrame or None
# -------------------------------------------------------
def run_sql_file(spark, sql_file_path):
    filename = os.path.basename(sql_file_path)

    with open(sql_file_path, "r") as f:
        sql = f.read()

    # Remove comment lines for execution
    lines = [
        line for line in sql.split("\n")
        if not line.strip().startswith("--")
        and line.strip()
    ]
    clean_sql = " ".join(lines).strip()

    if not clean_sql:
        return None

    try:
        result = spark.sql(clean_sql)
        count  = result.count()
        log_step("RESULT",
                 f"{filename} -> {count} rows")
        result.show(30, truncate=50)
        return result
    except Exception as e:
        log_error(filename, e)
        return None


# -------------------------------------------------------
# METHOD 4 : run_all_queries(spark, queries_folder)
# PURPOSE  : Runs all .sql files in the queries folder.
# PARAMS   : spark          — SparkSession
#            queries_folder — path to sql files
# -------------------------------------------------------
def run_all_queries(spark,
        queries_folder="sql/analytics_queries"):

    sql_files = sorted([
        os.path.join(queries_folder, f)
        for f in os.listdir(queries_folder)
        if f.endswith(".sql")
    ])

    log_step("QUERIES",
             f"Found {len(sql_files)} SQL files")

    for sql_file in sql_files:
        name = os.path.basename(sql_file)
        print(f"\n{'='*60}")
        print(f"  QUERY: {name}")
        print(f"{'='*60}")
        run_sql_file(spark, sql_file)


# -------------------------------------------------------
# MAIN
# -------------------------------------------------------
if __name__ == "__main__":
    print("="*60)
    print("Tamil Nadu YouTube Analytics — SQL Queries")
    print("="*60)

    spark  = create_analytics_spark()
    loaded = load_warehouse_tables(spark)

    if not loaded:
        print("ERROR: No tables loaded. Run pipeline first.")
        spark.stop()
        exit(1)

    run_all_queries(spark)
    spark.stop()
    print("\nAll queries complete!")
