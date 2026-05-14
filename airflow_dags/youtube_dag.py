# =============================================================
# youtube_dag.py
# PROJECT  : Tamil Nadu YouTube Analytics Data Platform
# PURPOSE  : Airflow DAG that orchestrates the full pipeline.
#            Runs daily at 6 AM IST automatically.
#
# PDF REF  : Section 12 — Scheduling
#
# DAG FLOW (as per PDF):
#   ingestion_task
#       -> bronze_to_silver_task
#           -> silver_to_gold_task
#               -> warehouse_load_task
#
# SCHEDULE : Once per day (daily batch job)
# =============================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


# -------------------------------------------------------
# DEFAULT ARGS
# PURPOSE : Applied to all tasks in the DAG.
#           retries=1 means if task fails, try once more.
# -------------------------------------------------------
default_args = {
    "owner"           : "tamil_nadu_analytics",
    "depends_on_past" : False,
    "start_date"      : datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry"  : False,
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=5),
}


# -------------------------------------------------------
# TASK FUNCTIONS
# Each task calls the pipeline master method.
# If one task fails Airflow retries it automatically.
# Other tasks are not affected.
# -------------------------------------------------------

# -------------------------------------------------------
# TASK 1 : ingestion_task()
# PURPOSE : Fetches Tamil Nadu trending data from
#           YouTube Data API v3.
#           Saves raw JSON to data/raw_json/
# PDF REF : Section 4 — Data Sources
# -------------------------------------------------------
def ingestion_task():
    import sys
    sys.path.insert(0, "/app")
    from data_ingestion.api_collectors.api_collector         import run_ingestion
    print("Starting Tamil Nadu YouTube ingestion...")
    result = run_ingestion()
    print(f"Ingestion complete: {result}")
    return str(result)


# -------------------------------------------------------
# TASK 2 : bronze_to_silver_task()
# PURPOSE : Reads raw JSON from bronze layer,
#           validates data quality,
#           writes clean data to silver layer.
# PDF REF : Section 6 — Data Lake Design
# -------------------------------------------------------
def bronze_to_silver_task():
    import sys
    sys.path.insert(0, "/app")

    # Step 1: Bronze
    from data_ingestion.pipelines.bronze_pipeline         import run_bronze_pipeline
    print("Running Bronze Pipeline...")
    bronze_result = run_bronze_pipeline()
    print(f"Bronze complete: {len(bronze_result)} tables")

    # Step 2: Silver
    from data_ingestion.pipelines.silver_pipeline         import run_silver_pipeline
    print("Running Silver Pipeline...")
    silver_result = run_silver_pipeline()
    print(f"Silver complete: {len(silver_result)} tables")

    return str(silver_result)


# -------------------------------------------------------
# TASK 3 : silver_to_gold_task()
# PURPOSE : Reads silver tables, joins them,
#           computes engagement metrics,
#           writes business-ready gold datasets.
# PDF REF : Section 6 — Gold Layer
# -------------------------------------------------------
def silver_to_gold_task():
    import sys
    sys.path.insert(0, "/app")
    from data_ingestion.pipelines.gold_pipeline         import run_gold_pipeline
    print("Running Gold Pipeline...")
    result = run_gold_pipeline()
    print(f"Gold complete: {len(result)} tables")
    return str(result)


# -------------------------------------------------------
# TASK 4 : warehouse_load_task()
# PURPOSE : Loads gold data into Hive warehouse.
#           Implements Kimball star schema.
#           Applies SCD Type 2 for dim_channel.
# PDF REF : Section 9 — Dimensional Warehouse
# -------------------------------------------------------
def warehouse_load_task():
    import sys
    sys.path.insert(0, "/app")
    from data_ingestion.pipelines.warehouse_pipeline         import run_warehouse_pipeline
    print("Running Warehouse Pipeline...")
    result = run_warehouse_pipeline()
    print(f"Warehouse complete: {len(result)} tables")
    return str(result)


# -------------------------------------------------------
# DAG DEFINITION
# schedule_interval="0 1 * * *" = every day at 1 AM UTC
# (6 AM IST = 1 AM UTC)
# -------------------------------------------------------
with DAG(
    dag_id            = "tamil_nadu_youtube_analytics",
    default_args      = default_args,
    description       = (
        "Daily batch pipeline for Tamil Nadu "
        "YouTube Analytics Data Platform"
    ),
    schedule_interval = "0 1 * * *",
    catchup           = False,
    tags              = [
        "youtube", "tamil_nadu",
        "pyspark", "batch"
    ],
) as dag:

    # ── Start marker ─────────────────────────────────────
    start = EmptyOperator(
        task_id = "pipeline_start"
    )

    # ── Task 1: Ingest from YouTube API ─────────────────
    ingest = PythonOperator(
        task_id         = "ingestion_task",
        python_callable = ingestion_task,
    )

    # ── Task 2: Bronze + Silver ──────────────────────────
    bronze_silver = PythonOperator(
        task_id         = "bronze_to_silver_task",
        python_callable = bronze_to_silver_task,
    )

    # ── Task 3: Gold ─────────────────────────────────────
    gold = PythonOperator(
        task_id         = "silver_to_gold_task",
        python_callable = silver_to_gold_task,
    )

    # ── Task 4: Warehouse ────────────────────────────────
    warehouse = PythonOperator(
        task_id         = "warehouse_load_task",
        python_callable = warehouse_load_task,
    )

    # ── End marker ───────────────────────────────────────
    end = EmptyOperator(
        task_id = "pipeline_complete"
    )

    # ── Task dependencies (PDF section 12) ──────────────
    # start -> ingest -> bronze_silver -> gold -> warehouse -> end
    start >> ingest >> bronze_silver >> gold >> warehouse >> end
