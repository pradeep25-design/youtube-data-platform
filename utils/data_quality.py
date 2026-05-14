# =============================================================
# data_quality.py
# PROJECT  : Tamil Nadu YouTube Analytics Data Platform
# PURPOSE  : Reusable data quality validation functions.
#            Used by silver pipeline to validate every row.
#            Good rows -> Silver | Bad rows -> Quarantine
#
# CHECKS (as per PDF section 5):
#   Completeness : no null in required columns
#   Validity     : views >= 0, likes >= 0
#   Uniqueness   : no duplicate video_id
# =============================================================

from pyspark.sql import functions as F
from utils.logger import log_step


# -------------------------------------------------------
# METHOD 1 : check_completeness(df, required_cols)
# PURPOSE  : Finds rows where required columns are NULL.
#            e.g. a video with no video_id is useless.
# PARAMS   : df            — input DataFrame
#            required_cols — columns that must not be null
# RETURNS  : tuple (good_df, bad_df)
# -------------------------------------------------------
def check_completeness(df, required_cols):
    existing = [c for c in required_cols
                if c in df.columns]

    if not existing:
        log_step("DQ", "No required cols found — skipping")
        return df, df.limit(0).withColumn(
            "dq_issue", F.lit("")
        )

    condition = F.lit(True)
    for col in existing:
        condition = condition & F.col(col).isNotNull()

    good_df = df.filter(condition)
    bad_df  = (
        df.filter(~condition)
        .withColumn(
            "dq_issue",
            F.lit(f"NULL in: {existing}")
        )
    )

    log_step("DQ_COMPLETE",
             f"Good: {good_df.count()} "
             f"| Bad: {bad_df.count()}")
    return good_df, bad_df


# -------------------------------------------------------
# METHOD 2 : check_validity(df, non_negative_cols)
# PURPOSE  : Finds rows where numeric columns < 0.
#            views=-5 or likes=-1 is impossible data.
# PARAMS   : df               — input DataFrame
#            non_negative_cols — columns that must be >= 0
# RETURNS  : tuple (good_df, bad_df)
# -------------------------------------------------------
def check_validity(df, non_negative_cols):
    existing = [c for c in non_negative_cols
                if c in df.columns]

    if not existing:
        return df, df.limit(0).withColumn(
            "dq_issue", F.lit("")
        )

    condition = F.lit(True)
    for col in existing:
        condition = condition & (
            F.col(col).isNull() | (F.col(col) >= 0)
        )

    good_df = df.filter(condition)
    bad_df  = (
        df.filter(~condition)
        .withColumn(
            "dq_issue",
            F.lit(f"Negative values in: {existing}")
        )
    )

    log_step("DQ_VALID",
             f"Good: {good_df.count()} "
             f"| Bad: {bad_df.count()}")
    return good_df, bad_df


# -------------------------------------------------------
# METHOD 3 : check_uniqueness(df, key_col)
# PURPOSE  : Removes duplicate rows by key column.
#            Keeps latest record per key.
# PARAMS   : df      — input DataFrame
#            key_col — column that must be unique
# RETURNS  : deduplicated DataFrame
# -------------------------------------------------------
def check_uniqueness(df, key_col):
    if key_col not in df.columns:
        log_step("DQ_UNIQUE",
                 f"Column {key_col} not found — skipping")
        return df

    before = df.count()

    if "ingestion_timestamp" in df.columns:
        df = df.orderBy(
            F.col("ingestion_timestamp").desc()
        )

    df    = df.dropDuplicates([key_col])
    after = df.count()

    log_step("DQ_UNIQUE",
             f"Before: {before} "
             f"| After: {after} "
             f"| Removed: {before - after}")
    return df


# -------------------------------------------------------
# METHOD 4 : run_all_checks(df, required_cols,
#                           non_negative_cols, key_col)
# PURPOSE  : Runs all 3 checks in sequence.
#            Collects all bad rows into quarantine df.
# PARAMS   : df               — input DataFrame
#            required_cols    — for completeness check
#            non_negative_cols — for validity check
#            key_col          — for uniqueness check
# RETURNS  : tuple (clean_df, quarantine_df)
# -------------------------------------------------------
def run_all_checks(df, required_cols,
                   non_negative_cols, key_col):
    quarantine_dfs = []

    # Check 1 — Completeness
    df, bad1 = check_completeness(df, required_cols)
    if bad1.count() > 0:
        quarantine_dfs.append(bad1)

    # Check 2 — Validity
    df, bad2 = check_validity(df, non_negative_cols)
    if bad2.count() > 0:
        quarantine_dfs.append(bad2)

    # Check 3 — Uniqueness
    df = check_uniqueness(df, key_col)

    # Combine all bad rows
    if quarantine_dfs:
        quarantine = quarantine_dfs[0]
        for q in quarantine_dfs[1:]:
            common = [c for c in quarantine.columns
                      if c in q.columns]
            quarantine = (
                quarantine.select(common)
                .union(q.select(common))
            )
    else:
        quarantine = df.limit(0).withColumn(
            "dq_issue", F.lit("")
        )

    log_step("DQ_RESULT",
             f"Clean: {df.count()} "
             f"| Quarantine: {quarantine.count()}")
    return df, quarantine
