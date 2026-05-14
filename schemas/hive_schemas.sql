-- =============================================================
-- hive_schemas.sql
-- PROJECT  : Tamil Nadu YouTube Analytics Data Platform
-- PURPOSE  : CREATE TABLE statements for Hive warehouse.
--            ALL tables are EXTERNAL as per PDF.
--            External = Hive reads files but does NOT own
--            them. Dropping table keeps data files safe.
--
-- STAR SCHEMA (Kimball):
--   fact_video_performance (center)
--       -> dim_video
--       -> dim_channel (SCD Type 2)
--       -> dim_category
--       -> dim_date
-- =============================================================

-- Drop existing tables (clean recreation)
DROP TABLE IF EXISTS dim_video;
DROP TABLE IF EXISTS dim_channel;
DROP TABLE IF EXISTS dim_category;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS fact_video_performance;

-- =============================================================
-- DIMENSION 1 : dim_video
-- PURPOSE : Describes each video (what it is)
-- =============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS dim_video (
    video_id          STRING,
    title             STRING,
    channel_id        STRING,
    channel_title     STRING,
    category_id       STRING,
    published_at      TIMESTAMP,
    duration          STRING,
    tag_count         INT,
    thumbnail_url     STRING
)
STORED AS PARQUET
LOCATION 'data/hive/warehouse/dim_video';

-- =============================================================
-- DIMENSION 2 : dim_channel (SCD Type 2)
-- PURPOSE : Channel info with full history of changes.
--           When subscriber_count changes, old row is
--           expired and new row is inserted.
--
-- SCD Type 2 columns:
--   effective_start_date : when this row became valid
--   effective_end_date   : when this row expired
--                          9999-12-31 = currently active
--   is_current           : TRUE if this is latest record
-- =============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS dim_channel (
    channel_key            INT,
    channel_id             STRING,
    channel_name           STRING,
    country                STRING,
    subscriber_count       BIGINT,
    total_views            BIGINT,
    video_count            BIGINT,
    effective_start_date   DATE,
    effective_end_date     DATE,
    is_current             BOOLEAN
)
STORED AS PARQUET
LOCATION 'data/hive/warehouse/dim_channel';

-- =============================================================
-- DIMENSION 3 : dim_category
-- PURPOSE : Maps category_id to category_name
-- =============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS dim_category (
    category_id    STRING,
    category_name  STRING,
    assignable     BOOLEAN
)
STORED AS PARQUET
LOCATION 'data/hive/warehouse/dim_category';

-- =============================================================
-- DIMENSION 4 : dim_date
-- PURPOSE : Date dimension for time-based analysis.
--           Allows queries like "videos published on weekends"
--           or "views in Q4 2025"
-- =============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS dim_date (
    date_id       STRING,
    full_date     DATE,
    year          INT,
    month         INT,
    month_name    STRING,
    quarter       INT,
    day_of_week   STRING,
    is_weekend    BOOLEAN
)
STORED AS PARQUET
LOCATION 'data/hive/warehouse/dim_date';

-- =============================================================
-- FACT TABLE : fact_video_performance
-- PURPOSE : One row per video — all numeric metrics.
--           Foreign keys link to all dimension tables.
--           Partitioned by year and month for fast queries.
-- =============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS fact_video_performance (
    video_id          STRING,
    channel_id        STRING,
    category_id       STRING,
    date_id           STRING,
    views             BIGINT,
    likes             BIGINT,
    comment_count     BIGINT,
    favorite_count    BIGINT,
    tag_count         INT,
    engagement_rate   DOUBLE,
    like_ratio        DOUBLE,
    view_velocity     DOUBLE
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 'data/hive/warehouse/fact_video_performance';
