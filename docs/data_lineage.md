# Data Lineage — Tamil Nadu YouTube Analytics Platform

## Lineage Flow (PDF Section 5)
## Detailed Lineage

### Source: YouTube Data API v3
- Endpoint: videos.list (trending)
- Endpoint: search.list (channels + keywords)
- Endpoint: channels.list (channel stats)
- Endpoint: videoCategories.list
- Region: IN (India)
- Language: ta (Tamil)

### Bronze Layer
| Table | Source | Format | Partition |
|-------|--------|--------|-----------|
| bronze_trending | API trending | Parquet | ingestion_date |
| bronze_trending_by_category | API trending/cat | Parquet | ingestion_date |
| bronze_channel_videos | API search | Parquet | ingestion_date |
| bronze_keyword_videos | API search | Parquet | ingestion_date |
| bronze_video_stats | API videos | Parquet | ingestion_date |
| bronze_channel_stats | API channels | Parquet | ingestion_date |
| bronze_categories | API categories | Parquet | ingestion_date |

### Silver Layer
| Table | Source | Transformations |
|-------|--------|----------------|
| silver_video_stats | bronze_video_stats | flatten, cast, validate, dedup |
| silver_trending | bronze_trending | flatten, cast, validate, dedup |
| silver_keyword_videos | bronze_keyword_videos | flatten, cast, validate, dedup |
| silver_categories | bronze_categories | flatten, cast, validate, dedup |

### Gold Layer
| Table | Source | Metrics Added |
|-------|--------|--------------|
| gold_video_engagement | silver_video_stats + silver_trending + silver_categories | engagement_rate, like_ratio, view_velocity |
| gold_channel_performance | silver_video_stats | aggregated channel metrics |
| gold_category_performance | silver_video_stats + silver_categories | aggregated category metrics |

### Warehouse (Hive External Tables)
| Table | Source | Type |
|-------|--------|------|
| dim_video | gold_video_engagement | Dimension |
| dim_channel | gold_channel_performance | Dimension (SCD Type 2) |
| dim_category | silver_categories | Dimension |
| dim_date | gold_video_engagement | Dimension |
| fact_video_performance | gold_video_engagement | Fact (partitioned year/month) |

## SCD Type 2 — dim_channel
Tracks changes in:
- subscriber_count
- video_count

Columns:
- effective_start_date
- effective_end_date (9999-12-31 = current)
- is_current (True/False)
