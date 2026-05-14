# Architecture — Tamil Nadu YouTube Analytics Platform

## Overview
End-to-end batch data platform that ingests Tamil Nadu
trending YouTube data daily, processes it through a
medallion data lake, and serves analytics via a
Hive warehouse and Streamlit dashboard.

## Architecture Flow
## Tools Used
| Tool | Purpose |
|------|---------|
| Apache Spark (PySpark) | All data processing |
| YouTube Data API v3 | Data source |
| Apache Hive | SQL warehouse |
| Apache Airflow | Pipeline orchestration |
| Streamlit | Dashboard |
| Plotly | Charts |

## Data Quality (PDF Section 5)
- Completeness: no null video_id, channel_id
- Validity: views >= 0, likes >= 0
- Uniqueness: no duplicate video_id
- Bad records saved to quarantine/
