# =============================================================
# dashboard.py
# PROJECT  : Tamil Nadu YouTube Analytics Data Platform
# PURPOSE  : Streamlit dashboard — visual analytics for
#            Tamil Nadu YouTube trending data.
# PDF REF  : Section 11 — Dashboard
#
# CHARTS:
#   1. Top trending videos by views
#   2. Category performance
#   3. Engagement rate analysis
#   4. Monthly trend
#   5. Fastest growing videos
#   6. Weekend vs Weekday
# =============================================================

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# -------------------------------------------------------
# METHOD 1 : get_spark()
# PURPOSE  : Creates SparkSession for dashboard.
#            Uses cache so Spark starts only once.
# -------------------------------------------------------
@st.cache_resource
def get_spark():
    spark = (
        SparkSession.builder
        .appName("TamilNaduDashboard")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# -------------------------------------------------------
# METHOD 2 : load_data(spark)
# PURPOSE  : Loads all warehouse tables into Spark views
#            and returns them as pandas DataFrames for
#            Streamlit charts.
# RETURNS  : dict of pandas DataFrames
# -------------------------------------------------------
@st.cache_data
def load_data():
    spark     = get_spark()
    warehouse = "data/hive/warehouse"
    gold      = "data/gold"
    data      = {}

    # Load warehouse tables — register with BOTH full name and alias
    wh_tables = [
        ("fact_video_performance", "fact"),
        ("dim_video",              "dim_video"),
        ("dim_category",           "dim_cat"),
        ("dim_date",               "dim_date"),
    ]
    for folder, alias in wh_tables:
        path = os.path.join(warehouse, folder)
        if os.path.exists(path):
            df = spark.read.parquet(path)
            df.createOrReplaceTempView(alias)
            df.createOrReplaceTempView(folder)

    # Load gold tables
    gold_tables = [
        ("gold_video_engagement",    "gold_video"),
        ("gold_category_performance","gold_cat"),
        ("gold_channel_performance", "gold_chan"),
    ]
    for folder, alias in gold_tables:
        path = os.path.join(gold, folder)
        if os.path.exists(path):
            df = spark.read.parquet(path)
            df.createOrReplaceTempView(alias)
            df.createOrReplaceTempView(folder)

    # Query 1: Top 20 trending videos
    data["top_videos"] = spark.sql("""
        SELECT
            dv.title,
            dv.channel_title,
            dc.category_name,
            f.views,
            f.likes,
            f.comment_count,
            ROUND(f.engagement_rate, 4) AS engagement_rate,
            ROUND(f.view_velocity, 2)   AS views_per_day
        FROM fact f
        JOIN dim_video dv ON f.video_id    = dv.video_id
        JOIN dim_cat   dc ON f.category_id = dc.category_id
        WHERE f.views IS NOT NULL
        ORDER BY f.views DESC
        LIMIT 20
    """).toPandas()

    # Query 2: Category performance
    data["categories"] = spark.sql("""
        SELECT
            dc.category_name,
            COUNT(f.video_id)                AS total_videos,
            SUM(f.views)                     AS total_views,
            ROUND(AVG(f.engagement_rate), 4) AS avg_engagement
        FROM fact f
        JOIN dim_cat dc ON f.category_id = dc.category_id
        WHERE f.views IS NOT NULL
        GROUP BY dc.category_name
        ORDER BY total_views DESC
        LIMIT 10
    """).toPandas()

    # Query 3: Monthly trend
    data["monthly"] = spark.sql("""
        SELECT
            dd.year,
            dd.month,
            dd.month_name,
            COUNT(f.video_id)   AS video_count,
            SUM(f.views)        AS total_views,
            ROUND(AVG(f.engagement_rate), 4) AS avg_engagement
        FROM fact f
        JOIN dim_date dd ON f.date_id = dd.date_id
        GROUP BY dd.year, dd.month, dd.month_name
        ORDER BY dd.year, dd.month
    """).toPandas()
    data["monthly"]["period"] = (
        data["monthly"]["month_name"] + " " +
        data["monthly"]["year"].astype(str)
    )

    # Query 4: Fastest growing
    data["velocity"] = spark.sql("""
        SELECT
            dv.title,
            dv.channel_title,
            dc.category_name,
            f.views,
            ROUND(f.view_velocity, 2) AS views_per_day,
            ROUND(f.engagement_rate, 4) AS engagement_rate
        FROM fact f
        JOIN dim_video dv ON f.video_id    = dv.video_id
        JOIN dim_cat   dc ON f.category_id = dc.category_id
        WHERE f.view_velocity IS NOT NULL
        ORDER BY f.view_velocity DESC
        LIMIT 15
    """).toPandas()

    # Query 5: Weekend vs weekday
    data["weekend"] = spark.sql("""
        SELECT
            CASE WHEN dd.is_weekend = TRUE
                 THEN \'Weekend\'
                 ELSE \'Weekday\'
            END AS day_type,
            COUNT(f.video_id)                AS total_videos,
            ROUND(AVG(f.views), 0)           AS avg_views,
            ROUND(AVG(f.engagement_rate), 4) AS avg_engagement
        FROM fact f
        JOIN dim_date dd ON f.date_id = dd.date_id
        GROUP BY dd.is_weekend
    """).toPandas()

    # Summary metrics
    data["summary"] = spark.sql("""
        SELECT
            COUNT(DISTINCT video_id) AS total_videos,
            SUM(views)               AS total_views,
            SUM(likes)               AS total_likes,
            ROUND(AVG(engagement_rate), 4) AS avg_engagement
        FROM fact
        WHERE views IS NOT NULL
    """).toPandas()

    return data


# -------------------------------------------------------
# METHOD 3 : render_header()
# PURPOSE  : Renders dashboard title and summary metrics.
# -------------------------------------------------------
def render_header(data):
    st.title("🎬 Tamil Nadu YouTube Analytics Platform")
    st.markdown(
        "**Real-time trending analysis powered by "
        "PySpark + YouTube Data API**"
    )
    st.divider()

    # Summary KPI cards
    summary = data["summary"].iloc[0]
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Videos",
            f"{int(summary['total_videos']):,}"
        )
    with col2:
        views = int(summary["total_views"])
        st.metric(
            "Total Views",
            f"{views/1_000_000:.1f}M"
        )
    with col3:
        likes = int(summary["total_likes"])
        st.metric(
            "Total Likes",
            f"{likes/1_000_000:.1f}M"
        )
    with col4:
        st.metric(
            "Avg Engagement Rate",
            f"{summary['avg_engagement']}%"
        )

    st.divider()


# -------------------------------------------------------
# METHOD 4 : render_top_videos(data)
# PURPOSE  : Bar chart of top 20 trending videos by views.
# -------------------------------------------------------
def render_top_videos(data):
    st.subheader("🔥 Top 20 Trending Videos in Tamil Nadu")

    df = data["top_videos"].copy()
    df["title_short"] = df["title"].str[:40] + "..."
    df["views_M"]     = (df["views"] / 1_000_000).round(2)

    fig = px.bar(
        df,
        x           = "views_M",
        y           = "title_short",
        orientation = "h",
        color       = "category_name",
        hover_data  = ["channel_title", "engagement_rate"],
        labels      = {
            "views_M"     : "Views (Millions)",
            "title_short" : "Video",
            "category_name": "Category"
        },
        title       = "Top Videos by Total Views",
        height      = 600,
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("View raw data"):
        st.dataframe(
            data["top_videos"][
                ["title", "channel_title",
                 "category_name", "views",
                 "engagement_rate"]
            ],
            use_container_width=True
        )


# -------------------------------------------------------
# METHOD 5 : render_category_performance(data)
# PURPOSE  : Pie + bar chart of category performance.
# -------------------------------------------------------
def render_category_performance(data):
    st.subheader("📊 Category Performance")

    col1, col2 = st.columns(2)
    df = data["categories"]

    with col1:
        fig = px.pie(
            df,
            values = "total_views",
            names  = "category_name",
            title  = "Views Share by Category",
            height = 400,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.bar(
            df,
            x      = "category_name",
            y      = "avg_engagement",
            color  = "avg_engagement",
            title  = "Avg Engagement Rate by Category",
            labels = {
                "avg_engagement" : "Engagement Rate %",
                "category_name"  : "Category"
            },
            height = 400,
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)


# -------------------------------------------------------
# METHOD 6 : render_monthly_trend(data)
# PURPOSE  : Line chart of monthly view trends.
# -------------------------------------------------------
def render_monthly_trend(data):
    st.subheader("📈 Monthly View Trend")

    df  = data["monthly"]
    col1, col2 = st.columns(2)

    with col1:
        fig = px.line(
            df,
            x      = "period",
            y      = "total_views",
            markers= True,
            title  = "Total Views by Month",
            labels = {
                "total_views": "Total Views",
                "period"     : "Month"
            },
            height = 350,
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.bar(
            df,
            x     = "period",
            y     = "video_count",
            title = "Videos Published by Month",
            labels = {
                "video_count": "Videos Published",
                "period"     : "Month"
            },
            height= 350,
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)


# -------------------------------------------------------
# METHOD 7 : render_velocity(data)
# PURPOSE  : Shows fastest growing Tamil Nadu videos.
# -------------------------------------------------------
def render_velocity(data):
    st.subheader("🚀 Fastest Growing Videos Right Now")

    df = data["velocity"].copy()
    df["title_short"]  = df["title"].str[:35] + "..."
    df["views_per_day_K"] = (
        df["views_per_day"] / 1000
    ).round(1)

    fig = px.scatter(
        df,
        x           = "views_per_day_K",
        y           = "engagement_rate",
        size        = "views",
        color       = "category_name",
        hover_name  = "title",
        hover_data  = ["channel_title"],
        labels      = {
            "views_per_day_K" : "Views per Day (K)",
            "engagement_rate" : "Engagement Rate %",
            "category_name"   : "Category"
        },
        title       = "View Velocity vs Engagement "
                      "(bubble size = total views)",
        height      = 450,
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        df[[
            "title", "channel_title",
            "views_per_day", "engagement_rate"
        ]].rename(columns={
            "views_per_day"  : "Views/Day",
            "engagement_rate": "Engagement %"
        }),
        use_container_width=True
    )


# -------------------------------------------------------
# METHOD 8 : render_weekend_analysis(data)
# PURPOSE  : Weekend vs Weekday performance comparison.
# -------------------------------------------------------
def render_weekend_analysis(data):
    st.subheader("📅 Weekend vs Weekday Publishing")

    df   = data["weekend"]
    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            df,
            x      = "day_type",
            y      = "avg_views",
            color  = "day_type",
            title  = "Avg Views: Weekend vs Weekday",
            labels = {
                "avg_views": "Avg Views",
                "day_type" : ""
            },
            height = 300,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.bar(
            df,
            x      = "day_type",
            y      = "avg_engagement",
            color  = "day_type",
            title  = "Avg Engagement: Weekend vs Weekday",
            labels = {
                "avg_engagement": "Avg Engagement %",
                "day_type"      : ""
            },
            height = 300,
        )
        st.plotly_chart(fig, use_container_width=True)


# -------------------------------------------------------
# METHOD 9 : render_sidebar(data)
# PURPOSE  : Sidebar with filters and data info.
# -------------------------------------------------------
def render_sidebar(data):
    st.sidebar.title("⚙️ Filters & Info")
    st.sidebar.markdown("---")

    st.sidebar.markdown("### 📊 Data Info")
    summary = data["summary"].iloc[0]
    st.sidebar.metric(
        "Videos Analyzed",
        f"{int(summary['total_videos']):,}"
    )
    st.sidebar.metric(
        "Data Source",
        "YouTube Data API v3"
    )
    st.sidebar.metric("Region", "India (IN)")
    st.sidebar.metric("Language Focus", "Tamil Nadu")

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔗 Pipeline Layers")
    st.sidebar.markdown("""
    ✅ **Bronze** — Raw JSON  
    ✅ **Silver** — Cleaned  
    ✅ **Gold**   — Analytics  
    ✅ **Hive**   — Warehouse  
    ✅ **SQL**    — Queries  
    ✅ **Dashboard** — You are here!
    """)

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "Built with **PySpark** + **Streamlit**"
    )


# -------------------------------------------------------
# METHOD 10 : main()
# PURPOSE  : Entry point — renders full dashboard.
# -------------------------------------------------------
def main():
    st.set_page_config(
        page_title = "Tamil Nadu YouTube Analytics",
        page_icon  = "🎬",
        layout     = "wide"
    )

    data = load_data()

    render_sidebar(data)
    render_header(data)

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🔥 Trending Videos",
        "📊 Categories",
        "📈 Monthly Trend",
        "🚀 Fastest Growing",
        "📅 Weekend Analysis"
    ])

    with tab1:
        render_top_videos(data)

    with tab2:
        render_category_performance(data)

    with tab3:
        render_monthly_trend(data)

    with tab4:
        render_velocity(data)

    with tab5:
        render_weekend_analysis(data)


if __name__ == "__main__":
    main()
