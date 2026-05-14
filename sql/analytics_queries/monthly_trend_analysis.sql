-- =============================================================
-- monthly_trend_analysis.sql
-- PURPOSE : Shows how video count and total views trend
--           month over month. Useful for identifying
--           seasonal patterns in Tamil Nadu content.
-- PDF REF : Section 10 - Analytics Queries
-- =============================================================

SELECT
    dd.year,
    dd.month,
    dd.month_name,
    dd.quarter,
    COUNT(f.video_id)                    AS videos_published,
    SUM(f.views)                         AS total_views,
    SUM(f.likes)                         AS total_likes,
    ROUND(AVG(f.engagement_rate), 4)     AS avg_engagement
FROM
    fact_video_performance f
JOIN
    dim_date dd ON f.date_id = dd.date_id
GROUP BY
    dd.year, dd.month, dd.month_name, dd.quarter
ORDER BY
    dd.year DESC, dd.month DESC;
