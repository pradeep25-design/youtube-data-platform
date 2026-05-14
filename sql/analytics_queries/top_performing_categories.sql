-- =============================================================
-- top_performing_categories.sql
-- PURPOSE : Find which YouTube categories perform best
--           in Tamil Nadu by total views and engagement.
-- PDF REF : Section 10 - Analytics Queries
-- =============================================================

SELECT
    dc.category_name,
    COUNT(f.video_id)                    AS total_videos,
    SUM(f.views)                         AS total_views,
    SUM(f.likes)                         AS total_likes,
    ROUND(AVG(f.views), 0)               AS avg_views_per_video,
    ROUND(AVG(f.engagement_rate), 4)     AS avg_engagement_rate,
    MAX(f.views)                         AS max_views
FROM
    fact_video_performance f
JOIN
    dim_category dc ON f.category_id = dc.category_id
WHERE
    f.views IS NOT NULL
GROUP BY
    dc.category_name
ORDER BY
    total_views DESC;
