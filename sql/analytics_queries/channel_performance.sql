-- =============================================================
-- channel_performance.sql
-- PURPOSE : Analyze performance of Tamil Nadu channels.
--           Shows subscriber count, total views, avg
--           engagement per channel.
-- PDF REF : Section 10 - Analytics Queries
-- =============================================================

SELECT
    f.channel_id,
    COUNT(DISTINCT f.video_id)           AS total_videos,
    SUM(f.views)                         AS total_views,
    SUM(f.likes)                         AS total_likes,
    ROUND(AVG(f.views), 0)               AS avg_views,
    ROUND(AVG(f.engagement_rate), 4)     AS avg_engagement,
    MAX(f.views)                         AS best_video_views
FROM
    fact_video_performance f
GROUP BY
    f.channel_id
ORDER BY
    total_views DESC;
