-- =============================================================
-- top_trending_videos.sql
-- PURPOSE : Find top 20 trending videos in Tamil Nadu
--           right now based on views and engagement.
-- PDF REF : Section 10 - Analytics Queries
-- =============================================================

SELECT
    dv.title,
    dv.channel_title,
    dc.category_name,
    f.views,
    f.likes,
    f.comment_count,
    ROUND(f.engagement_rate, 4)          AS engagement_rate,
    ROUND(f.view_velocity, 2)            AS views_per_day,
    CAST(dv.published_at AS STRING)      AS published_at
FROM
    fact_video_performance f
JOIN
    dim_video    dv ON f.video_id    = dv.video_id
JOIN
    dim_category dc ON f.category_id = dc.category_id
WHERE
    f.views IS NOT NULL
ORDER BY
    f.views DESC
LIMIT 20;
