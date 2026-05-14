-- =============================================================
-- most_engaging_videos.sql
-- PURPOSE : Find videos with highest audience engagement
--           in Tamil Nadu. High engagement = audience
--           actively likes and comments.
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
    ROUND(f.like_ratio, 4)               AS like_ratio_pct
FROM
    fact_video_performance f
JOIN
    dim_video    dv ON f.video_id    = dv.video_id
JOIN
    dim_category dc ON f.category_id = dc.category_id
WHERE
    f.views > 10000
ORDER BY
    f.engagement_rate DESC
LIMIT 20;
