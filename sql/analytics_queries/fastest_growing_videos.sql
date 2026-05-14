-- =============================================================
-- fastest_growing_videos.sql
-- PURPOSE : Find videos gaining views fastest (velocity).
--           view_velocity = views per day since published.
--           High velocity = currently TRENDING RIGHT NOW.
-- PDF REF : Section 10 - Analytics Queries
-- =============================================================

SELECT
    dv.title,
    dv.channel_title,
    dc.category_name,
    f.views,
    ROUND(f.view_velocity, 2)            AS views_per_day,
    CAST(dv.published_at AS STRING)      AS published_at,
    ROUND(f.engagement_rate, 4)          AS engagement_rate
FROM
    fact_video_performance f
JOIN
    dim_video    dv ON f.video_id    = dv.video_id
JOIN
    dim_category dc ON f.category_id = dc.category_id
WHERE
    f.view_velocity IS NOT NULL
ORDER BY
    f.view_velocity DESC
LIMIT 20;
