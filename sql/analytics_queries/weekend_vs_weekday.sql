-- =============================================================
-- weekend_vs_weekday_performance.sql
-- PURPOSE : Compare video performance on weekends vs
--           weekdays. Helps creators decide best day
--           to publish Tamil Nadu content.
-- PDF REF : Section 10 - Analytics Queries
-- =============================================================

SELECT
    CASE
        WHEN dd.is_weekend = TRUE
        THEN "Weekend"
        ELSE "Weekday"
    END                                  AS day_type,
    COUNT(f.video_id)                    AS total_videos,
    ROUND(AVG(f.views), 0)               AS avg_views,
    ROUND(AVG(f.likes), 0)               AS avg_likes,
    ROUND(AVG(f.engagement_rate), 4)     AS avg_engagement
FROM
    fact_video_performance f
JOIN
    dim_date dd ON f.date_id = dd.date_id
GROUP BY
    dd.is_weekend
ORDER BY
    avg_views DESC;
