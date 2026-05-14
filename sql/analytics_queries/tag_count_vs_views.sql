-- =============================================================
-- tag_count_vs_views.sql
-- PURPOSE : Analyze if number of tags affects video views.
--           More tags = better discovery?
-- PDF REF : Section 10 - Analytics Queries
-- =============================================================

SELECT
    CASE
        WHEN dv.tag_count = 0
        THEN "No tags"
        WHEN dv.tag_count BETWEEN 1 AND 5
        THEN "1-5 tags"
        WHEN dv.tag_count BETWEEN 6 AND 15
        THEN "6-15 tags"
        ELSE "15+ tags"
    END                                  AS tag_group,
    COUNT(f.video_id)                    AS video_count,
    ROUND(AVG(f.views), 0)               AS avg_views,
    ROUND(AVG(f.engagement_rate), 4)     AS avg_engagement
FROM
    fact_video_performance f
JOIN
    dim_video dv ON f.video_id = dv.video_id
GROUP BY
    tag_group
ORDER BY
    avg_views DESC;
