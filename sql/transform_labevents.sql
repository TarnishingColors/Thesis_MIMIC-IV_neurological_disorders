SELECT DISTINCT ON (l.subject_id, l.hadm_id, dl.category, dl.label)
    l.subject_id,
    l.hadm_id,
    dl.category,
    dl.label,
    MIN(l.valuenum) OVER w AS min_value,
    MAX(l.valuenum) OVER w AS max_value,
    AVG(l.valuenum) OVER w AS avg_value,
    l.ref_range_lower,
    l.ref_range_upper,
    l.valueuom,
    CASE
        WHEN SUM(CASE WHEN l.flag IS NOT NULL THEN 1 ELSE 0 END) OVER w > 0
        THEN TRUE
        ELSE FALSE
    END AS was_abnormal,
    ROUND(CAST(SUM(CASE WHEN l.flag IS NOT NULL THEN 1 ELSE 0 END) OVER w AS DECIMAL) /
        COUNT(*) OVER w, 2) AS ratio_abnormal
FROM raw.labevents l
JOIN mart.d_labitems dl
    ON l.itemid = dl.itemid
WINDOW w AS (PARTITION BY l.subject_id, l.hadm_id, dl.category, dl.label);