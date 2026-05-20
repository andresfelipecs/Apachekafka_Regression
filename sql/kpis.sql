-- ============================================================
-- Workshop 3 — KPI Queries
-- ============================================================

-- KPI 1: Average prediction error overall
SELECT
    ROUND(AVG(ABS(prediction_error))::NUMERIC, 4) AS avg_abs_error,
    ROUND(AVG(prediction_error)::NUMERIC, 4)       AS avg_signed_error,
    COUNT(*)                                        AS total_predictions
FROM fact_predictions;

-- KPI 2: Predictions by country (avg predicted vs actual)
SELECT
    c.country_name,
    COUNT(*)                                          AS total_predictions,
    ROUND(AVG(fp.actual_score)::NUMERIC, 4)           AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::NUMERIC, 4)        AS avg_predicted_score,
    ROUND(AVG(ABS(fp.prediction_error))::NUMERIC, 4)  AS avg_abs_error
FROM fact_predictions fp
JOIN dim_country c ON fp.country_id = c.country_id
GROUP BY c.country_name
ORDER BY avg_abs_error DESC;

-- KPI 3: Predicted vs actual score per event
SELECT
    rhe.raw_event_id,
    c.country_name,
    d.year,
    fp.actual_score,
    fp.predicted_score,
    fp.prediction_error,
    fp.prediction_timestamp
FROM fact_predictions fp
JOIN dim_country c ON fp.country_id = c.country_id
JOIN dim_date d    ON fp.date_id    = d.date_id
JOIN raw_happiness_events rhe ON fp.raw_event_id = rhe.raw_event_id
ORDER BY fp.prediction_timestamp;

-- KPI 4: Prediction trends over time (by year)
SELECT
    d.year,
    COUNT(*)                                          AS total_predictions,
    ROUND(AVG(fp.actual_score)::NUMERIC, 4)           AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::NUMERIC, 4)        AS avg_predicted_score,
    ROUND(AVG(ABS(fp.prediction_error))::NUMERIC, 4)  AS avg_abs_error
FROM fact_predictions fp
JOIN dim_date d ON fp.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;

-- Bonus: Processing status summary (valid vs invalid events)
SELECT
    processing_status,
    COUNT(*) AS total_events
FROM raw_happiness_events
GROUP BY processing_status
ORDER BY total_events DESC;
