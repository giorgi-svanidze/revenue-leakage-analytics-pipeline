-- mart_deduction_anomalies.sql
-- Question: Which retailer / reason-code combinations show abnormal spikes
-- in deduction counts or deduction amounts relative to their own history?

WITH base AS (
    SELECT
        r.retailer_name,
        d.reason_code,
        d.reason_description,
        d.fiscal_quarter,
        COUNT(*) AS deduction_count,
        SUM(d.deduction_amount) AS total_deduction_amount,
        COUNT(*) FILTER (WHERE NOT d.is_valid) AS invalid_count,
        SUM(d.deduction_amount) FILTER (WHERE NOT d.is_valid) AS invalid_amount
    FROM read_csv_auto('data/synthetic/fact_deductions.csv') d
    LEFT JOIN read_csv_auto('data/synthetic/dim_retailers.csv') r USING (retailer_id)
    GROUP BY 1, 2, 3, 4
),

with_stats AS (
    SELECT
        *,
        AVG(deduction_count) OVER (
            PARTITION BY retailer_name, reason_code
        ) AS avg_count_hist,

        STDDEV_SAMP(deduction_count) OVER (
            PARTITION BY retailer_name, reason_code
        ) AS std_count_hist,

        AVG(total_deduction_amount) OVER (
            PARTITION BY retailer_name, reason_code
        ) AS avg_amount_hist,

        STDDEV_SAMP(total_deduction_amount) OVER (
            PARTITION BY retailer_name, reason_code
        ) AS std_amount_hist
    FROM base
),

scored AS (
    SELECT
        *,
        ROUND(
            (deduction_count - avg_count_hist)
            / NULLIF(std_count_hist, 0),
            2
        ) AS count_zscore,

        ROUND(
            (total_deduction_amount - avg_amount_hist)
            / NULLIF(std_amount_hist, 0),
            2
        ) AS amount_zscore,

        ROUND(invalid_count::DOUBLE / NULLIF(deduction_count, 0) * 100, 1) AS invalid_rate_pct
    FROM with_stats
)

SELECT
    retailer_name,
    reason_code,
    reason_description,
    fiscal_quarter,
    deduction_count,
    ROUND(total_deduction_amount, 0) AS total_deduction_amount,
    invalid_count,
    ROUND(invalid_amount, 0) AS invalid_amount,
    invalid_rate_pct,
    ROUND(avg_count_hist, 2) AS avg_count_hist,
    ROUND(avg_amount_hist, 0) AS avg_amount_hist,
    COALESCE(count_zscore, 0) AS count_zscore,
    COALESCE(amount_zscore, 0) AS amount_zscore,

    CASE
        WHEN ABS(COALESCE(count_zscore, 0)) >= 2
          OR ABS(COALESCE(amount_zscore, 0)) >= 2
        THEN TRUE
        ELSE FALSE
    END AS is_anomaly,

    CASE
        WHEN COALESCE(count_zscore, 0) >= 2
          OR COALESCE(amount_zscore, 0) >= 2
        THEN 'spike'
        WHEN COALESCE(count_zscore, 0) <= -2
          OR COALESCE(amount_zscore, 0) <= -2
        THEN 'drop'
        ELSE 'normal'
    END AS anomaly_direction

FROM scored
ORDER BY is_anomaly DESC, ABS(COALESCE(amount_zscore, 0)) DESC, ABS(COALESCE(count_zscore, 0)) DESC;