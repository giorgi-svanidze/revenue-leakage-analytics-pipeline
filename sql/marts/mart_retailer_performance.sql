-- mart_retailer_performance.sql

SELECT *
FROM read_csv_auto('data/synthetic/fact_deductions.csv')
LIMIT 10;

WITH
base AS (
    SELECT
        r.retailer_name,
        r.channel                                             AS retailer_channel,
        r.deduction_aggressiveness,
        d.fiscal_quarter,
        COUNT(*)                                              AS total_deductions,
        COUNT(*) FILTER (WHERE NOT d.is_valid)               AS invalid_deductions,
        COUNT(*) FILTER (WHERE d.is_disputed)                AS disputed_deductions,
        SUM(d.deduction_amount)                              AS gross_deduction_amt,
        SUM(d.deduction_amount) FILTER (WHERE NOT d.is_valid) AS invalid_deduction_amt,
        SUM(COALESCE(d.recovered_amount, 0))                 AS recovered_amt,
        SUM(COALESCE(d.net_loss, 0))                         AS net_loss_amt,
        SUM(d.invoice_amount)                                AS total_invoiced,
        AVG(d.resolution_days) FILTER (WHERE d.is_disputed)  AS avg_resolution_days
    FROM read_csv_auto('data/synthetic/fact_deductions.csv') d
    LEFT JOIN read_csv_auto('data/synthetic/dim_retailers.csv') r USING (retailer_id)
    GROUP BY 1, 2, 3, 4
),
with_rates AS (
    SELECT
        *,
        ROUND(invalid_deductions::DOUBLE / NULLIF(total_deductions, 0) * 100, 1)   AS invalid_rate_pct,
        ROUND(gross_deduction_amt / NULLIF(total_invoiced, 0) * 100, 2)            AS deduction_rate_pct,
        ROUND(recovered_amt / NULLIF(invalid_deduction_amt, 0) * 100, 1)           AS recovery_rate_pct
    FROM base
),
with_trends AS (
    SELECT
        *,
        LAG(invalid_rate_pct,    1) OVER (PARTITION BY retailer_name ORDER BY fiscal_quarter) AS prior_qtr_invalid_rate,
        LAG(gross_deduction_amt, 1) OVER (PARTITION BY retailer_name ORDER BY fiscal_quarter) AS prior_qtr_deduction_amt
    FROM with_rates
)
SELECT
    retailer_name,
    retailer_channel,
    deduction_aggressiveness,
    fiscal_quarter,
    total_deductions,
    invalid_deductions,
    disputed_deductions,
    ROUND(gross_deduction_amt, 0)                                                  AS gross_deduction_amt,
    ROUND(invalid_deduction_amt, 0)                                                AS invalid_deduction_amt,
    ROUND(recovered_amt, 0)                                                        AS recovered_amt,
    ROUND(net_loss_amt, 0)                                                         AS net_loss_amt,
    invalid_rate_pct,
    deduction_rate_pct,
    recovery_rate_pct,
    ROUND(avg_resolution_days, 0)                                                  AS avg_resolution_days,
    ROUND(invalid_rate_pct    - prior_qtr_invalid_rate,    1)                      AS invalid_rate_qoq_pts,
    ROUND(gross_deduction_amt - prior_qtr_deduction_amt,   0)                      AS deduction_amt_qoq_change,
    ROUND(
        (COALESCE(deduction_rate_pct, 0) * 0.4)
      + ((100 - COALESCE(recovery_rate_pct, 0)) * 0.3)
      + (COALESCE(invalid_rate_pct, 0) * 0.3)
    , 1)                                                                            AS leakage_score
FROM with_trends
ORDER BY fiscal_quarter, gross_deduction_amt DESC
