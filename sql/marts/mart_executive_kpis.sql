-- ─────────────────────────────────────────────────────────────────────────────
-- mart_executive_kpis.sql
-- Audience: CFO / VP Supply Chain
-- Question: How is our overall trade spend leakage trending, and what's the
--           EBITDA recovery opportunity this quarter?
--
-- SQL CONCEPT — CTEs (Common Table Expressions):
--   Each WITH block is a named subquery. You can think of them like
--   temporary tables that only exist for this query. They make complex
--   queries readable by breaking them into named steps.
--   The alternative is deeply nested subqueries — much harder to read.
-- ─────────────────────────────────────────────────────────────────────────────

WITH

-- Step 1: Aggregate deductions by quarter
quarterly_deductions AS (
    SELECT
        fiscal_quarter,
        COUNT(*)                                              AS total_deductions,
        COUNT(DISTINCT shipment_id)                          AS shipments_with_deductions,
        COUNT(*) FILTER (WHERE NOT is_valid)                 AS invalid_count,
        COUNT(*) FILTER (WHERE is_disputed)                  AS disputed_count,
        SUM(deduction_amount)                                AS gross_deductions,
        SUM(deduction_amount) FILTER (WHERE NOT is_valid)    AS invalid_deductions,
        SUM(recovered_amount)                                AS recovered,
        SUM(net_loss)                                        AS net_loss,
        AVG(resolution_days) FILTER (WHERE is_disputed)      AS avg_resolution_days
    FROM read_csv_auto('data/synthetic/fact_deductions.csv')
    GROUP BY fiscal_quarter
),

-- Step 2: Aggregate invoiced revenue by quarter (from shipments)
quarterly_revenue AS (
    SELECT
        fiscal_quarter,
        COUNT(*)             AS total_shipments,
        SUM(invoice_amount)  AS total_invoiced
    FROM read_csv_auto('data/synthetic/fact_shipments.csv')
    GROUP BY fiscal_quarter
),

-- Step 3: Join and calculate rates
joined AS (
    SELECT
        d.fiscal_quarter,
        r.total_shipments,
        d.shipments_with_deductions,
        d.total_deductions,
        d.invalid_count,
        d.disputed_count,
        ROUND(r.total_invoiced, 0)                           AS total_invoiced,
        ROUND(d.gross_deductions, 0)                         AS gross_deductions,
        ROUND(d.invalid_deductions, 0)                       AS invalid_deductions,
        ROUND(d.recovered, 0)                                AS recovered,
        ROUND(d.net_loss, 0)                                 AS net_loss,

        -- Key rates
        ROUND(d.invalid_count::DOUBLE / NULLIF(d.total_deductions, 0) * 100, 1)  AS invalid_rate_pct,
        ROUND(d.gross_deductions / NULLIF(r.total_invoiced, 0) * 100, 2)         AS deduction_rate_pct,
        ROUND(d.recovered / NULLIF(d.invalid_deductions, 0) * 100, 1)            AS recovery_rate_pct,
        ROUND(d.avg_resolution_days, 0)                      AS avg_resolution_days,

        -- EBITDA opportunity: invalid deductions not yet recovered
        ROUND(d.invalid_deductions - d.recovered, 0)         AS unrecovered_opportunity

    FROM quarterly_deductions d
    LEFT JOIN quarterly_revenue r USING (fiscal_quarter)
),

-- Step 4: Add QoQ trend using window functions
with_trends AS (
    SELECT
        *,
        -- LAG looks back 1 row in the ordered partition
        LAG(gross_deductions,    1) OVER (ORDER BY fiscal_quarter) AS prev_gross_deductions,
        LAG(invalid_rate_pct,    1) OVER (ORDER BY fiscal_quarter) AS prev_invalid_rate,
        LAG(deduction_rate_pct,  1) OVER (ORDER BY fiscal_quarter) AS prev_deduction_rate
    FROM joined
)

SELECT
    fiscal_quarter,
    total_shipments,
    total_deductions,
    invalid_count,
    total_invoiced,
    gross_deductions,
    invalid_deductions,
    recovered,
    net_loss,
    unrecovered_opportunity,
    invalid_rate_pct,
    deduction_rate_pct,
    recovery_rate_pct,
    avg_resolution_days,

    -- QoQ changes
    ROUND(gross_deductions - COALESCE(prev_gross_deductions, gross_deductions), 0) AS deduction_qoq_change,
    ROUND(invalid_rate_pct - COALESCE(prev_invalid_rate, invalid_rate_pct), 1)     AS invalid_rate_qoq_pts,
    ROUND(deduction_rate_pct - COALESCE(prev_deduction_rate, deduction_rate_pct), 2) AS deduction_rate_qoq_pts,

    -- Direction flags (for dashboard coloring)
    CASE WHEN gross_deductions > COALESCE(prev_gross_deductions, gross_deductions) THEN 'worse' ELSE 'better' END AS trend_direction

FROM with_trends
ORDER BY fiscal_quarter
