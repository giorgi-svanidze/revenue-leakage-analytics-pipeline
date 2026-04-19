-- ─────────────────────────────────────────────────────────────────────────────
-- mart_warehouse_otif.sql
-- Question: Which warehouses are systematically driving OTIF deductions?
-- ─────────────────────────────────────────────────────────────────────────────

WITH shipment_otif AS (
    SELECT
        s.warehouse_id,
        w.warehouse_name,
        w.region,
        w.capacity_tier,
        s.fiscal_quarter,
        COUNT(*)                                         AS total_shipments,
        COUNT(*) FILTER (WHERE NOT s.on_time)            AS late_shipments,
        COUNT(*) FILTER (WHERE NOT s.delivered_in_full)  AS short_shipments,
        COUNT(*) FILTER (WHERE NOT s.asn_submitted)      AS missing_asn,
        COUNT(*) FILTER (WHERE NOT s.label_compliant)    AS label_violations,
        AVG(s.fill_rate)                                 AS avg_fill_rate,
        AVG(s.dock_delay_hours)                          AS avg_dock_delay_hrs
    FROM read_csv_auto('data/synthetic/fact_shipments.csv') s
    LEFT JOIN read_csv_auto('data/synthetic/dim_warehouses.csv') w USING (warehouse_id)
    GROUP BY 1, 2, 3, 4, 5
),

deduction_by_wh AS (
    SELECT
        warehouse_id,
        fiscal_quarter,
        COUNT(*)                                          AS total_deductions,
        SUM(deduction_amount)                            AS total_deduction_amt,
        SUM(deduction_amount) FILTER (WHERE NOT is_valid) AS invalid_deduction_amt,
        AVG(deduction_amount)                            AS avg_deduction_amt
    FROM read_csv_auto('data/synthetic/fact_deductions.csv')
    GROUP BY 1, 2
)

SELECT
    o.warehouse_name,
    o.region,
    o.capacity_tier,
    o.fiscal_quarter,
    o.total_shipments,
    o.late_shipments,
    ROUND(o.late_shipments::DOUBLE / NULLIF(o.total_shipments, 0) * 100, 1)    AS late_pct,
    o.short_shipments,
    ROUND(o.short_shipments::DOUBLE / NULLIF(o.total_shipments, 0) * 100, 1)   AS short_pct,
    o.missing_asn,
    o.label_violations,
    ROUND(o.avg_fill_rate * 100, 2)                                             AS avg_fill_rate_pct,
    ROUND(o.avg_dock_delay_hrs, 2)                                              AS avg_dock_delay_hrs,
    COALESCE(d.total_deductions, 0)                                             AS total_deductions,
    ROUND(COALESCE(d.total_deduction_amt, 0), 0)                               AS total_deduction_amt,
    ROUND(COALESCE(d.invalid_deduction_amt, 0), 0)                             AS invalid_deduction_amt,
    ROUND(COALESCE(d.avg_deduction_amt, 0), 0)                                 AS avg_deduction_amt,
    -- Deductions per shipment — key efficiency metric
    ROUND(COALESCE(d.total_deductions, 0)::DOUBLE / NULLIF(o.total_shipments, 0), 3) AS deductions_per_shipment

FROM shipment_otif o
LEFT JOIN deduction_by_wh d
    ON o.warehouse_id = d.warehouse_id
   AND o.fiscal_quarter = d.fiscal_quarter

ORDER BY o.fiscal_quarter, total_deduction_amt DESC
