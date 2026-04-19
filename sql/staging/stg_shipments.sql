-- ─────────────────────────────────────────────────────────────────────────────
-- stg_shipments.sql
-- Layer: Staging
-- Purpose: Clean and standardize raw shipments table.
--
-- KEY DECISIONS:
--   1. Cast timestamps properly — appointment_dt and arrival_dt are datetime, not date
--   2. Derive on_time_minutes: negative = early, positive = late. More useful than a boolean.
--   3. fill_rate as a decimal (0.0–1.0) is kept; we also add fill_rate_pct for display
--   4. Null-safe the asn_hours_before_arrival — if ASN not submitted, this is null (correct)
--   5. Add shortage_cases: the gap between ordered and received, 0 if fully delivered
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    shipment_id,
    retailer_id,
    warehouse_id,
    carrier_id,
    sku_id,

    -- Timestamps
    CAST(appointment_dt AS TIMESTAMP)                    AS appointment_dt,
    CAST(arrival_dt     AS TIMESTAMP)                    AS arrival_dt,

    -- On-time analysis: minutes late (negative = arrived early)
    ROUND(
        DATE_DIFF('minute',
            CAST(appointment_dt AS TIMESTAMP),
            CAST(arrival_dt     AS TIMESTAMP)
        ), 0
    )                                                    AS minutes_late,

    CAST(on_time AS BOOLEAN)                             AS on_time,

    -- Quantities
    CAST(cases_ordered   AS INTEGER)                     AS cases_ordered,
    CAST(cases_shipped   AS INTEGER)                     AS cases_shipped,
    CAST(cases_received  AS INTEGER)                     AS cases_received,

    -- Shortage: how many cases went missing
    CAST(cases_ordered AS INTEGER) - CAST(cases_received AS INTEGER) AS shortage_cases,

    -- Financials
    ROUND(CAST(invoice_amount AS DOUBLE), 2)             AS invoice_amount,

    -- Fill rate
    ROUND(CAST(fill_rate AS DOUBLE), 4)                  AS fill_rate,
    ROUND(CAST(fill_rate AS DOUBLE) * 100, 2)            AS fill_rate_pct,
    CAST(delivered_in_full AS BOOLEAN)                   AS delivered_in_full,

    -- Compliance flags
    CAST(asn_submitted    AS BOOLEAN)                    AS asn_submitted,
    CAST(label_compliant  AS BOOLEAN)                    AS label_compliant,

    CAST(asn_hours_before_arrival AS DOUBLE)             AS asn_hours_before_arrival,

    -- Dock delay at retailer
    ROUND(CAST(dock_delay_hours AS DOUBLE), 2)           AS dock_delay_hours,

    -- Time dimensions
    fiscal_quarter,
    fiscal_month,
    DATE_TRUNC('month',   CAST(appointment_dt AS TIMESTAMP)) AS ship_month_start,
    DATE_TRUNC('quarter', CAST(appointment_dt AS TIMESTAMP)) AS ship_quarter_start,

    CURRENT_TIMESTAMP AS _loaded_at

FROM read_csv_auto('data/synthetic/fact_shipments.csv')

WHERE shipment_id IS NOT NULL
