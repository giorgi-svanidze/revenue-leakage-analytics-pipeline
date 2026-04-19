-- ─────────────────────────────────────────────────────────────────────────────
-- int_deductions_enriched.sql
-- Layer: Intermediate
-- Purpose: Join deductions to all dimensions and derive analysis-ready flags.
--
-- WHY A SEPARATE INTERMEDIATE LAYER?
--   Staging = clean raw data (1 source per model)
--   Intermediate = join multiple sources together, derive business logic
--   Marts = answer specific business questions for specific audiences
--
--   Keeping joins HERE means every mart can use one clean enriched table
--   instead of each mart reimplementing the same joins. DRY principle applied
--   to SQL.
--
-- KEY JOINS:
--   - LEFT JOIN to retailers: we want ALL deductions even if retailer lookup fails
--   - LEFT JOIN to shipments: deduction may exist without a matched shipment (data quality flag)
--   - LEFT JOIN to products: SKU context
--   - LEFT JOIN to warehouses and carriers: operational context
--
-- KEY DERIVED FLAGS:
--   - retailer_caused: deduction is invalid AND root cause is dock/receiving side
--   - supplier_caused: deduction is valid AND root cause is supplier-side
--   - high_value: deduction > $500 (prioritization threshold)
--   - dispute_won: disputed AND fully/partially recovered
-- ─────────────────────────────────────────────────────────────────────────────

WITH

stg_ded AS (
    SELECT * FROM read_csv_auto('data/synthetic/fact_deductions.csv')
),

stg_shp AS (
    SELECT * FROM read_csv_auto('data/synthetic/fact_shipments.csv')
),

dim_ret AS (
    SELECT * FROM read_csv_auto('data/synthetic/dim_retailers.csv')
),

dim_wh AS (
    SELECT * FROM read_csv_auto('data/synthetic/dim_warehouses.csv')
),

dim_prod AS (
    SELECT * FROM read_csv_auto('data/synthetic/dim_products.csv')
),

dim_car AS (
    SELECT * FROM read_csv_auto('data/synthetic/dim_carriers.csv')

),

-- ── Core join ────────────────────────────────────────────────────────────────
enriched AS (
    SELECT
        -- Deduction core
        d.deduction_id,
        d.shipment_id,
        d.deduction_date,
        d.reason_code,
        d.reason_description,
        d.deduction_amount,
        d.invoice_amount,
        d.deduction_pct,
        COALESCE(d.recovered_amount, 0)             AS recovered_amount,
        COALESCE(d.net_loss, 0)                     AS net_loss,
        d.is_valid,
        NOT d.is_valid                              AS is_invalid,
        d.is_disputed,
        d.resolution_days,
        d.promo_id,
        d.fiscal_quarter,
        d.fiscal_month,

        -- Retailer attributes
        r.retailer_name,
        r.channel                                   AS retailer_channel,
        r.otif_threshold,
        r.deduction_aggressiveness,

        -- Warehouse attributes
        w.warehouse_name,
        w.region                                    AS warehouse_region,
        w.capacity_tier                             AS warehouse_tier,

        -- Product attributes
        p.sku_name,
        p.category                                  AS product_category,
        p.brand,

        -- Carrier attributes
        c.carrier_name,
        c.on_time_rate                              AS carrier_on_time_rate,
        c.tier                                      AS carrier_tier,

        -- Shipment context (nullable — not every deduction has a matched shipment)
        s.on_time                                   AS shipment_on_time,
        s.fill_rate,
        s.dock_delay_hours,
        s.asn_submitted,
        s.label_compliant,
        s.minutes_late,

        -- ── Derived business flags ────────────────────────────────────────────

        -- Was this a retailer-caused invalid deduction?
        CASE
            WHEN NOT d.is_valid
             AND d.reason_code IN ('OTIF-LATE', 'OTIF-INFULL')
             AND COALESCE(s.dock_delay_hours, 0) >= 2  THEN TRUE
            WHEN NOT d.is_valid
             AND d.reason_code IN ('EDI-ERROR', 'PROMO-DISCREP') THEN TRUE
            ELSE FALSE
        END                                         AS retailer_caused,

        -- Was this clearly the supplier's fault?
        CASE
            WHEN d.is_valid
             AND d.reason_code IN ('ASN-MISSING', 'LABEL-NONCOMP', 'SHORT-SHIP') THEN TRUE
            ELSE FALSE
        END                                         AS supplier_caused,

        -- Dispute outcome
        CASE
            WHEN d.is_disputed AND COALESCE(d.recovered_amount, 0) > 0 THEN 'won'
            WHEN d.is_disputed AND COALESCE(d.recovered_amount, 0) = 0 THEN 'lost'
            WHEN NOT d.is_disputed AND NOT d.is_valid                   THEN 'not_disputed'
            ELSE 'accepted'
        END                                         AS dispute_outcome,

        -- Priority tier for the dispute queue
        CASE
            WHEN NOT d.is_valid AND d.deduction_amount >= 1000 THEN 'P1_high'
            WHEN NOT d.is_valid AND d.deduction_amount >= 400  THEN 'P2_medium'
            WHEN NOT d.is_valid                                THEN 'P3_low'
            ELSE NULL
        END                                         AS dispute_priority,

        -- Broad root cause bucket (for aggregation)
        CASE
            WHEN d.reason_code IN ('OTIF-LATE', 'OTIF-INFULL') THEN 'OTIF'
            WHEN d.reason_code IN ('ASN-MISSING', 'EDI-ERROR')  THEN 'Compliance'
            WHEN d.reason_code IN ('LABEL-NONCOMP')             THEN 'Compliance'
            WHEN d.reason_code IN ('SHORT-SHIP', 'DAMAGE')      THEN 'Fulfillment'
            WHEN d.reason_code IN ('PROMO-DISCREP')             THEN 'Promotional'
            ELSE 'Other'
        END                                         AS root_cause_bucket

    FROM stg_ded d
    LEFT JOIN dim_ret r  ON d.retailer_id  = r.retailer_id
    LEFT JOIN dim_wh  w  ON d.warehouse_id = w.warehouse_id
    LEFT JOIN dim_prod p ON d.sku_id       = p.sku_id
    LEFT JOIN dim_car c  ON d.carrier_id   = c.carrier_id
    LEFT JOIN stg_shp s  ON d.shipment_id  = s.shipment_id
)

SELECT * FROM enriched
