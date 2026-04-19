-- ─────────────────────────────────────────────────────────────────────────────
-- stg_deductions.sql
-- Layer: Staging
-- Purpose: Clean and standardize the raw deductions table.
--
-- WHAT STAGING DOES (and why it matters):
--   Raw data is messy — wrong types, inconsistent casing, nulls in bad places.
--   Staging is where we fix all of that ONCE, so every downstream model
--   works from clean, trusted inputs. Think of it as the "source of truth" layer.
--
-- KEY DECISIONS MADE HERE:
--   1. Cast date strings to proper DATE type so comparisons work correctly
--   2. Standardize reason codes to uppercase (defensive — prevents "otif-late" vs "OTIF-LATE" bugs)
--   3. Add a clean boolean flag is_invalid (opposite of is_valid — more intuitive for analysts)
--   4. Coalesce recovered_amount nulls to 0 (nulls break math; 0 is the correct business meaning)
--   5. Derive deduction_month and deduction_quarter from date (avoids repeating this logic downstream)
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    deduction_id,
    shipment_id,
    retailer_id,
    warehouse_id,
    sku_id,
    carrier_id,

    -- Clean date: cast to DATE type explicitly
    CAST(deduction_date AS DATE)                        AS deduction_date,

    -- Standardize reason code to uppercase, trim whitespace
    UPPER(TRIM(reason_code))                            AS reason_code,
    TRIM(reason_description)                            AS reason_description,

    -- Financials: ensure numeric, coalesce nulls
    ROUND(CAST(deduction_amount AS DOUBLE), 2)          AS deduction_amount,
    ROUND(CAST(invoice_amount   AS DOUBLE), 2)          AS invoice_amount,
    ROUND(CAST(deduction_pct    AS DOUBLE), 4)          AS deduction_pct,
    ROUND(COALESCE(CAST(recovered_amount AS DOUBLE), 0), 2) AS recovered_amount,
    ROUND(COALESCE(CAST(net_loss AS DOUBLE), 0), 2)     AS net_loss,

    -- Validity flags
    CAST(is_valid    AS BOOLEAN)                        AS is_valid,
    NOT CAST(is_valid AS BOOLEAN)                       AS is_invalid,  -- analyst-friendly alias
    CAST(is_disputed AS BOOLEAN)                        AS is_disputed,

    -- Resolution timing
    CAST(resolution_days AS INTEGER)                    AS resolution_days,

    -- Promo linkage (nullable — most deductions aren't promo-related)
    promo_id,

    -- Time dimensions derived from date (compute once here, reuse downstream)
    fiscal_quarter,
    fiscal_month,
    DATE_TRUNC('month', CAST(deduction_date AS DATE))   AS month_start,
    DATE_TRUNC('quarter', CAST(deduction_date AS DATE)) AS quarter_start,

    -- Audit column
    CURRENT_TIMESTAMP                                   AS _loaded_at

FROM read_csv_auto('data/synthetic/fact_deductions.csv')

-- Filter out any rows with null deduction_id or deduction_amount (data quality guard)
WHERE deduction_id IS NOT NULL
  AND deduction_amount IS NOT NULL
  AND deduction_amount > 0
