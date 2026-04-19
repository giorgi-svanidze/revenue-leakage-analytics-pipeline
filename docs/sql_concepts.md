# SQL Concepts — Interview Study Guide
# Based on patterns used in this pipeline
# Read this. Be able to explain every concept out loud in 60 seconds.

---

## 1. GROUP BY + Aggregations

The most fundamental SQL pattern. Used in every mart.

```sql
-- Count ALL deductions, and separately count only the invalid ones
SELECT
    retailer_name,
    COUNT(*)                                    AS total_deductions,
    COUNT(*) FILTER (WHERE NOT is_valid)        AS invalid_deductions,
    SUM(deduction_amount)                       AS total_amount
FROM fact_deductions
GROUP BY retailer_name
```

WHY FILTER instead of CASE WHEN:
  Both work. FILTER is cleaner:
    COUNT(CASE WHEN NOT is_valid THEN 1 END)    -- old way, verbose
    COUNT(*) FILTER (WHERE NOT is_valid)         -- modern, readable

INTERVIEW QUESTION YOU'LL GET:
  "What's the difference between WHERE and HAVING?"
  Answer: WHERE filters BEFORE aggregation (filters rows).
          HAVING filters AFTER aggregation (filters groups).
  Example: WHERE deduction_amount > 100 → only process big deductions
           HAVING COUNT(*) > 5          → only show retailers with 5+ deductions


---

## 2. NULLIF — Preventing Division by Zero

```sql
-- BAD: crashes if total_deductions is 0
ROUND(invalid_deductions / total_deductions * 100, 1)

-- GOOD: returns NULL instead of crashing
ROUND(invalid_deductions::DOUBLE / NULLIF(total_deductions, 0) * 100, 1)
```

NULLIF(x, 0) means: "if x equals 0, return NULL instead"
NULL divided by anything = NULL (safe, doesn't crash)
COALESCE(result, 0) can then turn that NULL back to 0 if needed

You'll use this on EVERY rate calculation. Burn it into memory.


---

## 3. CTEs — Common Table Expressions

```sql
WITH

-- Step 1: get raw counts
base AS (
    SELECT retailer_id, COUNT(*) AS total
    FROM fact_deductions
    GROUP BY retailer_id
),

-- Step 2: compute rates (needs base to exist first)
with_rates AS (
    SELECT *, total / 100.0 AS rate
    FROM base
)

-- Final: use the last CTE
SELECT * FROM with_rates
```

Think of each CTE as a named temporary table.
They run top to bottom. Each one can reference the ones above it.

WHY NOT ONE BIG QUERY:
  You can't reference an alias you just created in the same SELECT.
  This fails:
    SELECT COUNT(*) AS n, n * 2 AS doubled   -- 'n' doesn't exist yet
  Solution: put COUNT(*) AS n in a CTE, then reference n in the next CTE.

INTERVIEW QUESTION:
  "How would you calculate month-over-month change in deductions?"
  Answer: Use a CTE to get monthly totals, then use LAG() in the next CTE.


---

## 4. Window Functions — LAG()

The most impressive SQL concept to understand. Used in retailer and KPI marts.

```sql
SELECT
    retailer_name,
    fiscal_quarter,
    gross_deduction_amt,

    -- Look back 1 row for the same retailer, sorted by quarter
    LAG(gross_deduction_amt, 1) OVER (
        PARTITION BY retailer_name    -- "for each retailer separately"
        ORDER BY fiscal_quarter       -- "in time order"
    ) AS prior_quarter_amt

FROM retailer_data
```

Result:
  retailer_name | quarter  | gross_amt | prior_amt
  Amazon        | FY2024Q1 | 45000     | NULL       ← no prior quarter
  Amazon        | FY2024Q2 | 52000     | 45000      ← Q1 value appears here
  Amazon        | FY2024Q3 | 48000     | 52000      ← Q2 value appears here
  Walmart       | FY2024Q1 | 38000     | NULL       ← resets for new retailer
  Walmart       | FY2024Q2 | 41000     | 38000

Then subtract: gross_amt - prior_amt = QoQ change. That's it.

PARTITION BY = "reset the window for each group"
ORDER BY     = "sort within that group"
LAG(col, 1)  = "give me the value 1 row back"
LAG(col, 2)  = "give me the value 2 rows back" (same quarter last year if quarterly data)

OTHER WINDOW FUNCTIONS TO KNOW:
  LEAD()        = look forward instead of back
  ROW_NUMBER()  = rank rows within a partition (1, 2, 3...)
  RANK()        = like ROW_NUMBER but ties get same rank
  SUM() OVER()  = running total
  AVG() OVER()  = rolling average

INTERVIEW QUESTION:
  "How would you find the top deduction reason per retailer?"
  Answer: Use ROW_NUMBER() OVER (PARTITION BY retailer ORDER BY amount DESC)
          then filter WHERE rn = 1


---

## 5. JOINs — When to Use Each Type

```sql
-- INNER JOIN: only rows that match in BOTH tables
-- Use when: you only want deductions that have a valid retailer record
SELECT d.*, r.retailer_name
FROM fact_deductions d
INNER JOIN dim_retailers r ON d.retailer_id = r.retailer_id

-- LEFT JOIN: ALL rows from left table, matched rows from right (NULLs if no match)
-- Use when: you want ALL deductions, even if retailer lookup fails
-- This is what we use — we never want to silently drop deductions
SELECT d.*, r.retailer_name   -- retailer_name will be NULL if no match
FROM fact_deductions d
LEFT JOIN dim_retailers r ON d.retailer_id = r.retailer_id
```

RULE OF THUMB FOR THIS PROJECT:
  Always LEFT JOIN dimensions onto facts.
  If a deduction has an unknown retailer_id, you still want to see it —
  it might be a data quality issue worth flagging.

INTERVIEW QUESTION:
  "When would you use LEFT JOIN vs INNER JOIN?"
  Answer: LEFT JOIN when you want to keep all rows from the driving table
          and flag missing matches. INNER JOIN when a match is required
          for the row to make business sense.


---

## 6. COALESCE — Null-Safe Defaults

```sql
-- If recovered_amount is NULL, treat it as 0 for math
SUM(COALESCE(recovered_amount, 0))

-- If retailer_name is NULL (no match), show 'Unknown'
COALESCE(retailer_name, 'Unknown')
```

COALESCE returns the first non-NULL value from its arguments.
Use it any time NULL would break your math or produce misleading results.


---

## 7. DATE_TRUNC — Time Bucketing

```sql
-- Round a date down to the start of its month
DATE_TRUNC('month',   deduction_date)   -- 2024-03-15 → 2024-03-01
DATE_TRUNC('quarter', deduction_date)   -- 2024-03-15 → 2024-01-01
DATE_TRUNC('year',    deduction_date)   -- 2024-03-15 → 2024-01-01
```

Use this for consistent time-series grouping.
Without it, GROUP BY deduction_date gives one row per day — usually not what you want.


---

## How to explain this project in an interview

30-second version:
  "I built a SQL analytics pipeline for CPG supplier trade spend.
   It ingests shipment and deduction data, runs through staging,
   intermediate, and mart layers following the dbt pattern,
   and surfaces KPIs like invalid deduction rate, QoQ trend,
   and warehouse-level OTIF failure rates.
   The key insight from the data: 87% of deductions in the sample
   were invalid, concentrated at Amazon and Walmart, pointing to
   retailer aggressiveness rather than supplier error."

If they ask about a specific query:
  Pick the retailer mart. Walk through:
  1. base CTE: "aggregate deductions per retailer per quarter"
  2. with_rates CTE: "compute rates — I had to use NULLIF to avoid
     division by zero, and a separate CTE because you can't reference
     a new alias in the same SELECT"
  3. with_trends CTE: "add LAG() to get prior quarter for QoQ comparison,
     partitioned by retailer so each retailer resets independently"
  4. Final SELECT: "clean output with composite leakage score"

That answer, delivered clearly, clears most analyst SQL screens.
