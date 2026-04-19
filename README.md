# CPG Trade Spend & Deductions Analytics Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![SQL](https://img.shields.io/badge/SQL-DuckDB-yellow)
![Status](https://img.shields.io/badge/Status-Live-brightgreen)

> A SQL/Python analytics pipeline that transforms raw retailer deduction and shipment data into decision-ready marts for identifying revenue leakage, invalid deductions, and operational root causes across CPG/FMCG supply chains.

---

## The business problem

Retailer OTIF and compliance deductions cost CPG suppliers **$40B+ annually**. Most go unanalyzed because:

- Finance sees the short pay, operations sees the shipment — **no one owns the full picture**
- Disputes are handled case-by-case with no visibility into systemic patterns
- Root causes (dock congestion vs. carrier failure vs. warehouse error) are never separated

This pipeline answers the upstream question: **where is the leakage coming from, and which operational patterns predict invalid deductions?**

It is the analytics layer behind [MarginRecover](https://github.com/giorgi-svanidze/marginrecover) — which handles individual dispute workflows.

---

## Results (sample dataset — FY2024)

| Metric | Value |
|---|---|
| Revenue analyzed | $50.5M |
| Gross deductions | $962K (1.9% of revenue) |
| Invalid deductions | $835K (87.4% of all deductions) |
| Recovered via disputes | $273K |
| **Unrecovered EBITDA opportunity** | **$563K** |

**Top finding:** Amazon and Walmart together account for 37% of total deductions, with invalid rates above 87% — indicating aggressive deduction behavior rather than genuine supplier failure.

**Fastest-growing issue:** SHORT-SHIP deductions growing +4.3 per quarter on average — points to a fulfillment or receiving discrepancy pattern worth investigating at the warehouse level.

---

## Architecture

```
raw data (CSVs)
    │
    ▼
data/generate_data.py        ← synthetic data generator (2,000 shipments, 1,633 deductions)
    │
    ▼
sql/staging/
  stg_deductions.sql         ← clean types, nulls, standardize codes
  stg_shipments.sql          ← clean timestamps, derive on-time minutes, shortage cases
    │
    ▼
sql/intermediate/
  int_deductions_enriched.sql ← join all dimensions, derive business flags
    │
    ▼
sql/marts/
  mart_executive_kpis.sql    ← quarterly KPI summary with QoQ trends
  mart_retailer_performance.sql ← per-retailer leakage scores and invalid rates
  mart_warehouse_otif.sql    ← warehouse-level OTIF failure analysis
    │
    ▼
analysis/                    ← output CSVs + printed executive summary
```

The three-layer model (staging → intermediate → marts) follows the **dbt standard**:
- **Staging**: one model per source, clean only, no joins
- **Intermediate**: join sources, derive business logic
- **Marts**: answer specific business questions for specific audiences

---

## SQL concepts demonstrated

| Concept | Where used | Why it matters |
|---|---|---|
| `GROUP BY` + `COUNT(*) FILTER` | All marts | Conditional aggregation without CASE WHEN |
| `NULLIF(x, 0)` | Rate calculations | Prevent division-by-zero gracefully |
| CTEs (`WITH` clauses) | All models | Readable, modular, debuggable query structure |
| `LAG()` window function | Retailer mart, Exec KPI mart | QoQ comparison without self-joins |
| `PARTITION BY` | Retailer mart | Reset window per retailer, not globally |
| `LEFT JOIN` | Intermediate model | Keep all deductions even if dimension lookup fails |
| `COALESCE` | Throughout | Null-safe arithmetic |
| `DATE_TRUNC` | Staging | Consistent time bucketing |

---

## Data sources

| Table | Rows | Source |
|---|---|---|
| `fact_deductions.csv` | 1,633 | Synthetic — designed to mirror CPG trade spend workflows |
| `fact_shipments.csv` | 2,000 | Synthetic — carrier, OTIF, and compliance patterns |
| `fact_promotions.csv` | 120 | Synthetic — TPR, display, ad feature promotion calendar |
| `dim_retailers.csv` | 8 | Synthetic — Walmart, Target, Amazon, Kroger, Costco, etc. |
| `dim_warehouses.csv` | 6 | Synthetic — regional DCs |
| `dim_products.csv` | 8 | Synthetic — nutrition, dairy, snack SKUs |
| `dim_carriers.csv` | 4 | Synthetic — with realistic on-time rate variance |

All deduction-level and shipment-level data is **synthetic**, designed to reflect real trade spend distributions based on RVCF and GMA industry benchmarks. Retailer penalty rates and OTIF thresholds reflect real published standards.

---

## How to run

```bash
git clone https://github.com/giorgi-svanidze/margin-analytics-pipeline
cd margin-analytics-pipeline
pip install -r requirements.txt

# Generate synthetic data
python data/generate_data.py

# Run full pipeline and print executive summary
python pipeline.py
```

Output CSVs are written to `analysis/`.

---

## Key business questions answered

1. Which retailers have the highest invalid deduction rate — and is it trending worse?
2. Which warehouses correlate most with OTIF-related deductions?
3. Which deduction reason codes are growing fastest quarter over quarter?
4. What is the total EBITDA recovery opportunity if invalid deductions were disputed?
5. Which retailers have the worst leakage score (composite of deduction rate, recovery rate, and invalid rate)?

---

## File structure

```
margin-analytics-pipeline/
├── data/
│   ├── generate_data.py       ← synthetic data generator
│   └── synthetic/             ← generated CSVs (gitignored except dims)
├── sql/
│   ├── staging/
│   │   ├── stg_deductions.sql
│   │   └── stg_shipments.sql
│   ├── intermediate/
│   │   └── int_deductions_enriched.sql
│   └── marts/
│       ├── mart_executive_kpis.sql
│       ├── mart_retailer_performance.sql
│       └── mart_warehouse_otif.sql
├── analysis/                  ← pipeline output CSVs
├── pipeline.py                ← main runner
├── requirements.txt
└── README.md
```

---

## Related project

[**MarginRecover**](https://github.com/giorgi-svanidze/marginrecover) — the operational tool that handles individual deduction disputes, generates AI dispute letters, and models EBITDA recovery at the shipment level.

This pipeline is the analytical layer that sits above it: while MarginRecover catches individual fires, this pipeline identifies where fires keep starting.

---

## Author

Giorgi Svanidze
Chemical Engineering + Supply Chain @ Case Western Reserve University
