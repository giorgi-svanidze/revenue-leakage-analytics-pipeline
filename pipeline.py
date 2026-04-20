"""
pipeline.py — CPG Trade Spend & Deductions Analytics Pipeline
Executes all SQL models using DuckDB and writes results to analysis/ as CSVs.
"""

import duckdb
import pandas as pd
import os

BASE = os.path.dirname(__file__)
OUT = os.path.join(BASE, "analysis")
os.makedirs(OUT, exist_ok=True)

def read_sql(path: str) -> str:
    with open(path, "r") as f:
        return f.read()

con = duckdb.connect()

print("=" * 60)
print("CPG Trade Spend & Deductions Analytics Pipeline")
print("=" * 60)

# ── Run marts ────────────────────────────────────────────────────────────────

print("\n[1/5] Running executive KPIs...")
exec_kpis = con.execute(read_sql(f"{BASE}/sql/marts/mart_executive_kpis.sql")).df()
exec_kpis.to_csv(f"{OUT}/exec_kpis.csv", index=False)

print("[2/5] Running retailer performance mart...")
retailer = con.execute(read_sql(f"{BASE}/sql/marts/mart_retailer_performance.sql")).df()
retailer.to_csv(f"{OUT}/retailer_performance.csv", index=False)

print("[3/5] Running warehouse OTIF mart...")
warehouse = con.execute(read_sql(f"{BASE}/sql/marts/mart_warehouse_otif.sql")).df()
warehouse.to_csv(f"{OUT}/warehouse_otif.csv", index=False)

print("[4/5] Running reason code analysis...")
reason_sql = """
SELECT
    reason_code,
    reason_description,
    fiscal_quarter,
    COUNT(*)                                           AS total,
    COUNT(*) FILTER (WHERE NOT is_valid)               AS invalid_count,
    SUM(deduction_amount)                              AS total_amount,
    SUM(deduction_amount) FILTER (WHERE NOT is_valid)  AS invalid_amount,
    ROUND(COUNT(*) FILTER (WHERE NOT is_valid)::DOUBLE
          / NULLIF(COUNT(*),0) * 100, 1)               AS invalid_rate_pct,
    ROUND(AVG(deduction_amount), 0)                    AS avg_deduction,
    LAG(COUNT(*), 1) OVER (
        PARTITION BY reason_code ORDER BY fiscal_quarter
    )                                                  AS prior_qtr_count,
    COUNT(*) - LAG(COUNT(*),1) OVER (
        PARTITION BY reason_code ORDER BY fiscal_quarter
    )                                                  AS count_qoq_change
FROM read_csv_auto('data/synthetic/fact_deductions.csv')
GROUP BY reason_code, reason_description, fiscal_quarter
ORDER BY fiscal_quarter, total_amount DESC
"""
reasons = con.execute(reason_sql).df()
reasons.to_csv(f"{OUT}/reason_code_analysis.csv", index=False)

print("[5/5] Running anomaly detection mart...")
anomalies = con.execute(read_sql(f"{BASE}/sql/marts/mart_deduction_anomalies.sql")).df()
anomalies.to_csv(f"{OUT}/deduction_anomalies.csv", index=False)

# ── Print summary report ─────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("EXECUTIVE SUMMARY — Full Year")
print("=" * 60)

fy = {
    "total_invoiced":        exec_kpis["total_invoiced"].sum(),
    "gross_deductions":      exec_kpis["gross_deductions"].sum(),
    "invalid_deductions":    exec_kpis["invalid_deductions"].sum(),
    "recovered":             exec_kpis["recovered"].sum(),
    "net_loss":              exec_kpis["net_loss"].sum(),
    "unrecovered_opp":       exec_kpis["unrecovered_opportunity"].sum(),
    "total_deductions":      exec_kpis["total_deductions"].sum(),
    "invalid_count":         exec_kpis["invalid_count"].sum(),
}

print(f"\n  Revenue invoiced:          ${fy['total_invoiced']:>12,.0f}")
print(f"  Gross deductions:          ${fy['gross_deductions']:>12,.0f}  ({fy['gross_deductions']/fy['total_invoiced']*100:.2f}% of revenue)")
print(f"  Invalid deductions:        ${fy['invalid_deductions']:>12,.0f}  ({fy['invalid_count']/fy['total_deductions']*100:.1f}% of all deductions)")
print(f"  Recovered via disputes:    ${fy['recovered']:>12,.0f}")
print(f"  Net loss:                  ${fy['net_loss']:>12,.0f}")
print(f"  Unrecovered opportunity:   ${fy['unrecovered_opp']:>12,.0f}  ← EBITDA upside")

print("\n── By Quarter ──")
q_view = exec_kpis[[
    "fiscal_quarter", "gross_deductions", "invalid_rate_pct",
    "deduction_rate_pct", "recovery_rate_pct", "trend_direction"
]].copy()
q_view.columns = ["Quarter","Gross Deductions","Invalid %","Deduction Rate %","Recovery %","Trend"]
print(q_view.to_string(index=False))

print("\n── Top 3 Problem Retailers (FY) ──")
top_ret = (
    retailer.groupby("retailer_name")
    .agg(gross=("gross_deduction_amt","sum"), invalid_rate=("invalid_rate_pct","mean"))
    .sort_values("gross", ascending=False)
    .head(3)
)
for r, row in top_ret.iterrows():
    print(f"  {r:<15} ${row['gross']:>10,.0f}   invalid rate: {row['invalid_rate']:.1f}%")

print("\n── Top 3 Problem Warehouses (FY) ──")
top_wh = (
    warehouse.groupby("warehouse_name")
    .agg(total_ded=("total_deduction_amt","sum"), late=("late_pct","mean"))
    .sort_values("total_ded", ascending=False)
    .head(3)
)
for w, row in top_wh.iterrows():
    print(f"  {w:<15} ${row['total_ded']:>10,.0f}   late %: {row['late']:.1f}%")

print("\n── Fastest-Growing Deduction Reasons (QoQ) ──")
growing = (
    reasons[reasons["count_qoq_change"].notna()]
    .groupby("reason_code")["count_qoq_change"]
    .mean()
    .sort_values(ascending=False)
    .head(3)
)
for rc, chg in growing.items():
    print(f"  {rc:<20} avg QoQ change: +{chg:.1f} deductions")

print("\n── Top Anomalies ──")
top_anoms = anomalies[anomalies["is_anomaly"] == True].head(5)
if not top_anoms.empty:
    for _, row in top_anoms.iterrows():
        print(
            f"  {row['retailer_name']:<12} | {row['reason_code']:<15} | "
            f"{row['fiscal_quarter']} | amount z-score: {row['amount_zscore']:.2f}"
        )
else:
    print("  No anomalies detected.")

print(f"\n✓ All outputs written to analysis/")
print("  exec_kpis.csv, retailer_performance.csv, warehouse_otif.csv,")
print("  reason_code_analysis.csv, deduction_anomalies.csv")
