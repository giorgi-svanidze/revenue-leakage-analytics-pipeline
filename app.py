import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="Revenue Leakage Dashboard", layout="wide")

BASE = os.path.dirname(__file__)
ANALYSIS = os.path.join(BASE, "analysis")

@st.cache_data
def load_data():
    exec_kpis = pd.read_csv(os.path.join(ANALYSIS, "exec_kpis.csv"))
    retailer = pd.read_csv(os.path.join(ANALYSIS, "retailer_performance.csv"))
    warehouse = pd.read_csv(os.path.join(ANALYSIS, "warehouse_otif.csv"))
    reasons = pd.read_csv(os.path.join(ANALYSIS, "reason_code_analysis.csv"))
    anomalies = pd.read_csv(os.path.join(ANALYSIS, "deduction_anomalies.csv"))
    return exec_kpis, retailer, warehouse, reasons, anomalies

st.title("Revenue Leakage Analytics Dashboard")
st.caption("CPG trade spend deductions, OTIF patterns, and anomaly detection")

if not os.path.exists(ANALYSIS):
    st.error("analysis/ folder not found. Run `python3 pipeline.py` first.")
    st.stop()

required_files = [
    "exec_kpis.csv",
    "retailer_performance.csv",
    "warehouse_otif.csv",
    "reason_code_analysis.csv",
    "deduction_anomalies.csv",
]

missing = [f for f in required_files if not os.path.exists(os.path.join(ANALYSIS, f))]
if missing:
    st.error(f"Missing files: {missing}. Run `python3 pipeline.py` first.")
    st.stop()

exec_kpis, retailer, warehouse, reasons, anomalies = load_data()

# ── KPI cards ────────────────────────────────────────────────────────────────

total_revenue = exec_kpis["total_invoiced"].sum()
gross_deductions = exec_kpis["gross_deductions"].sum()
invalid_deductions = exec_kpis["invalid_deductions"].sum()
recovered = exec_kpis["recovered"].sum()
unrecovered = exec_kpis["unrecovered_opportunity"].sum()
invalid_rate = (exec_kpis["invalid_count"].sum() / exec_kpis["total_deductions"].sum()) * 100

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Revenue Analyzed", f"${total_revenue:,.0f}")
c2.metric("Gross Deductions", f"${gross_deductions:,.0f}")
c3.metric("Invalid Deductions", f"${invalid_deductions:,.0f}")
c4.metric("Invalid Rate", f"{invalid_rate:.1f}%")
c5.metric("Unrecovered Opportunity", f"${unrecovered:,.0f}")

st.divider()

# ── Executive trends ─────────────────────────────────────────────────────────

st.subheader("Quarterly Executive KPIs")
quarterly_view = exec_kpis[[
    "fiscal_quarter", "gross_deductions", "invalid_rate_pct",
    "deduction_rate_pct", "recovery_rate_pct", "unrecovered_opportunity"
]].copy()
st.dataframe(quarterly_view, use_container_width=True)

trend_metric = st.selectbox(
    "Select quarterly trend metric",
    ["gross_deductions", "invalid_rate_pct", "recovery_rate_pct", "unrecovered_opportunity"]
)
st.line_chart(exec_kpis.set_index("fiscal_quarter")[trend_metric])

st.divider()

# ── Retailer section ─────────────────────────────────────────────────────────

st.subheader("Retailer Leakage Performance")
retailer_summary = (
    retailer.groupby("retailer_name", as_index=False)
    .agg({
        "gross_deduction_amt": "sum",
        "invalid_deduction_amt": "sum",
        "recovered_amt": "sum",
        "invalid_rate_pct": "mean",
        "recovery_rate_pct": "mean",
        "leakage_score": "mean"
    })
    .sort_values("gross_deduction_amt", ascending=False)
)

st.dataframe(retailer_summary, use_container_width=True)

chart_col1, chart_col2 = st.columns(2)
with chart_col1:
    st.markdown("**Gross Deductions by Retailer**")
    st.bar_chart(retailer_summary.set_index("retailer_name")["gross_deduction_amt"])

with chart_col2:
    st.markdown("**Average Leakage Score by Retailer**")
    st.bar_chart(retailer_summary.set_index("retailer_name")["leakage_score"])

st.divider()

# ── Warehouse section ────────────────────────────────────────────────────────

st.subheader("Warehouse OTIF Risk")
warehouse_summary = (
    warehouse.groupby(["warehouse_name", "region"], as_index=False)
    .agg({
        "total_deduction_amt": "sum",
        "invalid_deduction_amt": "sum",
        "late_pct": "mean",
        "short_pct": "mean",
        "deductions_per_shipment": "mean"
    })
    .sort_values("total_deduction_amt", ascending=False)
)

st.dataframe(warehouse_summary, use_container_width=True)

chart_col3, chart_col4 = st.columns(2)
with chart_col3:
    st.markdown("**Total Deductions by Warehouse**")
    st.bar_chart(warehouse_summary.set_index("warehouse_name")["total_deduction_amt"])

with chart_col4:
    st.markdown("**Average Late % by Warehouse**")
    st.bar_chart(warehouse_summary.set_index("warehouse_name")["late_pct"])

st.divider()

# ── Reason code trends ───────────────────────────────────────────────────────

st.subheader("Reason Code Trends")
reason_pick = st.selectbox(
    "Filter reason code",
    sorted(reasons["reason_code"].dropna().unique())
)
reason_filtered = reasons[reasons["reason_code"] == reason_pick].sort_values("fiscal_quarter")
st.dataframe(reason_filtered, use_container_width=True)
st.line_chart(reason_filtered.set_index("fiscal_quarter")["total_amount"])

st.divider()

# ── Anomaly section ──────────────────────────────────────────────────────────

st.subheader("Detected Anomalies")
only_anomalies = st.toggle("Show anomalies only", value=True)

anom_view = anomalies.copy()
if only_anomalies:
    anom_view = anom_view[anom_view["is_anomaly"] == True]

anom_view = anom_view.sort_values(
    by=["is_anomaly", "amount_zscore", "count_zscore"],
    ascending=[False, False, False]
)

st.dataframe(anom_view, use_container_width=True)

top_spikes = anom_view[anom_view["anomaly_direction"] == "spike"].head(10)
if not top_spikes.empty:
    st.markdown("**Top anomaly spikes**")
    st.dataframe(
        top_spikes[[
            "retailer_name", "reason_code", "fiscal_quarter",
            "deduction_count", "total_deduction_amount",
            "count_zscore", "amount_zscore"
        ]],
        use_container_width=True
    )

st.divider()

# ── Bottom note ──────────────────────────────────────────────────────────────

st.markdown(
    """
    **How to read this dashboard**
    
    - **Retailer Leakage Performance** ranks customers by deduction exposure and recovery weakness.
    - **Warehouse OTIF Risk** highlights where operational execution may be driving penalties.
    - **Detected Anomalies** flags retailer/reason combinations with unusual spikes relative to their own history.
    """
)
