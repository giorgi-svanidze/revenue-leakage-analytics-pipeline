"""
Synthetic data generator for CPG Trade Spend & Deductions Analytics Pipeline.

All deduction-level and shipment-level data is synthetic, designed to mirror
real trade spend workflows in CPG/FMCG. Distributions, error patterns, and
business logic are based on industry norms (RVCF, GMA trade spend benchmarks).

Run:
    python data/generate_data.py
"""

import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

SEED = 42
np.random.seed(SEED)
random.seed(SEED)

OUT = os.path.join(os.path.dirname(__file__), "synthetic")

# ── Master dimensions ─────────────────────────────────────────────────────────

RETAILERS = [
    {"retailer_id": "R001", "retailer_name": "Walmart",  "channel": "Mass",       "otif_threshold": 0.98, "deduction_aggressiveness": "high"},
    {"retailer_id": "R002", "retailer_name": "Target",   "channel": "Mass",       "otif_threshold": 0.95, "deduction_aggressiveness": "medium"},
    {"retailer_id": "R003", "retailer_name": "Amazon",   "channel": "Ecommerce",  "otif_threshold": 0.99, "deduction_aggressiveness": "very_high"},
    {"retailer_id": "R004", "retailer_name": "Kroger",   "channel": "Grocery",    "otif_threshold": 0.95, "deduction_aggressiveness": "medium"},
    {"retailer_id": "R005", "retailer_name": "Costco",   "channel": "Club",       "otif_threshold": 0.97, "deduction_aggressiveness": "medium"},
    {"retailer_id": "R006", "retailer_name": "Albertsons","channel": "Grocery",   "otif_threshold": 0.93, "deduction_aggressiveness": "low"},
    {"retailer_id": "R007", "retailer_name": "HEB",      "channel": "Grocery",    "otif_threshold": 0.94, "deduction_aggressiveness": "low"},
    {"retailer_id": "R008", "retailer_name": "Publix",   "channel": "Grocery",    "otif_threshold": 0.95, "deduction_aggressiveness": "medium"},
]

WAREHOUSES = [
    {"warehouse_id": "WH01", "warehouse_name": "Dallas DC",   "region": "South",    "capacity_tier": "large"},
    {"warehouse_id": "WH02", "warehouse_name": "Chicago DC",  "region": "Midwest",  "capacity_tier": "large"},
    {"warehouse_id": "WH03", "warehouse_name": "Atlanta DC",  "region": "South",    "capacity_tier": "medium"},
    {"warehouse_id": "WH04", "warehouse_name": "LA DC",       "region": "West",     "capacity_tier": "large"},
    {"warehouse_id": "WH05", "warehouse_name": "Edison DC",   "region": "Northeast","capacity_tier": "medium"},
    {"warehouse_id": "WH06", "warehouse_name": "Memphis DC",  "region": "South",    "capacity_tier": "small"},
]

PRODUCTS = [
    {"sku_id": "SKU001", "sku_name": "Protein Bar 12pk",     "category": "Nutrition Bars", "brand": "NutriBrand", "unit_cost": 18.50, "case_pack": 12},
    {"sku_id": "SKU002", "sku_name": "Greek Yogurt 32oz",    "category": "Dairy",          "brand": "FreshFarm",  "unit_cost": 4.20,  "case_pack": 6},
    {"sku_id": "SKU003", "sku_name": "Granola 24oz",         "category": "Cereal",         "brand": "NutriBrand", "unit_cost": 6.80,  "case_pack": 8},
    {"sku_id": "SKU004", "sku_name": "Almond Butter 16oz",   "category": "Spreads",        "brand": "NutSpread",  "unit_cost": 9.40,  "case_pack": 6},
    {"sku_id": "SKU005", "sku_name": "Plant Protein 2lb",    "category": "Supplements",    "brand": "NutriBrand", "unit_cost": 32.00, "case_pack": 4},
    {"sku_id": "SKU006", "sku_name": "Oat Milk 64oz",        "category": "Alt Milk",       "brand": "OatCo",      "unit_cost": 5.60,  "case_pack": 6},
    {"sku_id": "SKU007", "sku_name": "Energy Bar 6pk",       "category": "Nutrition Bars", "brand": "NutriBrand", "unit_cost": 11.20, "case_pack": 12},
    {"sku_id": "SKU008", "sku_name": "Trail Mix 20oz",       "category": "Snacks",         "brand": "NutSpread",  "unit_cost": 7.80,  "case_pack": 8},
]

CARRIERS = [
    {"carrier_id": "C01", "carrier_name": "Carrier X", "on_time_rate": 0.93, "tier": "preferred"},
    {"carrier_id": "C02", "carrier_name": "Carrier Y", "on_time_rate": 0.88, "tier": "standard"},
    {"carrier_id": "C03", "carrier_name": "Carrier Z", "on_time_rate": 0.91, "tier": "standard"},
    {"carrier_id": "C04", "carrier_name": "Carrier W", "on_time_rate": 0.79, "tier": "spot"},
]

PROMO_TYPES = ["TPR", "Display", "Ad Feature", "BOGO", "None", "None", "None"]  # None weighted higher

REASON_CODES = {
    "OTIF-LATE":    {"description": "Late delivery OTIF penalty",     "typically_valid": 0.55},
    "OTIF-INFULL":  {"description": "Not delivered in full",          "typically_valid": 0.45},
    "ASN-MISSING":  {"description": "ASN not submitted or late",      "typically_valid": 0.60},
    "LABEL-NONCOMP":{"description": "Label non-compliance",          "typically_valid": 0.50},
    "EDI-ERROR":    {"description": "EDI/PO mismatch",               "typically_valid": 0.35},
    "SHORT-SHIP":   {"description": "Shortage claim",                "typically_valid": 0.50},
    "DAMAGE":       {"description": "Damaged goods claim",           "typically_valid": 0.65},
    "PROMO-DISCREP":{"description": "Promotional allowance dispute", "typically_valid": 0.40},
}

# Aggressiveness multiplier — how often retailer deducts vs actual issues
AGG_MULTIPLIER = {"low": 0.8, "medium": 1.0, "high": 1.3, "very_high": 1.6}


def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def generate_promotions(n=120):
    rows = []
    start = datetime(2023, 10, 1)
    for i in range(n):
        retailer = random.choice(RETAILERS)
        sku = random.choice(PRODUCTS)
        promo_start = random_date(start, datetime(2024, 9, 30))
        rows.append({
            "promo_id":       f"PROMO-{i+1:04d}",
            "retailer_id":    retailer["retailer_id"],
            "sku_id":         sku["sku_id"],
            "promo_type":     random.choice(PROMO_TYPES),
            "promo_start_date": promo_start.date(),
            "promo_end_date": (promo_start + timedelta(days=random.randint(7, 28))).date(),
            "agreed_allowance_pct": round(random.uniform(0.05, 0.20), 3),
        })
    return pd.DataFrame(rows)


def generate_shipments(n=2000):
    rows = []
    start = datetime(2023, 10, 1)
    end   = datetime(2024, 9, 30)

    for i in range(n):
        retailer  = random.choice(RETAILERS)
        warehouse = random.choice(WAREHOUSES)
        carrier   = random.choice(CARRIERS)
        sku       = random.choice(PRODUCTS)
        appt_dt   = random_date(start, end)
        cases     = random.randint(50, 2000)
        unit_price= sku["unit_cost"] * random.uniform(1.8, 2.4)  # wholesale markup
        invoice   = round(cases * unit_price, 2)

        # Carrier on-time performance
        on_time = random.random() < carrier["on_time_rate"]
        if on_time:
            arrival_offset = random.randint(-120, 0)   # up to 2 hrs early
        else:
            arrival_offset = random.randint(15, 240)   # 15 min to 4 hrs late

        arrival_dt = appt_dt + timedelta(minutes=arrival_offset)
        dock_delay = max(0, random.gauss(1.5, 1.8))    # retailer-side unloading delay

        # Fill rate — most are 100%, occasional shorts
        fill_pct  = 1.0 if random.random() > 0.08 else random.uniform(0.85, 0.99)
        cases_rcvd= int(cases * fill_pct)

        # ASN compliance — mostly submitted, occasional miss
        asn_submitted = random.random() > 0.07
        asn_hours_before = random.uniform(-1, 48) if asn_submitted else None

        # Label compliance
        label_ok = random.random() > 0.06

        rows.append({
            "shipment_id":      f"SHP-{i+10000:05d}",
            "retailer_id":      retailer["retailer_id"],
            "warehouse_id":     warehouse["warehouse_id"],
            "carrier_id":       carrier["carrier_id"],
            "sku_id":           sku["sku_id"],
            "appointment_dt":   appt_dt,
            "arrival_dt":       arrival_dt,
            "cases_ordered":    cases,
            "cases_shipped":    cases,
            "cases_received":   cases_rcvd,
            "invoice_amount":   invoice,
            "dock_delay_hours": round(dock_delay, 2),
            "asn_submitted":    asn_submitted,
            "asn_hours_before_arrival": round(asn_hours_before, 1) if asn_hours_before else None,
            "label_compliant":  label_ok,
            "fill_rate":        round(fill_pct, 4),
            "delivered_in_full": fill_pct == 1.0,
            "on_time":          on_time,
            "fiscal_quarter":   f"FY{appt_dt.year}Q{(appt_dt.month-1)//3+1}",
            "fiscal_month":     appt_dt.strftime("%Y-%m"),
        })
    return pd.DataFrame(rows)


def generate_deductions(shipments_df, promotions_df):
    rows = []
    deduction_id = 1

    reason_weights = {
        "OTIF-LATE":     0.28,
        "OTIF-INFULL":   0.18,
        "ASN-MISSING":   0.14,
        "LABEL-NONCOMP": 0.10,
        "EDI-ERROR":     0.08,
        "SHORT-SHIP":    0.12,
        "DAMAGE":        0.05,
        "PROMO-DISCREP": 0.05,
    }
    reason_codes = list(reason_weights.keys())
    reason_probs  = list(reason_weights.values())

    retailer_map = {r["retailer_id"]: r for r in RETAILERS}

    for _, shp in shipments_df.iterrows():
        retailer = retailer_map[shp["retailer_id"]]
        agg = AGG_MULTIPLIER[retailer["deduction_aggressiveness"]]

        # Number of deductions per shipment — most have 0-1, some have 2+
        n_deductions = np.random.choice([0, 1, 2, 3], p=[0.45, 0.38, 0.12, 0.05])
        # High-aggression retailers deduct more
        if agg > 1.2 and random.random() < 0.3:
            n_deductions = max(n_deductions, 1)

        for _ in range(n_deductions):
            reason = random.choices(reason_codes, weights=reason_probs)[0]
            rc_info = REASON_CODES[reason]

            # Determine if this deduction is actually valid based on shipment data
            if reason == "OTIF-LATE":
                # Valid if carrier was actually late; invalid if dock delay caused it
                actually_valid = not shp["on_time"] and shp["dock_delay_hours"] < 2
                if shp["on_time"] and shp["dock_delay_hours"] >= 2:
                    actually_valid = False   # retailer's fault
            elif reason == "OTIF-INFULL":
                actually_valid = not shp["delivered_in_full"]
            elif reason == "ASN-MISSING":
                actually_valid = not shp["asn_submitted"]
            elif reason == "LABEL-NONCOMP":
                actually_valid = not shp["label_compliant"]
            elif reason == "SHORT-SHIP":
                actually_valid = shp["fill_rate"] < 1.0
            else:
                # EDI, DAMAGE, PROMO — use base probability + aggressiveness
                base_valid = rc_info["typically_valid"]
                actually_valid = random.random() < (base_valid / agg)

            # Deduction amount — % of invoice, varies by reason
            deduction_pct_map = {
                "OTIF-LATE":      (0.015, 0.04),
                "OTIF-INFULL":    (0.01,  0.03),
                "ASN-MISSING":    (0.005, 0.02),
                "LABEL-NONCOMP":  (0.005, 0.015),
                "EDI-ERROR":      (0.003, 0.01),
                "SHORT-SHIP":     (0.01,  0.05),
                "DAMAGE":         (0.01,  0.04),
                "PROMO-DISCREP":  (0.02,  0.08),
            }
            lo, hi = deduction_pct_map[reason]
            deduction_pct = random.uniform(lo, hi) * agg
            deduction_amt = round(shp["invoice_amount"] * deduction_pct, 2)

            # Resolution — some are disputed and won, some accepted
            disputed      = not actually_valid and random.random() < 0.65
            recovered_amt = round(deduction_amt * random.uniform(0.7, 1.0), 2) if disputed and random.random() < 0.58 else 0.0
            resolution_days = random.randint(14, 90) if disputed else random.randint(5, 20)

            # Link to promo if relevant
            promo_id = None
            if reason == "PROMO-DISCREP":
                matching = promotions_df[
                    (promotions_df["retailer_id"] == shp["retailer_id"]) &
                    (promotions_df["sku_id"] == shp["sku_id"])
                ]
                if not matching.empty:
                    promo_id = matching.sample(1).iloc[0]["promo_id"]

            deduction_dt = shp["appointment_dt"] + timedelta(days=random.randint(7, 45))

            rows.append({
                "deduction_id":      f"DED-{deduction_id:06d}",
                "shipment_id":       shp["shipment_id"],
                "retailer_id":       shp["retailer_id"],
                "warehouse_id":      shp["warehouse_id"],
                "sku_id":            shp["sku_id"],
                "carrier_id":        shp["carrier_id"],
                "deduction_date":    deduction_dt.date(),
                "reason_code":       reason,
                "reason_description": rc_info["description"],
                "deduction_amount":  deduction_amt,
                "invoice_amount":    shp["invoice_amount"],
                "deduction_pct":     round(deduction_pct, 4),
                "is_valid":          actually_valid,
                "is_disputed":       disputed,
                "recovered_amount":  recovered_amt,
                "net_loss":          round(deduction_amt - recovered_amt, 2),
                "resolution_days":   resolution_days if disputed else None,
                "promo_id":          promo_id,
                "fiscal_quarter":    shp["fiscal_quarter"],
                "fiscal_month":      shp["fiscal_month"],
            })
            deduction_id += 1

    return pd.DataFrame(rows)


# ── Generate & save ───────────────────────────────────────────────────────────
print("Generating master dimensions...")
pd.DataFrame(RETAILERS).to_csv(f"{OUT}/dim_retailers.csv", index=False)
pd.DataFrame(WAREHOUSES).to_csv(f"{OUT}/dim_warehouses.csv", index=False)
pd.DataFrame(PRODUCTS).to_csv(f"{OUT}/dim_products.csv", index=False)
pd.DataFrame(CARRIERS).to_csv(f"{OUT}/dim_carriers.csv", index=False)
pd.DataFrame([{"code": k, **v} for k, v in REASON_CODES.items()]).to_csv(f"{OUT}/dim_reason_codes.csv", index=False)

print("Generating promotions...")
promos = generate_promotions(120)
promos.to_csv(f"{OUT}/fact_promotions.csv", index=False)

print("Generating shipments (2,000 rows)...")
shipments = generate_shipments(2000)
shipments.to_csv(f"{OUT}/fact_shipments.csv", index=False)

print("Generating deductions...")
deductions = generate_deductions(shipments, promos)
deductions.to_csv(f"{OUT}/fact_deductions.csv", index=False)

print("\n── Summary ──────────────────────────────")
print(f"Shipments:   {len(shipments):,}")
print(f"Deductions:  {len(deductions):,}")
print(f"Promotions:  {len(promos):,}")
print(f"Total deduction $: ${deductions['deduction_amount'].sum():,.0f}")
print(f"Total recoverable: ${deductions[~deductions['is_valid']]['deduction_amount'].sum():,.0f}")
print(f"Invalid rate:      {(~deductions['is_valid']).mean()*100:.1f}%")
print("\nFiles written to data/synthetic/")
