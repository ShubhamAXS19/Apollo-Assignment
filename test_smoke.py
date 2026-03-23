# test_smoke.py — run this first to verify everything works
import sys, json
sys.path.insert(0, '.')

# ── Test 1: Privacy & filtering ──────────────────────────
from importlib.util import spec_from_file_location, module_from_spec

spec = spec_from_file_location("p1", "01_privacy_and_filtering.py")
p1 = module_from_spec(spec); spec.loader.exec_module(p1)

sample_rows = [
    # Should PASS — bank credit
    {"customer_id": "abc123", "sms_sender": "hdfcbk",
     "sms_body": "Rs.5000 credited to A/c XX1234. Avl Bal: Rs.12000",
     "inbox_time_in_millis": 1700000000000},
    # Should be DROPPED — OTP
    {"customer_id": "abc123", "sms_sender": "blinkr",
     "sms_body": "Your OTP is 123456. Valid for 10 mins.",
     "inbox_time_in_millis": 1700000001000},
    # Should PASS — EMI debit
    {"customer_id": "def456", "sms_sender": "kotakb",
     "sms_body": "EMI of Rs.3200 debited via NACH from A/c XX5678",
     "inbox_time_in_millis": 1700000002000},
    # Should PASS — salary
    {"customer_id": "def456", "sms_sender": "hdfcbk",
     "sms_body": "Salary of Rs.22000 credited to your account",
     "inbox_time_in_millis": 1700000003000},
    # Should PASS — UPI + alcohol flag
    {"customer_id": "ghi789", "sms_sender": "jiopay",
     "sms_body": "Rs.850 paid to liquor store via UPI. Ref: 123456789",
     "inbox_time_in_millis": 1700000004000},
]

print("=" * 60)
print("TEST 1: Privacy & Filtering")
print("=" * 60)
for row in sample_rows:
    result = p1.process_sms_row(row)
    status = "✅ KEPT" if result else "❌ DROPPED"
    print(f"{status} | sender={row['sms_sender']} | {row['sms_body'][:50]}...")
    if result:
        print(f"         categories={result['categories']} | amount={result['amount']}")

# ── Test 2: Feature engineering ──────────────────────────
import pandas as pd, numpy as np, json, random
from datetime import date, timedelta

spec2 = spec_from_file_location("p3", "03_feature_engineering.py")
p3 = module_from_spec(spec2); spec2.loader.exec_module(p3)

random.seed(42)
base = date.today() - timedelta(days=45)
mock = []
for i in range(300):
    d = base + timedelta(days=random.randint(0, 45))
    mock.append({
        "borrower_id":   f"borrower_{i % 5:03d}",
        "sender_tag":    random.choice(["BANK","UPI","LENDER"]),
        "categories":    json.dumps(random.sample(
            ["BANK_CREDIT","BANK_DEBIT","UPI_PAYMENT","EMI_PAYMENT","SALARY_CREDIT"], 2)),
        "amount":        round(random.uniform(500, 30000), 2),
        "balance":       round(random.uniform(1000, 80000), 2),
        "txn_date":      d,
        "txn_month":     d.month,
        "txn_year":      d.year,
        "txn_hour":      random.randint(0, 23),
        "inbox_time_ms": int(pd.Timestamp(d).timestamp() * 1000),  # ← add this
    })

df = pd.DataFrame(mock)
features = p3.compute_features_pandas(df)
print("Available columns:", features.columns.tolist())
print("\n" + "=" * 60)
print("TEST 2: Feature Engineering Output")
print("=" * 60)
print(features[[
    "borrower_id", "sum_credit_amount_90d", "sum_debit_amount_90d",
    "count_emi_payment_90d", "last_known_balance", "risk_flag"
]].to_string(index=False))
print(f"\n✅ Features computed for {len(features)} borrowers")