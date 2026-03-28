"""
06_advanced_features.py
=======================
Out-of-the-box Data Science features for STPL underwriting.
These go beyond what a data engineer would build — they capture
behavioural, temporal, and statistical signals from SMS data.

Features implemented:
  Tier 1 (high-impact):
    1. salary_regularity_score      — std dev of salary arrival day
    2. balance_trend_slope          — linear regression on balance over time
    3. night_transaction_ratio      — late-night spend as financial stress proxy
    4. weekend_spend_ratio          — weekend vs weekday spending pattern
    5. emi_stress_score             — reminders-per-EMI + recency weighting
    6. credit_utilization_proxy     — avg/max/volatility of spend-to-income ratio

  Tier 2 (solid additions):
    7. loan_stacking_risk           — multiple loans in any 90-day window
    8. income_diversity_score       — HHI across bank/UPI/lender income sources

Usage:
    from 06_advanced_features import compute_advanced_features
    advanced_df = compute_advanced_features(df_classified)
    final_df = base_features_df.merge(advanced_df, on="borrower_id", how="left")
"""

import numpy as np
import pandas as pd
from datetime import datetime
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _has(categories, *cats) -> bool:
    """Check if any of the given categories appear in the categories field."""
    if isinstance(categories, str):
        return any(c in categories for c in cats)
    if isinstance(categories, list):
        return any(c in categories for c in cats)
    return False


def _safe_divide(numerator: float, denominator: float, default=0.0) -> float:
    if denominator == 0 or pd.isna(denominator):
        return default
    return numerator / denominator


# ─────────────────────────────────────────────────────────────────────────────
# TIER 1 — FEATURES THAT WOULD GENUINELY SURPRISE THE INTERVIEWER
# ─────────────────────────────────────────────────────────────────────────────

def salary_regularity_score(group: pd.DataFrame) -> float:
    """
    Measures how consistently the salary arrives on the same day each month.

    Why it matters:
        salary_detected = True tells you income exists.
        regularity_score tells you HOW STABLE that income is.
        - Score 1.0 → salary arrives on exact same date every month (salaried employee)
        - Score 0.5 → salary arrives within ±5 days (still stable)
        - Score 0.0 → salary arrives randomly (gig/freelance income)

    A borrower with gig income and average ₹25,000/month is riskier than
    a salaried borrower at ₹18,000/month — the regularity score captures this.

    Interview line:
        "I didn't just detect salary — I measured salary regularity using
        standard deviation of the day-of-month. A std of 2 days = stable
        employment. A std of 12 days = gig income. The default risk profile
        is completely different even at identical average income."
    """
    salary_sms = group[group["categories"].apply(lambda c: _has(c, "SALARY_CREDIT"))]

    if len(salary_sms) < 2:
        return 0.0

    salary_days = pd.to_datetime(salary_sms["txn_date"]).dt.day.values
    day_std = float(np.std(salary_days))

    # std=0 → score 1.0 (perfect regularity)
    # std=15 → score 0.0 (completely random across a month)
    regularity = max(0.0, 1.0 - (day_std / 15.0))
    return round(regularity, 3)


def balance_trend_slope(group: pd.DataFrame) -> Optional[float]:
    """
    Fits a linear regression on extracted balance values over time.
    Returns the slope in ₹/day.

    Why it matters:
        A snapshot balance misses direction.
        - Slope = +150  → balance growing ₹150/day → financially healthy
        - Slope = -200  → balance shrinking ₹200/day → cashflow stress
        A borrower with ₹10,000 balance and slope -200 is in worse shape
        than one with ₹5,000 balance and slope +100.

    Interview line:
        "I ran a linear regression on balance values extracted from SMS.
        Negative slope — even with an adequate current balance — is a
        leading indicator of cashflow stress. CIBIL is backward-looking;
        this is forward-looking."
    """
    balance_sms = group[group["balance"].notna()].copy()

    if len(balance_sms) < 3:
        return None

    balance_sms["txn_dt"] = pd.to_datetime(balance_sms["txn_date"])
    balance_sms = balance_sms.sort_values("txn_dt")

    # Days since first observation (numeric x-axis)
    x = (balance_sms["txn_dt"] - balance_sms["txn_dt"].min()).dt.days.values
    y = balance_sms["balance"].values

    if len(set(x)) < 2:       # all on same day — can't fit a line
        return None

    slope, _ = np.polyfit(x, y, 1)
    return round(float(slope), 2)


def night_transaction_ratio(group: pd.DataFrame) -> float:
    """
    Fraction of all transactions that occur between 23:00 and 05:00.

    Why it matters:
        Behavioral economics research links late-night financial activity
        to financial anxiety — midnight bill payments to avoid default,
        late-night impulse spending. High ratio (>15%) correlates with
        elevated default rates in alternative credit scoring literature.

    Interview line:
        "Transaction timing is a behavioral signal. Borrowers under
        financial stress make more transactions at unusual hours.
        A night transaction ratio above 15% is a statistically
        meaningful risk signal that pure financial data misses."
    """
    if len(group) == 0:
        return 0.0

    night_mask = (group["txn_hour"] >= 23) | (group["txn_hour"] <= 5)
    return round(float(night_mask.sum()) / len(group), 3)


def weekend_spend_ratio(group: pd.DataFrame) -> float:
    """
    Fraction of total debit amount that occurs on weekends (Sat/Sun).

    Why it matters:
        High weekend spend = lifestyle / discretionary spending dominance.
        Essentials (rent, EMI, groceries) are paid on weekdays.
        Weekend-heavy spenders have less financial discipline and
        lower buffer for loan repayment.
    """
    g = group.copy()
    g["txn_dt"] = pd.to_datetime(g["txn_date"])
    g["is_weekend"] = g["txn_dt"].dt.dayofweek >= 5  # 5=Sat, 6=Sun

    debit_mask = g["categories"].apply(lambda c: _has(c, "BANK_DEBIT", "UPI_PAYMENT"))
    total_debit = g.loc[debit_mask, "amount"].sum()

    if total_debit == 0:
        return 0.0

    weekend_debit = g.loc[debit_mask & g["is_weekend"], "amount"].sum()
    return round(float(_safe_divide(weekend_debit, total_debit)), 3)


def emi_stress_features(group: pd.DataFrame) -> dict:
    """
    Detects whether EMI repayment is becoming harder over time.

    Two borrowers can both have 2 loan reminders:
        Borrower A: both reminders 6 months ago, none recently → resolved
        Borrower B: both reminders in last 30 days              → worsening

    Your existing pipeline treats them identically.
    This feature separates them.

    Returns:
        emi_stress_score          — composite 0–1 (higher = more stress)
        reminders_per_emi         — raw ratio
        emi_reminder_recent_ratio — fraction of reminders that are recent
                                    (>0.5 means getting worse)

    Interview line:
        "I built an EMI stress progression feature. Two borrowers with
        the same reminder count are very different if one's reminders
        are all recent. The recency weighting catches deteriorating
        repayment behaviour before it shows up in bureau data."
    """
    emi_sms = group[group["categories"].apply(lambda c: _has(c, "EMI_PAYMENT"))].copy()
    reminder_sms = group[group["categories"].apply(lambda c: _has(c, "LOAN_REMINDER"))].copy()

    if len(emi_sms) == 0:
        return {
            "emi_stress_score":           0.0,
            "reminders_per_emi":          0.0,
            "emi_reminder_recent_ratio":  0.0,
        }

    reminders_per_emi = _safe_divide(len(reminder_sms), len(emi_sms))

    if len(reminder_sms) >= 2:
        reminder_sms["txn_dt"] = pd.to_datetime(reminder_sms["txn_date"])
        reminder_sms = reminder_sms.sort_values("txn_dt")
        cutoff = reminder_sms["txn_dt"].max() - pd.Timedelta(days=30)
        recent_count = int((reminder_sms["txn_dt"] >= cutoff).sum())
        recent_ratio = round(_safe_divide(recent_count, len(reminder_sms)), 3)
    else:
        recent_ratio = 0.5  # neutral when insufficient data

    # Weighted composite: reminders/EMI matters more, recency modifies it
    stress_score = round((reminders_per_emi * 0.6) + (recent_ratio * 0.4), 3)

    return {
        "emi_stress_score":           min(stress_score, 1.0),
        "reminders_per_emi":          round(reminders_per_emi, 3),
        "emi_reminder_recent_ratio":  recent_ratio,
    }


def credit_utilization_features(group: pd.DataFrame) -> dict:
    """
    Computes monthly spend-to-income ratio — like a credit card utilization ratio.

    Metrics:
        avg_utilization      — average monthly debit / credit (0.9 = spends 90% of income)
        max_utilization      — worst month (>1.0 means spent more than earned)
        utilization_volatility — std dev across months (high = unstable cashflow)

    Why this matters:
        avg_utilization > 0.9 → no financial buffer
        max_utilization > 1.5 → at least one month went deeply negative
        utilization_volatility > 0.3 → income/spending is unpredictable

    Interview line:
        "I computed a credit utilization proxy analogous to credit card
        utilization — monthly debits divided by monthly credits. High
        average utilization with high volatility is the SMS equivalent
        of a maxed-out credit card."
    """
    g = group.copy()
    g["txn_dt"] = pd.to_datetime(g["txn_date"])
    g["yr_mo"] = g["txn_dt"].dt.to_period("M")

    credit_mask = g["categories"].apply(lambda c: _has(c, "BANK_CREDIT", "SALARY_CREDIT"))
    debit_mask  = g["categories"].apply(lambda c: _has(c, "BANK_DEBIT", "UPI_PAYMENT"))

    monthly_credit = g.loc[credit_mask].groupby("yr_mo")["amount"].sum()
    monthly_debit  = g.loc[debit_mask].groupby("yr_mo")["amount"].sum()

    aligned = pd.DataFrame({"credit": monthly_credit, "debit": monthly_debit}).dropna()

    if len(aligned) == 0 or aligned["credit"].sum() == 0:
        return {
            "avg_utilization":           None,
            "max_utilization":           None,
            "utilization_volatility":    None,
        }

    util = (aligned["debit"] / aligned["credit"]).clip(0, 3)  # cap at 3× to avoid outlier blow-up

    return {
        "avg_utilization":        round(float(util.mean()), 3),
        "max_utilization":        round(float(util.max()), 3),
        "utilization_volatility": round(float(util.std()), 3) if len(util) > 1 else 0.0,
    }


# ─────────────────────────────────────────────────────────────────────────────
# TIER 2 — SOLID DATA SCIENCE ADDITIONS
# ─────────────────────────────────────────────────────────────────────────────

def loan_stacking_risk(group: pd.DataFrame) -> dict:
    """
    Detects multiple loan disbursements within any 90-day window.

    Why it matters:
        Loan stacking — taking 3+ loans from different lenders in 90 days —
        is a major red flag that individual lenders cannot see because
        there's no shared bureau for small-ticket loans. But SMS CAN see it.

    Returns:
        stacking_flag       — True if 3+ loans in any 90-day window
        max_loans_in_90d    — the worst window count

    Interview line:
        "I detect loan stacking — a pattern invisible to any individual
        lender but visible in SMS. Three disbursals in 90 days means
        the borrower is overleveraged and cycling debt."
    """
    loan_sms = group[
        group["categories"].apply(lambda c: _has(c, "LOAN_DISBURSEMENT"))
    ].copy()

    loan_sms["txn_dt"] = pd.to_datetime(loan_sms["txn_date"])
    loan_sms = loan_sms.sort_values("txn_dt").reset_index(drop=True)

    if len(loan_sms) < 2:
        return {
            "stacking_flag":     False,
            "max_loans_in_90d":  len(loan_sms),
        }

    max_in_window = 0
    dates = loan_sms["txn_dt"].values

    for i, start in enumerate(dates):
        window_end = start + np.timedelta64(90, "D")
        count = int(((dates >= start) & (dates <= window_end)).sum())
        max_in_window = max(max_in_window, count)

    return {
        "stacking_flag":     bool(max_in_window >= 3),
        "max_loans_in_90d":  int(max_in_window),
    }


def income_diversity_score(group: pd.DataFrame) -> dict:
    """
    Applies the Herfindahl-Hirschman Index (HHI) to income source types.

    HHI = sum of squared market shares.
        HHI = 1.0  → 100% of income from one source (single employer, fragile)
        HHI = 0.33 → perfectly split across 3 sources (diverse, stable)
    We flip it: diversity_score = 1 - HHI, so higher = more diverse.

    Why it matters:
        A borrower with 100% UPI income has informal/gig income.
        A borrower with 70% bank credit + 30% UPI has a salaried job + side income.
        Same total income, very different stability.

    Interview line:
        "I applied the Herfindahl-Hirschman Index — an economic concentration
        measure — to income sources. A data engineer would never think of this.
        A low diversity score means the borrower depends on a single income
        channel, which is a concentration risk analogous to a single-stock portfolio."
    """
    credit_mask = group["categories"].apply(lambda c: _has(c, "BANK_CREDIT", "SALARY_CREDIT"))
    credit_sms = group[credit_mask]

    if len(credit_sms) == 0:
        return {
            "income_diversity_score": 0.0,
            "bank_income_pct":        0.0,
            "upi_income_pct":         0.0,
            "lender_income_pct":      0.0,
        }

    bank_credit   = float(credit_sms[credit_sms["sender_tag"] == "BANK"]["amount"].sum())
    upi_credit    = float(credit_sms[credit_sms["sender_tag"] == "UPI"]["amount"].sum())
    lender_credit = float(credit_sms[credit_sms["sender_tag"] == "LENDER"]["amount"].sum())
    total         = bank_credit + upi_credit + lender_credit

    if total == 0:
        return {
            "income_diversity_score": 0.0,
            "bank_income_pct":        0.0,
            "upi_income_pct":         0.0,
            "lender_income_pct":      0.0,
        }

    shares = [bank_credit / total, upi_credit / total, lender_credit / total]
    hhi = sum(s ** 2 for s in shares)
    diversity = round(1.0 - hhi, 3)

    return {
        "income_diversity_score": diversity,
        "bank_income_pct":        round(bank_credit / total, 3),
        "upi_income_pct":         round(upi_credit / total, 3),
        "lender_income_pct":      round(lender_credit / total, 3),
    }


# ─────────────────────────────────────────────────────────────────────────────
# MASTER AGGREGATION FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def compute_advanced_features(df_classified: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all advanced features for every borrower.

    Input:  df_classified — the sms_classified dataframe
            (same input as compute_features_pandas in 03_feature_engineering.py)

    Output: one row per borrower_id with all advanced feature columns.
            Merge with base features on borrower_id.

    Example:
        base_df     = compute_features_pandas(df_classified)
        advanced_df = compute_advanced_features(df_classified)
        final_df    = base_df.merge(advanced_df, on="borrower_id", how="left")
    """
    if df_classified.empty:
        return pd.DataFrame()

    df = df_classified.copy()
    df["txn_date"] = pd.to_datetime(df["inbox_time_ms"], unit="ms", utc=True)
    df["txn_hour"] = df["txn_date"].dt.hour
    df["txn_date"] = df["txn_date"].dt.date

    records = []

    for borrower_id, group in df.groupby("borrower_id"):
        g = group.reset_index(drop=True)

        # ── Tier 1 ────────────────────────────────────────────────────────────
        sal_reg   = salary_regularity_score(g)
        bal_slope = balance_trend_slope(g)
        night_r   = night_transaction_ratio(g)
        weekend_r = weekend_spend_ratio(g)
        emi_st    = emi_stress_features(g)
        util      = credit_utilization_features(g)

        # ── Tier 2 ────────────────────────────────────────────────────────────
        stacking  = loan_stacking_risk(g)
        diversity = income_diversity_score(g)

        records.append({
            "borrower_id": borrower_id,

            # Salary regularity
            "salary_regularity_score":      sal_reg,

            # Balance trend
            "balance_trend_slope":          bal_slope,   # ₹/day, None if < 3 readings

            # Temporal behaviour
            "night_transaction_ratio":      night_r,
            "weekend_spend_ratio":          weekend_r,

            # EMI stress
            "emi_stress_score":             emi_st["emi_stress_score"],
            "reminders_per_emi":            emi_st["reminders_per_emi"],
            "emi_reminder_recent_ratio":    emi_st["emi_reminder_recent_ratio"],

            # Credit utilization proxy
            "avg_utilization":              util["avg_utilization"],
            "max_utilization":              util["max_utilization"],
            "utilization_volatility":       util["utilization_volatility"],

            # Loan stacking
            "stacking_flag":                stacking["stacking_flag"],
            "max_loans_in_90d":             stacking["max_loans_in_90d"],

            # Income diversity (HHI)
            "income_diversity_score":       diversity["income_diversity_score"],
            "bank_income_pct":              diversity["bank_income_pct"],
            "upi_income_pct":              diversity["upi_income_pct"],
            "lender_income_pct":           diversity["lender_income_pct"],

            "advanced_features_at":         datetime.utcnow(),
        })

    return pd.DataFrame(records)


# ─────────────────────────────────────────────────────────────────────────────
# DDL — add these columns to borrower_sms_features
# ─────────────────────────────────────────────────────────────────────────────

ADVANCED_FEATURE_DDL = """
-- Run this ALTER once to add advanced feature columns to borrower_sms_features.
-- Works in both DuckDB and PostgreSQL.

ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS salary_regularity_score   DECIMAL(5,3);
ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS balance_trend_slope        DECIMAL(10,2);
ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS night_transaction_ratio    DECIMAL(5,3);
ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS weekend_spend_ratio        DECIMAL(5,3);
ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS emi_stress_score           DECIMAL(5,3);
ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS reminders_per_emi          DECIMAL(5,3);
ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS emi_reminder_recent_ratio  DECIMAL(5,3);
ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS avg_utilization            DECIMAL(5,3);
ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS max_utilization            DECIMAL(5,3);
ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS utilization_volatility     DECIMAL(5,3);
ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS stacking_flag              BOOLEAN;
ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS max_loans_in_90d           INTEGER;
ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS income_diversity_score     DECIMAL(5,3);
ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS bank_income_pct            DECIMAL(5,3);
ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS upi_income_pct             DECIMAL(5,3);
ALTER TABLE borrower_sms_features ADD COLUMN IF NOT EXISTS lender_income_pct          DECIMAL(5,3);
"""


# ─────────────────────────────────────────────────────────────────────────────
# QUICK SMOKE TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Minimal synthetic dataset — 5 SMS for one borrower
    sample_data = [
        {"borrower_id": "borrower_001", "sender_tag": "BANK",
         "categories": '["SALARY_CREDIT","BANK_CREDIT"]',
         "amount": 22000, "balance": 18500,
         "inbox_time_ms": 1704067200000},  # 2024-01-01 (salary day: 1st)

        {"borrower_id": "borrower_001", "sender_tag": "BANK",
         "categories": '["EMI_PAYMENT","BANK_DEBIT"]',
         "amount": 3200, "balance": 14500,
         "inbox_time_ms": 1704240000000},  # 2024-01-03

        {"borrower_id": "borrower_001", "sender_tag": "LENDER",
         "categories": '["LOAN_REMINDER"]',
         "amount": None, "balance": None,
         "inbox_time_ms": 1706745600000},  # 2024-02-01 (reminder arrives)

        {"borrower_id": "borrower_001", "sender_tag": "BANK",
         "categories": '["SALARY_CREDIT","BANK_CREDIT"]',
         "amount": 22000, "balance": 16200,
         "inbox_time_ms": 1706832000000},  # 2024-02-02 (salary day: 2nd → +1 day variation)

        {"borrower_id": "borrower_001", "sender_tag": "UPI",
         "categories": '["UPI_PAYMENT","BANK_DEBIT"]',
         "amount": 850, "balance": None,
         "inbox_time_ms": 1707782400000},  # 2024-02-13 02:30 (night transaction)
    ]

    df = pd.DataFrame(sample_data)
    result = compute_advanced_features(df)

    print("=== Advanced Features Smoke Test ===\n")
    for col in result.columns:
        if col not in ("borrower_id", "advanced_features_at"):
            print(f"  {col:<35} {result[col].iloc[0]}")

    print("\n✅ All advanced features computed successfully.")