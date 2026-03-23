"""
Part 4: Feature Engineering
===========================
Two approaches are provided here:
1. Pandas-based for the test dataset (local testing)
2. BigQuery SQL query for production batch processing.

Key Features Built (90-day lookback):
- count_credit_txn_90d
- sum_credit_amount_90d
- count_loan_disbursement_90d
- count_emi_payment_90d
- sum_emi_amount_90d
- count_bounce_txn_90d
- earliest_sms_date / latest_sms_date
- last_known_balance
- risk_flag (LOW/MEDIUM/HIGH based on heuristics)
- credit_momentum (MoM growth/shrink)
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta

def compute_features_pandas(df_classified: pd.DataFrame) -> pd.DataFrame:
    """
    Given a dataframe of classified SMS (post-filtering), compute borrower-level features.
    Matches the schema of borrower_sms_features table.
    """
    if df_classified.empty:
        return pd.DataFrame()

    df = df_classified.copy()

    # Convert timestamp for time-based features
    df['txn_date'] = pd.to_datetime(df['inbox_time_ms'], unit='ms').dt.tz_localize('UTC')

    def aggregate_borrower(group):
        # Base counts & sums
        total_sms = len(group)
        # explode categories: an array col to multiple rows for easier counting, or sum booleans
        # For simplicity in Pandas, we iterate string representations if they are JSON
        # Assuming df['categories'] is a list or string of list
        def has_cat(cat, row_cats):
            if isinstance(row_cats, str): return cat in row_cats
            if isinstance(row_cats, list): return cat in row_cats
            return False

        credits = group[group['categories'].apply(lambda c: has_cat('BANK_CREDIT', c) or has_cat('SALARY_CREDIT', c))]
        debits  = group[group['categories'].apply(lambda c: has_cat('BANK_DEBIT', c) or has_cat('UPI_PAYMENT', c))]
        loans   = group[group['categories'].apply(lambda c: has_cat('LOAN_DISBURSEMENT', c))]
        emis    = group[group['categories'].apply(lambda c: has_cat('EMI_PAYMENT', c))]
        bounces = group[group['categories'].apply(lambda c: has_cat('FAILED_TRANSACTION', c))]

        # Balance Proxy
        # Sort by time desc, find first non-null balance
        sorted_group = group.sort_values('inbox_time_ms', ascending=False)
        balances = sorted_group['balance'].dropna()
        last_known_balance = balances.iloc[0] if not balances.empty else None

        # MoM Credit Trend (M1 vs M2) - highly simplified
        now = sorted_group['txn_date'].max()
        if pd.isna(now):
            credit_trend = 1.0
        else:
            m1_start = now - pd.Timedelta(days=30)
            m2_start = now - pd.Timedelta(days=60)
            m1_credits_sum = credits[credits['txn_date'] > m1_start]['amount'].sum()
            m2_credits_sum = credits[(credits['txn_date'] <= m1_start) & (credits['txn_date'] > m2_start)]['amount'].sum()
            credit_trend = (m1_credits_sum / m2_credits_sum) if m2_credits_sum > 0 else 1.0

        res = {
            'total_financial_sms_90d': total_sms,
            'count_credit_txn_90d': len(credits),
            'sum_credit_amount_90d': credits['amount'].sum() or 0,
            'count_debit_txn_90d': len(debits),
            'sum_debit_amount_90d': debits['amount'].sum() or 0,
            'count_loan_disbursement_90d': len(loans),
            'count_emi_payment_90d': len(emis),
            'sum_emi_amount_90d': emis['amount'].sum() or 0,
            'count_bounce_txn_90d': len(bounces),
            'latest_sms_date': sorted_group['txn_date'].max(),
            'last_known_balance': last_known_balance,
            'credit_momentum': credit_trend,
        }

        # Calculate Risk Score (0-100)
        risk_score = 0
        if res['count_bounce_txn_90d'] > 0: risk_score += 30
        if res['count_loan_disbursement_90d'] > 3: risk_score += 20
        if (res['sum_emi_amount_90d'] / (res['sum_credit_amount_90d'] + 1)) > 0.5: risk_score += 40
        if res['credit_momentum'] < 0.8: risk_score += 10

        res['risk_score'] = min(risk_score, 100)

        if res['risk_score'] >= 60: res['risk_flag'] = 'HIGH'
        elif res['risk_score'] >= 30: res['risk_flag'] = 'MEDIUM'
        else: res['risk_flag'] = 'LOW'

        return pd.Series(res)

    # features_df = df.groupby('borrower_id').apply(aggregate_borrower).reset_index()
    features_df = df.groupby('borrower_id').apply(aggregate_borrower, include_groups=False).reset_index()
    features_df['last_updated'] = datetime.utcnow()
    return features_df


# ────────────────────────────────────────────────────────
# PRODUCTION: BIGQUERY SQL IMPLEMENTATION
# This query is executed via Airflow inside BigQuery to
# rebuild the feature table daily. It's much faster than Pandas.
# ────────────────────────────────────────────────────────
BQ_FEATURE_QUERY = """
CREATE OR REPLACE TABLE `my-project.underwriting.borrower_sms_features` AS
WITH base AS (
  SELECT
    borrower_id,
    categories,
    amount,
    balance,
    txn_date,
    DATETIME(TIMESTAMP_MILLIS(inbox_time_ms)) as txn_datetime,
    -- Booleans for easy summing
    CASE WHEN categories LIKE '%"BANK_CREDIT"%' OR categories LIKE '%"SALARY_CREDIT"%' THEN 1 ELSE 0 END as is_credit,
    CASE WHEN categories LIKE '%"BANK_DEBIT"%' OR categories LIKE '%"UPI_PAYMENT"%' THEN 1 ELSE 0 END as is_debit,
    CASE WHEN categories LIKE '%"EMI_PAYMENT"%' THEN 1 ELSE 0 END as is_emi,
    CASE WHEN categories LIKE '%"FAILED_TRANSACTION"%' THEN 1 ELSE 0 END as is_bounce,
    CASE WHEN categories LIKE '%"LOAN_DISBURSEMENT"%' THEN 1 ELSE 0 END as is_loan_disb
  FROM `my-project.underwriting.sms_classified`
  WHERE txn_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
),

balances AS (
  SELECT
    borrower_id,
    balance as last_known_balance
  FROM (
    SELECT borrower_id, balance,
           ROW_NUMBER() OVER(PARTITION BY borrower_id ORDER BY txn_datetime DESC) as rn
    FROM base WHERE balance IS NOT NULL
  )
  WHERE rn = 1
),

aggregations AS (
  SELECT
    b.borrower_id,
    COUNT(*) as total_financial_sms_90d,

    SUM(is_credit) as count_credit_txn_90d,
    SUM(CASE WHEN is_credit=1 THEN amount ELSE 0 END) as sum_credit_amount_90d,

    SUM(is_debit) as count_debit_txn_90d,
    SUM(CASE WHEN is_debit=1 THEN amount ELSE 0 END) as sum_debit_amount_90d,

    SUM(is_loan_disb) as count_loan_disbursement_90d,
    SUM(is_emi) as count_emi_payment_90d,
    SUM(CASE WHEN is_emi=1 THEN amount ELSE 0 END) as sum_emi_amount_90d,

    SUM(is_bounce) as count_bounce_txn_90d,

    MIN(DATE(txn_datetime)) as earliest_sms_date,
    MAX(DATE(txn_datetime)) as latest_sms_date
  FROM base b
  GROUP BY 1
)

SELECT
  a.*,
  b.last_known_balance,
  CURRENT_TIMESTAMP() as last_updated
FROM aggregations a
LEFT JOIN balances b ON a.borrower_id = b.borrower_id;
"""

if __name__ == "__main__":
    from datetime import datetime
    print("Feature Module loaded. Call `compute_features_pandas` or execute `BQ_FEATURE_QUERY`.")
