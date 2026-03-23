"""
Part 6 — Task 1: SMS Count Validation
=======================================
Methodology Explanation:
Validating SMS counts between the device and the ingested data ensures data integrity.

Steps:
1. Device SDK Side:
   - Before uploading, the Android SDK counts the total number of non-OTP, financial SMS parsed from the inbox.
   - It sends this integer (`device_sms_count_expected`) inside the JSON metadata of the POST payload.
2. Ingestion API Side:
   - The API receives the payload, parses the JSON list of SMS objects, and checks `len(messages)`.
   - If `len(messages) != device_sms_count_expected`, the API logs a `PAYLOAD_SIZE_MISMATCH` warning.
3. Pipeline Side (Airflow Task 4):
   - At the end of the daily DAG, we run a query grouped by `borrower_id`:
     `SELECT borrower_id, count(*) FROM sms_classified WHERE txn_date = run_date`
   - We compare this count against a master metadata table where the ingestion API logged the `device_sms_count_expected`.
   - Discrepancies > 5% trigger an Airflow PagerDuty alert indicating a silent drop bug in the classification pipeline or ingestion layer.



Part 6 — Task 5: Why would these features help underwriting an STPL (<15K)?
=============================================================================
Small Ticket Personal Loans (₹5k - ₹15k) are usually for lower-income, new-to-credit (NTC)
individuals who lack strong bureau histories (CIBIL).
Traditional underwriting fails here. SMS data acts as an "Alternate Bureau".

1. Capacity to Pay (Income vs. EMI):
   `sum_credit_amount_90d` proves cash inflow.
   `sum_emi_amount_90d` captures existing undisclosed loans (hidden payday loans).
   FOIR proxy = `sum_emi` / `sum_credit`. High FOIR = guaranteed default.

2. Intent to Pay (Bounces):
   `count_bounce_txn_90d` is the single strongest predictor of default in STPL.
   If they bounce ₹500 transactions often, they will bounce our ₹10k loan EMI.

3. Velocity and Distress (Hunger for credit):
   Spikes in `count_loan_disbursement_90d` (taking 4 different loans in one week) indicates
   extreme financial distress (loan stacking). It's an immediate reject criteria.

4. Minimum Cash Floor:
   `last_known_balance`: If their balance routinely drops to ₹10 right after payday,
   they have no liquidity buffer to service a new loan.



Part 6 — Task 6: End-to-End Walkthrough
=========================================
1. Collection:
   User installs the Lenders App.
   During onboarding, the app requests SMS read permissions.
   The Device SDK reads the inbox. It immediately creates a SHA-256 hash of the user's mobile number.
   It loops through SMS. If sender is `blinkr` (OTP), it skips.
   If sender is `hdfcbk`, it masks the account number `A/c XX4092` and pushes it to a local batch.
   The batch is compressed and POSTed to our API.

2. Ingestion:
   The API dumps this payload into an S3 bucket or Google Cloud Storage (GCS).
   An event triggers or Airflow runs a scheduled task to load this raw data into the `sms_raw` table (or BigQuery).

3. Filtering and Parsing:
   The `01_privacy_and_filtering.py` logic runs over `sms_raw`.
   It normalizes `hdfcbk` to `BANK`.
   It runs keywords and tags it `["BANK_CREDIT"]`.
   It extracts `amount=5000.0` and `balance=1000.0`.
   This writes to `sms_classified`.

4. Feature Aggregation:
   The `03_feature_engineering.py` logic runs over `sms_classified`.
   It sums up all credits for the borrower over 90 days.
   Calculates a `risk_flag` = 'LOW'.
   Upserts this into the `borrower_sms_features` table (SQL shown in Part 5).

5. Decision Point:
   A few seconds later, the Underwriting API queries `borrower_sms_features` by `borrower_id` Hash.
   It sees `risk_flag = LOW` and `sum_credit_amount_90d = 45000`.
   Based on the STPL rule engine, the loan API approves the user for a ₹10,000 credit limit instantly.
"""

if __name__ == "__main__":
    print("Methodology, Underwriting Explanation, and Walkthrough loaded natively in file comments.")
