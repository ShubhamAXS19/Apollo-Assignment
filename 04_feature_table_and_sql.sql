-- ====== PART 5 & PART 6 TASK 2 ==============================
-- 1. Full DDL for borrower_sms_features table
-- 2. SQL queries for tagging and UPSERTing features
-- Run this in PostgreSQL
-- ============================================================

-- ── 1. The Output Table ( borrower_sms_features ) ───────────
CREATE TABLE IF NOT EXISTS borrower_sms_features (
    borrower_id                     TEXT PRIMARY KEY,

    -- Demographics / Activity
    total_financial_sms_90d         INT DEFAULT 0,
    earliest_sms_date               DATE,
    latest_sms_date                 DATE,

    -- Credit / Income Features (Ability to Pay)
    count_credit_txn_90d            INT DEFAULT 0,
    sum_credit_amount_90d           NUMERIC(15,2) DEFAULT 0.00,
    max_single_credit_90d           NUMERIC(15,2) DEFAULT 0.00,
    count_salary_credits_90d        INT DEFAULT 0,

    -- Debit / Spending Features
    count_debit_txn_90d             INT DEFAULT 0,
    sum_debit_amount_90d            NUMERIC(15,2) DEFAULT 0.00,

    -- Debt & Borrowing Features (Existing obligations)
    count_loan_disbursement_90d     INT DEFAULT 0,
    sum_loan_disbursement_amount_90d NUMERIC(15,2) DEFAULT 0.00,
    count_emi_payment_90d           INT DEFAULT 0,
    sum_emi_amount_90d              NUMERIC(15,2) DEFAULT 0.00,

    -- Risk Triggers (Red flags)
    count_bounce_txn_90d            INT DEFAULT 0,
    count_loan_reminders_90d        INT DEFAULT 0,

    -- Balance (Floor estimation)
    last_known_balance              NUMERIC(15,2),
    min_balance_seen_90d            NUMERIC(15,2),

    -- Derived Risk
    foir_proxy_90d                  NUMERIC(5,4),  -- Fixed Obligation to Income Ratio (EMI sum / Credit sum)
    credit_momentum                 NUMERIC(6,4),  -- M1 Credit / M2 Credit
    risk_score                      INT,
    risk_flag                       VARCHAR(20),   -- LOW, MEDIUM, HIGH

    last_updated                    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_features_last_updated ON borrower_sms_features(last_updated);
CREATE INDEX IF NOT EXISTS idx_features_risk_flag ON borrower_sms_features(risk_flag);


-- ── 2. Part 6 Task 2: "Write SQL logic to tag incoming SMS" ─
-- While our main logic runs in Python (Part 2), here is how
-- it would look purely in SQL (BigQuery dialect representation)

CREATE OR REPLACE VIEW v_sms_tagged AS
SELECT
    id as raw_id,
    borrower_id,
    sender_tag,
    sms_body,
    inbox_time_ms,
    -- Tagging Logic using Regex in SQL
    CASE
        WHEN lower(sms_body) SIMILAR TO '%(salary|sal cr|payroll)%' AND sender_tag = 'BANK' THEN 'SALARY_CREDIT'
        WHEN lower(sms_body) SIMILAR TO '%(emi|installment|ecs|nach)%' THEN 'EMI_PAYMENT'
        WHEN lower(sms_body) SIMILAR TO '%(loan disbursed|loan amount credited)%' THEN 'LOAN_DISBURSEMENT'
        WHEN lower(sms_body) SIMILAR TO '%(failed|declined|insufficient)%' THEN 'FAILED_TRANSACTION'
        WHEN lower(sms_body) SIMILAR TO '%(credited|cr rs)%' THEN 'BANK_CREDIT'
        WHEN lower(sms_body) SIMILAR TO '%(debited|dr rs)%' THEN 'BANK_DEBIT'
        ELSE 'UNCATEGORISED'
    END as category,

    -- Extracting Amount using Regexp_replace
    CAST(
        NULLIF(
            REGEXP_REPLACE(
                substring(lower(sms_body) from '(?:rs\.?|inr|(?i)amount)\s*([\d,]+(?:\.\d{1,2})?)'),
                ',', ''
            ), ''
        ) AS NUMERIC
    ) as extracted_amount
FROM sms_raw
WHERE lower(sms_body) NOT SIMILAR TO '%(otp|one time password)%';


-- ── 3. Upsert Logic for PostgreSQL ─────────────────────────
-- When Airflow runs Task 3 (Features), it runs an INSERT ON CONFLICT query.
-- (Example query, building simplified features directly from classified)

INSERT INTO borrower_sms_features (
    borrower_id, total_financial_sms_90d, count_credit_txn_90d,
    sum_credit_amount_90d, count_emi_payment_90d, sum_emi_amount_90d,
    count_bounce_txn_90d, last_updated
)
SELECT
    borrower_id,
    COUNT(*) as total_financial_sms_90d,
    SUM(CASE WHEN categories::text LIKE '%BANK_CREDIT%' THEN 1 ELSE 0 END),
    SUM(CASE WHEN categories::text LIKE '%BANK_CREDIT%' THEN amount ELSE 0 END),
    SUM(CASE WHEN categories::text LIKE '%EMI_PAYMENT%' THEN 1 ELSE 0 END),
    SUM(CASE WHEN categories::text LIKE '%EMI_PAYMENT%' THEN amount ELSE 0 END),
    SUM(CASE WHEN categories::text LIKE '%FAILED_TRANSACTION%' THEN 1 ELSE 0 END),
    NOW()
FROM sms_classified
WHERE txn_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY borrower_id

ON CONFLICT (borrower_id) DO UPDATE SET
    total_financial_sms_90d = EXCLUDED.total_financial_sms_90d,
    count_credit_txn_90d    = EXCLUDED.count_credit_txn_90d,
    sum_credit_amount_90d   = EXCLUDED.sum_credit_amount_90d,
    count_emi_payment_90d   = EXCLUDED.count_emi_payment_90d,
    sum_emi_amount_90d      = EXCLUDED.sum_emi_amount_90d,
    count_bounce_txn_90d    = EXCLUDED.count_bounce_txn_90d,
    last_updated            = NOW();
