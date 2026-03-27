-- ============================================================
-- PART 5 & PART 6 TASK 2  — DuckDB Edition (FIXED & COMPLETE)
-- borrower_sms_features: Full DDL + tagging VIEWs + upsert
--
-- GAP FIXES in this version:
--   1. upi_txn_count + upi_total_amount_90d  — added to DDL & upsert
--   2. lender_count                          — added to DDL & upsert
--   3. salary_detected                       — BOOLEAN (was INT)
--   4. alcohol_txn_count + alcohol_spend     — added to DDL & upsert
--   5. avg_balance_proxy_90d                 — added to DDL
--   6. v_sms_tagged                          — multi-label (UNION ALL, not CASE WHEN)
--
-- DuckDB-specific notes:
--   • No SIMILAR TO  → use LIKE '%...%' chains with OR
--   • Upsert         → INSERT OR REPLACE INTO  (DuckDB v0.8+)
--   • BOOLEAN        → native type, TRUE/FALSE literals
--   • REGEXP_EXTRACT → built-in, used for amount extraction
--   • No CREATE INDEX IF NOT EXISTS ON PK → DuckDB auto-indexes PKs
-- ============================================================


-- ── 1. Main feature table ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS borrower_sms_features (
    borrower_id                  VARCHAR PRIMARY KEY,         -- SHA-256 hashed

    -- Activity window
    total_financial_sms_90d      INTEGER      DEFAULT 0,
    earliest_sms_date            DATE,
    latest_sms_date              DATE,

    -- Income / credit signals  (ability to repay)
    salary_detected              BOOLEAN      DEFAULT FALSE,  -- GAP 3 FIXED
    count_salary_credits_90d     INTEGER      DEFAULT 0,
    avg_salary_amount_90d        DECIMAL(15,2) DEFAULT 0.00,
    count_credit_txn_90d         INTEGER      DEFAULT 0,
    sum_credit_amount_90d        DECIMAL(15,2) DEFAULT 0.00,
    max_single_credit_90d        DECIMAL(15,2) DEFAULT 0.00,

    -- Debit / spending signals
    count_debit_txn_90d          INTEGER      DEFAULT 0,
    sum_debit_amount_90d         DECIMAL(15,2) DEFAULT 0.00,

    -- UPI  (GAP 1 FIXED)
    upi_txn_count                INTEGER      DEFAULT 0,
    upi_total_amount_90d         DECIMAL(15,2) DEFAULT 0.00,

    -- Debt & EMI obligations
    count_emi_payment_90d        INTEGER      DEFAULT 0,
    sum_emi_amount_90d           DECIMAL(15,2) DEFAULT 0.00,
    lender_count                 INTEGER      DEFAULT 0,      -- GAP 4 FIXED
    count_loan_disbursement_90d  INTEGER      DEFAULT 0,
    sum_loan_disbursement_90d    DECIMAL(15,2) DEFAULT 0.00,
    count_loan_reminders_90d     INTEGER      DEFAULT 0,

    -- Balance proxies
    last_known_balance           DECIMAL(15,2),
    min_balance_seen_90d         DECIMAL(15,2),
    avg_balance_proxy_90d        DECIMAL(15,2),

    -- Behavioural risk signals  (GAP 2 FIXED)
    alcohol_txn_count            INTEGER      DEFAULT 0,
    alcohol_spend_amount_90d     DECIMAL(15,2) DEFAULT 0.00,
    night_debit_count            INTEGER      DEFAULT 0,
    failed_txn_count             INTEGER      DEFAULT 0,

    -- Derived / scoring
    foir_proxy_90d               DECIMAL(5,4)  DEFAULT 0.00, -- EMI / credit ratio
    credit_momentum              DECIMAL(6,4)  DEFAULT 1.00, -- MoM credit change
    risk_score                   INTEGER       DEFAULT 0,
    risk_flag                    VARCHAR(10),                 -- LOW / MEDIUM / HIGH

    -- Metadata
    feature_date                 DATE          NOT NULL DEFAULT current_date,
    lookback_days                INTEGER       NOT NULL DEFAULT 90,
    last_updated                 TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- DuckDB secondary indexes (PK is auto-indexed)
CREATE INDEX IF NOT EXISTS idx_bsf_risk_flag    ON borrower_sms_features(risk_flag);
CREATE INDEX IF NOT EXISTS idx_bsf_salary       ON borrower_sms_features(salary_detected);
CREATE INDEX IF NOT EXISTS idx_bsf_lender       ON borrower_sms_features(lender_count);


-- ── 2. Multi-label SMS tagging VIEW  (GAP 5 FIXED) ──────────────────────────
--
-- Old version used CASE WHEN → single label per SMS (wrong).
-- New version uses UNION ALL → one row per (SMS × matched category).
-- This is consistent with the Python multi-label classifier.
--
-- DuckDB note: SIMILAR TO not supported → use OR-chained LIKE expressions.

CREATE OR REPLACE VIEW v_sms_tagged AS

WITH base AS (
    SELECT
        id           AS raw_id,
        borrower_id,
        sender_tag,
        lower(sms_body) AS body_lower,
        inbox_time_ms,

        -- Amount extraction using DuckDB's REGEXP_EXTRACT
        TRY_CAST(
            REGEXP_REPLACE(
                REGEXP_EXTRACT(lower(sms_body), '(?:rs\.?|inr|₹)\s*([\d,]+(?:\.\d{1,2})?)', 1),
                ',', '', 'g'
            ) AS DECIMAL(15,2)
        ) AS extracted_amount,

        -- Global exclusion flag (drop before any matching)
        (
            lower(sms_body) LIKE '%otp%'
            OR lower(sms_body) LIKE '%one time password%'
            OR lower(sms_body) LIKE '%one-time password%'
            OR lower(sms_body) LIKE '%verification code%'
            OR lower(sms_body) LIKE '%do not share%'
            OR lower(sms_body) LIKE '%cashback%'
            OR lower(sms_body) LIKE '%lucky draw%'
            OR lower(sms_body) LIKE '%download app%'
            OR lower(sms_body) LIKE '%your order%'
            OR lower(sms_body) LIKE '%out for delivery%'
            OR lower(sms_body) LIKE '%data pack%'
            OR lower(sms_body) LIKE '%recharge%'
            OR lower(sms_body) LIKE '%login attempt%'
        ) AS is_excluded

    FROM sms_raw
    WHERE sender_tag NOT IN ('OTP_SENDER', 'OTHER')
),

-- Category match booleans (all evaluated independently — multi-label)
tagged AS (
    SELECT
        raw_id, borrower_id, sender_tag, inbox_time_ms, extracted_amount,

        (
            NOT is_excluded
            AND sender_tag IN ('BANK')
            AND (
                body_lower LIKE '%salary%'
                OR body_lower LIKE '%sal cr%'
                OR body_lower LIKE '%payroll%'
                OR body_lower LIKE '%wages%'
                OR body_lower LIKE '%stipend%'
            )
        ) AS is_salary_credit,

        (
            NOT is_excluded
            AND sender_tag IN ('BANK', 'UPI')
            AND (
                body_lower LIKE '%credited%'
                OR body_lower LIKE '%neft credit%'
                OR body_lower LIKE '%rtgs credit%'
                OR body_lower LIKE '%imps credit%'
                OR body_lower LIKE '%amount credited%'
                OR body_lower LIKE '%funds received%'
                OR body_lower LIKE '%cr rs%'
            )
        ) AS is_bank_credit,

        (
            NOT is_excluded
            AND sender_tag IN ('BANK', 'UPI')
            AND (
                body_lower LIKE '%debited%'
                OR body_lower LIKE '%nach debit%'
                OR body_lower LIKE '%ecs debit%'
                OR body_lower LIKE '%auto debit%'
                OR body_lower LIKE '%ach debit%'
                OR body_lower LIKE '%atm withdrawal%'
                OR body_lower LIKE '%dr rs%'
            )
        ) AS is_bank_debit,

        (
            NOT is_excluded
            AND sender_tag IN ('BANK', 'UPI')
            AND (
                body_lower LIKE '%upi%'
                OR body_lower LIKE '%gpay%'
                OR body_lower LIKE '%phonepe%'
                OR body_lower LIKE '%paytm%'
                OR body_lower LIKE '%bhim%'
                OR body_lower LIKE '%sent to%'
                OR body_lower LIKE '%paid to%'
                OR body_lower LIKE '%received from%'
                OR body_lower LIKE '%upi ref%'
            )
            AND body_lower NOT LIKE '%failed%'
        ) AS is_upi_payment,

        (
            NOT is_excluded
            AND sender_tag IN ('BANK', 'LENDER')
            AND (
                body_lower LIKE '%emi%'
                OR body_lower LIKE '%installment%'
                OR body_lower LIKE '%nach%'
                OR body_lower LIKE '%ecs%'
                OR body_lower LIKE '%standing instruction%'
            )
        ) AS is_emi_payment,

        (
            NOT is_excluded
            AND sender_tag IN ('BANK', 'LENDER')
            AND (
                body_lower LIKE '%loan disbursed%'
                OR body_lower LIKE '%loan amount credited%'
                OR body_lower LIKE '%disbursement%'
                OR body_lower LIKE '%loan sanctioned%'
                OR body_lower LIKE '%loan approved%'
                OR body_lower LIKE '%pl disbursed%'
            )
        ) AS is_loan_disbursement,

        (
            NOT is_excluded
            AND sender_tag IN ('BANK', 'LENDER')
            AND (
                body_lower LIKE '%due date%'
                OR body_lower LIKE '%payment due%'
                OR body_lower LIKE '%overdue%'
                OR body_lower LIKE '%outstanding%'
                OR body_lower LIKE '%kindly pay%'
                OR body_lower LIKE '%repayment due%'
                OR body_lower LIKE '%amount due%'
                OR body_lower LIKE '%last date%'
            )
        ) AS is_loan_reminder,

        (
            NOT is_excluded
            AND sender_tag = 'BANK'
            AND (
                body_lower LIKE '%available balance%'
                OR body_lower LIKE '%avl bal%'
                OR body_lower LIKE '%closing balance%'
                OR body_lower LIKE '%current balance%'
                OR body_lower LIKE '%bal rs%'
                OR body_lower LIKE '%account balance%'
            )
        ) AS is_balance_inquiry,

        (
            NOT is_excluded
            AND sender_tag IN ('BANK', 'UPI')
            AND (
                body_lower LIKE '%liquor%'
                OR body_lower LIKE '%wine%'
                OR body_lower LIKE '% beer%'
                OR body_lower LIKE '% bar %'
                OR body_lower LIKE '% pub %'
                OR body_lower LIKE '%alcohol%'
                OR body_lower LIKE '%whisky%'
                OR body_lower LIKE '%whiskey%'
                OR body_lower LIKE '% rum %'
                OR body_lower LIKE '%vodka%'
                OR body_lower LIKE '%spirits%'
                OR body_lower LIKE '%breweries%'
            )
        ) AS is_alcohol_spend,

        (
            NOT is_excluded
            AND sender_tag IN ('BANK', 'UPI', 'LENDER')
            AND (
                body_lower LIKE '%failed%'
                OR body_lower LIKE '%declined%'
                OR body_lower LIKE '%rejected%'
                OR body_lower LIKE '%transaction failed%'
                OR body_lower LIKE '%payment failed%'
                OR body_lower LIKE '%insufficient funds%'
                OR body_lower LIKE '%insufficient balance%'
            )
        ) AS is_failed_transaction

    FROM base
),

-- Unpivot: one row per (SMS × matched category) — the multi-label fix
unpivoted AS (
    SELECT raw_id, borrower_id, sender_tag, inbox_time_ms, extracted_amount, 'SALARY_CREDIT'     AS category FROM tagged WHERE is_salary_credit
    UNION ALL
    SELECT raw_id, borrower_id, sender_tag, inbox_time_ms, extracted_amount, 'BANK_CREDIT'        FROM tagged WHERE is_bank_credit
    UNION ALL
    SELECT raw_id, borrower_id, sender_tag, inbox_time_ms, extracted_amount, 'BANK_DEBIT'         FROM tagged WHERE is_bank_debit
    UNION ALL
    SELECT raw_id, borrower_id, sender_tag, inbox_time_ms, extracted_amount, 'UPI_PAYMENT'        FROM tagged WHERE is_upi_payment
    UNION ALL
    SELECT raw_id, borrower_id, sender_tag, inbox_time_ms, extracted_amount, 'EMI_PAYMENT'        FROM tagged WHERE is_emi_payment
    UNION ALL
    SELECT raw_id, borrower_id, sender_tag, inbox_time_ms, extracted_amount, 'LOAN_DISBURSEMENT'  FROM tagged WHERE is_loan_disbursement
    UNION ALL
    SELECT raw_id, borrower_id, sender_tag, inbox_time_ms, extracted_amount, 'LOAN_REMINDER'      FROM tagged WHERE is_loan_reminder
    UNION ALL
    SELECT raw_id, borrower_id, sender_tag, inbox_time_ms, extracted_amount, 'BALANCE_INQUIRY'    FROM tagged WHERE is_balance_inquiry
    UNION ALL
    SELECT raw_id, borrower_id, sender_tag, inbox_time_ms, extracted_amount, 'ALCOHOL_SPEND'      FROM tagged WHERE is_alcohol_spend
    UNION ALL
    SELECT raw_id, borrower_id, sender_tag, inbox_time_ms, extracted_amount, 'FAILED_TRANSACTION' FROM tagged WHERE is_failed_transaction
)

SELECT * FROM unpivoted;


-- ── 3. Upsert query (DuckDB syntax) ─────────────────────────────────────────
--
-- DuckDB supports INSERT OR REPLACE for upsert on primary key conflicts.
-- Run this from run_full_pipeline.py via duckdb.execute().
--
-- Also see: compute_features_pandas() in 03_feature_engineering.py which
-- does the same aggregation in Python and writes directly to DuckDB.

INSERT OR REPLACE INTO borrower_sms_features (
    borrower_id,
    total_financial_sms_90d,
    salary_detected,
    count_salary_credits_90d,
    count_credit_txn_90d,
    sum_credit_amount_90d,
    max_single_credit_90d,
    count_debit_txn_90d,
    sum_debit_amount_90d,
    upi_txn_count,
    upi_total_amount_90d,
    count_emi_payment_90d,
    sum_emi_amount_90d,
    lender_count,
    count_loan_disbursement_90d,
    count_loan_reminders_90d,
    alcohol_txn_count,
    alcohol_spend_amount_90d,
    failed_txn_count,
    foir_proxy_90d,
    risk_score,
    risk_flag,
    last_updated
)
WITH agg AS (
    SELECT
        borrower_id,
        COUNT(*)                                                                      AS total_financial_sms_90d,

        -- salary_detected as TRUE/FALSE  (GAP 3 FIX)
        SUM(CASE WHEN categories LIKE '%SALARY_CREDIT%'     THEN 1 ELSE 0 END) > 0  AS salary_detected,
        SUM(CASE WHEN categories LIKE '%SALARY_CREDIT%'     THEN 1 ELSE 0 END)      AS count_salary_credits_90d,
        SUM(CASE WHEN categories LIKE '%BANK_CREDIT%'       THEN 1 ELSE 0 END)      AS count_credit_txn_90d,
        SUM(CASE WHEN categories LIKE '%BANK_CREDIT%'       THEN amount ELSE 0 END) AS sum_credit_amount_90d,
        MAX(CASE WHEN categories LIKE '%BANK_CREDIT%'       THEN amount ELSE 0 END) AS max_single_credit_90d,
        SUM(CASE WHEN categories LIKE '%BANK_DEBIT%'        THEN 1 ELSE 0 END)      AS count_debit_txn_90d,
        SUM(CASE WHEN categories LIKE '%BANK_DEBIT%'        THEN amount ELSE 0 END) AS sum_debit_amount_90d,

        -- UPI (GAP 1 FIX)
        SUM(CASE WHEN categories LIKE '%UPI_PAYMENT%'       THEN 1 ELSE 0 END)      AS upi_txn_count,
        SUM(CASE WHEN categories LIKE '%UPI_PAYMENT%'       THEN amount ELSE 0 END) AS upi_total_amount_90d,

        SUM(CASE WHEN categories LIKE '%EMI_PAYMENT%'       THEN 1 ELSE 0 END)      AS count_emi_payment_90d,
        SUM(CASE WHEN categories LIKE '%EMI_PAYMENT%'       THEN amount ELSE 0 END) AS sum_emi_amount_90d,

        -- lender_count (GAP 4 FIX)
        SUM(CASE WHEN sender_tag = 'LENDER'                 THEN 1 ELSE 0 END)      AS lender_count,

        SUM(CASE WHEN categories LIKE '%LOAN_DISBURSEMENT%' THEN 1 ELSE 0 END)      AS count_loan_disbursement_90d,
        SUM(CASE WHEN categories LIKE '%LOAN_REMINDER%'     THEN 1 ELSE 0 END)      AS count_loan_reminders_90d,

        -- Alcohol (GAP 2 FIX)
        SUM(CASE WHEN categories LIKE '%ALCOHOL_SPEND%'     THEN 1 ELSE 0 END)      AS alcohol_txn_count,
        SUM(CASE WHEN categories LIKE '%ALCOHOL_SPEND%'     THEN amount ELSE 0 END) AS alcohol_spend_amount_90d,

        SUM(CASE WHEN categories LIKE '%FAILED_TRANSACTION%'THEN 1 ELSE 0 END)      AS failed_txn_count,

        -- FOIR proxy
        CASE
            WHEN SUM(CASE WHEN categories LIKE '%BANK_CREDIT%' THEN amount ELSE 0 END) > 0
            THEN ROUND(
                SUM(CASE WHEN categories LIKE '%EMI_PAYMENT%' THEN amount ELSE 0 END) /
                SUM(CASE WHEN categories LIKE '%BANK_CREDIT%' THEN amount ELSE 0 END),
                4
            )
            ELSE 0
        END AS foir_proxy_90d

    FROM sms_classified
    WHERE txn_date >= current_date - INTERVAL 90 DAYS
    GROUP BY borrower_id
),

-- Risk scoring CTE (uses all the fixed columns)
scored AS (
    SELECT
        *,
        LEAST(100,
            IF(failed_txn_count >= 3,           30, 0) +
            IF(count_loan_reminders_90d >= 2,   20, 0) +
            IF(foir_proxy_90d > 0.5,            20, 0) +
            IF(lender_count >= 4,               15, 0) +   -- lender_count now feeds scoring
            IF(NOT salary_detected,             10, 0) +   -- boolean now feeds scoring
            IF(alcohol_txn_count >= 5,           5, 0) +   -- alcohol now feeds scoring
            IF(count_loan_disbursement_90d >= 3, 10, 0)
        ) AS risk_score

    FROM agg
)

SELECT
    borrower_id,
    total_financial_sms_90d,
    salary_detected,
    count_salary_credits_90d,
    count_credit_txn_90d,
    sum_credit_amount_90d,
    max_single_credit_90d,
    count_debit_txn_90d,
    sum_debit_amount_90d,
    upi_txn_count,
    upi_total_amount_90d,
    count_emi_payment_90d,
    sum_emi_amount_90d,
    lender_count,
    count_loan_disbursement_90d,
    count_loan_reminders_90d,
    alcohol_txn_count,
    alcohol_spend_amount_90d,
    failed_txn_count,
    foir_proxy_90d,
    risk_score,
    CASE
        WHEN risk_score >= 60 THEN 'HIGH'
        WHEN risk_score >= 30 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_flag,
    now() AS last_updated

FROM scored;