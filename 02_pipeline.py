"""
Part 3: Production Data Pipeline
=================================
Architecture:
    Device SDK → Raw SMS Table (PostgreSQL/BigQuery)
                    ↓  (Airflow DAG, daily)
              Filtered SMS Table
                    ↓
              Feature Table: borrower_sms_features
                    ↓
              Metabase Dashboard

Stack: Python + PostgreSQL (local/dev) / BigQuery (prod) + Airflow
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Iterator

import pandas as pd
from google.cloud import bigquery

# ─────────────────────────────────────────────────────────
# DATABASE SCHEMAS (PostgreSQL DDL)
# ─────────────────────────────────────────────────────────

POSTGRES_DDL = """
-- ── 1. Raw ingestion table (append-only, partitioned by date) ───────────────
CREATE TABLE IF NOT EXISTS sms_raw (
    id                  BIGSERIAL PRIMARY KEY,
    borrower_id         TEXT        NOT NULL,   -- pre-hashed on device
    sender_tag          TEXT        NOT NULL,   -- BANK / UPI / LENDER
    sms_body            TEXT        NOT NULL,   -- PII-masked body
    inbox_time_ms       BIGINT      NOT NULL,
    txn_date            DATE,
    collected_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    pipeline_run_id     TEXT                    -- Airflow run_id for lineage
) PARTITION BY RANGE (txn_date);

CREATE INDEX IF NOT EXISTS idx_sms_raw_borrower ON sms_raw(borrower_id);
CREATE INDEX IF NOT EXISTS idx_sms_raw_date     ON sms_raw(txn_date);

-- ── 2. Classified / filtered SMS table ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS sms_classified (
    id              BIGSERIAL PRIMARY KEY,
    raw_id          BIGINT      REFERENCES sms_raw(id),
    borrower_id     TEXT        NOT NULL,
    sender_tag      TEXT        NOT NULL,
    categories      JSONB       NOT NULL,       -- e.g. ["BANK_CREDIT","SALARY_CREDIT"]
    sms_body        TEXT        NOT NULL,
    amount          NUMERIC(15,2),
    balance         NUMERIC(15,2),
    txn_year        SMALLINT,
    txn_month       SMALLINT,
    txn_date        DATE,
    txn_hour        SMALLINT,
    inbox_time_ms   BIGINT      NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sms_cls_borrower  ON sms_classified(borrower_id);
CREATE INDEX IF NOT EXISTS idx_sms_cls_date      ON sms_classified(txn_date);
CREATE INDEX IF NOT EXISTS idx_sms_cls_cats      ON sms_classified USING GIN(categories);

-- ── 3. Final feature table (see Part 5 for full DDL) ────────────────────────
-- Defined in 05_feature_table.sql
"""

# BigQuery equivalent dataset + table creation
BIGQUERY_SCHEMA = {
    "sms_raw": [
        bigquery.SchemaField("borrower_id",     "STRING",  mode="REQUIRED"),
        bigquery.SchemaField("sender_tag",      "STRING",  mode="REQUIRED"),
        bigquery.SchemaField("sms_body",        "STRING",  mode="REQUIRED"),
        bigquery.SchemaField("inbox_time_ms",   "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("txn_date",        "DATE"),
        bigquery.SchemaField("collected_at",    "TIMESTAMP"),
        bigquery.SchemaField("pipeline_run_id", "STRING"),
    ],
    "sms_classified": [
        bigquery.SchemaField("borrower_id",   "STRING",  mode="REQUIRED"),
        bigquery.SchemaField("sender_tag",    "STRING",  mode="REQUIRED"),
        bigquery.SchemaField("categories",    "JSON"),
        bigquery.SchemaField("sms_body",      "STRING",  mode="REQUIRED"),
        bigquery.SchemaField("amount",        "FLOAT64"),
        bigquery.SchemaField("balance",       "FLOAT64"),
        bigquery.SchemaField("txn_year",      "INTEGER"),
        bigquery.SchemaField("txn_month",     "INTEGER"),
        bigquery.SchemaField("txn_date",      "DATE"),
        bigquery.SchemaField("txn_hour",      "INTEGER"),
        bigquery.SchemaField("inbox_time_ms", "INTEGER", mode="REQUIRED"),
    ],
}


# ─────────────────────────────────────────────────────────
# INGESTION: Raw CSV → sms_raw (chunked for 900MB file)
# ─────────────────────────────────────────────────────────

def ingest_raw_csv_to_postgres(
    csv_path: str,
    pg_conn_str: str,
    run_id: str,
    chunk_size: int = 50_000,
) -> int:
    """
    Stream-load raw SMS CSV into PostgreSQL in chunks.
    Safe for 900MB / 2.9M row files.
    Returns total rows loaded.
    """
    import psycopg2
    from psycopg2.extras import execute_values

    total = 0
    conn = psycopg2.connect(pg_conn_str)
    cur = conn.cursor()

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        chunk = chunk.rename(columns={
            "customer_id":          "borrower_id",
            "sms_sender":           "sender_tag_raw",
            "inbox_time_in_millis": "inbox_time_ms",
        })
        chunk["txn_date"]       = pd.to_datetime(chunk["inbox_time_ms"], unit="ms", utc=True).dt.date
        chunk["collected_at"]   = datetime.utcnow()
        chunk["pipeline_run_id"]= run_id

        rows = [
            (
                r["borrower_id"],
                r["sms_sender"] if "sms_sender" in r else r.get("sender_tag_raw", ""),
                r["sms_body"],
                int(r["inbox_time_ms"]),
                r["txn_date"],
                r["collected_at"],
                r["pipeline_run_id"],
            )
            for _, r in chunk.iterrows()
        ]
        execute_values(cur,
            """
            INSERT INTO sms_raw
                (borrower_id, sender_tag, sms_body, inbox_time_ms,
                 txn_date, collected_at, pipeline_run_id)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            rows,
            page_size=1000,
        )
        conn.commit()
        total += len(rows)
        logging.info(f"Loaded {total:,} rows so far...")

    cur.close()
    conn.close()
    return total


def ingest_raw_csv_to_bigquery(
    csv_path: str,
    project_id: str,
    dataset_id: str,
    run_id: str,
    chunk_size: int = 100_000,
) -> int:
    """Stream-load raw SMS CSV into BigQuery."""
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.sms_raw"
    total = 0

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        chunk = chunk.rename(columns={
            "customer_id":          "borrower_id",
            "sms_sender":           "sender_tag",
            "inbox_time_in_millis": "inbox_time_ms",
        })
        chunk["txn_date"]        = pd.to_datetime(chunk["inbox_time_ms"], unit="ms", utc=True).dt.date.astype(str)
        chunk["collected_at"]    = datetime.utcnow().isoformat()
        chunk["pipeline_run_id"] = run_id

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=BIGQUERY_SCHEMA["sms_raw"],
        )
        job = client.load_table_from_dataframe(chunk, table_ref, job_config=job_config)
        job.result()
        total += len(chunk)
        logging.info(f"[BQ] Loaded {total:,} rows...")

    return total


# ─────────────────────────────────────────────────────────
# CLASSIFICATION STEP: sms_raw → sms_classified
# ─────────────────────────────────────────────────────────

def run_classification_postgres(pg_conn_str: str, run_date: str) -> int:
    """
    Pull unclassified rows from sms_raw for run_date,
    classify them, and insert into sms_classified.
    Runs as Airflow task 2.
    """
    import psycopg2
    from psycopg2.extras import execute_values, Json
    from sms_pipeline.privacy_and_filtering import (
        normalize_sender, is_excluded, classify_sms,
        extract_amount, extract_balance, sanitize_sms_body,
    )

    conn = psycopg2.connect(pg_conn_str)
    cur  = conn.cursor()

    cur.execute(
        """
        SELECT id, borrower_id, sender_tag, sms_body, inbox_time_ms, txn_date
        FROM   sms_raw
        WHERE  txn_date = %s
        AND    id NOT IN (SELECT raw_id FROM sms_classified WHERE raw_id IS NOT NULL)
        """,
        (run_date,),
    )
    rows = cur.fetchall()
    classified = []

    for raw_id, borrower_id, sender_raw, body, ts_ms, txn_date in rows:
        sender_tag = normalize_sender(sender_raw)
        if sender_tag in ("OTP_SENDER", "OTHER"):
            continue
        if is_excluded(body):
            continue
        cats = classify_sms(sender_tag, body)
        if not cats:
            continue
        clean_body = sanitize_sms_body(body)
        amount  = extract_amount(body)
        balance = extract_balance(body)
        ts      = datetime.utcfromtimestamp(ts_ms / 1000) if ts_ms else None

        classified.append((
            raw_id, borrower_id, sender_tag,
            Json(cats), clean_body, amount, balance,
            ts.year if ts else None, ts.month if ts else None,
            txn_date, ts.hour if ts else None, ts_ms,
        ))

    if classified:
        execute_values(cur,
            """
            INSERT INTO sms_classified
                (raw_id, borrower_id, sender_tag, categories, sms_body,
                 amount, balance, txn_year, txn_month, txn_date, txn_hour, inbox_time_ms)
            VALUES %s
            """,
            classified,
            page_size=500,
        )
        conn.commit()

    cur.close()
    conn.close()
    return len(classified)


# ─────────────────────────────────────────────────────────
# AIRFLOW DAG DEFINITION
# ─────────────────────────────────────────────────────────

AIRFLOW_DAG_CODE = '''
"""
Airflow DAG: sms_feature_pipeline
Runs daily at 02:00 IST. Processes previous day's SMS data.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    "owner":            "analytics_eng",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": True,
}

with DAG(
    dag_id           = "sms_feature_pipeline",
    default_args     = default_args,
    schedule_interval= "30 20 * * *",  # 02:00 IST = 20:30 UTC
    start_date       = datetime(2024, 1, 1),
    catchup          = False,
    tags             = ["sms", "underwriting"],
) as dag:

    def task_ingest(**ctx):
        from pipeline.ingestion import ingest_raw_csv_to_postgres
        run_date = ctx["ds"]  # YYYY-MM-DD
        conn_str = PostgresHook("postgres_sms").get_uri()
        # In prod: data arrives via API, not CSV. Adapt accordingly.
        ingest_raw_csv_to_postgres("/data/sms_daily.csv", conn_str, run_id=ctx["run_id"])

    def task_classify(**ctx):
        from pipeline.classification import run_classification_postgres
        conn_str = PostgresHook("postgres_sms").get_uri()
        run_classification_postgres(conn_str, ctx["ds"])

    def task_features(**ctx):
        from pipeline.features import run_feature_refresh_postgres
        conn_str = PostgresHook("postgres_sms").get_uri()
        run_feature_refresh_postgres(conn_str, ctx["ds"])

    def task_validate(**ctx):
        from pipeline.validation import run_data_quality_checks
        conn_str = PostgresHook("postgres_sms").get_uri()
        run_data_quality_checks(conn_str, ctx["ds"])

    t1 = PythonOperator(task_id="ingest_raw_sms",    python_callable=task_ingest,    provide_context=True)
    t2 = PythonOperator(task_id="classify_sms",      python_callable=task_classify,  provide_context=True)
    t3 = PythonOperator(task_id="build_features",    python_callable=task_features,  provide_context=True)
    t4 = PythonOperator(task_id="validate_output",   python_callable=task_validate,  provide_context=True)

    t1 >> t2 >> t3 >> t4
'''

if __name__ == "__main__":
    print(AIRFLOW_DAG_CODE)
