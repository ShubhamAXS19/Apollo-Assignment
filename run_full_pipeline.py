"""
run_full_pipeline.py
=====================
Runs the full SMS pipeline with three backend options:
  1. PostgreSQL (Docker)  — primary
  2. DuckDB               — local fallback (no cloud needed)
  3. BigQuery             — production analytics

Usage:
    python run_full_pipeline.py --backend duckdb
    python run_full_pipeline.py --backend postgres
    python run_full_pipeline.py --backend bigquery
    python run_full_pipeline.py              # auto-detects best available
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime, date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config (edit these if needed) ────────────────────────────────────────────
CSV_PATH   = os.getenv("SMS_CSV_PATH", "/Users/shubham/Desktop/Apollo/data/1k_sample_raw_sms_data (1).csv")
PG_CONN    = os.getenv("PG_CONN",      "postgresql://postgres:yourpassword@localhost:5432/sms_db")
BQ_PROJECT = os.getenv("BQ_PROJECT",   "your-gcp-project-id")
BQ_DATASET = os.getenv("BQ_DATASET",   "sms_pipeline")
DUCKDB_PATH= os.getenv("DUCKDB_PATH",  "data/sms_pipeline.duckdb")
CHUNK_SIZE = 50_000


# ══════════════════════════════════════════════════════════════════════════════
# BACKEND DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def detect_backend() -> str:
    """Auto-detect best available backend."""
    # Try PostgreSQL
    try:
        import psycopg2
        conn = psycopg2.connect(PG_CONN, connect_timeout=3)
        conn.close()
        log.info("✅ PostgreSQL detected — using postgres backend")
        return "postgres"
    except Exception as e:
        log.warning(f"PostgreSQL not available: {e}")

    # Try DuckDB
    try:
        import duckdb
        log.info("✅ DuckDB detected — using duckdb backend")
        return "duckdb"
    except ImportError:
        log.warning("DuckDB not installed. Run: pip install duckdb")

    # Try BigQuery
    try:
        from google.cloud import bigquery
        creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if creds and Path(creds).exists():
            log.info("✅ BigQuery credentials found — using bigquery backend")
            return "bigquery"
    except ImportError:
        pass

    raise RuntimeError("No backend available. Install duckdb: pip install duckdb")


# ══════════════════════════════════════════════════════════════════════════════
# SHARED: LOAD + CLASSIFY SMS (works for all backends)
# ══════════════════════════════════════════════════════════════════════════════

def load_and_classify_csv(csv_path: str, chunk_size: int = CHUNK_SIZE) -> pd.DataFrame:
    """
    Stream the 900MB CSV in chunks, classify each row, return
    a single classified DataFrame. Memory-efficient.
    """
    # Load classifier
    sys.path.insert(0, ".")
    from importlib.util import spec_from_file_location, module_from_spec
    spec = spec_from_file_location("p1", "01_privacy_and_filtering.py")
    p1   = module_from_spec(spec)
    spec.loader.exec_module(p1)

    if not Path(csv_path).exists():
        raise FileNotFoundError(
            f"CSV not found at: {csv_path}\n"
            f"Set SMS_CSV_PATH in your .env or pass the correct path."
        )

    classified_chunks = []
    total_raw = 0
    total_kept = 0

    log.info(f"Reading CSV: {csv_path}")
    for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size)):
        total_raw += len(chunk)
        results = chunk.apply(lambda row: p1.process_sms_row(row.to_dict()), axis=1)
        kept = [r for r in results if r is not None]
        total_kept += len(kept)

        if kept:
            classified_chunks.append(pd.DataFrame(kept))

        if (i + 1) % 10 == 0:
            log.info(f"  Processed {total_raw:,} rows, kept {total_kept:,} ({100*total_kept/total_raw:.1f}%)...")

    log.info(f"✅ Classification complete: {total_kept:,} / {total_raw:,} rows kept")
    return pd.concat(classified_chunks, ignore_index=True) if classified_chunks else pd.DataFrame()


def build_features(df_classified: pd.DataFrame) -> pd.DataFrame:
    """Run feature engineering on classified DataFrame."""
    from importlib.util import spec_from_file_location, module_from_spec
    spec = spec_from_file_location("p3", "03_feature_engineering.py")
    p3   = module_from_spec(spec)
    spec.loader.exec_module(p3)

    log.info("Building borrower-level features...")
    features = p3.compute_features_pandas(df_classified)
    log.info(f"✅ Features built for {len(features):,} borrowers")
    return features


# ══════════════════════════════════════════════════════════════════════════════
# BACKEND: DuckDB
# ══════════════════════════════════════════════════════════════════════════════

def run_duckdb(csv_path: str):
    import duckdb

    os.makedirs("data", exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    log.info(f"DuckDB database: {DUCKDB_PATH}")

    # ── Step 1: Load raw SMS directly from CSV ────────────────────────────
    log.info("Step 1: Loading raw SMS into DuckDB...")
    con.execute(f"""
        CREATE OR REPLACE TABLE sms_raw AS
        SELECT
            customer_id                                    AS borrower_id,
            sms_sender                                     AS sender_tag_raw,
            sms_body,
            CAST(inbox_time_in_millis AS BIGINT)           AS inbox_time_ms,
            epoch_ms(inbox_time_in_millis)::DATE           AS txn_date,
            hash(customer_id || sms_body || CAST(inbox_time_in_millis AS VARCHAR))
                                                           AS dedup_key
        FROM read_csv_auto('{csv_path}', header=true)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, inbox_time_in_millis) = 1
    """)
    raw_count = con.execute("SELECT COUNT(*) FROM sms_raw").fetchone()[0]
    log.info(f"  Loaded {raw_count:,} raw rows (deduplicated)")

    # ── Step 2: Classify using Python (DuckDB → DataFrame → classify → back) 
    log.info("Step 2: Classifying SMS...")
    df_classified = load_and_classify_csv(csv_path)

    if df_classified.empty:
        log.error("No rows classified — check your CSV path and column names")
        return

    # Write classified back to DuckDB
    con.execute("DROP TABLE IF EXISTS sms_classified")
    con.register("df_classified_view", df_classified)
    con.execute("""
        CREATE TABLE sms_classified AS
        SELECT * FROM df_classified_view
    """)
    classified_count = con.execute("SELECT COUNT(*) FROM sms_classified").fetchone()[0]
    log.info(f"  {classified_count:,} rows written to sms_classified")

    # ── Step 3: Build features ────────────────────────────────────────────
    log.info("Step 3: Building features...")
    features = build_features(df_classified)

    con.execute("DROP TABLE IF EXISTS borrower_sms_features")
    con.register("features_view", features)
    con.execute("""
        CREATE TABLE borrower_sms_features AS
        SELECT * FROM features_view
    """)
    log.info(f"  {len(features):,} borrower rows written to borrower_sms_features")

    # ── Step 4: Validation & summary ─────────────────────────────────────
    log.info("Step 4: Validation summary...")
    print("\n" + "=" * 60)
    print("RISK FLAG DISTRIBUTION")
    print("=" * 60)
    print(con.execute("""
        SELECT risk_flag,
               COUNT(*)                          AS borrowers,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM borrower_sms_features
        GROUP BY 1 ORDER BY 2 DESC
    """).df().to_string(index=False))

    print("\n" + "=" * 60)
    print("TOP 10 BORROWERS BY CREDIT AMOUNT (90d)")
    print("=" * 60)
    credit_col = "sum_credit_amount_90d" if "sum_credit_amount_90d" in features.columns else "monthly_credit_amount"
    print(con.execute(f"""
        SELECT borrower_id,
               {credit_col}          AS credit_90d,
               risk_flag
        FROM borrower_sms_features
        ORDER BY {credit_col} DESC
        LIMIT 10
    """).df().to_string(index=False))

    print("\n" + "=" * 60)
    print("SMS CATEGORY BREAKDOWN")
    print("=" * 60)
    print(con.execute("""
        SELECT sender_tag,
               COUNT(*) AS sms_count
        FROM sms_classified
        GROUP BY 1
        ORDER BY 2 DESC
    """).df().to_string(index=False))

    con.close()
    log.info(f"\n✅ DuckDB pipeline complete. Database saved to: {DUCKDB_PATH}")
    log.info("   Open with: python -c \"import duckdb; con=duckdb.connect('data/sms_pipeline.duckdb'); print(con.execute('SELECT * FROM borrower_sms_features LIMIT 5').df())\"")


# ══════════════════════════════════════════════════════════════════════════════
# BACKEND: PostgreSQL
# ══════════════════════════════════════════════════════════════════════════════

def run_postgres(csv_path: str):
    import psycopg2
    from psycopg2.extras import execute_values, Json

    log.info("Step 1: Classifying SMS from CSV...")
    df_classified = load_and_classify_csv(csv_path)
    if df_classified.empty:
        log.error("No rows classified.")
        return

    conn = psycopg2.connect(PG_CONN)
    cur  = conn.cursor()

    # ── Write to sms_classified ───────────────────────────────────────────
    log.info("Step 2: Writing classified SMS to PostgreSQL...")
    cur.execute("TRUNCATE sms_classified")

    rows = []
    for _, r in df_classified.iterrows():
        cats = r["categories"]
        if isinstance(cats, str):
            cats = json.loads(cats)
        rows.append((
            r["borrower_id"], r["sender_tag"], Json(cats),
            r["sms_body"], r.get("amount"), r.get("balance"),
            r.get("txn_year"), r.get("txn_month"), r.get("txn_date"),
            r.get("txn_hour"), int(r["inbox_time_ms"]),
        ))

    execute_values(cur, """
        INSERT INTO sms_classified
            (borrower_id, sender_tag, categories, sms_body, amount, balance,
             txn_year, txn_month, txn_date, txn_hour, inbox_time_ms)
        VALUES %s
    """, rows, page_size=1000)
    conn.commit()
    log.info(f"  {len(rows):,} rows written")

    # ── Build + write features ────────────────────────────────────────────
    log.info("Step 3: Building features...")
    features = build_features(df_classified)

    cur.execute("TRUNCATE borrower_sms_features")
    feat_rows = [tuple(r) for r in features.itertuples(index=False)]
    cols = list(features.columns)

    execute_values(cur, f"""
        INSERT INTO borrower_sms_features ({', '.join(cols)})
        VALUES %s
        ON CONFLICT (borrower_id) DO UPDATE SET
        {', '.join(f"{c} = EXCLUDED.{c}" for c in cols if c != 'borrower_id')}
    """, feat_rows, page_size=500)
    conn.commit()

    # ── Summary ───────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("RISK FLAG DISTRIBUTION")
    print("=" * 60)
    summary = pd.read_sql("""
        SELECT risk_flag, COUNT(*) AS borrowers
        FROM borrower_sms_features GROUP BY 1 ORDER BY 2 DESC
    """, conn)
    print(summary.to_string(index=False))

    cur.close()
    conn.close()
    log.info("✅ PostgreSQL pipeline complete.")


# ══════════════════════════════════════════════════════════════════════════════
# BACKEND: BigQuery
# ══════════════════════════════════════════════════════════════════════════════

def run_bigquery(csv_path: str):
    from google.cloud import bigquery

    client = bigquery.Client(project=BQ_PROJECT)
    dataset_ref = f"{BQ_PROJECT}.{BQ_DATASET}"

    # Create dataset if needed
    client.create_dataset(dataset_ref, exists_ok=True)
    log.info(f"BigQuery dataset: {dataset_ref}")

    log.info("Step 1: Classifying SMS from CSV...")
    df_classified = load_and_classify_csv(csv_path)
    if df_classified.empty:
        log.error("No rows classified.")
        return

    log.info("Step 2: Writing classified SMS to BigQuery...")
    job = client.load_table_from_dataframe(
        df_classified,
        f"{dataset_ref}.sms_classified",
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
    )
    job.result()
    log.info(f"  {len(df_classified):,} rows written to sms_classified")

    log.info("Step 3: Building features...")
    features = build_features(df_classified)

    job2 = client.load_table_from_dataframe(
        features,
        f"{dataset_ref}.borrower_sms_features",
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
    )
    job2.result()
    log.info(f"  {len(features):,} rows written to borrower_sms_features")
    log.info(f"✅ BigQuery pipeline complete. Check: {dataset_ref}")


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SMS Feature Pipeline")
    parser.add_argument(
        "--backend",
        choices=["postgres", "duckdb", "bigquery", "auto"],
        default="auto",
        help="Backend to use (default: auto-detect)"
    )
    parser.add_argument(
        "--csv",
        default=CSV_PATH,
        help=f"Path to SMS CSV file (default: {CSV_PATH})"
    )
    args = parser.parse_args()

    backend = args.backend if args.backend != "auto" else detect_backend()
    csv     = args.csv

    log.info(f"Backend : {backend.upper()}")
    log.info(f"CSV path: {csv}")
    log.info(f"Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    if backend == "duckdb":
        run_duckdb(csv)
    elif backend == "postgres":
        run_postgres(csv)
    elif backend == "bigquery":
        run_bigquery(csv)