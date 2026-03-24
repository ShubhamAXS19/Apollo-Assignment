"""
run_full_pipeline.py
=====================
Runs the full SMS pipeline with four backend options:
  1. DuckDB               — local fallback (no cloud needed)
  2. PostgreSQL (Docker)  — local operational DB
  3. BigQuery (GCP)       — cloud analytics
  4. Athena (AWS)         — cloud analytics

Usage:
    python run_full_pipeline.py --backend duckdb
    python run_full_pipeline.py --backend postgres
    python run_full_pipeline.py --backend bigquery
    python run_full_pipeline.py --backend athena
    python run_full_pipeline.py              # auto-detects best available

Setup per backend:
  DuckDB:   pip install duckdb  (zero config)
  Postgres: Docker running + PG_CONN in .env
  BigQuery: GOOGLE_APPLICATION_CREDENTIALS + BQ_PROJECT + BQ_DATASET in .env
  Athena:   AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY + AWS_REGION +
            ATHENA_S3_BUCKET + ATHENA_DATABASE in .env
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
CSV_PATH   = os.getenv("SMS_CSV_PATH", "data/sms_data.csv")
PG_CONN    = os.getenv("PG_CONN",      "postgresql://postgres:yourpassword@localhost:5432/sms_db")
BQ_PROJECT = os.getenv("BQ_PROJECT",   "your-gcp-project-id")
BQ_DATASET = os.getenv("BQ_DATASET",   "sms_pipeline")
DUCKDB_PATH= os.getenv("DUCKDB_PATH",  "data/sms_pipeline.duckdb")
CHUNK_SIZE = 50_000

# ── AWS / Athena config ───────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID",     "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION            = os.getenv("AWS_REGION",            "ap-south-1")   # Mumbai
ATHENA_S3_BUCKET      = os.getenv("ATHENA_S3_BUCKET",      "")             # e.g. sms-pipeline-bucket
ATHENA_DATABASE       = os.getenv("ATHENA_DATABASE",       "sms_pipeline")


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
        if creds and Path(creds).exists() and BQ_PROJECT != "your-gcp-project-id":
            log.info("✅ BigQuery credentials found — using bigquery backend")
            return "bigquery"
    except ImportError:
        pass

    # Try Athena
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and ATHENA_S3_BUCKET:
        try:
            import boto3
            log.info("✅ AWS credentials found — using athena backend")
            return "athena"
        except ImportError:
            log.warning("boto3 not installed. Run: pip install boto3 pyathena")

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
# BACKEND: BigQuery (GCP)
# ══════════════════════════════════════════════════════════════════════════════

def run_bigquery(csv_path: str):
    """
    Writes classified SMS and borrower features to Google BigQuery.

    Prerequisites:
        pip install google-cloud-bigquery google-cloud-bigquery-storage pyarrow db-dtypes

    .env keys needed:
        GOOGLE_APPLICATION_CREDENTIALS=/path/to/bq_key.json
        BQ_PROJECT=your-gcp-project-id
        BQ_DATASET=sms_pipeline
    """
    from google.cloud import bigquery

    # Validate credentials
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not creds or not Path(creds).exists():
        raise FileNotFoundError(
            f"BigQuery credentials not found at: {creds}\n"
            "Set GOOGLE_APPLICATION_CREDENTIALS in your .env file.\n"
            "Get a service account JSON from: console.cloud.google.com/iam-admin/serviceaccounts"
        )
    if BQ_PROJECT == "your-gcp-project-id":
        raise ValueError("Set BQ_PROJECT to your actual GCP project ID in .env")

    client      = bigquery.Client(project=BQ_PROJECT)
    dataset_ref = f"{BQ_PROJECT}.{BQ_DATASET}"

    # Create dataset if it doesn't exist
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)
    log.info(f"BigQuery dataset: {dataset_ref}")

    # ── Step 1: Classify SMS ──────────────────────────────────────────────
    log.info("Step 1: Classifying SMS from CSV...")
    df_classified = load_and_classify_csv(csv_path)
    if df_classified.empty:
        log.error("No rows classified.")
        return

    # ── Step 2: Write sms_classified to BigQuery ──────────────────────────
    log.info("Step 2: Writing classified SMS to BigQuery...")

    # BigQuery doesn't support Python list columns — convert to JSON string
    df_bq = df_classified.copy()
    df_bq["categories"] = df_bq["categories"].apply(
        lambda x: x if isinstance(x, str) else json.dumps(x)
    )
    # Convert date objects to strings for BQ compatibility
    if "txn_date" in df_bq.columns:
        df_bq["txn_date"] = df_bq["txn_date"].astype(str)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(
        df_bq, f"{dataset_ref}.sms_classified", job_config=job_config
    )
    job.result()
    log.info(f"  ✅ {len(df_bq):,} rows written to {dataset_ref}.sms_classified")

    # ── Step 3: Build features ────────────────────────────────────────────
    log.info("Step 3: Building borrower features...")
    features = build_features(df_classified)

    # Sanitise features for BigQuery (convert dates, booleans)
    feat_bq = features.copy()
    for col in feat_bq.select_dtypes(include=["object"]).columns:
        feat_bq[col] = feat_bq[col].astype(str)
    for col in feat_bq.select_dtypes(include=["datetime64"]).columns:
        feat_bq[col] = feat_bq[col].astype(str)

    job2_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job2 = client.load_table_from_dataframe(
        feat_bq, f"{dataset_ref}.borrower_sms_features", job_config=job2_config
    )
    job2.result()
    log.info(f"  ✅ {len(feat_bq):,} rows written to {dataset_ref}.borrower_sms_features")

    # ── Step 4: Validation summary via BQ SQL ────────────────────────────
    log.info("Step 4: Validation summary...")
    print("\n" + "=" * 60)
    print("RISK FLAG DISTRIBUTION  (BigQuery)")
    print("=" * 60)
    query = f"""
        SELECT risk_flag,
               COUNT(*)                                           AS borrowers,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct
        FROM `{dataset_ref}.borrower_sms_features`
        GROUP BY 1
        ORDER BY 2 DESC
    """
    print(client.query(query).to_dataframe().to_string(index=False))

    print("\n" + "=" * 60)
    print("SMS CATEGORY BREAKDOWN  (BigQuery)")
    print("=" * 60)
    query2 = f"""
        SELECT sender_tag, COUNT(*) AS sms_count
        FROM `{dataset_ref}.sms_classified`
        GROUP BY 1
        ORDER BY 2 DESC
    """
    print(client.query(query2).to_dataframe().to_string(index=False))

    log.info(f"\n✅ BigQuery pipeline complete.")
    log.info(f"   View tables at: console.cloud.google.com/bigquery?project={BQ_PROJECT}")


# ══════════════════════════════════════════════════════════════════════════════
# BACKEND: AWS Athena
# ══════════════════════════════════════════════════════════════════════════════

def run_athena(csv_path: str):
    """
    Uploads classified SMS and features to S3, then registers them
    as Athena tables for SQL querying.

    Prerequisites:
        pip install boto3 pyathena pandas

    .env keys needed:
        AWS_ACCESS_KEY_ID=your-access-key
        AWS_SECRET_ACCESS_KEY=your-secret-key
        AWS_REGION=ap-south-1
        ATHENA_S3_BUCKET=your-s3-bucket-name    # must already exist
        ATHENA_DATABASE=sms_pipeline

    How to create the S3 bucket (one-time):
        aws s3 mb s3://your-bucket-name --region ap-south-1
    Or via AWS console: S3 → Create bucket → choose a unique name.
    """
    import boto3
    import pyathena
    from pyathena import connect as athena_connect

    # Validate config
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise ValueError(
            "AWS credentials missing. Set AWS_ACCESS_KEY_ID and "
            "AWS_SECRET_ACCESS_KEY in your .env file.\n"
            "Get them from: console.aws.amazon.com/iam → Users → Security credentials"
        )
    if not ATHENA_S3_BUCKET:
        raise ValueError(
            "ATHENA_S3_BUCKET not set in .env.\n"
            "Create a bucket at console.aws.amazon.com/s3 and set its name."
        )

    # S3 paths
    S3_PREFIX         = f"s3://{ATHENA_S3_BUCKET}/sms-pipeline"
    S3_CLASSIFIED     = f"{S3_PREFIX}/sms_classified/"
    S3_FEATURES       = f"{S3_PREFIX}/borrower_sms_features/"
    S3_ATHENA_RESULTS = f"{S3_PREFIX}/athena-results/"

    # Boto3 clients
    s3_client = boto3.client(
        "s3",
        region_name            = AWS_REGION,
        aws_access_key_id      = AWS_ACCESS_KEY_ID,
        aws_secret_access_key  = AWS_SECRET_ACCESS_KEY,
    )

    def upload_df_to_s3(df: pd.DataFrame, s3_path: str, name: str):
        """Save DataFrame as Parquet and upload to S3."""
        import io
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        bucket, key = s3_path.replace("s3://", "").split("/", 1)
        key = key + "data.parquet"
        s3_client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
        log.info(f"  ✅ {len(df):,} rows uploaded to s3://{bucket}/{key}")

    # ── Step 1: Classify SMS ──────────────────────────────────────────────
    log.info("Step 1: Classifying SMS from CSV...")
    df_classified = load_and_classify_csv(csv_path)
    if df_classified.empty:
        log.error("No rows classified.")
        return

    # Sanitise for Parquet (Athena doesn't support Python list columns)
    df_s3 = df_classified.copy()
    df_s3["categories"] = df_s3["categories"].apply(
        lambda x: x if isinstance(x, str) else json.dumps(x)
    )
    if "txn_date" in df_s3.columns:
        df_s3["txn_date"] = pd.to_datetime(df_s3["txn_date"]).dt.date.astype(str)

    # ── Step 2: Upload classified SMS to S3 ──────────────────────────────
    log.info("Step 2: Uploading classified SMS to S3...")
    upload_df_to_s3(df_s3, S3_CLASSIFIED, "sms_classified")

    # ── Step 3: Build features + upload to S3 ────────────────────────────
    log.info("Step 3: Building features and uploading to S3...")
    features = build_features(df_classified)

    feat_s3 = features.copy()
    for col in feat_s3.select_dtypes(include=["datetime64", "object"]).columns:
        feat_s3[col] = feat_s3[col].astype(str)

    upload_df_to_s3(feat_s3, S3_FEATURES, "borrower_sms_features")

    # ── Step 4: Register Athena tables ────────────────────────────────────
    log.info("Step 4: Registering Athena tables...")

    # Build column definitions dynamically from DataFrames
    def pandas_dtype_to_athena(dtype) -> str:
        if pd.api.types.is_integer_dtype(dtype):   return "BIGINT"
        if pd.api.types.is_float_dtype(dtype):     return "DOUBLE"
        if pd.api.types.is_bool_dtype(dtype):      return "BOOLEAN"
        return "STRING"

    def make_athena_cols(df: pd.DataFrame) -> str:
        return ",\n  ".join(
            f"`{col}` {pandas_dtype_to_athena(df[col].dtype)}"
            for col in df.columns
        )

    classified_cols = make_athena_cols(df_s3)
    features_cols   = make_athena_cols(feat_s3)

    # Athena connection
    conn = athena_connect(
        s3_staging_dir    = S3_ATHENA_RESULTS,
        region_name       = AWS_REGION,
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    )
    cursor = conn.cursor()

    # Create database
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {ATHENA_DATABASE}")
    log.info(f"  Database: {ATHENA_DATABASE}")

    # Create sms_classified table
    cursor.execute(f"DROP TABLE IF EXISTS {ATHENA_DATABASE}.sms_classified")
    cursor.execute(f"""
        CREATE EXTERNAL TABLE {ATHENA_DATABASE}.sms_classified (
          {classified_cols}
        )
        STORED AS PARQUET
        LOCATION '{S3_CLASSIFIED}'
        TBLPROPERTIES ('parquet.compress'='SNAPPY')
    """)
    log.info("  ✅ Table registered: sms_classified")

    # Create borrower_sms_features table
    cursor.execute(f"DROP TABLE IF EXISTS {ATHENA_DATABASE}.borrower_sms_features")
    cursor.execute(f"""
        CREATE EXTERNAL TABLE {ATHENA_DATABASE}.borrower_sms_features (
          {features_cols}
        )
        STORED AS PARQUET
        LOCATION '{S3_FEATURES}'
        TBLPROPERTIES ('parquet.compress'='SNAPPY')
    """)
    log.info("  ✅ Table registered: borrower_sms_features")

    # ── Step 5: Validation summary ────────────────────────────────────────
    log.info("Step 5: Validation summary...")
    print("\n" + "=" * 60)
    print("RISK FLAG DISTRIBUTION  (Athena)")
    print("=" * 60)
    cursor.execute(f"""
        SELECT risk_flag,
               COUNT(*) AS borrowers
        FROM {ATHENA_DATABASE}.borrower_sms_features
        GROUP BY 1
        ORDER BY 2 DESC
    """)
    print(pd.DataFrame(cursor.fetchall(), columns=["risk_flag", "borrowers"]).to_string(index=False))

    print("\n" + "=" * 60)
    print("SMS CATEGORY BREAKDOWN  (Athena)")
    print("=" * 60)
    cursor.execute(f"""
        SELECT sender_tag, COUNT(*) AS sms_count
        FROM {ATHENA_DATABASE}.sms_classified
        GROUP BY 1
        ORDER BY 2 DESC
    """)
    print(pd.DataFrame(cursor.fetchall(), columns=["sender_tag", "sms_count"]).to_string(index=False))

    cursor.close()
    log.info(f"\n✅ Athena pipeline complete.")
    log.info(f"   Query tables at: console.aws.amazon.com/athena → Database: {ATHENA_DATABASE}")
    log.info(f"   S3 data location: {S3_PREFIX}")


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SMS Feature Pipeline")
    parser.add_argument(
        "--backend",
        choices=["postgres", "duckdb", "bigquery", "athena", "auto"],
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
    elif backend == "athena":
        run_athena(csv)