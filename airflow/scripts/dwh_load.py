"""
=================================================
ETL1 - DWH Load Phase (Neon PostgreSQL)

Tasks:
1. Connect to Neon PostgreSQL (schema = dwh)
2. Load all transformed CSVs (dim & fact)
3. Use TRUNCATE + append to maintain schema & FKs
4. Add created_at and updated_at timestamps
=================================================
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

print("Starting DWH Load Phase...")

# ============================================================
# 1. Database Configuration
# ============================================================

HOST = "ep-red-grass-a1y4mh5d-pooler.ap-southeast-1.aws.neon.tech"
PORT = "5432"
DATABASE = "neondb"
USER = "neondb_owner"
PASSWORD = "npg_3MPZY4FkDGbr"
SSLMODE = "require"

CONNECTION_STRING = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?sslmode={SSLMODE}"
engine = create_engine(CONNECTION_STRING)

# ============================================================
# 2. Load CSV files
# ============================================================

DATA_DIR = "/opt/airflow/data/dwh"

tables = [
    "dwh_dim_country",
    "dwh_dim_time",
    "dwh_dim_emission_source",
    "dwh_fact_emission",
    "dwh_fact_population"
]

# ============================================================
# 3. Helper Function
# ============================================================

def load_table(schema_table: str, csv_path: str):
    schema = schema_table.split(".")[0]
    table = schema_table.split(".")[1]

    print(f"Loading {schema}.{table} ...")

    df = pd.read_csv(csv_path)

    # Add metadata timestamps if missing
    if "created_at" not in df.columns:
        df["created_at"] = datetime.now()
    if "updated_at" not in df.columns:
        df["updated_at"] = datetime.now()

    # Detect and remove generated columns
    with engine.begin() as conn:
        query = text(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table AND is_generated = 'ALWAYS'
        """)
        generated_cols = [row[0] for row in conn.execute(query, {"schema": schema, "table": table})]
        if generated_cols:
            print(f"Skipping generated columns for {schema}.{table}: {generated_cols}")
            df = df.drop(columns=[col for col in generated_cols if col in df.columns], errors="ignore")

        # Truncate + append (preserves FK relationships)
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table} RESTART IDENTITY CASCADE;"))
        df.to_sql(table, con=conn, schema=schema, if_exists="append", index=False)

    print(f"Loaded {schema}.{table} ({len(df)} rows)")

# ============================================================
# 4. Main Load Loop
# ============================================================

for csv_name in tables:
    csv_path = os.path.join(DATA_DIR, f"{csv_name}.csv")
    if not os.path.exists(csv_path):
        print(f"Skipped missing file: {csv_path}")
        continue

    schema_table = f"dwh.{csv_name.replace('dwh_', '')}"

    try:
        load_table(schema_table, csv_path)
    except Exception as e:
        print(f"Error loading {schema_table}: {e}")
        raise

print("All DWH tables loaded successfully into Neon PostgreSQL.")
