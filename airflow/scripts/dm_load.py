"""
=================================================
ETL2 - DataMart Load Phase (Neon)
=================================================
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

print(" Starting DataMart Load Phase...")

# --- Connection Config ---
HOST = "ep-red-grass-a1y4mh5d-pooler.ap-southeast-1.aws.neon.tech"
PORT = "5432"
DATABASE = "neondb"
USER = "neondb_owner"
PASSWORD = "npg_3MPZY4FkDGbr"
SSLMODE = "require"

CONNECTION_STRING = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?sslmode={SSLMODE}"
engine = create_engine(CONNECTION_STRING)

# --- Load CSV ---
DATA_DIR = "/opt/airflow/data/datamart"
csv_path = os.path.join(DATA_DIR, "dm_fact_emission_enriched.csv")
df = pd.read_csv(csv_path)

# --- Upload to Neon ---
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE dm.fact_emission_enriched RESTART IDENTITY CASCADE;"))
    df.to_sql("fact_emission_enriched", con=conn, schema="dm", if_exists="append", index=False)

print(f"Loaded {len(df)} rows into dm.fact_emission_enriched")
print("Datamart Load Phase completed successfully!")
