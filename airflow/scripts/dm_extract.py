"""
=================================================
ETL2 - Datamart Extract Phase
Extracts required datasets from Neon DWH (schema = dwh)
=================================================
"""
import pandas as pd
from sqlalchemy import create_engine
import os

print("Starting Datamart Extract Phase...")

# --- 1. Connect to Neon DWH ---
HOST = "ep-red-grass-a1y4mh5d-pooler.ap-southeast-1.aws.neon.tech"
PORT = "5432"
DATABASE = "neondb"
USER = "neondb_owner"
PASSWORD = "npg_3MPZY4FkDGbr"
SSLMODE = "require"

engine = create_engine(
    f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?sslmode={SSLMODE}"
)

# --- 2. Extract required tables ---
queries = {
    "dim_country": "SELECT * FROM dwh.dim_country;",
    "dim_time": "SELECT * FROM dwh.dim_time;",
    "dim_emission_source": "SELECT * FROM dwh.dim_emission_source;",
    "fact_emission": "SELECT * FROM dwh.fact_emission;",
    "fact_population": "SELECT * FROM dwh.fact_population;"
}

DATA_DIR = "/opt/airflow/data/datamart_raw"
os.makedirs(DATA_DIR, exist_ok=True)

for name, query in queries.items():
    print(f"Extracting {name} ...")
    df = pd.read_sql(query, con=engine)
    df.to_csv(f"{DATA_DIR}/{name}.csv", index=False)
    print(f"Extracted {name} ({len(df)} rows)")

print("Datamart Extract completed successfully.")
