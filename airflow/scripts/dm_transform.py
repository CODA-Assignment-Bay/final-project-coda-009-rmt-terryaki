"""
=================================================
ETL2 - DataMart Transform Phase
Unified Datamart: dm_fact_emission_enriched
=================================================
"""

import pandas as pd
import os
import re
from datetime import datetime
from sqlalchemy import create_engine

# --- Config ---
DATA_DIR = "/opt/airflow/data/dwh"
OUTPUT_DIR = "/opt/airflow/data/datamart"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("🚀 Starting DataMart Transformation Phase...")

# --- Database Connection (for extraction from DWH) ---
HOST = "ep-red-grass-a1y4mh5d-pooler.ap-southeast-1.aws.neon.tech"
PORT = "5432"
DATABASE = "neondb"
USER = "neondb_owner"
PASSWORD = "npg_3MPZY4FkDGbr"
SSLMODE = "require"

CONNECTION_STRING = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?sslmode={SSLMODE}"
engine = create_engine(CONNECTION_STRING)

# ------------------------------------------------------------
# 1. LOAD DWH TABLES
# ------------------------------------------------------------
dim_country = pd.read_sql("SELECT * FROM dwh.dim_country", con=engine)
dim_emission_source = pd.read_sql("SELECT * FROM dwh.dim_emission_source", con=engine)
dim_time = pd.read_sql("SELECT * FROM dwh.dim_time", con=engine)
fact_emission = pd.read_sql("SELECT * FROM dwh.fact_emission", con=engine)
fact_population = pd.read_sql("SELECT * FROM dwh.fact_population", con=engine)

print(f"✅ Loaded DWH tables: {len(fact_emission)} emission rows, {len(fact_population)} population rows")

# ------------------------------------------------------------
# 2. CLEAN DUPLICATE METADATA COLUMNS
# ------------------------------------------------------------
for df_ in [dim_country, dim_emission_source, dim_time, fact_emission, fact_population]:
    df_.drop(columns=["created_at", "updated_at"], errors="ignore", inplace=True)

# ------------------------------------------------------------
# 3. MERGE DIMENSIONS INTO ONE ENRICHED FACT TABLE
# ------------------------------------------------------------
dm_fact = (
    fact_emission
    .merge(dim_country, on="country_id", how="left")
    .merge(dim_emission_source, on="source_id", how="left")
    .merge(dim_time, on="year", how="left")
    .merge(fact_population, on=["country_id", "year"], how="left")
)

# ------------------------------------------------------------
# 4. FEATURE ENGINEERING & METRICS
# ------------------------------------------------------------
# Fill missing numerics
for col in ["emission_value", "total_emission", "average_temperature_c"]:
    if col in dm_fact.columns:
        dm_fact[col] = dm_fact[col].fillna(0)

# Derived metrics
dm_fact["population_total"] = (
    dm_fact[["rural_population", "urban_population", "population_male", "population_female"]]
    .fillna(0)
    .sum(axis=1)
)

dm_fact["emission_per_capita"] = dm_fact["total_emission"] / dm_fact["population_total"].replace(0, pd.NA)
dm_fact["emission_per_area_source"] = dm_fact["emission_value"] / dm_fact["population_total"].replace(0, pd.NA)

# Add timestamps
now = datetime.now()
dm_fact["created_at"] = now
dm_fact["updated_at"] = now

# ------------------------------------------------------------
# 5. CLEAN COUNTRY NAMES
# ------------------------------------------------------------
def clean_country_name(name):
    if pd.isna(name):
        return name
    # Remove symbols: ' , ( )
    return re.sub(r"[',()]", "", str(name)).strip()

dm_fact["country_name"] = dm_fact["country_name"].apply(clean_country_name)

# ------------------------------------------------------------
# 6. FINAL COLUMN ORDER
# ------------------------------------------------------------
final_cols = [
    "country_id", "country_name", "iso3", "region",
    "source_id", "source_name", "source_category", "description",
    "year",
    "emission_value", "total_emission", "average_temperature_c",
    "rural_population", "urban_population", "population_male", "population_female",
    "population_total", "emission_per_capita", "emission_per_area_source",
    "created_at", "updated_at"
]

dm_fact = dm_fact[final_cols]

# ------------------------------------------------------------
# 7. SAVE OUTPUT
# ------------------------------------------------------------
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "dm_fact_emission_enriched.csv")
dm_fact.to_csv(OUTPUT_FILE, index=False)
print(f"✅ DataMart saved to {OUTPUT_FILE} with {len(dm_fact)} records")

print("🎉 DataMart Transformation Phase completed successfully!")
