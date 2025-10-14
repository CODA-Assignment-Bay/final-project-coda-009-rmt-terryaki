"""
=================================================
ETL1 - DWH Transform Phase (Global Scope)
Name  : Irhammaula Ario
Batch : CODA-009-RMT

Tasks:
1. Normalize raw CSV into DIM and FACT tables
2. Ensure consistent snake_case naming
3. Enrich country info (ISO3, region)
4. Classify emission sources using full mapping
5. Keep ALL countries globally (filter in ETL2)
=================================================
"""

import pandas as pd
import os
from datetime import datetime
import pycountry
import pycountry_convert as pc

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------
DATA_DIR = "/opt/airflow/data"
RAW_FILE = os.path.join(DATA_DIR, "Agrofood_co2_emission.csv")
OUTPUT_DIR = os.path.join(DATA_DIR, "dwh")
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("🚀 Starting DWH Transformation Phase (Global Scope)...")

# ------------------------------------------------------------
# READ RAW DATA
# ------------------------------------------------------------
df = pd.read_csv(RAW_FILE)

# --- Clean column names ---
df.columns = (
    df.columns.str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("°", "")
    .str.replace("__", "_")
)

# ------------------------------------------------------------
# DEFINE METADATA
# ------------------------------------------------------------
meta_cols = [
    'area', 'year',
    'forestland', 'net_forest_conversion', 'on-farm_electricity_use',
    'on-farm_energy_use', 'rural_population', 'urban_population',
    'total_population_-_male', 'total_population_-_female',
    'average_temperature_c', 'total_emission'
]
emission_cols = [c for c in df.columns if c not in meta_cols]
now = datetime.now()

# ------------------------------------------------------------
# DETAILED EMISSION SOURCE CLASSIFICATION
# ------------------------------------------------------------
source_info = {
    "savanna_fires": ("Non_Industrial", "Emissions from fires in savanna ecosystems."),
    "forest_fires": ("Non_Industrial", "Emissions from fires in forested areas."),
    "crop_residues": ("Non_Industrial", "Emissions from burning or decomposing leftover plant material after crop harvesting."),
    "rice_cultivation": ("Non_Industrial", "Emissions from methane released during rice cultivation."),
    "drained_organic_soils_(co2)": ("Non_Industrial", "Emissions from carbon dioxide released when draining organic soils."),
    "pesticides_manufacturing": ("Industrial", "Emissions from the production of pesticides."),
    "food_transport": ("Non_Industrial", "Emissions from transporting food products."),
    "forestland": ("Non_Industrial", "Land covered by forests (emissions related to forest carbon storage changes)."),
    "net_forest_conversion": ("Non_Industrial", "Change in forest area due to deforestation and afforestation."),
    "food_household_consumption": ("Non_Industrial", "Emissions from food consumption at the household level."),
    "food_retail": ("Non_Industrial", "Emissions from the operation of retail establishments selling food."),
    "on-farm_electricity_use": ("Non_Industrial", "Electricity consumption on farms."),
    "food_packaging": ("Industrial", "Emissions from the production and disposal of food packaging materials."),
    "agrifood_systems_waste_disposal": ("Non_Industrial", "Emissions from waste disposal in the agrifood system."),
    "food_processing": ("Industrial", "Emissions from processing food products."),
    "fertilizers_manufacturing": ("Industrial", "Emissions from the production of fertilizers."),
    "ippu": ("Industrial", "Emissions from industrial processes and product use."),
    "manure_applied_to_soils": ("Non_Industrial", "Emissions from applying animal manure to agricultural soils."),
    "manure_left_on_pasture": ("Non_Industrial", "Emissions from animal manure on pasture or grazing land."),
    "manure_management": ("Non_Industrial", "Emissions from managing and treating animal manure."),
    "fires_in_organic_soils": ("Non_Industrial", "Emissions from fires in organic soils."),
    "fires_in_humid_tropical_forests": ("Non_Industrial", "Emissions from fires in humid tropical forests."),
    "on-farm_energy_use": ("Non_Industrial", "Energy consumption on farms.")
}

def get_source_category(source):
    return source_info.get(source, ("Non_Industrial", "Unclassified source"))[0]

def get_source_desc(source):
    return source_info.get(source, ("Non_Industrial", f"Emissions from {source.replace('_', ' ')}."))[1]

# ------------------------------------------------------------
# COUNTRY ENRICHMENT (ISO3 + REGION)
# ------------------------------------------------------------
def get_iso3(country):
    try:
        return pycountry.countries.lookup(country).alpha_3
    except:
        return None

def get_region(country):
    try:
        iso = pycountry.countries.lookup(country).alpha_2
        continent = pc.country_alpha2_to_continent_code(iso)
        mapping = {
            "AS": "Asia", "EU": "Europe", "AF": "Africa",
            "NA": "North America", "SA": "South America", "OC": "Oceania"
        }
        return mapping.get(continent, "Unknown")
    except:
        return "Unknown"

# ------------------------------------------------------------
# DIM COUNTRY
# ------------------------------------------------------------
dim_country = (
    df[['area']]
    .drop_duplicates()
    .rename(columns={'area': 'country_name'})
    .reset_index(drop=True)
)
dim_country.insert(0, 'country_id', range(1, len(dim_country) + 1))
dim_country['iso3'] = dim_country['country_name'].apply(get_iso3)
dim_country['region'] = dim_country['country_name'].apply(get_region)
dim_country['created_at'] = now
dim_country['updated_at'] = now
dim_country.to_csv(os.path.join(OUTPUT_DIR, "dwh_dim_country.csv"), index=False)
print(f"✅ dim_country created with {len(dim_country)} rows")

# ============================================================
# DIM_TIME
# ============================================================
import pandas as pd
from datetime import datetime

dim_time = pd.DataFrame({
    "year": list(range(1990, 2021)),
    "created_at": [datetime.now()] * 31,
    "updated_at": [datetime.now()] * 31
})

dim_time.to_csv("/opt/airflow/data/dwh/dwh_dim_time.csv", index=False)
print(f"✅ Dimension Time saved with {len(dim_time)} records (without decade column).")


# ------------------------------------------------------------
# DIM EMISSION SOURCE
# ------------------------------------------------------------
dim_emission_source = pd.DataFrame({
    'source_id': range(1, len(emission_cols) + 1),
    'source_name': emission_cols
})
dim_emission_source['source_category'] = dim_emission_source['source_name'].apply(get_source_category)
dim_emission_source['description'] = dim_emission_source['source_name'].apply(get_source_desc)
dim_emission_source['created_at'] = now
dim_emission_source['updated_at'] = now
dim_emission_source.to_csv(os.path.join(OUTPUT_DIR, "dwh_dim_emission_source.csv"), index=False)
print(f"✅ dim_emission_source created with {len(dim_emission_source)} rows")

# ------------------------------------------------------------
# FACT EMISSION
# ------------------------------------------------------------
id_vars = ['area', 'year', 'total_emission', 'average_temperature_c']
melted = df.melt(
    id_vars=id_vars,
    value_vars=emission_cols,
    var_name='source_name',
    value_name='emission_value'
)
melted = melted.rename(columns={'area': 'country_name'})

fact_emission = (
    melted.merge(dim_country[['country_id', 'country_name']], on='country_name', how='left')
          .merge(dim_emission_source[['source_id', 'source_name']], on='source_name', how='left')
)[['country_id', 'source_id', 'year', 'emission_value', 'total_emission', 'average_temperature_c']]

# 🩹 Fill missing numeric values to avoid NOT NULL constraint violations
fact_emission['emission_value'] = fact_emission['emission_value'].fillna(0)
fact_emission['total_emission'] = fact_emission['total_emission'].fillna(0)

fact_emission['created_at'] = now
fact_emission['updated_at'] = now
fact_emission.insert(0, 'fact_id', range(1, len(fact_emission) + 1))

# Save cleaned fact table
fact_emission.to_csv(os.path.join(OUTPUT_DIR, "dwh_fact_emission.csv"), index=False)
print(f"✅ fact_emission created with {len(fact_emission)} rows (nulls filled with 0)")

# ------------------------------------------------------------
# FACT POPULATION
# ------------------------------------------------------------
fact_population = (
    df.merge(dim_country, left_on='area', right_on='country_name', how='left')
)[[
    'country_id', 'year', 'rural_population', 'urban_population',
    'total_population_-_male', 'total_population_-_female'
]].rename(columns={
    'total_population_-_male': 'population_male',
    'total_population_-_female': 'population_female'
})

fact_population['created_at'] = now
fact_population['updated_at'] = now
fact_population.insert(0, 'fact_id', range(1, len(fact_population) + 1))
fact_population.to_csv(os.path.join(OUTPUT_DIR, "dwh_fact_population.csv"), index=False)
print(f"✅ fact_population created with {len(fact_population)} rows")

print("🎉 Transformation phase completed successfully — all sources categorized and countries retained globally.")
