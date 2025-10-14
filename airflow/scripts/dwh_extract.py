"""
=================================================
ETL1 - DWH Extract Phase
=================================================
"""
import os
import shutil
import kagglehub

print("Starting DWH Extraction Phase...")

# --- Config ---
DATASET_NAME = "alessandrolobello/agri-food-co2-emission-dataset-forecasting-ml"
DATA_DIR = "/opt/airflow/data"
RAW_CSV_PATH = os.path.join(DATA_DIR, "Agrofood_co2_emission.csv")

# --- Ensure output dir ---
os.makedirs(DATA_DIR, exist_ok=True)

# --- Download dataset from KaggleHub ---
path = kagglehub.dataset_download(DATASET_NAME)
print(f"Dataset downloaded to: {path}")

# --- Find CSV file in the downloaded folder ---
source_csv = None
for root, _, files in os.walk(path):
    for f in files:
        if f.endswith(".csv"):
            source_csv = os.path.join(root, f)
            break
    if source_csv:
        break

if not source_csv:
    raise FileNotFoundError("No CSV file found in the Kaggle dataset.")

# --- Copy file to Airflow data folder ---
shutil.copy2(source_csv, RAW_CSV_PATH)
print(f"✅ Raw dataset saved to: {RAW_CSV_PATH}")
print("✅ Extraction phase completed successfully.")
