'''
=================================================
Milestone 3 - Load Phase

Name  : Irhammaula Ario
Batch : CODA-009-RMT

This script used as the extract module (load.py) in the ETL pipeline where data is extracted using kagglehub.
=================================================
'''

import os
import shutil
import kagglehub

print("Starting data extraction...")

# Read environment variables
dataset_name = "jr2ngb/superstore-data"
raw_csv_output_path = "/opt/airflow/data/P2M3_Irhammaula_Ario_data_raw.csv"

# Define data directory
data_dir = "/opt/airflow/data"

# Ensure the output directory exists
os.makedirs(data_dir, exist_ok=True)

# Download the dataset using kagglehub
path = kagglehub.dataset_download(dataset_name)
print(f"Dataset downloaded to: {path}")

# Find the first CSV file in the dataset folder
source_csv_path_in_output = None
for root, _, files in os.walk(path):
    for file in files:
        if file.endswith(".csv"):
            source_csv_path_in_output = os.path.join(root, file)
            break
    if source_csv_path_in_output:
        break

if not source_csv_path_in_output:
    raise FileNotFoundError("No CSV file found in the downloaded dataset folder.")

print(f"Detected CSV file: {source_csv_path_in_output}")

# Copy to /opt/airflow/data with the expected filename
FULL_FINAL_RAW_CSV_PATH = os.path.join(data_dir, "P2M3_Irhammaula_Ario_data_raw.csv")
shutil.copy2(source_csv_path_in_output, FULL_FINAL_RAW_CSV_PATH)

print(f"File successfully copied to: {FULL_FINAL_RAW_CSV_PATH}")
print("Data extraction completed successfully.")
