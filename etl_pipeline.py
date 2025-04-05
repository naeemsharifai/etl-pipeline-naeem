import pandas as pd
import json
import os

# Set correct file paths based on your structure
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

CSV_FILE = os.path.join(DATA_DIR, "sample_data.csv")
JSON_FILE = os.path.join(DATA_DIR, "sample_weather.json")
GOOGLE_SHEET_FILE = os.path.join(DATA_DIR, "google_sheet_sample.csv")

def extract_csv(file_path):
    """Extracts data from a CSV file."""
    try:
        df = pd.read_csv(file_path)
        print(f"[INFO] Loaded CSV data from {file_path}")
        return df
    except Exception as e:
        print(f"[ERROR] Failed to load CSV from {file_path}: {e}")
        return pd.DataFrame()

def extract_json(file_path):
    """Extracts data from a JSON file."""
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        df = pd.DataFrame(data)
        print(f"[INFO] Loaded JSON data from {file_path}")
        return df
    except Exception as e:
        print(f"[ERROR] Failed to load JSON from {file_path}: {e}")
        return pd.DataFrame()

# Load data
df_csv = extract_csv(CSV_FILE)
df_json = extract_json(JSON_FILE)
df_google = extract_csv(GOOGLE_SHEET_FILE)

# Preview extracted data
print("\n--- sample_data.csv ---")
print(df_csv.head())
print("\n--- sample_weather.json ---")
print(df_json.head())
print("\n--- google_sheet_sample.csv ---")
print(df_google.head())

# Data Cleaning and Preprocessing

from datetime import datetime

def clean_dataframe(df, source_name):
    print(f"\n[INFO] Cleaning data from {source_name}...")

    # Drop duplicates
    df.drop_duplicates(inplace=True)

    # Handle missing values
    df.dropna(how='all', inplace=True)  # Drop rows where all values are NaN

    # Convert date column (if it exists)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')  # ISO 8601

    # Convert Fahrenheit to Celsius if applicable
    if 'temperature_f' in df.columns:
        df['temperature_c'] = ((df['temperature_f'] - 32) * 5.0/9.0).round(1)
        df.drop(columns=['temperature_f'], inplace=True)

    # Rename columns to consistent lowercase snake_case
    df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]

    print(f"[INFO] Cleaned {source_name} ✅\n")
    return df

# Clean each dataset
df_csv_cleaned = clean_dataframe(df_csv, "sample_data.csv")
df_json_cleaned = clean_dataframe(df_json, "sample_weather.json")
df_google_cleaned = clean_dataframe(df_google, "google_sheet_sample.csv")

# Preview cleaned data
print("--- Cleaned CSV ---")
print(df_csv_cleaned.head())

print("\n--- Cleaned JSON ---")
print(df_json_cleaned.head())

print("\n--- Cleaned Google Sheet CSV ---")
print(df_google_cleaned.head())


# Feature Engineering: Create a new feature weather_impact_score
# it shows how severe the weather is

def add_weather_impact_score(df, source_name):
    if all(col in df.columns for col in ['temperature_c', 'humidity', 'wind_speed_kmph']):
        df['weather_impact_score'] = (
            df['temperature_c'] * 0.4 +
            df['humidity'] * 0.3 +
            df['wind_speed_kmph'] * 0.3
        ).round(2)
        print(f"[INFO] Added weather_impact_score to {source_name}")
    else:
        print(f"[WARN] Skipped weather_impact_score for {source_name} — required columns missing")
    return df

# Apply feature engineering
df_csv_fe = add_weather_impact_score(df_csv_cleaned.copy(), "CSV data")
df_json_fe = add_weather_impact_score(df_json_cleaned.copy(), "JSON data")
df_google_fe = add_weather_impact_score(df_google_cleaned.copy(), "Google Sheet data")

# Preview result
print("\n--- Feature Engineered: CSV ---")
print(df_csv_fe.head())

print("\n--- Feature Engineered: JSON ---")
print(df_json_fe.head())

print("\n--- Feature Engineered: Google Sheet ---")
print(df_google_fe.head())


# Save Final Cleaned Data

# Merge datasets on 'date' (inner join on common rows)
def merge_datasets(df1, df2, df3):
    try:
        merged = pd.merge(df1, df2, on="date", how="outer", suffixes=('_csv', '_json'))
        merged = pd.merge(merged, df3, on="date", how="outer", suffixes=('', '_google'))
        print(f"[INFO] Merged all datasets on 'date'")
        return merged
    except Exception as e:
        print(f"[ERROR] Could not merge datasets: {e}")
        return pd.DataFrame()

# Merge and save final data
final_df = merge_datasets(df_csv_fe, df_json_fe, df_google_fe)

# Fill any remaining NaNs with reasonable defaults (optional)
final_df.fillna({'temperature_c': 0, 'humidity': 0, 'wind_speed_kmph': 0, 'weather_impact_score': 0}, inplace=True)

# Save to output/final_cleaned_data.csv
OUTPUT_PATH = os.path.join(BASE_DIR, "output/final_cleaned_data.csv")
try:
    final_df.to_csv(OUTPUT_PATH, index=False)
    print(f"[SUCCESS] Final cleaned data saved to: {OUTPUT_PATH}")
except Exception as e:
    print(f"[ERROR] Failed to save cleaned data: {e}")
