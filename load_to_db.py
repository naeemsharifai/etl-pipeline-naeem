import pandas as pd
import json
from pymongo import MongoClient

# Load DB config
with open("config/db_config.json", "r") as f:
    config = json.load(f)

# Connect using MongoDB Atlas URI
client = MongoClient(config['uri'])
db = client[config['database']]
collection = db[config['collection']]

# Load cleaned CSV
df = pd.read_csv("output/final_cleaned_data.csv")

# Convert DataFrame to JSON-like dict and insert
data = df.to_dict(orient='records')
collection.delete_many({})  # Optional: clear existing data
collection.insert_many(data)

print("✅ Data loaded into MongoDB Atlas!")