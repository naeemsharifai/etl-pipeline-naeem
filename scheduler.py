import schedule
import time
import subprocess
from datetime import datetime

def run_etl():
    print(f"\n[INFO] Running ETL Pipeline at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    subprocess.run(["python", "etl_pipeline.py"])

# Schedule it to run every day at 1:00 PM
schedule.every(1).day.at("13:00").do(run_etl)

print("[INFO] Scheduler started. Press Ctrl+C to stop.")
while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute