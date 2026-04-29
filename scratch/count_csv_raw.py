
import csv
import os
import sys

# Add backend and root to sys.path
root_path = os.getcwd()
backend_path = os.path.join(root_path, "backend")
sys.path.insert(0, backend_path)
sys.path.insert(0, root_path)

from ingest_csvs import normalise_country, normalise_state

def count_csv_england_raw():
    csv_path = "backend/clean_business_records.csv"
    count = 0
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            country = normalise_country(row.get("Country") or "")
            state = normalise_state(row.get("State") or "", country or "")
            if country == "UK" and state == "England":
                count += 1
    print(f"Raw England records in CSV: {count}")

if __name__ == "__main__":
    count_csv_england_raw()
