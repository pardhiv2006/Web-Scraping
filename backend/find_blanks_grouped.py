import sqlite3
from collections import defaultdict

conn = sqlite3.connect('businesses.db')
cursor = conn.cursor()
cursor.execute("SELECT * FROM businesses")
columns = [desc[0] for desc in cursor.description]
rows = cursor.fetchall()
empty_counts = defaultdict(int)

for row in rows:
    for col_idx, val in enumerate(row):
        col_name = columns[col_idx]
        if val is None:
            empty_counts[col_name] += 1
        elif isinstance(val, str) and not val.strip():
            empty_counts[col_name] += 1
        elif isinstance(val, str) and val.strip().lower() in ['n/a', '-', 'none', 'unknown', 'null', 'not available', 'to be updated', '']:
            empty_counts[col_name] += 1

print("--- Blank Fields Summary ---")
for col, count in empty_counts.items():
    print(f"{col}: {count}")

