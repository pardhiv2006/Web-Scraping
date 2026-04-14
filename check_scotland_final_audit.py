import sqlite3
import os

DB_PATH = 'businesses.db'
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("PRAGMA table_info(businesses)")
columns = [col[1] for col in cursor.fetchall()]

def is_blank(val):
    if val is None: return True
    s = str(val).strip().lower()
    return s in ["", "none", "-", "null", "undefined", "n/a", "-"] or "-" in s

scot_records = cursor.execute("SELECT * FROM businesses WHERE state = 'SCT'").fetchall()
print(f"Total Scotland records in DB: {len(scot_records)}")

perfect = []
dirty = []

for row in scot_records:
    has_blank = False
    for i, val in enumerate(row):
        if columns[i] in ['id', 'scraped_at', 'status', 'registration_date']: continue
        if is_blank(val):
            has_blank = True
            dirty.append((row[0], columns[i], val))
            break
    if not has_blank:
        perfect.append(row[0])

print(f"Perfect: {len(perfect)}")
print(f"Dirty: {len(dirty)}")
if dirty:
    print("Example dirty records:")
    for d in dirty[:3]:
        print(f"  ID {d[0]}: field {d[1]} is '{d[2]}'")

conn.close()
