import sqlite3

conn = sqlite3.connect('businesses.db')
cursor = conn.cursor()
cursor.execute("SELECT * FROM businesses")
columns = [desc[0] for desc in cursor.description]
rows = cursor.fetchall()
empty_count = 0
for row in rows:
    for col_idx, val in enumerate(row):
        col_name = columns[col_idx]
        if val is None:
            if empty_count < 20: print(f"Row {row[0]} ({row[1]}): {col_name} is None")
            empty_count += 1
        elif isinstance(val, str) and not val.strip():
            if empty_count < 20: print(f"Row {row[0]} ({row[1]}): {col_name} is empty string '{val}'")
            empty_count += 1
        elif isinstance(val, str) and val.strip().lower() in ['n/a', '-', 'none', 'unknown', 'null', 'pending', 'not available', 'to be updated']:
            if empty_count < 20: print(f"Row {row[0]} ({row[1]}): {col_name} is placeholder '{val}'")
            empty_count += 1

print(f"TOTAL BLANKS FOUND: {empty_count}")
