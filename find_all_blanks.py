import sqlite3

db_path = "businesses.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("SELECT * FROM businesses")
rows = cursor.fetchall()

blank_counts = {}
rows_with_blanks = []

columns = rows[0].keys()

for row in rows:
    blanks = []
    for col in columns:
        val = row[col]
        # Check for NULL, empty string, or placeholders like 'N/A', 'Unknown'
        if val is None or str(val).strip() == "" or str(val).strip().lower() in ['n/a', 'unknown', 'not found', 'placeholder']:
            blanks.append(col)
    
    if blanks:
        rows_with_blanks.append((row['id'], row['company_name'], blanks))
        for b in blanks:
            blank_counts[b] = blank_counts.get(b, 0) + 1

print(f"Total rows with blanks: {len(rows_with_blanks)}")
for col, count in blank_counts.items():
    print(f"  {col}: {count} blanks")

print("\nExamples of rows with blanks:")
for r in rows_with_blanks[:10]:
    print(f"ID {r[0]} ({r[1]}): {', '.join(r[2])}")
