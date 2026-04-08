import sqlite3
import os

DB_PATH = "/Users/rajeshchinthala/Desktop/Pardhiv_Intern/Cyborg/application-6/businesses.db"

def final_fix():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    fixes = [
        (6, "$5.0 Million", "12"),
        (10, "$8.5 Million", "15"),
        (13, "$12.7 Million", "250"),
        (1695, "$12.0 Million", "42"),
        (1714, "$120.0 Million", "500")
    ]
    
    for rid, rev, emp in fixes:
        cursor.execute("UPDATE businesses SET revenue = ?, employee_count = ? WHERE id = ?", (rev, emp, rid))
    
    conn.commit()
    conn.close()
    print("Final 5 records updated successfully.")

if __name__ == "__main__":
    final_fix()
