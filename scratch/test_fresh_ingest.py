
import os
import sys
import sqlite3
import shutil

# Copy DB to temp
shutil.copy2("businesses.db", "temp_businesses.db")

# Setup path
root_path = os.getcwd()
backend_path = os.path.join(root_path, "backend")
sys.path.insert(0, backend_path)
sys.path.insert(0, root_path)

# Mock database.DATABASE_URL to use temp_businesses.db
import database
database.DATABASE_URL = "sqlite:///temp_businesses.db"
# We need to re-create engine and SessionLocal if we want to use them, 
# but ingest_csvs imports them.

# Instead of complex mocking, let's just run a script that clears businesses 
# and runs ingest_csvs on the temp db.

def test_fresh_ingest():
    conn = sqlite3.connect("temp_businesses.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM businesses")
    conn.commit()
    conn.close()
    
    # Now run ingest_csvs pointing to temp_businesses.db
    # We'll just manually call the logic
    from ingest_csvs import ingest_file
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    
    engine = create_engine("sqlite:///temp_businesses.db")
    Session = sessionmaker(bind=engine)
    db = Session()
    
    print("Ingesting into fresh temp DB...")
    ingest_file("backend/clean_business_records.csv", db)
    db.close()
    
    # Check count for England
    conn = sqlite3.connect("temp_businesses.db")
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM businesses WHERE upper(country) = 'UK' AND upper(state) = 'ENGLAND'")
    count = cursor.fetchone()[0]
    print(f"Total England records after fresh ingest: {count}")
    conn.close()

if __name__ == "__main__":
    test_fresh_ingest()
