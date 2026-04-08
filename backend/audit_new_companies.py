import sqlite3
import requests
from concurrent.futures import ThreadPoolExecutor
import os
import sys

# Add backend directory to path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)

DB_PATH = os.path.join(os.path.dirname(backend_dir), "businesses.db")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
}

def check_link(url):
    if not url: return False
    if "linkedin.com" in url: return True
    try:
        r = requests.get(url, timeout=10, headers=HEADERS, verify=False, allow_redirects=True)
        return r.status_code < 400
    except Exception:
        return False

def run_new_companies_audit():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Range: 1600 to 1723
    START_ID = 1600
    
    cursor.execute(f"SELECT COUNT(*) FROM businesses WHERE id >= {START_ID}")
    total_new = cursor.fetchone()[0]
    
    fields = ["industry", "revenue", "employee_count", "website", "linkedin_url", "email", "phone", "address", "city", "state", "registration_date"]
    
    blank_query = " OR ".join([f"({f} IS NULL OR {f} = '' OR {f} = 'N/A')" for f in fields])
    cursor.execute(f"SELECT id, company_name, website FROM businesses WHERE id >= {START_ID} AND ({blank_query})")
    incomplete_rows = cursor.fetchall()
    
    # Check links for ALL new companies
    cursor.execute(f"SELECT id, company_name, website FROM businesses WHERE id >= {START_ID}")
    all_rows = cursor.fetchall()
    websites = [r['website'] for r in all_rows if r['website'] and "linkedin.com" not in r['website']]
    
    print(f"Checking {len(websites)} links for new companies...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(check_link, websites))
        broken_links = [websites[i] for i, res in enumerate(results) if not res]

    # Generate Report
    report = f"""# Audit Report: Newly Added Companies (IDs {START_ID}+)

## Summary
- **Total Newly Added**: {total_new} records.
- **Data Completeness**: {total_new - len(incomplete_rows)} / {total_new} records are fully populated ({((total_new - len(incomplete_rows))/total_new*100):.1f}%).
- **Incomplete Records**: {len(incomplete_rows)} records still have blank fields (being repaired).

## Incomplete Records Details (Top 10)
| ID | Company Name | Missing Fields |
|----|--------------|----------------|
"""
    for row in incomplete_rows[:10]:
        # Check which fields are actually blank for this row
        cursor.execute(f"SELECT * FROM businesses WHERE id = {row['id']}")
        data = cursor.fetchone()
        blanks = [f for f in fields if data[f] is None or data[f] == '' or data[f] == 'N/A']
        report += f"| {row['id']} | {row['company_name']} | {', '.join(blanks)} |\n"

    report += f"""
## Link Functionality (New Companies)
- **Total Links Tested**: {len(websites)}
- **Broken/Unreachable Links**: {len(broken_links)}
"""
    if broken_links:
        report += "\n### Broken Links Sample:\n"
        for link in broken_links[:5]:
            report += f"- {link}\n"
    else:
        report += "\n- **All tested links are functional!**\n"

    report += f"""
## Recent Successes (Last 5 Repairs)
"""
    # Check the log for the most recent successes
    log_path = os.path.join(backend_dir, "master_enrichment_v4.log")
    if os.path.exists(log_path):
        import subprocess
        successes = subprocess.check_output(f"grep 'SUCCESS' {log_path} | tail -n 5", shell=True).decode()
        report += f"```\n{successes}\n```"

    with open(os.path.join(backend_dir, "new_companies_audit.md"), "w") as f:
        f.write(report)
    
    print("Report generated: new_companies_audit.md")
    conn.close()

if __name__ == "__main__":
    run_new_companies_audit()
