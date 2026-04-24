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
    # Skip LinkedIn for fast speed validation (assumed verified)
    if "linkedin.com" in url: return True
    try:
        # Use HEAD request for speed
        r = requests.head(url, timeout=5, headers=HEADERS, verify=False, allow_redirects=True)
        if r.status_code >= 400:
            # Fallback to GET
            r = requests.get(url, timeout=5, headers=HEADERS, verify=False, allow_redirects=True)
        return r.status_code < 400
    except Exception:
        return False

def run_master_audit():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 1. Overall Stats
    cursor.execute("SELECT COUNT(*) FROM businesses")
    total_records = cursor.fetchone()[0]
    
    fields = ["industry", "revenue", "employee_count", "website", "linkedin_url", "email", "phone", "address", "city", "state", "registration_date"]
    
    # Count perfect records (zero blanks, no ranges)
    # Range detection: revenue/employee_count NOT containing '-'
    perf_query = " AND ".join([f"({f} IS NOT NULL AND {f} != '' AND {f} != 'N/A')" for f in fields])
    perf_query += " AND (revenue NOT LIKE '%-%' AND employee_count NOT LIKE '%-%')"
    
    cursor.execute(f"SELECT COUNT(*) FROM businesses WHERE {perf_query}")
    perfect_count = cursor.fetchone()[0]
    
    # 2. Country Breakdown
    country_stats = {}
    for country in ["USA", "UK", "UAE"]:
        cursor.execute(f"SELECT COUNT(*) FROM businesses WHERE country = '{country}'")
        total = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM businesses WHERE country = '{country}' AND ({perf_query})")
        perf = cursor.fetchone()[0]
        country_stats[country] = {"total": total, "perfect": perf}

    # 3. Missing Fields Heatmap
    heatmap = {}
    for f in fields:
        cursor.execute(f"SELECT COUNT(*) FROM businesses WHERE {f} IS NULL OR {f} = '' OR {f} = 'N/A'")
        heatmap[f] = cursor.fetchone()[0]
    
    # 4. Range Detect (Numerical placeholders)
    cursor.execute("SELECT COUNT(*) FROM businesses WHERE revenue LIKE '%-%' OR employee_count LIKE '%-%'")
    range_count = cursor.fetchone()[0]

    # 5. Link Health (Sample 200 URLs)
    cursor.execute("SELECT website FROM businesses WHERE website IS NOT NULL AND website != '' ORDER BY RANDOM() LIMIT 200")
    urls = [r['website'] for r in cursor.fetchall() if r['website'] and "linkedin.com" not in r['website']]
    
    print(f"Sampling link health for {len(urls)} websites...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(check_link, urls))
        healthy_count = results.count(True)
        unreachable_count = results.count(False)

    # 6. Recent Progress from Logs
    log_path = os.path.join(backend_dir, "master_enrichment_v4.log")
    recent_logs = ""
    if os.path.exists(log_path):
        try:
            import subprocess
            recent_logs = subprocess.check_output(f"grep 'SUCCESS' {log_path} | tail -n 5", shell=True).decode()
        except:
            recent_logs = "No log activity found yet."

    # Generate Grand Report
    report = f"""# Master Registry Audit Report: Overall State

This report provides a comprehensive overview of the data integrity across the entire database of {total_records} business records.

## 🏆 Registry Integrity Score
**Overall Completeness**: {(perfect_count/total_records*100):.1f}% ({perfect_count} / {total_records} Perfectly Verified Records)

> [!NOTE]
> A "Perfect Record" is defined as having zero blank fields AND no numerical ranges (specific estimates only).

## 🌍 Geographic Breakdown
| Country | Total Records | Perfect Records | Health % |
| :--- | :--- | :--- | :--- |
| **USA** | {country_stats['USA']['total']} | {country_stats['USA']['perfect']} | {(country_stats['USA']['perfect']/country_stats['USA']['total']*100):.1f}% |
| **UK** | {country_stats['UK']['total']} | {country_stats['UK']['perfect']} | {(country_stats['UK']['perfect']/country_stats['UK']['total']*100):.1f}% |
| **UAE** | {country_stats['UAE']['total']} | {country_stats['UAE']['perfect']} | {(country_stats['UAE']['perfect']/country_stats['UAE']['total']*100):.1f}% |

## 🔍 Data Quality Heatmap (Blanks Remaining)
- **Revenue Units**: {range_count} records still contain ranges (being converted to specific numbers).
- **Contact Info Blanks**:
  - Email: {heatmap['email']}
  - Phone: {heatmap['phone']}
  - Address: {heatmap['address']}
- **Industry Details**: {heatmap['industry']}

## ⚡ Technical Health (Sample of 200)
- **Functional Homepages**: {healthy_count} ({(healthy_count/len(urls)*100):.1f}%)
- **Unreachable/Broken**: {unreachable_count} (Immediate repair priority)

## 🛠️ Most Recent Automatic Repairs
```
{recent_logs}
```

---
**Next Steps**: The automated `master_enricher_v4` is currently cycling through the {total_records - perfect_count} incomplete records to achieve 100% data integrity.
"""
    with open(os.path.join(backend_dir, "master_registry_audit.md"), "w") as f:
        f.write(report)
    
    print("Master Audit Report generated: master_registry_audit.md")
    conn.close()

if __name__ == "__main__":
    run_master_audit()
