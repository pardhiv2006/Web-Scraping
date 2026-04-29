import sqlite3

def final_website_normalization():
    db_path = "businesses.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # List of IDs for the 174 records
    target_ids = [1728,1729,1730,1731,1732,1740,1746,1751,1752,1754,1755,1757,1758,1760,1761,1762,1763,1764,1765,1766,1767,1768,1769,1770,1772,1773,1774,1775,1778,1783,1784,1787,1788,1789,1790,1791,1794,1796,1797,1798,1800,1801,1803,1804,1807,1813,1816,1820,1822,1826,1835,1839,1842,1847,1849,1857,1861,1872,1874,1883,1889,1890,1893,1899,1907,1926,1928,1931,1934,1936,1947,1949,1952,1957,1958,1959,1960,1961,1962,1963,2026,2027,2028,2029,2030,2031,2032,2033,2034,2035,2036,2037,2039,2040,2041,2042,2043,2044,2045,2046,2047,2048,2049,2050,2051,2052,2053,2054,2055,2056,2057,2058,2059,2060,2061,2062,2063,2064,2065,2066,2067,2068,2069,2070,2071,2072,2073,2074,2075,2076,2077,2078,2079,2080,2081,2082,2083,2085,2086,2087,2088,2106,2121,2127,2128,2129,2133,2140,2147,2151,3042,3043,3047,3048,3049,3050,3051,3052,3053,3054,3055,3056,3058,3062]
    
    # Specific final corrections
    final_corrections = {
        1789: "https://www.gov.uk/government/organisations/companies-house",
        3042: "https://www.vesselsvalue.com",
        3051: "https://azcc.gov",
        1961: "https://www.ted.com",
        1962: "https://www.deloitte.com",
        1963: "https://www.ibm.com",
        2026: "https://www.concentrix.com",
        2027: "https://www.marriott.com",
        2029: "https://www.americanexpress.com",
        2031: "https://www.starbucks.com",
        2032: "https://www.bankofamerica.com",
        2033: "https://www.airbnb.com",
        2034: "https://www.wellsfargo.com",
        2035: "https://www.inc.com",
        2037: "https://www.lockheedmartin.com",
        2039: "https://www.bakerhughes.com",
        2040: "https://www.halliburton.com",
        2041: "https://www.harvard.edu",
        2043: "https://www.undp.org",
        2044: "https://www.gehealthcare.com",
        2045: "https://www.techcrunch.com",
        2046: "https://www.blackrock.com",
        2047: "https://www.merck.com",
        2048: "https://www.colgatepalmolive.com",
        2050: "https://www.bain.com",
        2051: "https://www.worldbank.org",
        2052: "https://www.epam.com",
        2053: "https://www.simonsinek.com",
        2056: "https://www.time.com",
        2057: "https://www.hilton.com",
        2058: "https://www.dxc.com",
        2059: "https://www.iqvia.com",
        2060: "https://www.lilly.com",
        2061: "https://www.3m.com",
        2065: "https://www.aecom.com",
        2066: "https://www.mastercard.com",
        2067: "https://www.entrepreneur.com",
        2068: "https://www.jll.com",
        2070: "https://www.fedex.com",
        2071: "https://www.gartner.com",
        2072: "https://www.ups.com",
        2074: "https://www.interestingengineering.com",
        2076: "https://www.caterpillar.com",
        2077: "https://www.thermofisher.com",
        2078: "https://www.nasa.gov",
        2079: "https://www.kornferry.com",
        2080: "https://www.vmware.com",
        2081: "https://www.fortune.com",
        2082: "https://www.amd.com",
        2083: "https://www.freecodecamp.org",
        2085: "https://www.qualcomm.com",
        2086: "https://www.jacobs.com",
        2087: "https://www.transperfect.com",
        2088: "https://www.abbvie.com"
    }
    
    updated = 0
    
    # 1. Apply final corrections
    for rec_id, website in final_corrections.items():
        cursor.execute("UPDATE businesses SET website = ? WHERE id = ?", (website, rec_id))
        updated += cursor.rowcount
        
    # 2. Add https:// to any that are missing it among the target 174
    cursor.execute(f"SELECT id, website FROM businesses WHERE id IN ({','.join(map(str, target_ids))})")
    rows = cursor.fetchall()
    for rec_id, website in rows:
        if website and not website.startswith('http'):
            new_website = 'https://www.' + website if not website.startswith('www.') else 'https://' + website
            cursor.execute("UPDATE businesses SET website = ? WHERE id = ?", (new_website, rec_id))
            updated += cursor.rowcount
            
    conn.commit()
    conn.close()
    print(f"Final normalization complete. Total updates: {updated}")

if __name__ == "__main__":
    final_website_normalization()
