import sqlite3

def fix_websites_v2():
    db_path = "businesses.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    corrections = [
        (1778, "https://www.craig-international.com"),
        (1797, "https://www.neon-communications.com"),
        (2049, "https://www.pmi.org"),
        (2129, "https://www.munkservices.com"),
        (3051, "https://azcc.gov"),
        (1730, "https://www.agbarr.co.uk"),
        (1728, "https://www.standardlife.co.uk"),
        (1729, "https://www.rbs.co.uk"),
        (1731, "https://www.skyscanner.net"),
        (1732, "https://www.mobunti.co.uk"),
        (1755, "https://www.phoenixrefreshmentgroup.com"),
        (1813, "https://www.ft.com"),
        (1820, "https://www.thefemalelead.com"),
        (1822, "https://www.chanel.com"),
        (1835, "https://www.revolut.com"),
        (1961, "https://www.ted.com"),
        (1962, "https://www.deloitte.com"),
        (1963, "https://www.ibm.com"),
        (2026, "https://www.concentrix.com"),
        (2027, "https://www.marriott.com"),
        (2028, "https://advertising.businessinsider.com"),
        (2029, "https://www.americanexpress.com"),
        (2030, "https://advertising.businessinsider.com"),
        (2031, "https://www.starbucks.com"),
        (2032, "https://www.bankofamerica.com"),
        (2033, "https://www.airbnb.com"),
        (2034, "https://www.wellsfargo.com"),
        (2035, "https://www.inc.com"),
        (2037, "https://www.lockheedmartin.com"),
        (2039, "https://www.bakerhughes.com"),
        (2040, "https://www.halliburton.com"),
        (2041, "https://www.harvard.edu"),
        (2043, "https://www.undp.org"),
        (2044, "https://www.gehealthcare.com"),
        (2045, "https://www.techcrunch.com"),
        (2046, "https://www.blackrock.com"),
        (2047, "https://www.merck.com"),
        (2048, "https://www.colgatepalmolive.com"),
        (2050, "https://www.bain.com"),
        (2051, "https://www.worldbank.org"),
        (2052, "https://www.epam.com"),
        (2053, "https://www.simonsinek.com"),
        (2054, "https://www.visa.com"),
        (2055, "https://www.hbs.edu"),
        (2056, "https://www.time.com"),
        (2057, "https://www.hilton.com"),
        (2058, "https://www.dxc.com"),
        (2059, "https://www.iqvia.com"),
        (2060, "https://www.lilly.com"),
        (2061, "https://www.3m.com"),
        (2063, "https://corporate.target.com"),
        (2064, "https://corporate.mcdonalds.com"),
        (2065, "https://www.aecom.com"),
        (2066, "https://www.mastercard.com"),
        (2067, "https://www.entrepreneur.com"),
        (2068, "https://www.jll.com"),
        (2070, "https://www.fedex.com"),
        (2071, "https://www.gartner.com"),
        (2072, "https://www.ups.com"),
        (2074, "https://www.interestingengineering.com"),
        (2076, "https://www.caterpillar.com"),
        (2077, "https://www.thermofisher.com"),
        (2078, "https://www.nasa.gov"),
        (2079, "https://www.kornferry.com"),
        (2080, "https://www.vmware.com"),
        (2081, "https://www.fortune.com"),
        (2082, "https://www.amd.com"),
        (2083, "https://www.freecodecamp.org"),
        (2085, "https://www.qualcomm.com"),
        (2086, "https://www.jacobs.com"),
        (2087, "https://www.transperfect.com"),
        (2088, "https://www.abbvie.com"),
        (2106, "https://www.cscec.ae")
    ]
    
    updated = 0
    for rec_id, website in corrections:
        cursor.execute("UPDATE businesses SET website = ? WHERE id = ?", (website, rec_id))
        updated += cursor.rowcount
        
    conn.commit()
    conn.close()
    print(f"Updated {updated} websites.")

if __name__ == "__main__":
    fix_websites_v2()
