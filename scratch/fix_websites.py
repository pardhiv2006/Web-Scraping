import sqlite3

def fix_websites():
    db_path = "businesses.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    corrections = [
        (1958, "https://www.amazon.com"),
        (1959, "https://www.linkedin.com"),
        (1960, "https://www.microsoft.com"),
        (1761, "https://abc.xyz"),
        (1773, "https://about.meta.com"),
        (1816, "https://www.gsk.com"),
        (1800, "https://www.abbvie.com"),
        (1794, "https://www.corning.com"),
        (1790, "https://www.stagecoachgroup.com"),
        (1740, "https://www.walgreens.com/topic/duane-reade.jsp"),
        (1757, "https://www.arangodb.com"),
        (1758, "https://www.fiskerinc.com"),
        (1746, "https://www.humanhire.com"),
        (1751, "https://liscardbusinesscentre.com"),
        (1760, "https://www.broadcom.com"),
        (1769, "https://www.southwest.com"),
        (1770, "https://www.dell.com"),
        (1774, "https://www.netflix.com"),
        (1775, "https://www.salesforce.com"),
        (1784, "https://www.masaood.com"),
        (1787, "https://dce-uae.com"), # Based on search for DCE Contracting LLC Dubai
        (1796, "https://www.aon.com"),
        (1801, "https://www.aaa.com"),
        (1803, "https://www.eyemailinc.com"),
        (1804, "https://www.cpuinc.com"),
        (1826, "https://www.bbc.co.uk"),
        (2036, "https://www.cnbc.com"),
        (2042, "https://www.cnn.com/business"),
        (2069, "https://careerconfidential.com"),
        (2073, "https://www.confidential.com"), # This is likely a placeholder itself if company name is 'Confidential'
        (2075, "https://www.bloomberg.com"),
        (2128, "https://www.bestructured.com"),
        (2133, "https://www.concordcorporate.com"), # Fixed typo
        (1907, "https://www.halian.com"),
        (1813, "https://www.ft.com"),
        (1820, "https://www.thefemalelead.com"),
        (1822, "https://www.chanel.com"),
        (1835, "https://www.revolut.com"),
        (1839, "https://www.savethechildren.net"),
        (1842, "https://www.nesfircroft.com"),
        (1847, "https://www.antal.com"),
        (1849, "https://www.cam.ac.uk"),
        (1857, "https://www.computrabajo.com"),
        (1861, "https://www.britishcouncil.org"),
        (1872, "https://www.stellantis.com"), # FCA is now Stellantis
        (1874, "https://www.england.nhs.uk"),
        (1883, "https://www.technipfmc.com"),
        (1889, "https://www.storm2.com"),
        (1890, "https://www.petroplan.com"),
        (1893, "https://www.spencer-ogden.com"),
        (1899, "https://www.canonical.com"),
        (1926, "https://www.tide.co"),
        (1928, "https://cex.io"),
        (1931, "https://monzo.com"),
        (1934, "https://pe-insights.com"),
        (1936, "https://www.sky.com"),
        (1947, "https://www.monks.com"),
        (1949, "https://k2partnering.com"),
        (1952, "https://www.controlrisks.com")
    ]
    
    updated = 0
    for rec_id, website in corrections:
        cursor.execute("UPDATE businesses SET website = ? WHERE id = ?", (website, rec_id))
        updated += cursor.rowcount
        
    conn.commit()
    conn.close()
    print(f"Updated {updated} websites.")

if __name__ == "__main__":
    fix_websites()
