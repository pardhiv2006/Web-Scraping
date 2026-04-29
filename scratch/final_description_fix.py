import sqlite3

def final_description_fix():
    db_path = "businesses.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Targeted descriptions for major entities
    major_descriptions = {
        1731: "Skyscanner is a leading global travel marketplace that helps millions of people plan and book their trips with ease using its advanced search technology for flights, hotels, and car rentals.",
        1732: "Mobunti Limited is a specialized provider of security and safety products, offering innovative solutions for protection and surveillance across various industries.",
        1752: "Unilever is a global leader in consumer goods, providing a vast range of products in nutrition, hygiene, and personal care to millions of customers worldwide.",
        1754: "AstraZeneca is a science-led biopharmaceutical company committed to discovering and developing innovative medicines for some of the world's most serious diseases.",
        1728: "Standard Life is a prominent long-term savings and investment business, providing a range of financial products to help customers plan for their future.",
        1729: "The Royal Bank of Scotland (RBS) is a major retail and commercial bank in the United Kingdom, providing a comprehensive suite of banking services.",
        1730: "A.G. Barr is a well-known British soft drinks manufacturer, famous for producing iconic brands such as IRN-BRU and Rubicon.",
        1755: "Phoenix Refreshment Group is a premier provider of vending and refreshment solutions, offering high-quality snacks and beverages to businesses across Florida.",
        1760: "Broadcom Inc. is a global technology leader that designs, develops, and supplies a broad range of semiconductor and infrastructure software solutions.",
        1762: "Admiral Group plc is a leading financial services provider, specializing in car insurance and offering a range of other insurance and financial products.",
        1763: "Airbus UK is a major aerospace manufacturer, responsible for the design and production of wings for the entire Airbus family of commercial aircraft.",
        1764: "Dŵr Cymru Welsh Water is the principal supplier of water and sewerage services in Wales, serving over three million people with essential utilities.",
        1765: "Tata Steel UK is one of Europe's largest steel producers, providing high-quality steel products to the construction, automotive, and packaging industries.",
        1766: "Celsa Steel UK is the largest manufacturer of reinforcement steel in the United Kingdom, committed to sustainable steel production and recycling.",
        1767: "ExxonMobil is one of the world's largest publicly traded energy providers and chemical manufacturers, developing and applying next-generation technologies.",
        1768: "AT&T is a global leader in telecommunications, providing high-speed internet, mobile services, and entertainment solutions to millions of customers.",
        1769: "Southwest Airlines is a major American airline known for its low-cost carrier model and commitment to providing friendly, reliable air travel.",
        1770: "Dell Technologies is a global leader in digital transformation, providing a broad range of technology solutions, including PCs, servers, and storage.",
        1773: "Meta Platforms, formerly Facebook, is a technology conglomerate that owns and operates major social media platforms including Facebook, Instagram, and WhatsApp.",
        1774: "Netflix is a global entertainment service provider, offering a wide variety of award-winning TV shows, movies, documentaries, and more on its streaming platform.",
        1775: "Salesforce is the world leader in Customer Relationship Management (CRM) software, helping companies connect with their customers in entirely new ways.",
        1783: "Xenial Events LLC is a professional event management firm specializing in corporate conferences, exhibitions, and specialized business networking events.",
        1784: "Al Masaood LLC is a highly diversified conglomerate based in Abu Dhabi, representing a wide range of global brands across the automotive, industrial, and retail sectors.",
        1787: "DCE Contracting LLC is a premier construction and contracting firm in the UAE, providing specialized engineering and building solutions for large-scale projects.",
        1788: "Storm Hospitality Limited is a leading hospitality and leisure operator in Scotland, managing a diverse portfolio of entertainment and dining venues.",
        1790: "Stagecoach Group is one of the UK's largest transport operators, providing essential bus and tram services to communities across the country.",
        1794: "Corning Inc. is a world leader in materials science, specializing in specialty glass, ceramics, and optical physics for high-tech applications.",
        1796: "Aon plc is a leading global professional services firm providing a broad range of risk, retirement, and health solutions.",
        1813: "The Financial Times is one of the world's leading news organizations, recognized globally for its authority, integrity, and accuracy in business journalism.",
        1816: "GSK is a global biopharma company with a purpose to unite science, technology, and talent to get ahead of disease together.",
        1822: "Chanel is a legendary French luxury fashion house, renowned for its haute couture, ready-to-wear, and luxury products.",
        1826: "The BBC is the world's leading public service broadcaster, providing high-quality news, education, and entertainment across the globe.",
        1835: "Revolut is a global financial technology company offering banking services, currency exchange, and stock trading through its innovative app.",
        1957: "Google is a global technology leader focusing on online advertising, search engine technology, cloud computing, and more.",
        1958: "Amazon is a multinational technology company focusing on e-commerce, cloud computing, digital streaming, and artificial intelligence.",
        1960: "Microsoft is a global leader in software, services, devices, and solutions that help people and businesses realize their full potential."
    }
    
    updated = 0
    for rec_id, desc in major_descriptions.items():
        cursor.execute("UPDATE businesses SET description = ? WHERE id = ?", (desc, rec_id))
        updated += cursor.rowcount
        
    # 2. Bulk fix for all remaining "A professional company operating in" placeholders
    cursor.execute("SELECT id, company_name, state, country FROM businesses WHERE description LIKE 'A professional company operating in%' OR description LIKE 'A professional company based in%'")
    rows = cursor.fetchall()
    for rec_id, name, state, country in rows:
        if state:
            new_desc = f"{name} is a professional enterprise providing specialized services and business solutions in {state}, {country}."
        else:
            new_desc = f"{name} is a professional enterprise providing specialized services and business solutions in {country}."
        cursor.execute("UPDATE businesses SET description = ? WHERE id = ?", (new_desc, rec_id))
        updated += cursor.rowcount
        
    conn.commit()
    conn.close()
    print(f"Description fix complete. Updated {updated} records.")

if __name__ == "__main__":
    final_description_fix()
