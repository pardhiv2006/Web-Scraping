import sqlite3

def update_batch_4_giants():
    db_path = "businesses.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    updates = [
        {"id": 1773, "ceo_name": "Mark Zuckerberg", "ceo_email": "mz@meta.com", "employee_count": "67,000", "revenue": "$134 Billion", "linkedin_url": "https://www.linkedin.com/company/meta", "description": "Meta Platforms, Inc. is an American multinational technology conglomerate based in Menlo Park, California. The company owns Facebook, Instagram, and WhatsApp, among other products and services."},
        {"id": 1774, "ceo_name": "Ted Sarandos", "ceo_email": "tsarandos@netflix.com", "employee_count": "13,000", "revenue": "$33.7 Billion", "linkedin_url": "https://www.linkedin.com/company/netflix", "description": "Netflix, Inc. is an American media company based in Los Gatos, California. It is one of the world's leading entertainment services with over 200 million paid memberships."},
        {"id": 1775, "ceo_name": "Marc Benioff", "ceo_email": "ceo@salesforce.com", "employee_count": "72,000", "revenue": "$34.9 Billion", "linkedin_url": "https://www.linkedin.com/company/salesforce", "description": "Salesforce, Inc. is an American cloud-based software company headquartered in San Francisco, California. It provides customer relationship management (CRM) software and applications."},
        {"id": 1957, "ceo_name": "Sundar Pichai", "ceo_email": "sundar@google.com", "employee_count": "182,000", "revenue": "$307 Billion", "linkedin_url": "https://www.linkedin.com/company/google", "description": "Google LLC is an American multinational technology company focusing on online advertising, search engine technology, cloud computing, and more."},
        {"id": 1958, "ceo_name": "Andy Jassy", "ceo_email": "ajassy@amazon.com", "employee_count": "1,500,000", "revenue": "$574 Billion", "linkedin_url": "https://www.linkedin.com/company/amazon", "description": "Amazon.com, Inc. is an American multinational technology company focusing on e-commerce, cloud computing, online advertising, digital streaming, and artificial intelligence."},
        {"id": 1959, "ceo_name": "Ryan Roslansky", "ceo_email": "ryan@linkedin.com", "employee_count": "21,000", "revenue": "$15 Billion", "linkedin_url": "https://www.linkedin.com/company/linkedin", "description": "LinkedIn is an American business and employment-oriented online service that operates via websites and mobile apps."},
        {"id": 1960, "ceo_name": "Satya Nadella", "ceo_email": "satyan@microsoft.com", "employee_count": "221,000", "revenue": "$211 Billion", "linkedin_url": "https://www.linkedin.com/company/microsoft", "description": "Microsoft Corporation is an American multinational technology corporation headquartered in Redmond, Washington."},
        {"id": 1961, "ceo_name": "Chris Anderson", "ceo_email": "chris@ted.com", "employee_count": "500", "revenue": "$60 Million", "linkedin_url": "https://www.linkedin.com/company/ted-conferences", "description": "TED is a non-profit organization devoted to Ideas Worth Spreading, usually in the form of short, powerful talks."},
        {"id": 1962, "ceo_name": "Joe Ucuzoglu", "ceo_email": "joe@deloitte.com", "employee_count": "457,000", "revenue": "$64.9 Billion", "linkedin_url": "https://www.linkedin.com/company/deloitte", "description": "Deloitte is a global professional services network, providing audit, consulting, financial advisory, risk management, and tax services."},
        {"id": 1963, "ceo_name": "Arvind Krishna", "ceo_email": "arvind.krishna@ibm.com", "employee_count": "288,000", "revenue": "$61.9 Billion", "linkedin_url": "https://www.linkedin.com/company/ibm", "description": "IBM (International Business Machines Corporation) is an American multinational technology corporation."},
        {"id": 1816, "ceo_name": "Emma Walmsley", "ceo_email": "emma.walmsley@gsk.com", "employee_count": "70,000", "revenue": "£30 Billion", "linkedin_url": "https://www.linkedin.com/company/gsk", "description": "GSK (GlaxoSmithKline) is a global biopharmaceutical company with a purpose to unite science, technology and talent to get ahead of disease together."},
        {"id": 1826, "ceo_name": "Tim Davie", "ceo_email": "tim.davie@bbc.co.uk", "employee_count": "21,000", "revenue": "£5.7 Billion", "linkedin_url": "https://www.linkedin.com/company/bbc", "description": "The British Broadcasting Corporation (BBC) is the national broadcaster of the United Kingdom."},
        {"id": 1936, "ceo_name": "Dana Strong", "ceo_email": "dana.strong@sky.uk", "employee_count": "30,000", "revenue": "$20 Billion", "linkedin_url": "https://www.linkedin.com/company/sky", "description": "Sky Group is a British media and telecommunications conglomerate, a subsidiary of Comcast."},
        {"id": 1835, "ceo_name": "Nikolay Storonsky", "ceo_email": "nik@revolut.com", "employee_count": "7,500", "revenue": "$1.1 Billion", "linkedin_url": "https://www.linkedin.com/company/revolut", "description": "Revolut is a global neobank and financial technology company offering banking services."},
        {"id": 1931, "ceo_name": "TS Anil", "ceo_email": "ts.anil@monzo.com", "employee_count": "2,500", "revenue": "$450 Million", "linkedin_url": "https://www.linkedin.com/company/monzo-bank", "description": "Monzo is a digital bank based in the United Kingdom, one of the first of a new breed of challenger banks."},
        {"id": 1813, "ceo_name": "John Ridding", "ceo_email": "john.ridding@ft.com", "employee_count": "2,500", "revenue": "£450 Million", "linkedin_url": "https://www.linkedin.com/company/financial-times", "description": "The Financial Times (FT) is a daily newspaper printed in broadsheet and published digitally that focuses on business and economic current affairs."},
        {"id": 1822, "ceo_name": "Leena Nair", "ceo_email": "leena.nair@chanel.com", "employee_count": "32,000", "revenue": "$17.2 Billion", "linkedin_url": "https://www.linkedin.com/company/chanel", "description": "Chanel is a French luxury fashion house that was founded by couturière Coco Chanel in 1910."}
    ]
    
    for up in updates:
        fields = []
        values = []
        for k, v in up.items():
            if k == 'id': continue
            fields.append(f"{k} = ?")
            values.append(v)
        
        if not fields: continue
        
        values.append(up['id'])
        query = f"UPDATE businesses SET {', '.join(fields)} WHERE id = ?"
        cursor.execute(query, tuple(values))
    
    conn.commit()
    conn.close()
    print("Batch 4 Giants updated successfully.")

if __name__ == "__main__":
    update_batch_4_giants()
