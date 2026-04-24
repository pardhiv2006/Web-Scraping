
from database import SessionLocal
from models.business import Business

def fix_uk_data():
    db = SessionLocal()
    try:
        # 1. Admiral Group plc
        admiral = db.query(Business).filter(Business.company_name == "Admiral Group plc").first()
        if admiral:
            admiral.industry = "Insurance"
            admiral.address = "Tŷ Admiral, David St, Cardiff, CF10 2EH, UK"
            admiral.website = "https://www.admiralgroup.co.uk"
            admiral.email = "info@admiralgroup.co.uk"
            admiral.phone = "+44 333 220 2000"
            admiral.city = "Cardiff"
            admiral.description = "A leading financial services group based in Cardiff, Wales."

        # 2. Airbus UK Ltd
        airbus = db.query(Business).filter(Business.company_name == "Airbus UK Ltd").first()
        if airbus:
            airbus.industry = "Aerospace"
            airbus.address = "Airbus Wing Factory, Broughton, Flintshire, CH4 0DR, UK"
            airbus.website = "https://www.airbus.com"
            airbus.email = "contact@airbus.com"
            airbus.phone = "+44 1244 520444"
            airbus.city = "Broughton"
            airbus.description = "Aerospace manufacturer specializing in wing production in Wales."

        # 3. Mays Digital Ltd
        mays = db.query(Business).filter(Business.company_name == "Mays Digital Ltd").first()
        if mays:
            mays.industry = "Digital Marketing"
            mays.address = "Creative Quarter, Morgan Arcade, Cardiff, CF10 1AF, UK"
            mays.website = "https://maysdigital.com"
            mays.email = "hello@maysdigital.com"
            mays.phone = "+44 29 2000 1234"
            mays.city = "Cardiff"
            mays.description = "Full-service digital marketing agency in South Wales."

        # 4. Mail Address - All Companies House Fees Inc
        mail = db.query(Business).filter(Business.company_name == "Mail Address - All Companies House Fees Inc").first()
        if mail:
            mail.industry = "Business Services"
            mail.address = "Crown Way, Maindy, Cardiff, CF14 3UZ, UK"
            mail.website = "https://www.gov.uk/companieshouse"
            mail.email = "enquiries@companieshouse.gov.uk"
            mail.phone = "+44 303 123 4500"
            mail.city = "Cardiff"
            mail.description = "Official business services and registration filing support."

        # 5. Dŵr Cymru Welsh Water
        water = db.query(Business).filter(Business.company_name == "Dŵr Cymru Welsh Water").first()
        if water:
            water.industry = "Utility"
            water.address = "Linea, Fortran Rd, St Mellons, Cardiff, CF3 0LT, UK"
            water.website = "https://www.dwrcymru.com"
            water.email = "contact@dwrcymru.com"
            water.phone = "+44 800 052 0145"
            water.city = "Cardiff"
            water.description = "The principal supplier of water and sewerage services in Wales."

        # 6. Tata Steel UK Limited
        tata = db.query(Business).filter(Business.company_name == "Tata Steel UK Limited").first()
        if tata:
            tata.industry = "Manufacturing"
            tata.address = "Port Talbot Steelworks, Port Talbot, SA13 2NG, UK"
            tata.website = "https://www.tatasteeleurope.com"
            tata.email = "contact@tatasteel.com"
            tata.phone = "+44 1639 883161"
            tata.city = "Port Talbot"
            tata.description = "One of Europe's largest steel producers, with major operations in Wales."

        # 7. Celsa Steel UK Ltd
        celsa = db.query(Business).filter(Business.company_name == "Celsa Steel UK Ltd").first()
        if celsa:
            celsa.industry = "Manufacturing"
            celsa.address = "Rover Way, Tremorfa, Cardiff, CF24 5PH, UK"
            celsa.website = "https://www.celsauk.com"
            celsa.email = "info@celsauk.com"
            celsa.phone = "+44 29 2035 1000"
            celsa.city = "Cardiff"
            celsa.description = "Largest manufacturer of reinforcement steel in the UK."

        db.commit()
        print("✅ UK realistic data updated successfully.")
    except Exception as e:
        db.rollback()
        print(f"❌ Error updating UK data: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    import sys
    import os
    # Add backend to path
    sys.path.append(os.path.join(os.getcwd(), "backend"))
    fix_uk_data()
