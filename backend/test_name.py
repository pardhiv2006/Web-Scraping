import sys
import os
sys.path.append(os.getcwd())
from services.smart_scraper import _is_valid_name
print("Date of birth:", _is_valid_name("Date of birth"))
print("-:", _is_valid_name("-"))
